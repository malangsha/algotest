"""
ATM Option Strategy

This strategy trades ATM (At-The-Money) options based on price action and volatility.
"""

import logging
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent
from models.position import Position
from utils.constants import SignalType
from utils.time_utils import is_market_open, time_in_range
from utils.technical_indicators import calculate_ema, calculate_rsi, calculate_bollinger_bands
from strategies.strategy_registry import StrategyRegistry

@StrategyRegistry.register('atm_option_strategy')
class AtmOptionStrategy(OptionStrategy):
    """
    ATM Option Strategy
    
    Trades ATM options based on price action and volatility.
    Uses Bollinger Bands and RSI for entry/exit signals.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any], data_manager, option_manager, portfolio_manager, event_manager):
        """
        Initialize the strategy.
        
        Args:
            strategy_id: Unique identifier for the strategy
            config: Strategy configuration dictionary
            data_manager: Market data manager instance
            option_manager: Option manager for option-specific operations
            portfolio_manager: Portfolio manager for position management
            event_manager: Event manager for event handling
        """
        super().__init__(strategy_id, config, data_manager, option_manager, portfolio_manager, event_manager)
        
        # Strategy-specific configurations
        self.indices = config.get('indices', [])
        self.expiry_offset = config.get('expiry_offset', 0)
        self.bb_period = config.get('parameters', {}).get('bb_period', 20)
        self.bb_std = config.get('parameters', {}).get('bb_std', 2)
        self.rsi_period = config.get('parameters', {}).get('rsi_period', 14)
        self.rsi_overbought = config.get('parameters', {}).get('rsi_overbought', 70)
        self.rsi_oversold = config.get('parameters', {}).get('rsi_oversold', 30)
        self.stop_loss_percent = config.get('parameters', {}).get('stop_loss_percent', 20)
        self.target_percent = config.get('parameters', {}).get('target_percent', 10)
        self.volatility_threshold = config.get('parameters', {}).get('volatility_threshold', 0.02)
        
        # Entry and exit times
        self.entry_time = dt_time.fromisoformat(config.get('parameters', {}).get('entry_time', '09:45:00'))
        self.exit_time = dt_time.fromisoformat(config.get('parameters', {}).get('exit_time', '15:00:00'))
        
        # Position tracking
        self.position_opened = False
        self.entry_pending = True
        self.exit_triggered = False
        
        # Store positions and price history
        self.positions = {}  # index -> position info
        self.price_history = {}  # index -> list of prices
        self.bb_values = {}  # index -> (upper, middle, lower) bands
        self.rsi_values = {}  # index -> current RSI value
        
        self.logger.info(f"ATM Option Strategy initialized for indices: {self.indices}")
        
    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all options required by this strategy.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping index symbols to lists of option requirements
        """
        option_requirements = {}
        
        for index in self.indices:
            # Get strike interval from option manager
            strike_interval = self.option_manager.strike_intervals.get(index)
            if not strike_interval:
                self.logger.warning(f"Strike interval not configured for {index}")
                continue
                
            # Get current index price
            current_price = self.option_manager.index_prices.get(index)
            if not current_price:
                self.logger.warning(f"Current price not available for {index}")
                continue
                
            # Calculate ATM strike
            atm_strike = self.option_manager._get_atm_strike(current_price, strike_interval)
            
            # Subscribe to both call and put options
            option_requirements[index] = [
                {'strike': atm_strike, 'option_type': 'CE', 'expiry_offset': self.expiry_offset},
                {'strike': atm_strike, 'option_type': 'PE', 'expiry_offset': self.expiry_offset}
            ]
            
        return option_requirements
        
    def on_market_data(self, event: MarketDataEvent):
        """
        Process market data events
        
        Args:
            event: Market data event
        """
        symbol = event.instrument.symbol
        current_time = datetime.now().time()
        
        # Check if this is an index update
        if symbol in self.indices:
            # Update price history
            price = event.data.get('close') or event.data.get('last') or event.data.get('price')
            if price:
                if symbol not in self.price_history:
                    self.price_history[symbol] = []
                self.price_history[symbol].append(price)
                
                # Calculate indicators if we have enough data
                if len(self.price_history[symbol]) >= self.bb_period:
                    # Calculate Bollinger Bands
                    upper, middle, lower = calculate_bollinger_bands(self.price_history[symbol], self.bb_period, self.bb_std)
                    self.bb_values[symbol] = (upper, middle, lower)
                    
                    # Calculate RSI
                    self.rsi_values[symbol] = calculate_rsi(self.price_history[symbol], self.rsi_period)
                
                # If positions are open, check for exit conditions
                if self.position_opened and not self.exit_triggered:
                    self._check_exit_conditions(symbol, price, event.timestamp)
        
        # Check if this is an option update and we have positions
        elif self.position_opened and self._is_option_symbol(symbol):
            if symbol in [pos['symbol'] for pos in self.positions.values()]:
                # Update position tracking and check for stop loss/target
                self._update_option_position(symbol, event)
                
    def on_bar(self, event: BarEvent):
        """
        Process bar events
        
        Args:
            event: Bar event
        """
        symbol = event.instrument.symbol
        current_time = datetime.now().time()
        
        # Check if this is the primary timeframe
        if event.timeframe != self.timeframe:
            return
        
        # Check entry conditions at the right time
        if (self.entry_pending and not self.position_opened and 
            time_in_range(self.entry_time, current_time, self.exit_time) and
            is_market_open()):
            
            # Try to enter positions
            self._try_enter_positions()
        
        # Check time-based exit
        if (self.position_opened and not self.exit_triggered and 
            current_time >= self.exit_time):
            
            self._exit_all_positions("Time-based exit")
            
    def _try_enter_positions(self):
        """Try to enter positions for all configured indices"""
        for index in self.indices:
            # Get strike interval
            strike_interval = self.option_manager.strike_intervals.get(index)
            if not strike_interval:
                self.logger.warning(f"Strike interval not configured for {index}")
                continue
                
            # Get current index price
            current_price = self.option_manager.index_prices.get(index)
            if not current_price:
                self.logger.warning(f"Current price not available for {index}")
                continue
                
            # Get current indicators
            bb = self.bb_values.get(index)
            rsi = self.rsi_values.get(index, 50)  # Default to neutral if not available
            
            if not bb:
                continue
                
            upper, middle, lower = bb
            
            # Calculate volatility (BB width)
            volatility = (upper - lower) / middle
            
            # Determine option type based on price action and indicators
            if (current_price >= upper and rsi >= self.rsi_overbought and 
                volatility >= self.volatility_threshold):
                # Price at upper band, overbought RSI, high volatility -> Buy puts
                option_type = 'PE'
                signal_type = SignalType.BUY
            elif (current_price <= lower and rsi <= self.rsi_oversold and 
                  volatility >= self.volatility_threshold):
                # Price at lower band, oversold RSI, high volatility -> Buy calls
                option_type = 'CE'
                signal_type = SignalType.BUY
            else:
                continue
                
            # Calculate ATM strike
            atm_strike = self.option_manager._get_atm_strike(current_price, strike_interval)
            
            # Get option symbol
            option_symbol = self.option_manager.get_option_symbol(index, atm_strike, option_type, self.expiry_offset)
            if not option_symbol:
                self.logger.warning(f"Could not get option symbol for {index}")
                continue
                
            # Get option price
            option_price = self.option_manager.get_option_price(option_symbol)
            if not option_price:
                self.logger.warning(f"Option price not available for {option_symbol}")
                continue
                
            # Generate signal
            self.generate_signal(option_symbol, signal_type, {
                'price': option_price,
                'quantity': 1,
                'strategy': 'atm_option',
                'index': index,
                'type': option_type,
                'volatility': volatility,
                'rsi': rsi
            })
            
            # Update position tracking
            self.positions[index] = {
                'symbol': option_symbol,
                'entry_price': option_price,
                'current_price': option_price,
                'high_price': option_price,
                'stop_loss': option_price * (1 - self.stop_loss_percent / 100),
                'target': option_price * (1 + self.target_percent / 100),
                'volatility_entry': volatility,
                'rsi_entry': rsi
            }
            
            self.logger.info(f"Entered {index} {option_type} position at {option_price} (Vol: {volatility:.2%}, RSI: {rsi})")
            
        # Update state if we entered any positions
        if self.positions:
            self.position_opened = True
            self.entry_pending = False
            
    def _check_exit_conditions(self, index: str, price: float, timestamp: float):
        """Check if we should exit positions based on current market conditions"""
        if index not in self.positions:
            return
            
        # Get current price
        current_price = self.option_manager.get_option_price(self.positions[index]['symbol'])
        if not current_price:
            return
            
        # Update position tracking
        self.positions[index]['current_price'] = current_price
        
        # Get current indicators
        bb = self.bb_values.get(index)
        current_rsi = self.rsi_values.get(index, 50)
        
        if not bb:
            return
            
        upper, middle, lower = bb
        
        # Calculate current volatility
        current_volatility = (upper - lower) / middle
        
        # Check stop loss
        if current_price <= self.positions[index]['stop_loss']:
            self._exit_all_positions("Stop loss triggered")
            return
            
        # Check target
        if current_price >= self.positions[index]['target']:
            self._exit_all_positions("Target achieved")
            return
            
        # Check volatility reversal
        position = self.positions[index]
        if current_volatility <= position['volatility_entry'] * 0.7:  # 30% drop in volatility
            self._exit_all_positions("Volatility reversal")
            return
            
        # Check RSI reversal
        if position['symbol'].endswith('PE'):
            # For puts, exit if RSI drops below overbought
            if current_rsi <= self.rsi_overbought - 10:
                self._exit_all_positions("RSI reversal")
                return
        else:
            # For calls, exit if RSI rises above oversold
            if current_rsi >= self.rsi_oversold + 10:
                self._exit_all_positions("RSI reversal")
                return
            
    def _update_option_position(self, symbol: str, event: MarketDataEvent):
        """Update position tracking for an option"""
        # Find which position this symbol belongs to
        position = None
        for index in self.positions:
            if self.positions[index]['symbol'] == symbol:
                position = self.positions[index]
                break
                
        if not position:
            return
            
        # Update price
        price = event.data.get('close') or event.data.get('last') or event.data.get('price')
        if price:
            position['current_price'] = price
            position['high_price'] = max(position['high_price'], price)
                
    def _exit_all_positions(self, reason: str):
        """Exit all positions"""
        self.logger.info(f"Exiting all positions: {reason}")
        
        for index in self.positions:
            # Generate sell signal to close position
            self.generate_signal(self.positions[index]['symbol'], SignalType.SELL, {
                'price': self.positions[index]['current_price'],
                'quantity': 1,
                'strategy': 'atm_option',
                'index': index,
                'type': self.positions[index]['symbol'][-2:],  # CE or PE
                'exit': True
            })
            
        # Clear positions
        self.positions = {}
        
        # Update state
        self.position_opened = False
        self.exit_triggered = True
        
    def on_fill(self, event):
        """Handle fill events"""
        # Update performance tracking
        if hasattr(event, 'signal_type') and hasattr(event, 'symbol'):
            symbol = event.symbol
            
            if event.signal_type == SignalType.BUY:
                self.logger.info(f"Buy order filled for {symbol} at {event.fill_price}")
            elif event.signal_type == SignalType.SELL:
                self.logger.info(f"Sell order filled for {symbol} at {event.fill_price}")
                
                # Check if this was a profitable trade
                if hasattr(event, 'entry_price') and event.fill_price > event.entry_price:
                    self.successful_trades += 1
                else:
                    self.failed_trades += 1
                    
    def on_stop(self):
        """Called when the strategy is stopped"""
        # Exit all positions if any are open
        if self.position_opened and not self.exit_triggered:
            self._exit_all_positions("Strategy stopped")
            
        self.logger.info("ATM Option Strategy stopped") 