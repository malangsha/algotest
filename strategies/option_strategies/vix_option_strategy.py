"""
VIX Option Strategy

This strategy trades options based on VIX levels and volatility signals.
"""

import logging
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent
from models.position import Position
from utils.constants import SignalType
from utils.time_utils import is_market_open, time_in_range
from utils.technical_indicators import calculate_ema, calculate_rsi
from strategies.strategy_registry import StrategyRegistry

@StrategyRegistry.register('vix_option_strategy')
class VixOptionStrategy(OptionStrategy):
    """
    VIX Option Strategy
    
    Trades options based on VIX levels and volatility signals.
    Implements mean reversion and breakout strategies based on VIX levels.
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
        self.vix_threshold_high = config.get('parameters', {}).get('vix_threshold_high', 25)
        self.vix_threshold_low = config.get('parameters', {}).get('vix_threshold_low', 15)
        self.rsi_period = config.get('parameters', {}).get('rsi_period', 14)
        self.rsi_overbought = config.get('parameters', {}).get('rsi_overbought', 70)
        self.rsi_oversold = config.get('parameters', {}).get('rsi_oversold', 30)
        self.stop_loss_percent = config.get('parameters', {}).get('stop_loss_percent', 20)
        self.target_percent = config.get('parameters', {}).get('target_percent', 10)
        
        # Entry and exit times
        self.entry_time = dt_time.fromisoformat(config.get('parameters', {}).get('entry_time', '09:45:00'))
        self.exit_time = dt_time.fromisoformat(config.get('parameters', {}).get('exit_time', '15:00:00'))
        
        # Position tracking
        self.position_opened = False
        self.entry_pending = True
        self.exit_triggered = False
        
        # Store positions and price history
        self.positions = {}  # index -> position info
        self.vix_history = {}  # index -> list of VIX values
        self.rsi_values = {}  # index -> current RSI value
        
        self.logger.info(f"VIX Option Strategy initialized for indices: {self.indices}")
        
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
            
            # For high VIX: Buy puts
            # For low VIX: Buy calls
            option_requirements[index] = [
                {'strike': atm_strike, 'option_type': 'PE', 'expiry_offset': self.expiry_offset},
                {'strike': atm_strike, 'option_type': 'CE', 'expiry_offset': self.expiry_offset}
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
            # Update VIX if available
            vix = event.data.get('vix') or event.data.get('volatility')
            if vix:
                if symbol not in self.vix_history:
                    self.vix_history[symbol] = []
                self.vix_history[symbol].append(vix)
                
                # Calculate RSI if we have enough data
                if len(self.vix_history[symbol]) >= self.rsi_period:
                    self.rsi_values[symbol] = calculate_rsi(self.vix_history[symbol], self.rsi_period)
                
                # If positions are open, check for exit conditions
                if self.position_opened and not self.exit_triggered:
                    self._check_exit_conditions(symbol, vix, event.timestamp)
        
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
                
            # Get current VIX and RSI
            vix = self.vix_history.get(index, [0])[-1] if self.vix_history.get(index) else 0
            rsi = self.rsi_values.get(index, 50)  # Default to neutral if not available
            
            # Calculate ATM strike
            atm_strike = self.option_manager._get_atm_strike(current_price, strike_interval)
            
            # Determine option type based on VIX and RSI
            if vix >= self.vix_threshold_high and rsi >= self.rsi_overbought:
                # High VIX and overbought RSI -> Buy puts
                option_type = 'PE'
                signal_type = SignalType.BUY
            elif vix <= self.vix_threshold_low and rsi <= self.rsi_oversold:
                # Low VIX and oversold RSI -> Buy calls
                option_type = 'CE'
                signal_type = SignalType.BUY
            else:
                continue
                
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
                'strategy': 'vix_option',
                'index': index,
                'type': option_type,
                'vix': vix,
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
                'vix_entry': vix,
                'rsi_entry': rsi
            }
            
            self.logger.info(f"Entered {index} {option_type} position at {option_price} (VIX: {vix}, RSI: {rsi})")
            
        # Update state if we entered any positions
        if self.positions:
            self.position_opened = True
            self.entry_pending = False
            
    def _check_exit_conditions(self, index: str, vix: float, timestamp: float):
        """Check if we should exit positions based on current market conditions"""
        if index not in self.positions:
            return
            
        # Get current price
        current_price = self.option_manager.get_option_price(self.positions[index]['symbol'])
        if not current_price:
            return
            
        # Update position tracking
        self.positions[index]['current_price'] = current_price
        
        # Get current RSI
        current_rsi = self.rsi_values.get(index, 50)
        
        # Check stop loss
        if current_price <= self.positions[index]['stop_loss']:
            self._exit_all_positions("Stop loss triggered")
            return
            
        # Check target
        if current_price >= self.positions[index]['target']:
            self._exit_all_positions("Target achieved")
            return
            
        # Check VIX reversal
        position = self.positions[index]
        if position['symbol'].endswith('PE'):
            # For puts, exit if VIX drops significantly
            if vix <= position['vix_entry'] * 0.9:  # 10% drop
                self._exit_all_positions("VIX reversal")
                return
        else:
            # For calls, exit if VIX rises significantly
            if vix >= position['vix_entry'] * 1.1:  # 10% rise
                self._exit_all_positions("VIX reversal")
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
                'strategy': 'vix_option',
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
            
        self.logger.info("VIX Option Strategy stopped") 