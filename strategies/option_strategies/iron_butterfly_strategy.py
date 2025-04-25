"""
Iron Butterfly Option Strategy

This strategy sells an ATM straddle and buys wings (OTM calls and puts)
to create a limited risk strategy that profits from low volatility.
"""

import logging
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent
from models.position import Position
from utils.constants import SignalType
from utils.time_utils import is_market_open, time_in_range
from strategies.strategy_registry import StrategyRegistry

@StrategyRegistry.register('iron_butterfly_strategy')
class IronButterflyStrategy(OptionStrategy):
    """
    Iron Butterfly Option Strategy
    
    Sells ATM straddle and buys wings to create a limited risk strategy
    that profits from low volatility. Implements stop-loss and target-based exits.
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
        self.wings_width = config.get('parameters', {}).get('wings_width', 500)
        self.stop_loss_percent = config.get('parameters', {}).get('stop_loss_percent', 40)
        self.target_percent = config.get('parameters', {}).get('target_percent', 20)
        
        # Entry and exit times
        self.entry_time = dt_time.fromisoformat(config.get('parameters', {}).get('entry_time', '09:45:00'))
        self.exit_time = dt_time.fromisoformat(config.get('parameters', {}).get('exit_time', '15:00:00'))
        
        # Position tracking
        self.position_opened = False
        self.entry_pending = True
        self.exit_triggered = False
        
        # Store positions
        self.ce_positions = {}  # index -> position info
        self.pe_positions = {}  # index -> position info
        self.wing_ce_positions = {}  # index -> position info
        self.wing_pe_positions = {}  # index -> position info
        
        self.logger.info(f"Iron Butterfly Strategy initialized for indices: {self.indices}")
        
    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all options required by this strategy.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping index symbols to lists of option requirements
        """
        option_requirements = {}
        
        for index in self.indices:
            # ATM options for straddle
            option_requirements[index] = [
                {'strike': 'ATM', 'option_type': 'CE', 'expiry_offset': self.expiry_offset},
                {'strike': 'ATM', 'option_type': 'PE', 'expiry_offset': self.expiry_offset},
                # Wing options
                {'strike': f'ATM+{self.wings_width}', 'option_type': 'CE', 'expiry_offset': self.expiry_offset},
                {'strike': f'ATM-{self.wings_width}', 'option_type': 'PE', 'expiry_offset': self.expiry_offset}
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
            # Update ATM options if price changed significantly
            price = event.data.get('close') or event.data.get('last') or event.data.get('price')
            if price:
                # Store current index price
                self.option_manager.update_index_price(symbol, price)
                
                # If positions are open, check for exit conditions
                if self.position_opened and not self.exit_triggered:
                    self._check_exit_conditions(symbol, price, event.timestamp)
        
        # Check if this is an option update and we have positions
        elif self.position_opened and self._is_option_symbol(symbol):
            if (symbol in self.ce_positions.values() or 
                symbol in self.pe_positions.values() or
                symbol in self.wing_ce_positions.values() or
                symbol in self.wing_pe_positions.values()):
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
        """Try to enter iron butterfly positions for all configured indices"""
        for index in self.indices:
            # Get ATM strike
            atm_strike = self.option_manager.get_atm_strike(index)
            if not atm_strike:
                self.logger.warning(f"Could not get ATM strike for {index}")
                continue
                
            # Get option symbols
            ce_symbol = self.option_manager.get_option_symbol(index, atm_strike, 'CE', self.expiry_offset)
            pe_symbol = self.option_manager.get_option_symbol(index, atm_strike, 'PE', self.expiry_offset)
            wing_ce_symbol = self.option_manager.get_option_symbol(index, atm_strike + self.wings_width, 'CE', self.expiry_offset)
            wing_pe_symbol = self.option_manager.get_option_symbol(index, atm_strike - self.wings_width, 'PE', self.expiry_offset)
            
            if not all([ce_symbol, pe_symbol, wing_ce_symbol, wing_pe_symbol]):
                self.logger.warning(f"Could not get all option symbols for {index} iron butterfly")
                continue
                
            # Get option prices
            ce_price = self.option_manager.get_option_price(ce_symbol)
            pe_price = self.option_manager.get_option_price(pe_symbol)
            wing_ce_price = self.option_manager.get_option_price(wing_ce_symbol)
            wing_pe_price = self.option_manager.get_option_price(wing_pe_symbol)
            
            if not all([ce_price, pe_price, wing_ce_price, wing_pe_price]):
                self.logger.warning(f"Option prices not available for {index} iron butterfly")
                continue
                
            # Calculate net credit
            net_credit = (ce_price + pe_price) - (wing_ce_price + wing_pe_price)
            
            # Generate sell signals for ATM options
            self.generate_signal(ce_symbol, SignalType.SELL, {
                'price': ce_price,
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'CE'
            })
            
            self.generate_signal(pe_symbol, SignalType.SELL, {
                'price': pe_price,
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'PE'
            })
            
            # Generate buy signals for wing options
            self.generate_signal(wing_ce_symbol, SignalType.BUY, {
                'price': wing_ce_price,
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'WING_CE'
            })
            
            self.generate_signal(wing_pe_symbol, SignalType.BUY, {
                'price': wing_pe_price,
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'WING_PE'
            })
            
            # Update position tracking
            self.ce_positions[index] = {
                'symbol': ce_symbol,
                'entry_price': ce_price,
                'current_price': ce_price,
                'high_price': ce_price,
                'stop_loss': ce_price * (1 + self.stop_loss_percent / 100),
                'target': ce_price * (1 - self.target_percent / 100)
            }
            
            self.pe_positions[index] = {
                'symbol': pe_symbol,
                'entry_price': pe_price,
                'current_price': pe_price,
                'high_price': pe_price,
                'stop_loss': pe_price * (1 + self.stop_loss_percent / 100),
                'target': pe_price * (1 - self.target_percent / 100)
            }
            
            self.wing_ce_positions[index] = {
                'symbol': wing_ce_symbol,
                'entry_price': wing_ce_price,
                'current_price': wing_ce_price
            }
            
            self.wing_pe_positions[index] = {
                'symbol': wing_pe_symbol,
                'entry_price': wing_pe_price,
                'current_price': wing_pe_price
            }
            
            self.logger.info(f"Entered {index} iron butterfly - Net Credit: {net_credit:.2f}")
            
        # Update state if we entered any positions
        if self.ce_positions or self.pe_positions:
            self.position_opened = True
            self.entry_pending = False
            
    def _check_exit_conditions(self, index: str, price: float, timestamp: float):
        """Check if we should exit positions based on current market conditions"""
        if index not in self.ce_positions or index not in self.pe_positions:
            return
            
        # Get current prices
        ce_price = self.option_manager.get_option_price(self.ce_positions[index]['symbol'])
        pe_price = self.option_manager.get_option_price(self.pe_positions[index]['symbol'])
        
        if not ce_price or not pe_price:
            return
            
        # Update position tracking
        self.ce_positions[index]['current_price'] = ce_price
        self.pe_positions[index]['current_price'] = pe_price
        
        # Check stop loss
        if (ce_price >= self.ce_positions[index]['stop_loss'] or 
            pe_price >= self.pe_positions[index]['stop_loss']):
            
            self._exit_all_positions("Stop loss triggered")
            return
            
        # Check target
        if (ce_price <= self.ce_positions[index]['target'] and 
            pe_price <= self.pe_positions[index]['target']):
            
            self._exit_all_positions("Target achieved")
            return
            
    def _update_option_position(self, symbol: str, event: MarketDataEvent):
        """Update position tracking for an option"""
        # Find which position this symbol belongs to
        position = None
        position_type = None
        
        for index in self.ce_positions:
            if self.ce_positions[index]['symbol'] == symbol:
                position = self.ce_positions[index]
                position_type = 'CE'
                break
            elif self.pe_positions[index]['symbol'] == symbol:
                position = self.pe_positions[index]
                position_type = 'PE'
                break
            elif self.wing_ce_positions[index]['symbol'] == symbol:
                position = self.wing_ce_positions[index]
                position_type = 'WING_CE'
                break
            elif self.wing_pe_positions[index]['symbol'] == symbol:
                position = self.wing_pe_positions[index]
                position_type = 'WING_PE'
                break
                
        if not position:
            return
            
        # Update price
        price = event.data.get('close') or event.data.get('last') or event.data.get('price')
        if price:
            position['current_price'] = price
            
            # Update high price for CE/PE positions
            if position_type in ['CE', 'PE']:
                position['high_price'] = max(position['high_price'], price)
                
    def _exit_all_positions(self, reason: str):
        """Exit all positions"""
        self.logger.info(f"Exiting all positions: {reason}")
        
        for index in self.ce_positions:
            # Generate buy signals to close ATM positions
            self.generate_signal(self.ce_positions[index]['symbol'], SignalType.BUY, {
                'price': self.ce_positions[index]['current_price'],
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'CE',
                'exit': True
            })
            
            self.generate_signal(self.pe_positions[index]['symbol'], SignalType.BUY, {
                'price': self.pe_positions[index]['current_price'],
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'PE',
                'exit': True
            })
            
            # Generate sell signals to close wing positions
            self.generate_signal(self.wing_ce_positions[index]['symbol'], SignalType.SELL, {
                'price': self.wing_ce_positions[index]['current_price'],
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'WING_CE',
                'exit': True
            })
            
            self.generate_signal(self.wing_pe_positions[index]['symbol'], SignalType.SELL, {
                'price': self.wing_pe_positions[index]['current_price'],
                'quantity': 1,
                'strategy': 'iron_butterfly',
                'index': index,
                'type': 'WING_PE',
                'exit': True
            })
            
        # Clear positions
        self.ce_positions = {}
        self.pe_positions = {}
        self.wing_ce_positions = {}
        self.wing_pe_positions = {}
        
        # Update state
        self.position_opened = False
        self.exit_triggered = True
        
    def on_fill(self, event):
        """Handle fill events"""
        # Update performance tracking
        if hasattr(event, 'signal_type') and hasattr(event, 'symbol'):
            symbol = event.symbol
            
            if event.signal_type == SignalType.SELL:
                self.logger.info(f"Sell order filled for {symbol} at {event.fill_price}")
            elif event.signal_type == SignalType.BUY:
                self.logger.info(f"Buy order filled for {symbol} at {event.fill_price}")
                
                # Check if this was a profitable trade
                if hasattr(event, 'entry_price') and event.fill_price < event.entry_price:
                    self.successful_trades += 1
                else:
                    self.failed_trades += 1
                    
    def on_stop(self):
        """Called when the strategy is stopped"""
        # Exit all positions if any are open
        if self.position_opened and not self.exit_triggered:
            self._exit_all_positions("Strategy stopped")
            
        self.logger.info("Iron Butterfly Strategy stopped") 