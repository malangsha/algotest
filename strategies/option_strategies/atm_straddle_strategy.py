"""
ATM Straddle Option Strategy

This strategy buys both Call and Put options at the ATM strike price
and aims to profit from large price movements in either direction.
"""

import logging
import time
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set

import numpy as np

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent
from models.position import Position
from utils.constants import SignalType
from utils.time_utils import is_market_open, time_in_range
from strategies.strategy_registry import StrategyRegistry

@StrategyRegistry.register('atm_straddle_strategy')
class AtmStraddleStrategy(OptionStrategy):
    """
    ATM Straddle Option Strategy
    
    Buys both CE and PE options at the ATM strike and profits from large price movements
    in either direction. Implements stop-loss and target-based exits.
    """
    
    def __init__(self, strategy_id: str, config: Dict[str, Any], 
                 data_manager, option_manager, portfolio_manager, event_manager):
        super().__init__(strategy_id, config, data_manager, 
                        option_manager, portfolio_manager, event_manager)
        
        # Extract strategy-specific parameters
        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:30:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.stop_loss_percent = self.params.get('stop_loss_percent', 30)
        self.target_percent = self.params.get('target_percent', 15)
        self.trail_points = self.params.get('trail_points', 0.25)
        
        # Parse time strings to time objects
        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)
        
        # Parse underlyings (handle both old and new formats)
        underlyings_config = config.get('underlyings', [])
        self.underlyings = []
        
        if isinstance(underlyings_config, list):
            for underlying in underlyings_config:
                if isinstance(underlying, str):
                    # Old format: just the symbol name
                    self.underlyings.append(underlying)
                elif isinstance(underlying, dict) and 'name' in underlying:
                    # New format: dict with name and exchanges
                    self.underlyings.append(underlying['name'])
        
        self.expiry_offset = config.get('expiry_offset', 0)
        
        # Position tracking
        self.ce_positions = {}  # underlying -> {symbol, entry_price, high_price, stop_loss, target}
        self.pe_positions = {}  # underlying -> {symbol, entry_price, high_price, stop_loss, target}
        self.position_opened = False
        self.entry_atm_prices = {}  # underlying -> atm_price at entry
        
        # State
        self.entry_pending = False
        self.exit_triggered = False
        
        self.logger.info(f"ATM Straddle Strategy initialized with SL: {self.stop_loss_percent}%, Target: {self.target_percent}%, Underlyings: {self.underlyings}")
    
    def _parse_time(self, time_str: str) -> dt_time:
        """Parse time string to time object"""
        try:
            hour, minute, second = map(int, time_str.split(':'))
            return dt_time(hour, minute, second)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format: {time_str}, using default")
            return dt_time(9, 30, 0) if 'entry' in time_str else dt_time(15, 15, 0)
    
    def on_start(self):
        """Called when the strategy is started"""
        # Initialize state
        self.entry_pending = True
        self.exit_triggered = False
        self.position_opened = False
        
        # Subscribe to all required underlyings
        for underlying in self.underlyings:
            self.request_symbol(underlying)
        
        self.logger.info(f"ATM Straddle Strategy started, waiting for entry time: {self.entry_time_str}")
    
    def on_market_data(self, event: MarketDataEvent):
        """
        Process market data events
        
        Args:
            event: Market data event
        """
        symbol = event.instrument.symbol
        current_time = datetime.now().time()

        self.logger.debug(f"Received MarketDataEvent: {event}")
        
        # Check if this is an underlying update
        if symbol in self.underlyings:
            # Update ATM options if price changed significantly
            price = event.data.get('CLOSE') or event.data.get('LAST_PRICE')
            if price:
                # Store current underlying price
                self.option_manager.update_underlying_price(symbol, price)
                
                # If positions are open, check for exit conditions
                if self.position_opened and not self.exit_triggered:
                    self._check_exit_conditions(symbol, price, event.timestamp)
        
        # Check if this is an option update and we have positions
        elif self.position_opened and self._is_option_symbol(symbol):
            if symbol in self.ce_positions.values() or symbol in self.pe_positions.values():
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
        self.logger.info(f"Received BarEvent: {event}")
        
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
        """Try to enter straddle positions for all configured underlyings"""
        for underlying in self.underlyings:
            # Check if we already have positions for this underlying
            if underlying in self.ce_positions or underlying in self.pe_positions:
                continue
                
            # Get current ATM strike
            atm_strike = self.option_manager.current_atm_strikes.get(underlying)
            underlying_price = self.option_manager.underlying_prices.get(underlying)
            
            if not atm_strike or not underlying_price:
                self.logger.warning(f"Cannot enter positions for {underlying}: ATM strike or underlying price not available")
                continue
                
            self.logger.info(f"Entering straddle for {underlying} at strike {atm_strike} (underlying price: {underlying_price})")
            
            # Store ATM price at entry for later reference
            self.entry_atm_prices[underlying] = underlying_price
            
            # Subscribe to ATM options
            ce_symbol = self._get_option_symbol(underlying, atm_strike, "CE")
            pe_symbol = self._get_option_symbol(underlying, atm_strike, "PE")
            
            # Request option subscription
            self.request_symbol(ce_symbol)
            self.request_symbol(pe_symbol)
            
            # Wait a bit for data to arrive
            time.sleep(1)
            
            # Get option prices
            ce_data = self.option_data.get(ce_symbol)
            pe_data = self.option_data.get(pe_symbol)
            
            if not ce_data or not pe_data:
                self.logger.warning(f"Option data not available for {underlying} ATM options")
                continue
                
            # Extract prices
            ce_price = ce_data.get('last') or ce_data.get('close') or ce_data.get('price')
            pe_price = pe_data.get('last') or pe_data.get('close') or pe_data.get('price')
            
            if not ce_price or not pe_price:
                self.logger.warning(f"Option prices not available for {underlying} ATM options")
                continue
            
            # Generate buy signals
            self.generate_signal(ce_symbol, SignalType.BUY, {
                'price': ce_price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'underlying': underlying,
                'type': 'CE'
            })
            
            self.generate_signal(pe_symbol, SignalType.BUY, {
                'price': pe_price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'underlying': underlying,
                'type': 'PE'
            })
            
            # Update position tracking
            self.ce_positions[underlying] = {
                'symbol': ce_symbol,
                'entry_price': ce_price,
                'current_price': ce_price,
                'high_price': ce_price,
                'stop_loss': ce_price * (1 - self.stop_loss_percent / 100),
                'target': ce_price * (1 + self.target_percent / 100)
            }
            
            self.pe_positions[underlying] = {
                'symbol': pe_symbol,
                'entry_price': pe_price,
                'current_price': pe_price,
                'high_price': pe_price,
                'stop_loss': pe_price * (1 - self.stop_loss_percent / 100),
                'target': pe_price * (1 + self.target_percent / 100)
            }
            
            self.logger.info(f"Entered {underlying} straddle - CE: {ce_symbol} @ {ce_price}, PE: {pe_symbol} @ {pe_price}")
        
        # Update state if we entered any positions
        if self.ce_positions or self.pe_positions:
            self.position_opened = True
            self.entry_pending = False
    
    def _get_option_symbol(self, underlying: str, strike: float, option_type: str) -> str:
        """Get option symbol based on underlying, strike and option type"""
        # This is a simplified example, in reality you would use your option symbol construction logic
        # or the option_manager to get the actual symbol
        from datetime import date
        from utils.expiry_utils import get_next_expiry
        
        expiry_date = get_next_expiry(underlying, offset=self.expiry_offset)
        expiry_str = expiry_date.strftime("%d%b%y").upper()
        
        return f"{underlying}{expiry_str}{strike}{option_type}"
    
    def _update_option_position(self, symbol: str, event: MarketDataEvent):
        """Update option position tracking and check for exit conditions"""
        price = event.data.get('last') or event.data.get('close') or event.data.get('price')
        if not price:
            return
            
        # Find the position this symbol belongs to
        position_info = None
        position_type = None
        underlying = None
        
        for idx, pos in self.ce_positions.items():
            if pos['symbol'] == symbol:
                position_info = pos
                position_type = 'CE'
                underlying = idx
                break
                
        if not position_info:
            for idx, pos in self.pe_positions.items():
                if pos['symbol'] == symbol:
                    position_info = pos
                    position_type = 'PE'
                    underlying = idx
                    break
        
        if not position_info or not underlying:
            return
            
        # Update current price
        position_info['current_price'] = price
        
        # Update high price for trailing stop
        if price > position_info['high_price']:
            position_info['high_price'] = price
            
            # Update trailing stop if enabled
            if self.trail_points > 0:
                new_stop = price * (1 - self.trail_points / 100)
                if new_stop > position_info['stop_loss']:
                    position_info['stop_loss'] = new_stop
                    self.logger.info(f"Updated trailing stop for {symbol} to {new_stop}")
        
        # Check stop loss
        if price <= position_info['stop_loss']:
            # Exit this leg
            self.generate_signal(symbol, SignalType.SELL, {
                'price': price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'reason': 'stop_loss',
                'underlying': underlying,
                'type': position_type
            })
            
            self.logger.info(f"Stop loss triggered for {symbol} at {price} (entry: {position_info['entry_price']})")
            
            # Remove from tracking
            if position_type == 'CE':
                del self.ce_positions[underlying]
            else:
                del self.pe_positions[underlying]
                
        # Check target
        elif price >= position_info['target']:
            # Exit this leg
            self.generate_signal(symbol, SignalType.SELL, {
                'price': price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'reason': 'target',
                'underlying': underlying,
                'type': position_type
            })
            
            self.logger.info(f"Target reached for {symbol} at {price} (entry: {position_info['entry_price']})")
            
            # Remove from tracking
            if position_type == 'CE':
                del self.ce_positions[underlying]
            else:
                del self.pe_positions[underlying]
    
    def _check_exit_conditions(self, underlying: str, current_price: float, timestamp: datetime):
        """Check underlying-based exit conditions"""
        # For ATM straddle, we might want to exit based on underlying movement
        # For now, this is just a placeholder
        pass
    
    def _exit_all_positions(self, reason: str):
        """Exit all open positions"""
        self.exit_triggered = True
        
        # Exit all CE positions
        for underlying, position in list(self.ce_positions.items()):
            symbol = position['symbol']
            price = position['current_price']
            
            self.generate_signal(symbol, SignalType.SELL, {
                'price': price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'reason': reason,
                'underlying': underlying,
                'type': 'CE'
            })
            
            self.logger.info(f"Exiting CE position for {underlying}: {symbol} at {price} - {reason}")
        
        # Exit all PE positions
        for underlying, position in list(self.pe_positions.items()):
            symbol = position['symbol']
            price = position['current_price']
            
            self.generate_signal(symbol, SignalType.SELL, {
                'price': price,
                'quantity': 1,
                'strategy': 'atm_straddle',
                'reason': reason,
                'underlying': underlying,
                'type': 'PE'
            })
            
            self.logger.info(f"Exiting PE position for {underlying}: {symbol} at {price} - {reason}")
        
        # Clear position tracking
        self.ce_positions.clear()
        self.pe_positions.clear()
    
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
        
        self.logger.info("ATM Straddle Strategy stopped")