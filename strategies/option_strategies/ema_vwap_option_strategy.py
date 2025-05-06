"""
EMA VWAP Option Strategy

This strategy buys Call or Put options based on EMA crossovers and VWAP conditions.
- For CE: Buy when price is above VWAP and EMA_5 crosses EMA_21 from below, after a 60% retracement to EMA_21
- For PE: Buy when price is below VWAP and EMA_5 crosses EMA_21 from above, after a 60% retracement to EMA_21
"""

import logging
import time
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set, Tuple
import numpy as np
import pandas as pd

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent, FillEvent
from models.position import Position
from utils.constants import SignalType, MarketDataType
from utils.time_utils import is_market_open, time_in_range
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument

@StrategyRegistry.register('ema_vwap_option_strategy')
class EmaVwapOptionStrategy(OptionStrategy):
    """
    EMA VWAP Option Strategy

    Buys CE options when price is above VWAP and EMA_5 crosses above EMA_21,
    after a 60% retracement to EMA_21.
    
    Buys PE options when price is below VWAP and EMA_5 crosses below EMA_21,
    after a 60% retracement to EMA_21.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager):
        super().__init__(strategy_id, config, data_manager,
                        option_manager, portfolio_manager, event_manager)

        # Extract strategy-specific parameters
        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:30:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.stop_loss_percent = self.params.get('stop_loss_percent', 40)
        self.target_percent = self.params.get('target_percent', 50)
        self.trail_percent = self.params.get('trail_percent', 0)
        self.quantity = config.get('execution', {}).get('quantity', 1)
        
        # EMA parameters
        self.ema_short_period = self.params.get('ema_short_period', 5)
        self.ema_long_period = self.params.get('ema_long_period', 21)
        self.retracement_threshold = self.params.get('retracement_threshold', 0.6)  # 60% retracement

        # Parse time strings to time objects
        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        # Parse underlyings
        raw = config.get("underlyings", [])
        self.underlyings = []
        for u in raw:
            try:
                self.underlyings.append(u["name"])
            except (TypeError, KeyError):
                self.logger.error(
                    f"Strategy {strategy_id}: invalid underlying entry {u!r}; expected dict with 'name'."
                )

        self.expiry_offset = config.get('expiry_offset', 0)

        # Data storage for indicators
        self.data_store = {}  # underlying_symbol -> {'1m': DataFrame, '5m': DataFrame}
        
        # Signals tracking
        self.crossover_signals = {}  # underlying_symbol -> {'ce_signal': bool, 'pe_signal': bool}
        self.retracement_signals = {}  # underlying_symbol -> {'ce_signal': bool, 'pe_signal': bool}
        
        # Position tracking
        self.active_positions = {}  # symbol_key -> position_info

        # State
        self.entry_enabled = False  # Will be set to True when entry time is reached

        self.logger.info(f"EMA VWAP Option Strategy '{self.id}' initialized. Underlyings: {self.underlyings}, "
                         f"Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, Target: {self.target_percent}%")

    def _parse_time(self, time_str: str) -> dt_time:
        """Parse time string HH:MM:SS to time object"""
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}', using default 09:30 or 15:15.")
            # Provide context-aware defaults
            if 'entry' in time_str.lower(): return dt_time(9, 30, 0)
            elif 'exit' in time_str.lower(): return dt_time(15, 15, 0)
            else: return dt_time(15, 15, 0)  # Default exit

    def get_required_symbols(self) -> Dict[str, List[str]]:
        """
        Return the list of underlying symbols required by this strategy.
        Strategy needs both 1m and 5m timeframes.
        """
        symbols_by_timeframe = {}
        # We need both 1m and 5m timeframes for the strategy
        symbols_by_timeframe["1m"] = list(self.underlyings)
        symbols_by_timeframe["5m"] = list(self.underlyings)
        
        self.logger.debug(f"Strategy '{self.id}' requires underlyings: {self.underlyings} on timeframes 1m, 5m")
        return symbols_by_timeframe

    def on_start(self):
        """Called when the strategy is started by StrategyManager."""
        # Initialize data storage
        for underlying in self.underlyings:
            self.data_store[underlying] = {
                '1m': pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']),
                '5m': pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            }
            self.crossover_signals[underlying] = {'ce_signal': False, 'pe_signal': False}
            self.retracement_signals[underlying] = {'ce_signal': False, 'pe_signal': False}
        
        # Reset entry enabled flag
        self.entry_enabled = False
        self.active_positions.clear()
        
        self.logger.info(f"EMA VWAP Option Strategy '{self.id}' started. Waiting for market data and entry time {self.entry_time_str}.")

    def on_market_data(self, event: MarketDataEvent):
        """Process tick data for options, primarily for checking SL/Target."""
        if not event.instrument or not event.data:
            return

        symbol_key = event.instrument.instrument_id
        if not symbol_key:
            return

        # Check if it's an option we are holding
        if symbol_key in self.active_positions:
            # Update price and check exits
            price = event.data.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                try:
                    current_price = float(price)
                    self._update_position(symbol_key, current_price)
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid price format for {symbol_key}: {price}")

    def on_bar(self, event: BarEvent):
        """
        Process bar data for indicators calculation and entry/exit logic.
        This is the main method where the strategy logic is implemented.
        """
        if not event.instrument:
            return

        symbol_key = event.instrument.instrument_id
        timeframe = event.timeframe
        
        # Check if it's an underlying we're tracking
        if symbol_key not in self.underlyings:
            return
            
        # Process bar data for 1m and 5m timeframes
        if timeframe in ['1m', '5m']:
            self._update_data_store(symbol_key, timeframe, event)
            self._calculate_indicators(symbol_key, timeframe)
            
            # Check the time for entry/exit
            current_dt = datetime.fromtimestamp(event.timestamp)
            current_time = current_dt.time()
            
            # Enable entry after entry time is reached
            if not self.entry_enabled and current_time >= self.entry_time:
                self.entry_enabled = True
                self.logger.info(f"Entry time {self.entry_time_str} reached. Strategy is now active.")
            
            # Check for exit time
            if current_time >= self.exit_time:
                if self.active_positions:
                    self.logger.info(f"Exit time {self.exit_time_str} reached. Exiting all open positions.")
                    self._exit_all_positions("Time-based exit")
            
            # If entry is enabled, check for signals
            if self.entry_enabled:
                if timeframe == '5m':
                    # Check for crossover signals on 5m timeframe
                    self._check_crossover_signals(symbol_key)
                elif timeframe == '1m':
                    # Check for retracement and entry on 1m timeframe
                    self._check_retracement_and_enter(symbol_key)

    def _update_data_store(self, symbol: str, timeframe: str, event: BarEvent):
        """Update the data store with the latest bar data"""
        if symbol not in self.data_store or timeframe not in self.data_store[symbol]:
            return

        # Extract OHLCV data from the bar event
        bar_data = {
            'timestamp': event.timestamp,
            'open': event.open,
            'high': event.high,
            'low': event.low,
            'close': event.close,
            'volume': event.volume
        }
        
        # Append to dataframe
        new_row = pd.DataFrame([bar_data])
        self.data_store[symbol][timeframe] = pd.concat([self.data_store[symbol][timeframe], new_row], ignore_index=True)
        
        # Keep only the last 100 bars for efficiency
        if len(self.data_store[symbol][timeframe]) > 100:
            self.data_store[symbol][timeframe] = self.data_store[symbol][timeframe].iloc[-100:]

    def _calculate_indicators(self, symbol: str, timeframe: str):
        """Calculate EMA and VWAP indicators for the given symbol and timeframe"""
        df = self.data_store[symbol][timeframe]
        if len(df) < self.ema_long_period:
            return
        
        # Calculate EMAs
        df['ema_short'] = self._calculate_ema(df['close'], self.ema_short_period)
        df['ema_long'] = self._calculate_ema(df['close'], self.ema_long_period)
        
        # Calculate VWAP
        df['vwap'] = self._calculate_vwap(df)
        
        # Update the dataframe in the data store
        self.data_store[symbol][timeframe] = df

    def _calculate_ema(self, series, period):
        """Calculate Exponential Moving Average"""
        return series.ewm(span=period, adjust=False).mean()

    def _calculate_vwap(self, df):
        """Calculate Volume Weighted Average Price"""
        df = df.copy()
        df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
        df['price_volume'] = df['typical_price'] * df['volume']
        df['cumulative_price_volume'] = df['price_volume'].cumsum()
        df['cumulative_volume'] = df['volume'].cumsum()
        vwap = df['cumulative_price_volume'] / df['cumulative_volume']
        return vwap

    def _check_crossover_signals(self, symbol: str):
        """Check for EMA crossover and VWAP conditions on 5m timeframe"""
        df = self.data_store[symbol]['5m']
        if len(df) < 2:  # Need at least 2 bars to check for crossover
            return
        
        # Get last two bars for crossover check
        prev_bar = df.iloc[-2]
        curr_bar = df.iloc[-1]
        
        # Check CE signal conditions: EMA5 crosses above EMA21 and price above VWAP
        if (prev_bar['ema_short'] <= prev_bar['ema_long'] and 
            curr_bar['ema_short'] > curr_bar['ema_long'] and 
            curr_bar['close'] > curr_bar['vwap']):
            
            if not self.crossover_signals[symbol]['ce_signal']:  # Only log new signals
                self.logger.info(f"5m CE signal triggered for {symbol}: EMA5 crossed above EMA21 and price above VWAP")
            self.crossover_signals[symbol]['ce_signal'] = True
        
        # Check PE signal conditions: EMA5 crosses below EMA21 and price below VWAP
        if (prev_bar['ema_short'] >= prev_bar['ema_long'] and 
            curr_bar['ema_short'] < curr_bar['ema_long'] and 
            curr_bar['close'] < curr_bar['vwap']):
            
            if not self.crossover_signals[symbol]['pe_signal']:  # Only log new signals
                self.logger.info(f"5m PE signal triggered for {symbol}: EMA5 crossed below EMA21 and price below VWAP")
            self.crossover_signals[symbol]['pe_signal'] = True

    def _check_retracement_and_enter(self, symbol: str):
        """Check for 60% retracement to EMA21 on 1m timeframe and enter positions if conditions met"""
        # Skip if no crossover signals are active
        if not self.crossover_signals[symbol]['ce_signal'] and not self.crossover_signals[symbol]['pe_signal']:
            return
            
        df_1m = self.data_store[symbol]['1m']
        if len(df_1m) < 2:
            return
            
        curr_bar = df_1m.iloc[-1]
        
        # Check for CE retracement (if CE signal is active)
        if self.crossover_signals[symbol]['ce_signal'] and not self.retracement_signals[symbol]['ce_signal']:
            # For CE, we need price to retrace down toward EMA21 by at least 60%
            # First, find the high point after the crossover
            crossover_index = self._find_ce_crossover_point(symbol)
            if crossover_index is not None:
                high_after_crossover = df_1m['high'].iloc[crossover_index:].max()
                ema21_value = curr_bar['ema_long']
                
                # Calculate retracement percentage
                if high_after_crossover > ema21_value:  # Ensure there's room for retracement
                    retracement_distance = high_after_crossover - curr_bar['close']
                    full_distance = high_after_crossover - ema21_value
                    
                    if full_distance > 0:  # Avoid division by zero
                        retracement_pct = retracement_distance / full_distance
                        
                        if retracement_pct >= self.retracement_threshold:
                            self.logger.info(f"1m CE retracement signal triggered for {symbol}: "
                                            f"Price retraced {retracement_pct:.2%} back to EMA21")
                            self.retracement_signals[symbol]['ce_signal'] = True
                            self._enter_ce_position(symbol)
        
        # Check for PE retracement (if PE signal is active)
        if self.crossover_signals[symbol]['pe_signal'] and not self.retracement_signals[symbol]['pe_signal']:
            # For PE, we need price to retrace up toward EMA21 by at least 60%
            crossover_index = self._find_pe_crossover_point(symbol)
            if crossover_index is not None:
                low_after_crossover = df_1m['low'].iloc[crossover_index:].min()
                ema21_value = curr_bar['ema_long']
                
                # Calculate retracement percentage
                if low_after_crossover < ema21_value:  # Ensure there's room for retracement
                    retracement_distance = curr_bar['close'] - low_after_crossover
                    full_distance = ema21_value - low_after_crossover
                    
                    if full_distance > 0:  # Avoid division by zero
                        retracement_pct = retracement_distance / full_distance
                        
                        if retracement_pct >= self.retracement_threshold:
                            self.logger.info(f"1m PE retracement signal triggered for {symbol}: "
                                            f"Price retraced {retracement_pct:.2%} back to EMA21")
                            self.retracement_signals[symbol]['pe_signal'] = True
                            self._enter_pe_position(symbol)

    def _find_ce_crossover_point(self, symbol: str) -> Optional[int]:
        """Find the index where EMA5 crossed above EMA21 in the 5m data"""
        df = self.data_store[symbol]['5m']
        for i in range(len(df) - 1, 0, -1):
            if df['ema_short'].iloc[i] > df['ema_long'].iloc[i] and df['ema_short'].iloc[i-1] <= df['ema_long'].iloc[i-1]:
                return i
        return None

    def _find_pe_crossover_point(self, symbol: str) -> Optional[int]:
        """Find the index where EMA5 crossed below EMA21 in the 5m data"""
        df = self.data_store[symbol]['5m']
        for i in range(len(df) - 1, 0, -1):
            if df['ema_short'].iloc[i] < df['ema_long'].iloc[i] and df['ema_short'].iloc[i-1] >= df['ema_long'].iloc[i-1]:
                return i
        return None

    def _enter_ce_position(self, underlying_symbol: str):
        """Enter a CE (Call) position"""
        if not self.option_manager:
            self.logger.error("OptionManager not available. Cannot enter CE position.")
            return
            
        # Get current ATM strike
        current_price = self.option_manager.underlying_prices.get(underlying_symbol)
        strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)
        
        if current_price is None or strike_interval is None:
            self.logger.warning(f"Cannot determine ATM strike for {underlying_symbol}: Price or interval missing.")
            return
            
        atm_strike = self.option_manager._get_atm_strike_internal(current_price, strike_interval)
        self.logger.info(f"Entering CE position for {underlying_symbol}. Current Price: {current_price:.2f}, ATM Strike: {atm_strike}")
        
        # Get the CE Instrument
        ce_instrument = self.option_manager.get_option_instrument(underlying_symbol, atm_strike, "CE", self.expiry_offset)
        
        if not ce_instrument:
            self.logger.error(f"Could not get CE instrument for {underlying_symbol} ATM strike {atm_strike}.")
            return
            
        ce_symbol_key = ce_instrument.instrument_id
        
        # Get latest price for this option
        ce_data = self.data_manager.get_latest_tick(ce_symbol_key)
        
        if not ce_data:
            self.logger.warning(f"Market data not yet available for {ce_symbol_key}. Skipping entry.")
            return
            
        ce_price = ce_data.get(MarketDataType.LAST_PRICE.value)
        
        if ce_price is None:
            self.logger.warning(f"Last price not found in data for {ce_symbol_key}.")
            return
            
        try:
            ce_price = float(ce_price)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid price format for CE ({ce_price}).")
            return
            
        # Generate BUY signal
        signal_data = {
            'quantity': self.quantity,
            'strategy': self.id,
            'underlying': underlying_symbol,
            'atm_strike': atm_strike,
            'type': 'CE',
            'price': ce_price
        }
        
        self.generate_signal(ce_instrument.symbol, SignalType.BUY, data=signal_data)
        
        # Initialize position tracking
        self.active_positions[ce_symbol_key] = {
            'symbol_key': ce_symbol_key,
            'symbol': ce_instrument.symbol,
            'underlying': underlying_symbol,
            'type': 'CE',
            'entry_price': ce_price,
            'current_price': ce_price,
            'high_price': ce_price,
            'stop_loss': ce_price * (1 - self.stop_loss_percent / 100),
            'target': ce_price * (1 + self.target_percent / 100),
            'exited': False
        }
        
        self.logger.info(f"BUY signal generated for CE: {ce_instrument.symbol} @ ~{ce_price:.2f}")
        
        # Reset signals for this underlying
        self.crossover_signals[underlying_symbol]['ce_signal'] = False
        self.retracement_signals[underlying_symbol]['ce_signal'] = False

    def _enter_pe_position(self, underlying_symbol: str):
        """Enter a PE (Put) position"""
        if not self.option_manager:
            self.logger.error("OptionManager not available. Cannot enter PE position.")
            return
            
        # Get current ATM strike
        current_price = self.option_manager.underlying_prices.get(underlying_symbol)
        strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)
        
        if current_price is None or strike_interval is None:
            self.logger.warning(f"Cannot determine ATM strike for {underlying_symbol}: Price or interval missing.")
            return
            
        atm_strike = self.option_manager._get_atm_strike_internal(current_price, strike_interval)
        self.logger.info(f"Entering PE position for {underlying_symbol}. Current Price: {current_price:.2f}, ATM Strike: {atm_strike}")
        
        # Get the PE Instrument
        pe_instrument = self.option_manager.get_option_instrument(underlying_symbol, atm_strike, "PE", self.expiry_offset)
        
        if not pe_instrument:
            self.logger.error(f"Could not get PE instrument for {underlying_symbol} ATM strike {atm_strike}.")
            return
            
        pe_symbol_key = pe_instrument.instrument_id
        
        # Get latest price for this option
        pe_data = self.data_manager.get_latest_tick(pe_symbol_key)
        
        if not pe_data:
            self.logger.warning(f"Market data not yet available for {pe_symbol_key}. Skipping entry.")
            return
            
        pe_price = pe_data.get(MarketDataType.LAST_PRICE.value)
        
        if pe_price is None:
            self.logger.warning(f"Last price not found in data for {pe_symbol_key}.")
            return
            
        try:
            pe_price = float(pe_price)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid price format for PE ({pe_price}).")
            return
            
        # Generate BUY signal
        signal_data = {
            'quantity': self.quantity,
            'strategy': self.id,
            'underlying': underlying_symbol,
            'atm_strike': atm_strike,
            'type': 'PE',
            'price': pe_price
        }
        
        self.generate_signal(pe_instrument.symbol, SignalType.BUY, data=signal_data)
        
        # Initialize position tracking
        self.active_positions[pe_symbol_key] = {
            'symbol_key': pe_symbol_key,
            'symbol': pe_instrument.symbol,
            'underlying': underlying_symbol,
            'type': 'PE',
            'entry_price': pe_price,
            'current_price': pe_price,
            'high_price': pe_price,
            'stop_loss': pe_price * (1 - self.stop_loss_percent / 100),
            'target': pe_price * (1 + self.target_percent / 100),
            'exited': False
        }
        
        self.logger.info(f"BUY signal generated for PE: {pe_instrument.symbol} @ ~{pe_price:.2f}")
        
        # Reset signals for this underlying
        self.crossover_signals[underlying_symbol]['pe_signal'] = False
        self.retracement_signals[underlying_symbol]['pe_signal'] = False

    def _update_position(self, symbol_key: str, current_price: float):
        """Update position tracking and check for individual exits (SL/Target)."""
        if symbol_key not in self.active_positions:
            return
            
        position_info = self.active_positions[symbol_key]
        
        # Ignore if already exited
        if position_info['exited']:
            return
            
        # Update current price
        position_info['current_price'] = current_price
        symbol = position_info['symbol']
        entry_price = position_info['entry_price']
        
        # Update high price for trailing stop calculation
        position_info['high_price'] = max(position_info['high_price'], current_price)
        
        # Update trailing stop loss if applicable
        if self.trail_percent > 0:
            potential_new_stop = position_info['high_price'] * (1 - self.trail_percent / 100)
            if potential_new_stop > position_info['stop_loss']:
                position_info['stop_loss'] = potential_new_stop
        
        # Check Exit Conditions
        exit_reason = None
        if current_price <= position_info['stop_loss']:
            exit_reason = "stop_loss"
        elif current_price >= position_info['target']:
            exit_reason = "target"
            
        if exit_reason:
            self.logger.info(f"{exit_reason.replace('_',' ').title()} triggered for {symbol} at {current_price:.2f} "
                           f"(Entry: {entry_price:.2f}, SL: {position_info['stop_loss']:.2f}, Target: {position_info['target']:.2f})")
            
            # Generate SELL signal
            self.generate_signal(symbol, SignalType.SELL, data={
                'price': current_price,
                'quantity': self.quantity,
                'strategy': self.id,
                'reason': exit_reason,
                'underlying': position_info['underlying'],
                'type': position_info['type'],
                'entry_price': entry_price
            })
            
            # Mark as exited
            position_info['exited'] = True
            
            # Remove from active positions
            del self.active_positions[symbol_key]

    def _exit_all_positions(self, reason: str):
        """Exit all currently open positions."""
        self.logger.info(f"Exiting all active positions. Reason: {reason}")
        positions_to_exit = list(self.active_positions.keys())
        
        for symbol_key in positions_to_exit:
            position_info = self.active_positions.get(symbol_key)
            if not position_info or position_info['exited']:
                continue
                
            symbol = position_info['symbol']
            current_price = position_info['current_price']
            entry_price = position_info['entry_price']
            
            self.logger.info(f"Generating SELL signal for {symbol} at ~{current_price:.2f}. Reason: {reason}")
            self.generate_signal(symbol, SignalType.SELL, data={
                'price': current_price,
                'quantity': self.quantity,
                'strategy': self.id,
                'reason': reason,
                'underlying': position_info['underlying'],
                'type': position_info['type'],
                'entry_price': entry_price
            })
            
            # Mark as exited
            position_info['exited'] = True
            
        # Clear all positions
        self.active_positions.clear()
        self.logger.info("Finished generating exit signals for all active positions.")

    def on_fill(self, event: FillEvent):
        """Handle fill events to confirm trades."""
        if hasattr(event, 'symbol') and hasattr(event, 'fill_price') and hasattr(event, 'quantity'):
            action = "Bought" if event.quantity > 0 else "Sold"
            self.logger.info(f"Fill received: {action} {abs(event.quantity)} of {event.symbol} @ {event.fill_price:.2f} (Order ID: {event.order_id})")

    def on_stop(self):
        """Called when the strategy is stopped by StrategyManager."""
        # Ensure all positions are exited
        if self.active_positions:
            self.logger.warning(f"Strategy '{self.id}' stopping with active positions. Forcing exit.")
            self._exit_all_positions("Strategy stopped")
            
        self.logger.info(f"EMA VWAP Option Strategy '{self.id}' stopped.")
