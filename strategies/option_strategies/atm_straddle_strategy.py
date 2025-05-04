"""
ATM Straddle Option Strategy

This strategy buys both Call and Put options at the ATM strike price
and aims to profit from large price movements in either direction.
Relies on StrategyManager and OptionManager for option subscriptions.
"""

import logging
import time
from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set

import numpy as np

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent, FillEvent # Added FillEvent
from models.position import Position
from utils.constants import SignalType, MarketDataType 
from utils.time_utils import is_market_open, time_in_range
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument # Import Instrument

@StrategyRegistry.register('atm_straddle_strategy')
class AtmStraddleStrategy(OptionStrategy):
    """
    ATM Straddle Option Strategy

    Buys both CE and PE options at the ATM strike and profits from large price movements
    in either direction. Implements stop-loss and target-based exits.
    Relies on dynamic option subscription handled by StrategyManager/OptionManager.
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
        self.trail_percent = self.params.get('trail_percent', 0) # Use percentage for trailing SL
        self.quantity = config.get('execution', {}).get('quantity', 1) # Get quantity from config

        # Parse time strings to time objects
        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        # Parse underlyings (expects list of strings, e.g., ["NIFTY INDEX", "SENSEX"])
        # The StrategyManager will use this list to know which underlyings trigger option updates for this strategy.
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

        # Position tracking (using option symbol key, e.g., "NFO:NIFTY...")
        self.active_straddles: Dict[str, Dict[str, Any]] = {} # underlying_symbol -> {'ce': pos_dict, 'pe': pos_dict, 'atm': atm_strike}
                                                              # pos_dict = {symbol_key, entry_price, high_price, stop_loss, target}

        # State
        self.entry_triggered_today = False # Ensure entry happens only once per day per underlying
        self.exit_triggered_today = False # Ensure exit happens only once per day

        self.logger.info(f"ATM Straddle Strategy '{self.id}' initialized. Underlyings: {self.underlyings}, Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, SL: {self.stop_loss_percent}%, Target: {self.target_percent}%")

    def _parse_time(self, time_str: str) -> dt_time:
        """Parse time string HH:MM:SS to time object"""
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}', using default 09:30 or 15:15.")
            # Provide context-aware defaults
            if 'entry' in time_str.lower(): return dt_time(9, 30, 0)
            elif 'exit' in time_str.lower(): return dt_time(15, 15, 0)
            else: return dt_time(15, 15, 0) # Default exit

    def get_required_symbols(self) -> Dict[str, List[str]]:
        """
        Return the list of underlying symbols required by this strategy.
        StrategyManager uses this to subscribe to the underlyings initially.
        Option symbols are subscribed dynamically later.
        """
        symbols_by_timeframe = {}
        # Strategy needs underlyings on its primary timeframe and any additional ones
        for tf in self.all_timeframes:
             symbols_by_timeframe[tf] = list(self.underlyings) # Return list of underlying symbols
        self.logger.debug(f"Strategy '{self.id}' requires underlyings: {self.underlyings} on timeframes {self.all_timeframes}")
        return symbols_by_timeframe

    def on_start(self):
        """Called when the strategy is started by StrategyManager."""
        # Reset daily state
        self.entry_triggered_today = False
        self.exit_triggered_today = False
        self.active_straddles.clear()
        # Note: Initial underlying subscriptions are handled by StrategyManager based on get_required_symbols()
        # Dynamic option subscriptions are handled by StrategyManager reacting to OptionManager events.
        self.logger.info(f"ATM Straddle Strategy '{self.id}' started. Waiting for market data and entry time {self.entry_time_str}.")


    def on_market_data(self, event: MarketDataEvent):
        """Process tick data, primarily for updating option prices and checking SL/Target."""
        if not event.instrument or not event.data: return

        symbol_key = event.instrument.instrument_id # e.g., NFO:NIFTY... or NSE:NIFTY INDEX
        if not symbol_key: return

        # self.logger.debug(f"Received MarketDataEvent for {symbol_key}")

        # Check if it's an option we are holding
        underlying_holding = None
        position_type = None # 'ce' or 'pe'
        for u_sym, straddle_info in self.active_straddles.items():
            if straddle_info.get('ce') and straddle_info['ce']['symbol_key'] == symbol_key:
                underlying_holding = u_sym
                position_type = 'ce'
                break
            if straddle_info.get('pe') and straddle_info['pe']['symbol_key'] == symbol_key:
                underlying_holding = u_sym
                position_type = 'pe'
                break

        if underlying_holding and position_type:
            # It's an option we hold, update its price and check exits
            price = event.data.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                try:
                    current_price = float(price)
                    self._update_option_position(underlying_holding, position_type, current_price)
                except (ValueError, TypeError):
                     self.logger.warning(f"Invalid price format for {symbol_key}: {price}")
            # else:
                 # self.logger.debug(f"No last price in market data for {symbol_key}")

        # We don't need to process underlying ticks here, as OptionManager handles
        # underlying price updates and triggers ATM changes.


    def on_bar(self, event: BarEvent):
        """Process bar data, primarily for entry and time-based exit logic."""
        if not event.instrument: return

        symbol_key = event.instrument.instrument_id
        timeframe = event.timeframe

        # Only act on the primary timeframe defined for the strategy
        if timeframe != self.timeframe:
            return

        # self.logger.debug(f"Received BarEvent for {symbol_key}@{timeframe}")

        current_dt = datetime.fromtimestamp(event.timestamp) # Bar completion time
        current_time = current_dt.time()

        # --- Entry Logic ---
        # Check if it's time to enter and we haven't entered today
        if not self.entry_triggered_today and current_time >= self.entry_time:
            # Check if market is open (redundant if entry_time is within market hours, but safe)
            # if is_market_open(current_dt): # Assumes is_market_open checks date too
            self.logger.info(f"Entry time {self.entry_time_str} reached. Attempting to enter straddles.")
            self._try_enter_positions()
            self.entry_triggered_today = True # Mark entry attempt for today
            # else:
            #     self.logger.info(f"Entry time {self.entry_time_str} reached, but market is closed.")
            #     self.entry_triggered_today = True # Prevent re-attempting today


        # --- Time-Based Exit Logic ---
        # Check if it's time to exit and we haven't exited today
        if not self.exit_triggered_today and current_time >= self.exit_time:
             if self.active_straddles: # Check if we have any open positions
                 self.logger.info(f"Exit time {self.exit_time_str} reached. Exiting all open straddle positions.")
                 self._exit_all_positions("Time-based exit")
                 self.exit_triggered_today = True # Mark exit for today
             # else: # No positions open, just mark exit time passed
             #     self.exit_triggered_today = True


    def _try_enter_positions(self):
        """Attempts to enter ATM straddle positions for all configured underlyings."""
        if not self.option_manager:
             self.logger.error("OptionManager not available. Cannot enter positions.")
             return

        for underlying_symbol in self.underlyings:
            # Skip if already holding a position for this underlying
            if underlying_symbol in self.active_straddles:
                 self.logger.info(f"Already have an active straddle for {underlying_symbol}. Skipping entry.")
                 continue

            # 1. Get current ATM strike from OptionManager
            # Use the direct calculation based on latest price, not the potentially stale cached value
            current_price = self.option_manager.underlying_prices.get(underlying_symbol)
            strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)

            if current_price is None or strike_interval is None:
                self.logger.warning(f"Cannot determine ATM strike for {underlying_symbol}: Price or interval missing.")
                continue

            atm_strike = self.option_manager._get_atm_strike_internal(current_price, strike_interval)
            self.logger.info(f"Attempting entry for {underlying_symbol}. Current Price: {current_price:.2f}, ATM Strike: {atm_strike}")

            # 2. Get the CE and PE Instrument objects using OptionManager
            ce_instrument = self.option_manager.get_option_instrument(underlying_symbol, atm_strike, "CE", self.expiry_offset)
            pe_instrument = self.option_manager.get_option_instrument(underlying_symbol, atm_strike, "PE", self.expiry_offset)

            if not ce_instrument or not pe_instrument:
                self.logger.error(f"Could not get CE/PE instruments for {underlying_symbol} ATM strike {atm_strike}. Subscription might be pending or failed.")
                continue

            ce_symbol_key = ce_instrument.instrument_id
            pe_symbol_key = pe_instrument.instrument_id

            # 3. Get latest prices for these options from DataManager's cache
            ce_data = self.data_manager.get_latest_tick(ce_symbol_key) # Use DataManager cache
            pe_data = self.data_manager.get_latest_tick(pe_symbol_key) # Use DataManager cache

            if not ce_data or not pe_data:
                self.logger.warning(f"Market data not yet available for {ce_symbol_key} or {pe_symbol_key}. Retrying next cycle.")
                continue # Data might arrive shortly after subscription

            ce_price = ce_data.get(MarketDataType.LAST_PRICE.value)
            pe_price = pe_data.get(MarketDataType.LAST_PRICE.value)

            if ce_price is None or pe_price is None:
                self.logger.warning(f"Last price not found in data for {ce_symbol_key} or {pe_symbol_key}.")
                continue

            try:
                ce_price = float(ce_price)
                pe_price = float(pe_price)
            except (ValueError, TypeError):
                 self.logger.error(f"Invalid price format for CE ({ce_price}) or PE ({pe_price}).")
                 continue

            # 4. Generate BUY signals
            common_signal_data = {
                'quantity': self.quantity,
                'strategy': self.id,
                'underlying': underlying_symbol,
                'atm_strike': atm_strike
            }

            self.generate_signal(ce_instrument.symbol, SignalType.BUY, data={**common_signal_data, 'type': 'CE', 'price': ce_price})
            self.generate_signal(pe_instrument.symbol, SignalType.BUY, data={**common_signal_data, 'type': 'PE', 'price': pe_price})

            # 5. Initialize position tracking immediately (don't wait for fill)
            #    We assume the order will likely fill at or near the signal price for MARKET orders.
            #    Stop loss/target calculations are based on this assumed entry.
            self.active_straddles[underlying_symbol] = {
                 'atm': atm_strike,
                 'ce': {
                     'symbol_key': ce_symbol_key,
                     'symbol': ce_instrument.symbol, # Store trading symbol too
                     'entry_price': ce_price,
                     'current_price': ce_price,
                     'high_price': ce_price, # Initialize high price
                     'stop_loss': ce_price * (1 - self.stop_loss_percent / 100),
                     'target': ce_price * (1 + self.target_percent / 100),
                     'exited': False
                 },
                 'pe': {
                     'symbol_key': pe_symbol_key,
                     'symbol': pe_instrument.symbol,
                     'entry_price': pe_price,
                     'current_price': pe_price,
                     'high_price': pe_price, # Initialize high price
                     'stop_loss': pe_price * (1 - self.stop_loss_percent / 100),
                     'target': pe_price * (1 + self.target_percent / 100),
                     'exited': False
                 }
            }
            self.logger.info(f"BUY signals generated for {underlying_symbol} straddle. CE: {ce_instrument.symbol} @ ~{ce_price:.2f}, PE: {pe_instrument.symbol} @ ~{pe_price:.2f}")


    def _update_option_position(self, underlying_symbol: str, position_type: str, current_price: float):
        """Update option position tracking (price, high, SL) and check for individual leg exits."""
        if underlying_symbol not in self.active_straddles or position_type not in self.active_straddles[underlying_symbol]:
            # self.logger.warning(f"Attempted to update non-existent position: {underlying_symbol} {position_type}")
            return

        position_info = self.active_straddles[underlying_symbol][position_type]

        # Ignore if already exited
        if position_info['exited']:
            return

        # Update current price
        position_info['current_price'] = current_price
        symbol = position_info['symbol'] # Trading symbol
        entry_price = position_info['entry_price']

        # Update high price for trailing stop calculation
        position_info['high_price'] = max(position_info['high_price'], current_price)

        # Update trailing stop loss if applicable
        if self.trail_percent > 0:
            # Calculate potential new stop loss based on high price
            potential_new_stop = position_info['high_price'] * (1 - self.trail_percent / 100)
            # Only trail upwards
            if potential_new_stop > position_info['stop_loss']:
                old_sl = position_info['stop_loss']
                position_info['stop_loss'] = potential_new_stop
                # self.logger.info(f"Trailing SL updated for {symbol}: {old_sl:.2f} -> {potential_new_stop:.2f} (High: {position_info['high_price']:.2f})")

        # --- Check Exit Conditions for this leg ---
        exit_reason = None
        if current_price <= position_info['stop_loss']:
            exit_reason = "stop_loss"
        elif current_price >= position_info['target']:
            exit_reason = "target"

        if exit_reason:
            self.logger.info(f"{exit_reason.replace('_',' ').title()} triggered for {symbol} at {current_price:.2f} (Entry: {entry_price:.2f}, SL: {position_info['stop_loss']:.2f}, Target: {position_info['target']:.2f})")
            # Generate SELL signal for this leg only
            self.generate_signal(symbol, SignalType.SELL, data={
                'price': current_price, # Use current price for market order exit
                'quantity': self.quantity,
                'strategy': self.id,
                'reason': exit_reason,
                'underlying': underlying_symbol,
                'type': position_type.upper(),
                'entry_price': entry_price # Include entry for PnL calculation later
            })
            # Mark this leg as exited
            position_info['exited'] = True

            # Check if both legs have exited, if so, remove the straddle entry
            other_leg_type = 'pe' if position_type == 'ce' else 'ce'
            if self.active_straddles[underlying_symbol][other_leg_type]['exited']:
                self.logger.info(f"Both legs exited for {underlying_symbol} straddle. Removing from active tracking.")
                del self.active_straddles[underlying_symbol]


    def _exit_all_positions(self, reason: str):
        """Exit all currently open legs of all active straddles."""
        self.logger.info(f"Exiting all active positions. Reason: {reason}")
        straddles_to_remove = list(self.active_straddles.keys()) # Avoid modifying dict while iterating

        for underlying_symbol in straddles_to_remove:
            straddle_info = self.active_straddles.get(underlying_symbol)
            if not straddle_info: continue # Should not happen

            for leg_type in ['ce', 'pe']:
                position_info = straddle_info.get(leg_type)
                if position_info and not position_info['exited']:
                    symbol = position_info['symbol']
                    current_price = position_info['current_price'] # Use last known price
                    entry_price = position_info['entry_price']

                    self.logger.info(f"Generating SELL signal for {symbol} at ~{current_price:.2f}. Reason: {reason}")
                    self.generate_signal(symbol, SignalType.SELL, data={
                        'price': current_price,
                        'quantity': self.quantity,
                        'strategy': self.id,
                        'reason': reason,
                        'underlying': underlying_symbol,
                        'type': leg_type.upper(),
                        'entry_price': entry_price
                    })
                    # Mark as exited immediately
                    position_info['exited'] = True

            # Remove the straddle entry after processing both legs
            if underlying_symbol in self.active_straddles:
                 del self.active_straddles[underlying_symbol]

        self.logger.info("Finished generating exit signals for all active positions.")


    def on_fill(self, event: FillEvent):
        """Handle fill events to confirm trades and potentially update entry prices."""
        # Optional: Update entry price based on actual fill price if needed
        # For now, just log the fill.
        if hasattr(event, 'symbol') and hasattr(event, 'fill_price') and hasattr(event, 'quantity'):
             action = "Bought" if event.quantity > 0 else "Sold"
             self.logger.info(f"Fill received: {action} {abs(event.quantity)} of {event.symbol} @ {event.fill_price:.2f} (Order ID: {event.order_id})")

             # Update performance tracking (basic example)
             # More complex logic needed if tracking PnL per trade
             # if action == "Sold":
             #      # Find corresponding entry - requires linking fills to entries
             #      pass


    def on_stop(self):
        """Called when the strategy is stopped by StrategyManager."""
        # Ensure all positions are exited cleanly
        if self.active_straddles:
            self.logger.warning(f"Strategy '{self.id}' stopping with active positions. Forcing exit.")
            self._exit_all_positions("Strategy stopped")

        self.logger.info(f"ATM Straddle Strategy '{self.id}' stopped.")


