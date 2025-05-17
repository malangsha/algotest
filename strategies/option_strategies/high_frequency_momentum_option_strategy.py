"""
High-Frequency Momentum Option Strategy

This strategy aims for a high hit-rate (>= 90%) by identifying strong intraday momentum
in NIFTY and SENSEX options. It buys Call or Put options based on EMA crossovers,
RSI confirmation, and price breakouts.

- Bar Subscription: Requires 1-minute (1m) bar data for the underlying indices (NIFTY, SENSEX).

- Entry Logic (Call):
  1. Fast EMA (e.g., 5-period) crosses above Slow EMA (e.g., 20-period) on 1m chart.
  2. RSI (e.g., 14-period) is above 50 and below 70 (confirming bullish momentum, not overbought).
  3. Price breaks above the high of the last N (e.g., 3) bars.
  4. Enter ATM Call option.

- Entry Logic (Put):
  1. Fast EMA (e.g., 5-period) crosses below Slow EMA (e.g., 20-period) on 1m chart.
  2. RSI (e.g., 14-period) is below 50 and above 30 (confirming bearish momentum, not oversold).
  3. Price breaks below the low of the last N (e.g., 3) bars.
  4. Enter ATM Put option.

- Risk Management:
  - Uses the framework's RiskManagement class.
  - Stop-loss: Defined percentage (e.g., 5-10%) of option premium.
  - Target Profit: Defined percentage (e.g., 10-20%) of option premium.
  - Position Sizing: Handled by RiskManagement class based on configuration.

- Exit Logic:
  1. Target profit hit.
  2. Stop-loss hit.
  3. EMA crossover in the opposite direction.
  4. End of trading day (e.g., 15:15).
"""

from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set 
import pandas as pd
import numpy as np

from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, SignalEvent, FillEvent
from models.instrument import Instrument, InstrumentType, AssetClass
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide
from strategies.strategy_registry import StrategyRegistry
from core.logging_manager import get_logger

@StrategyRegistry.register('high_frequency_momentum_option_strategy')
class HighFrequencyMomentumOptionStrategy(OptionStrategy):
    """
    High-Frequency Momentum Option Strategy for NIFTY and SENSEX.
    Subscribes to 1-minute bar data for underlyings.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager, broker=None, strategy_manager=None):
        # Pass broker to super if it's now part of OptionStrategy's __init__ signature
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager, broker, strategy_manager)

        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:20:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.stop_loss_percent = self.params.get('stop_loss_percent', 5)
        self.target_percent = self.params.get('target_percent', 10)
        self.quantity = config.get('execution', {}).get('quantity', 1)

        self.fast_ema_period = self.params.get('fast_ema_period', 5)
        self.slow_ema_period = self.params.get('slow_ema_period', 20)
        self.rsi_period = self.params.get('rsi_period', 14)
        self.rsi_upper_band = self.params.get('rsi_upper_band', 70)
        self.rsi_lower_band = self.params.get('rsi_lower_band', 30)
        self.rsi_mid_point = 50
        self.breakout_bars = self.params.get('breakout_bars', 3)

        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        # Process underlying configurations to store instrument_ids and details
        # This strategy expects 'underlyings' key in its own config section in strategies.yaml
        self.strategy_specific_underlyings_config = config.get("underlyings", [])
        self.underlying_instrument_ids: List[str] = [] # Stores instrument_id like "NSE:NIFTY INDEX"
        self.underlying_details: Dict[str, Dict[str, Any]] = {} # Store details by instrument_id

        for u_conf in self.strategy_specific_underlyings_config:
            name = u_conf.get("name") # e.g., "NIFTY INDEX"
            exchange_str = u_conf.get("spot_exchange") # e.g., "NSE"
            
            # Infer instrument_type and asset_class or use defaults if not provided
            # For indices, these should be explicitly set in config or inferred robustly
            inst_type_str = u_conf.get("instrument_type", "IND" if u_conf.get("index") is True else "EQ")
            asset_cls_str = u_conf.get("asset_class", "INDEX" if u_conf.get("index") is True else "EQUITY")


            if not name or not exchange_str:
                self.logger.warning(f"Strategy {self.id}: Skipping underlying config due to missing name or exchange: {u_conf}")
                continue
            
            instrument_id = f"{exchange_str.upper()}:{name}" # Construct the key
            self.underlying_instrument_ids.append(instrument_id)
            self.underlying_details[instrument_id] = {
                "name": name, # Original name from config
                "exchange": exchange_str.upper(),
                "instrument_type": inst_type_str.upper(), # Store as string
                "asset_class": asset_cls_str.upper(),   # Store as string
                "option_exchange": u_conf.get("option_exchange", "NFO").upper() # Default to NFO
            }

        self.expiry_offset = config.get('expiry_offset', 0) # For option selection

        # Data store for each underlying's 1m bar data and indicators
        self.data_store: Dict[str, Dict[str, pd.DataFrame]] = {}  # instrument_id -> {'1m': DataFrame}
        
        # Tracks active option positions taken by this strategy
        # Key: option_instrument_id (e.g., "NFO:NIFTY23JUL20000CE")
        # Value: Dict with position info (entry_price, sl, tp, underlying_instrument_id, etc.)
        self.active_positions: Dict[str, Dict[str, Any]] = {}
        
        self.entry_enabled = False # Controls if strategy can take new entries
        
        # Cooldown mechanism per underlying to prevent rapid re-entry after an exit
        self.trade_cooldown: Dict[str, float] = {} # underlying_instrument_id -> timestamp of last exit
        self.cooldown_period_seconds = self.params.get('cooldown_period_seconds', 300) # Default 5 minutes

        self.logger.info(f"HighFrequencyMomentumOptionStrategy '{self.id}' initialized. Underlyings (IDs): {self.underlying_instrument_ids}, "
                         f"Entry Time: {self.entry_time_str}, Exit Time: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, Target: {self.target_percent}%")

    def _parse_time(self, time_str: str) -> dt_time:
        """Helper to parse time strings from config."""
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Strategy {self.id}: Invalid time format '{time_str}'. Using default.")
            # Provide sensible defaults based on typical usage
            return dt_time(9, 20, 0) if 'entry' in time_str.lower() else dt_time(15, 15, 0)

    def initialize(self):
        """
        Called by StrategyManager after __init__ and event registration.
        Any specific one-time setup for the strategy can go here.
        """
        super().initialize() # Call base class initialize if it has any logic
        self.logger.info(f"Strategy '{self.id}' custom initialization complete.")
        # Initial symbol requests are handled in on_start for this strategy.

    def on_start(self):
        """
        Called by StrategyManager when the strategy is started.
        This is where initial symbol subscriptions for underlyings are made.
        """
        super().on_start() # Call base class on_start
        
        self.logger.info(f"Strategy '{self.id}' on_start: Requesting subscriptions for configured underlyings.")
        for instrument_id, details in self.underlying_details.items():
            symbol_name = details["name"]
            exchange_value = details["exchange"] # e.g., "NSE"
            instrument_type_value = details["instrument_type"] # e.g., "INDEX"
            asset_class_value = details["asset_class"] # e.g., "INDEX"

            # Initialize data_store for this underlying
            self.data_store[instrument_id] = {
                '1m': pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                            'fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low'])
            }
            self.trade_cooldown[instrument_id] = 0 # Reset cooldown timestamp

            self.logger.debug(f"Strategy {self.id} on_start: Requesting symbol {symbol_name} on {exchange_value} (Type: {instrument_type_value})")

            # self.all_timeframes is inherited from BaseStrategy and should include "1m"
            # as per this strategy's requirement.
            timeframes_to_sub = self.all_timeframes if self.all_timeframes else {"1m"}

            # Use the request_symbol method from BaseStrategy
            self.request_symbol(
                symbol_name=symbol_name,
                exchange_value=exchange_value,
                instrument_type_value=instrument_type_value,
                asset_class_value=asset_class_value,
                timeframes_to_subscribe=timeframes_to_sub # Explicitly pass the timeframes
            )

        self.entry_enabled = False # Reset entry enabled flag
        self.active_positions.clear() # Clear any stale active positions
        self.logger.info(f"Strategy '{self.id}' started. Waiting for market data and entry time ({self.entry_time_str}).")

    def on_market_data(self, event: MarketDataEvent):
        """
        Handles incoming raw tick data (MarketDataEvent).
        Primarily used here to check SL/TP for active option positions.
        """
        # self.logger.debug(f"Strategy {self.id} received MarketDataEvent: {event.instrument.symbol if event.instrument else 'N/A'}")
        if not event.instrument or not event.data or not event.instrument.instrument_id:
            return

        option_instrument_id = event.instrument.instrument_id
        # Check if this tick is for an option we are actively trading
        if option_instrument_id in self.active_positions:
            price_data = event.data.get(MarketDataType.LAST_PRICE.value) # Standardized key
            if price_data is not None:
                try:
                    current_option_price = float(price_data)
                    self._check_option_exit_conditions(option_instrument_id, current_option_price)
                except (ValueError, TypeError):
                    self.logger.warning(f"Strategy {self.id}: Invalid option price '{price_data}' for {option_instrument_id}.")
            else:
                self.logger.debug(f"Strategy {self.id}: No last price in MarketDataEvent for active option {option_instrument_id}")

    def on_bar(self, event: BarEvent): 
        """ 
        Handles incoming bar data (BarEvent). 
        This is where the core logic for analyzing underlying momentum resides. 
        """ 
        # self.logger.debug(f"Strategy {self.id} received BarEvent: {event.instrument.symbol if event.instrument else 'N/A'}@{event.timeframe}") 
        if not event.instrument or not event.instrument.instrument_id: 
            return 

        underlying_instrument_id = event.instrument.instrument_id 

        # self.logger.debug(f"instrument_id: {underlying_instrument_id}, self.underlying_instrument_ids: {self.underlying_instrument_ids}, all_timeframes: {self.all_timeframes}") 

        # This strategy only processes bars for its configured underlyings on 1m timeframe 
        if underlying_instrument_id not in self.underlying_instrument_ids or event.timeframe != '1m': 
            # self.logger.debug(f"Strategy {self.id}: Ignoring BarEvent for {underlying_instrument_id}@{event.timeframe} (not a tracked underlying or wrong TF).") 
            return 

        self._update_data_store(underlying_instrument_id, event) 
        self._calculate_indicators(underlying_instrument_id) 

        # Log after updating data store to show the actual state with the current bar 
        self.logger.debug(f"data_store: {self.data_store}") 
        df = self.data_store[underlying_instrument_id]['1m'] 
        self.logger.debug(f"df: {df}") 

        # current_dt = datetime.fromtimestamp(event.timestamp / 1000 if event.timestamp > 1e12 else event.timestamp) # Handle ms or s 
        current_dt = datetime.fromtimestamp(event.timestamp) # Assuming timestamp is in seconds 
        current_time = current_dt.time() 

        self.logger.debug(f"current_time: {current_time}, entry_time: {self.entry_time}, entry_enabled: {self.entry_enabled}") 
        if not self.entry_enabled and current_time >= self.entry_time: 
            self.entry_enabled = True 
            self.logger.info(f"Strategy '{self.id}': Entry time {self.entry_time_str} reached. Trading is now active.") 

        if current_time >= self.exit_time: 
            if self.active_positions: 
                self.logger.info(f"Strategy '{self.id}': Exit time {self.exit_time_str} reached. Exiting all open positions.") 
                self._exit_all_positions(f"End of day exit at {self.exit_time_str}") 
            if self.entry_enabled: # Disable further entries after exit time 
                self.logger.info(f"Strategy '{self.id}': Past exit time. Disabling further entries for the day.") 
                self.entry_enabled = False 
            return # No further processing after exit time 

        # Only check for entry signals if we have enough bars for a proper analysis 
        required_bars_for_signal = max(self.slow_ema_period, self.rsi_period) + self.breakout_bars + 1 
        self.logger.debug(f"required_bars_for_signal: {required_bars_for_signal}, current bars: {len(df)}")

        # Debug the indicator values for the last few bars
        if len(df) >= 5:  # Show last 5 bars if available
            self.logger.debug("Last 5 bars of indicators:")
            debug_df = df[['fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low']].tail(5)
            for i, row in debug_df.iterrows():
                self.logger.debug(f"Bar {i}: fast_ema={row['fast_ema']:.2f}, slow_ema={row['slow_ema']:.2f}, rsi={row['rsi']:.2f}, recent_high={row['recent_high']:.2f}, recent_low={row['recent_low']:.2f}")

        # Check if any indicators are NaN in the last bar
        if len(df) > 0:
            last_bar = df[['fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low']].iloc[-1]
            null_indicators = [col for col, val in last_bar.items() if pd.isna(val)]
            if null_indicators:
                self.logger.warning(f"NaN values detected in indicators: {null_indicators}")

        if len(df) >= required_bars_for_signal and not df[['fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low']].iloc[-1].isnull().any(): 
            if self.entry_enabled and not self._is_in_cooldown(underlying_instrument_id, event.timestamp): 
                self.logger.debug("Checking entry signal")
                # Add this before calling _check_entry_signals to debug entry criteria
                self._debug_entry_conditions(underlying_instrument_id, df)
                self._check_entry_signals(underlying_instrument_id, current_dt) 
            else: 
                reason = "Cooldown active" if self._is_in_cooldown(underlying_instrument_id, event.timestamp) else "Entry not enabled"
                self.logger.debug(f"Strategy '{self.id}' for {underlying_instrument_id} - not checking entry: {reason}")
        else:
            missing_requirements = []
            if len(df) < required_bars_for_signal:
                missing_requirements.append(f"Need {required_bars_for_signal} bars, have {len(df)}")
            if len(df) > 0 and df[['fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low']].iloc[-1].isnull().any():
                missing_requirements.append("Some indicators are NaN")

            self.logger.debug(f"Not checking entry signals: {', '.join(missing_requirements)}")

    def _debug_entry_conditions(self, underlying_instrument_id, df):
        """Debug the entry signal conditions"""
        last_bar = df.iloc[-1]

        # Extract the relevant indicator values
        close = last_bar['close']
        fast_ema = last_bar['fast_ema']
        slow_ema = last_bar['slow_ema']
        rsi = last_bar['rsi']
        recent_high = last_bar['recent_high']
        recent_low = last_bar['recent_low']

        # Debug long entry conditions
        ema_condition_long = fast_ema > slow_ema
        rsi_condition_long = rsi > self.rsi_upper_band
        price_condition_long = close > recent_high

        self.logger.debug(f"Long Entry Conditions for {underlying_instrument_id}:")
        self.logger.debug(f"  - EMA Condition (fast_ema > slow_ema): {fast_ema:.2f} > {slow_ema:.2f} = {ema_condition_long}")
        self.logger.debug(f"  - RSI Condition (rsi > {self.rsi_upper_band}): {rsi:.2f} > {self.rsi_upper_band} = {rsi_condition_long}")
        self.logger.debug(f"  - Price Condition (close > recent_high): {close:.2f} > {recent_high:.2f} = {price_condition_long}")
        self.logger.debug(f"  - All Conditions Met: {ema_condition_long and rsi_condition_long and price_condition_long}")

        # Debug short entry conditions
        ema_condition_short = fast_ema < slow_ema
        rsi_condition_short = rsi < self.rsi_lower_band
        price_condition_short = close < recent_low

        self.logger.debug(f"Short Entry Conditions for {underlying_instrument_id}:")
        self.logger.debug(f"  - EMA Condition (fast_ema < slow_ema): {fast_ema:.2f} < {slow_ema:.2f} = {ema_condition_short}")
        self.logger.debug(f"  - RSI Condition (rsi < {self.rsi_lower_band}): {rsi:.2f} < {self.rsi_lower_band} = {rsi_condition_short}")
        self.logger.debug(f"  - Price Condition (close < recent_low): {close:.2f} < {recent_low:.2f} = {price_condition_short}")
        self.logger.debug(f"  - All Conditions Met: {ema_condition_short and rsi_condition_short and price_condition_short}")


    def _is_in_cooldown(self, underlying_instrument_id: str, current_timestamp: float) -> bool:
        """Checks if the strategy is in a cooldown period for the given underlying."""
        last_exit_time = self.trade_cooldown.get(underlying_instrument_id, 0)
        if current_timestamp < last_exit_time + self.cooldown_period_seconds:
            return True
        return False

    def _update_data_store(self, instrument_id: str, event: BarEvent):
        """Updates the local DataFrame with new bar data for the given instrument_id."""
        df = self.data_store[instrument_id]['1m']
        new_row_data = {
            'timestamp': event.timestamp, # Assuming BarEvent.timestamp is epoch seconds
            'open': event.open_price,
            'high': event.high_price,
            'low': event.low_price,
            'close': event.close_price,
            'volume': event.volume
        }
        
        # Use pd.concat for appending, more robust for empty DataFrames
        new_row_df = pd.DataFrame([new_row_data])
        self.data_store[instrument_id]['1m'] = pd.concat([df, new_row_df], ignore_index=True)
        
        # Trim DataFrame to manage memory, keeping enough for indicator calculations + buffer
        max_len = max(self.slow_ema_period, self.rsi_period, self.breakout_bars) + 100 # Buffer of 50 bars
        if len(self.data_store[instrument_id]['1m']) > max_len:
            self.data_store[instrument_id]['1m'] = self.data_store[instrument_id]['1m'].iloc[-max_len:]

        self.logger.debug(f"_update_data_store, df: {df}")    

    def _calculate_indicators(self, instrument_id: str):
        """Calculates EMAs, RSI, and recent high/low for the given instrument_id's data."""
        df = self.data_store[instrument_id]['1m']
        # Ensure enough data points for the longest period indicator
        if len(df) < max(self.slow_ema_period, self.rsi_period):
            # self.logger.debug(f"Strategy {self.id}: Not enough data for {instrument_id} to calculate all indicators (have {len(df)}).")
            return

        df['fast_ema'] = df['close'].ewm(span=self.fast_ema_period, adjust=False).mean()
        df['slow_ema'] = df['close'].ewm(span=self.slow_ema_period, adjust=False).mean()

        # Calculate RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0.0).ewm(alpha=1/self.rsi_period, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0.0)).ewm(alpha=1/self.rsi_period, adjust=False).mean()
        
        rs = gain / loss.replace(0, 1e-9) # Avoid division by zero
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi'].fillna(50, inplace=True) # Fill initial NaNs with neutral 50

        # Calculate recent high/low for breakout confirmation (high/low of last N bars, excluding current)
        if len(df) >= self.breakout_bars + 1 : # Need N prior bars + current bar
            df['recent_high'] = df['high'].rolling(window=self.breakout_bars).max().shift(1)
            df['recent_low'] = df['low'].rolling(window=self.breakout_bars).min().shift(1)
        else:
            df['recent_high'] = np.nan
            df['recent_low'] = np.nan
        
        self.logger.debug(f"_calculate_indicators, df: {df}")    
        # self.data_store[instrument_id]['1m'] = df # df is a reference, changes are already applied

    def _check_entry_signals(self, underlying_instrument_id: str, current_dt: datetime):
        """Checks for entry signals based on calculated indicators."""
        self.logger.debug(f"data_store: {self.data_store}")
        df = self.data_store[underlying_instrument_id]['1m']
        self.logger.debug(f"df: {df}")

        # # Ensure enough data and indicators are calculated
        # required_bars_for_signal = max(self.slow_ema_period, self.rsi_period) + self.breakout_bars + 1 # +1 for previous bar comparison
        # if len(df) < required_bars_for_signal or df[['fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low']].iloc[-1].isnull().any():
        #     # self.logger.debug(f"Strategy {self.id}: Insufficient data or indicators for {underlying_instrument_id} to check entry signals.")
        #     return

        latest = df.iloc[-1]
        previous = df.iloc[-2] # For crossover detection

        # Check if already holding any option position (CE or PE) for this underlying
        for opt_key, pos_info in list(self.active_positions.items()): # Iterate copy
            if pos_info.get('underlying_instrument_id') == underlying_instrument_id:
                # self.logger.debug(f"Strategy {self.id}: Already holding an option position related to {underlying_instrument_id}. Skipping new entry check.")
                return

        call_signal = False
        # Bullish EMA Crossover: Fast EMA crosses above Slow EMA
        if previous['fast_ema'] <= previous['slow_ema'] and latest['fast_ema'] > latest['slow_ema']:
            # RSI Confirmation: Bullish momentum, not overbought
            if self.rsi_mid_point < latest['rsi'] < self.rsi_upper_band:
                # Breakout Confirmation: Price breaks above recent high
                if not pd.isna(latest['recent_high']) and latest['close'] > latest['recent_high']:
                    call_signal = True
                    self.logger.info(f"Strategy {self.id}: CALL Signal for {underlying_instrument_id} at {current_dt}. "
                                     f"EMA Crossover ({latest['fast_ema']:.2f} > {latest['slow_ema']:.2f}), "
                                     f"RSI={latest['rsi']:.2f}, Breakout > {latest['recent_high']:.2f}")
                    self._enter_option_position(underlying_instrument_id, SignalType.BUY_CALL, latest['close'], current_dt)

        put_signal = False
        # Bearish EMA Crossover: Fast EMA crosses below Slow EMA
        if not call_signal and previous['fast_ema'] >= previous['slow_ema'] and latest['fast_ema'] < latest['slow_ema']:
            # RSI Confirmation: Bearish momentum, not oversold
            if self.rsi_lower_band < latest['rsi'] < self.rsi_mid_point:
                # Breakout Confirmation: Price breaks below recent low
                if not pd.isna(latest['recent_low']) and latest['close'] < latest['recent_low']:
                    put_signal = True
                    self.logger.info(f"Strategy {self.id}: PUT Signal for {underlying_instrument_id} at {current_dt}. "
                                     f"EMA Crossover ({latest['fast_ema']:.2f} < {latest['slow_ema']:.2f}), "
                                     f"RSI={latest['rsi']:.2f}, Breakout < {latest['recent_low']:.2f}")
                    self._enter_option_position(underlying_instrument_id, SignalType.BUY_PUT, latest['close'], current_dt)
        
        # If no entry signal, check for EMA reversal exit for any active positions related to this underlying
        if not call_signal and not put_signal:
            self._check_ema_reversal_exit(underlying_instrument_id, latest)


    def _enter_option_position(self, underlying_instrument_id: str, signal_type: SignalType, underlying_price: float, current_dt: datetime):
        """Handles the logic to enter an option position."""
        
        # Get ATM strike using OptionManager
        # OptionManager needs the underlying_instrument_id (e.g., "NSE:NIFTY INDEX")
        atm_strike = self.option_manager.get_atm_strike(underlying_instrument_id) # This uses cached underlying price
        if atm_strike is None:
            # Fallback: try to calculate based on current underlying_price if OM cache miss
            underlying_config = self.underlying_details.get(underlying_instrument_id)
            if underlying_config:
                 strike_interval = self.config.get("market", {}).get("underlyings", [{}])[0].get("strike_interval", 50.0) # Simplified
                 if underlying_instrument_id in self.option_manager.strike_intervals:
                     strike_interval = self.option_manager.strike_intervals[underlying_instrument_id]

                 atm_strike = round(underlying_price / strike_interval) * strike_interval
                 self.logger.info(f"Strategy {self.id}: Calculated ATM strike {atm_strike} for {underlying_instrument_id} as fallback.")
            else:
                self.logger.warning(f"Strategy {self.id}: Could not get ATM strike for {underlying_instrument_id} at price {underlying_price}. Cannot enter position.")
                return

        option_type_enum = OptionType.CALL if signal_type == SignalType.BUY_CALL else OptionType.PUT
        
        # Get the specific option instrument using OptionManager
        # OptionManager.get_option_instrument needs the underlying's simple name (e.g., "NIFTY INDEX")
        underlying_simple_name = self.underlying_details[underlying_instrument_id]["name"]
        option_instrument = self.option_manager.get_option_instrument(
            underlying_simple_name, atm_strike, option_type_enum.value, self.expiry_offset
        )

        if not option_instrument or not option_instrument.instrument_id:
            self.logger.error(f"Strategy {self.id}: Could not find/get valid {option_type_enum.value} option instrument for {underlying_instrument_id}, strike {atm_strike}.")
            return

        option_instrument_id_to_trade = option_instrument.instrument_id # e.g., "NFO:NIFTY..."
        if option_instrument_id_to_trade in self.active_positions:
            self.logger.info(f"Strategy {self.id}: Already holding {option_instrument_id_to_trade}. Skipping duplicate entry signal.")
            return

        # Get latest tick price for the option to estimate entry price.
        # This relies on OptionManager + StrategyManager having subscribed to this option's feed.
        option_tick_data = self.data_manager.get_latest_tick(option_instrument_id_to_trade)
        if not option_tick_data or MarketDataType.LAST_PRICE.value not in option_tick_data:
            self.logger.warning(f"Strategy {self.id}: Tick data or last price not yet available for option {option_instrument_id_to_trade}. Cannot determine entry price accurately. Skipping entry.")
            return
        
        entry_option_price_estimate = option_tick_data[MarketDataType.LAST_PRICE.value]
        try:
            entry_option_price_estimate = float(entry_option_price_estimate)
        except (ValueError, TypeError):
            self.logger.error(f"Strategy {self.id}: Invalid option price '{entry_option_price_estimate}' for {option_instrument_id_to_trade}. Skipping entry.")
            return

        self.logger.info(f"Strategy {self.id}: Placing {signal_type.name} order for {option_instrument_id_to_trade} at estimated market price {entry_option_price_estimate:.2f}")
        
        # Generate a generic BUY signal. PortfolioManager/OrderExecution will handle specifics.
        self.generate_signal(
            instrument_id=option_instrument_id_to_trade,
            signal_type=SignalType.BUY, # Generic BUY
            data={'quantity': self.quantity,
                  'order_type': 'MARKET', # Or 'LIMIT' with entry_option_price_estimate
                  'product_type': 'INTRADAY', # Example
                  'intended_option_type': option_type_enum.value # For context if needed by PM
                 }
        )
        
        # Tentatively add to active_positions. This will be confirmed/updated by on_fill.
        self.active_positions[option_instrument_id_to_trade] = {
            'underlying_instrument_id': underlying_instrument_id, # Link back to the underlying that triggered
            'option_type': option_type_enum.value,
            'entry_price': entry_option_price_estimate, # This is an estimate, actual fill price is source of truth
            'stop_loss_price': entry_option_price_estimate * (1 - self.stop_loss_percent / 100),
            'target_price': entry_option_price_estimate * (1 + self.target_percent / 100),
            'entry_time': current_dt,
            'instrument_object': option_instrument # Store the full Instrument object
        }
        self.logger.info(f"Strategy {self.id}: Position for {option_instrument_id_to_trade} tentatively initialized. "
                         f"Est. Entry: {entry_option_price_estimate:.2f}, "
                         f"SL: {self.active_positions[option_instrument_id_to_trade]['stop_loss_price']:.2f}, "
                         f"TP: {self.active_positions[option_instrument_id_to_trade]['target_price']:.2f}")


    def on_fill(self, event: FillEvent):
        """Handles fill events to update active position details."""
        super().on_fill(event) # Call base class method if it exists
        
        filled_option_id = event.instrument_id
        if not filled_option_id or filled_option_id not in self.active_positions:
            self.logger.debug(f"Strategy {self.id}: Received fill for {filled_option_id}, but not in active_positions or no ID. Ignoring.")
            return

        pos_info = self.active_positions[filled_option_id]
        
        # Assuming entry is always a BUY for this strategy's options
        if event.order_side == OrderSide.BUY and event.fill_quantity > 0: # Entry fill
            actual_entry_price = event.fill_price
            # Update entry price and recalculate SL/TP based on actual fill
            # This assumes one fill completes the entry. For partial fills, logic would be more complex (averaging).
            pos_info['entry_price'] = actual_entry_price
            pos_info['stop_loss_price'] = actual_entry_price * (1 - self.stop_loss_percent / 100)
            pos_info['target_price'] = actual_entry_price * (1 + self.target_percent / 100)
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) + event.fill_quantity
            
            self.logger.info(f"Strategy {self.id}: ENTRY FILL for {filled_option_id}. "
                             f"Actual Entry Price: {actual_entry_price:.2f}. "
                             f"New SL: {pos_info['stop_loss_price']:.2f}, TP: {pos_info['target_price']:.2f}. "
                             f"Filled Qty: {event.fill_quantity}, Total Filled: {pos_info['filled_quantity']}")

        elif event.order_side == OrderSide.SELL and event.fill_quantity > 0: # Exit fill
            self.logger.info(f"Strategy {self.id}: EXIT FILL for {filled_option_id} at {event.fill_price:.2f}. "
                             f"Reason: {event.data.get('reason', 'N/A')}. Filled Qty: {event.fill_quantity}")
            # Clean up the position from active_positions
            self._handle_exit_cleanup(filled_option_id)


    def _check_option_exit_conditions(self, option_instrument_id: str, current_price: float):
        """Checks SL/TP for a given active option position based on its current market price."""
        if option_instrument_id not in self.active_positions:
            return
        pos_info = self.active_positions[option_instrument_id]

        exit_reason = None
        if current_price <= pos_info['stop_loss_price']:
            exit_reason = f"Stop-loss hit at {current_price:.2f} (SL was: {pos_info['stop_loss_price']:.2f})"
        elif current_price >= pos_info['target_price']:
            exit_reason = f"Target-profit hit at {current_price:.2f} (TP was: {pos_info['target_price']:.2f})"
        
        if exit_reason:
            self.logger.info(f"Strategy {self.id}: Exiting {option_instrument_id}. Reason: {exit_reason}")
            self._exit_position(option_instrument_id, exit_reason)


    def _check_ema_reversal_exit(self, underlying_instrument_id: str, latest_underlying_bar: pd.Series):
        """Checks for EMA reversal on the underlying to exit related option positions."""
        for opt_id, pos_info in list(self.active_positions.items()): # Iterate over a copy
            if pos_info.get('underlying_instrument_id') == underlying_instrument_id:
                option_type = pos_info.get('option_type')
                exit_signal = False
                
                # If holding a CALL, exit on bearish EMA crossover on underlying
                if option_type == OptionType.CALL.value and latest_underlying_bar['fast_ema'] < latest_underlying_bar['slow_ema']:
                    exit_signal = True
                # If holding a PUT, exit on bullish EMA crossover on underlying
                elif option_type == OptionType.PUT.value and latest_underlying_bar['fast_ema'] > latest_underlying_bar['slow_ema']:
                    exit_signal = True
                
                if exit_signal:
                    reason = f"EMA reversal on underlying {underlying_instrument_id} (FastEMA: {latest_underlying_bar['fast_ema']:.2f}, SlowEMA: {latest_underlying_bar['slow_ema']:.2f})"
                    self.logger.info(f"Strategy {self.id}: Exiting option {opt_id} due to {reason}.")
                    self._exit_position(opt_id, reason)


    def _exit_position(self, option_instrument_id: str, reason: str):
        """Generates a SELL signal to exit an option position."""
        if option_instrument_id not in self.active_positions:
            self.logger.warning(f"Strategy {self.id}: Attempted to exit {option_instrument_id} but not in active_positions.")
            return
        
        pos_info = self.active_positions[option_instrument_id]
        # Quantity to sell should match the quantity held.
        # This simple model assumes full exit. Partial exits would need more complex quantity management.
        quantity_to_sell = pos_info.get('filled_quantity', self.quantity) # Use filled_quantity if available
        if quantity_to_sell <= 0:
            self.logger.warning(f"Strategy {self.id}: Attempted to exit {option_instrument_id} with zero or negative quantity. Aborting exit signal.")
            return

        self.logger.info(f"Strategy {self.id}: Generating SELL signal for {option_instrument_id} (Qty: {quantity_to_sell}). Reason: {reason}")
        self.generate_signal(
            instrument_id=option_instrument_id,
            signal_type=SignalType.SELL,
            data={'quantity': quantity_to_sell,
                  'order_type': 'MARKET',
                  'product_type': 'INTRADAY', # Example
                  'reason': reason}
        )
        # Actual removal from active_positions and cooldown update should ideally happen upon receiving the SELL FillEvent.
        # For robustness, if fill events are not guaranteed or timely, one might do a preliminary removal here,
        # but it's cleaner to wait for the fill.


    def _handle_exit_cleanup(self, option_instrument_id: str):
        """Cleans up after an option position is confirmed exited (e.g., by a fill)."""
        if option_instrument_id in self.active_positions:
            pos_info = self.active_positions.pop(option_instrument_id) # Remove from active list
            underlying_id = pos_info.get('underlying_instrument_id')
            if underlying_id:
                self.trade_cooldown[underlying_id] = datetime.now().timestamp() # Start cooldown for the underlying
                self.logger.info(f"Strategy {self.id}: Position {option_instrument_id} removed from active. Cooldown started for underlying {underlying_id}.")
            else:
                self.logger.warning(f"Strategy {self.id}: Exited option {option_instrument_id} but no underlying_instrument_id found in pos_info to start cooldown.")
        # else:
            # self.logger.debug(f"Strategy {self.id}: _handle_exit_cleanup called for {option_instrument_id}, but not found in active_positions (possibly already cleaned).")


    def _exit_all_positions(self, reason: str):
        """Exits all currently active option positions held by the strategy."""
        if not self.active_positions:
            self.logger.info(f"Strategy {self.id}: _exit_all_positions called, but no active positions to exit.")
            return
            
        self.logger.info(f"Strategy {self.id}: Exiting all {len(self.active_positions)} active positions. Reason: {reason}")
        for opt_id in list(self.active_positions.keys()): # Iterate over a copy of keys
            self._exit_position(opt_id, reason)


    def on_stop(self):
        """Called by StrategyManager when the strategy is stopped."""
        super().on_stop() # Call base class on_stop
        self.logger.info(f"Strategy '{self.id}' on_stop: Exiting all open positions if any.")
        self._exit_all_positions("Strategy stop command received")
        # Clear data stores and reset state if necessary for a clean restart
        self.data_store.clear()
        self.active_positions.clear()
        self.trade_cooldown.clear()
        self.entry_enabled = False
        self.logger.info(f"Strategy '{self.id}' stopped and cleaned up.")


