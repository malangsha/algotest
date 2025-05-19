"""
Simple Test Option Strategy (Enhanced)

This strategy is designed specifically for end-to-end testing of the trading framework.
It uses simplified, deterministic entry conditions to guarantee trade execution within
realistic timeframes, enabling comprehensive validation of all framework components.

Key Features:
- Simplified entry logic based on time intervals and basic price movement.
- Guaranteed signal generation for both CALL and PUT trades under normal conditions.
- Comprehensive testing of fill scenarios (full fills, partial fills, slippage).
- Maintains full architectural consistency with production strategies.
- Extensive logging for debugging framework interactions.
- Enhanced with clear docstrings, type hints, input validation, and robust parameter handling.

Entry Logic:
- CALL Entry: At configurable intervals (e.g., every `test_interval_minutes`),
              if price > `sma_period`-SMA and other conditions met.
- PUT Entry: At configurable intervals, offset from CALL timing,
             if price < `sma_period`-SMA and other conditions met.
- Alternates between CALL and PUT signals for each underlying.
- Simple price-based confirmation to avoid completely random entries.

Exit Logic:
- Standard exits: Stop-loss, target-profit.
- Test-specific exit: Time-based exit after `test_exit_minutes` if no SL/TP hit.
- End-of-day exit: All positions squared off at `exit_time`.

Risk Management:
- Configurable stop-loss percentage and target percentage.
- Configurable `max_positions_per_underlying`.
- Cooldown period (`cooldown_period_seconds`) after a trade on an underlying.
- Position sizing via `quantity` parameter.
"""

from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np

# Assuming these are the correct import paths for your framework
from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, SignalEvent, FillEvent # FillEvent might be handled by base or portfolio
from models.instrument import Instrument, InstrumentType, AssetClass
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide # Make sure OrderSide is correctly defined
from strategies.strategy_registry import StrategyRegistry
# from core.logging_manager import get_logger # Assuming logger is handled by base class or passed in

@StrategyRegistry.register('simple_test_option_strategy') # Renamed to avoid conflict if old one exists
class SimpleTestOptionStrategy(OptionStrategy):
    """
    Enhanced Simple Test Option Strategy for end-to-end framework validation.
    Designed to guarantee trade execution with simplified, configurable entry conditions,
    and includes robust parameter handling, type hints, and detailed docstrings.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager: Any, option_manager: Any, portfolio_manager: Any,
                 event_manager: Any, broker: Optional[Any] = None, strategy_manager: Optional[Any] = None):
        """
        Initializes the SimpleTestOptionStrategy.

        Args:
            strategy_id: Unique identifier for the strategy instance.
            config: Configuration dictionary for the strategy.
            data_manager: Manages market data.
            option_manager: Manages option instrument details and selection.
            portfolio_manager: Manages portfolio state and positions.
            event_manager: Handles event queuing and dispatching.
            broker: Broker interface for order execution (optional).
            strategy_manager: Manages strategy lifecycle (optional).
        """
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager, broker, strategy_manager)

        # --- Parameter Loading & Validation ---
        self.params: Dict[str, Any] = config.get('parameters', {})
        self.execution_params: Dict[str, Any] = config.get('execution', {})
        self.risk_params: Dict[str, Any] = config.get('risk', {})

        # Entry and Exit timing
        self.entry_time_str: str = self._get_param('entry_time', '09:20:00', self.params)
        self.exit_time_str: str = self._get_param('exit_time', '15:15:00', self.params)
        self.entry_time: dt_time = self._parse_time(self.entry_time_str, dt_time(9, 20, 0))
        self.exit_time: dt_time = self._parse_time(self.exit_time_str, dt_time(15, 15, 0))

        # Core strategy parameters
        self.stop_loss_percent: float = self._get_param('stop_loss_percent', 5.0, self.params, (0, 100))
        self.target_percent: float = self._get_param('target_percent', 10.0, self.params, (0, 1000)) # Generous upper bound
        self.quantity: int = self._get_param('quantity', 1, self.execution_params, (1, 1000))

        # Test-specific parameters
        self.test_interval_minutes: int = self._get_param('test_interval_minutes', 15, self.params, (1, 120))
        self.sma_period: int = self._get_param('sma_period', 5, self.params, (2, 200))
        self.max_positions_per_underlying: int = self._get_param('max_positions_per_underlying', 2, self.risk_params, (1, 10))
        self.test_exit_minutes: int = self._get_param('test_exit_minutes', 30, self.params, (1, 180))
        self.cooldown_period_seconds: int = self._get_param('cooldown_period_seconds', 60, self.params, (0, 3600))

        # --- Underlying Configuration ---
        self.strategy_specific_underlyings_config: List[Dict[str, Any]] = config.get("underlyings", [])
        self.underlying_instrument_ids: List[str] = []
        self.underlying_details: Dict[str, Dict[str, Any]] = {}
        self._configure_underlyings()

        self.expiry_offset: int = config.get('expiry_offset', 0)

        # --- Data Storage & State ---
        self.data_store: Dict[str, Dict[str, pd.DataFrame]] = {} # Stores bar data and indicators
        self.active_positions: Dict[str, Dict[str, Any]] = {} # Tracks active option positions
        self.entry_enabled: bool = False # Flag to enable entries after entry_time
        self.last_signal_times: Dict[str, float] = {} # Tracks last signal time per underlying
        self.signal_counter: Dict[str, int] = {} # Alternates CALL/PUT signals per underlying
        self.trade_cooldown: Dict[str, float] = {} # Tracks cooldown end time per underlying

        # --- Test Statistics ---
        self.test_statistics: Dict[str, Any] = self._reset_test_statistics()

        self.logger.info(f"Strategy '{self.id}' (SimpleTestOptionStrategy) initialized. "
                         f"Underlyings: {self.underlying_instrument_ids}, "
                         f"Test Interval: {self.test_interval_minutes}min, SMA: {self.sma_period}, "
                         f"Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, TP: {self.target_percent}%")

    def _get_param(self, key: str, default: Any, source_dict: Dict[str, Any],
                   value_range: Optional[Tuple[Any, Any]] = None) -> Any:
        """
        Safely retrieves a parameter, applies a default, and optionally validates its range.
        Logs a warning if the parameter is missing or invalid.
        """
        value = source_dict.get(key)
        if value is None:
            self.logger.warning(f"Strategy '{self.id}': Parameter '{key}' not found in config. Using default: {default}.")
            return default

        # Type check based on default's type (basic check)
        if not isinstance(value, type(default)):
            try:
                # Attempt type conversion for common cases (e.g., int from float if compatible)
                if isinstance(default, float) and isinstance(value, int):
                    value = float(value)
                elif isinstance(default, int) and isinstance(value, float) and value.is_integer():
                    value = int(value)
                else:
                    raise TypeError
            except (TypeError, ValueError):
                self.logger.error(f"Strategy '{self.id}': Parameter '{key}' has invalid type {type(value).__name__} "
                                  f"(expected {type(default).__name__}). Using default: {default}.")
                return default

        if value_range:
            min_val, max_val = value_range
            if not (min_val <= value <= max_val):
                self.logger.error(f"Strategy '{self.id}': Parameter '{key}' value {value} is outside the valid range "
                                  f"[{min_val}, {max_val}]. Using default: {default}.")
                return default
        return value

    def _parse_time(self, time_str: str, default_time: dt_time) -> dt_time:
        """
        Parses a time string (HH:MM:SS) into a datetime.time object.
        Logs an error and returns default_time if parsing fails.
        """
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError) as e:
            self.logger.error(f"Strategy '{self.id}': Invalid time format '{time_str}'. Error: {e}. Using default: {default_time}.")
            return default_time

    def _configure_underlyings(self) -> None:
        """
        Processes underlying configurations from the strategy config.
        """
        if not self.strategy_specific_underlyings_config:
            self.logger.warning(f"Strategy '{self.id}': No underlyings configured. Strategy may not generate signals.")
            return

        for u_conf in self.strategy_specific_underlyings_config:
            name: Optional[str] = u_conf.get("name")
            exchange_str: Optional[str] = u_conf.get("spot_exchange")

            if not name or not exchange_str:
                self.logger.warning(f"Strategy '{self.id}': Skipping underlying config due to missing 'name' or 'spot_exchange': {u_conf}")
                continue

            # Determine instrument type and asset class, defaulting for indices
            is_index: bool = u_conf.get("index", False) # Default to False if 'index' key is missing
            inst_type_str: str = u_conf.get("instrument_type", "IND" if is_index else "EQ").upper()
            asset_cls_str: str = u_conf.get("asset_class", "INDEX" if is_index else "EQUITY").upper()
            option_exchange_str: str = u_conf.get("option_exchange", "NFO").upper() # Default NFO

            instrument_id = f"{exchange_str.upper()}:{name.upper()}" # Standardize to uppercase
            self.underlying_instrument_ids.append(instrument_id)
            self.underlying_details[instrument_id] = {
                "name": name.upper(),
                "exchange": exchange_str.upper(),
                "instrument_type": inst_type_str,
                "asset_class": asset_cls_str,
                "option_exchange": option_exchange_str
            }
            self.logger.info(f"Strategy '{self.id}': Configured underlying: {instrument_id}")

    def _reset_test_statistics(self) -> Dict[str, Any]:
        """Resets and returns the initial test statistics dictionary."""
        return {
            'signals_generated': 0,
            'calls_attempted': 0,
            'puts_attempted': 0,
            'successful_entries': 0,
            'successful_exits': 0,
            'partial_fills_on_entry': 0, # More specific
            'failed_option_fetches': 0,
            'failed_orders_sent_to_broker': 0, # If broker interaction is tracked
            'stop_loss_exits': 0,
            'target_profit_exits': 0,
            'time_based_test_exits': 0,
            'eod_exits': 0
        }

    def initialize(self) -> None:
        """Strategy-specific initialization called once before on_start."""
        super().initialize()
        self.test_statistics = self._reset_test_statistics()
        # Any other one-time setup specific to this strategy
        self.logger.info(f"Strategy '{self.id}' (SimpleTestOptionStrategy) initialized. Test statistics reset.")

    def on_start(self) -> None:
        """Called when the strategy is started (e.g., at the beginning of a trading day or backtest)."""
        super().on_start()
        self.logger.info(f"Strategy '{self.id}' starting. Requesting subscriptions for test underlyings.")

        self.active_positions.clear()
        self.entry_enabled = False # Ensure it's reset on start

        for instrument_id, details in self.underlying_details.items():
            # Initialize data stores and state for each underlying
            self.data_store[instrument_id] = {
                tf: pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'sma'])
                for tf in (self.all_timeframes if self.all_timeframes else {"1m"}) # Use configured timeframes
            }
            self.trade_cooldown[instrument_id] = 0.0 # Timestamp of when cooldown ends
            self.last_signal_times[instrument_id] = 0.0 # Timestamp of last signal
            self.signal_counter[instrument_id] = 0 # Initialize signal counter

            self.logger.info(f"Strategy '{self.id}': Subscribing to {details['name']} ({instrument_id}) "
                             f"on {details['exchange']} for timeframes: {self.all_timeframes or {'1m'}}.")

            # Use the base class method to request data subscriptions
            self.request_symbol(
                symbol_name=details["name"],
                exchange_value=details["exchange"],
                instrument_type_value=details["instrument_type"],
                asset_class_value=details["asset_class"],
                timeframes_to_subscribe=self.all_timeframes if self.all_timeframes else {"1m"}
            )
        self.logger.info(f"Strategy '{self.id}' started successfully. Test framework ready.")

    def on_market_data(self, event: MarketDataEvent) -> None:
        """
        Handles incoming market data (ticks) for subscribed instruments, typically options.
        Used here for checking exit conditions on active option positions.
        """
        if not event.instrument or not event.instrument.instrument_id or not event.data:
            self.logger.debug(f"Strategy '{self.id}': Received incomplete market data event.")
            return

        option_instrument_id: str = event.instrument.instrument_id
        if option_instrument_id in self.active_positions:
            # Assuming MarketDataType.LAST_PRICE is an enum or constant
            ltp_data = event.data.get(MarketDataType.LAST_PRICE.value) # Or your framework's way to get LTP
            if ltp_data is not None:
                try:
                    current_option_price: float = float(ltp_data)
                    self._check_option_exit_conditions(option_instrument_id, current_option_price, event.timestamp)
                    self._check_time_based_test_exit(option_instrument_id, event.timestamp)
                except (ValueError, TypeError):
                    self.logger.warning(f"Strategy '{self.id}': Invalid option price '{ltp_data}' "
                                        f"for {option_instrument_id}. Type: {type(ltp_data).__name__}")
            # else:
            #     self.logger.debug(f"Strategy '{self.id}': No LTP in market data for active position {option_instrument_id}")

    def on_bar(self, event: BarEvent) -> None:
        """
        Handles incoming bar data for subscribed underlying instruments.
        This is the primary method where trading logic (entry signals) resides.
        """
        if not event.instrument or not event.instrument.instrument_id:
            self.logger.debug(f"Strategy '{self.id}': Received incomplete bar event.")
            return

        underlying_instrument_id: str = event.instrument.instrument_id
        timeframe: str = event.timeframe

        # Process only for configured underlyings and their primary timeframe (e.g., '1m')
        if underlying_instrument_id not in self.underlying_instrument_ids or timeframe not in self.data_store[underlying_instrument_id]:
            return # Not a relevant bar for this strategy instance or timeframe
    
        self.logger.debug(f"Received on_bar: underlying_instrument_id: {underlying_instrument_id}, timeframe: {timeframe}")

        self._update_data_store(underlying_instrument_id, timeframe, event)
        self._calculate_simple_indicators(underlying_instrument_id, timeframe)

        # Perform actions only on the primary timeframe (e.g. '1m' if configured as such)
        # This check assumes self.timeframe is the primary decision-making timeframe.
        if timeframe != self.timeframe: # self.timeframe is from base_strategy, usually primary
            return

        current_dt: datetime = datetime.fromtimestamp(event.timestamp)
        current_time: dt_time = current_dt.time()

        # Enable entry logic after market open/strategy entry time
        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Strategy '{self.id}': Test entry window opened at {self.entry_time_str}. Current time: {current_time}.")

        # Square off all positions and disable entries before market close/strategy exit time
        if current_time >= self.exit_time:
            if self.active_positions:
                self.logger.info(f"Strategy '{self.id}': Test exit time ({self.exit_time_str}) reached. "
                                 f"Closing all active positions. Current time: {current_time}.")
                self._exit_all_positions(reason="End of Test Day Square Off", timestamp=event.timestamp)
            if self.entry_enabled:
                self.entry_enabled = False # Disable further entries
                self.logger.info(f"Strategy '{self.id}': Test entry window closed at {self.exit_time_str}.")
                self._log_test_statistics() # Log stats at EOD
            return

        # Check for entry signals if entry is enabled
        df = self.data_store[underlying_instrument_id][timeframe]
        if self.entry_enabled and len(df) >= self.sma_period:
            self._check_test_entry_signals(underlying_instrument_id, current_dt, event.timestamp)

    def _update_data_store(self, instrument_id: str, timeframe: str, event: BarEvent) -> None:
        """Appends new bar data to the internal DataFrame for the given instrument and timeframe."""
        df = self.data_store[instrument_id][timeframe]
        new_row_data = {
            'timestamp': event.timestamp,
            'open': event.open_price,
            'high': event.high_price,
            'low': event.low_price,
            'close': event.close_price,
            'volume': event.volume
        }
        # Create a new DataFrame for the new row to avoid direct append if it's deprecated/inefficient
        new_row_df = pd.DataFrame([new_row_data])
        self.data_store[instrument_id][timeframe] = pd.concat([df, new_row_df], ignore_index=True)

        # Keep memory usage in check by limiting DataFrame size
        # Max length should be enough for indicators + some buffer
        max_len = max(self.sma_period + 50, self.min_history_bars_required + 10) # min_history_bars_required from base
        if len(self.data_store[instrument_id][timeframe]) > max_len:
            self.data_store[instrument_id][timeframe] = self.data_store[instrument_id][timeframe].iloc[-max_len:]

    def _calculate_simple_indicators(self, instrument_id: str, timeframe: str) -> None:
        """Calculates the Simple Moving Average (SMA) for the given instrument and timeframe."""
        df = self.data_store[instrument_id][timeframe]
        if len(df) >= self.sma_period:
            df['sma'] = df['close'].rolling(window=self.sma_period, min_periods=self.sma_period).mean()
        else:
            df['sma'] = np.nan # Not enough data for SMA

    def _check_test_entry_signals(self, underlying_instrument_id: str, current_dt: datetime, current_timestamp: float) -> None:
        """
        Checks and generates simplified test entry signals (CALL/PUT) based on time intervals and SMA.
        """
        last_signal_time: float = self.last_signal_times.get(underlying_instrument_id, 0.0)
        interval_seconds: int = self.test_interval_minutes * 60

        # Ensure enough time has passed since the last signal for this underlying
        if current_timestamp < last_signal_time + interval_seconds:
            return

        # Check if the underlying is in a cooldown period
        if self._is_in_cooldown(underlying_instrument_id, current_timestamp):
            self.logger.debug(f"Strategy '{self.id}': {underlying_instrument_id} in cooldown until "
                              f"{datetime.fromtimestamp(self.trade_cooldown[underlying_instrument_id]).time()}. Skipping signal.")
            return

        # Check if max positions for this underlying are already open
        active_count_for_underlying = sum(
            1 for pos in self.active_positions.values()
            if pos.get('underlying_instrument_id') == underlying_instrument_id
        )
        if active_count_for_underlying >= self.max_positions_per_underlying:
            self.logger.debug(f"Strategy '{self.id}': Max positions ({self.max_positions_per_underlying}) "
                              f"reached for {underlying_instrument_id}. Skipping signal.")
            return

        # Get the latest data bar for the primary timeframe
        df = self.data_store[underlying_instrument_id][self.timeframe] # Use primary timeframe
        if df.empty or pd.isna(df.iloc[-1]['sma']):
            self.logger.debug(f"Strategy '{self.id}': Not enough data or SMA not calculated for {underlying_instrument_id}. Skipping.")
            return

        latest_bar = df.iloc[-1]
        current_price: float = latest_bar['close']
        sma_value: float = latest_bar['sma']

        # Alternate between CALL and PUT signals
        signal_count: int = self.signal_counter.get(underlying_instrument_id, 0)
        is_call_turn: bool = (signal_count % 2) == 0 # 0th, 2nd, 4th signals are CALLs

        signal_to_generate: Optional[SignalType] = None
        entry_reason: str = ""

        if is_call_turn:
            if current_price > sma_value: # Bullish confirmation for CALL
                signal_to_generate = SignalType.BUY_CALL
                entry_reason = f"Test CALL Signal #{signal_count + 1}: Price {current_price:.2f} > SMA({self.sma_period}) {sma_value:.2f}"
                self.test_statistics['calls_attempted'] += 1
        else: # PUT turn
            if current_price < sma_value: # Bearish confirmation for PUT
                signal_to_generate = SignalType.BUY_PUT
                entry_reason = f"Test PUT Signal #{signal_count + 1}: Price {current_price:.2f} < SMA({self.sma_period}) {sma_value:.2f}"
                self.test_statistics['puts_attempted'] += 1

        if signal_to_generate:
            self.logger.info(f"Strategy '{self.id}': {entry_reason} for {underlying_instrument_id}.")
            self._enter_option_position(underlying_instrument_id, signal_to_generate, current_price, current_dt, current_timestamp)
            self.last_signal_times[underlying_instrument_id] = current_timestamp
            self.signal_counter[underlying_instrument_id] = signal_count + 1
            self.test_statistics['signals_generated'] += 1
        # else:
            # self.logger.debug(f"Strategy '{self.id}': Conditions not met for {'CALL' if is_call_turn else 'PUT'} "
            #                   f"signal on {underlying_instrument_id} at {current_dt.time()}. Price: {current_price}, SMA: {sma_value}")


    def _is_in_cooldown(self, underlying_instrument_id: str, current_timestamp: float) -> bool:
        """Checks if the strategy is in a cooldown period for the specified underlying."""
        cooldown_end_time = self.trade_cooldown.get(underlying_instrument_id, 0.0)
        return current_timestamp < cooldown_end_time

    def _enter_option_position(self, underlying_instrument_id: str, signal_type: SignalType,
                               underlying_price: float, current_dt: datetime, current_timestamp: float) -> None:
        """
        Selects an ATM option and generates a BUY signal for it.
        """
        self.logger.debug(f"_enter_option_position: {underlying_instrument_id}, signal_type:{signal_type}, underlying_price:{underlying_price}")
        option_type_enum: OptionType = OptionType.CALL if signal_type == SignalType.BUY_CALL else OptionType.PUT
        underlying_config = self.underlying_details.get(underlying_instrument_id)
        if not underlying_config:
            self.logger.error(f"Strategy '{self.id}': Missing underlying details for {underlying_instrument_id}. Cannot enter position.")
            self.test_statistics['failed_option_fetches'] +=1
            return

        underlying_name: str = underlying_config["name"]
        option_exchange: str = underlying_config["option_exchange"]

        # Get ATM strike (framework might provide this, or we estimate)
        # This part is highly dependent on your OptionManager's capabilities
        atm_strike: Optional[float] = self.option_manager.get_atm_strike(
            underlying_symbol=underlying_name            
        )

        if atm_strike is None:
            # Fallback: simple rounding if OptionManager fails (basic for testing)
            strike_interval = self.option_manager.strike_intervals(underlying_name)            
            if strike_interval is None: # Absolute fallback
                strike_interval = 50.0 if "NIFTY" in underlying_name.upper() else 100.0
                self.logger.warning(f"Strategy '{self.id}': Could not get strike interval for {underlying_name}. Using default: {strike_interval}")
            atm_strike = round(underlying_price / strike_interval) * strike_interval
            self.logger.warning(f"Strategy '{self.id}': OptionManager failed to provide ATM strike for {underlying_instrument_id}. "
                                f"Estimated ATM strike: {atm_strike:.2f}")

        # Get the option instrument
        self.logger.debug(f"option_instrument: underlying_name: {underlying_name}, atm_strike:{atm_strike}, option_type_enum.value: {option_type_enum.value}, expiry_offset: {self.expiry_offset}")
        option_instrument: Optional[Instrument] = self.option_manager.get_option_instrument(
            underlying_symbol=underlying_name,
            strike=atm_strike,
            option_type=option_type_enum.value, # "CALL" or "PUT"
            expiry_offset=self.expiry_offset
        )

        if not option_instrument or not option_instrument.instrument_id:
            self.logger.error(f"Strategy '{self.id}': Failed to get {option_type_enum.value} option instrument "
                              f"for {underlying_instrument_id} at strike {atm_strike:.2f}, expiry offset {self.expiry_offset}.")
            self.test_statistics['failed_option_fetches'] += 1
            return

        option_instrument_id: str = option_instrument.instrument_id
        if option_instrument_id in self.active_positions:
            self.logger.info(f"Strategy '{self.id}': Already have an active test position for {option_instrument_id}. Skipping new entry.")
            return

        # For testing, we might not have live option prices immediately.
        # A real strategy would wait for option tick or use a reference.
        # Here, we'll use a placeholder or try to fetch if available.
        entry_option_price: float = 0.0
        option_tick_data = self.data_manager.get_latest_tick(option_instrument_id)
        if option_tick_data and MarketDataType.LAST_PRICE.value in option_tick_data:
            try:
                entry_option_price = float(option_tick_data[MarketDataType.LAST_PRICE.value])
            except (ValueError, TypeError):
                self.logger.warning(f"Strategy '{self.id}': Invalid LTP in tick data for {option_instrument_id}. Estimating price.")
                entry_option_price = max(0.1, underlying_price * 0.015) # Rough 1.5% of underlying as fallback
        else: # Estimate if no tick data (common in backtests or if subscription is slow)
            entry_option_price = max(0.1, underlying_price * 0.015) # Rough 1.5% of underlying as fallback
            self.logger.warning(f"Strategy '{self.id}': No live tick data for {option_instrument_id}. "
                                f"Using estimated entry price: {entry_option_price:.2f}")

        if entry_option_price <= 0: # Basic sanity check
            self.logger.error(f"Strategy '{self.id}': Estimated/fetched option price for {option_instrument_id} is {entry_option_price:.2f}. "
                              f"Cannot proceed with entry.")
            self.test_statistics['failed_option_fetches'] += 1
            return

        # Prepare signal data
        signal_data = {
            'quantity': self.quantity,
            'order_type': self.execution_params.get('order_type', 'MARKET'), # Use configured order type
            'product_type': self.execution_params.get('product_type', 'INTRADAY'), # Default to INTRADAY
            'intended_option_type': option_type_enum.value,
            'entry_price_ref': entry_option_price, # Reference price at signal time
            'underlying_price_at_signal': underlying_price,
            'atm_strike_at_signal': atm_strike,
            'test_signal_flag': True # Custom flag for test signals
        }

        self.logger.info(f"Strategy '{self.id}': Generating BUY signal for {option_instrument_id} ({option_type_enum.value}) "
                         f"Qty: {self.quantity}, Est. Price: {entry_option_price:.2f}")

        # Generate the BUY signal (to acquire the option)
        # The base class generate_signal method should handle event creation.
        self.generate_signal(
            instrument_id=option_instrument_id, # Pass the full instrument object
            signal_type=SignalType.BUY_CALL, # This means buy the option (long call or long put)
            data=signal_data
        )

        # Tentatively add to active_positions; will be confirmed/updated by FillEvent
        # Store necessary info for SL/TP calculation and management
        self.active_positions[option_instrument_id] = {
            'underlying_instrument_id': underlying_instrument_id,
            'option_type': option_type_enum.value,
            'instrument_object': option_instrument, # Store the instrument object
            'intended_entry_price': entry_option_price, # Price at time of signal
            'actual_entry_price': None, # To be updated on fill
            'filled_quantity': 0,
            'stop_loss_price': None, # Will be set after fill
            'target_price': None, # Will be set after fill
            'entry_timestamp': current_timestamp, # Timestamp of signal generation
            'test_position_flag': True
        }
        # Note: SL/TP prices are calculated based on actual fill price in on_fill.

    def on_fill(self, event: FillEvent) -> None:
        """
        Handles fill events for orders placed by this strategy.
        Updates active position details, calculates SL/TP based on actual fill price.
        """
        # Call super().on_fill if it has generic logic you want to reuse
        # super().on_fill(event) # Uncomment if your base class has on_fill logic

        filled_option_id: str = event.instrument_id # Assuming instrument_id is directly on event
        if not filled_option_id or filled_option_id not in self.active_positions:
            # This fill might be for another strategy or an old order if not filtered by strategy_id
            # self.logger.debug(f"Strategy '{self.id}': Received fill for non-tracked/irrelevant instrument {filled_option_id}.")
            return

        pos_info = self.active_positions[filled_option_id]

        # Ensure this fill belongs to the current strategy instance if your EventManager doesn't filter by strategy_id
        # if event.strategy_id and event.strategy_id != self.id:
        #     return

        self.logger.info(f"Strategy '{self.id}': Received FillEvent for {filled_option_id}. "
                         f"Side: {event.order_side}, Qty: {event.fill_quantity}@{event.fill_price}, "
                         f"Order ID: {event.order_id}, Status: {event.status}")

        if event.order_side == OrderSide.BUY and event.fill_quantity > 0: # Entry fill
            pos_info['actual_entry_price'] = event.fill_price
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) + event.fill_quantity

            # Calculate SL and TP based on actual fill price
            pos_info['stop_loss_price'] = event.fill_price * (1 - self.stop_loss_percent / 100)
            pos_info['target_price'] = event.fill_price * (1 + self.target_percent / 100)

            if event.fill_quantity < pos_info.get('intended_quantity', self.quantity): # Assuming intended_quantity was stored or use self.quantity
                self.test_statistics['partial_fills_on_entry'] += 1
                self.logger.warning(f"Strategy '{self.id}': PARTIAL ENTRY FILL for {filled_option_id}. "
                                   f"Filled: {event.fill_quantity} / {self.quantity} @ {event.fill_price:.2f}")
            else: # Full fill or final part of partial fill for entry
                self.test_statistics['successful_entries'] += 1
                self.logger.info(f"Strategy '{self.id}': Test ENTRY COMPLETED for {filled_option_id}. "
                                 f"Price: {event.fill_price:.2f}, SL: {pos_info['stop_loss_price']:.2f}, "
                                 f"TP: {pos_info['target_price']:.2f}, Total Filled: {pos_info['filled_quantity']}")

            # Subscribe to live ticks for the newly acquired option if not already
            if pos_info['instrument_object']:
                self.request_market_data_subscription(pos_info['instrument_object'])
                self.logger.debug(f"Strategy '{self.id}': Requested market data for {filled_option_id} after entry.")

        elif event.order_side == OrderSide.SELL and event.fill_quantity > 0: # Exit fill
            # Reduce filled quantity from position; if it reaches zero, position is closed.
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) - event.fill_quantity
            
            exit_reason = event.data.get('reason', 'Unknown Exit Reason') # Assuming reason is passed in signal data
            pnl_per_share = event.fill_price - pos_info.get('actual_entry_price', 0) # Simple PnL calculation
            total_pnl = pnl_per_share * event.fill_quantity # For this part of the fill

            self.logger.info(f"Strategy '{self.id}': Test EXIT FILL for {filled_option_id}. "
                             f"Price: {event.fill_price:.2f}, Qty: {event.fill_quantity}, PnL (this fill): {total_pnl:.2f}, Reason: {exit_reason}")

            if pos_info['filled_quantity'] <= 0: # Position fully exited
                self.test_statistics['successful_exits'] += 1
                self._handle_exit_cleanup(filled_option_id, event.timestamp) # Pass current timestamp
                self.logger.info(f"Strategy '{self.id}': Position {filled_option_id} fully exited and cleaned up.")
            else:
                self.logger.info(f"Strategy '{self.id}': PARTIAL EXIT FILL for {filled_option_id}. "
                                 f"Remaining Qty: {pos_info['filled_quantity']}")
        
        # Update portfolio manager about the fill (if not handled by a central component)
        # self.portfolio_manager.update_position_on_fill(event)


    def _check_option_exit_conditions(self, option_instrument_id: str, current_price: float, current_timestamp: float) -> None:
        """
        Checks Stop-Loss (SL) and Target Profit (TP) conditions for an active option position.
        """
        if option_instrument_id not in self.active_positions:
            return

        pos_info = self.active_positions[option_instrument_id]
        if pos_info.get('actual_entry_price') is None or pos_info.get('filled_quantity', 0) <= 0:
            # Position not properly entered or already exited
            return

        sl_price = pos_info.get('stop_loss_price')
        tp_price = pos_info.get('target_price')
        exit_reason: Optional[str] = None

        if sl_price is not None and current_price <= sl_price:
            exit_reason = f"Test Stop-Loss hit: Price {current_price:.2f} <= SL {sl_price:.2f}"
            self.test_statistics['stop_loss_exits'] += 1
        elif tp_price is not None and current_price >= tp_price:
            exit_reason = f"Test Target-Profit hit: Price {current_price:.2f} >= TP {tp_price:.2f}"
            self.test_statistics['target_profit_exits'] += 1

        if exit_reason:
            self.logger.info(f"Strategy '{self.id}': {exit_reason} for {option_instrument_id}.")
            self._exit_position(option_instrument_id, exit_reason, current_timestamp)

    def _check_time_based_test_exit(self, option_instrument_id: str, current_timestamp: float) -> None:
        """
        Implements a test-specific time-based exit (e.g., exit after X minutes).
        """
        if option_instrument_id not in self.active_positions:
            return

        pos_info = self.active_positions[option_instrument_id]
        if pos_info.get('actual_entry_price') is None or pos_info.get('filled_quantity', 0) <= 0:
            return # Not a valid, open position

        entry_ts: Optional[float] = pos_info.get('entry_timestamp')
        if not entry_ts:
            return # Should not happen if position is active

        time_elapsed_seconds = current_timestamp - entry_ts
        if time_elapsed_seconds >= (self.test_exit_minutes * 60):
            reason = f"Test Time-Based Exit after {self.test_exit_minutes} mins"
            self.logger.info(f"Strategy '{self.id}': {reason} for {option_instrument_id}.")
            self.test_statistics['time_based_test_exits'] += 1
            self._exit_position(option_instrument_id, reason, current_timestamp)

    def _exit_position(self, option_instrument_id: str, reason: str, current_timestamp: float) -> None:
        """
        Generates a SELL signal to exit an active option position.
        """
        if option_instrument_id not in self.active_positions:
            self.logger.warning(f"Strategy '{self.id}': Attempted to exit {option_instrument_id} but not in active_positions.")
            return

        pos_info = self.active_positions[option_instrument_id]
        instrument_obj = pos_info.get('instrument_object')
        quantity_to_sell = pos_info.get('filled_quantity', 0) # Sell the currently held quantity

        if not instrument_obj or quantity_to_sell <= 0:
            self.logger.error(f"Strategy '{self.id}': Cannot exit {option_instrument_id} - missing instrument or zero quantity.")
            # Potentially remove from active_positions if it's in a bad state
            if quantity_to_sell <= 0 and option_instrument_id in self.active_positions:
                 self._handle_exit_cleanup(option_instrument_id, current_timestamp, "Invalid quantity for exit")
            return

        self.logger.info(f"Strategy '{self.id}': Generating SELL signal for {option_instrument_id} "
                         f"(Qty: {quantity_to_sell}). Reason: {reason}")

        signal_data = {
            'quantity': quantity_to_sell,
            'order_type': self.execution_params.get('order_type', 'MARKET'),
            'product_type': self.execution_params.get('product_type', 'INTRADAY'),
            'reason': reason,
            'test_exit_flag': True
        }
        self.generate_signal(
            instrument=instrument_obj,
            signal_type=SignalType.SELL, # Sell the option
            data=signal_data
        )
        # Position is removed from active_positions in _handle_exit_cleanup after fill confirmation.
        # However, to prevent re-entry attempts before fill, mark it as 'exit_pending'
        pos_info['exit_pending'] = True


    def _handle_exit_cleanup(self, option_instrument_id: str, current_timestamp: float, reason: str = "Position exited") -> None:
        """
        Cleans up internal state after a position is confirmed exited (e.g., via FillEvent).
        Initiates cooldown for the underlying.
        """
        if option_instrument_id in self.active_positions:
            pos_info = self.active_positions.pop(option_instrument_id) # Remove from active
            underlying_id = pos_info.get('underlying_instrument_id')

            if underlying_id:
                self.trade_cooldown[underlying_id] = current_timestamp + self.cooldown_period_seconds
                self.logger.info(f"Strategy '{self.id}': Position {option_instrument_id} exited and removed. "
                                 f"Reason: {reason}. Cooldown for {underlying_id} until "
                                 f"{datetime.fromtimestamp(self.trade_cooldown[underlying_id]).time()}.")
            else:
                self.logger.warning(f"Strategy '{self.id}': Position {option_instrument_id} exited but no underlying_id found in pos_info.")
            
            # Unsubscribe from option's market data if no longer needed
            if pos_info.get('instrument_object'):
                self.request_market_data_unsubscription(pos_info['instrument_object'])
                self.logger.debug(f"Strategy '{self.id}': Unsubscribed from market data for {option_instrument_id}.")
        else:
            self.logger.warning(f"Strategy '{self.id}': Attempted to cleanup non-existent position {option_instrument_id}")


    def _exit_all_positions(self, reason: str, timestamp: float) -> None:
        """Exits all currently active option positions held by this strategy."""
        if not self.active_positions:
            self.logger.info(f"Strategy '{self.id}': No active test positions to exit for reason: {reason}.")
            return

        self.logger.info(f"Strategy '{self.id}': Exiting all {len(self.active_positions)} active test positions. Reason: {reason}")
        # Iterate over a copy of keys as dictionary might change during iteration
        for opt_id in list(self.active_positions.keys()):
            if not self.active_positions[opt_id].get('exit_pending', False): # Avoid sending duplicate exit orders
                self._exit_position(opt_id, reason, timestamp)
                if reason == "End of Test Day Square Off": # Specific stat for EOD exits
                    self.test_statistics['eod_exits'] +=1


    def _log_test_statistics(self) -> None:
        """Logs the collected test statistics at the end or at intervals."""
        stats = self.test_statistics
        self.logger.info(f"========== Test Strategy Statistics for '{self.id}' ==========")
        for key, value in stats.items():
            self.logger.info(f"{key.replace('_', ' ').capitalize()}: {value}")

        # Calculate and log success/failure rates if applicable
        total_attempts = stats['calls_attempted'] + stats['puts_attempted']
        if total_attempts > 0:
            entry_success_rate = (stats['successful_entries'] / total_attempts) * 100
            self.logger.info(f"Entry Attempt Success Rate: {entry_success_rate:.2f}% ({stats['successful_entries']}/{total_attempts})")

        if stats['signals_generated'] > 0: # Signals that led to an order
             actual_entry_rate = (stats['successful_entries'] / stats['signals_generated']) * 100
             self.logger.info(f"Signal to Entry Rate: {actual_entry_rate:.2f}% ({stats['successful_entries']}/{stats['signals_generated']})")


        if stats['successful_entries'] > 0:
            exit_rate = (stats['successful_exits'] / stats['successful_entries']) * 100
            self.logger.info(f"Position Exit Rate (of entered): {exit_rate:.2f}% ({stats['successful_exits']}/{stats['successful_entries']})")
        self.logger.info(f"==================== End Test Statistics ====================")

    def on_stop(self) -> None:
        """Called when the strategy is stopped (e.g., end of trading day or backtest)."""
        super().on_stop()
        current_time = datetime.now().timestamp() # Or get from framework if available
        self.logger.info(f"Strategy '{self.id}' (SimpleTestOptionStrategy) stopping. Finalizing test operations.")

        # Ensure all positions are exited
        self._exit_all_positions(reason="Strategy Stop Signal", timestamp=current_time)

        # Log final statistics
        self._log_test_statistics()

        # Clean up resources
        self.data_store.clear()
        self.active_positions.clear() # Should be empty if _exit_all_positions worked
        self.trade_cooldown.clear()
        self.last_signal_times.clear()
        self.signal_counter.clear()
        self.entry_enabled = False

        self.logger.info(f"Test strategy '{self.id}' (SimpleTestOptionStrategy) stopped and cleaned up.")

    # --- Properties for BaseStrategy compatibility (if needed) ---
    @property
    def min_history_bars_required(self) -> int:
        """Minimum number of historical bars required for indicators."""
        # Based on SMA period, can be expanded if other indicators are added
        return self.sma_period + 5 # A small buffer

    # Add any other methods or properties required by your BaseStrategy or framework

