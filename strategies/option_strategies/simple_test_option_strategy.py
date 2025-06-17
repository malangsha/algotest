"""
Simple Test Option Strategy

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
- Includes an intraday data recovery mechanism to handle service restarts.

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

import pytz
import pandas as pd
import numpy as np
import time

from datetime import datetime, date, time as dt_time, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union, Set
from collections import defaultdict
from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, FillEvent, OrderEvent, ExecutionEvent
from models.instrument import Instrument, InstrumentType, AssetClass
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide, OrderStatus
from strategies.strategy_registry import StrategyRegistry

@StrategyRegistry.register('simple_test_option_strategy')
class SimpleTestOptionStrategy(OptionStrategy):
    """
    Enhanced Simple Test Option Strategy with proper timestamp handling and real-time logging.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager: Any, option_manager: Any, portfolio_manager: Any,
                 event_manager: Any, broker: Optional[Any] = None, strategy_manager: Optional[Any] = None):
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager, broker, strategy_manager)

        # Initialize IST timezone for consistent timestamp handling
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        
        self.params: Dict[str, Any] = config.get('parameters', {})
        self.execution_params: Dict[str, Any] = config.get('execution', {})
        self.risk_params: Dict[str, Any] = config.get('risk', {})

        self.entry_time_str: str = self._get_param('entry_time', '09:20:00', self.params)
        self.exit_time_str: str = self._get_param('exit_time', '15:15:00', self.params)
        self.entry_time: dt_time = self._parse_time(self.entry_time_str, dt_time(9, 20, 0))
        self.exit_time: dt_time = self._parse_time(self.exit_time_str, dt_time(15, 15, 0))

        self.stop_loss_percent: float = self._get_param('stop_loss_percent', 5.0, self.params, (0, 100))
        self.target_percent: float = self._get_param('target_percent', 10.0, self.params, (0, 1000))
        self.quantity: int = self._get_param('quantity', 1, self.execution_params, (1, 1000))        

        self.test_interval_minutes: int = self._get_param('test_interval_minutes', 15, self.params, (1, 120))
        self.sma_period: int = self._get_param('sma_period', 5, self.params, (2, 200))
        self.max_positions_per_underlying: int = self._get_param('max_positions_per_underlying', 2, self.risk_params, (1, 10))
        self.test_exit_minutes: int = self._get_param('test_exit_minutes', 30, self.params, (1, 180))
        self.cooldown_period_seconds: int = self._get_param('cooldown_period_seconds', 60, self.params, (0, 3600))

        self.strategy_specific_underlyings_config: List[Dict[str, Any]] = config.get("underlyings", [])
        self.underlying_instrument_ids: List[str] = []
        self.underlying_details: Dict[str, Dict[str, Any]] = {}
        self._configure_underlyings()

        self.expiry_offset: int = config.get('expiry_offset', 0)

        self.data_store: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.active_positions: Dict[str, Dict[str, Any]] = {}
        self.entry_enabled: bool = False
        self.last_signal_times: Dict[str, float] = {}
        self.signal_counter: Dict[str, int] = {}
        self.trade_cooldown: Dict[str, float] = {}

        self.test_statistics: Dict[str, Any] = self._reset_test_statistics()
        self.recovery_attempted: Dict[str, Set[str]] = defaultdict(set)

        self.logger.info(f"Strategy '{self.id}' (SimpleTestOptionStrategy) initialized. "
                         f"Underlyings: {self.underlying_instrument_ids}, "
                         f"Test Interval: {self.test_interval_minutes}min, SMA: {self.sma_period}, "
                         f"Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, TP: {self.target_percent}%")

    def _get_param(self, key: str, default: Any, source_dict: Dict[str, Any],
                   value_range: Optional[Tuple[Any, Any]] = None) -> Any:
        value = source_dict.get(key)
        if value is None:
            self.logger.warning(f"Strategy '{self.id}': Parameter '{key}' not found. Using default: {default}.")
            return default
        if not isinstance(value, type(default)):
            try:
                if isinstance(default, float) and isinstance(value, int): value = float(value)
                elif isinstance(default, int) and isinstance(value, float) and value.is_integer(): value = int(value)
                else: raise TypeError
            except (TypeError, ValueError):
                self.logger.error(f"Strategy '{self.id}': Param '{key}' type {type(value).__name__} != expected {type(default).__name__}. Using default: {default}.")
                return default
        if value_range and not (value_range[0] <= value <= value_range[1]):
            self.logger.error(f"Strategy '{self.id}': Param '{key}' value {value} outside range [{value_range[0]}, {value_range[1]}]. Using default: {default}.")
            return default
        return value

    def _parse_time(self, time_str: str, default_time: dt_time) -> dt_time:
        try: return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError) as e:
            self.logger.error(f"Strategy '{self.id}': Invalid time format '{time_str}'. Error: {e}. Using default: {default_time}.")
            return default_time

    def _configure_underlyings(self) -> None:
        if not self.strategy_specific_underlyings_config:
            self.logger.warning(f"Strategy '{self.id}': No underlyings configured.")
            return
        for u_conf in self.strategy_specific_underlyings_config:
            name: Optional[str] = u_conf.get("name")
            exchange_str: Optional[str] = u_conf.get("spot_exchange")
            if not name or not exchange_str:
                self.logger.warning(f"Strategy '{self.id}': Skipping underlying config missing 'name' or 'spot_exchange': {u_conf}")
                continue
            is_index: bool = u_conf.get("index", False)
            inst_type_str: str = u_conf.get("instrument_type", "IND" if is_index else "EQ").upper()
            asset_cls_str: str = u_conf.get("asset_class", "INDEX" if is_index else "EQUITY").upper()
            option_exchange_str: str = u_conf.get("option_exchange", "NFO").upper()
            instrument_id = f"{exchange_str.upper()}:{name.upper()}"
            self.underlying_instrument_ids.append(instrument_id)
            self.underlying_details[instrument_id] = {
                "name": name.upper(), "exchange": exchange_str.upper(),
                "instrument_type": inst_type_str, "asset_class": asset_cls_str,
                "option_exchange": option_exchange_str
            }
            self.logger.info(f"Strategy '{self.id}': Configured underlying: {instrument_id}")

    def _reset_test_statistics(self) -> Dict[str, Any]:
        return {
            'signals_generated': 0, 'calls_attempted': 0, 'puts_attempted': 0,
            'successful_entries': 0, 'successful_exits': 0, 'partial_fills_on_entry': 0,
            'failed_option_fetches': 0, 'failed_orders_sent_to_broker': 0,
            'stop_loss_exits': 0, 'target_profit_exits': 0,
            'time_based_test_exits': 0, 'eod_exits': 0,
            'rejected_orders_handled': 0, 'cancelled_orders_handled': 0
        }

    def _timestamp_to_ist_string(self, timestamp: float) -> str:
        """Convert epoch timestamp to IST time string in HH:MM format"""
        try:
            dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            dt_ist = dt_utc.astimezone(self.ist_tz)
            return dt_ist.strftime('%H:%M')
        except Exception as e:
            self.logger.error(f"Error converting timestamp {timestamp} to IST: {e}")
            return f"INVALID_TS:{timestamp}"

    def _timestamp_to_ist_datetime(self, timestamp: float) -> datetime:
        """Convert epoch timestamp to IST datetime object"""
        try:
            dt_utc = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            return dt_utc.astimezone(self.ist_tz)
        except Exception as e:
            self.logger.error(f"Error converting timestamp {timestamp} to IST datetime: {e}")
            return datetime.now(self.ist_tz)

    def _validate_timestamp(self, timestamp: float, context: str = "") -> bool:
        """Validate that timestamp is within reasonable market hours (IST)"""
        try:
            dt_ist = self._timestamp_to_ist_datetime(timestamp)
            current_time = dt_ist.time()
            
            # Check if within extended market hours (9:00 AM to 4:00 PM IST)
            if dt_time(9, 0) <= current_time <= dt_time(16, 0):
                return True
            else:
                self.logger.warning(f"Timestamp validation failed for {context}: {current_time} is outside market hours")
                return False
        except Exception as e:
            self.logger.error(f"Error validating timestamp {timestamp} for {context}: {e}")
            return False

    def initialize(self) -> None:
        super().initialize()
        self.test_statistics = self._reset_test_statistics()
        self.logger.info(f"Strategy '{self.id}' (SimpleTestOptionStrategy) custom initialized. Stats reset.")

    def on_start(self) -> None:
        super().on_start()
        self.logger.info(f"Strategy '{self.id}' starting. Requesting subscriptions and loading historical data.")
        self.active_positions.clear()
        self.entry_enabled = False
        self.recovery_attempted.clear()

        self._load_initial_historical_data()

        for instrument_id, details in self.underlying_details.items():
            if instrument_id not in self.data_store:
                self.data_store[instrument_id] = {
                    tf: pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'sma'])
                    for tf in (self.all_timeframes if self.all_timeframes else {"1m"})
                }

            self.trade_cooldown[instrument_id] = 0.0
            self.last_signal_times[instrument_id] = 0.0
            self.signal_counter[instrument_id] = 0

            self.request_symbol(
                symbol_name=details["name"], 
                exchange_value=details["exchange"],
                instrument_type_value=details["instrument_type"], 
                asset_class_value=details["asset_class"],
                timeframes_to_subscribe=self.all_timeframes if self.all_timeframes else {"1m"}
            )

        self.logger.info(f"Strategy '{self.id}' started successfully.")
    
    def _load_initial_historical_data(self) -> None:
        """Load historical data from market opening time (9:15 AM IST) for all configured underlyings."""
        current_dt = datetime.now(self.ist_tz)
        market_open_time = datetime.combine(current_dt.date(), dt_time(9, 15)).replace(tzinfo=self.ist_tz)

        if current_dt.time() < dt_time(9, 15):
            self.logger.info(f"Strategy '{self.id}': Market not yet open. Skipping historical data load.")
            return

        self.logger.info(f"Strategy '{self.id}': Loading historical data from market opening ({market_open_time}) to current time ({current_dt})")

        for instrument_id, details in self.underlying_details.items():
            try:
                primary_tf = self.timeframe if hasattr(self, 'timeframe') else '1m'

                # Get historical data from data_manager (returns DataFrame with timestamp_hhmm index)
                historical_df = self.data_manager.get_historical_data(
                    symbol=instrument_id,
                    timeframe=primary_tf,
                    start_time=market_open_time,
                    end_time=current_dt
                )

                if historical_df is not None and not historical_df.empty:
                    if instrument_id not in self.data_store:
                        self.data_store[instrument_id] = {}

                    processed_df = self._process_historical_data(historical_df, current_dt.date())

                    if processed_df is not None and not processed_df.empty:
                        self.data_store[instrument_id][primary_tf] = processed_df.copy()
                        self._calculate_simple_indicators(instrument_id, primary_tf)
                        self._print_loaded_data(instrument_id, primary_tf)
                        self.logger.info(f"Strategy '{self.id}': Loaded {len(processed_df)} historical bars for {instrument_id}")
                    else:
                        self.logger.warning(f"Strategy '{self.id}': No valid processed data for {instrument_id}")
                else:
                    self.logger.warning(f"Strategy '{self.id}': No historical data returned for {instrument_id}")

            except Exception as e:
                self.logger.error(f"Strategy '{self.id}': Error loading historical data for {instrument_id}: {e}", exc_info=True)

    def _process_historical_data(self, df: pd.DataFrame, trading_date: date) -> Optional[pd.DataFrame]:
        """Process historical data DataFrame with proper timestamp conversion from HH:MM format."""
        try:
            processed_df = df.copy()

            # Handle the specific case where both index and column are named 'timestamp'
            # This is the root cause of the ambiguity error
            if hasattr(processed_df.index, 'name') and processed_df.index.name == 'timestamp':
                if 'timestamp' in processed_df.columns:
                    # Drop the timestamp column since we have the same data in index
                    processed_df = processed_df.drop('timestamp', axis=1)
                    self.logger.info("Dropped duplicate timestamp column to resolve ambiguity")

                # Reset index to convert timestamp index to column
                processed_df = processed_df.reset_index()
                self.logger.info("Reset index to convert timestamp index to column")

            # Check if data has timestamp_hhmm as index (from parquet file)
            elif hasattr(processed_df.index, 'name') and processed_df.index.name == 'timestamp_hhmm':
                # Convert HH:MM index to epoch timestamps
                timestamp_list = []
                for time_str in processed_df.index:
                    try:
                        # Parse HH:MM format and create datetime for trading_date
                        hour, minute = map(int, time_str.split(':'))
                        dt_ist = datetime.combine(trading_date, dt_time(hour, minute)).replace(tzinfo=self.ist_tz)
                        epoch_timestamp = dt_ist.timestamp()
                        timestamp_list.append(epoch_timestamp)
                    except (ValueError, TypeError) as e:
                        self.logger.error(f"Error parsing time '{time_str}': {e}")
                        continue
                    
                # Create new DataFrame with timestamp column
                processed_df = processed_df.reset_index()
                processed_df['timestamp'] = timestamp_list

                # Log the conversion for debugging
                self.logger.info(f"Converted {len(timestamp_list)} timestamps from HH:MM format to epoch")

            # Now handle timestamp column conversion
            if 'timestamp' in processed_df.columns:
                # Handle case where timestamp column already exists but might be in different format
                timestamp_list = []
                for ts in processed_df['timestamp']:
                    try:
                        # Check if timestamp is already in epoch format
                        if isinstance(ts, (int, float)):
                            timestamp_list.append(float(ts))
                        # Handle pandas Timestamp objects (most common case from your error)
                        elif hasattr(ts, 'timestamp'):
                            timestamp_list.append(ts.timestamp())
                        # Handle string timestamps
                        elif isinstance(ts, str):
                            # Try to parse as datetime string
                            dt_parsed = pd.to_datetime(ts)
                            if dt_parsed.tz is None:
                                # Assume IST if no timezone
                                dt_parsed = dt_parsed.tz_localize(self.ist_tz)
                            timestamp_list.append(dt_parsed.timestamp())
                        else:
                            # Try to convert to pandas timestamp then to epoch
                            dt_parsed = pd.to_datetime(ts)
                            if dt_parsed.tz is None:
                                dt_parsed = dt_parsed.tz_localize(self.ist_tz)
                            timestamp_list.append(dt_parsed.timestamp())
                    except Exception as e:
                        self.logger.error(f"Error converting timestamp {ts}: {e}")
                        continue
                    
                processed_df['timestamp'] = timestamp_list
                self.logger.info(f"Converted {len(timestamp_list)} timestamps to epoch format")

            else:
                self.logger.error("DataFrame from data_manager is missing 'timestamp' data.")
                return None

            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in processed_df.columns]
            if missing_cols:
                self.logger.error(f"Missing required columns in historical data: {missing_cols}")
                return None

            # Validate timestamps
            valid_rows = []
            for idx, row in processed_df.iterrows():
                ts = row['timestamp']
                if isinstance(ts, (int, float)) and not pd.isna(ts):
                    try:
                        # Test if timestamp is valid by converting to datetime
                        test_dt = datetime.fromtimestamp(ts, tz=self.ist_tz)
                        valid_rows.append(idx)
                    except (ValueError, OSError) as e:
                        self.logger.warning(f"Invalid timestamp at row {idx}: {ts} - {e}")
                else:
                    self.logger.warning(f"Invalid timestamp type at row {idx}: {type(ts)} - {ts}")

            if not valid_rows:
                self.logger.error("No valid timestamps found in historical data")
                return None

            # Keep only valid rows
            processed_df = processed_df.loc[valid_rows]

            final_df = processed_df[required_cols].copy()
            final_df['sma'] = np.nan

            # Sort by timestamp (no ambiguity now since we have clean DataFrame)
            final_df = final_df.sort_values('timestamp').reset_index(drop=True)

            return final_df

        except Exception as e:
            self.logger.error(f"Error processing historical data: {e}", exc_info=True)
            return None

    def _print_loaded_data(self, instrument_id: str, timeframe: str) -> None:
        """Print all loaded historical data with properly formatted IST timestamps in HH:MM format."""
        if instrument_id in self.data_store and timeframe in self.data_store[instrument_id]:
            df = self.data_store[instrument_id][timeframe]
            if not df.empty:
                self.logger.info(f"=== LOADED HISTORICAL DATA for {instrument_id} @ {timeframe} ===")

                df_display = df.copy()
                if 'timestamp' in df_display.columns:
                    # Convert epoch timestamps back to HH:MM format for display
                    df_display['time_hhmm'] = df_display['timestamp'].apply(
                        lambda ts: self._safe_timestamp_to_ist_datetime(ts).strftime('%H:%M') if pd.notna(ts) else 'N/A'
                    )

                # Display in HH:MM format (matching your parquet file format)
                display_cols = ['time_hhmm', 'open', 'high', 'low', 'close', 'volume']
                available_cols = [col for col in display_cols if col in df_display.columns]

                if available_cols:
                    df_to_show = df_display[available_cols]
                    # Set time_hhmm as index for display (matching original format)
                    if 'time_hhmm' in df_to_show.columns:
                        df_to_show = df_to_show.set_index('time_hhmm')
                else:
                    df_to_show = df_display[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
                    self.logger.info(f"\n{df_to_show}")

                self.logger.info(f"=== END DATA for {instrument_id} @ {timeframe} ({len(df)} rows) ===")


    def on_market_data(self, event: MarketDataEvent) -> None:
        if not event.instrument or not event.instrument.instrument_id or not event.data: return
        option_instrument_id: str = event.instrument.instrument_id
        if option_instrument_id in self.active_positions:
            ltp_data = event.data.get(MarketDataType.LAST_PRICE.value)            
            if ltp_data is not None:
                try:
                    current_option_price: float = float(ltp_data)
                    self._check_option_exit_conditions(option_instrument_id, current_option_price, event.timestamp)
                    self._check_time_based_test_exit(option_instrument_id, event.timestamp)
                except (ValueError, TypeError):
                    self.logger.warning(f"Strategy '{self.id}': Invalid option price '{ltp_data}' for {option_instrument_id}.")

    def on_bar(self, event: BarEvent) -> None:        
        if not event.instrument or not event.instrument.instrument_id: return
        
        underlying_instrument_id: str = event.instrument.instrument_id
        timeframe: str = event.timeframe
        
        # Validate timestamp first
        if not self._validate_timestamp(event.timestamp, f"bar_event_{underlying_instrument_id}"):
            self.logger.warning(f"Skipping bar with invalid timestamp: {event.timestamp}")
            return
            
        # Real-time logging of incoming bar with formatted timestamp
        bar_time_ist = self._timestamp_to_ist_string(event.timestamp)
        self.logger.info(f"📊 INCOMING BAR [{bar_time_ist}] {underlying_instrument_id}@{timeframe}: "
                        f"O={event.open_price:.2f} H={event.high_price:.2f} L={event.low_price:.2f} "
                        f"C={event.close_price:.2f} V={event.volume}")
        
        if underlying_instrument_id not in self.underlying_instrument_ids or timeframe not in self.data_store.get(underlying_instrument_id, {}):
            return
            
        current_dt: datetime = self._timestamp_to_ist_datetime(event.timestamp)
        current_time: dt_time = current_dt.time()
            
        # Recovery Logic Trigger
        if timeframe == self.timeframe:
            df = self.data_store[underlying_instrument_id][timeframe]
            is_recovery_needed = df.empty and underlying_instrument_id not in self.recovery_attempted[timeframe]
            
            if is_recovery_needed and current_time >= dt_time(9, 15):
                self._handle_intraday_recovery(underlying_instrument_id, timeframe, event.timestamp)
                self.recovery_attempted[timeframe].add(underlying_instrument_id)

        self._update_data_store(underlying_instrument_id, timeframe, event)
        self._calculate_simple_indicators(underlying_instrument_id, timeframe)
        
        if timeframe != self.timeframe: return

        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Strategy '{self.id}': Entry window opened at {self.entry_time_str}.")
            
        if current_time >= self.exit_time:
            if self.active_positions:
                self.logger.info(f"Strategy '{self.id}': Exit time ({self.exit_time_str}) reached. Closing positions.")
                self._exit_all_positions(reason="End of Test Day Square Off", timestamp=event.timestamp)
            if self.entry_enabled:
                self.entry_enabled = False
                self.logger.info(f"Strategy '{self.id}': Entry window closed.")
                self._log_test_statistics()
            return
            
        df = self.data_store[underlying_instrument_id][timeframe]
        if self.entry_enabled and len(df) >= self.sma_period:
            self._check_test_entry_signals(underlying_instrument_id, current_dt, event.timestamp)

    def _handle_intraday_recovery(self, instrument_id: str, timeframe: str, current_timestamp: float) -> None:
        """Handles the backfilling of intraday data with proper timestamp handling from parquet format."""
        self.logger.info(f"Initiating intraday data recovery for {instrument_id} on {timeframe}.")
        try:
            current_dt = self._safe_timestamp_to_ist_datetime(current_timestamp)
            session_start_time = datetime.combine(current_dt.date(), dt_time(9, 15)).replace(tzinfo=self.ist_tz)

            # Get backfill data (will have timestamp_hhmm index)
            backfill_df = self.data_manager.get_historical_data(
                symbol=instrument_id,
                timeframe=timeframe,
                start_time=session_start_time,
                end_time=current_dt
            )

            if backfill_df is not None and not backfill_df.empty:
                self.logger.info(f"Raw recovery data for {instrument_id}@{timeframe}:")

                # Display raw data in original HH:MM format first
                with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                    self.logger.info(f"Recovery data from parquet file:\n{backfill_df}")

                # Process the data (convert timestamp_hhmm to epoch timestamps)
                processed_recovery_df = self._process_historical_data(backfill_df, current_dt.date())

                if processed_recovery_df is not None and not processed_recovery_df.empty:
                    required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

                    if all(col in processed_recovery_df.columns for col in required_cols):
                        # Store the processed data with epoch timestamps
                        processed_recovery_df['sma'] = np.nan
                        self.data_store[instrument_id][timeframe] = processed_recovery_df.copy()

                        # Enhanced logging with HH:MM format display
                        self.logger.info(f"Successfully recovered {len(processed_recovery_df)} bars for {instrument_id}@{timeframe}:")

                        # Create display DataFrame with HH:MM format
                        display_df = processed_recovery_df.copy()
                        display_df['time_hhmm'] = display_df['timestamp'].apply(
                            lambda ts: self._safe_timestamp_to_ist_datetime(ts).strftime('%H:%M') if pd.notna(ts) else 'N/A'
                        )

                        # Display processed data in HH:MM format
                        display_cols = ['time_hhmm', 'open', 'high', 'low', 'close', 'volume']
                        display_data = display_df[display_cols].set_index('time_hhmm')

                        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                            self.logger.info(f"Processed recovery data:\n{display_data}")

                        self.logger.info(f"Recovery completed: {len(processed_recovery_df)} bars loaded")
                    else:
                        missing = [col for col in required_cols if col not in processed_recovery_df.columns]
                        self.logger.error(f"Processed recovery data missing required columns: {missing}")
                else:
                    self.logger.warning("Failed to process recovery data")
            else:
                self.logger.warning("No historical data returned for backfill")

        except Exception as e:
            self.logger.error(f"Recovery error: {e}", exc_info=True)

    def _safe_timestamp_to_ist_datetime(self, timestamp: float) -> datetime:
        """Safely convert epoch timestamp to IST datetime with error handling."""
        try:
            if pd.isna(timestamp):
                return datetime.now(self.ist_tz)
            return datetime.fromtimestamp(timestamp, tz=self.ist_tz)
        except (ValueError, OSError, TypeError) as e:
            self.logger.warning(f"Error converting timestamp {timestamp} to datetime: {e}")
            return datetime.now(self.ist_tz)        
            
    def _update_data_store(self, instrument_id: str, timeframe: str, event: BarEvent) -> None:
        """Update data store with real-time bar data and log the addition."""
        df = self.data_store[instrument_id][timeframe]
        new_row_data = {
            'timestamp': event.timestamp, 
            'open': event.open_price, 
            'high': event.high_price, 
            'low': event.low_price, 
            'close': event.close_price, 
            'volume': event.volume
        }
        
        # Avoid appending duplicate bar data
        if not df.empty and df.iloc[-1]['timestamp'] == event.timestamp:
            self.logger.debug(f"Duplicate timestamp {event.timestamp} for {instrument_id}@{timeframe}, skipping")
            return

        # Log the addition with IST timestamp
        bar_time_ist = self._timestamp_to_ist_string(event.timestamp)
        self.logger.info(f"📈 ADDED TO STORE [{bar_time_ist}] {instrument_id}@{timeframe}: "
                        f"Close={event.close_price:.2f} (Total bars: {len(df) + 1})")

        new_row_df = pd.DataFrame([new_row_data])
        self.data_store[instrument_id][timeframe] = pd.concat([df, new_row_df], ignore_index=True)
        
        # Trim data store to reasonable size
        max_len = max(self.sma_period + 50, getattr(self, 'min_history_bars_required', self.sma_period + 10) + 10)
        if len(self.data_store[instrument_id][timeframe]) > max_len:
            self.data_store[instrument_id][timeframe] = self.data_store[instrument_id][timeframe].iloc[-max_len:]

    def _calculate_simple_indicators(self, instrument_id: str, timeframe: str) -> None:
        """Calculate indicators and log when SMA becomes available."""
        df = self.data_store[instrument_id][timeframe]
        if len(df) >= self.sma_period:
            old_sma = df['sma'].iloc[-1] if not df.empty and 'sma' in df.columns else None
            df['sma'] = df['close'].rolling(window=self.sma_period, min_periods=self.sma_period).mean()
            
            # Log when SMA first becomes available or significant changes
            new_sma = df['sma'].iloc[-1]
            if pd.isna(old_sma) and not pd.isna(new_sma):
                bar_time_ist = self._timestamp_to_ist_string(df['timestamp'].iloc[-1])
                self.logger.info(f"📊 SMA({self.sma_period}) NOW AVAILABLE [{bar_time_ist}] {instrument_id}: "
                               f"Price={df['close'].iloc[-1]:.2f}, SMA={new_sma:.2f}")
        else:
            df['sma'] = np.nan

    def _check_test_entry_signals(self, underlying_instrument_id: str, current_dt: datetime, current_timestamp: float) -> None:
        last_signal_time: float = self.last_signal_times.get(underlying_instrument_id, 0.0)
        if current_timestamp < last_signal_time + (self.test_interval_minutes * 60): return
        if self._is_in_cooldown(underlying_instrument_id, current_timestamp):
            self.logger.debug(f"Strategy '{self.id}': {underlying_instrument_id} in cooldown. Skipping signal.")
            return
        active_count_for_underlying = sum(1 for pos in self.active_positions.values() if pos.get('underlying_instrument_id') == underlying_instrument_id and not pos.get('exit_pending'))
        if active_count_for_underlying >= self.max_positions_per_underlying:
            self.logger.debug(f"Strategy '{self.id}': Max positions ({self.max_positions_per_underlying}) for {underlying_instrument_id}. Skipping.")
            return
        df = self.data_store[underlying_instrument_id][self.timeframe]
        if df.empty or pd.isna(df.iloc[-1]['sma']): return
        latest_bar = df.iloc[-1]; current_price: float = latest_bar['close']; sma_value: float = latest_bar['sma']
        signal_count: int = self.signal_counter.get(underlying_instrument_id, 0)
        is_call_turn: bool = (signal_count % 2) == 0
        signal_to_generate: Optional[SignalType] = None; entry_reason: str = ""
        
        bar_time_ist = self._timestamp_to_ist_string(current_timestamp)
        
        if is_call_turn and current_price > sma_value:
            signal_to_generate = SignalType.BUY_CALL
            entry_reason = f"Test CALL Signal #{signal_count + 1} [{bar_time_ist}]: Price {current_price:.2f} > SMA({self.sma_period}) {sma_value:.2f}"
            self.test_statistics['calls_attempted'] += 1
        elif not is_call_turn and current_price < sma_value:
            signal_to_generate = SignalType.BUY_PUT
            entry_reason = f"Test PUT Signal #{signal_count + 1} [{bar_time_ist}]: Price {current_price:.2f} < SMA({self.sma_period}) {sma_value:.2f}"
            self.test_statistics['puts_attempted'] += 1
        if signal_to_generate:
            self.logger.info(f"Strategy '{self.id}': {entry_reason} for {underlying_instrument_id}.")
            self._enter_option_position(underlying_instrument_id, signal_to_generate, current_price, current_dt, current_timestamp)
            self.last_signal_times[underlying_instrument_id] = current_timestamp
            self.signal_counter[underlying_instrument_id] = signal_count + 1
            self.test_statistics['signals_generated'] += 1

    def _is_in_cooldown(self, underlying_instrument_id: str, current_timestamp: float) -> bool:
        """Check if underlying is in cooldown period after a trade."""
        return current_timestamp < self.trade_cooldown.get(underlying_instrument_id, 0.0)

    def _enter_option_position(self, underlying_instrument_id: str, signal_type: SignalType,
                               underlying_price: float, current_dt: datetime, current_timestamp: float) -> None:
        """Enter option position based on signal type with enhanced timestamp logging."""
        self.logger.debug(f"_enter_option_position: {underlying_instrument_id}, signal:{signal_type}, underlying_px:{underlying_price}")
        
        option_type_enum: OptionType = OptionType.CALL if signal_type == SignalType.BUY_CALL else OptionType.PUT
        underlying_config = self.underlying_details.get(underlying_instrument_id)
        
        if not underlying_config:
            self.logger.error(f"Strategy '{self.id}': Missing underlying details for {underlying_instrument_id}.")
            return
            
        underlying_name: str = underlying_config["name"]
        
        # Get ATM strike
        atm_strike: Optional[float] = self.option_manager.get_atm_strike(underlying_symbol=underlying_name)
        if atm_strike is None:
            strike_interval = self.option_manager.strike_intervals(underlying_name) or (50.0 if "NIFTY" in underlying_name.upper() else 100.0)
            atm_strike = round(underlying_price / strike_interval) * strike_interval
            self.logger.warning(f"Strategy '{self.id}': OM failed to give ATM for {underlying_instrument_id}. Estimated: {atm_strike:.2f}")
            
        # Get option instrument
        option_instrument: Optional[Instrument] = self.option_manager.get_option_instrument(
            underlying_symbol=underlying_name, 
            strike=atm_strike,
            option_type=option_type_enum.value, 
            expiry_offset=self.expiry_offset
        )
        
        if not option_instrument or not option_instrument.instrument_id:
            self.logger.error(f"Strategy '{self.id}': Failed to get {option_type_enum.value} option for {underlying_instrument_id} @ {atm_strike:.2f}.")
            self.test_statistics['failed_option_fetches'] += 1
            return
            
        option_instrument_id: str = option_instrument.instrument_id
        
        # Check if already active
        if option_instrument_id in self.active_positions and not self.active_positions[option_instrument_id].get('exit_pending'):
            self.logger.info(f"Strategy '{self.id}': Already active (non-pending-exit) position for {option_instrument_id}. Skipping.")
            return
            
        # Get option price
        entry_option_price: float = 0.0
        option_tick_data = self.data_manager.get_latest_tick_with_validated_price(option_instrument_id)
        if option_tick_data and option_tick_data.get('validated_price') is not None:
            entry_option_price = option_tick_data['validated_price']
            self.logger.debug(f"Strategy '{self.id}': Using validated price {entry_option_price:.2f} for {option_instrument_id}")
        else:
            # Fallback to calculated price when no valid market price available
            entry_option_price = max(0.1, underlying_price * 0.015)
            self.logger.warning(f"Strategy '{self.id}': No valid price data for {option_instrument_id}, using fallback price {entry_option_price:.2f}")
            self.test_statistics['failed_option_fetches'] += 1

        # Additional validation for option prices
        if entry_option_price <= 0:
            self.logger.error(f"Strategy '{self.id}': Option price for {option_instrument_id} is {entry_option_price:.2f}. Cannot enter.")
            self.test_statistics['failed_option_fetches'] += 1
            return

        # Generate signal with IST timestamp logging
        entry_time_ist = self._timestamp_to_ist_string(current_timestamp)
        signal_data = {
            'quantity': self.quantity,
            'order_type': self.execution_params.get('order_type', 'LIMIT'),
            'product_type': self.execution_params.get('product_type', 'INTRADAY'),
            'intended_option_type': option_type_enum.value,
            'entry_price_ref': entry_option_price,
            'underlying_price_at_signal': underlying_price,
            'atm_strike_at_signal': atm_strike,
            'test_signal_flag': True
        }
        
        self.logger.info(f"Strategy '{self.id}': 🎯 ENTRY SIGNAL [{entry_time_ist}] {option_instrument_id} ({option_type_enum.value}) "
                        f"Qty: {self.quantity}, Est. Price: {entry_option_price:.2f}, Strike: {atm_strike}")
        
        self.generate_signal(instrument_id=option_instrument_id, signal_type=SignalType.BUY, data=signal_data)
        
        # Track position
        self.active_positions[option_instrument_id] = {
            'underlying_instrument_id': underlying_instrument_id,
            'option_type': option_type_enum.value,
            'instrument_object': option_instrument,
            'intended_entry_price': entry_option_price,
            'actual_entry_price': None,
            'filled_quantity': 0,
            'stop_loss_price': None,
            'target_price': None,
            'entry_timestamp': current_timestamp,
            'test_position_flag': True,
            'exit_pending': False
        }
        
        self.logger.debug(f"Active positions updated: {list(self.active_positions.keys())}")

    def on_fill(self, event: FillEvent) -> None:
        """Handle fill events with enhanced timestamp logging."""
        super().on_fill(event)
        
        
        filled_option_id: str = event.symbol
        if not filled_option_id or filled_option_id not in self.active_positions:            
            return
            
        pos_info = self.active_positions[filled_option_id]
        fill_time_ist = self._timestamp_to_ist_string(event.timestamp)
        quantity_with_lotsize = self.quantity * pos_info['instrument_object'].lot_size
        self.logger.debug(f"quantity_with_lotsize: {quantity_with_lotsize}")
        
        self.logger.info(f"Strategy '{self.id}': 📋 FILL [{fill_time_ist}] {filled_option_id} - "
                        f"Side: {event.side}, Qty: {event.quantity}@{event.price:.2f}")
        
        if event.side == OrderSide.BUY and event.quantity > 0:
            # Entry fill
            pos_info['actual_entry_price'] = event.price
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) + event.quantity
            pos_info['stop_loss_price'] = event.price * (1 - self.stop_loss_percent / 100)
            pos_info['target_price'] = event.price * (1 + self.target_percent / 100)
            pos_info['exit_pending'] = False
            
            if event.quantity < quantity_with_lotsize:
                self.test_statistics['partial_fills_on_entry'] += 1
                self.logger.warning(f"Strategy '{self.id}': ⚠️ PARTIAL ENTRY FILL - {event.quantity}/{quantity_with_lotsize}")
            else:
                self.test_statistics['successful_entries'] += 1
                
            self.logger.info(f"Strategy '{self.id}': ✅ ENTRY COMPLETED [{fill_time_ist}] {filled_option_id} - "
                           f"Price: {event.price:.2f}, SL: {pos_info['stop_loss_price']:.2f}, "
                           f"TP: {pos_info['target_price']:.2f}")
            
            # Subscribe to option market data
            if pos_info['instrument_object']:
                self.request_symbol(
                    symbol_name=pos_info['instrument_object'].symbol,
                    exchange_value=pos_info['instrument_object'].exchange.value,
                    instrument_type_value=pos_info['instrument_object'].instrument_type.value,
                    asset_class_value=pos_info['instrument_object'].asset_class.value,
                    option_details={
                        'option_type': pos_info['instrument_object'].option_type if pos_info['instrument_object'].option_type else None,
                        'strike_price': pos_info['instrument_object'].strike,
                        'expiry_date': pos_info['instrument_object'].expiry_date,
                        'underlying_symbol_key': pos_info['instrument_object'].underlying_symbol
                    } if pos_info['instrument_object'].instrument_type == InstrumentType.OPTION else None
                )
            self.logger.debug(f"self.active_positions: {self.active_positions}")
            self.logger.debug(f"pos_info: {pos_info}")
        elif event.side == OrderSide.SELL and event.quantity > 0:
            # Exit fill
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) - event.quantity
            
            if pos_info['filled_quantity'] <= 0:
                self.test_statistics['successful_exits'] += 1
                self.logger.info(f"Strategy '{self.id}': ✅ EXIT COMPLETED [{fill_time_ist}] {filled_option_id}")
                self._handle_exit_cleanup(filled_option_id, event.timestamp)
            else:
                self.logger.info(f"Strategy '{self.id}': 🔄 PARTIAL EXIT [{fill_time_ist}] {filled_option_id} - "
                               f"Remaining: {pos_info['filled_quantity']}")

    def on_order(self, event: Union[OrderEvent, ExecutionEvent]) -> None:
        """Handle order status events with enhanced logging."""
        self.logger.debug(f"Received order event: {event}")
        super().on_order(event)
        
        order_instrument_id = event.symbol
        order_status = event.status
        order_time_ist = self._timestamp_to_ist_string(event.timestamp) if hasattr(event, 'timestamp') and event.timestamp else "N/A"
        
        # Normalize order status
        if isinstance(order_status, str):
            try:
                order_status = OrderStatus(order_status.upper())
            except ValueError:
                self.logger.warning(f"Strategy '{self.id}': Unknown order status '{event.status}'. Skipping.")
                return
        elif not isinstance(order_status, OrderStatus):
            self.logger.warning(f"Strategy '{self.id}': Invalid order status type '{type(event.status)}'. Skipping.")
            return

        if order_instrument_id in self.active_positions:
            pos_info = self.active_positions[order_instrument_id]
            underlying_id = pos_info.get('underlying_instrument_id')

            if order_status in [OrderStatus.REJECTED, OrderStatus.CANCELLED]:
                self.logger.info(f"Strategy '{self.id}': ❌ ORDER {order_status.value} [{order_time_ist}] {order_instrument_id} - "
                               f"Reason: {getattr(event, 'rejection_reason', 'N/A')}")
                
                if order_status == OrderStatus.REJECTED:
                    self.test_statistics['rejected_orders_handled'] += 1
                else:
                    self.test_statistics['cancelled_orders_handled'] += 1

                # Clean up and set cooldown
                if order_instrument_id in self.active_positions:
                    removed_pos_info = self.active_positions.pop(order_instrument_id, None)
                    
                    if removed_pos_info and underlying_id:
                        cooldown_trigger_time = getattr(event, 'timestamp', time.time())
                        self.trade_cooldown[underlying_id] = cooldown_trigger_time + self.cooldown_period_seconds
                        cooldown_end_time = self._timestamp_to_ist_string(self.trade_cooldown[underlying_id])
                        
                        self.logger.info(f"Strategy '{self.id}': 🕐 COOLDOWN SET for {underlying_id} until {cooldown_end_time} "
                                       f"(due to {order_status.value})")
                    
                    if removed_pos_info and removed_pos_info.get('instrument_object'):
                        self.request_market_data_unsubscription(removed_pos_info['instrument_object'])
                        
            elif order_status == OrderStatus.FILLED and hasattr(event, 'side') and event.side == OrderSide.BUY:
                pos_info['exit_pending'] = False
                self.logger.debug(f"Strategy '{self.id}': Entry order filled, clearing exit_pending flag for {order_instrument_id}")

    def _check_option_exit_conditions(self, option_instrument_id: str, current_price: float, current_timestamp: float) -> None:
        """Check stop loss and target profit conditions with IST logging."""
        # self.logger.debug(f"option_instrument_id: {option_instrument_id}, self.active_positions: {self.active_positions}")
        if option_instrument_id not in self.active_positions:
            return
            
        pos_info = self.active_positions[option_instrument_id]
        # self.logger.debug(f"pos_info: {pos_info}")
        
        # Skip if position not properly filled or exit already pending
        if (pos_info.get('actual_entry_price') is None or 
            pos_info.get('filled_quantity', 0) <= 0 or 
            pos_info.get('exit_pending')):
            return
            
        sl_price = pos_info.get('stop_loss_price')
        tp_price = pos_info.get('target_price')
        exit_reason: Optional[str] = None
        current_time_ist = self._timestamp_to_ist_string(current_timestamp)
        
        if sl_price is not None and current_price <= sl_price:
            exit_reason = f"SL Hit [{current_time_ist}]: Price {current_price:.2f} <= SL {sl_price:.2f}"
            self.test_statistics['stop_loss_exits'] += 1
            
        elif tp_price is not None and current_price >= tp_price:
            exit_reason = f"TP Hit [{current_time_ist}]: Price {current_price:.2f} >= TP {tp_price:.2f}"
            self.test_statistics['target_profit_exits'] += 1
            
        if exit_reason:
            self.logger.info(f"Strategy '{self.id}': 🎯 {exit_reason} for {option_instrument_id}")
            self._exit_position(option_instrument_id, exit_reason, current_timestamp)

    def _check_time_based_test_exit(self, option_instrument_id: str, current_timestamp: float) -> None:
        """Check if position should exit based on time duration."""
        if option_instrument_id not in self.active_positions:
            return
            
        pos_info = self.active_positions[option_instrument_id]
        
        # Skip if position not properly filled or exit already pending
        if (pos_info.get('actual_entry_price') is None or 
            pos_info.get('filled_quantity', 0) <= 0 or 
            pos_info.get('exit_pending')):
            return
            
        entry_ts: Optional[float] = pos_info.get('entry_timestamp')
        if not entry_ts:
            return
            
        time_elapsed_minutes = (current_timestamp - entry_ts) / 60
        
        if time_elapsed_minutes >= self.test_exit_minutes:
            current_time_ist = self._timestamp_to_ist_string(current_timestamp)
            reason = f"Time-Based Exit [{current_time_ist}]: {time_elapsed_minutes:.1f} mins >= {self.test_exit_minutes} mins"
            
            self.logger.info(f"Strategy '{self.id}': ⏰ {reason} for {option_instrument_id}")
            self.test_statistics['time_based_test_exits'] += 1
            self._exit_position(option_instrument_id, reason, current_timestamp)

    
    def _exit_position(self, option_instrument_id: str, reason: str, current_timestamp: float) -> None:
        """Exit a specific position with enhanced logging, including exit price."""
        if option_instrument_id not in self.active_positions:
            return

        pos_info = self.active_positions[option_instrument_id]

        if pos_info.get('exit_pending'):
            self.logger.debug(f"Strategy '{self.id}': Exit already pending for {option_instrument_id}. Skipping.")
            return

        instrument_obj = pos_info.get('instrument_object')
        quantity_to_sell = pos_info.get('filled_quantity', 0)
        lot_size = instrument_obj.lot_size
        quantity_to_sell = int(quantity_to_sell / lot_size)
      
        if not instrument_obj or quantity_to_sell <= 0 or quantity_to_sell % lot_size == 0:
            self.logger.error(
                f"Strategy '{self.id}': Cannot exit {option_instrument_id} - "
                f"Missing instrument or zero quantity ({quantity_to_sell})"
            )
            if quantity_to_sell <= 0:
                self._handle_exit_cleanup(option_instrument_id, current_timestamp, "Invalid quantity for exit")
            return

        # Determine exit price: use target_price if set, otherwise fetch LTP
        exit_price = pos_info.get('target_price')
        if exit_price is None:
            tick = self.data_manager.get_latest_tick_with_validated_price(option_instrument_id)
            exit_price = tick.get('validated_price') if tick and tick.get('validated_price') is not None else None

        if exit_price is None:
            # Fallback to market order if we really have no price
            order_type = 'MARKET'
            self.logger.warning(
                f"Strategy '{self.id}': No exit price available for {option_instrument_id}, using MARKET order."
            )
        else:
            order_type = 'LIMIT'

        # Mark exit as pending
        pos_info['exit_pending'] = True
        exit_time_ist = self._timestamp_to_ist_string(current_timestamp)

        self.logger.info(
            f"Strategy '{self.id}': 🔄 EXIT SIGNAL [{exit_time_ist}] {option_instrument_id} - "
            f"Qty: {quantity_to_sell}, Price: {exit_price}, Reason: {reason}"
        )

        signal_data = {
            'quantity': quantity_to_sell,
            'order_type': order_type,
            'product_type': self.execution_params.get('product_type', 'INTRADAY'),
            'reason': reason,
            'test_exit_flag': True,
        }

        if order_type == 'LIMIT':
            signal_data['price'] = exit_price

        self.generate_signal(
            instrument_id=option_instrument_id,
            signal_type=SignalType.SELL,
            data=signal_data
        )    

    def _handle_exit_cleanup(self, option_instrument_id: str, current_timestamp: float, reason: str = "Position exited") -> None:
        """Clean up after position exit with cooldown and unsubscription."""
        if option_instrument_id in self.active_positions:
            pos_info = self.active_positions.pop(option_instrument_id)
            underlying_id = pos_info.get('underlying_instrument_id')
            cleanup_time_ist = self._timestamp_to_ist_string(current_timestamp)
            
            if underlying_id:
                self.trade_cooldown[underlying_id] = current_timestamp + self.cooldown_period_seconds
                cooldown_end_time = self._timestamp_to_ist_string(self.trade_cooldown[underlying_id])
                
                self.logger.info(f"Strategy '{self.id}': 🧹 CLEANUP [{cleanup_time_ist}] {option_instrument_id} - "
                               f"{reason}. Cooldown for {underlying_id} until {cooldown_end_time}")
            
            if pos_info.get('instrument_object'):
                self.request_market_data_unsubscription(pos_info['instrument_object'])
        else:
            self.logger.warning(f"Strategy '{self.id}': Attempted cleanup for non-existent position {option_instrument_id}")

    def _exit_all_positions(self, reason: str, timestamp: float) -> None:
        """Exit all active positions with bulk logging."""
        if not self.active_positions:
            return
            
        exit_time_ist = self._timestamp_to_ist_string(timestamp)
        position_count = len(self.active_positions)
        
        self.logger.info(f"Strategy '{self.id}': 🛑 EXITING ALL POSITIONS [{exit_time_ist}] - "
                        f"Count: {position_count}, Reason: {reason}")
        
        for opt_id in list(self.active_positions.keys()):
            if not self.active_positions[opt_id].get('exit_pending'):
                self._exit_position(opt_id, reason, timestamp)
                if reason == "End of Test Day Square Off":
                    self.test_statistics['eod_exits'] += 1

    def _log_test_statistics(self) -> None:
        """Log comprehensive test statistics."""
        stats = self.test_statistics
        
        self.logger.info(f"=" * 60)
        self.logger.info(f"📊 TEST STRATEGY STATISTICS for '{self.id}'")
        self.logger.info(f"=" * 60)
        
        # Basic statistics
        for key, value in stats.items():
            formatted_key = key.replace('_', ' ').title()
            self.logger.info(f"{formatted_key:.<35} {value}")
        
        # Calculated ratios
        total_attempts = stats['calls_attempted'] + stats['puts_attempted']
        if total_attempts > 0:
            entry_success_rate = (stats['successful_entries'] / total_attempts) * 100
            self.logger.info(f"Entry Success Rate:................. {entry_success_rate:.2f}%")
        
        if stats['signals_generated'] > 0:
            signal_to_entry_rate = (stats['successful_entries'] / stats['signals_generated']) * 100
            self.logger.info(f"Signal To Entry Rate:............... {signal_to_entry_rate:.2f}%")
        
        if stats['successful_entries'] > 0:
            exit_completion_rate = (stats['successful_exits'] / stats['successful_entries']) * 100
            self.logger.info(f"Exit Completion Rate:............... {exit_completion_rate:.2f}%")
        
        self.logger.info(f"=" * 60)
        self.logger.info(f"📈 END TEST STATISTICS")
        self.logger.info(f"=" * 60)

    def request_market_data_unsubscription(self, instrument: Union[Instrument, str]) -> None:
        """Request unsubscription from market data for an instrument."""
        instrument_id_str = instrument.instrument_id if isinstance(instrument, Instrument) else instrument
        
        if self.strategy_manager and hasattr(self.strategy_manager, 'unregister_dynamic_subscription'):
            self.logger.info(f"Strategy '{self.id}': 📡 UNSUBSCRIBING from market data: {instrument_id_str}")
            # Actual unsubscription would be implemented here
        else:
            self.logger.warning(f"Strategy '{self.id}': Placeholder unsubscription call for {instrument_id_str}")

    def on_stop(self) -> None:
        """Stop strategy with complete cleanup and statistics."""
        super().on_stop()
        
        current_time = time.time()
        stop_time_ist = self._timestamp_to_ist_string(current_time)
        
        self.logger.info(f"Strategy '{self.id}': 🛑 STOPPING [{stop_time_ist}] - Finalizing test operations")
        
        # Exit all positions
        self._exit_all_positions(reason="Strategy Stop Signal", timestamp=current_time)
        
        # Log final statistics
        self._log_test_statistics()
        
        # Complete cleanup
        self.data_store.clear()
        self.active_positions.clear()
        self.trade_cooldown.clear()
        self.last_signal_times.clear()
        self.signal_counter.clear()
        self.entry_enabled = False
        
        self.logger.info(f"Strategy '{self.id}': ✅ STOPPED AND CLEANED UP [{stop_time_ist}]")

    @property
    def min_history_bars_required(self) -> int:
        """Return minimum number of historical bars required for strategy operation."""
        return self.sma_period + 5
