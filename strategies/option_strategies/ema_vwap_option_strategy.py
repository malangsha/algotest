"""
EMA VWAP Option Strategy (Refactored)

This strategy buys Call or Put options based on EMA crossovers, VWAP conditions,
and price retracements, following the structural patterns of HighFrequencyMomentumOptionStrategy.

- Bar Subscription:
  - Primary Timeframe (e.g., 1m): Used for retracement confirmation and entry trigger.
  - Context Timeframe (e.g., 5m, from additional_timeframes): Used for EMA crossover and VWAP context.
- Underlyings: Configurable (e.g., NIFTY, SENSEX).

- Entry Logic (Call):
  1. Context (Context Timeframe):
     - Fast EMA crosses above Slow EMA.
     - Price is above VWAP.
     - Sets a "Bullish" context and flags "CE Retracement Pending".
  2. Trigger (Primary Timeframe, if CE Retracement Pending):
     - Price retraces to touch or dip below the Slow EMA.
     - Price then closes above the Slow EMA.
  3. Action: Buy an ATM Call option.

- Entry Logic (Put):
  1. Context (Context Timeframe):
     - Fast EMA crosses below Slow EMA.
     - Price is below VWAP.
     - Sets a "Bearish" context and flags "PE Retracement Pending".
  2. Trigger (Primary Timeframe, if PE Retracement Pending):
     - Price retraces to touch or rise above the Slow EMA.
     - Price then closes below the Slow EMA.
  3. Action: Buy an ATM Put option.

- Risk Management (defined in strategy, executed by framework):
  - Stop-loss: Percentage of option entry premium.
  - Target Profit: Percentage of option entry premium.
  - Position Sizing: Base quantity defined; actual sizing by framework.

- Exit Logic (for an active option position):
  1. Target profit hit (based on option price).
  2. Stop-loss hit (based on option price).
  3. End of trading day (all positions squared off).
  4. Cooldown: After an exit, a cooldown period is initiated for the specific underlying to prevent immediate re-entry.
"""

from datetime import datetime, time as dt_time
from typing import Dict, List, Any, Optional, Set
import pandas as pd
import numpy as np

from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, FillEvent
from models.instrument import Instrument, InstrumentType, AssetClass
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide, Exchange
from strategies.strategy_registry import StrategyRegistry
from core.logging_manager import get_logger # Assuming get_logger is available

@StrategyRegistry.register('ema_vwap_option_strategy')
class EmaVwapOptionStrategy(OptionStrategy):
    """
    Refactored EMA VWAP Option Strategy.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager,
                 event_manager, broker=None, strategy_manager=None):
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager, broker, strategy_manager)

        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:30:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.fast_ema_period = self.params.get('fast_ema', 5) # Renamed from ema_short_period
        self.slow_ema_period = self.params.get('slow_ema', 21) # Renamed from ema_long_period
        self.retracement_percent = self.params.get('retracement_percent', 60) # Retracement threshold
        self.stop_loss_percent = self.params.get('stop_loss_percent', 40)
        self.target_percent = self.params.get('target_percent', 50)

        self.quantity = config.get('execution', {}).get('quantity', 1)
        self.order_type = config.get('execution', {}).get('order_type', 'MARKET')

        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        # Process underlying configurations
        self.strategy_specific_underlyings_config = config.get("underlyings", [])
        self.underlying_instrument_ids: List[str] = []
        self.underlying_details: Dict[str, Dict[str, Any]] = {}

        for u_conf in self.strategy_specific_underlyings_config:
            name = u_conf.get("name")
            exchange_str = u_conf.get("spot_exchange")
            inst_type_str = u_conf.get("instrument_type", "IND" if u_conf.get("index") else "EQ")
            asset_cls_str = u_conf.get("asset_class", "INDEX" if u_conf.get("index") else "EQUITY")

            if not name or not exchange_str:
                self.logger.warning(f"Strategy {self.id}: Skipping underlying due to missing name/exchange: {u_conf}")
                continue
            
            instrument_id = f"{exchange_str.upper()}:{name}"
            self.underlying_instrument_ids.append(instrument_id)
            self.underlying_details[instrument_id] = {
                "name": name,
                "exchange": exchange_str.upper(),
                "instrument_type": inst_type_str.upper(),
                "asset_class": asset_cls_str.upper(),
                "option_exchange": u_conf.get("option_exchange", "NFO").upper()
            }

        self.expiry_offset = config.get('expiry_offset', 0)

        # Data store: instrument_id -> {timeframe: DataFrame}
        self.data_store: Dict[str, Dict[str, pd.DataFrame]] = {}
        # Tracks active option positions: option_instrument_id -> position_info
        self.active_positions: Dict[str, Dict[str, Any]] = {}
        self.entry_enabled = False
        self.trade_cooldown: Dict[str, float] = {} # underlying_instrument_id -> timestamp
        self.cooldown_period_seconds = self.params.get('cooldown_period_seconds', 300) # Default 5 mins

        # State for EMA/VWAP logic per underlying
        self.crossover_state: Dict[str, str] = {} # underlying_id -> "BULLISH", "BEARISH", None
        self.retracement_pending: Dict[str, str] = {} # underlying_id -> "CE", "PE", None

        self.logger.info(f"EmaVwapOptionStrategy '{self.id}' initialized. Underlyings: {self.underlying_instrument_ids}")

    def _parse_time(self, time_str: str) -> dt_time:
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}'. Using default.")
            return dt_time(9, 30, 0) if 'entry' in time_str.lower() else dt_time(15, 15, 0)

    def initialize(self):
        super().initialize()
        self.logger.info(f"Strategy '{self.id}' custom initialization complete.")

    def on_start(self):
        super().on_start()
        self.logger.info(f"Strategy '{self.id}' on_start: Requesting subscriptions.")
        for instrument_id, details in self.underlying_details.items():
            # Initialize data_store for each underlying and timeframe
            # This strategy uses the primary timeframe (e.g., 1m) for retracement
            # and an additional timeframe (e.g., 5m) for crossover/VWAP context.
            # Ensure these are defined in strategy's config and self.all_timeframes
            for tf in self.all_timeframes: # self.all_timeframes from BaseStrategy
                if instrument_id not in self.data_store:
                    self.data_store[instrument_id] = {}
                self.data_store[instrument_id][tf] = pd.DataFrame(
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                             'fast_ema', 'slow_ema', 'vwap']
                )
            
            self.trade_cooldown[instrument_id] = 0
            self.crossover_state[instrument_id] = None
            self.retracement_pending[instrument_id] = None

            self.request_symbol(
                symbol_name=details["name"],
                exchange_value=details["exchange"],
                instrument_type_value=details["instrument_type"],
                asset_class_value=details["asset_class"],
                timeframes_to_subscribe=self.all_timeframes
            )
        self.active_positions.clear()
        self.entry_enabled = False
        self.logger.info(f"Strategy '{self.id}' started. Entry: {self.entry_time_str}, Exit: {self.exit_time_str}")

    def on_market_data(self, event: MarketDataEvent):
        # self.logger.debug(f"Strategy {self.id} received MarketDataEvent for {event.instrument.symbol if event.instrument else 'N/A'}")
        if not event.instrument or not event.data or not event.instrument.instrument_id:
            return

        option_instrument_id = event.instrument.instrument_id
        if option_instrument_id in self.active_positions:
            price_data = event.data.get(MarketDataType.LAST_PRICE.value)
            if price_data is not None:
                try:
                    current_option_price = float(price_data)
                    self._check_option_exit_conditions(option_instrument_id, current_option_price)
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid option price '{price_data}' for {option_instrument_id}.")

    def on_bar(self, event: BarEvent):
        # self.logger.debug(f"Strategy {self.id} received BarEvent: {event.instrument.symbol if event.instrument else 'N/A'}@{event.timeframe}")
        if not event.instrument or not event.instrument.instrument_id:
            return

        underlying_instrument_id = event.instrument.instrument_id
        timeframe = event.timeframe

        #self.logger.debug(f"instrument_id: {underlying_instrument_id}, self.underlying_instrument_ids: {self.underlying_instrument_ids}, timeframe: {timeframe}, all_timeframes: {self.all_timeframes}")
        if underlying_instrument_id not in self.underlying_instrument_ids or timeframe not in self.all_timeframes:
            #self.logger.debug(f"Ignoring BarEvent for {underlying_instrument_id}@{timeframe}.")
            return

        self._update_data_store(underlying_instrument_id, timeframe, event)
        self._calculate_indicators(underlying_instrument_id, timeframe)        

        current_dt = datetime.fromtimestamp(event.timestamp)
        current_time = current_dt.time()

        self.logger.debug(f"current_time: {current_time}, entry_time: {self.entry_time}, entry_enabled: {self.entry_enabled} ")

        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Entry time {self.entry_time_str} reached. Trading active.")

        if current_time >= self.exit_time:
            if self.active_positions:
                self.logger.info(f"Exit time {self.exit_time_str} reached. Exiting all positions.")
                self._exit_all_positions(f"End of day exit at {self.exit_time_str}")
            if self.entry_enabled:
                self.logger.info("Past exit time. Disabling further entries.")
                self.entry_enabled = False
            return

        if self.entry_enabled and not self._is_in_cooldown(underlying_instrument_id, event.timestamp):
            # Use a longer timeframe (e.g., 5m, if configured in additional_timeframes) for crossover & VWAP context
            # Use primary timeframe (e.g., 1m) for retracement check
            context_timeframe = list(self.additional_timeframes)[0] if self.additional_timeframes else self.timeframe
            
            if timeframe == context_timeframe: # Process context on the designated context timeframe
                self._check_crossover_and_vwap_context(underlying_instrument_id, context_timeframe)
            
            if timeframe == self.timeframe: # Process retracement and entry on primary timeframe
                 if self.retracement_pending.get(underlying_instrument_id):
                    self._check_retracement_and_enter(underlying_instrument_id, self.timeframe, current_dt)
        elif self.entry_enabled:
             self.logger.debug(f"{underlying_instrument_id} in cooldown. No new entry checks.")


    def _is_in_cooldown(self, underlying_instrument_id: str, current_timestamp: float) -> bool:
        last_exit_time = self.trade_cooldown.get(underlying_instrument_id, 0)
        return current_timestamp < last_exit_time + self.cooldown_period_seconds

    def _update_data_store(self, instrument_id: str, timeframe: str, event: BarEvent):
        df = self.data_store[instrument_id][timeframe]
        new_row_data = {
            'timestamp': event.timestamp, 'open': event.open_price, 'high': event.high_price,
            'low': event.low_price, 'close': event.close_price, 'volume': event.volume
        }
        new_row_df = pd.DataFrame([new_row_data])
        self.data_store[instrument_id][timeframe] = pd.concat([df, new_row_df], ignore_index=True)
        
        max_len = max(self.slow_ema_period, 20) + 50 # Keep enough for VWAP and EMAs
        if len(self.data_store[instrument_id][timeframe]) > max_len:
            self.data_store[instrument_id][timeframe] = self.data_store[instrument_id][timeframe].iloc[-max_len:]

    def _calculate_indicators(self, instrument_id: str, timeframe: str):
        df = self.data_store[instrument_id][timeframe]
        if len(df) < self.slow_ema_period:
            return

        df['fast_ema'] = df['close'].ewm(span=self.fast_ema_period, adjust=False).mean()
        df['slow_ema'] = df['close'].ewm(span=self.slow_ema_period, adjust=False).mean()
        
        # Calculate VWAP
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        price_volume = typical_price * df['volume']
        # For daily VWAP, reset cumsum at start of day. For rolling, use rolling window.
        # Simple cumulative VWAP for the loaded data history:
        df['vwap'] = price_volume.cumsum() / df['volume'].cumsum()
        df['vwap'].fillna(method='bfill', inplace=True) # Backfill initial NaNs

    def _check_crossover_and_vwap_context(self, underlying_instrument_id: str, timeframe: str):
        self.logger.debug(f"_check_crossover_and_vwap_context()")
        df = self.data_store[underlying_instrument_id][timeframe]
        self.logger.debug(f"df: {df}")

        if len(df) < 2 or df[['fast_ema', 'slow_ema', 'vwap']].iloc[-1].isnull().any():
            return

        latest = df.iloc[-1]
        previous = df.iloc[-2]

        self.logger.debug(f"checking fast ema")
        # Bullish context: Fast EMA crosses above Slow EMA, and price is above VWAP
        if previous['fast_ema'] <= previous['slow_ema'] and latest['fast_ema'] > latest['slow_ema'] and latest['close'] > latest['vwap']:
            if self.crossover_state.get(underlying_instrument_id) != "BULLISH":
                self.logger.info(f"CONTEXT: Bullish for {underlying_instrument_id} on {timeframe}. FastEMA > SlowEMA, Price > VWAP. Pending CE retracement.")
            self.crossover_state[underlying_instrument_id] = "BULLISH"
            self.retracement_pending[underlying_instrument_id] = "CE" # Look for CE entry

        # Bearish context: Fast EMA crosses below Slow EMA, and price is below VWAP
        elif previous['fast_ema'] >= previous['slow_ema'] and latest['fast_ema'] < latest['slow_ema'] and latest['close'] < latest['vwap']:
            if self.crossover_state.get(underlying_instrument_id) != "BEARISH":
                self.logger.info(f"CONTEXT: Bearish for {underlying_instrument_id} on {timeframe}. FastEMA < SlowEMA, Price < VWAP. Pending PE retracement.")
            self.crossover_state[underlying_instrument_id] = "BEARISH"
            self.retracement_pending[underlying_instrument_id] = "PE" # Look for PE entry
        
        # If conditions no longer met, clear pending retracement
        elif (self.crossover_state.get(underlying_instrument_id) == "BULLISH" and \
              (latest['fast_ema'] <= latest['slow_ema'] or latest['close'] <= latest['vwap'])) or \
             (self.crossover_state.get(underlying_instrument_id) == "BEARISH" and \
              (latest['fast_ema'] >= latest['slow_ema'] or latest['close'] >= latest['vwap'])):
            if self.retracement_pending.get(underlying_instrument_id):
                self.logger.info(f"CONTEXT: Conditions for {self.retracement_pending[underlying_instrument_id]} on {underlying_instrument_id} no longer met. Clearing pending retracement.")
                self.retracement_pending[underlying_instrument_id] = None
                self.crossover_state[underlying_instrument_id] = None


    def _check_retracement_and_enter(self, underlying_instrument_id: str, timeframe: str, current_dt: datetime):
        df = self.data_store[underlying_instrument_id][timeframe] # Primary timeframe (e.g., 1m)
        self.logger.debug(f"retracement: df:  {df}")
        if len(df) < 1 or df[['slow_ema', 'high', 'low', 'close']].iloc[-1].isnull().any():
            self.logger.debug(f"df:  {df}")
            return

        latest = df.iloc[-1]
        pending_type = self.retracement_pending.get(underlying_instrument_id)
        self.logger.debug("pending_type: {pending_type}")

        if pending_type == "CE":
            # Price must retrace towards slow_ema. High of bar that formed crossover to slow_ema.
            # This logic needs refinement: "60% retracement to EMA21" means from what high/low?
            # Assuming retracement from a recent high (post-crossover) towards current slow_ema.
            # For simplicity here: if current low touches or goes below slow_ema.
            # A more precise retracement calculation would require identifying the swing high after bullish context.
            if latest['low'] <= latest['slow_ema'] and latest['close'] > latest['slow_ema']: # Touched and bounced
                self.logger.info(f"RETRACEMENT: CE entry condition met for {underlying_instrument_id} on {timeframe}. Low touched/crossed SlowEMA and closed above.")
                self._enter_option_position(underlying_instrument_id, SignalType.BUY_CALL, latest['close'], current_dt)
                self.retracement_pending[underlying_instrument_id] = None # Clear pending state
                self.crossover_state[underlying_instrument_id] = None

        elif pending_type == "PE":
            # Assuming retracement from a recent low (post-crossover) towards current slow_ema.
            if latest['high'] >= latest['slow_ema'] and latest['close'] < latest['slow_ema']: # Touched and bounced
                self.logger.info(f"RETRACEMENT: PE entry condition met for {underlying_instrument_id} on {timeframe}. High touched/crossed SlowEMA and closed below.")
                self._enter_option_position(underlying_instrument_id, SignalType.BUY_PUT, latest['close'], current_dt)
                self.retracement_pending[underlying_instrument_id] = None # Clear pending state
                self.crossover_state[underlying_instrument_id] = None

    def _enter_option_position(self, underlying_instrument_id: str, signal_type: SignalType, underlying_price: float, current_dt: datetime):
        # Check if already holding an option for this underlying
        for opt_key, pos_info in list(self.active_positions.items()):
            if pos_info.get('underlying_instrument_id') == underlying_instrument_id:
                self.logger.info(f"Already holding an option for {underlying_instrument_id}. Skipping new entry.")
                return

        atm_strike = self.option_manager.get_atm_strike(underlying_instrument_id)
        if atm_strike is None: # Fallback if OM cache is not populated yet
            underlying_config_main = next((u for u in self.config.get("market", {}).get("underlyings", []) if u["symbol"] == self.underlying_details[underlying_instrument_id]["name"]), None)
            strike_interval = underlying_config_main.get("strike_interval", 50.0) if underlying_config_main else 50.0
            atm_strike = round(underlying_price / strike_interval) * strike_interval
            self.logger.info(f"Using fallback ATM calculation: {atm_strike} for {underlying_instrument_id}")


        option_type_enum = OptionType.CALL if signal_type == SignalType.BUY_CALL else OptionType.PUT
        underlying_simple_name = self.underlying_details[underlying_instrument_id]["name"]
        
        option_instrument = self.option_manager.get_option_instrument(
            underlying_simple_name, atm_strike, option_type_enum.value, self.expiry_offset
        )

        if not option_instrument or not option_instrument.instrument_id:
            self.logger.error(f"Could not get {option_type_enum.value} for {underlying_instrument_id}, strike {atm_strike}.")
            return

        option_id_to_trade = option_instrument.instrument_id
        if option_id_to_trade in self.active_positions:
             self.logger.info(f"Already holding {option_id_to_trade}. Skipping entry.")
             return

        option_tick_data = self.data_manager.get_latest_tick(option_id_to_trade)
        if not option_tick_data or MarketDataType.LAST_PRICE.value not in option_tick_data:
            self.logger.warning(f"Tick data not available for {option_id_to_trade}. Cannot estimate entry price.")
            # Optionally, request subscription if not already active
            # self.request_symbol(option_instrument.symbol, option_instrument.exchange.value, ...)
            return
        
        entry_price_est = float(option_tick_data[MarketDataType.LAST_PRICE.value])

        self.logger.info(f"Placing {signal_type.name} for {option_id_to_trade} at market (est. {entry_price_est:.2f})")
        self.generate_signal(
            instrument_id=option_id_to_trade,
            signal_type=SignalType.BUY,
            data={'quantity': self.quantity, 'order_type': self.order_type,
                  'intended_option_type': option_type_enum.value}
        )
        self.active_positions[option_id_to_trade] = {
            'underlying_instrument_id': underlying_instrument_id,
            'option_type': option_type_enum.value,
            'entry_price': entry_price_est, # Will be updated on fill
            'stop_loss_price': entry_price_est * (1 - self.stop_loss_percent / 100),
            'target_price': entry_price_est * (1 + self.target_percent / 100),
            'entry_time': current_dt,
            'instrument_object': option_instrument,
            'high_price_since_entry': entry_price_est # For trailing SL if implemented
        }

    def on_fill(self, event: FillEvent):
        super().on_fill(event) # Base strategy might have logging
        filled_option_id = event.instrument_id
        if not filled_option_id or filled_option_id not in self.active_positions:
            self.logger.debug(f"Fill for {filled_option_id} not relevant to this strategy's active positions.")
            return

        pos_info = self.active_positions[filled_option_id]
        if event.order_side == OrderSide.BUY and event.fill_quantity > 0: # Entry fill
            actual_entry_price = event.fill_price
            pos_info['entry_price'] = actual_entry_price
            pos_info['stop_loss_price'] = actual_entry_price * (1 - self.stop_loss_percent / 100)
            pos_info['target_price'] = actual_entry_price * (1 + self.target_percent / 100)
            pos_info['filled_quantity'] = pos_info.get('filled_quantity', 0) + event.fill_quantity
            pos_info['high_price_since_entry'] = actual_entry_price
            self.logger.info(f"ENTRY FILL for {filled_option_id} @ {actual_entry_price:.2f}. New SL/TP: "
                             f"{pos_info['stop_loss_price']:.2f}/{pos_info['target_price']:.2f}")
        elif event.order_side == OrderSide.SELL and event.fill_quantity > 0: # Exit fill
            self.logger.info(f"EXIT FILL for {filled_option_id} @ {event.fill_price:.2f}. Reason: {event.data.get('reason', 'N/A')}")
            self._handle_exit_cleanup(filled_option_id)


    def _check_option_exit_conditions(self, option_instrument_id: str, current_price: float):
        if option_instrument_id not in self.active_positions: return
        pos_info = self.active_positions[option_instrument_id]
        
        pos_info['high_price_since_entry'] = max(pos_info.get('high_price_since_entry', current_price), current_price)
        # Add trailing SL logic if configured (e.g., self.params.get('trail_sl_percent'))
        # For now, fixed SL/TP

        exit_reason = None
        if current_price <= pos_info['stop_loss_price']:
            exit_reason = f"Stop-loss ({pos_info['stop_loss_price']:.2f})"
        elif current_price >= pos_info['target_price']:
            exit_reason = f"Target-profit ({pos_info['target_price']:.2f})"
        
        if exit_reason:
            self.logger.info(f"Exiting {option_instrument_id} at {current_price:.2f}. Reason: {exit_reason}")
            self._exit_position(option_instrument_id, exit_reason)

    def _exit_position(self, option_instrument_id: str, reason: str):
        if option_instrument_id not in self.active_positions: return
        
        pos_info = self.active_positions[option_instrument_id]
        quantity_to_sell = pos_info.get('filled_quantity', self.quantity)
        if quantity_to_sell <=0: return

        self.generate_signal(
            instrument_id=option_instrument_id, signal_type=SignalType.SELL,
            data={'quantity': quantity_to_sell, 'order_type': self.order_type, 'reason': reason}
        )
        # Actual removal from active_positions happens in _handle_exit_cleanup on fill

    def _handle_exit_cleanup(self, option_instrument_id: str):
        if option_instrument_id in self.active_positions:
            pos_info = self.active_positions.pop(option_instrument_id)
            underlying_id = pos_info.get('underlying_instrument_id')
            if underlying_id:
                self.trade_cooldown[underlying_id] = datetime.now().timestamp()
                self.logger.info(f"Position {option_instrument_id} removed. Cooldown for {underlying_id} started.")
            self.retracement_pending.pop(underlying_id, None) # Clear any pending retracement state for the underlying
            self.crossover_state.pop(underlying_id, None) # Clear crossover state

    def _exit_all_positions(self, reason: str):
        self.logger.info(f"Exiting all {len(self.active_positions)} active positions. Reason: {reason}")
        for opt_id in list(self.active_positions.keys()):
            self._exit_position(opt_id, reason)

    def on_stop(self):
        super().on_stop()
        self._exit_all_positions("Strategy stop command")
        self.data_store.clear()
        self.active_positions.clear()
        self.trade_cooldown.clear()
        self.crossover_state.clear()
        self.retracement_pending.clear()
        self.entry_enabled = False
        self.logger.info(f"Strategy '{self.id}' stopped and cleaned up.")


