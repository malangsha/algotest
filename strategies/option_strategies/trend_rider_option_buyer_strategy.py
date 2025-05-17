"""
Trend Rider Option Buyer Strategy

This strategy aims for a high hit-rate by identifying strong intraday trends
in NIFTY and SENSEX options using EMA crossovers and ADX for trend strength.
It buys Call or Put options accordingly.

- Bar Subscription: Configurable, e.g., 5-minute (5m) bar data for underlyings.
- Underlyings: NIFTY and SENSEX.

- Entry Logic (Call):
  1. Fast EMA (e.g., 9-period) crosses above Slow EMA (e.g., 21-period).
  2. ADX (e.g., 14-period) is above a threshold (e.g., 20) and rising,
     OR +DI is above -DI and ADX is above threshold.
  3. Buy an ATM or slightly OTM Call option.

- Entry Logic (Put):
  1. Fast EMA (e.g., 9-period) crosses below Slow EMA (e.g., 21-period).
  2. ADX (e.g., 14-period) is above a threshold (e.g., 20) and rising,
     OR -DI is above +DI and ADX is above threshold.
  3. Buy an ATM or slightly OTM Put option.

- Risk Management (defined in strategy, executed by framework):
  - Stop-loss: Percentage of option premium.
  - Target Profit: Percentage of option premium or multiple of SL.
  - Position Sizing: Base quantity defined, actual sizing by framework.

- Exit Logic:
  1. Target profit hit.
  2. Stop-loss hit.
  3. EMA crossover in the opposite direction.
  4. End of trading day.
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
from core.logging_manager import get_logger

@StrategyRegistry.register('trend_rider_option_buyer_strategy')
class TrendRiderOptionBuyerStrategy(OptionStrategy):
    """
    Option buying strategy based on EMA crossover and ADX trend strength.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager,
                 event_manager, broker=None):
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager, broker)

        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:25:00')
        self.exit_time_str = self.params.get('exit_time', '15:10:00')
        
        self.fast_ema_period = self.params.get('fast_ema_period', 9)
        self.slow_ema_period = self.params.get('slow_ema_period', 21)
        self.adx_period = self.params.get('adx_period', 14)
        self.adx_threshold = self.params.get('adx_threshold', 20)

        self.stop_loss_percent = self.params.get('stop_loss_percent', 25)
        self.target_percent = self.params.get('target_percent', 50)
        
        self.quantity = config.get('execution', {}).get('quantity', 1)
        self.order_type = config.get('execution', {}).get('order_type', 'MARKET')

        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        self.strategy_specific_underlyings_config = config.get("underlyings", [])
        self.underlying_instrument_ids: List[str] = []
        self.underlying_details: Dict[str, Dict[str, Any]] = {}

        for u_conf in self.strategy_specific_underlyings_config:
            name = u_conf.get("name")
            exchange_str = u_conf.get("spot_exchange")
            inst_type_str = u_conf.get("instrument_type", "IND" if u_conf.get("index") else "EQ")
            asset_cls_str = u_conf.get("asset_class", "INDEX" if u_conf.get("index") else "EQUITY")

            if not name or not exchange_str:
                self.logger.warning(f"Skipping underlying: missing name/exchange: {u_conf}")
                continue
            
            instrument_id = f"{exchange_str.upper()}:{name}"
            self.underlying_instrument_ids.append(instrument_id)
            self.underlying_details[instrument_id] = {
                "name": name, "exchange": exchange_str.upper(),
                "instrument_type": inst_type_str.upper(),
                "asset_class": asset_cls_str.upper(),
                "option_exchange": u_conf.get("option_exchange", "NFO").upper()
            }

        self.expiry_offset = config.get('expiry_offset', 0)
        self.otm_strikes_away = self.params.get('otm_strikes_away', 0) # 0 for ATM

        self.data_store: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.active_positions: Dict[str, Dict[str, Any]] = {}
        self.entry_enabled = False
        self.trade_cooldown: Dict[str, float] = {}
        self.cooldown_period_seconds = self.params.get('cooldown_period_seconds', 300)

        self.logger.info(f"TrendRiderOptionBuyerStrategy '{self.id}' initialized. Underlyings: {self.underlying_instrument_ids}")

    def _parse_time(self, time_str: str) -> dt_time:
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}'. Defaulting.")
            return dt_time(9, 25, 0) if 'entry' in time_str.lower() else dt_time(15, 10, 0)

    def initialize(self):
        super().initialize()
        self.logger.info(f"Strategy '{self.id}' custom initialization complete.")

    def on_start(self):
        super().on_start()
        primary_tf = self.timeframe # From base_strategy, e.g., "5m"
        self.logger.info(f"Strategy '{self.id}' on_start. Primary TF: {primary_tf}. Requesting subscriptions.")
        for instrument_id, details in self.underlying_details.items():
            if instrument_id not in self.data_store: self.data_store[instrument_id] = {}
            self.data_store[instrument_id][primary_tf] = pd.DataFrame(
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                         'fast_ema', 'slow_ema', 'adx', 'plus_di', 'minus_di']
            )
            self.trade_cooldown[instrument_id] = 0
            self.request_symbol(
                symbol_name=details["name"], exchange_value=details["exchange"],
                instrument_type_value=details["instrument_type"],
                asset_class_value=details["asset_class"],
                timeframes_to_subscribe={primary_tf} # Only primary timeframe needed
            )
        self.active_positions.clear()
        self.entry_enabled = False
        self.logger.info(f"Strategy '{self.id}' started. Entry: {self.entry_time_str}, Exit: {self.exit_time_str}")

    def on_market_data(self, event: MarketDataEvent):
        if not event.instrument or not event.data or not event.instrument.instrument_id: return
        option_instrument_id = event.instrument.instrument_id
        if option_instrument_id in self.active_positions:
            price_data = event.data.get(MarketDataType.LAST_PRICE.value)
            if price_data is not None:
                try:
                    self._check_option_exit_conditions(option_instrument_id, float(price_data))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid option price '{price_data}' for {option_instrument_id}.")

    def on_bar(self, event: BarEvent):
        if not event.instrument or not event.instrument.instrument_id: return

        underlying_id = event.instrument.instrument_id
        timeframe = event.timeframe

        if underlying_id not in self.underlying_instrument_ids or timeframe != self.timeframe:
            return # Process only configured underlyings on the strategy's primary timeframe

        self._update_data_store(underlying_id, timeframe, event)
        self._calculate_indicators(underlying_id, timeframe)

        current_dt = datetime.fromtimestamp(event.timestamp)
        current_time = current_dt.time()

        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Entry time {self.entry_time_str} reached. Trading active.")

        if current_time >= self.exit_time:
            if self.active_positions:
                self._exit_all_positions(f"End of day exit at {self.exit_time_str}")
            if self.entry_enabled: self.entry_enabled = False
            return

        if self.entry_enabled and not self._is_in_cooldown(underlying_id, event.timestamp):
            self._check_entry_signals(underlying_id, current_dt)
        elif self.entry_enabled:
            self.logger.debug(f"{underlying_id} in cooldown.")

    def _is_in_cooldown(self, uid: str, ts: float) -> bool:
        return ts < self.trade_cooldown.get(uid, 0) + self.cooldown_period_seconds

    def _update_data_store(self, iid: str, tf: str, event: BarEvent):
        df = self.data_store[iid][tf]
        new_row = pd.DataFrame([{'timestamp': event.timestamp, 'open': event.open, 
                               'high': event.high, 'low': event.low, 
                               'close': event.close, 'volume': event.volume}])
        self.data_store[iid][tf] = pd.concat([df, new_row], ignore_index=True)
        max_len = max(self.slow_ema_period, self.adx_period) + 50
        if len(self.data_store[iid][tf]) > max_len:
            self.data_store[iid][tf] = self.data_store[iid][tf].iloc[-max_len:]

    def _calculate_indicators(self, iid: str, tf: str):
        df = self.data_store[iid][tf]
        if len(df) < max(self.slow_ema_period, self.adx_period): return

        df['fast_ema'] = df['close'].ewm(span=self.fast_ema_period, adjust=False).mean()
        df['slow_ema'] = df['close'].ewm(span=self.slow_ema_period, adjust=False).mean()
        
        # ADX Calculation
        df['tr'] = np.maximum(df['high'] - df['low'], 
                           np.maximum(abs(df['high'] - df['close'].shift(1)), 
                                      abs(df['low'] - df['close'].shift(1))))
        df['atr'] = df['tr'].ewm(alpha=1/self.adx_period, adjust=False).mean()

        df['up_move'] = df['high'] - df['high'].shift(1)
        df['down_move'] = df['low'].shift(1) - df['low']
        
        df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
        df['minus_dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)

        df['plus_di'] = 100 * (df['plus_dm'].ewm(alpha=1/self.adx_period, adjust=False).mean() / df['atr'])
        df['minus_di'] = 100 * (df['minus_dm'].ewm(alpha=1/self.adx_period, adjust=False).mean() / df['atr'])
        
        dx = (abs(df['plus_di'] - df['minus_di']) / abs(df['plus_di'] + df['minus_di']).replace(0, 1e-9)) * 100
        df['adx'] = dx.ewm(alpha=1/self.adx_period, adjust=False).mean()
        
        df.fillna(method='bfill', inplace=True) # Fill initial NaNs

    def _check_entry_signals(self, underlying_id: str, current_dt: datetime):
        df = self.data_store[underlying_id][self.timeframe]
        req_bars = max(self.slow_ema_period, self.adx_period) + 1
        if len(df) < req_bars or df[['fast_ema', 'slow_ema', 'adx', 'plus_di', 'minus_di']].iloc[-1].isnull().any():
            return

        latest = df.iloc[-1]
        previous = df.iloc[-2]

        # Check if already holding an option for this underlying
        for opt_key, pos_info in list(self.active_positions.items()):
            if pos_info.get('underlying_instrument_id') == underlying_id: return

        # Call Entry
        if previous['fast_ema'] <= previous['slow_ema'] and latest['fast_ema'] > latest['slow_ema']:
            if latest['adx'] > self.adx_threshold and latest['plus_di'] > latest['minus_di']:
                self.logger.info(f"CALL Signal for {underlying_id} at {current_dt}. EMA Crossover, ADX={latest['adx']:.2f}, +DI > -DI.")
                self._enter_option_position(underlying_id, SignalType.BUY_CALL, latest['close'], current_dt)
                return # Avoid immediate PUT signal

        # Put Entry
        if previous['fast_ema'] >= previous['slow_ema'] and latest['fast_ema'] < latest['slow_ema']:
            if latest['adx'] > self.adx_threshold and latest['minus_di'] > latest['plus_di']:
                self.logger.info(f"PUT Signal for {underlying_id} at {current_dt}. EMA Crossover, ADX={latest['adx']:.2f}, -DI > +DI.")
                self._enter_option_position(underlying_id, SignalType.BUY_PUT, latest['close'], current_dt)

    def _enter_option_position(self, underlying_id: str, signal_type: SignalType, underlying_price: float, current_dt: datetime):
        atm_strike_raw = self.option_manager.get_atm_strike(underlying_id)
        if atm_strike_raw is None:
            u_details = self.underlying_details[underlying_id]
            # Fallback to config for strike interval if OM not ready
            u_main_cfg = next((u for u in self.config.get("market", {}).get("underlyings", []) if u["symbol"] == u_details["name"]), None)
            strike_interval = u_main_cfg.get("strike_interval", 50.0) if u_main_cfg else 50.0
            atm_strike_raw = round(underlying_price / strike_interval) * strike_interval
            self.logger.info(f"Using fallback ATM {atm_strike_raw} for {underlying_id}")

        option_type_enum = OptionType.CALL if signal_type == SignalType.BUY_CALL else OptionType.PUT
        
        # Adjust strike for OTM if configured
        strike_interval = self.option_manager.strike_intervals.get(underlying_id, 50.0) # Get actual or default
        target_strike = atm_strike_raw
        if self.otm_strikes_away > 0:
            if option_type_enum == OptionType.CALL:
                target_strike = atm_strike_raw + (self.otm_strikes_away * strike_interval)
            else: # PUT
                target_strike = atm_strike_raw - (self.otm_strikes_away * strike_interval)
        
        underlying_simple_name = self.underlying_details[underlying_id]["name"]
        option_instrument = self.option_manager.get_option_instrument(
            underlying_simple_name, target_strike, option_type_enum.value, self.expiry_offset
        )

        if not option_instrument or not option_instrument.instrument_id:
            self.logger.error(f"Could not get {option_type_enum.value} for {underlying_id}, strike {target_strike}.")
            return

        opt_id_trade = option_instrument.instrument_id
        if opt_id_trade in self.active_positions: return

        opt_tick = self.data_manager.get_latest_tick(opt_id_trade)
        if not opt_tick or MarketDataType.LAST_PRICE.value not in opt_tick:
            self.logger.warning(f"Tick data not available for {opt_id_trade}. Cannot estimate entry price.")
            # Consider requesting symbol if not subscribed:
            # self.request_symbol(option_instrument.symbol, option_instrument.exchange.value, InstrumentType.OPTION.value, ...)
            return
        
        entry_price_est = float(opt_tick[MarketDataType.LAST_PRICE.value])

        self.logger.info(f"Placing {signal_type.name} for {opt_id_trade} at market (est. {entry_price_est:.2f})")
        self.generate_signal(
            instrument_id=opt_id_trade, signal_type=SignalType.BUY,
            data={'quantity': self.quantity, 'order_type': self.order_type,
                  'intended_option_type': option_type_enum.value}
        )
        self.active_positions[opt_id_trade] = {
            'underlying_instrument_id': underlying_id, 'option_type': option_type_enum.value,
            'entry_price': entry_price_est, # Updated on fill
            'stop_loss_price': entry_price_est * (1 - self.stop_loss_percent / 100),
            'target_price': entry_price_est * (1 + self.target_percent / 100),
            'entry_time': current_dt, 'instrument_object': option_instrument,
            'high_price_since_entry': entry_price_est
        }

    def on_fill(self, event: FillEvent):
        super().on_fill(event)
        filled_opt_id = event.instrument_id
        if not filled_opt_id or filled_opt_id not in self.active_positions: return

        pos_info = self.active_positions[filled_opt_id]
        if event.order_side == OrderSide.BUY and event.fill_quantity > 0: # Entry
            actual_entry = event.fill_price
            pos_info.update({
                'entry_price': actual_entry,
                'stop_loss_price': actual_entry * (1 - self.stop_loss_percent / 100),
                'target_price': actual_entry * (1 + self.target_percent / 100),
                'filled_quantity': pos_info.get('filled_quantity', 0) + event.fill_quantity,
                'high_price_since_entry': actual_entry
            })
            self.logger.info(f"ENTRY FILL {filled_opt_id} @ {actual_entry:.2f}. SL/TP: "
                             f"{pos_info['stop_loss_price']:.2f}/{pos_info['target_price']:.2f}")
        elif event.order_side == OrderSide.SELL and event.fill_quantity > 0: # Exit
            self.logger.info(f"EXIT FILL {filled_opt_id} @ {event.fill_price:.2f}. Reason: {event.data.get('reason', 'N/A')}")
            self._handle_exit_cleanup(filled_opt_id)

    def _check_option_exit_conditions(self, opt_id: str, current_price: float):
        if opt_id not in self.active_positions: return
        pos_info = self.active_positions[opt_id]
        pos_info['high_price_since_entry'] = max(pos_info.get('high_price_since_entry', current_price), current_price)

        exit_reason = None
        if current_price <= pos_info['stop_loss_price']: exit_reason = f"SL ({pos_info['stop_loss_price']:.2f})"
        elif current_price >= pos_info['target_price']: exit_reason = f"TP ({pos_info['target_price']:.2f})"
        
        # EMA Reversal Exit (check on underlying's bar)
        underlying_id = pos_info.get('underlying_instrument_id')
        if not exit_reason and underlying_id:
            df_underlying = self.data_store[underlying_id][self.timeframe]
            if len(df_underlying) > 0:
                latest_underlying = df_underlying.iloc[-1]
                if pos_info['option_type'] == OptionType.CALL.value and latest_underlying['fast_ema'] < latest_underlying['slow_ema']:
                    exit_reason = "EMA Reversal (Underlying Bearish)"
                elif pos_info['option_type'] == OptionType.PUT.value and latest_underlying['fast_ema'] > latest_underlying['slow_ema']:
                    exit_reason = "EMA Reversal (Underlying Bullish)"
        
        if exit_reason:
            self.logger.info(f"Exiting {opt_id} at {current_price:.2f}. Reason: {exit_reason}")
            self._exit_position(opt_id, exit_reason)

    def _exit_position(self, opt_id: str, reason: str):
        if opt_id not in self.active_positions: return
        pos_info = self.active_positions[opt_id]
        qty_sell = pos_info.get('filled_quantity', self.quantity)
        if qty_sell <= 0: return
        self.generate_signal(instrument_id=opt_id, signal_type=SignalType.SELL,
                             data={'quantity': qty_sell, 'order_type': self.order_type, 'reason': reason})

    def _handle_exit_cleanup(self, opt_id: str):
        if opt_id in self.active_positions:
            pos_info = self.active_positions.pop(opt_id)
            uid = pos_info.get('underlying_instrument_id')
            if uid:
                self.trade_cooldown[uid] = datetime.now().timestamp()
                self.logger.info(f"Position {opt_id} removed. Cooldown for {uid} started.")

    def _exit_all_positions(self, reason: str):
        self.logger.info(f"Exiting all {len(self.active_positions)} positions. Reason: {reason}")
        for opt_id in list(self.active_positions.keys()): self._exit_position(opt_id, reason)

    def on_stop(self):
        super().on_stop()
        self._exit_all_positions("Strategy stop command")
        self.data_store.clear(); self.active_positions.clear(); self.trade_cooldown.clear()
        self.entry_enabled = False
        self.logger.info(f"Strategy '{self.id}' stopped and cleaned up.")


