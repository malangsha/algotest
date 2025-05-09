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
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, SignalEvent
from models.instrument import Instrument
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide
from strategies.strategy_registry import StrategyRegistry
from core.logging_manager import get_logger

@StrategyRegistry.register('high_frequency_momentum_option_strategy')
class HighFrequencyMomentumOptionStrategy(OptionStrategy):
    """
    High-Frequency Momentum Option Strategy for NIFTY and SENSEX.
    Subscribes to 1-minute bar data.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager):
        super().__init__(strategy_id, config, data_manager,
                         option_manager, portfolio_manager, event_manager)

        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:20:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.stop_loss_percent = self.params.get('stop_loss_percent', 5)  # Tighter SL for high hit rate
        self.target_percent = self.params.get('target_percent', 10)    # Modest target for high hit rate
        self.quantity = config.get('execution', {}).get('quantity', 1)

        self.fast_ema_period = self.params.get('fast_ema_period', 5)
        self.slow_ema_period = self.params.get('slow_ema_period', 20)
        self.rsi_period = self.params.get('rsi_period', 14)
        self.rsi_upper_band = self.params.get('rsi_upper_band', 70)
        self.rsi_lower_band = self.params.get('rsi_lower_band', 30)
        self.rsi_mid_point = 50
        self.breakout_bars = self.params.get('breakout_bars', 3) # Lookback for breakout

        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        self.raw_underlyings = config.get("underlyings", [])
        self.underlyings = [u["name"] for u in self.raw_underlyings if isinstance(u, dict) and "name" in u]
        self.expiry_offset = config.get('expiry_offset', 0)

        self.data_store = {}  # underlying_symbol -> {'1m': DataFrame}
        self.active_positions = {}  # option_symbol_key -> position_info (entry_price, stop_loss, target)
        self.entry_enabled = False
        self.trade_cooldown = {} # underlying_symbol -> timestamp of last trade exit to avoid immediate re-entry
        self.cooldown_period_seconds = self.params.get('cooldown_period_seconds', 300) # 5 minutes

        self.logger.info(f"HighFrequencyMomentumOptionStrategy '{self.id}' initialized. Underlyings: {self.underlyings}, "
                         f"Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, Target: {self.target_percent}%")

    def _parse_time(self, time_str: str) -> dt_time:
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}', using default.")
            return dt_time(9, 20, 0) if 'entry' in time_str.lower() else dt_time(15, 15, 0)

    def get_required_symbols(self) -> Dict[str, List[str]]:
        symbols_by_timeframe = {"1m": list(self.underlyings)}
        self.logger.debug(f"Strategy '{self.id}' requires underlyings: {self.underlyings} on timeframe 1m")
        return symbols_by_timeframe

    def on_start(self):
        for underlying in self.raw_underlyings:
            symbol = underlying.get("name")
            exchange = underlying.get("spot_exchange")

            self.data_store[symbol] = {
                '1m': pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                                            'fast_ema', 'slow_ema', 'rsi', 'recent_high', 'recent_low'])
            }
            self.trade_cooldown[symbol] = 0
            self.logger.debug(f"on_start(): symbol: {symbol} exchange: {exchange}")            
            if symbol and exchange:
                self.request_symbol(symbol, exchange=exchange)
            elif symbol:
                # Fallback if exchange info is missing (should have been handled in __init__)
                self.logger.warning(f"Requesting symbol {symbol} without explicit exchange.")
                self.request_symbol(symbol)

        self.entry_enabled = False
        self.active_positions.clear()
        self.logger.info(f"Strategy '{self.id}' started. Waiting for market data and entry time {self.entry_time_str}.")

    def on_market_data(self, event: MarketDataEvent):
        self.logger.debug(f"Received MarketDataEvent: {event}")

        if not event.instrument or not event.data or not event.instrument.instrument_id:
            return
        
        option_symbol_key = event.instrument.instrument_id
        if option_symbol_key in self.active_positions:
            price = event.data.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                try:
                    current_price = float(price)
                    self._check_option_exit_conditions(option_symbol_key, current_price)
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid option price for {option_symbol_key}: {price}")

    def on_bar(self, event: BarEvent):
        
        self.logger.debug(f"Received BarEvent: {event}")
                          
        if not event.instrument or event.instrument.instrument_id not in self.underlyings or event.timeframe != '1m':
            return

        underlying_symbol = event.instrument.instrument_id
        self._update_data_store(underlying_symbol, event)
        self._calculate_indicators(underlying_symbol)

        current_dt = datetime.fromtimestamp(event.timestamp)
        current_time = current_dt.time()

        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Entry time {self.entry_time_str} reached. Strategy '{self.id}' is now active.")

        if current_time >= self.exit_time:
            if self.active_positions:
                self.logger.info(f"Exit time {self.exit_time_str} reached. Exiting all open positions for '{self.id}'.")
                self._exit_all_positions("Time-based exit")
            return # No new trades after exit time

        if self.entry_enabled and not self._is_in_cooldown(underlying_symbol, event.timestamp):
            self._check_entry_signals(underlying_symbol, current_dt)

    def _is_in_cooldown(self, underlying_symbol: str, current_timestamp: int) -> bool:
        last_exit_time = self.trade_cooldown.get(underlying_symbol, 0)
        if current_timestamp < last_exit_time + self.cooldown_period_seconds:
            self.logger.debug(f"Strategy '{self.id}' for {underlying_symbol} in cooldown.")
            return True
        return False

    def _update_data_store(self, symbol: str, event: BarEvent):
        df = self.data_store[symbol]['1m']
        new_row = pd.DataFrame([{
            'timestamp': event.timestamp,
            'open': event.open, 'high': event.high, 'low': event.low, 'close': event.close, 'volume': event.volume
        }])
        self.data_store[symbol]['1m'] = pd.concat([df, new_row], ignore_index=True)
        if len(self.data_store[symbol]['1m']) > max(self.slow_ema_period, self.rsi_period) + self.breakout_bars + 50: # Keep reasonable history
            self.data_store[symbol]['1m'] = self.data_store[symbol]['1m'].iloc[-(max(self.slow_ema_period, self.rsi_period) + self.breakout_bars + 50):]

    def _calculate_indicators(self, symbol: str):
        df = self.data_store[symbol]['1m']
        if len(df) < self.slow_ema_period or len(df) < self.rsi_period:
            return

        df['fast_ema'] = df['close'].ewm(span=self.fast_ema_period, adjust=False).mean()
        df['slow_ema'] = df['close'].ewm(span=self.slow_ema_period, adjust=False).mean()
        
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        df['recent_high'] = df['high'].rolling(window=self.breakout_bars).max().shift(1) # Shift 1 to use previous N bars high
        df['recent_low'] = df['low'].rolling(window=self.breakout_bars).min().shift(1)   # Shift 1 to use previous N bars low
        
        self.data_store[symbol]['1m'] = df

    def _check_entry_signals(self, underlying_symbol: str, current_dt: datetime):
        df = self.data_store[underlying_symbol]['1m']
        required_bars = max(self.slow_ema_period, self.rsi_period) + self.breakout_bars
        if len(df) < required_bars:
            self.logger.debug(f"waiting for enough bars to consolidate: current length: {len(df)}, required: {required_bars}")
            return

        latest = df.iloc[-1]
        previous = df.iloc[-2]

        # Check if already holding a position for this underlying (CE or PE)
        for opt_key, pos_info in list(self.active_positions.items()):
            if pos_info['underlying'] == underlying_symbol:
                # self.logger.debug(f"Already holding position for {underlying_symbol}, skipping new entry check.")
                return

        # Call Entry
        call_signal = False
        if previous['fast_ema'] <= previous['slow_ema'] and latest['fast_ema'] > latest['slow_ema']: # Bullish crossover
            if self.rsi_mid_point < latest['rsi'] < self.rsi_upper_band: # RSI confirmation
                if not pd.isna(latest['recent_high']) and latest['close'] > latest['recent_high']: # Breakout confirmation
                    call_signal = True
                    self.logger.info(f"CALL Signal for {underlying_symbol} at {current_dt}: EMA Crossover, RSI={latest['rsi']:.2f}, Breakout above {latest['recent_high']:.2f}")
                    self._enter_option_position(underlying_symbol, SignalType.BUY_CALL, latest['close'], current_dt)

        # Put Entry (only if no call signal to avoid conflicting signals on same bar)
        put_signal = False
        if not call_signal and previous['fast_ema'] >= previous['slow_ema'] and latest['fast_ema'] < latest['slow_ema']: # Bearish crossover
            if self.rsi_lower_band < latest['rsi'] < self.rsi_mid_point: # RSI confirmation
                if not pd.isna(latest['recent_low']) and latest['close'] < latest['recent_low']: # Breakout confirmation
                    put_signal = True
                    self.logger.info(f"PUT Signal for {underlying_symbol} at {current_dt}: EMA Crossover, RSI={latest['rsi']:.2f}, Breakout below {latest['recent_low']:.2f}")
                    self._enter_option_position(underlying_symbol, SignalType.BUY_PUT, latest['close'], current_dt)
        
        # Check for EMA reversal exit for existing positions if no new entry
        if not call_signal and not put_signal:
            self._check_ema_reversal_exit(underlying_symbol, latest)

    def _enter_option_position(self, underlying_symbol: str, signal_type: SignalType, underlying_price: float, current_dt: datetime):
        strike_price = self.option_manager.get_atm_strike(underlying_symbol, underlying_price)
        if strike_price is None:
            self.logger.warning(f"Could not get ATM strike for {underlying_symbol} at price {underlying_price}")
            return

        option_type = OptionType.CALL.value if signal_type == SignalType.BUY_CALL else OptionType.PUT.value
        option_instrument = self.option_manager.get_option_instrument(
            underlying_symbol, strike_price, option_type, self.expiry_offset, current_dt.date()
        )

        if not option_instrument:
            self.logger.error(f"Could not find {option_type} option for {underlying_symbol} strike {strike_price}")
            return

        # Check if we are already holding this specific option (unlikely with current logic but good practice)
        if option_instrument.instrument_id in self.active_positions:
            self.logger.info(f"Already holding {option_instrument.instrument_id}, skipping duplicate entry.")
            return
        
        symbol_key = option_instrument.instrument_id      
        
        # 3. Get latest prices for these options from DataManager's cache
        symbol_data = self.data_manager.get_latest_tick(symbol_key) # Use DataManager cache       
        if not symbol_data:
            self.logger.warning(f"Market data not yet available for {symbol_key}. Retrying next cycle.")
             # Data might arrive shortly after subscription
        symbol_price = symbol_data.get(MarketDataType.LAST_PRICE.value)        
        if symbol_price is None:
            self.logger.warning(f"Last price not found in data for {symbol_key}.")            
        try:
            symbol_price = float(symbol_price)            
        except (ValueError, TypeError):
             self.logger.error(f"Invalid price format for CE ({symbol_price}).")             
    
        self.logger.info(f"Placing {signal_type.name} order for {option_instrument.symbol} (Underlying: {underlying_symbol})")        
        
        # 4. Generate BUY signals
        common_signal_data = {
            'quantity': self.quantity,
            'strategy': self.id,
            'underlying': underlying_symbol        
        }
        
        self.generate_signal(option_instrument.symbol, 
                             SignalType.BUY, 
                             data={**common_signal_data, 
                                   'type': OptionType.CALL.value, 
                                   'price': symbol_price})
        
        # 5. Initialize position tracking immediately (don't wait for fill)
        #    We assume the order will likely fill at or near the signal price for MARKET orders.
        #    Stop loss/target calculations are based on this assumed entry.
        self.active_positions[option_instrument.instrument_id] = {
            'underlying': underlying_symbol,
            'option_type': option_type,
            'entry_price': None, # To be filled by on_fill
            'stop_loss_price': None,
            'target_price': None,
            'entry_time': current_dt,
            'instrument': option_instrument
        }
        self.logger.info(f"Market order placed for {option_instrument.symbol}. Waiting for fill.")
        
    def generate_signal(self, symbol: str, signal_type: SignalType, data: Dict[str, Any] = None, option_instrument: str = None):
        """
        Generate a trading signal with specified priority.

        Args:
            symbol: Symbol to trade
            signal_type: Type of signal (BUY, SELL, etc.)
            data: Additional signal data
            priority: Priority level of the signal
        """
        # Basic signal validation
        if not symbol:
            self.logger.error("Cannot generate signal: symbol is required")
            return
            
        if not signal_type:
            self.logger.error("Cannot generate signal: signal type is required")
            return
            
        # Create signal data
        signal_data = {
            'symbol': symbol,
            'signal_type': signal_type,
            'strategy_id': self.id,
            'timestamp': datetime.now(),
            'data': data or {}
        }
        
        # Create a signal event with specified priority
        signal_event = SignalEvent(
            timestamp=datetime.now(),
            instrument=option_instrument,
            signal_type=signal_type,
            strategy_id=self.id,
            data=signal_data
        )
        
        # Publish the signal event
        self.event_manager.publish(signal_event)
        
        # Increment signals generated
        self.signals_generated += 1
        
        self.logger.info(f"Generated {signal_type} signal for {symbol} with priority {priority.name}")

    def on_fill(self, fill_event):
        """Handle fill events to update position details and set SL/TP."""
        super().on_fill(fill_event) # Call base class method if it exists and does something useful
        
        filled_order = fill_event.order
        option_symbol_key = filled_order.instrument.instrument_id

        if option_symbol_key in self.active_positions and filled_order.status == "FILLED":
            entry_price = filled_order.average_price
            self.active_positions[option_symbol_key]['entry_price'] = entry_price
            
            sl_price = entry_price * (1 - self.stop_loss_percent / 100)
            tp_price = entry_price * (1 + self.target_percent / 100)
            
            self.active_positions[option_symbol_key]['stop_loss_price'] = sl_price
            self.active_positions[option_symbol_key]['target_price'] = tp_price
            
            self.logger.info(f"Position opened for {option_symbol_key} at {entry_price:.2f}. "
                             f"SL: {sl_price:.2f}, TP: {tp_price:.2f}")
        elif filled_order.status == "REJECTED" or filled_order.status == "CANCELLED":
            if option_symbol_key in self.active_positions:
                del self.active_positions[option_symbol_key]
            self.logger.warning(f"Order for {option_symbol_key} was {filled_order.status}. Reason: {fill_event.message}")

    def _check_option_exit_conditions(self, option_symbol_key: str, current_price: float):
        if option_symbol_key not in self.active_positions:
            return

        pos_info = self.active_positions[option_symbol_key]
        exit_reason = None

        if pos_info['stop_loss_price'] and current_price <= pos_info['stop_loss_price']:
            exit_reason = f"Stop-loss hit at {current_price:.2f} (SL: {pos_info['stop_loss_price']:.2f})"
        elif pos_info['target_price'] and current_price >= pos_info['target_price']:
            exit_reason = f"Target-profit hit at {current_price:.2f} (TP: {pos_info['target_price']:.2f})"
        
        if exit_reason:
            self.logger.info(f"Exiting {option_symbol_key}: {exit_reason}")
            self._exit_position(option_symbol_key, exit_reason)

    def _check_ema_reversal_exit(self, underlying_symbol: str, latest_bar_data):
        for opt_key, pos_info in list(self.active_positions.items()):
            if pos_info['underlying'] == underlying_symbol:
                exit_signal = False
                if pos_info['option_type'] == 'CE' and latest_bar_data['fast_ema'] < latest_bar_data['slow_ema']:
                    exit_signal = True
                    reason = "EMA bearish crossover (exit CE)"
                elif pos_info['option_type'] == 'PE' and latest_bar_data['fast_ema'] > latest_bar_data['slow_ema']:
                    exit_signal = True
                    reason = "EMA bullish crossover (exit PE)"
                
                if exit_signal:
                    self.logger.info(f"Exiting {opt_key} for {underlying_symbol} due to {reason}")
                    self._exit_position(opt_key, reason)

    def _exit_position(self, option_symbol_key: str, reason: str):
        if option_symbol_key not in self.active_positions:
            return

        pos_info = self.active_positions[option_symbol_key]
        option_instrument = pos_info['instrument']
        underlying_symbol = pos_info['underlying']

        self.logger.info(f"Placing SELL order for {option_instrument.symbol} to close position. Reason: {reason}")
        order = self.portfolio_manager.create_market_order(
            instrument=option_instrument,
            quantity=self.quantity, # Assuming full quantity exit
            order_type="SELL"
        )
        
        if order:
            self.logger.info(f"SELL order placed for {option_instrument.symbol}. Waiting for fill to confirm exit.")
            # Actual removal from active_positions should happen on fill confirmation of this sell order.
            # For simplicity in this context, we remove it now. In a robust system, manage pending exits.
            del self.active_positions[option_symbol_key]
            self.trade_cooldown[underlying_symbol] = datetime.now().timestamp() # Use current time for cooldown start
            self.data_manager.unsubscribe_market_data([option_instrument]) # Unsubscribe from option data
        else:
            self.logger.error(f"Failed to create SELL order for {option_instrument.symbol}")

    def _exit_all_positions(self, reason: str):
        self.logger.info(f"Exiting all ({len(self.active_positions)}) open positions for strategy '{self.id}'. Reason: {reason}")
        for option_symbol_key in list(self.active_positions.keys()): # Iterate over a copy of keys
            self._exit_position(option_symbol_key, reason)
        self.active_positions.clear() # Ensure all cleared after attempting exit

    def on_stop(self):
        self.logger.info(f"Strategy '{self.id}' stopping. Exiting all positions.")
        self._exit_all_positions("Strategy stop requested")
        self.logger.info(f"Strategy '{self.id}' stopped.")


