import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Any, Set, Tuple

from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, SignalEvent, FillEvent
from models.instrument import Instrument, InstrumentType, AssetClass # Assuming these are in models.instrument
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide, Exchange # Ensure these are correctly pathed
from strategies.strategy_registry import StrategyRegistry # Assuming this is how strategies are registered
from core.logging_manager import get_logger # Assuming this is your logging setup

@StrategyRegistry.register('gamma_scalping_iv_filter_strategy')
class GammaScalpingStraddleStrategy(OptionStrategy):
    """
    Option-Buying Strategy: "Gamma Scalping Straddle with Dynamic IV Filter"

    Logic:
    1. Entry: Buy ATM straddle (call + put) when:
       - 1-day IV percentile of underlying < configured threshold (e.g., 25%)
       - Price of underlying breaks Bollinger Band (e.g., 20,2) on 15min chart
       - This breakout occurs after a period of consolidation (e.g., 5 days with daily range < 1%)
    2. Exit: Close entire straddle position when either:
       - Configured % absolute return on total premium paid is met (e.g., 2%) OR
       - Configured holding period expires (e.g., 24 hours) OR
       - Configured % stop-loss on total premium paid is hit (e.g., 15%)
    3. Strike Selection: Closest ATM strike for both call and put.
       - Filter: Underlying price must be within a small percentage (e.g., 0.5%) of the chosen ATM strike.
    4. Timeframe: 15min bars for underlying analysis and entry signals.
    5. Options: Weekly options (typically expiry_offset = 0).
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager, broker=None):
        super().__init__(strategy_id, config, data_manager, option_manager, portfolio_manager, event_manager, broker)

        self.params = config.get('parameters', {})
        self.underlying_configs = config.get('underlyings', []) # List of dicts, e.g., [{"name": "NIFTY INDEX", "exchange": "NSE", "option_exchange": "NFO"}]
        
        # Entry Parameters
        self.iv_percentile_lookback_days = int(self.params.get('iv_percentile_lookback_days', 252)) # Approx 1 year
        self.iv_percentile_threshold = float(self.params.get('iv_percentile_threshold', 25.0))
        self.bb_period = int(self.params.get('bb_period', 20))
        self.bb_std_dev = float(self.params.get('bb_std_dev', 2.0))
        self.consolidation_days = int(self.params.get('consolidation_days', 5))
        self.consolidation_max_range_percent = float(self.params.get('consolidation_max_range_percent', 1.0))
        self.atm_strike_proximity_percent = float(self.params.get('atm_strike_proximity_percent', 0.5)) # For ATM filter

        # Exit Parameters
        self.profit_target_percent_on_premium = float(self.params.get('profit_target_percent_on_premium', 2.0))
        self.stop_loss_percent_on_premium = float(self.params.get('stop_loss_percent_on_premium', 15.0))
        self.max_holding_period_hours = int(self.params.get('max_holding_period_hours', 24))

        # Execution Parameters
        self.quantity_per_leg = int(config.get('execution', {}).get('quantity_per_leg', 1)) # Number of lots
        self.expiry_offset = int(config.get('expiry_offset', 0)) # 0 for current weekly expiry

        # Time control for entries
        self.trade_entry_start_time_str = self.params.get('trade_entry_start_time', '09:30:00')
        self.trade_entry_end_time_str = self.params.get('trade_entry_end_time', '15:00:00')
        self.trade_entry_start_time = dt_time.fromisoformat(self.trade_entry_start_time_str)
        self.trade_entry_end_time = dt_time.fromisoformat(self.trade_entry_end_time_str)
        
        # Cooldown to prevent immediate re-entry
        self.cooldown_period_minutes = int(self.params.get('cooldown_period_minutes', 60))
        self.last_exit_times: Dict[str, datetime] = {} # underlying_instrument_id -> exit datetime

        # State for active straddles
        # Key: underlying_instrument_id (e.g., "NSE:NIFTY INDEX")
        # Value: Dict containing details of the active straddle
        self.active_straddles: Dict[str, Dict[str, Any]] = {}
        
        # To store 15-min bar data and indicators for each underlying
        self.underlying_data_cache: Dict[str, pd.DataFrame] = {}

        # Store underlying details for easy access
        self.processed_underlyings: Dict[str, Dict[str, Any]] = {}


        self.logger.info(f"Strategy '{self.id}' (GammaScalpingStraddle) initialized. Primary TF: {self.timeframe}.")

    def initialize(self):
        super().initialize()
        # Process underlying configurations specified in the strategy's YAML
        for u_conf in self.underlying_configs:
            name = u_conf.get("name")
            exchange_str = u_conf.get("exchange")
            option_exchange_str = u_conf.get("option_exchange")

            if not name or not exchange_str or not option_exchange_str:
                self.logger.warning(f"Strategy {self.id}: Skipping underlying config due to missing name, exchange, or option_exchange: {u_conf}")
                continue
            
            underlying_instrument_id = f"{exchange_str.upper()}:{name}"
            self.processed_underlyings[underlying_instrument_id] = {
                "name": name, # Simple name like "NIFTY INDEX"
                "exchange_str": exchange_str.upper(),
                "option_exchange_str": option_exchange_str.upper(),
                "instrument_id": underlying_instrument_id
            }
            self.underlying_data_cache[underlying_instrument_id] = pd.DataFrame()
        self.logger.info(f"Strategy '{self.id}' processed {len(self.processed_underlyings)} underlyings for tracking.")


    def on_start(self):
        super().on_start()
        self.active_straddles.clear()
        self.last_exit_times.clear()

        for underlying_id, details in self.processed_underlyings.items():
            self.logger.info(f"Strategy '{self.id}' requesting 15min bar data for underlying: {underlying_id}")
            # Request data for the underlying on the strategy's primary timeframe (15m)
            # and any additional timeframes (e.g., '1d' if needed for consolidation/IV history)
            
            # Determine instrument type for underlying (e.g. INDEX, EQUITY)
            # This is a simplified inference; more robust would be to have this in config
            underlying_instrument_type = InstrumentType.INDEX if "INDEX" in details["name"].upper() else InstrumentType.EQUITY
            underlying_asset_class = AssetClass.INDEX if underlying_instrument_type == InstrumentType.INDEX else AssetClass.EQUITY

            self.request_symbol(
                symbol_name=details["name"],
                exchange_value=details["exchange_str"],
                instrument_type_value=underlying_instrument_type.value,
                asset_class_value=underlying_asset_class.value,
                timeframes_to_subscribe={self.timeframe} # Ensure 15m is subscribed
            )
            # Also request '1d' timeframe for IV and consolidation checks
            self.request_symbol(
                symbol_name=details["name"],
                exchange_value=details["exchange_str"],
                instrument_type_value=underlying_instrument_type.value,
                asset_class_value=underlying_asset_class.value,
                timeframes_to_subscribe={'1d'} 
            )


    def on_bar(self, event: BarEvent):
        if not event.instrument or not event.instrument.instrument_id:
            return

        underlying_instrument_id = event.instrument.instrument_id
        
        # Process only if the bar is for a configured underlying and is on the strategy's primary timeframe (15m)
        if underlying_instrument_id not in self.processed_underlyings or event.timeframe != self.timeframe:
            return
        
        self.logger.debug(f"Strategy '{self.id}' received 15min bar for {underlying_instrument_id}")
        self._update_underlying_data_cache(underlying_instrument_id, event)
        
        # Check if an active straddle exists for this underlying
        if underlying_instrument_id in self.active_straddles:
            # If position exists, on_market_data handles SL/TP. Check holding period here.
            self._check_holding_period_exit(underlying_instrument_id)
            return # Don't look for new entries if already in a position for this underlying

        # Check cooldown period
        if underlying_instrument_id in self.last_exit_times:
            cooldown_expiry = self.last_exit_times[underlying_instrument_id] + timedelta(minutes=self.cooldown_period_minutes)
            if datetime.now() < cooldown_expiry:
                # self.logger.debug(f"Strategy '{self.id}' for {underlying_instrument_id} is in cooldown. Ends at {cooldown_expiry.strftime('%H:%M:%S')}")
                return

        # Check trading time window for new entries
        current_time = datetime.fromtimestamp(event.timestamp).time()
        if not (self.trade_entry_start_time <= current_time <= self.trade_entry_end_time):
            # self.logger.debug(f"Strategy '{self.id}': Outside entry time window ({self.trade_entry_start_time_str} - {self.trade_entry_end_time_str}). Current: {current_time}")
            return

        # --- Entry Conditions Check ---
        # 1. IV Percentile
        current_iv, historical_iv_list = self._get_current_daily_iv_and_historical_iv(underlying_instrument_id, self.iv_percentile_lookback_days)
        if current_iv is None or not historical_iv_list: # Ensure historical list is not empty for percentile calc
            self.logger.warning(f"Strategy '{self.id}': Could not get sufficient IV data for {underlying_instrument_id}. Skipping entry check.")
            return
        
        iv_percentile = percentileofscore(historical_iv_list, current_iv)
        self.logger.debug(f"Strategy '{self.id}': {underlying_instrument_id} Current IV: {current_iv:.4f}, IV Percentile ({self.iv_percentile_lookback_days}d based on {len(historical_iv_list)} points): {iv_percentile:.2f}%")
        if iv_percentile >= self.iv_percentile_threshold:
            self.logger.debug(f"Strategy '{self.id}': IV percentile ({iv_percentile:.2f}%) >= threshold ({self.iv_percentile_threshold}%). No entry for {underlying_instrument_id}.")
            return

        # 2. Bollinger Bands and Consolidation
        df = self.underlying_data_cache[underlying_instrument_id]
        if len(df) < self.bb_period:
            self.logger.debug(f"Strategy '{self.id}': Not enough 15min bars ({len(df)}) for BB calculation (period {self.bb_period}) for {underlying_instrument_id}.")
            return
            
        self._calculate_bollinger_bands(underlying_instrument_id)
        # Refresh df after calculation
        df = self.underlying_data_cache[underlying_instrument_id] 
        if 'bb_upper' not in df.columns or 'bb_lower' not in df.columns or df[['bb_upper', 'bb_lower']].iloc[-1].isnull().any():
             self.logger.warning(f"Strategy '{self.id}': BB calculation failed or resulted in NaN for {underlying_instrument_id}")
             return

        latest_bar = df.iloc[-1]
        price = latest_bar['close']

        # 3. Consolidation Check (prior to breakout)
        in_consolidation = self._check_consolidation(underlying_instrument_id, price) # price is current 15m close
        if not in_consolidation:
            self.logger.debug(f"Strategy '{self.id}': {underlying_instrument_id} not in consolidation period.")
            return
        
        self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} IV percentile OK ({iv_percentile:.2f}%) and IN CONSOLIDATION.")

        # 4. Bollinger Band Breakout
        breakout_signal = None
        if price > latest_bar['bb_upper']:
            breakout_signal = "UP"
            self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} - Price {price:.2f} broke ABOVE Upper BB {latest_bar['bb_upper']:.2f}")
        elif price < latest_bar['bb_lower']:
            breakout_signal = "DOWN" 
            self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} - Price {price:.2f} broke BELOW Lower BB {latest_bar['bb_lower']:.2f}")

        if breakout_signal:
            self.logger.info(f"Strategy '{self.id}': ENTRY CONDITIONS MET for {underlying_instrument_id}. IV Percentile: {iv_percentile:.2f}%, Consolidation: True, BB Breakout: {breakout_signal}.")
            self._enter_straddle(underlying_instrument_id, price)
        else:
            self.logger.debug(f"Strategy '{self.id}': No BB breakout for {underlying_instrument_id}. Price: {price:.2f}, UpperBB: {latest_bar['bb_upper']:.2f}, LowerBB: {latest_bar['bb_lower']:.2f}")


    def _update_underlying_data_cache(self, underlying_instrument_id: str, event: BarEvent):
        df = self.underlying_data_cache.get(underlying_instrument_id)
        if df is None: 
            df = pd.DataFrame()
            self.underlying_data_cache[underlying_instrument_id] = df

        new_row_data = {
            'timestamp': pd.to_datetime(event.timestamp, unit='s'), 
            'open': event.open,
            'high': event.high,
            'low': event.low,
            'close': event.close,
            'volume': event.volume
        }
        
        new_row_df = pd.DataFrame([new_row_data]).set_index('timestamp')
        
        if df.empty:
            self.underlying_data_cache[underlying_instrument_id] = new_row_df
        else:
            self.underlying_data_cache[underlying_instrument_id] = pd.concat([df, new_row_df])
        
        max_cache_len = self.bb_period + (self.consolidation_days * 28) + 20 
        current_len = len(self.underlying_data_cache[underlying_instrument_id])
        if current_len > max_cache_len:
            self.underlying_data_cache[underlying_instrument_id] = self.underlying_data_cache[underlying_instrument_id].iloc[-max_cache_len:]

    def _get_current_daily_iv_and_historical_iv(self, underlying_instrument_id: str, lookback_days: int) -> Tuple[Optional[float], List[float]]:
        """
        Fetches an estimate of the current daily IV and a list of historical daily IVs for the underlying.

        This implementation assumes that:
        1. For historical IV: Daily bars of the 'underlying_instrument_id' (retrieved via self.get_bars)
           might contain an 'implied_volatility' column. This column would represent some form of
           daily aggregated/representative IV for the underlying.
        2. For current IV: It takes the 'implied_volatility' from the most recent historical daily bar
           as a proxy for the "current day's opening/representative IV".

        If 'implied_volatility' is not present in the daily bars of the underlying, this method
        will use MOCK DATA for testing purposes. In production, this should be an error or a clear indication
        that real IV data is missing.
        """
        self.logger.debug(f"Strategy '{self.id}': Attempting to fetch current and historical daily IV for {underlying_instrument_id}.")

        historical_iv_list: List[float] = []
        current_iv: Optional[float] = None

        # Fetch historical daily bars for the underlying.
        # Need `lookback_days` for history, plus one for the "current" day's IV proxy.
        daily_bars_df = self.get_bars(
            instrument_id=underlying_instrument_id,
            timeframe="1d",
            limit=lookback_days + 5 # Fetch a bit more for buffer and to get the latest point
        )

        if daily_bars_df is None or daily_bars_df.empty:
            self.logger.warning(f"Strategy '{self.id}': Could not retrieve historical daily bars for {underlying_instrument_id} to get IV.")
            # Fallback to MOCK DATA if real data is missing
            self.logger.warning(f"Strategy '{self.id}': USING MOCK IV DATA due to missing daily bars for {underlying_instrument_id}.")
            current_iv = np.random.uniform(0.10, 0.40)
            historical_iv_list = [np.random.uniform(0.10, 0.50) for _ in range(lookback_days)]
            return current_iv, historical_iv_list

        # Standardize column name for IV
        iv_column_name = None
        potential_iv_cols = ['implied_volatility', 'iv', 'IV', 'close_iv', 'day_iv'] # Common names
        for col in potential_iv_cols:
            if col in daily_bars_df.columns:
                iv_column_name = col
                break
        
        if not iv_column_name:
            self.logger.warning(f"Strategy '{self.id}': No standard IV column found in daily bars for {underlying_instrument_id}. Columns: {daily_bars_df.columns.tolist()}")
            # Fallback to MOCK DATA
            self.logger.warning(f"Strategy '{self.id}': USING MOCK IV DATA as no IV column was found for {underlying_instrument_id}.")
            current_iv = np.random.uniform(0.10, 0.40)
            historical_iv_list = [np.random.uniform(0.10, 0.50) for _ in range(lookback_days)]
            return current_iv, historical_iv_list
            
        if not pd.api.types.is_numeric_dtype(daily_bars_df[iv_column_name]):
            self.logger.warning(f"Strategy '{self.id}': IV column '{iv_column_name}' for {underlying_instrument_id} is not numeric.")
            # Fallback to MOCK DATA
            self.logger.warning(f"Strategy '{self.id}': USING MOCK IV DATA due to non-numeric IV column for {underlying_instrument_id}.")
            current_iv = np.random.uniform(0.10, 0.40)
            historical_iv_list = [np.random.uniform(0.10, 0.50) for _ in range(lookback_days)]
            return current_iv, historical_iv_list

        valid_iv_series = daily_bars_df[iv_column_name].dropna()

        if len(valid_iv_series) < 2: # Need at least one historical point and one current
            self.logger.warning(f"Strategy '{self.id}': Insufficient valid IV data points ({len(valid_iv_series)}) in daily bars for {underlying_instrument_id}.")
            # Fallback to MOCK DATA
            self.logger.warning(f"Strategy '{self.id}': USING MOCK IV DATA due to insufficient IV data points for {underlying_instrument_id}.")
            current_iv = np.random.uniform(0.10, 0.40)
            historical_iv_list = [np.random.uniform(0.10, 0.50) for _ in range(lookback_days)]
            return current_iv, historical_iv_list

        # The last point in valid_iv_series is taken as the "current" day's IV proxy
        current_iv = valid_iv_series.iloc[-1]
        
        # The historical list comprises points before the "current" one
        historical_iv_raw = valid_iv_series.iloc[:-1].tolist()
        
        # Ensure we have enough historical data for percentile; if not, use what's available
        if len(historical_iv_raw) >= lookback_days:
            historical_iv_list = historical_iv_raw[-lookback_days:]
        elif len(historical_iv_raw) > 0 : # Use available history if less than lookback_days but non-empty
            historical_iv_list = historical_iv_raw
            self.logger.info(f"Strategy '{self.id}': Using {len(historical_iv_list)} available historical IV points for {underlying_instrument_id} (less than lookback_days: {lookback_days}).")
        else: # No historical points apart from the current_iv one
             historical_iv_list = []


        if current_iv is not None:
            self.logger.info(f"Strategy '{self.id}': Current daily IV proxy for {underlying_instrument_id} (from last daily bar's '{iv_column_name}'): {current_iv:.4f}")
        else: # Should not happen if len(valid_iv_series) >= 1
            self.logger.warning(f"Strategy '{self.id}': Could not determine current daily IV proxy for {underlying_instrument_id}.")
            # Fallback to MOCK DATA if current_iv ended up None
            self.logger.warning(f"Strategy '{self.id}': USING MOCK IV DATA as current_iv is None for {underlying_instrument_id}.")
            current_iv = np.random.uniform(0.10, 0.40)
            if not historical_iv_list: # Ensure historical list also gets mock data if empty
                 historical_iv_list = [np.random.uniform(0.10, 0.50) for _ in range(lookback_days)]


        if not historical_iv_list and current_iv is not None:
            self.logger.warning(f"Strategy '{self.id}': Current IV ({current_iv:.4f}) determined for {underlying_instrument_id}, but no historical IV data available for percentile calculation. IV filter may behave unexpectedly.")
            # Provide a dummy historical list containing only current_iv if you want percentileofscore to return 100 or 0
            # historical_iv_list = [current_iv] 
            # Or, more safely, ensure the strategy handles an empty historical_iv_list gracefully (e.g., by not entering)
            # For now, returning empty list, on_bar should check this.

        return current_iv, historical_iv_list

    def _calculate_bollinger_bands(self, underlying_instrument_id: str):
        df = self.underlying_data_cache[underlying_instrument_id]
        if len(df) >= self.bb_period:
            df['bb_sma'] = df['close'].rolling(window=self.bb_period, min_periods=self.bb_period).mean()
            df['bb_std'] = df['close'].rolling(window=self.bb_period, min_periods=self.bb_period).std()
            df['bb_upper'] = df['bb_sma'] + (self.bb_std_dev * df['bb_std'])
            df['bb_lower'] = df['bb_sma'] - (self.bb_std_dev * df['bb_std'])
        else:
            # Ensure columns exist even if not enough data, filled with NaN
            for col in ['bb_sma', 'bb_std', 'bb_upper', 'bb_lower']:
                if col not in df.columns:
                    df[col] = np.nan


    def _check_consolidation(self, underlying_instrument_id: str, current_15m_close_price: float) -> bool:
        daily_bars_needed = self.consolidation_days 
        
        daily_df = self.get_bars(instrument_id=underlying_instrument_id, timeframe="1d", limit=daily_bars_needed)

        if daily_df is None or len(daily_df) < self.consolidation_days:
            self.logger.warning(f"Strategy '{self.id}': Not enough daily data ({len(daily_df) if daily_df is not None else 0} bars) for {underlying_instrument_id} to check consolidation (need {self.consolidation_days}).")
            return False

        # Use the most recent 'consolidation_days' full daily bars
        recent_daily_bars = daily_df.iloc[-self.consolidation_days:]
        
        # Calculate max_allowed_daily_range based on the current 15m closing price of the underlying
        # This makes the consolidation range dynamic to the current price level.
        max_allowed_daily_range_value = current_15m_close_price * (self.consolidation_max_range_percent / 100.0)
        
        for idx, row in recent_daily_bars.iterrows():
            daily_range = row['high'] - row['low']
            if daily_range > max_allowed_daily_range_value:
                self.logger.debug(f"Strategy '{self.id}': Consolidation check failed for {underlying_instrument_id}. Day {idx.strftime('%Y-%m-%d')} range {daily_range:.2f} > allowed {max_allowed_daily_range_value:.2f} (based on current 15m price {current_15m_close_price:.2f})")
                return False
        
        self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} PASSED consolidation check for last {self.consolidation_days} days (max allowed daily range: {max_allowed_daily_range_value:.2f}).")
        return True

    def _enter_straddle(self, underlying_instrument_id: str, underlying_price: float):
        underlying_details = self.processed_underlyings[underlying_instrument_id]
        underlying_simple_name = underlying_details["name"] 
        
        strike_interval = self.option_manager.strike_intervals.get(underlying_simple_name)
        if strike_interval is None:
            market_config = self.config.get('market', {})
            u_conf_list = market_config.get('underlyings', [])
            u_conf = next((uc for uc in u_conf_list if uc.get("symbol") == underlying_simple_name or uc.get("name") == underlying_simple_name), None)
            strike_interval = u_conf.get("strike_interval", 50.0) if u_conf else 50.0
            self.logger.warning(f"Strategy '{self.id}': Using fallback/default strike interval {strike_interval} for {underlying_simple_name}")

        atm_strike_price = round(underlying_price / strike_interval) * strike_interval
        self.logger.info(f"Strategy '{self.id}': Calculated ATM strike for {underlying_simple_name} at price {underlying_price:.2f} is {atm_strike_price:.2f}")

        if abs(underlying_price - atm_strike_price) / underlying_price > (self.atm_strike_proximity_percent / 100.0): # Check against underlying_price
            self.logger.info(f"Strategy '{self.id}': ATM strike {atm_strike_price:.2f} is not within {self.atm_strike_proximity_percent}% of underlying price {underlying_price:.2f}. Skipping entry.")
            return

        call_instrument = self.option_manager.get_option_instrument(
            underlying_simple_name, atm_strike_price, OptionType.CALL.value, self.expiry_offset
        )
        put_instrument = self.option_manager.get_option_instrument(
            underlying_simple_name, atm_strike_price, OptionType.PUT.value, self.expiry_offset
        )

        if not call_instrument or not call_instrument.instrument_id:
            self.logger.error(f"Strategy '{self.id}': Could not get CALL option instrument for {underlying_simple_name} ATM {atm_strike_price}.")
            return
        if not put_instrument or not put_instrument.instrument_id:
            self.logger.error(f"Strategy '{self.id}': Could not get PUT option instrument for {underlying_simple_name} ATM {atm_strike_price}.")
            return

        option_timeframes = {self.timeframe} 
        # Prepare option_details for request_symbol
        call_option_details = {
            'option_type': call_instrument.option_type,
            'strike_price': call_instrument.strike_price,
            'expiry_date': call_instrument.expiry_date, # Should be datetime.date object
            'underlying_symbol_key': call_instrument.underlying_symbol_key # If available
        }
        put_option_details = {
            'option_type': put_instrument.option_type,
            'strike_price': put_instrument.strike_price,
            'expiry_date': put_instrument.expiry_date, # Should be datetime.date object
            'underlying_symbol_key': put_instrument.underlying_symbol_key # If available
        }

        self.request_symbol(symbol_name=call_instrument.symbol, exchange_value=call_instrument.exchange.value,
                            instrument_type_value=InstrumentType.OPTION.value, asset_class_value=AssetClass.OPTIONS.value,
                            timeframes_to_subscribe=option_timeframes, option_details=call_option_details)
        self.request_symbol(symbol_name=put_instrument.symbol, exchange_value=put_instrument.exchange.value,
                            instrument_type_value=InstrumentType.OPTION.value, asset_class_value=AssetClass.OPTIONS.value,
                            timeframes_to_subscribe=option_timeframes, option_details=put_option_details)
        
        self.logger.info(f"Strategy '{self.id}': Generating BUY signal for CALL {call_instrument.instrument_id} (Qty: {self.quantity_per_leg} lots)")
        self.generate_signal(
            instrument_id=call_instrument.instrument_id,
            signal_type=SignalType.BUY,
            data={'quantity': self.quantity_per_leg, 'order_type': 'MARKET', 'product_type': 'INTRADAY'}
        )
        self.logger.info(f"Strategy '{self.id}': Generating BUY signal for PUT {put_instrument.instrument_id} (Qty: {self.quantity_per_leg} lots)")
        self.generate_signal(
            instrument_id=put_instrument.instrument_id,
            signal_type=SignalType.BUY,
            data={'quantity': self.quantity_per_leg, 'order_type': 'MARKET', 'product_type': 'INTRADAY'}
        )

        call_lot_size = call_instrument.lot_size if call_instrument.lot_size and call_instrument.lot_size > 0 else 1 
        put_lot_size = put_instrument.lot_size if put_instrument.lot_size and put_instrument.lot_size > 0 else 1   
        if call_instrument.lot_size is None or call_instrument.lot_size <=0 or put_instrument.lot_size is None or put_instrument.lot_size <=0 :
            self.logger.warning(f"Strategy '{self.id}': Lot size missing or invalid for {call_instrument.symbol} ({call_instrument.lot_size}) or {put_instrument.symbol} ({put_instrument.lot_size}). Calculations might be inaccurate. Using 1 as fallback.")


        self.active_straddles[underlying_instrument_id] = {
            'call_option_id': call_instrument.instrument_id,
            'put_option_id': put_instrument.instrument_id,
            'call_instrument': call_instrument,
            'put_instrument': put_instrument,
            'entry_timestamp': datetime.now(), 
            'call_entry_price': None, 
            'put_entry_price': None,  
            'call_filled_quantity': 0, # In terms of contracts/shares (lots * lot_size)
            'put_filled_quantity': 0,  # In terms of contracts/shares (lots * lot_size)
            'total_entry_premium_paid_actual': 0.0, 
            'call_lot_size': call_lot_size,
            'put_lot_size': put_lot_size, 
            'stop_loss_premium_level': None, 
            'target_profit_premium_level': None, 
            'current_call_mtm_price': None,
            'current_put_mtm_price': None,
        }
        self.logger.info(f"Strategy '{self.id}': Straddle entry initiated for {underlying_instrument_id}. Call: {call_instrument.symbol}, Put: {put_instrument.symbol}. Waiting for fills.")


    def on_fill(self, event: FillEvent):
        super().on_fill(event)
        
        for underlying_id, straddle_info in list(self.active_straddles.items()): # Iterate copy
            is_call_fill = event.instrument_id == straddle_info.get('call_option_id')
            is_put_fill = event.instrument_id == straddle_info.get('put_option_id')

            if (is_call_fill or is_put_fill) and event.order_side == OrderSide.BUY:
                self.logger.info(f"Strategy '{self.id}': Received ENTRY FILL for {event.instrument_id} (Order: {event.order_id}), "
                                 f"Price: {event.fill_price:.2f}, Qty (lots): {event.fill_quantity}.")
                
                leg_type = 'call' if is_call_fill else 'put'
                
                # Assuming event.fill_quantity is in lots for options
                filled_contracts_this_fill = event.fill_quantity * straddle_info[f'{leg_type}_lot_size']
                
                # Update entry price: if partially filled multiple times, average price would be more complex.
                # For simplicity, assume each BUY fill is for the full leg or a significant part.
                # If multiple fills for one leg, this logic would need weighted averaging for entry_price.
                # Current logic: last fill price for the leg.
                straddle_info[f'{leg_type}_entry_price'] = event.fill_price 
                straddle_info[f'{leg_type}_filled_quantity'] += filled_contracts_this_fill


                # Check if both legs have received at least one fill to calculate combined premium
                if straddle_info.get('call_entry_price') is not None and straddle_info.get('put_entry_price') is not None:
                    # Calculate total premium based on the number of lots configured per leg
                    # and their respective entry prices and lot sizes.
                    call_premium_paid_total = straddle_info['call_entry_price'] * straddle_info['call_lot_size'] * self.quantity_per_leg
                    put_premium_paid_total = straddle_info['put_entry_price'] * straddle_info['put_lot_size'] * self.quantity_per_leg
                    straddle_info['total_entry_premium_paid_actual'] = call_premium_paid_total + put_premium_paid_total

                    if straddle_info['total_entry_premium_paid_actual'] > 0:
                        straddle_info['stop_loss_premium_level'] = straddle_info['total_entry_premium_paid_actual'] * (1 - self.stop_loss_percent_on_premium / 100.0)
                        straddle_info['target_profit_premium_level'] = straddle_info['total_entry_premium_paid_actual'] * (1 + self.profit_target_percent_on_premium / 100.0)
                        self.logger.info(f"Strategy '{self.id}': Straddle for {underlying_id} entry prices recorded. "
                                         f"Total Target Premium Paid (for {self.quantity_per_leg} lots/leg): {straddle_info['total_entry_premium_paid_actual']:.2f}. "
                                         f"SL Level: {straddle_info['stop_loss_premium_level']:.2f}, "
                                         f"TP Level: {straddle_info['target_profit_premium_level']:.2f}.")
                    else:
                        self.logger.error(f"Strategy '{self.id}': Total entry premium for {underlying_id} is zero or negative after fills. SL/TP cannot be set accurately.")
                # break # Found the relevant straddle, no need to check others for this fill

            elif (is_call_fill or is_put_fill) and event.order_side == OrderSide.SELL:
                self.logger.info(f"Strategy '{self.id}': Received EXIT FILL for {event.instrument_id} (Order: {event.order_id}), "
                                 f"Price: {event.fill_price:.2f}, Qty (lots): {event.fill_quantity}.")
                
                leg_type = 'call' if is_call_fill else 'put'
                straddle_info[f'{leg_type}_filled_quantity'] -= event.fill_quantity * straddle_info[f'{leg_type}_lot_size']


                # Check if both legs are fully exited
                # This assumes that exit signals are for the full quantity.
                # A more robust check would be if filled_quantity for both legs is <= 0
                # For simplicity, if any SELL fill comes, we assume the straddle is being closed.
                # The _exit_straddle method already sent signals for both legs.
                # We can refine this to check if total filled sell quantity matches total buy quantity.
                
                # If this fill means the leg is closed, mark it.
                # A simple check: if a sell fill comes, assume that leg is being closed.
                # If both legs have reported sell fills (or their quantities go to 0), then cleanup.
                # For now, any sell fill on either leg triggers cleanup.
                self._cleanup_active_straddle(underlying_id, f"Exit fill received for {leg_type} leg")
                break


    def on_market_data(self, event: MarketDataEvent):
        if not event.instrument or not event.instrument.instrument_id or not event.data:
            return

        option_id = event.instrument.instrument_id
        current_price = event.data.get(MarketDataType.LAST_PRICE.value)
        if current_price is None: return
        try:
            current_price = float(current_price)
        except ValueError:
            self.logger.warning(f"Strategy '{self.id}': Invalid price '{current_price}' in MarketDataEvent for {option_id}")
            return

        for underlying_id, straddle_info in list(self.active_straddles.items()): 
            updated_mtm = False
            if option_id == straddle_info.get('call_option_id'):
                straddle_info['current_call_mtm_price'] = current_price
                updated_mtm = True
            elif option_id == straddle_info.get('put_option_id'):
                straddle_info['current_put_mtm_price'] = current_price
                updated_mtm = True

            if updated_mtm:
                call_mtm = straddle_info.get('current_call_mtm_price')
                put_mtm = straddle_info.get('current_put_mtm_price')
                
                if call_mtm is not None and put_mtm is not None and \
                   straddle_info.get('total_entry_premium_paid_actual', 0) > 0 and \
                   straddle_info.get('stop_loss_premium_level') is not None:

                    # Calculate current MTM premium based on configured lots
                    current_total_mtm_premium = (call_mtm * straddle_info['call_lot_size'] * self.quantity_per_leg) + \
                                                (put_mtm * straddle_info['put_lot_size'] * self.quantity_per_leg)
                    
                    self.logger.debug(f"Strategy '{self.id}': MTM update for {underlying_id}. Current Straddle Premium: {current_total_mtm_premium:.2f}. "
                                     f"Entry Premium: {straddle_info['total_entry_premium_paid_actual']:.2f}. "
                                     f"SL: {straddle_info['stop_loss_premium_level']:.2f}, TP: {straddle_info['target_profit_premium_level']:.2f}")

                    if current_total_mtm_premium >= straddle_info['target_profit_premium_level']:
                        self.logger.info(f"Strategy '{self.id}': PROFIT TARGET hit for straddle on {underlying_id}. "
                                         f"Current Premium: {current_total_mtm_premium:.2f}, Target: {straddle_info['target_profit_premium_level']:.2f}. Exiting.")
                        self._exit_straddle(underlying_id, "Profit target hit")
                        # break # Exit loop after initiating exit for one straddle
                    elif current_total_mtm_premium <= straddle_info['stop_loss_premium_level']:
                        self.logger.info(f"Strategy '{self.id}': STOP LOSS hit for straddle on {underlying_id}. "
                                         f"Current Premium: {current_total_mtm_premium:.2f}, SL: {straddle_info['stop_loss_premium_level']:.2f}. Exiting.")
                        self._exit_straddle(underlying_id, "Stop loss hit")
                        # break # Exit loop after initiating exit for one straddle
                # No break here, allow MTM update for other straddles if any (though unlikely for same option_id)


    def _check_holding_period_exit(self, underlying_instrument_id: str):
        if underlying_instrument_id not in self.active_straddles:
            return
        
        straddle_info = self.active_straddles[underlying_instrument_id]
        entry_time = straddle_info.get('entry_timestamp')
        if not entry_time: return 

        if datetime.now() >= entry_time + timedelta(hours=self.max_holding_period_hours):
            self.logger.info(f"Strategy '{self.id}': MAX HOLDING PERIOD ({self.max_holding_period_hours} hrs) reached for straddle on {underlying_instrument_id}. Exiting.")
            self._exit_straddle(underlying_instrument_id, "Max holding period reached")


    def _exit_straddle(self, underlying_instrument_id: str, reason: str):
        if underlying_instrument_id not in self.active_straddles:
            self.logger.warning(f"Strategy '{self.id}': Attempted to exit straddle for {underlying_instrument_id}, but no active straddle found.")
            return

        straddle_info = self.active_straddles[underlying_instrument_id]
        
        call_id_to_exit = straddle_info.get('call_option_id')
        put_id_to_exit = straddle_info.get('put_option_id')
        
        call_qty_to_exit = self.quantity_per_leg 
        put_qty_to_exit = self.quantity_per_leg

        if call_id_to_exit:
            self.logger.info(f"Strategy '{self.id}': Exiting CALL {call_id_to_exit} (Qty: {call_qty_to_exit} lots). Reason: {reason}")
            self.generate_signal(
                instrument_id=call_id_to_exit,
                signal_type=SignalType.SELL,
                data={'quantity': call_qty_to_exit, 'order_type': 'MARKET', 'reason': reason, 'product_type': 'INTRADAY'}
            )
        if put_id_to_exit:
            self.logger.info(f"Strategy '{self.id}': Exiting PUT {put_id_to_exit} (Qty: {put_qty_to_exit} lots). Reason: {reason}")
            self.generate_signal(
                instrument_id=put_id_to_exit,
                signal_type=SignalType.SELL,
                data={'quantity': put_qty_to_exit, 'order_type': 'MARKET', 'reason': reason, 'product_type': 'INTRADAY'}
            )
        
        # Important: Do not clean up active_straddles here.
        # Cleanup should happen in on_fill when SELL orders are confirmed,
        # or if orders are rejected/expired and need manual intervention simulation.
        # For now, we assume fills will arrive and trigger cleanup via _cleanup_active_straddle.


    def _cleanup_active_straddle(self, underlying_instrument_id: str, reason: str):
        if underlying_instrument_id in self.active_straddles:
            removed_straddle = self.active_straddles.pop(underlying_instrument_id)
            self.last_exit_times[underlying_instrument_id] = datetime.now()
            call_id = removed_straddle.get('call_option_id', 'N/A')
            put_id = removed_straddle.get('put_option_id', 'N/A')
            self.logger.info(f"Strategy '{self.id}': Straddle for {underlying_instrument_id} (Call: {call_id}, Put: {put_id}) "
                             f"removed from active list. Reason: {reason}. Cooldown started.")
        else:
            self.logger.debug(f"Strategy '{self.id}': Cleanup called for {underlying_instrument_id}, but no active straddle found (possibly already cleaned).")


    def on_stop(self):
        super().on_stop()
        self.logger.info(f"Strategy '{self.id}' on_stop: Exiting all open straddles if any.")
        for underlying_id in list(self.active_straddles.keys()):
            self._exit_straddle(underlying_id, "Strategy stop command")
        
        self.underlying_data_cache.clear()
        self.active_straddles.clear()
        self.last_exit_times.clear()
        self.logger.info(f"Strategy '{self.id}' stopped and internal states cleared.")

    def run_iteration(self):
        super().run_iteration() 
        # If on_bar is not guaranteed to run frequently enough for all underlyings with active positions
        # (e.g., if underlying stops trading but options are still live),
        # this is where you might add periodic checks for holding period or other time-based exits.
        # For a 24-hour holding period, a 15-minute on_bar check is likely sufficient.
        # However, if strategy has its own thread, this is a good place for such checks.
        # now = datetime.now()
        # for underlying_id in list(self.active_straddles.keys()):
        #     if underlying_id in self.active_straddles: # Check again as it might be removed by another thread/event
        #         self._check_holding_period_exit(underlying_id)
        pass


