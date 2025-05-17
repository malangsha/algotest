import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dt_time
from typing import Dict, List, Optional, Any, Set, Tuple

from strategies.base_strategy import OptionStrategy
from models.events import BarEvent, MarketDataEvent, SignalEvent, FillEvent
from models.instrument import Instrument, InstrumentType, AssetClass
from utils.constants import SignalType, MarketDataType, OptionType, OrderSide, Exchange
from strategies.strategy_registry import StrategyRegistry
from core.logging_manager import get_logger

@StrategyRegistry.register('iv_crush_calendar_condor_strategy')
class IVCrushCalendarCondorStrategy(OptionStrategy):
    """
    Option-Selling Strategy: "IV Crush Calendar Iron Condor"

    Logic:
    1. Entry (Daily @ 15:00):
       - Sell a 7-DTE Iron Condor (short 16D call/put spreads, long 8D wings)
       - Simultaneously, Buy a 21-DTE Iron Condor using the *same four strikes*.
       - Conditions:
         - 7D IV / 21D IV skew > configured threshold (e.g., 15%)
         - Underlying spot price is within a band around its 20D SMA (e.g., +/- 1.5%)
       - Net credit for the combined 8-leg position must be >= 1.2x estimated transaction costs.
    2. Exit:
       - If current net value of the 8-leg position shows a profit of 60% of the max profit (initial net credit).
       - OR, when the short 7-DTE options are near expiry (e.g., 0-1 DTE).
       - OR, if stop-loss is hit (position value deteriorates by 1.5x initial net credit).
    3. Strikes:
       - Short 7DTE options (Call & Put) at 16 Delta.
       - Long 7DTE wings (Call & Put) at 8 Delta.
       - 21DTE options use the same four strikes as the 7DTE legs.
    4. Timeframe: Daily check at/after 15:00 for entry/adjustment. Option MTM via MarketDataEvents.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager, broker=None):
        super().__init__(strategy_id, config, data_manager, option_manager, portfolio_manager, event_manager, broker)

        self.params = config.get('parameters', {})
        self.underlying_configs = config.get('underlyings', [])
        
        # Entry Parameters
        self.iv_skew_threshold = float(self.params.get('iv_skew_threshold_percent', 15.0)) / 100.0 # e.g., 0.15
        self.sma_period = int(self.params.get('sma_period', 20))
        self.sma_band_percent = float(self.params.get('sma_band_percent', 1.5)) / 100.0
        
        self.short_dte_target = int(self.params.get('short_dte_target', 7))
        self.long_dte_target = int(self.params.get('long_dte_target', 21))
        
        self.short_leg_delta = float(self.params.get('short_leg_delta', 16.0)) # Target delta (e.g., 16)
        self.long_wing_delta = float(self.params.get('long_wing_delta', 8.0))   # Target delta (e.g., 8)

        self.min_credit_to_cost_ratio = float(self.params.get('min_credit_to_cost_ratio', 1.2))
        self.estimated_cost_per_leg = float(self.params.get('estimated_cost_per_leg', 25.0)) # Placeholder

        # Exit Parameters
        self.profit_target_percent_of_max = float(self.params.get('profit_target_percent_of_max_profit', 60.0)) / 100.0
        self.stop_loss_multiplier_on_credit = float(self.params.get('stop_loss_multiplier_on_credit', 1.5))
        self.exit_before_short_expiry_days = int(self.params.get('exit_before_short_expiry_days', 1)) # Exit if 7DTE leg has <=1 DTE

        # Execution
        self.quantity_per_spread = int(config.get('execution', {}).get('quantity_per_spread', 1)) # Lots per spread

        # Time control
        self.check_time_str = self.params.get('daily_check_time', '15:00:00')
        self.check_time = dt_time.fromisoformat(self.check_time_str)
        
        self.active_condors: Dict[str, Dict[str, Any]] = {} # underlying_instrument_id -> condor_details
        self.underlying_data_cache: Dict[str, pd.DataFrame] = {} # For daily SMA
        self.processed_underlyings: Dict[str, Dict[str, Any]] = {}

        # VIX for position sizing (simplified for now)
        self.vix_symbol_id = self.params.get('vix_instrument_id', 'NSE:INDIAVIX') # Example
        self.base_capital_alloc_percent = float(self.params.get('base_capital_allocation_percent', 5.0)) / 100.0
        self.max_capital_alloc_percent_low_vix = float(self.params.get('max_capital_allocation_percent_low_vix', 7.5)) / 100.0
        self.low_vix_threshold = float(self.params.get('low_vix_threshold', 15.0))

        self.logger.info(f"Strategy '{self.id}' (IVCrushCalendarCondor) initialized. Daily check time: {self.check_time_str}.")

    def initialize(self):
        super().initialize()
        for u_conf in self.underlying_configs:
            name = u_conf.get("name")
            exchange_str = u_conf.get("exchange")
            option_exchange_str = u_conf.get("option_exchange")
            if not name or not exchange_str or not option_exchange_str:
                self.logger.warning(f"Strategy {self.id}: Skipping underlying config: {u_conf}")
                continue
            
            underlying_instrument_id = f"{exchange_str.upper()}:{name}"
            self.processed_underlyings[underlying_instrument_id] = {
                "name": name, "exchange_str": exchange_str.upper(),
                "option_exchange_str": option_exchange_str.upper(),
                "instrument_id": underlying_instrument_id
            }
            self.underlying_data_cache[underlying_instrument_id] = pd.DataFrame()
        self.logger.info(f"Strategy '{self.id}' processed {len(self.processed_underlyings)} underlyings.")

    def on_start(self):
        super().on_start()
        self.active_condors.clear()
        for underlying_id, details in self.processed_underlyings.items():
            underlying_instrument_type = InstrumentType.INDEX if "INDEX" in details["name"].upper() else InstrumentType.EQUITY
            underlying_asset_class = AssetClass.INDEX if underlying_instrument_type == InstrumentType.INDEX else AssetClass.EQUITY
            
            self.request_symbol( # For underlying's daily data
                symbol_name=details["name"], exchange_value=details["exchange_str"],
                instrument_type_value=underlying_instrument_type.value, asset_class_value=underlying_asset_class.value,
                timeframes_to_subscribe={'1d'} # Strategy timeframe is daily
            )
        # Request VIX data if configured
        if self.vix_symbol_id:
             parts = self.vix_symbol_id.split(':')
             if len(parts) == 2:
                self.request_symbol(symbol_name=parts[1], exchange_value=parts[0], instrument_type_value=InstrumentType.INDEX.value, asset_class_value=AssetClass.INDEX.value, timeframes_to_subscribe={'1d'})
             else:
                self.logger.warning(f"Strategy {self.id}: Invalid VIX instrument ID format: {self.vix_symbol_id}")


    def on_bar(self, event: BarEvent):
        if not event.instrument or not event.instrument.instrument_id: return

        instrument_id = event.instrument.instrument_id
        current_dt = datetime.fromtimestamp(event.timestamp)

        # Handle VIX data if this bar is for VIX
        if instrument_id == self.vix_symbol_id and event.timeframe == '1d':
            # Store latest VIX, e.g., self.latest_vix = event.close
            # This part is for position sizing, not implemented in detail here
            return

        if instrument_id not in self.processed_underlyings or event.timeframe != '1d':
            return

        self.logger.debug(f"Strategy '{self.id}' received daily bar for {instrument_id} @ {current_dt.strftime('%Y-%m-%d')}")
        self._update_underlying_daily_cache(instrument_id, event)

        # Check if it's time for the daily check
        if current_dt.time() < self.check_time:
            # self.logger.debug(f"Strategy '{self.id}': Too early for daily check. Current: {current_dt.time()}, Check: {self.check_time}")
            return
        
        self.logger.info(f"Strategy '{self.id}': Performing daily check for {instrument_id} at {current_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        if instrument_id in self.active_condors:
            self._manage_active_condor(instrument_id, current_dt.date())
        else:
            self._check_entry_conditions(instrument_id, event.close, current_dt.date())


    def _update_underlying_daily_cache(self, underlying_instrument_id: str, event: BarEvent):
        df = self.underlying_data_cache.get(underlying_instrument_id, pd.DataFrame())
        new_row = pd.DataFrame([{
            'timestamp': pd.to_datetime(event.timestamp, unit='s'),
            'open': event.open, 'high': event.high, 'low': event.low, 'close': event.close, 'volume': event.volume
        }]).set_index('timestamp')
        
        if df.empty: self.underlying_data_cache[underlying_instrument_id] = new_row
        else: self.underlying_data_cache[underlying_instrument_id] = pd.concat([df, new_row])
        
        max_cache_len = self.sma_period + 50 # Enough for SMA and buffer
        if len(self.underlying_data_cache[underlying_instrument_id]) > max_cache_len:
            self.underlying_data_cache[underlying_instrument_id] = self.underlying_data_cache[underlying_instrument_id].iloc[-max_cache_len:]

    def _get_iv_for_dte(self, underlying_simple_name: str, target_dte: int, current_date: datetime.date) -> Optional[float]:
        """
        Placeholder: Fetches a representative IV for options of the underlying with a specific DTE.
        This would ideally query OptionManager or a dedicated IV service.
        """
        self.logger.warning(f"Strategy '{self.id}': _get_iv_for_dte is a PLACEHOLDER. Real IV data source for DTE {target_dte} needed for {underlying_simple_name}.")
        # Mock implementation:
        # Find closest expiry to target_dte
        # expiry_dates = self.option_manager.get_expiry_dates(underlying_simple_name) # Needs to be robust
        # if not expiry_dates: return None
        # closest_expiry = min(expiry_dates, key=lambda d: abs((d - current_date).days - target_dte))
        # dte = (closest_expiry - current_date).days
        # atm_strike = self.option_manager.get_atm_strike(underlying_simple_name) # Needs underlying_id
        # if atm_strike is None: return None
        # atm_call = self.option_manager.get_option_instrument(underlying_simple_name, atm_strike, "CE", expiry_date=closest_expiry) # Needs get_option_instrument to accept expiry_date
        # if atm_call and atm_call.instrument_id:
        #     # iv = self.data_manager.get_option_iv(atm_call.instrument_id) # Hypothetical
        #     return np.random.uniform(0.10, 0.30) # Mock IV
        return np.random.uniform(0.10, 0.30) + (0.01 * target_dte) # Slight skew for mock

    def _get_strikes_for_delta(self, underlying_simple_name: str, underlying_price: float, target_delta: float, option_type: OptionType, target_dte: int, current_date: datetime.date) -> Optional[float]:
        """
        Placeholder: Finds the strike price for an option of the given type and DTE that has the closest delta to target_delta.
        This would ideally query OptionManager or a service that provides options chains with Greeks.
        """
        self.logger.warning(f"Strategy '{self.id}': _get_strikes_for_delta is a PLACEHOLDER. Real option chain with Greeks needed for {underlying_simple_name} {option_type.value} DTE {target_dte} Delta {target_delta}.")
        # Mock implementation:
        # atm_strike = round(underlying_price / 50) * 50 # Assuming 50 strike interval for NIFTY like
        # delta_factor = (50 - target_delta) / 20 # Rough approximation
        # if option_type == OptionType.CALL:
        #     return atm_strike + delta_factor * 100
        # else: # PUT
        #     return atm_strike - delta_factor * 100
        strike_interval = self.option_manager.strike_intervals.get(underlying_simple_name, 50.0)
        atm_strike = round(underlying_price / strike_interval) * strike_interval
        
        # Simplified mock: OTM by a few strikes based on delta
        num_strikes_otm = 0
        if target_delta == 16: num_strikes_otm = 3 if underlying_simple_name == "NIFTY INDEX" else 2 # Example
        elif target_delta == 8: num_strikes_otm = 5 if underlying_simple_name == "NIFTY INDEX" else 3 # Example
        
        if option_type == OptionType.CALL:
            return atm_strike + (num_strikes_otm * strike_interval)
        else: # PUT
            return atm_strike - (num_strikes_otm * strike_interval)


    def _check_entry_conditions(self, underlying_instrument_id: str, current_price: float, current_date: datetime.date):
        underlying_details = self.processed_underlyings[underlying_instrument_id]
        underlying_simple_name = underlying_details["name"]

        # 1. Spot price vs SMA
        df_daily = self.underlying_data_cache[underlying_instrument_id]
        if len(df_daily) < self.sma_period:
            self.logger.debug(f"Strategy '{self.id}': Not enough daily bars for SMA ({len(df_daily)} < {self.sma_period}) for {underlying_instrument_id}.")
            return
        
        sma = df_daily['close'].rolling(window=self.sma_period).mean().iloc[-1]
        lower_sma_band = sma * (1 - self.sma_band_percent)
        upper_sma_band = sma * (1 + self.sma_band_percent)

        if not (lower_sma_band <= current_price <= upper_sma_band):
            self.logger.debug(f"Strategy '{self.id}': {underlying_instrument_id} price {current_price:.2f} outside SMA band ({lower_sma_band:.2f} - {upper_sma_band:.2f}). No entry.")
            return
        self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} price {current_price:.2f} is WITHIN SMA band.")

        # 2. IV Skew (7D vs 21D)
        iv_7dte = self._get_iv_for_dte(underlying_simple_name, self.short_dte_target, current_date)
        iv_21dte = self._get_iv_for_dte(underlying_simple_name, self.long_dte_target, current_date)

        if iv_7dte is None or iv_21dte is None or iv_21dte == 0:
            self.logger.warning(f"Strategy '{self.id}': Could not get IV for DTEs {self.short_dte_target}/{self.long_dte_target} for {underlying_simple_name}. Cannot check skew.")
            return
        
        iv_skew = (iv_7dte / iv_21dte) - 1.0
        self.logger.info(f"Strategy '{self.id}': {underlying_simple_name} IVs - {self.short_dte_target}DTE: {iv_7dte:.4f}, {self.long_dte_target}DTE: {iv_21dte:.4f}. Skew: {iv_skew*100:.2f}%")

        if iv_skew <= self.iv_skew_threshold: # Strategy enters if 7D IV is significantly HIGHER
            self.logger.debug(f"Strategy '{self.id}': IV skew {iv_skew*100:.2f}% not > threshold {self.iv_skew_threshold*100:.2f}%. No entry for {underlying_instrument_id}.")
            return
        self.logger.info(f"Strategy '{self.id}': {underlying_instrument_id} IV SKEW OK ({iv_skew*100:.2f}% > {self.iv_skew_threshold*100:.2f}%).")
        
        # All entry conditions met, proceed to construct and enter condor
        self.logger.info(f"Strategy '{self.id}': ALL ENTRY CONDITIONS MET for {underlying_instrument_id}. Proceeding to construct Calendar Condor.")
        self._enter_calendar_condor(underlying_instrument_id, current_price, current_date)


    def _enter_calendar_condor(self, underlying_instrument_id: str, underlying_price: float, current_date: datetime.date):
        underlying_details = self.processed_underlyings[underlying_instrument_id]
        underlying_simple_name = underlying_details["name"]
        
        # --- Determine Strikes for 7DTE Iron Condor ---
        # Short Call (16D)
        sc_strike = self._get_strikes_for_delta(underlying_simple_name, underlying_price, self.short_leg_delta, OptionType.CALL, self.short_dte_target, current_date)
        # Long Call Wing (8D)
        lc_strike = self._get_strikes_for_delta(underlying_simple_name, underlying_price, self.long_wing_delta, OptionType.CALL, self.short_dte_target, current_date)
        # Short Put (16D)
        sp_strike = self._get_strikes_for_delta(underlying_simple_name, underlying_price, self.short_leg_delta, OptionType.PUT, self.short_dte_target, current_date)
        # Long Put Wing (8D)
        lp_strike = self._get_strikes_for_delta(underlying_simple_name, underlying_price, self.long_wing_delta, OptionType.PUT, self.short_dte_target, current_date)

        if not all([sc_strike, lc_strike, sp_strike, lp_strike]):
            self.logger.error(f"Strategy '{self.id}': Failed to determine all strikes for 7DTE condor on {underlying_simple_name}. Aborting entry.")
            return
        
        # Ensure wing strikes are further OTM
        if lc_strike <= sc_strike or lp_strike >= sp_strike:
            self.logger.error(f"Strategy '{self.id}': Invalid wing strikes relative to short strikes for 7DTE condor. SC:{sc_strike}, LC:{lc_strike}, SP:{sp_strike}, LP:{lp_strike}. Aborting.")
            return

        self.logger.info(f"Strategy '{self.id}': Determined 7DTE Condor Strikes for {underlying_simple_name}: "
                         f"ShortCall@{sc_strike}, LongCall@{lc_strike}, ShortPut@{sp_strike}, LongPut@{lp_strike}")

        legs = []
        total_estimated_premium_change = 0.0 # Positive for credit, negative for debit
        
        # --- Leg Definitions ---
        # Short 7DTE Iron Condor (Net Credit expected)
        legs_info = [
            {'action': SignalType.SELL_CALL, 'strike': sc_strike, 'dte_target': self.short_dte_target, 'role': 'short_call_7dte'},
            {'action': SignalType.BUY_CALL,   'strike': lc_strike, 'dte_target': self.short_dte_target, 'role': 'long_call_7dte'},
            {'action': SignalType.SELL_PUT,  'strike': sp_strike, 'dte_target': self.short_dte_target, 'role': 'short_put_7dte'},
            {'action': SignalType.BUY_PUT,    'strike': lp_strike, 'dte_target': self.short_dte_target, 'role': 'long_put_7dte'},
        # Long 21DTE Iron Condor (Net Debit expected) - using SAME strikes
            {'action': SignalType.BUY_CALL,   'strike': sc_strike, 'dte_target': self.long_dte_target,  'role': 'long_call_21dte'},
            {'action': SignalType.SELL_CALL, 'strike': lc_strike, 'dte_target': self.long_dte_target,  'role': 'short_call_21dte'},
            {'action': SignalType.BUY_PUT,    'strike': sp_strike, 'dte_target': self.long_dte_target,  'role': 'long_put_21dte'},
            {'action': SignalType.SELL_PUT,  'strike': lp_strike, 'dte_target': self.long_dte_target,  'role': 'short_put_21dte'},
        ]

        condor_legs_data = {} # To store instrument objects and details

        for leg_info in legs_info:
            option_type = OptionType.CALL if "CALL" in leg_info['action'].name else OptionType.PUT
            
            # Find closest actual expiry date to target DTE
            # This logic should be robust in OptionManager or a utility
            actual_expiry_date = self._get_actual_expiry_for_dte(underlying_simple_name, leg_info['dte_target'], current_date)
            if not actual_expiry_date:
                self.logger.error(f"Strategy '{self.id}': Could not find expiry for DTE {leg_info['dte_target']} for {underlying_simple_name}. Aborting leg.")
                return

            instrument = self.option_manager.get_option_instrument(
                underlying_simple_name, leg_info['strike'], option_type.value, 
                expiry_date=actual_expiry_date # Pass specific date
            )
            if not instrument or not instrument.instrument_id:
                self.logger.error(f"Strategy '{self.id}': Failed to get instrument for {leg_info['role']} @ K={leg_info['strike']}. Aborting entry.")
                return

            # Request feed for this option if not already active
            self.request_symbol(
                symbol_name=instrument.symbol, exchange_value=instrument.exchange.value,
                instrument_type_value=InstrumentType.OPTION.value, asset_class_value=AssetClass.OPTIONS.value,
                timeframes_to_subscribe={'1m'}, # For MTM, or just rely on MarketDataEvent
                option_details=instrument.get_option_details_dict()
            )
            
            # Estimate premium (placeholder - needs live tick or recent close)
            # In a live system, you'd fetch the current market price (bid for sell, ask for buy)
            option_tick = self.data_manager.get_latest_tick(instrument.instrument_id)
            est_price = None
            if option_tick and MarketDataType.LAST_PRICE.value in option_tick:
                est_price = option_tick[MarketDataType.LAST_PRICE.value]
            elif option_tick and MarketDataType.BID.value in option_tick and MarketDataType.ASK.value in option_tick:
                 if "SELL" in leg_info['action'].name: est_price = option_tick[MarketDataType.BID.value]
                 else: est_price = option_tick[MarketDataType.ASK.value]
            
            if est_price is None:
                self.logger.warning(f"Strategy '{self.id}': Could not get price for {instrument.symbol}. Using mock price for estimation. THIS IS RISKY.")
                est_price = 10.0 if target_delta == 16 else 5.0 # Very rough mock
            
            try: est_price = float(est_price)
            except: self.logger.error(f"Bad price {est_price} for {instrument.symbol}"); return

            lot_size = instrument.lot_size if instrument.lot_size and instrument.lot_size > 0 else 1
            premium_effect_per_lot = est_price * lot_size
            
            if "SELL" in leg_info['action'].name:
                total_estimated_premium_change += premium_effect_per_lot * self.quantity_per_spread
            else: # BUY
                total_estimated_premium_change -= premium_effect_per_lot * self.quantity_per_spread
            
            condor_legs_data[leg_info['role']] = {
                'instrument': instrument,
                'instrument_id': instrument.instrument_id,
                'action': leg_info['action'], # This is SignalType enum member
                'estimated_price': est_price,
                'lot_size': lot_size,
                'filled_quantity_contracts': 0, # contracts = lots * lot_size
                'entry_price_actual': None
            }
        
        # Check net credit vs transaction costs
        total_transaction_costs = len(legs_info) * self.estimated_cost_per_leg * self.quantity_per_spread # Per lot basis
        if total_estimated_premium_change < (self.min_credit_to_cost_ratio * total_transaction_costs):
            self.logger.info(f"Strategy '{self.id}': Estimated net credit {total_estimated_premium_change:.2f} for {underlying_instrument_id} "
                             f"is less than {self.min_credit_to_cost_ratio:.1f}x transaction costs ({total_transaction_costs:.2f}). No entry.")
            return

        self.logger.info(f"Strategy '{self.id}': Estimated Net Credit for Calendar Condor on {underlying_instrument_id}: {total_estimated_premium_change:.2f}. Costs: {total_transaction_costs:.2f}. Ratio OK.")

        # Generate signals for all 8 legs
        for role, leg_data in condor_legs_data.items():
            self.logger.info(f"Strategy '{self.id}': Generating signal: {leg_data['action'].name} for {leg_data['instrument_id']} (Role: {role}, Qty: {self.quantity_per_spread} lots)")
            self.generate_signal(
                instrument_id=leg_data['instrument_id'],
                signal_type=leg_data['action'], # Use the specific SignalType like SELL_CALL, BUY_CALL
                data={'quantity': self.quantity_per_spread, 'order_type': 'MARKET', 'product_type': 'NRML'} # NRML for multi-day
            )
        
        self.active_condors[underlying_instrument_id] = {
            'legs': condor_legs_data,
            'entry_timestamp': datetime.now(),
            'initial_net_credit_estimated': total_estimated_premium_change,
            'initial_net_credit_actual': None, # To be updated on fill
            'target_profit_value': None, # Calculated after actual credit
            'stop_loss_value': None,     # Calculated after actual credit
            'short_legs_expiry_date': condor_legs_data['short_call_7dte']['instrument'].expiry_date, # Store for exit check
            'current_mtm_value': 0.0
        }
        self.logger.info(f"Strategy '{self.id}': Calendar Condor entry initiated for {underlying_instrument_id}. Waiting for fills.")


    def _get_actual_expiry_for_dte(self, underlying_simple_name: str, target_dte: int, current_date: datetime.date) -> Optional[datetime.date]:
        # This should ideally be a robust method in OptionManager or a utility class
        expiry_dates = self.option_manager.get_expiry_dates(underlying_simple_name) # Assumes OM can provide this
        if not expiry_dates:
            self.logger.error(f"Strategy {self.id}: No expiry dates found for {underlying_simple_name} via OptionManager.")
            return None
        
        # Filter for expiries >= current_date
        future_expiries = [exp_date for exp_date in expiry_dates if exp_date >= current_date]
        if not future_expiries:
            self.logger.error(f"Strategy {self.id}: No future expiry dates found for {underlying_simple_name}.")
            return None
            
        # Find the expiry date that results in a DTE closest to the target_dte
        closest_expiry = min(future_expiries, key=lambda exp_d: abs((exp_d - current_date).days - target_dte))
        actual_dte = (closest_expiry - current_date).days
        self.logger.info(f"Strategy '{self.id}': For {underlying_simple_name} target DTE {target_dte}, selected expiry {closest_expiry.strftime('%Y-%m-%d')} (Actual DTE: {actual_dte})")
        return closest_expiry


    def on_fill(self, event: FillEvent):
        super().on_fill(event)
        # Find which active condor and leg this fill belongs to
        for underlying_id, condor_info in self.active_condors.items():
            for role, leg_data in condor_info['legs'].items():
                if event.instrument_id == leg_data['instrument_id']:
                    self.logger.info(f"Strategy '{self.id}': Received FILL for {role} ({event.instrument_id}) of condor {underlying_id}. "
                                     f"Price: {event.fill_price:.2f}, Qty (lots): {event.fill_quantity}, Side: {event.order_side.name}")
                    
                    leg_data['entry_price_actual'] = event.fill_price # Assuming single fill per leg for now
                    # Fill quantity in contracts (lots * lot_size)
                    filled_contracts_this_fill = event.fill_quantity * leg_data['lot_size']
                    
                    if event.order_side.name.startswith("BUY"): # BUY_CALL or BUY_PUT
                        leg_data['filled_quantity_contracts'] += filled_contracts_this_fill
                    elif event.order_side.name.startswith("SELL"): # SELL_CALL or SELL_PUT
                        # For entry, these are SELLs. For exit, these would be BUY_TO_COVER.
                        # This logic assumes entry fills. Exit fills need separate handling or flag.
                        if condor_info.get('is_exiting'):
                             leg_data['filled_quantity_contracts'] -= filled_contracts_this_fill
                        else: # Entry SELL
                             leg_data['filled_quantity_contracts'] += filled_contracts_this_fill


                    # Check if all legs have reported an entry fill price
                    all_legs_filled_for_entry = True
                    current_actual_net_credit = 0.0
                    for r, ld in condor_info['legs'].items():
                        if ld['entry_price_actual'] is None:
                            all_legs_filled_for_entry = False
                            break
                        
                        # Calculate credit/debit for this leg based on actual fill
                        # SignalType enum members are like SELL_CALL, BUY_CALL etc.
                        action_str = ld['action'].name 
                        leg_value = ld['entry_price_actual'] * ld['lot_size'] * self.quantity_per_spread
                        if action_str.startswith("SELL"):
                            current_actual_net_credit += leg_value
                        else: # BUY
                            current_actual_net_credit -= leg_value
                    
                    if all_legs_filled_for_entry:
                        condor_info['initial_net_credit_actual'] = current_actual_net_credit
                        max_profit_potential = current_actual_net_credit # For a net credit trade
                        
                        if max_profit_potential > 0: # Only if it's a net credit trade
                            condor_info['target_profit_value'] = max_profit_potential * self.profit_target_percent_of_max
                            condor_info['stop_loss_value'] = - (max_profit_potential * self.stop_loss_multiplier_on_credit) # SL is a negative PNL
                            self.logger.info(f"Strategy '{self.id}': Condor for {underlying_id} FULLY ENTERED. "
                                             f"Actual Net Credit: {condor_info['initial_net_credit_actual']:.2f}. "
                                             f"Target Profit (Value): {condor_info['target_profit_value']:.2f}, "
                                             f"Stop Loss (Value): {condor_info['stop_loss_value']:.2f}.")
                        else:
                            self.logger.warning(f"Strategy '{self.id}': Condor for {underlying_id} resulted in a net DEBIT or zero credit ({current_actual_net_credit:.2f}). "
                                                f"Profit target/SL might not be meaningful. Consider exiting or re-evaluating.")
                            # If it's a net debit, the "profit target" logic needs to be rethought.
                            # For now, we'll still set them, but they might trigger immediate exit if not handled.
                            # Max profit for a debit spread is more complex. This strategy aims for credit.
                            condor_info['target_profit_value'] = None # Or handle debit case differently
                            condor_info['stop_loss_value'] = - (abs(current_actual_net_credit) * self.stop_loss_multiplier_on_credit) if current_actual_net_credit != 0 else None


                    return # Processed this fill for one leg


    def on_market_data(self, event: MarketDataEvent):
        # MTM updates for active condor legs
        if not event.instrument or not event.instrument.instrument_id or not event.data: return
        option_id = event.instrument.instrument_id
        current_price = event.data.get(MarketDataType.LAST_PRICE.value)
        if current_price is None: return
        try: current_price = float(current_price)
        except ValueError: return

        for underlying_id, condor_info in list(self.active_condors.items()):
            if condor_info.get('is_exiting'): continue # Don't check exits if already exiting

            leg_updated = False
            for role, leg_data in condor_info['legs'].items():
                if option_id == leg_data['instrument_id']:
                    leg_data['current_mtm_price'] = current_price
                    leg_updated = True
                    break # Found the leg for this option_id

            if leg_updated and condor_info.get('initial_net_credit_actual') is not None: # Only if entry is complete
                self._check_condor_exit_conditions_mtm(underlying_id)


    def _check_condor_exit_conditions_mtm(self, underlying_id: str):
        condor_info = self.active_condors.get(underlying_id)
        if not condor_info or condor_info.get('initial_net_credit_actual') is None:
            return

        current_position_value = 0.0
        all_legs_have_mtm = True
        for role, leg_data in condor_info['legs'].items():
            if leg_data.get('current_mtm_price') is None:
                all_legs_have_mtm = False
                break
            
            # Value of each leg: (Current Price - Entry Price) * quantity * multiplier
            # For sold options: (Entry Price - Current Price)
            # For bought options: (Current Price - Entry Price)
            # Multiplier is lot_size * self.quantity_per_spread
            
            qty_contracts = leg_data['lot_size'] * self.quantity_per_spread
            entry_price_actual = leg_data.get('entry_price_actual', 0) # Default to 0 if not filled yet
            mtm_price = leg_data['current_mtm_price']
            
            if leg_data['action'].name.startswith("SELL"): # Initially sold this leg
                current_position_value += (entry_price_actual - mtm_price) * qty_contracts
            else: # Initially bought this leg
                current_position_value += (mtm_price - entry_price_actual) * qty_contracts
        
        if not all_legs_have_mtm:
            # self.logger.debug(f"Strategy '{self.id}': Not all legs have MTM prices for condor {underlying_id}. Cannot check PNL based exit.")
            return

        condor_info['current_mtm_pnl'] = current_position_value # This is PnL, not total value
        
        target_profit_value = condor_info.get('target_profit_value')
        stop_loss_value = condor_info.get('stop_loss_value') # This is a negative PNL threshold

        # self.logger.debug(f"Strategy '{self.id}': Condor {underlying_id} MTM PnL: {current_position_value:.2f}. Target Profit: {target_profit_value}, SL: {stop_loss_value}")

        if target_profit_value is not None and current_position_value >= target_profit_value:
            self.logger.info(f"Strategy '{self.id}': PROFIT TARGET hit for condor on {underlying_id}. "
                             f"Current PnL: {current_position_value:.2f}, Target PnL: {target_profit_value:.2f}. Exiting.")
            self._exit_condor_position(underlying_id, "Profit target hit")
        elif stop_loss_value is not None and current_position_value <= stop_loss_value:
            self.logger.info(f"Strategy '{self.id}': STOP LOSS hit for condor on {underlying_id}. "
                             f"Current PnL: {current_position_value:.2f}, SL PnL Threshold: {stop_loss_value:.2f}. Exiting.")
            self._exit_condor_position(underlying_id, "Stop loss hit")


    def _manage_active_condor(self, underlying_instrument_id: str, current_date: datetime.date):
        condor_info = self.active_condors.get(underlying_instrument_id)
        if not condor_info or condor_info.get('is_exiting'): return

        # 1. Check 7DTE expiry
        short_legs_expiry = condor_info.get('short_legs_expiry_date')
        if short_legs_expiry:
            days_to_short_expiry = (short_legs_expiry - current_date).days
            if days_to_short_expiry <= self.exit_before_short_expiry_days:
                self.logger.info(f"Strategy '{self.id}': Short legs for condor {underlying_instrument_id} expiring in {days_to_short_expiry} days (<= threshold {self.exit_before_short_expiry_days}). Exiting.")
                self._exit_condor_position(underlying_instrument_id, "Short leg expiry approaching")
                return
        
        # 2. Daily Delta Hedging (Placeholder - conceptual)
        # self._perform_delta_hedge(underlying_instrument_id, condor_info)
        # self.logger.debug(f"Strategy '{self.id}': Placeholder for daily delta hedge check for {underlying_instrument_id}.")


    def _exit_condor_position(self, underlying_instrument_id: str, reason: str):
        condor_info = self.active_condors.get(underlying_instrument_id)
        if not condor_info or condor_info.get('is_exiting'): # Prevent duplicate exit signals
            self.logger.warning(f"Strategy '{self.id}': Attempted to exit condor for {underlying_instrument_id}, but no active/non-exiting condor found.")
            return
        
        condor_info['is_exiting'] = True # Mark as exiting
        self.logger.info(f"Strategy '{self.id}': Initiating exit for all 8 legs of condor on {underlying_instrument_id}. Reason: {reason}")

        for role, leg_data in condor_info['legs'].items():
            # To close, do the opposite action
            exit_action = None
            if leg_data['action'] == SignalType.SELL_CALL: exit_action = SignalType.BUY_CALL
            elif leg_data['action'] == SignalType.BUY_CALL: exit_action = SignalType.SELL_CALL
            elif leg_data['action'] == SignalType.SELL_PUT: exit_action = SignalType.BUY_PUT
            elif leg_data['action'] == SignalType.BUY_PUT: exit_action = SignalType.SELL_PUT
            
            if exit_action:
                # Quantity should be based on initial entry or current holdings
                qty_to_close = self.quantity_per_spread # Assumes closing the original number of lots
                self.logger.info(f"Strategy '{self.id}': Generating exit signal: {exit_action.name} for {leg_data['instrument_id']} (Role: {role}, Qty: {qty_to_close} lots)")
                self.generate_signal(
                    instrument_id=leg_data['instrument_id'],
                    signal_type=exit_action,
                    data={'quantity': qty_to_close, 'order_type': 'MARKET', 'reason': f"Condor exit: {reason}", 'product_type': 'NRML'}
                )
            else:
                self.logger.error(f"Strategy '{self.id}': Could not determine exit action for leg {role} with original action {leg_data['action']}")
        
        # Actual cleanup from self.active_condors happens in on_fill when all SELL/BUY_TO_COVER fills are received.


    def on_stop(self):
        super().on_stop()
        self.logger.info(f"Strategy '{self.id}' on_stop: Exiting all open condor positions if any.")
        for underlying_id in list(self.active_condors.keys()):
            self._exit_condor_position(underlying_id, "Strategy stop command")
        
        self.underlying_data_cache.clear()
        self.active_condors.clear()
        self.logger.info(f"Strategy '{self.id}' stopped and internal states cleared.")

    # run_iteration can be used for periodic checks if on_bar is not sufficient
    # For a daily strategy, on_bar(daily) at 15:00 should be enough.
    # def run_iteration(self):
    #     super().run_iteration()
    #     pass


