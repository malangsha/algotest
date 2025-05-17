"""
Option Manager module for option trading.

This module handles option-specific functionality including:
- Option chain management
- ATM strike calculation
- Option subscription/unsubscription based on underlying movements
- Option data caching and management
"""

import threading
from typing import Dict, List, Set, Optional, Tuple, Any
from datetime import datetime, date, timedelta as dt_time 
import time
import numpy as np
from functools import lru_cache
from collections import defaultdict

from models.instrument import Instrument, AssetClass
from core.event_manager import EventManager
from utils.constants import (OptionType, 
        Exchange, InstrumentType, NSE_INDICES, 
        BSE_INDICES, MarketDataType, SYMBOL_MAPPINGS)
from core.logging_manager import get_logger
from models.events import MarketDataEvent, OptionsSubscribedEvent
from core.event_manager import EventType

class OptionManager:
    """
    Manages option chain data, subscriptions, and provides option trading utilities.
    Optimized for low-latency and high-throughput option data processing.
    """
    
    def __init__(self, data_manager, event_manager, position_manager, config: Dict[str, Any], cache_size=1000):
        """
        Initialize the Option Manager.
        
        Args:
            data_manager: Data manager instance for market data operations
            event_manager: Event manager for publishing/subscribing to events
            position_manager: Position manager instance for checking open positions
            config: Configuration dictionary
            cache_size: Size of LRU cache for option calculations
        """
        self.logger = get_logger("core.option_manager")
        self.data_manager = data_manager
        self.event_manager = event_manager
        self.position_manager = position_manager # Store position manager
        self.config = config
        
        self.underlying_prices: Dict[str, float] = {}
        self._subscribed_underlyings: Set[str] = set()
        # underlying_symbol -> {option_instrument_id -> Instrument object}
        self.option_subscriptions: Dict[str, Dict[str, Instrument]] = defaultdict(dict) 
        self.option_chain_data: Dict[str, Dict[Any, Any]] = {}
        
        # ATM strike tracking
        self.current_atm_strikes: Dict[str, float] = {}
        self.strike_intervals: Dict[str, float] = {}
        self.atm_ranges: Dict[str, int] = {}

        # Hysteresis buffer parameters
        self.buffer_strikes_factor = config.get('options', {}).get('buffer_strikes_factor', 0.3)  # Default 30% of strike interval
        self.debounce_ms = config.get('options', {}).get('debounce_ms', 500)  # Default 500ms debounce
        self.pending_atm_updates: Dict[str, Dict[str, Any]] = {}  # Symbol -> update info
        self.pending_timers: Dict[str, threading.Timer] = {}  # Symbol -> timer

        # Add cache for constructed option symbols to prevent duplicates
        self.recently_constructed_symbols: Dict[str, Set[str]] = defaultdict(set)  # underlying -> set of symbols
        # Add expiry of recently constructed symbols (in seconds)
        self.symbol_cache_expiry = config.get('options', {}).get('symbol_cache_expiry', 60)  # Default 10 seconds
        self.symbol_cache_timestamps: Dict[str, Dict[str, float]] = defaultdict(dict)  # underlying -> {symbol -> timestamp}

        # Option data cache
        self.option_data_cache: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()
        
        # Configure LRU cache for frequently used calculations
        self._get_atm_strike = lru_cache(maxsize=cache_size)(self._get_atm_strike_internal)
        
        # Underlying configurations
        market_config = config.get('market', {})
        underlyings_config = market_config.get('underlyings', [])       
        self.underlyings: Dict[str, Dict[str, Any]] = {}
        self._process_underlyings_config(underlyings_config) 
            
        self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data, component_name="OptionManager")
        
        self.logger.info("Option Manager initialized")
    
    def _process_underlyings_config(self, underlyings_config: List[Dict[str, Any]]):
        if not isinstance(underlyings_config, list):
            self.logger.error("Underlyings configuration is not a list. Please check config.yaml.")
            return

        for underlying_config in underlyings_config:
            symbol = underlying_config.get("name", underlying_config.get("symbol"))
            if not symbol:
                self.logger.warning(f"Skipping underlying config without name/symbol: {underlying_config}")
                continue

            spot_exchange_str = underlying_config.get("spot_exchange", underlying_config.get("exchange"))
            option_exchange_str = underlying_config.get("option_exchange", underlying_config.get("derivative_exchange"))

            if not spot_exchange_str:
                if symbol in NSE_INDICES: spot_exchange_str = Exchange.NSE.value
                elif symbol in BSE_INDICES: spot_exchange_str = Exchange.BSE.value
                else: spot_exchange_str = "NSE"
            if not option_exchange_str:
                if symbol in NSE_INDICES: option_exchange_str = Exchange.NFO.value
                elif symbol in BSE_INDICES: option_exchange_str = Exchange.BFO.value
                else: option_exchange_str = "NFO"
            try:
                spot_exchange = Exchange(spot_exchange_str)
            except ValueError:
                self.logger.warning(f"Invalid spot exchange 	'{spot_exchange_str}	' for {symbol}, using fallback.")
                spot_exchange = Exchange.NSE if symbol in NSE_INDICES else Exchange.BSE if symbol in BSE_INDICES else Exchange.NSE
            try:
                option_exchange = Exchange(option_exchange_str)
            except ValueError:
                self.logger.warning(f"Invalid option exchange 	'{option_exchange_str}	' for {symbol}, using fallback.")
                option_exchange = Exchange.NFO if symbol in NSE_INDICES else Exchange.BFO if symbol in BSE_INDICES else Exchange.NFO

            strike_interval = underlying_config.get("strike_interval", 50.0 if "NIFTY" in symbol else 100.0)
            atm_range = underlying_config.get("atm_range", 1) 

            self.underlyings[symbol] = {
                "symbol": symbol,
                "exchange": spot_exchange,
                "derivative_exchange": option_exchange,
                "strike_interval": float(strike_interval),
                "atm_range": int(atm_range),
                **{k: v for k, v in underlying_config.items() if k not in
                   ["name", "symbol", "spot_exchange", "exchange", "option_exchange", "derivative_exchange", "strike_interval", "atm_range"]}
            }
            self.strike_intervals[symbol] = float(strike_interval)
            self.atm_ranges[symbol] = int(atm_range)
            self.logger.info(f"Configured underlying {symbol} with strike interval {strike_interval}, ATM range {atm_range}, spot exch {spot_exchange.value}, option exch {option_exchange.value}")

    def _cancel_pending_atm_update(self, underlying_symbol: str) -> None:
       """Cancel any pending ATM update for the given underlying."""
       with self.lock:
           timer = self.pending_timers.pop(underlying_symbol, None)
           if timer:
               timer.cancel()
               self.logger.debug(f"Cancelled pending ATM update for {underlying_symbol}")
           # Also remove from pending updates
           self.pending_atm_updates.pop(underlying_symbol, None)
    
    def _schedule_atm_update(self, underlying_symbol: str, new_atm: float, price: float) -> None:
       """Schedule an ATM update with debounce logic."""
       with self.lock:
           # Cancel any existing timer for this underlying
           self._cancel_pending_atm_update(underlying_symbol)
           
           # Create update info dictionary
           update_info = {
               'new_atm': new_atm,
               'price': price,
               'timestamp': time.time()
           }
           self.pending_atm_updates[underlying_symbol] = update_info
           
           # Create and start a new timer
           timer = threading.Timer(self.debounce_ms / 1000.0, self._process_atm_update, args=[underlying_symbol])
           timer.daemon = True  # Allow the program to exit even if the timer is still running
           self.pending_timers[underlying_symbol] = timer
           timer.start()
           self.logger.debug(f"Scheduled ATM update for {underlying_symbol} in {self.debounce_ms}ms: {new_atm} (price: {price:.2f})")
    
    def _process_atm_update(self, underlying_symbol: str) -> None:
        """Process the pending ATM update after the debounce period."""
        with self.lock:
            # Check if the update is still pending (not cancelled)
            update_info = self.pending_atm_updates.pop(underlying_symbol, None)
            if not update_info:
                return  # Already cancelled or processed
            
            # Clean up the timer
            self.pending_timers.pop(underlying_symbol, None)
            
            # Get the cached values
            new_atm = update_info['new_atm']
            price = update_info['price']
            old_atm = self.current_atm_strikes.get(underlying_symbol)
            
            # Update the current ATM strike
            self.logger.info(f"Processing delayed ATM strike change for {underlying_symbol}. "
                             f"Price: {price:.2f}, Old ATM: {old_atm}, New ATM: {new_atm}")
            
            # Store the new ATM strike
            self.current_atm_strikes[underlying_symbol] = new_atm
            
            # Perform the subscription update
            self._update_atm_subscriptions(underlying_symbol, new_atm, old_atm)

    def configure_underlyings(self, underlyings_config: List[Dict[str, Any]]):
        with self.lock:
            self._process_underlyings_config(underlyings_config)
    
    def subscribe_underlying(self, underlying_symbol: str) -> bool:
        """Uses DataManager.subscribe_instrument (Item 2.1 from todo.md)"""
        if underlying_symbol in self._subscribed_underlyings:
            return True
        try:
            underlying_config = self.underlyings.get(underlying_symbol)
            if not underlying_config:
                self.logger.error(f"No configuration found for underlying {underlying_symbol}")
                return False

            exchange = underlying_config.get("exchange")
            if not isinstance(exchange, Exchange):
                 self.logger.error(f"Invalid exchange configuration for {underlying_symbol}")
                 return False

            instrument_type = InstrumentType.INDEX if underlying_symbol in SYMBOL_MAPPINGS.get("indices", []) else InstrumentType.EQUITY
            asset_class = AssetClass.INDEX if instrument_type == InstrumentType.INDEX else AssetClass.EQUITY

            instrument = Instrument(
                symbol=underlying_symbol,
                exchange=exchange,
                instrument_type=instrument_type,
                asset_class=asset_class,
                instrument_id=f"{exchange.value}:{underlying_symbol}" # Ensure consistent key format
            )
            # MODIFIED: Use DataManager's specific method for instrument feed subscription
            if self.data_manager.subscribe_instrument(instrument):
                self._subscribed_underlyings.add(underlying_symbol)
                self.logger.info(f"OptionManager: Successfully requested DataManager to subscribe to underlying feed {underlying_symbol}")
                return True
            else:
                self.logger.error(f"OptionManager: DataManager failed to subscribe to underlying feed {underlying_symbol}")
                return False
        except Exception as e:
            self.logger.error(f"Error subscribing to underlying {underlying_symbol}: {e}", exc_info=True)
            return False
            
    def _on_market_data(self, event: MarketDataEvent):
        if not isinstance(event, MarketDataEvent) or not event.instrument:
            return

        instrument = event.instrument
        symbol = instrument.symbol
        symbol_key = event.data.get("symbol_key", self.data_manager._get_symbol_key(instrument)) # Ensure symbol_key

        if not symbol_key or symbol_key.startswith("INVALID:"):
            self.logger.warning(f"MarketDataEvent for {symbol} missing valid symbol_key. Cannot process.")
            return

        if symbol in self._subscribed_underlyings: # Check against simple symbol name for underlyings
            self._process_underlying_data(symbol, event.data)
        elif symbol_key in self.option_data_cache: # Check against symbol_key for options
             self.option_data_cache[symbol_key] = event.data
            
    def _process_underlying_data(self, underlying_symbol: str, data: Dict):
        raw_price = data.get(MarketDataType.LAST_PRICE.value, data.get("LAST_PRICE", data.get("CLOSE")))
        if raw_price is None: return
        try:
            price_float = float(str(raw_price).replace(",", ""))
        except (ValueError, TypeError) as conv_err:
            self.logger.warning(f"Could not convert price {raw_price!r} for {underlying_symbol}: {conv_err}")
            return
        try:
            self.update_underlying_price(underlying_symbol, price_float)
        except Exception as upd_err:
            self.logger.error(f"Error in update_underlying_price for {underlying_symbol} price {price_float}: {upd_err}", exc_info=True)
            
    def _clean_expired_symbol_cache(self, underlying_symbol: str = None) -> None:
        """Clean expired symbols from the cache"""
        now = time.time()
        with self.lock:
            if underlying_symbol:
                # Clean only for specific underlying
                symbols_to_remove = []
                for symbol, timestamp in self.symbol_cache_timestamps.get(underlying_symbol, {}).items():
                    if now - timestamp > self.symbol_cache_expiry:
                        symbols_to_remove.append(symbol)

                for symbol in symbols_to_remove:
                    self.symbol_cache_timestamps[underlying_symbol].pop(symbol, None)
                    if symbol in self.recently_constructed_symbols.get(underlying_symbol, set()):
                        self.recently_constructed_symbols[underlying_symbol].remove(symbol)
            else:
                # Clean for all underlyings
                for underlying in list(self.symbol_cache_timestamps.keys()):
                    self._clean_expired_symbol_cache(underlying)
    
    def update_underlying_price(self, underlying_symbol: str, price: float) -> None:
        with self.lock:
            # Clean expired symbols from cache
            self._clean_expired_symbol_cache(underlying_symbol)

            old_price = self.underlying_prices.get(underlying_symbol)
            self.underlying_prices[underlying_symbol] = price

            if underlying_symbol not in self.strike_intervals:
                return

            strike_interval = self.strike_intervals[underlying_symbol]
            buffer_strikes = strike_interval * self.buffer_strikes_factor
            new_atm = self._get_atm_strike_internal(price, strike_interval)
            old_atm = self.current_atm_strikes.get(underlying_symbol)

            # Initial price or ATM setting (no hysteresis needed)
            if old_price is None or old_atm is None:
                self.logger.info(f"Initial ATM strike set for {underlying_symbol}. "
                                f"Price: {price:.2f}, ATM: {new_atm}")
                self.current_atm_strikes[underlying_symbol] = new_atm
                self._update_atm_subscriptions(underlying_symbol, new_atm, None)
                return

            # Check if there's a significant ATM change beyond half the strike interval
            # This preserves the original behavior but adds hysteresis
            if new_atm != old_atm:
                # Calculate thresholds for hysteresis
                upper_threshold = old_atm + buffer_strikes
                lower_threshold = old_atm - buffer_strikes

                # Check if price has moved beyond the thresholds
                if (price >= upper_threshold and new_atm > old_atm) or (price <= lower_threshold and new_atm < old_atm):
                    # Check if we already have a pending update for the same new ATM value
                    pending_update = self.pending_atm_updates.get(underlying_symbol)
                    if pending_update and pending_update['new_atm'] == new_atm:
                        # We already have a pending update for this exact ATM value, ignore this one
                        self.logger.debug(f"Ignoring duplicate ATM update for {underlying_symbol}. "
                                         f"Already pending update to {new_atm}")
                        return

                    # Price has crossed a threshold, schedule an update with debounce
                    self.logger.debug(f"Threshold crossed for {underlying_symbol}. "
                                     f"Price: {price:.2f}, Old ATM: {old_atm}, New ATM: {new_atm}, "
                                     f"Thresholds: [{lower_threshold:.2f}, {upper_threshold:.2f}]")
                    self._schedule_atm_update(underlying_symbol, new_atm, price)
                else:
                    # Price is within thresholds, cancel any pending updates if it reversed direction
                    pending_update = self.pending_atm_updates.get(underlying_symbol)
                    if pending_update and ((pending_update['new_atm'] > old_atm and new_atm <= old_atm) or
                                          (pending_update['new_atm'] < old_atm and new_atm >= old_atm)):
                        self.logger.debug(f"Price reversed direction for {underlying_symbol}, cancelling pending update")
                        self._cancel_pending_atm_update(underlying_symbol)

    def _get_atm_strike_internal(self, price: float, strike_interval: float) -> float:
        if strike_interval == 0: return price # Should not happen for options
        return round(price / strike_interval) * strike_interval

    def _update_atm_subscriptions(self, underlying_symbol: str, new_atm_strike: float, old_atm_strike: Optional[float]):
        """Uses DataManager.subscribe_instrument/unsubscribe_instrument (Item 2.2 from todo.md)"""
        underlying_config = self.underlyings.get(underlying_symbol)
        if not underlying_config: return

        atm_range = self.atm_ranges.get(underlying_symbol, 1)
        strike_interval = self.strike_intervals[underlying_symbol]
        option_exchange = underlying_config["derivative_exchange"]
        expiry_offset = underlying_config.get("expiry_offset", 0) # Default to current expiry
        
        # Check if data_manager and market_data_feed are available
        if not hasattr(self.data_manager, 'market_data_feed') or not self.data_manager.market_data_feed:
            self.logger.error(f"Cannot update ATM options: market data feed not available")
            return             
        market_data_feed = self.data_manager.market_data_feed
        
        # Use mapped symbol for broker interaction if necessary
        mapped_underlying = SYMBOL_MAPPINGS.get(underlying_symbol, underlying_symbol)
        expiry_dates = market_data_feed.get_expiry_dates(mapped_underlying) # Use mapped symbol if needed by feed
        if not expiry_dates:
            self.logger.error(f"No expiry dates found for {mapped_underlying}")
            return None
        
        if expiry_offset >= len(expiry_dates):
            self.logger.warning(f"Expiry offset {expiry_offset} out of range for {mapped_underlying}, using last available")
            expiry_date = expiry_dates[-1]
        else:
            expiry_date = expiry_dates[expiry_offset]

        newly_subscribed_instruments: List[Instrument] = []
        current_option_keys_for_underlying = set(self.option_subscriptions.get(underlying_symbol, {}).keys())
        needed_option_keys = set()
        instrument_type = InstrumentType.OPTION.value

        # Determine strikes to subscribe around the new ATM
        for i in range(-atm_range, atm_range + 1):
            strike = new_atm_strike + (i * strike_interval)
            for option_type in ['CE', 'PE']:  # Call and Put options
                # Get the broker-specific option symbol                   
                self.logger.debug(f"calling construct_trading_symbol with symbol: {str(mapped_underlying)}, expriy_date: {expiry_date}, instrument_type: {instrument_type}, option_type: {option_type}, exchange: {str(option_exchange.value)}")           
                option_instrument =  market_data_feed.construct_trading_symbol(
                    symbol=str(mapped_underlying),
                    expiry_date=expiry_date,
                    instrument_type=instrument_type,
                    strike=strike,
                    option_type=option_type,
                    exchange=str(option_exchange.value)
                )
                
                if not option_instrument:
                    self.logger.error(f"Failed to construct option symbol for {underlying_symbol} {strike} {option_type}")
                    return None
            
                # Create the Instrument object
                instrument = Instrument(
                    symbol=option_instrument, # The actual trading symbol
                    exchange=option_exchange,
                    asset_class=AssetClass.OPTIONS,
                    instrument_type=InstrumentType.OPTION,
                    underlying_symbol=underlying_symbol, # Original underlying symbol
                    strike=strike,
                    option_type=option_type,
                    expiry_date=expiry_date, # Store expiry date object
                    # Use DataManager's preferred key format if available, else construct one
                    instrument_id=f"{option_exchange.value}:{option_instrument}"
                )

                needed_option_keys.add(instrument.instrument_id)
                if instrument.instrument_id not in current_option_keys_for_underlying:                    
                    if self.data_manager.subscribe_instrument(instrument):
                        self.option_subscriptions.setdefault(underlying_symbol, {})[instrument.instrument_id] = instrument
                        self.option_data_cache[instrument.instrument_id] = {} # Initialize cache
                        newly_subscribed_instruments.append(instrument)
                        self.logger.info(f"OptionManager: Subscribed to new option feed: {instrument.instrument_id}")
                    else:
                        self.logger.error(f"OptionManager: DataManager failed to subscribe to option feed: {instrument.instrument_id}")
        
        # Unsubscribe from options no longer needed
        options_to_unsubscribe = current_option_keys_for_underlying - needed_option_keys
        for option_key_to_remove in list(options_to_unsubscribe): # Iterate over a copy
            instrument_to_remove = self.option_subscriptions.get(underlying_symbol, {}).pop(option_key_to_remove, None)
            if instrument_to_remove:
                # MODIFIED: Use DataManager's specific method for instrument feed unsubscription
                if self.data_manager.unsubscribe_instrument(instrument_to_remove):
                    self.logger.info(f"OptionManager: Unsubscribed from option feed: {instrument_to_remove.instrument_id}")
                    self.option_data_cache.pop(instrument_to_remove.instrument_id, None)
                else:
                    self.logger.error(f"OptionManager: DataManager failed to unsubscribe from option feed: {instrument_to_remove.instrument_id}")
                    # If DM fails, put it back? Or assume DM handles its state?
                    # For now, we proceed with removing from OM's tracking.
            else:
                self.logger.warning(f"Tried to unsubscribe {option_key_to_remove} but it was not in OM's subscriptions for {underlying_symbol}")

        if newly_subscribed_instruments:
            self.event_manager.publish(OptionsSubscribedEvent(
                underlying_symbol=underlying_symbol,
                instruments=newly_subscribed_instruments               
            ))
            self.logger.info(f"Published OptionsSubscribedEvent for {underlying_symbol} with {len(newly_subscribed_instruments)} options.")
    
    # Public method using the cached internal one
    def get_atm_strike(self, underlying_symbol: str) -> Optional[float]:
        """
        Get the current ATM strike for an underlying using cached price.

        Args:
            underlying_symbol: Underlying symbol (e.g., "NIFTY INDEX")

        Returns:
            Current ATM strike if available, None otherwise
        """
        with self.lock:
            current_price = self.underlying_prices.get(underlying_symbol)
            strike_interval = self.strike_intervals.get(underlying_symbol)
            if current_price is not None and strike_interval is not None:
                # Use the cached internal method
                return self._get_atm_strike(current_price, strike_interval)
            else:
                # Try direct calculation if available
                 cached_atm = self.current_atm_strikes.get(underlying_symbol)
                 if cached_atm: return cached_atm
                 self.logger.warning(f"Cannot calculate ATM for {underlying_symbol}: Price or interval missing.")
                 return None
        
    def _get_strike_range(self, atm_strike: float, strike_interval: float, range_count: int) -> List[float]:
        """Get a range of strikes around the ATM strike."""
        strikes = []
        # Ensure range_count is non-negative
        range_count = max(0, range_count)
        for i in range(-range_count, range_count + 1):
            strike = atm_strike + (i * strike_interval)
            # Ensure strikes are positive
            if strike > 0:
                strikes.append(strike)
        return strikes
        
    def get_option_instrument(self, underlying_symbol: str, strike: float, option_type: str, expiry_offset: int = 0) -> Optional[Instrument]:
        """
        Constructs an Instrument object for a given option.

        Args:
            underlying_symbol: The underlying symbol (e.g., "NIFTY INDEX")
            strike: Strike price
            option_type: Option type ('CE' or 'PE')
            expiry_offset: 0 for current expiry, 1 for next expiry, etc.

        Returns:
            Instrument object or None if construction fails.
        """
        try:
            underlying_config = self.underlyings.get(underlying_symbol)
            if not underlying_config:
                self.logger.error(f"No configuration for underlying {underlying_symbol}")
                return None

            derivative_exchange = underlying_config.get('derivative_exchange') # Should be Enum
            if not isinstance(derivative_exchange, Exchange):
                 self.logger.error(f"Invalid derivative exchange for {underlying_symbol}")
                 return None

            # Get expiry date
            # Assume market_data_feed object is accessible via data_manager
            if not hasattr(self.data_manager, 'market_data_feed') or not self.data_manager.market_data_feed:
                self.logger.error("Market data feed not available in DataManager")
                return None
            market_data_feed = self.data_manager.market_data_feed

            # Use mapped symbol for broker interaction if necessary
            mapped_underlying = SYMBOL_MAPPINGS.get(underlying_symbol, underlying_symbol)
            expiry_dates = market_data_feed.get_expiry_dates(mapped_underlying) # Use mapped symbol if needed by feed
            if not expiry_dates:
                self.logger.error(f"No expiry dates found for {mapped_underlying}")
                return None
            if expiry_offset >= len(expiry_dates):
                self.logger.warning(f"Expiry offset {expiry_offset} out of range for {mapped_underlying}, using last available")
                expiry_date = expiry_dates[-1]
            else:
                expiry_date = expiry_dates[expiry_offset]

            # Construct the broker-specific trading symbol
            option_symbol_str = market_data_feed.construct_trading_symbol(
                symbol=mapped_underlying, # Use mapped symbol for construction
                expiry_date=expiry_date,
                instrument_type="OPT", # Assuming OPT for options
                option_type=option_type,
                strike=strike,
                exchange=derivative_exchange.value # Pass exchange value string
            )

            if not option_symbol_str:
                self.logger.error(f"Failed to construct option symbol for {underlying_symbol} {strike} {option_type}")
                return None

            # Create the Instrument object
            instrument = Instrument(
                symbol=option_symbol_str, # The actual trading symbol
                exchange=derivative_exchange,
                asset_class=AssetClass.OPTIONS,
                instrument_type=InstrumentType.OPTION,
                underlying_symbol=underlying_symbol, # Original underlying symbol
                strike=strike,
                option_type=option_type,
                expiry_date=expiry_date, # Store expiry date object
                # Use DataManager's preferred key format if available, else construct one
                instrument_id=f"{derivative_exchange.value}:{option_symbol_str}"
            )
            return instrument

        except Exception as e:
            self.logger.error(f"Error getting option instrument for {underlying_symbol} {strike} {option_type}: {e}", exc_info=True)
            return None

    def get_option_symbol(self, underlying: str, strike: float, option_type: str, expiry_offset: int = 0) -> Optional[str]:
        """
        Get the broker-specific option symbol string.

        Args:
            underlying: Underlying symbol (e.g., "NIFTY INDEX")
            strike: Strike price
            option_type: 'CE' or 'PE'
            expiry_offset: Number of expiries from current (0=current)

        Returns:
            Option symbol string if available, None otherwise
        """
        instrument = self.get_option_instrument(underlying, strike, option_type, expiry_offset)
        return instrument.symbol if instrument else None
    
       
    def _subscribe_option_instruments(self, instruments: List[Instrument], underlying_symbol: str) -> List[Instrument]:
        """Helper to subscribe to a list of option instruments and update tracking."""
        subscribed_instruments = []
        if not instruments:
            return subscribed_instruments

        # Assume market_data_feed object is accessible via data_manager
        if not hasattr(self.data_manager, 'market_data_feed') or not self.data_manager.market_data_feed:
            self.logger.error("Market data feed not available in DataManager for option subscription")
            return subscribed_instruments
        market_data_feed = self.data_manager.market_data_feed

        # Use market_data_feed's batch subscription method
        subscription_results = market_data_feed.subscribe_symbols(instruments) # Expects list of Instruments

        # Initialize subscription dict for underlying if not present
        if underlying_symbol not in self.option_subscriptions:
            self.option_subscriptions[underlying_symbol] = {}

        # Process results and update tracking
        for instrument in instruments:
            symbol_key = instrument.instrument_id # Use the unique ID as key
            # Check result using the instrument's primary symbol (trading symbol)
            was_successful = subscription_results.get(instrument.symbol, False)

            if was_successful:
                # Add to tracking using symbol_key -> Instrument
                self.option_subscriptions[underlying_symbol][symbol_key] = instrument
                subscribed_instruments.append(instrument)
                # self.logger.debug(f"Successfully subscribed to option: {symbol_key}")
            else:
                self.logger.warning(f"Failed to subscribe to option: {instrument.symbol} ({symbol_key})")

        return subscribed_instruments

    def subscribe_atm_options(self, underlying_symbol: str, expiry_offset: int = 0) -> None:
        """
        Subscribe to ATM options for an underlying and publish event.

        Args:
            underlying_symbol: The underlying symbol (e.g., "NIFTY INDEX")
            expiry_offset: 0 for current expiry, 1 for next expiry, etc.
        """
        with self.lock:
            atm_strike = self.current_atm_strikes.get(underlying_symbol)
            strike_interval = self.strike_intervals.get(underlying_symbol)
            atm_range = self.atm_ranges.get(underlying_symbol)

            if atm_strike is None or strike_interval is None or atm_range is None:
                self.logger.warning(f"Cannot subscribe ATM options for {underlying_symbol}: Config or ATM strike missing.")
                return

            # Get range of strikes around ATM
            strikes = self._get_strike_range(atm_strike, strike_interval, atm_range)

            # Prepare list of new option instruments to subscribe
            option_instruments_to_add = []
            current_subscriptions = self.option_subscriptions.get(underlying_symbol, {})

            for strike in strikes:
                for option_type in ['CE', 'PE']:
                    instrument = self.get_option_instrument(underlying_symbol, strike, option_type, expiry_offset)
                    if instrument and instrument.instrument_id not in current_subscriptions:
                        option_instruments_to_add.append(instrument)

            # Subscribe via helper method
            newly_subscribed_instruments = self._subscribe_option_instruments(option_instruments_to_add, underlying_symbol)

            if newly_subscribed_instruments:
                self.logger.info(f"Subscribed to {len(newly_subscribed_instruments)} new ATM options for {underlying_symbol}")
                # Publish event with the list of *newly* subscribed Instrument objects
                self.logger.debug(f"Publish OptionsSubscribedEvent (CUSTOM) event with the list of newly subscribed Insrument Objects")
                event = OptionsSubscribedEvent(
                    underlying_symbol=underlying_symbol,
                    instruments=newly_subscribed_instruments
                )
                self.event_manager.publish(event)
            else:
                self.logger.info(f"No new ATM options needed subscription for {underlying_symbol}")
                     
    def update_atm_options(self, underlying_symbol: str, expiry_offset: int = 0) -> None:
        """
        Update option subscriptions based on the new ATM strike.
        Unsubscribes from out-of-range options and subscribes to new in-range options.
        Publishes event for newly subscribed options.

        Args:
            underlying_symbol: The underlying symbol (e.g., "NIFTY INDEX")
            expiry_offset: 0 for current expiry, 1 for next expiry, etc.
        """
        with self.lock:
            new_atm_strike = self.current_atm_strikes.get(underlying_symbol)
            strike_interval = self.strike_intervals.get(underlying_symbol)
            atm_range = self.atm_ranges.get(underlying_symbol)

            if new_atm_strike is None or strike_interval is None or atm_range is None:
                self.logger.warning(f"Cannot update ATM options for {underlying_symbol}: Config or ATM strike missing.")
                return

            # Get new required range of strikes
            new_strikes = set(self._get_strike_range(new_atm_strike, strike_interval, atm_range))

            # Get current subscriptions for this underlying
            current_subscriptions = self.option_subscriptions.get(underlying_symbol, {}) # symbol_key -> Instrument

            # Determine potential new instruments needed
            potential_instruments = {} # symbol_key -> Instrument
            for strike in new_strikes:
                for option_type in ['CE', 'PE']:
                    instrument = self.get_option_instrument(underlying_symbol, strike, option_type, expiry_offset)
                    if instrument:
                        potential_instruments[instrument.instrument_id] = instrument

            # Identify instruments to add and remove
            instruments_to_add = []
            instruments_to_remove = [] # Store Instrument objects to unsubscribe

            potential_keys = set(potential_instruments.keys())
            current_keys = set(current_subscriptions.keys())

            # Find new keys to subscribe
            keys_to_add = potential_keys - current_keys
            for key in keys_to_add:
                instruments_to_add.append(potential_instruments[key])

            # Find old keys to potentially unsubscribe
            keys_to_remove = current_keys - potential_keys
            for key in keys_to_remove:
                instrument_to_remove = current_subscriptions[key]
                # --- Check for open positions before unsubscribing ---
                should_unsubscribe = True
                if self.position_manager:
                    # Position manager needs the symbol_key (e.g., NFO:NIFTY...)
                    if self.position_manager.has_open_position(instrument_to_remove.instrument_id):
                        should_unsubscribe = False
                        self.logger.info(f"Keeping subscription to {instrument_to_remove.symbol} ({key}) due to open position.")

                if should_unsubscribe:
                    instruments_to_remove.append(instrument_to_remove)

            # --- Perform Unsubscriptions ---
            unsubscribed_count = 0
            if instruments_to_remove:
                # Assume market_data_feed object is accessible via data_manager
                if hasattr(self.data_manager, 'market_data_feed') and self.data_manager.market_data_feed:
                    market_data_feed = self.data_manager.market_data_feed
                    # Use market_data_feed's batch unsubscription method
                    # unsubscription_results = market_data_feed.unsubscribe_symbols(instruments_to_remove)
                    unsubscription_results = {instrument.symbol: True for instrument in instruments_to_remove}

                    for instrument in instruments_to_remove:
                        symbol_key = instrument.instrument_id
                        was_successful = unsubscription_results.get(instrument.symbol, False)
                        if was_successful:
                            # Remove from tracking
                            if underlying_symbol in self.option_subscriptions and symbol_key in self.option_subscriptions[underlying_symbol]:
                                del self.option_subscriptions[underlying_symbol][symbol_key]
                            # Remove from cache
                            self.option_data_cache.pop(symbol_key, None)
                            unsubscribed_count += 1
                            # self.logger.debug(f"Successfully unsubscribed from option: {symbol_key}")
                        else:
                            self.logger.warning(f"Failed to unsubscribe from option: {instrument.symbol} ({symbol_key})")
                else:
                    self.logger.error("Market data feed not available for option unsubscription")


            # --- Perform Subscriptions ---
            newly_subscribed_instruments = self._subscribe_option_instruments(instruments_to_add, underlying_symbol)

            # --- Logging and Event Publishing ---
            if unsubscribed_count > 0 or newly_subscribed_instruments:
                 self.logger.info(f"Updated ATM options for {underlying_symbol}: Unsubscribed {unsubscribed_count}, Subscribed {len(newly_subscribed_instruments)} new.")

            if newly_subscribed_instruments:
                # Publish event only for the newly subscribed instruments
                event = OptionsSubscribedEvent(
                    underlying_symbol=underlying_symbol,
                    instruments=newly_subscribed_instruments
                )
                self.event_manager.publish(event)
    
    def get_option_data(self, option_symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest data for an option.
        
        Args:
            option_symbol: The option symbol
            
        Returns:
            Optional[Dict[str, Any]]: The latest option data or None if not available
        """
        return self.option_data_cache.get(option_symbol)
    
    def get_active_option_instruments(self, underlying_symbol: Optional[str] = None) -> List[Instrument]:
        """
        Get the list of currently subscribed option Instrument objects.

        Args:
            underlying_symbol: Optional underlying symbol (e.g., "NIFTY INDEX") to filter by.

        Returns:
            List[Instrument]: List of active option Instrument objects.
        """
        with self.lock:
            if underlying_symbol:
                return list(self.option_subscriptions.get(underlying_symbol, {}).values())
            else:
                all_instruments = []
                for sub_dict in self.option_subscriptions.values():
                    all_instruments.extend(sub_dict.values())
                return all_instruments

    def get_active_option_symbols(self, underlying_symbol: Optional[str] = None) -> List[str]:
         """Get the list of currently subscribed option symbols (trading symbols)."""
         instruments = self.get_active_option_instruments(underlying_symbol)
         return [inst.symbol for inst in instruments]
    
    def get_atm_strikes(self) -> Dict[str, float]:
        """Get the current ATM strikes for all managed underlyings."""
        with self.lock:
            return self.current_atm_strikes.copy()
    
    def start_tracking(self) -> bool:
        """
        Start tracking configured underlyings. Subscribes to underlyings only.
        Option subscriptions happen reactively when underlying prices arrive.

        Returns:
            bool: True if tracking started successfully for at least one underlying.
        """
        success_count = 0
        with self.lock:
            self.logger.info("Starting underlying tracking...")
            configured_underlyings = list(self.underlyings.keys()) # Get symbols like "NIFTY INDEX"
            if not configured_underlyings:
                 self.logger.warning("No underlyings configured in OptionManager.")
                 return False

            for underlying_symbol in configured_underlyings:
                if self.subscribe_underlying(underlying_symbol):
                    success_count += 1
                else:
                    # Log error inside subscribe_underlying
                    pass # Continue trying others

            if success_count > 0:
                self.logger.info(f"Underlying tracking started for {success_count}/{len(configured_underlyings)} configured underlyings.")
                return True
            else:
                 self.logger.error("Failed to start tracking for any configured underlyings.")
                 return False 
