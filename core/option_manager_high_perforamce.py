"""
Option Manager module for option trading - High-Performance Version.

This module handles option-specific functionality including:
- Option chain management
- ATM strike calculation 
- Option subscription/unsubscription based on underlying movements
- Option data caching and management

Optimized for high-throughput, low-latency trading with fine-grained locking,
improved memory usage, and more efficient data structures.
"""

import threading
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from datetime import datetime, date, timedelta as dt_time
import time
import numpy as np
import random
import asyncio
import logging
import queue
from functools import lru_cache, wraps
from collections import defaultdict, namedtuple
import weakref

from models.instrument import Instrument, AssetClass
from core.event_manager import EventManager
from utils.constants import (OptionType, 
        Exchange, InstrumentType, NSE_INDICES, 
        BSE_INDICES, MarketDataType, SYMBOL_MAPPINGS)
from core.logging_manager import get_logger
from models.events import MarketDataEvent, OptionsSubscribedEvent
from core.event_manager import EventType


# Define named tuples for better memory usage and access speed
UnderlyingConfig = namedtuple('UnderlyingConfig', [
    'symbol', 'exchange', 'derivative_exchange', 'strike_interval', 
    'atm_range', 'expiry_offset'
])

ATMUpdateInfo = namedtuple('ATMUpdateInfo', ['new_atm', 'price', 'timestamp'])


class LockFreeCache:
    """A thread-safe cache that minimizes locking for read operations."""
    
    def __init__(self, ttl_seconds=60):
        self._data = {}
        self._timestamps = {}
        self._ttl = ttl_seconds
        self._write_lock = threading.RLock()
    
    def get(self, key, default=None):
        """Get value for key - no locking for reads."""
        if key in self._data:
            # Check if expired
            if time.time() - self._timestamps.get(key, 0) < self._ttl:
                return self._data[key]
            # Mark for cleanup on next write but don't block the read
        return default
    
    def set(self, key, value):
        """Set value with locking."""
        with self._write_lock:
            # Clean expired entries periodically
            if len(self._data) > 100 and random.random() < 0.01:  # 1% chance to clean
                self._clean_expired()
            
            self._data[key] = value
            self._timestamps[key] = time.time()
    
    def _clean_expired(self):
        """Clean expired entries."""
        now = time.time()
        expired_keys = [k for k, ts in self._timestamps.items() if now - ts >= self._ttl]
        for k in expired_keys:
            self._data.pop(k, None)
            self._timestamps.pop(k, None)


def profile(func):
    """Simple decorator for profiling hot methods."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self._high_throughput_mode and self.high_freq_logging:
            start_time = time.time()
            result = func(self, *args, **kwargs)
            elapsed = (time.time() - start_time) * 1000
            if elapsed > 1.0:  # Only log if took more than 1ms
                self.logger.debug(f"PROFILING: {func.__name__} took {elapsed:.2f}ms")
            return result
        else:
            return func(self, *args, **kwargs)
    return wrapper


class OptionManager:
    """
    Manages option chain data, subscriptions, and provides option trading utilities.
    Optimized for low-latency and high-throughput option data processing.
    
    Public API:
    - subscribe_underlying(underlying_symbol): Subscribe to an underlying
    - get_atm_strike(underlying_symbol): Get ATM strike price
    - get_option_instrument(underlying_symbol, strike, option_type, expiry_offset): Get option instrument
    - get_option_symbol(underlying, strike, option_type, expiry_offset): Get option symbol string
    - subscribe_atm_options(underlying_symbol, expiry_offset): Subscribe to ATM options
    - update_atm_options(underlying_symbol, expiry_offset): Update ATM option subscriptions
    - get_option_data(option_symbol): Get option data
    - get_active_option_instruments(underlying_symbol): Get active option instruments
    - get_active_option_symbols(underlying_symbol): Get active option symbols
    - get_atm_strikes(): Get all ATM strikes
    - start_tracking(): Start tracking configured underlyings
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
        
        # Configure log levels for high-frequency paths
        self.high_freq_logging = config.get('options', {}).get('high_frequency_logging', False)
        
        # Fine-grained locking
        self.price_lock = threading.RLock()  # For underlying_prices
        self.subscription_lock = threading.RLock()  # For option_subscriptions
        self.atm_lock = threading.RLock()  # For ATM strike data
        self.cache_lock = threading.RLock()  # For cache operations
        
        # Data structures with optimized memory usage
        self.underlying_prices = {}
        self._subscribed_underlyings = set()
        self.option_subscriptions = defaultdict(dict)
        
        # ATM strike tracking - separate locks
        self.current_atm_strikes = {}
        self.strike_intervals = {}
        self.atm_ranges = {}

        # Hysteresis buffer parameters
        options_config = config.get('options', {})
        self.buffer_strikes_factor = options_config.get('buffer_strikes_factor', 0.3)
        self.debounce_ms = options_config.get('debounce_ms', 500)
        self.pending_atm_updates = {}
        self.pending_timers = {}

        # Cache for constructed option symbols with weak references to reduce memory
        self.recently_constructed_symbols = defaultdict(weakref.WeakSet)
        self.symbol_cache_expiry = options_config.get('symbol_cache_expiry', 60)
        self.symbol_cache_timestamps = defaultdict(dict)

        # Option data cache - use TTL cache for auto-cleaning
        self.option_data_cache = LockFreeCache(ttl_seconds=options_config.get('cache_ttl', 300))
        
        # Use NumPy for faster numerical operations
        self.np_zeros = np.zeros(1)  # Pre-allocate for faster instantiation
        
        # Configure LRU cache for frequently used calculations
        self._get_atm_strike = lru_cache(maxsize=cache_size)(self._get_atm_strike_internal)
        self._get_strike_range_cached = lru_cache(maxsize=cache_size)(self._get_strike_range_internal)
        self._get_option_symbol_cached = lru_cache(maxsize=cache_size)(self._get_option_symbol_internal)
        self._get_option_instrument_cached = lru_cache(maxsize=cache_size)(self._get_option_instrument_internal)
        
        # Underlying configurations - store as named tuples for memory efficiency
        market_config = config.get('market', {})
        underlyings_config = market_config.get('underlyings', [])       
        self.underlyings = {}
        self._process_underlyings_config(underlyings_config)
            
        self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data, component_name="OptionManager")
        
        # Flag to indicate if we're in a high-throughput mode
        self._high_throughput_mode = False
        
        self.logger.info("Option Manager initialized with optimized performance settings")
    
    def set_high_throughput_mode(self, enabled: bool = True) -> None:
        """
        Enable or disable high-throughput mode which reduces logging and increases performance.
        
        Args:
            enabled: Whether to enable high-throughput mode
        """
        self._high_throughput_mode = enabled
        self.logger.info(f"High-throughput mode {'enabled' if enabled else 'disabled'}")
    
    def _process_underlyings_config(self, underlyings_config: List[Dict[str, Any]]):
        """Process and store underlying configurations."""
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
                self.logger.warning(f"Invalid spot exchange '{spot_exchange_str}' for {symbol}, using fallback.")
                spot_exchange = Exchange.NSE if symbol in NSE_INDICES else Exchange.BSE if symbol in BSE_INDICES else Exchange.NSE
            try:
                option_exchange = Exchange(option_exchange_str)
            except ValueError:
                self.logger.warning(f"Invalid option exchange '{option_exchange_str}' for {symbol}, using fallback.")
                option_exchange = Exchange.NFO if symbol in NSE_INDICES else Exchange.BFO if symbol in BSE_INDICES else Exchange.NFO

            strike_interval = float(underlying_config.get("strike_interval", 50.0 if "NIFTY" in symbol else 100.0))
            atm_range = int(underlying_config.get("atm_range", 1))
            expiry_offset = int(underlying_config.get("expiry_offset", 0))

            # Store in named tuple for memory efficiency and faster access
            config_tuple = UnderlyingConfig(
                symbol=symbol,
                exchange=spot_exchange,
                derivative_exchange=option_exchange,
                strike_interval=strike_interval,
                atm_range=atm_range,
                expiry_offset=expiry_offset
            )
            
            self.underlyings[symbol] = config_tuple
            self.strike_intervals[symbol] = strike_interval
            self.atm_ranges[symbol] = atm_range
            
            if not self._high_throughput_mode:
                self.logger.info(f"Configured underlying {symbol} with strike interval {strike_interval}, "
                                 f"ATM range {atm_range}, spot exch {spot_exchange.value}, "
                                 f"option exch {option_exchange.value}")

    def _cancel_pending_atm_update(self, underlying_symbol: str) -> None:
       """Cancel any pending ATM update for the given underlying."""
       with self.atm_lock:
           timer = self.pending_timers.pop(underlying_symbol, None)
           if timer:
               timer.cancel()
               if self.high_freq_logging and self.logger.isEnabledFor(logging.DEBUG):
                   self.logger.debug(f"Cancelled pending ATM update for {underlying_symbol}")
           # Also remove from pending updates
           self.pending_atm_updates.pop(underlying_symbol, None)
    
    def _schedule_atm_update(self, underlying_symbol: str, new_atm: float, price: float) -> None:
       """Schedule an ATM update with debounce logic."""
       with self.atm_lock:
           # Cancel any existing timer for this underlying
           self._cancel_pending_atm_update(underlying_symbol)
           
           # Create update info dictionary - use named tuple for better memory usage
           update_info = ATMUpdateInfo(
               new_atm=new_atm,
               price=price,
               timestamp=time.time()
           )
           self.pending_atm_updates[underlying_symbol] = update_info
           
           # Create and start a new timer
           timer = threading.Timer(self.debounce_ms / 1000.0, self._process_atm_update, args=[underlying_symbol])
           timer.daemon = True  # Allow the program to exit even if the timer is still running
           self.pending_timers[underlying_symbol] = timer
           timer.start()
           
           if self.high_freq_logging and self.logger.isEnabledFor(logging.DEBUG):
               self.logger.debug(f"Scheduled ATM update for {underlying_symbol} in {self.debounce_ms}ms: "
                                f"{new_atm} (price: {price:.2f})")
    
    def _process_atm_update(self, underlying_symbol: str) -> None:
        """Process the pending ATM update after the debounce period."""
        with self.atm_lock:
            # Check if the update is still pending (not cancelled)
            update_info = self.pending_atm_updates.pop(underlying_symbol, None)
            if not update_info:
                return  # Already cancelled or processed
            
            # Clean up the timer
            self.pending_timers.pop(underlying_symbol, None)
            
            # Get the cached values
            new_atm = update_info.new_atm
            price = update_info.price
            old_atm = self.current_atm_strikes.get(underlying_symbol)
            
            # Update the current ATM strike
            if not self._high_throughput_mode:
                self.logger.info(f"Processing delayed ATM strike change for {underlying_symbol}. "
                                f"Price: {price:.2f}, Old ATM: {old_atm}, New ATM: {new_atm}")
            
            # Store the new ATM strike
            self.current_atm_strikes[underlying_symbol] = new_atm
            
            # Perform the subscription update - release the lock before the expensive operation
        
        # Execute without holding the lock
        self._update_atm_subscriptions(underlying_symbol, new_atm, old_atm)

    def configure_underlyings(self, underlyings_config: List[Dict[str, Any]]):
        """Configure underlying symbols."""
        self._process_underlyings_config(underlyings_config)
    
    def subscribe_underlying(self, underlying_symbol: str) -> bool:
        """
        Subscribe to an underlying symbol.
        
        Args:
            underlying_symbol: The underlying symbol to subscribe to
            
        Returns:
            bool: True if subscribed successfully
        """
        with self.subscription_lock:
            if underlying_symbol in self._subscribed_underlyings:
                return True
                
        try:
            underlying_config = self.underlyings.get(underlying_symbol)
            if not underlying_config:
                self.logger.error(f"No configuration found for underlying {underlying_symbol}")
                return False

            exchange = underlying_config.exchange
            if not isinstance(exchange, Exchange):
                 self.logger.error(f"Invalid exchange configuration for {underlying_symbol}")
                 return False

            # Efficiently check if symbol is an index
            is_index = underlying_symbol in SYMBOL_MAPPINGS.get("indices", [])
            instrument_type = InstrumentType.INDEX if is_index else InstrumentType.EQUITY
            asset_class = AssetClass.INDEX if is_index else AssetClass.EQUITY

            instrument = Instrument(
                symbol=underlying_symbol,
                exchange=exchange,
                instrument_type=instrument_type,
                asset_class=asset_class,
                instrument_id=f"{exchange.value}:{underlying_symbol}"
            )
            
            success = self.data_manager.subscribe_instrument(instrument)
            
            if success:
                with self.subscription_lock:
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
        """
        Process market data events - critical high-frequency path.
        
        This method is optimized to minimize locking and processing time.
        """
        if not isinstance(event, MarketDataEvent) or not event.instrument:
            return

        instrument = event.instrument
        symbol = instrument.symbol
        
        # Check if it's an underlying first (faster path)
        with self.subscription_lock:
            is_subscribed_underlying = symbol in self._subscribed_underlyings
        
        if is_subscribed_underlying:
            self._process_underlying_data(symbol, event.data)
            return
            
        # If not an underlying, check if it's an option
        symbol_key = event.data.get("symbol_key", self.data_manager._get_symbol_key(instrument))
        
        if not symbol_key or symbol_key.startswith("INVALID:"):
            return  # Skip invalid data - don't even log in high-frequency path
            
        # Use lock-free cache for option data updates
        if symbol_key in self.option_data_cache._data:
            self.option_data_cache.set(symbol_key, event.data)
            
    def _process_underlying_data(self, underlying_symbol: str, data: Dict):
        """
        Process underlying data - extract price and update.
        Critical high-frequency path.
        """
        # Fast path for price extraction - prioritize LAST_PRICE
        raw_price = data.get(MarketDataType.LAST_PRICE.value)
        if raw_price is None:
            raw_price = data.get("LAST_PRICE")  
            if raw_price is None:
                raw_price = data.get("CLOSE")
                if raw_price is None:
                    return
                
        try:
            # Optimize string operations - only do if necessary
            if isinstance(raw_price, str) and ',' in raw_price:
                price_float = float(raw_price.replace(",", ""))
            else:
                price_float = float(raw_price)
                
            self.update_underlying_price(underlying_symbol, price_float)
            
        except (ValueError, TypeError):
            # Don't log in high-frequency path to avoid I/O overhead
            pass
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error in update_underlying_price: {e}", exc_info=True)
            
    def _clean_expired_symbol_cache(self) -> None:
        """Clean expired symbols from the cache - optimized version."""
        now = time.time()
        with self.cache_lock:
            # Process in batches for better performance
            for underlying_symbol, timestamps in list(self.symbol_cache_timestamps.items()):
                # Find expired symbols
                expired_symbols = [
                    symbol for symbol, timestamp in timestamps.items()
                    if now - timestamp > self.symbol_cache_expiry
                ]
                
                # Remove expired symbols
                for symbol in expired_symbols:
                    timestamps.pop(symbol, None)
                    
                    # Use weakref.WeakSet for auto-cleanup of references
                    if underlying_symbol in self.recently_constructed_symbols:
                        try:
                            self.recently_constructed_symbols[underlying_symbol].discard(symbol)
                        except KeyError:
                            pass
    
    @profile
    def update_underlying_price(self, underlying_symbol: str, price: float) -> None:
        """
        Update underlying price and manage ATM strikes.
        Critical high-frequency path.
        
        Args:
            underlying_symbol: The underlying symbol
            price: The new price
        """
        # Get strike interval outside lock to minimize lock time
        strike_interval = self.strike_intervals.get(underlying_symbol)
        if strike_interval is None:
            return  # Not a configured underlying
            
        # Calculate new ATM strike without lock
        new_atm = self._get_atm_strike_internal(price, strike_interval)
        
        with self.price_lock:
            # Quick retrieval of old values
            old_price = self.underlying_prices.get(underlying_symbol)
            self.underlying_prices[underlying_symbol] = price  # Update price immediately
            
            old_atm = self.current_atm_strikes.get(underlying_symbol)
            
            # Initial price or ATM setting (no hysteresis needed)
            if old_price is None or old_atm is None:
                self.current_atm_strikes[underlying_symbol] = new_atm
                
                if not self._high_throughput_mode:
                    self.logger.info(f"Initial ATM strike set for {underlying_symbol}. "
                                    f"Price: {price:.2f}, ATM: {new_atm}")
                    
                # Release lock before expensive operation
                atm_to_update = new_atm
                need_update = True
            elif new_atm != old_atm:
                # Calculate thresholds for hysteresis - fast calculation
                buffer_strikes = strike_interval * self.buffer_strikes_factor
                upper_threshold = old_atm + buffer_strikes
                lower_threshold = old_atm - buffer_strikes
                
                # Check if price has moved beyond the thresholds
                if (price >= upper_threshold and new_atm > old_atm) or (price <= lower_threshold and new_atm < old_atm):
                    # Schedule update with debounce
                    self._schedule_atm_update(underlying_symbol, new_atm, price)
                    need_update = False
                else:
                    # Check for direction reversal - cancel pending updates
                    with self.atm_lock:
                        pending_update = self.pending_atm_updates.get(underlying_symbol)
                        if pending_update and ((pending_update.new_atm > old_atm and new_atm <= old_atm) or
                                            (pending_update.new_atm < old_atm and new_atm >= old_atm)):
                            if self.high_freq_logging and self.logger.isEnabledFor(logging.DEBUG):
                                self.logger.debug(f"Price reversed direction for {underlying_symbol}, cancelling pending update")
                            self._cancel_pending_atm_update(underlying_symbol)
                    need_update = False
            else:
                need_update = False
                
        # Initial update without holding the price lock
        if need_update:
            self._update_atm_subscriptions(underlying_symbol, atm_to_update, old_atm)
            
    @profile
    def _update_atm_subscriptions(self, underlying_symbol: str, new_atm: float, old_atm: Optional[float]) -> None:
        """
        Update option subscriptions when ATM strike changes.
        
        Args:
            underlying_symbol: The underlying symbol
            new_atm: The new ATM strike
            old_atm: The old ATM strike
        """
        # Check if new is same as old - fast path
        if new_atm == old_atm:
            return
            
        # Get underlying configuration
        underlying_config = self.underlyings.get(underlying_symbol)
        if not underlying_config:
            return
            
        atm_range = self.atm_ranges.get(underlying_symbol, 1)
        strike_interval = self.strike_intervals.get(underlying_symbol)
        
        # Determine expiry offsets to update
        with self.subscription_lock:
            active_expiry_offsets = self.option_subscriptions.get(underlying_symbol, {}).keys()
            
        # If no active subscriptions yet, just use the default
        if not active_expiry_offsets:
            active_expiry_offsets = [underlying_config.expiry_offset]
        
        # Process each expiry offset
        for expiry_offset in active_expiry_offsets:
            # Calculate new strikes to subscribe/unsubscribe efficiently using NumPy
            if old_atm is not None:
                old_strikes = np.arange(
                    old_atm - (atm_range * strike_interval),
                    old_atm + (atm_range * strike_interval) + strike_interval,
                    strike_interval
                )
            else:
                old_strikes = np.array([])
                
            new_strikes = np.arange(
                new_atm - (atm_range * strike_interval),
                new_atm + (atm_range * strike_interval) + strike_interval,
                strike_interval
            )
            
            # Calculate additions and removals using NumPy set operations
            # for better performance than Python sets
            to_add = np.setdiff1d(new_strikes, old_strikes)
            to_remove = np.setdiff1d(old_strikes, new_strikes)
            
            # Process additions
            for strike in to_add:
                self._subscribe_option_strike(
                    underlying_symbol=underlying_symbol,
                    strike=float(strike),
                    expiry_offset=expiry_offset
                )
                
            # Process removals
            for strike in to_remove:
                self._unsubscribe_option_strike(
                    underlying_symbol=underlying_symbol,
                    strike=float(strike),
                    expiry_offset=expiry_offset
                )
        
        # Publish ATM update event if configured
        if not self._high_throughput_mode:
            self.event_manager.publish(
                EventType.ATM_STRIKE_CHANGED,
                {
                    "underlying": underlying_symbol,
                    "old_atm": old_atm,
                    "new_atm": new_atm
                }
            )
            
    @profile
    def _subscribe_option_strike(self, underlying_symbol: str, strike: float, expiry_offset: int) -> bool:
        """
        Subscribe to options for a specific strike.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            expiry_offset: The expiry offset
            
        Returns:
            bool: True if subscribed successfully
        """
        try:
            # Subscribe to both call and put
            call_success = self._subscribe_option(
                underlying_symbol=underlying_symbol,
                strike=strike,
                option_type=OptionType.CALL,
                expiry_offset=expiry_offset
            )
            
            put_success = self._subscribe_option(
                underlying_symbol=underlying_symbol,
                strike=strike,
                option_type=OptionType.PUT,
                expiry_offset=expiry_offset
            )
            
            return call_success and put_success
            
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error subscribing to strike {strike} for {underlying_symbol}: {e}", exc_info=True)
            return False

    @profile
    def _subscribe_option(self, underlying_symbol: str, strike: float, option_type: OptionType, expiry_offset: int) -> bool:
        """
        Subscribe to a specific option.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            option_type: Call or Put
            expiry_offset: The expiry offset
            
        Returns:
            bool: True if subscribed successfully
        """
        try:
            # Get the option instrument
            option_instrument = self.get_option_instrument(
                underlying_symbol=underlying_symbol,
                strike=strike,
                option_type=option_type,
                expiry_offset=expiry_offset
            )
            
            if not option_instrument:
                if not self._high_throughput_mode:
                    self.logger.error(f"Failed to get option instrument for {underlying_symbol} {strike} {option_type} {expiry_offset}")
                return False
                
            # Subscribe to the option
            success = self.data_manager.subscribe_instrument(option_instrument)
            
            if success:
                # Update subscription tracking
                option_key = f"{option_type.value}:{strike}"
                
                with self.subscription_lock:
                    # Flat map with compound keys for better performance
                    subscription_key = f"{underlying_symbol}:{expiry_offset}"
                    
                    if subscription_key not in self.option_subscriptions:
                        self.option_subscriptions[subscription_key] = {}
                        
                    self.option_subscriptions[subscription_key][option_key] = {
                        "instrument": option_instrument,
                        "timestamp": time.time()
                    }
                
                # Cache option data
                symbol_key = self.data_manager._get_symbol_key(option_instrument)
                self.option_data_cache.set(symbol_key, {})  # Initialize with empty data
                
                # Publish event if configured
                if not self._high_throughput_mode:
                    self.event_manager.publish(
                        EventType.OPTION_SUBSCRIBED,
                        {
                            "underlying": underlying_symbol,
                            "strike": strike,
                            "option_type": option_type.value,
                            "expiry_offset": expiry_offset,
                            "instrument": option_instrument
                        }
                    )
                
                return True
            else:
                if not self._high_throughput_mode:
                    self.logger.error(f"DataManager failed to subscribe to option {option_instrument.symbol}")
                return False
                
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error subscribing to option {underlying_symbol} {strike} {option_type} {expiry_offset}: {e}", exc_info=True)
            return False
            
    def _unsubscribe_option_strike(self, underlying_symbol: str, strike: float, expiry_offset: int) -> bool:
        """
        Unsubscribe from options for a specific strike.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            expiry_offset: The expiry offset
            
        Returns:
            bool: True if unsubscribed successfully
        """
        try:
            # Unsubscribe from both call and put
            call_success = self._unsubscribe_option(
                underlying_symbol=underlying_symbol,
                strike=strike,
                option_type=OptionType.CALL,
                expiry_offset=expiry_offset
            )
            
            put_success = self._unsubscribe_option(
                underlying_symbol=underlying_symbol,
                strike=strike,
                option_type=OptionType.PUT,
                expiry_offset=expiry_offset
            )
            
            return call_success and put_success
            
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error unsubscribing from strike {strike} for {underlying_symbol}: {e}", exc_info=True)
            return False
            
            
    @profile
    def _unsubscribe_option(self, underlying_symbol: str, strike: float, option_type: OptionType, expiry_offset: int) -> bool:
        """
        Unsubscribe from a specific option.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            option_type: Call or Put
            expiry_offset: The expiry offset
            
        Returns:
            bool: True if unsubscribed successfully
        """
        try:
            # Get option from subscription cache first for faster lookup
            option_key = f"{option_type.value}:{strike}"
            subscription_key = f"{underlying_symbol}:{expiry_offset}"
            
            with self.subscription_lock:
                if subscription_key not in self.option_subscriptions:
                    return True  # Already unsubscribed
                
                option_data = self.option_subscriptions[subscription_key].get(option_key)
                if not option_data:
                    return True  # Already unsubscribed
                
                option_instrument = option_data.get("instrument")
                if not option_instrument:
                    # Clean up invalid entry
                    self.option_subscriptions[subscription_key].pop(option_key, None)
                    return True
                
                # Remove from subscriptions before actual unsubscribe to prevent race conditions
                self.option_subscriptions[subscription_key].pop(option_key, None)
                
                # Clean up empty subscription maps
                if not self.option_subscriptions[subscription_key]:
                    self.option_subscriptions.pop(subscription_key, None)
            
            # Unsubscribe from the option
            success = self.data_manager.unsubscribe_instrument(option_instrument)
            
            # Clean up option data cache
            symbol_key = self.data_manager._get_symbol_key(option_instrument)
            with self.cache_lock:
                # Use _data directly for efficiency
                if symbol_key in self.option_data_cache._data:
                    self.option_data_cache._data.pop(symbol_key, None)
                    self.option_data_cache._timestamps.pop(symbol_key, None)
            
            # Publish event if configured
            if not self._high_throughput_mode:
                self.event_manager.publish(
                    EventType.OPTION_UNSUBSCRIBED,
                    {
                        "underlying": underlying_symbol,
                        "strike": strike,
                        "option_type": option_type.value,
                        "expiry_offset": expiry_offset,
                        "instrument": option_instrument
                    }
                )
            
            return success
            
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error unsubscribing from option {underlying_symbol} {strike} {option_type} {expiry_offset}: {e}", exc_info=True)
            return False

    def _get_atm_strike_internal(self, price: float, strike_interval: float) -> float:
        """
        Calculate ATM strike based on price and strike interval.
        Optimized internal implementation for LRU caching.
        
        Args:
            price: The price
            strike_interval: The strike interval
            
        Returns:
            float: The ATM strike
        """
        # Fast path using NumPy for better performance
        return np.floor(price / strike_interval) * strike_interval + (strike_interval / 2)

    def get_atm_strike(self, underlying_symbol: str) -> Optional[float]:
        """
        Get the current ATM strike for an underlying.
        
        Args:
            underlying_symbol: The underlying symbol
            
        Returns:
            Optional[float]: The ATM strike or None if not found
        """
        with self.atm_lock:
            return self.current_atm_strikes.get(underlying_symbol)

    def _get_strike_range_internal(self, atm_strike: float, strike_interval: float, range_count: int) -> np.ndarray:
        """
        Calculate the range of strikes around ATM.
        Uses NumPy for fast array operations.
        
        Args:
            atm_strike: The ATM strike
            strike_interval: The strike interval
            range_count: Number of strikes on each side
            
        Returns:
            np.ndarray: Array of strike prices
        """
        # Use NumPy for better performance than Python lists
        return np.arange(
            atm_strike - (range_count * strike_interval),
            atm_strike + (range_count * strike_interval) + strike_interval,
            strike_interval
        )

    def get_strike_range(self, underlying_symbol: str) -> List[float]:
        """
        Get the range of strikes for an underlying based on ATM and configured range.
        
        Args:
            underlying_symbol: The underlying symbol
            
        Returns:
            List[float]: List of strike prices
        """
        with self.atm_lock:
            atm_strike = self.current_atm_strikes.get(underlying_symbol)
            if atm_strike is None:
                return []
            
            strike_interval = self.strike_intervals.get(underlying_symbol)
            range_count = self.atm_ranges.get(underlying_symbol, 1)
            
        # Convert numpy array to list for API compatibility
        return self._get_strike_range_cached(atm_strike, strike_interval, range_count).tolist()

    def _get_option_symbol_internal(self, underlying: str, strike: float, option_type: str, expiry_offset: int = 0) -> Optional[str]:
        """
        Internal method to construct option symbol.
        Optimized for caching.
        
        Args:
            underlying: The underlying symbol
            strike: The strike price
            option_type: Call or Put
            expiry_offset: Expiry offset in weeks
            
        Returns:
            Optional[str]: Option symbol or None if error
        """
        try:
            # Get underlying config
            underlying_config = self.underlyings.get(underlying)
            if not underlying_config:
                return None
            
            # Get expiry date
            expiry = self.data_manager.get_next_expiry(
                underlying=underlying,
                offset=expiry_offset
            )
            if not expiry:
                return None
            
            # Format strike with appropriate precision
            strike_str = f"{int(strike)}" if strike.is_integer() else f"{strike}"
            
            # Construct option symbol based on exchange format
            exchange = underlying_config.derivative_exchange
            if exchange == Exchange.NFO or exchange == Exchange.BFO:
                # NSE format: NIFTY23JUN20000CE
                month_str = expiry.strftime("%b").upper()
                year_str = expiry.strftime("%y")
                return f"{underlying}{year_str}{month_str}{strike_str}{option_type}"
            else:
                # Generic format
                expiry_str = expiry.strftime("%Y%m%d")
                return f"{underlying}_{expiry_str}_{strike_str}_{option_type}"
                
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error constructing option symbol: {e}", exc_info=True)
            return None

    def get_option_symbol(self, underlying: str, strike: float, option_type: str, expiry_offset: int = 0) -> Optional[str]:
        """
        Get option symbol for trading.
        
        Args:
            underlying: The underlying symbol
            strike: The strike price
            option_type: Call or Put
            expiry_offset: Expiry offset in weeks
            
        Returns:
            Optional[str]: Option symbol or None if error
        """
        # Validate inputs
        if not isinstance(underlying, str) or not underlying:
            return None
        
        if not isinstance(strike, (int, float)) or strike <= 0:
            return None
        
        if not isinstance(option_type, str) or option_type not in ("CE", "PE"):
            if isinstance(option_type, OptionType):
                option_type = option_type.value
            else:
                return None
        
        # Cache hit check for recently constructed symbols
        with self.cache_lock:
            # Check if we have this symbol in the recently constructed symbols cache
            if underlying in self.recently_constructed_symbols:
                # Create a lookup key for faster matching
                lookup_key = f"{underlying}:{strike}:{option_type}:{expiry_offset}"
                for symbol in self.recently_constructed_symbols[underlying]:
                    if symbol.startswith(lookup_key):
                        # Update timestamp to keep it fresh
                        self.symbol_cache_timestamps[underlying][symbol] = time.time()
                        return symbol.split(':', 1)[1]  # Return the actual symbol part
        
        # Cache miss - calculate the symbol
        symbol = self._get_option_symbol_cached(underlying, float(strike), option_type, expiry_offset)
        
        # Cache the result for future use
        if symbol:
            with self.cache_lock:
                # Create compound key for cache
                cache_key = f"{underlying}:{strike}:{option_type}:{expiry_offset}:{symbol}"
                # Store in weakref set for auto-cleanup
                if underlying not in self.recently_constructed_symbols:
                    self.recently_constructed_symbols[underlying] = weakref.WeakSet()
                self.recently_constructed_symbols[underlying].add(cache_key)
                # Record timestamp
                if underlying not in self.symbol_cache_timestamps:
                    self.symbol_cache_timestamps[underlying] = {}
                self.symbol_cache_timestamps[underlying][cache_key] = time.time()
                
                # Periodically clean cache (1% chance)
                if random.random() < 0.01:
                    self._clean_expired_symbol_cache()
        
        return symbol

    def _get_option_instrument_internal(self, underlying_symbol: str, strike: float, option_type: str, expiry_offset: int = 0) -> Optional[Instrument]:
        """
        Internal implementation to get option instrument.
        Optimized for caching.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            option_type: Call or Put
            expiry_offset: Expiry offset in weeks
            
        Returns:
            Optional[Instrument]: Option instrument or None if error
        """
        try:
            # Get option symbol
            option_symbol = self.get_option_symbol(underlying_symbol, strike, option_type, expiry_offset)
            if not option_symbol:
                return None
            
            # Get underlying config
            underlying_config = self.underlyings.get(underlying_symbol)
            if not underlying_config:
                return None
            
            # Create instrument
            instrument = Instrument(
                symbol=option_symbol,
                exchange=underlying_config.derivative_exchange,
                instrument_type=InstrumentType.OPTION,
                asset_class=AssetClass.DERIVATIVE,
                instrument_id=f"{underlying_config.derivative_exchange.value}:{option_symbol}",
                option_data={
                    "underlying": underlying_symbol,
                    "strike": strike,
                    "option_type": option_type,
                    "expiry_offset": expiry_offset
                }
            )
            
            return instrument
            
        except Exception as e:
            if not self._high_throughput_mode:
                self.logger.error(f"Error getting option instrument: {e}", exc_info=True)
            return None

    def get_option_instrument(self, underlying_symbol: str, strike: float, option_type: Union[str, OptionType], expiry_offset: int = 0) -> Optional[Instrument]:
        """
        Get option instrument for trading.
        
        Args:
            underlying_symbol: The underlying symbol
            strike: The strike price
            option_type: Call or Put (string or OptionType enum)
            expiry_offset: Expiry offset in weeks
            
        Returns:
            Optional[Instrument]: Option instrument or None if error
        """
        # Convert OptionType enum to string if needed
        if isinstance(option_type, OptionType):
            option_type = option_type.value
        
        return self._get_option_instrument_cached(underlying_symbol, float(strike), option_type, expiry_offset)

    def get_option_data(self, option_symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get option market data.
        
        Args:
            option_symbol: The option symbol
            
        Returns:
            Optional[Dict[str, Any]]: Option data or None if not found
        """
        # Direct access to the cache without locking for reads
        return self.option_data_cache.get(option_symbol)

    def get_active_option_instruments(self, underlying_symbol: Optional[str] = None) -> List[Instrument]:
        """
        Get list of active option instruments.
        
        Args:
            underlying_symbol: Filter by underlying symbol
            
        Returns:
            List[Instrument]: List of active option instruments
        """
        result = []
        
        with self.subscription_lock:
            # If underlying is specified, only get options for that underlying
            if underlying_symbol:
                # Use compound keys for faster filtering
                subscription_keys = [k for k in self.option_subscriptions.keys() if k.startswith(f"{underlying_symbol}:")]
                for key in subscription_keys:
                    for option_data in self.option_subscriptions[key].values():
                        if "instrument" in option_data:
                            result.append(option_data["instrument"])
            else:
                # Get all options
                for options_dict in self.option_subscriptions.values():
                    for option_data in options_dict.values():
                        if "instrument" in option_data:
                            result.append(option_data["instrument"])
        
        return result

    def get_active_option_symbols(self, underlying_symbol: Optional[str] = None) -> List[str]:
        """
        Get list of active option symbols.
        
        Args:
            underlying_symbol: Filter by underlying symbol
            
        Returns:
            List[str]: List of active option symbols
        """
        instruments = self.get_active_option_instruments(underlying_symbol)
        return [instrument.symbol for instrument in instruments]

    def get_atm_strikes(self) -> Dict[str, float]:
        """
        Get all ATM strikes.
        
        Returns:
            Dict[str, float]: Map of underlying symbol to ATM strike
        """
        with self.atm_lock:
            # Return a copy to avoid threading issues
            return dict(self.current_atm_strikes)

    def start_tracking(self) -> bool:
        """
        Start tracking configured underlyings. Subscribes to underlyings only.
        Option subscriptions happen reactively when underlying prices arrive.

        Returns:
            bool: True if tracking started successfully for at least one underlying.
        """
        success_count = 0
        self.logger.info("Starting underlying tracking...")
        
        # Get configured underlyings without lock
        configured_underlyings = list(self.underlyings.keys())
        if not configured_underlyings:
            self.logger.warning("No underlyings configured in OptionManager.")
            return False

        for underlying_symbol in configured_underlyings:
            if self.subscribe_underlying(underlying_symbol):
                success_count += 1
            # Log error inside subscribe_underlying

        if success_count > 0:
            self.logger.info(f"Underlying tracking started for {success_count}/{len(configured_underlyings)} configured underlyings.")
            return True
        else:
            self.logger.error("Failed to start tracking for any configured underlyings.")
            return False


class AsyncOptionManager(OptionManager):
    """
    Asynchronous version of OptionManager that uses an event loop for processing.
    This improves throughput and reduces latency by decoupling event processing
    from market data callbacks.
    """
    
    def __init__(self, data_manager, event_manager, position_manager, config: Dict[str, Any], cache_size=1000):
        """Initialize AsyncOptionManager with an event queue."""
        super().__init__(data_manager, event_manager, position_manager, config, cache_size)
        
        # Event processing queue
        self.event_queue = queue.Queue()
        
        # Event processing thread
        self.event_thread = None
        self.running = False
        
        # Set high throughput mode by default
        self.set_high_throughput_mode(True)
        
        self.logger.info("AsyncOptionManager initialized with event queue")
    
    def start(self):
        """Start the event processing thread."""
        if self.event_thread and self.event_thread.is_alive():
            self.logger.warning("AsyncOptionManager already running")
            return
        
        self.running = True
        self.event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self.event_thread.start()
        self.logger.info("AsyncOptionManager event loop started")
        
        # Start tracking after event loop is running
        return self.start_tracking()
    
    def stop(self):
        """Stop the event processing thread."""
        self.running = False
        
        if self.event_thread and self.event_thread.is_alive():
            # Put a stop event in the queue
            self.event_queue.put(None)
            
            # Wait for thread to terminate with timeout
            self.event_thread.join(timeout=5.0)
            if self.event_thread.is_alive():
                self.logger.warning("AsyncOptionManager event thread did not terminate gracefully")
            else:
                self.logger.info("AsyncOptionManager event thread terminated")
        
        self.event_thread = None
    
    def _event_loop(self):
        """Main event processing loop."""
        self.logger.info("AsyncOptionManager event loop started")
        
        while self.running:
            try:
                # Get event from queue with timeout
                event = self.event_queue.get(timeout=0.1)
                
                # None event means stop
                if event is None:
                    break
                
                # Process event based on type
                event_type = event.get("type")
                if event_type == "market_data":
                    self._process_market_data_async(event.get("data"))
                elif event_type == "atm_update":
                    self._process_atm_update_async(event.get("data"))
                else:
                    self.logger.warning(f"Unknown event type: {event_type}")
                
                # Mark task as done
                self.event_queue.task_done()
                
            except queue.Empty:
                # Timeout - just continue
                continue
            except Exception as e:
                self.logger.error(f"Error in event loop: {e}", exc_info=True)
        
        self.logger.info("AsyncOptionManager event loop terminated")
    
    def _on_market_data(self, event: MarketDataEvent):
        """
        Enqueue market data for async processing instead of processing directly.
        This keeps the event callback fast and non-blocking.
        """
        if not isinstance(event, MarketDataEvent) or not event.instrument:
            return
        
        # Enqueue event for async processing
        self.event_queue.put({
            "type": "market_data",
            "data": {
                "instrument": event.instrument,
                "data": event.data,
                "timestamp": time.time()
            }
        })
    
    def _process_market_data_async(self, event_data: Dict[str, Any]):
        """
        Process market data asynchronously.
        """
        instrument = event_data.get("instrument")
        data = event_data.get("data")
        
        if not instrument or not data:
            return
        
        symbol = instrument.symbol
        
        # Use the optimized processing methods from parent class
        with self.subscription_lock:
            is_subscribed_underlying = symbol in self._subscribed_underlyings
        
        if is_subscribed_underlying:
            self._process_underlying_data(symbol, data)
        else:
            # If not an underlying, check if it's an option
            symbol_key = data.get("symbol_key", self.data_manager._get_symbol_key(instrument))
            
            if symbol_key and not symbol_key.startswith("INVALID:"):
                # Use lock-free cache for option data updates
                self.option_data_cache.set(symbol_key, data)
    
    def _schedule_atm_update(self, underlying_symbol: str, new_atm: float, price: float) -> None:
        """
        Enqueue ATM update for async processing instead of using timers.
        """
        # Cancel any existing timer
        with self.atm_lock:
            self._cancel_pending_atm_update(underlying_symbol)
            
            # Store update info
            update_info = ATMUpdateInfo(
                new_atm=new_atm,
                price=price,
                timestamp=time.time()
            )
            self.pending_atm_updates[underlying_symbol] = update_info
            
            # Create a timer that will enqueue an event
            timer = threading.Timer(
                self.debounce_ms / 1000.0,
                self._enqueue_atm_update,
                args=[underlying_symbol]
            )
            timer.daemon = True
            self.pending_timers[underlying_symbol] = timer
            timer.start()
    
    def _enqueue_atm_update(self, underlying_symbol: str):
        """
        Enqueue ATM update for async processing.
        """
        with self.atm_lock:
            update_info = self.pending_atm_updates.get(underlying_symbol)
            if not update_info:
                return
            
            # Enqueue event
            self.event_queue.put({
                "type": "atm_update",
                "data": {
                    "underlying": underlying_symbol,
                    "update_info": update_info
                }
            })
    
    def _process_atm_update_async(self, event_data: Dict[str, Any]):
        """
        Process ATM update asynchronously.
        """
        underlying_symbol = event_data.get("underlying")
        update_info = event_data.get("update_info")
        
        if not underlying_symbol or not update_info:
            return
        
        with self.atm_lock:
            # Clean up
            self.pending_atm_updates.pop(underlying_symbol, None)
            self.pending_timers.pop(underlying_symbol, None)
            
            # Get values
            new_atm = update_info.new_atm
            price = update_info.price
            old_atm = self.current_atm_strikes.get(underlying_symbol)
            
            # Update ATM strike
            self.current_atm_strikes[underlying_symbol] = new_atm
            
            if not self._high_throughput_mode:
                self.logger.info(f"Processing delayed ATM strike change for {underlying_symbol}. "
                                f"Price: {price:.2f}, Old ATM: {old_atm}, New ATM: {new_atm}")
        
        # Execute update without holding the lock
        self._update_atm_subscriptions(underlying_symbol, new_atm, old_atm)


# Performance metrics collector for hot-path profiling
class PerformanceMetrics:
    """
    Collect performance metrics for hot methods.
    """
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.enabled = False
        self.lock = threading.RLock()
    
    def record(self, method_name: str, elapsed_ms: float):
        """Record a method execution time."""
        if not self.enabled:
            return
        
        with self.lock:
            self.metrics[method_name].append(elapsed_ms)
    
    def get_summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary statistics for all methods."""
        result = {}
        
        with self.lock:
            for method_name, timings in self.metrics.items():
                if not timings:
                    continue
                
                # Calculate statistics
                avg = sum(timings) / len(timings)
                min_time = min(timings)
                max_time = max(timings)
                p95 = sorted(timings)[int(len(timings) * 0.95)] if timings else 0
                
                result[method_name] = {
                    "count": len(timings),
                    "avg_ms": avg,
                    "min_ms": min_time,
                    "max_ms": max_time,
                    "p95_ms": p95
                }
        
        return result
    
    def reset(self):
        """Reset all metrics."""
        with self.lock:
            self.metrics.clear()


# Global performance metrics collector
performance_metrics = PerformanceMetrics()


def create_option_manager(data_manager, event_manager, position_manager, config: Dict[str, Any], async_mode: bool = True, cache_size: int = 1000):
    """
    Factory function to create appropriate OptionManager instance.
    
    Args:
        data_manager: Data manager instance
        event_manager: Event manager instance
        position_manager: Position manager instance
        config: Configuration dictionary
        async_mode: Whether to use AsyncOptionManager
        cache_size: Size of LRU cache
        
    Returns:
        OptionManager or AsyncOptionManager instance
    """
    if async_mode:
        return AsyncOptionManager(data_manager, event_manager, position_manager, config, cache_size)
    else:
        return OptionManager(data_manager, event_manager, position_manager, config, cache_size)        