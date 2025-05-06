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
from datetime import datetime, date, timedelta
import time
import numpy as np
from functools import lru_cache

from models.instrument import Instrument, AssetClass
from core.event_manager import EventManager
from utils.constants import OptionType, Exchange, InstrumentType, NSE_INDICES, BSE_INDICES, MarketDataType, SYMBOL_MAPPINGS
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
        
        # Dictionaries to store option data and subscriptions
        self.underlying_prices = {}  # symbol -> current price
        self.underlying_subscriptions = set()  # Set of subscribed underlyings
        self.option_subscriptions = {}  # underlying_symbol -> {option_symbols}
        self.option_chain_data = {}  # underlying_symbol -> {expiry -> {strike -> {CE/PE -> option_info}}}
        
        # ATM strike tracking
        self.current_atm_strikes = {}  # underlying_symbol -> current ATM strike
        self.strike_intervals = {}  # underlying_symbol -> strike interval
        self.atm_ranges = {}  # underlying_symbol -> number of strikes around ATM to maintain
        
        # Option data cache
        self.option_data_cache = {}  # option_symbol -> latest data
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Configure LRU cache for frequently used calculations
        self.get_atm_strike = lru_cache(maxsize=cache_size)(self._get_atm_strike_internal)
        # self.get_option_symbol = lru_cache(maxsize=cache_size)(self._get_option_symbol)
        
        # Underlying configurations
        market_config = config.get('market', {})
        underlyings_config = market_config.get('underlyings', [])       
        
        # Initialize underlyings dictionary
        self.underlyings = {}

        # Process underlyings config (improved handling)
        self._process_underlyings_config(underlyings_config) 
            
        # Track subscribed underlyings
        self._subscribed_underlyings: Set[str] = set()
       
        # Commented out for now as we are using the strategy manager to receive market data events
        self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data)
        
        self.logger.info("Option Manager initialized")
    
    def _process_underlyings_config(self, underlyings_config: List[Dict[str, Any]]):
        """Helper to process underlying configurations from list."""
        if not isinstance(underlyings_config, list):
            self.logger.error("Underlyings configuration is not a list. Please check config.yaml.")
            return

        for underlying_config in underlyings_config:
            # Handle both old ('symbol') and new ('name') format for the underlying identifier
            symbol = underlying_config.get('name', underlying_config.get('symbol'))
            if not symbol:
                self.logger.warning(f"Skipping underlying config without name/symbol: {underlying_config}")
                continue

            # Extract exchanges with support for both old and new format
            spot_exchange_str = underlying_config.get('spot_exchange', underlying_config.get('exchange'))
            option_exchange_str = underlying_config.get('option_exchange', underlying_config.get('derivative_exchange'))

            # Set default exchanges if not provided, based on known indices
            if not spot_exchange_str:
                if symbol in NSE_INDICES: spot_exchange_str = Exchange.NSE.value
                elif symbol in BSE_INDICES: spot_exchange_str = Exchange.BSE.value
                else: spot_exchange_str = 'NSE' # Default fallback
            if not option_exchange_str:
                if symbol in NSE_INDICES: option_exchange_str = Exchange.NFO.value
                elif symbol in BSE_INDICES: option_exchange_str = Exchange.BFO.value
                else: option_exchange_str = 'NFO' # Default fallback

            # Convert exchange strings to Enum members
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


            strike_interval = underlying_config.get('strike_interval', 50)
            atm_range = underlying_config.get('atm_range', 1) # Default ATM range from logs

            # Store unified config
            self.underlyings[symbol] = {
                'symbol': symbol,
                'exchange': spot_exchange,
                'derivative_exchange': option_exchange,
                'strike_interval': strike_interval,
                'atm_range': atm_range,
                # Copy other potential fields
                **{k: v for k, v in underlying_config.items() if k not in
                   ['name', 'symbol', 'spot_exchange', 'exchange', 'option_exchange', 'derivative_exchange', 'strike_interval', 'atm_range']}
            }
            # Store strike interval separately for quick lookup
            self.strike_intervals[symbol] = strike_interval
            self.atm_ranges[symbol] = atm_range

            self.logger.info(f"Configured underlying {symbol} with strike interval {strike_interval}, ATM range {atm_range}, exchange {spot_exchange.value}, and derivative exchange {option_exchange.value}")

    def configure_underlyings(self, underlyings_config: List[Dict[str, Any]]):
        """
        Configure underlyings from the provided configuration. (Can be called again if needed)

        Args:
            underlyings_config: List of underlying configurations.
        """
        with self.lock:
            self._process_underlyings_config(underlyings_config)
    
    def subscribe_underlying(self, underlying_symbol: str) -> bool:
        """
        Subscribe to an underlying for market data.

        Args:
            underlying_symbol: Underlying symbol (e.g., "NIFTY INDEX") to subscribe to.

        Returns:
            bool: True if subscription successful or already subscribed, False otherwise.
        """
        if underlying_symbol in self._subscribed_underlyings:
            # self.logger.info(f"Already subscribed to underlying {underlying_symbol}")
            return True

        try:
            underlying_config = self.underlyings.get(underlying_symbol)
            if not underlying_config:
                self.logger.error(f"No configuration found for underlying {underlying_symbol}")
                return False

            exchange = underlying_config.get('exchange') # Should be an Enum now
            if not isinstance(exchange, Exchange):
                 self.logger.error(f"Invalid exchange configuration for {underlying_symbol}")
                 return False

            # Create instrument for the underlying
            # Determine instrument type and asset class (basic heuristic)
            instrument_type = InstrumentType.INDEX if underlying_symbol in NSE_INDICES or underlying_symbol in BSE_INDICES else InstrumentType.EQUITY
            asset_class = AssetClass.INDEX if instrument_type == InstrumentType.INDEX else AssetClass.EQUITY

            instrument = Instrument(
                symbol=underlying_symbol,
                exchange=exchange,
                instrument_type=instrument_type,
                asset_class=asset_class,
                # Use DataManager's preferred key format if available, else construct one
                instrument_id=f"{exchange.value}:{underlying_symbol}"
            )

            # Subscribe using DataManager's generic subscribe method
            # DataManager needs to handle the actual broker/feed subscription
            if self.data_manager.subscribe(instrument): # Pass the full Instrument object
                self._subscribed_underlyings.add(underlying_symbol)
                self.logger.info(f"Successfully subscribed to underlying {underlying_symbol}")
                return True
            else:
                self.logger.error(f"DataManager failed to subscribe to underlying {underlying_symbol}")
                return False

        except Exception as e:
            self.logger.error(f"Error subscribing to underlying {underlying_symbol}: {e}", exc_info=True)
            return False
            
    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events. Filters for relevant underlyings or options.

        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent) or not event.instrument:
            # self.logger.debug("Received non-market data or event without instrument")
            return

        instrument = event.instrument
        symbol = instrument.symbol # e.g., "NIFTY INDEX" or "NIFTY08MAY25C24350"
        symbol_key = event.data.get('symbol_key') # e.g., "NSE:NIFTY INDEX" or "NFO:NIFTY08MAY25C24350"

        if not symbol_key:
            self.logger.warning(f"MarketDataEvent for {symbol} missing 'symbol_key'. Cannot process.")
            return

        # self.logger.debug(f"Received MarketDataEvent for {symbol_key}")

        # Check if it's one of the underlyings we are managing
        if symbol in self._subscribed_underlyings:
            # Process underlying market data
            self._process_underlying_data(symbol, event.data)

        # Check if it's one of the options we have subscribed to
        elif any(symbol_key in sub_dict for sub_dict in self.option_subscriptions.values()):
             # Cache the latest option data
             self.option_data_cache[symbol_key] = event.data
             # self.logger.debug(f"Updated option cache for {symbol_key}")
             # Note: We don't trigger ATM updates from option ticks, only underlying ticks.
            
    def _process_underlying_data(self, underlying_symbol: str, data: Dict):
        """
        Process market data for a managed underlying. Updates price and checks ATM.

        Args:
            underlying_symbol: Underlying symbol (e.g., "NIFTY INDEX")
            data: Market data dictionary (MarketDataEvent.data)
        """
        # Extract raw price from the standardized keys
        raw_price = data.get(
            MarketDataType.LAST_PRICE.value,
            data.get('LAST_PRICE', data.get('CLOSE'))
        )
        if raw_price is None:
            return

        # 1) Convert to float (only catch conversion errors here)
        try:
            # handle strings with commas, ints, floats, etc.
            if isinstance(raw_price, str):
                price_clean = raw_price.replace(',', '')
                price_float = float(price_clean)
            else:
                price_float = float(raw_price)
        except (ValueError, TypeError) as conv_err:
            self.logger.warning(
                f"Could not convert price {raw_price!r} (type {type(raw_price)}) "
                f"to float for {underlying_symbol}: {conv_err}"
            )
            return

        # 2) Now update the underlying price and catch _its_ errors separately
        try:
            self.update_underlying_price(underlying_symbol, price_float)
        except Exception as upd_err:
            # This is truly an update/ATM‐logic error, not a conversion error
            self.logger.error(
                f"Error in update_underlying_price for {underlying_symbol} "
                f"with price {price_float}: {upd_err}"
            )

    # def _process_underlying_data(self, underlying_symbol: str, data: Dict):
    #     """
    #     Process market data for a managed underlying. Updates price and checks ATM.

    #     Args:
    #         underlying_symbol: Underlying symbol (e.g., "NIFTY INDEX")
    #         data: Market data dictionary (MarketDataEvent.data)
    #     """
    #     # Use standard MarketDataType keys if available, fallback to original keys
    #     price = data.get(MarketDataType.LAST_PRICE.value, data.get('LAST_PRICE', data.get('CLOSE')))

    #     if price is not None:
    #         try:
    #             # Add explicit type conversion - handle both numeric and string formats
    #             if isinstance(price, (int, float)):
    #                 price_float = float(price)
    #             else:
    #                 # For string representation, handle comma-separated numbers
    #                 price_str = str(price).replace(',', '')
    #                 price_float = float(price_str)
                
    #             # Update underlying price cache and check if ATM strike needs updating
    #             self.update_underlying_price(underlying_symbol, price_float)
    #         except (ValueError, TypeError) as e:
    #             self.logger.warning(f"Could not convert price '{price}' (type: {type(price)}) to float for {underlying_symbol}: {e}")
    #     # else:
    #         # self.logger.debug(f"No price found in data for {underlying_symbol}: {data.keys()}")
            
    def update_underlying_price(self, underlying_symbol: str, price: float) -> None:
        """
        Update the cached underlying price and check if ATM strikes need to be updated.
        This is the main trigger for subscribing/unsubscribing options.

        Args:
            underlying_symbol: The underlying symbol (e.g., "NIFTY INDEX")
            price: The current price of the underlying
        """
        with self.lock:
            old_price = self.underlying_prices.get(underlying_symbol)
            self.underlying_prices[underlying_symbol] = price

            if underlying_symbol not in self.strike_intervals:
                # self.logger.warning(f"Cannot update ATM for {underlying_symbol}: strike interval not configured")
                return # Config likely not loaded yet or symbol not managed

            strike_interval = self.strike_intervals[underlying_symbol]

            # Calculate potential new ATM strike
            new_atm = self._get_atm_strike_internal(price, strike_interval)
            old_atm = self.current_atm_strikes.get(underlying_symbol)

            # If it's the first price update for this underlying
            if old_price is None:
                self.logger.info(f"First price received for {underlying_symbol}: {price:.2f}, ATM strike: {new_atm}")
                self.current_atm_strikes[underlying_symbol] = new_atm
                # Trigger initial subscription to ATM options
                self.subscribe_atm_options(underlying_symbol) # Will publish event if successful
            # If ATM strike has changed
            elif old_atm != new_atm:
                self.logger.info(f"ATM strike for {underlying_symbol} changed from {old_atm} to {new_atm} (Price: {price:.2f})")
                self.current_atm_strikes[underlying_symbol] = new_atm
                # Update option subscriptions based on the new ATM strike
                self.update_atm_options(underlying_symbol) # Will publish event if successful
    
    # Renamed to avoid conflict with cached method
    def _get_atm_strike_internal(self, price: float, strike_interval: float) -> float:
        """Internal method to calculate ATM strike."""
        if strike_interval <= 0:
            self.logger.error(f"Invalid strike interval: {strike_interval}")
            return price # Fallback
        return round(price / strike_interval) * strike_interval

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
                return self.get_atm_strike(current_price, strike_interval)
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
