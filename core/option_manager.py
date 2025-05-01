"""
Option Manager module for option trading.

This module handles option-specific functionality including:
- Option chain management
- ATM strike calculation
- Option subscription/unsubscription based on underlying movements
- Option data caching and management
"""

import logging
import threading
from typing import Dict, List, Set, Optional, Tuple, Any
from datetime import datetime, date, timedelta
import time
import numpy as np
from functools import lru_cache

from models.instrument import Instrument, AssetClass
from core.event_manager import EventManager
from utils.constants import OptionType, Exchange, InstrumentType, NSE_INDICES, BSE_INDICES
from core.logging_manager import get_logger
from models.events import MarketDataEvent
from core.event_manager import EventType

class OptionManager:
    """
    Manages option chain data, subscriptions, and provides option trading utilities.
    Optimized for low-latency and high-throughput option data processing.
    """
    
    def __init__(self, data_manager, event_manager, config: Dict[str, Any], cache_size=1000):
        """
        Initialize the Option Manager.
        
        Args:
            data_manager: Data manager instance for market data operations
            event_manager: Event manager for publishing/subscribing to events
            config: Configuration dictionary
            cache_size: Size of LRU cache for option calculations
        """
        self.logger = get_logger("core.option_manager")
        self.data_manager = data_manager
        self.event_manager = event_manager
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
        self.get_atm_strike = lru_cache(maxsize=cache_size)(self._get_atm_strike)
        self.get_option_symbol = lru_cache(maxsize=cache_size)(self._get_option_symbol)
        
        # Underlying configurations
        market_config = config.get('market', {})
        underlyings_config = market_config.get('underlyings', [])

        self.underlying_mapping = {
            "NIFTY INDEX": "NIFTY",
            "NIFTY BANK": "BANKNIFTY",
            "MIDCPNIFTY": "MIDCAP",
            "FINNIFTY": "FINNIFTY",
            "SENSEX": "SENSEX",
            "BANKEX": "BANKEX"
        }
        
        # Initialize underlyings dictionary
        self.underlyings = {}
        
        # Convert list configuration to dictionary if needed
        if isinstance(underlyings_config, list):
            for underlying_config in underlyings_config:
                # Handle both old and new format
                # New format: {'name': 'NIFTY INDEX', 'spot_exchange': 'NSE', 'option_exchange': 'NFO'}
                # Old format: {'symbol': 'NIFTY INDEX', 'exchange': 'NSE', 'derivative_exchange': 'NFO'}
                
                # Extract symbol/name
                symbol = underlying_config.get('name', underlying_config.get('symbol'))
                if not symbol:
                    self.logger.warning(f"Skipping underlying config without name/symbol: {underlying_config}")
                    continue
                
                # Extract exchanges
                spot_exchange = underlying_config.get('spot_exchange', underlying_config.get('exchange'))
                option_exchange = underlying_config.get('option_exchange', underlying_config.get('derivative_exchange'))
                
                # Set default exchanges if not provided
                if not spot_exchange or not option_exchange:
                    if symbol in NSE_INDICES:
                        spot_exchange = Exchange.NSE.value
                        option_exchange = Exchange.NFO.value
                    elif symbol in BSE_INDICES:
                        spot_exchange = Exchange.BSE.value
                        option_exchange = Exchange.BFO.value
                
                # Store in our dictionary with unified field names
                self.underlyings[symbol] = {
                    'symbol': symbol,
                    'exchange': spot_exchange,
                    'derivative_exchange': option_exchange,
                    # Copy any other fields from the original config
                    **{k: v for k, v in underlying_config.items() 
                       if k not in ['name', 'symbol', 'spot_exchange', 'exchange', 'option_exchange', 'derivative_exchange']}
                }
        else:
            # If it's already a dictionary (old format)
            self.underlyings = underlyings_config
            
        # Initialize strike intervals
        self.strike_intervals = {}
        for symbol, details in self.underlyings.items():
            self.strike_intervals[symbol] = details.get('strike_interval', 50)
            
            # Make sure exchange is set properly
            if symbol in NSE_INDICES:
                self.underlyings[symbol]['exchange'] = Exchange.NSE.value
                self.underlyings[symbol]['derivative_exchange'] = Exchange.NFO.value
            elif symbol in BSE_INDICES:
                self.underlyings[symbol]['exchange'] = Exchange.BSE.value
                self.underlyings[symbol]['derivative_exchange'] = Exchange.BFO.value
            
        # Track subscribed underlyings
        self._subscribed_underlyings: Set[str] = set()
       
        # Commented out for now as we are using the strategy manager to receive market data events
        # self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data)
        
        self.logger.info("Option Manager initialized")
    
    def configure_underlyings(self, underlyings_config: List[Dict[str, Any]]):
        """
        Configure underlyings from the provided configuration.
        
        Args:
            underlyings_config: List of underlying configurations from the market.underlyings section of config.yaml
                               Can be in old format with 'symbol' key or new format with 'name' key
        """
        with self.lock:
            for underlying_config in underlyings_config:
                # Extract symbol/name
                symbol = underlying_config.get('name', underlying_config.get('symbol'))
                if not symbol:
                    self.logger.warning(f"Skipping underlying config without name/symbol: {underlying_config}")
                    continue
                
                strike_interval = underlying_config.get('strike_interval', 50)
                atm_range = underlying_config.get('atm_range', 5)

                # Extract exchanges with support for both old and new format
                spot_exchange = underlying_config.get('spot_exchange', underlying_config.get('exchange'))
                option_exchange = underlying_config.get('option_exchange', underlying_config.get('derivative_exchange'))
                
                # Set default exchanges if not provided
                if not spot_exchange or not option_exchange:
                    if symbol in NSE_INDICES:
                        spot_exchange = Exchange.NSE.value
                        option_exchange = Exchange.NFO.value
                    elif symbol in BSE_INDICES:
                        spot_exchange = Exchange.BSE.value
                        option_exchange = Exchange.BFO.value

                self.strike_intervals[symbol] = strike_interval
                self.atm_ranges[symbol] = atm_range

                # Ensure all index parameters are stored in the underlyings dict
                if symbol not in self.underlyings:
                    self.underlyings[symbol] = {}
                
                # Store with unified field names
                self.underlyings[symbol].update({
                    'symbol': symbol,
                    'strike_interval': strike_interval,
                    'atm_range': atm_range,
                    'exchange': spot_exchange,
                    'derivative_exchange': option_exchange
                })
                
                self.logger.info(f"Configured underlying {symbol} with strike interval {strike_interval}, ATM range {atm_range}, exchange {spot_exchange}, and derivative exchange {option_exchange}")
    
    def subscribe_underlying(self, underlying: str) -> bool:
        """
        Subscribe to an underlying for market data.
        
        Args:
            underlying: Underlying symbol to subscribe to
            
        Returns:
            bool: True if subscription successful, False otherwise
        """
        if underlying in self._subscribed_underlyings:
            self.logger.info(f"Already subscribed to underlying {underlying}")
            return True
            
        try:
            # Get underlying configuration
            underlying_config = self.underlyings.get(underlying)
            if not underlying_config:
                self.logger.error(f"No configuration found for underlying {underlying}")
                return False
                
            # Get exchange from config or use default
            exchange = underlying_config.get('exchange', Exchange.NSE.value)
            if isinstance(exchange, str):
                exchange = Exchange[exchange]
            
            # Create instrument for the underlying with all required fields
            instrument = Instrument(
                symbol=underlying,
                exchange=exchange,
                instrument_type=InstrumentType.INDEX,
                asset_class=AssetClass.INDEX,
                instrument_id=f"{exchange}:{underlying}"  # Create a unique ID
            )
            
            # Subscribe to market data using the instrument
            if self.data_manager.subscribe(instrument):
                self._subscribed_underlyings.add(underlying)
                self.logger.info(f"Successfully subscribed to underlying {underlying}")
                return True
                
            self.logger.error(f"Failed to subscribe to underlying {underlying}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error subscribing to underlying {underlying}: {str(e)}")
            return False
            
    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events for subscribed underlyings.
        
        Args:
            event: Market data event
        """
        if not isinstance(event.instrument, Instrument):
            self.logger.error("Received market data event with invalid instrument")
            return

        self.logger.debug(f"Received MarketDataEvent for {event}")

        symbol = event.instrument.symbol
        
        if symbol in self._subscribed_underlyings:
            # Process underlying market data
            self._process_underlying_data(symbol, event.data)
            
    def _process_underlying_data(self, symbol: str, data: Dict):
        """
        Process market data for an underlying.
        
        Args:
            symbol: Underlying symbol
            data: Market data dictionary
        """
        # Update underlying price and calculate ATM strike
        price = data.get('CLOSE') or data.get('LAST_PRICE')
        if price:
            self._update_atm_strike(symbol, price)
            
    def _update_atm_strike(self, symbol: str, price: float):
        """
        Update ATM strike for an underlying.
        
        Args:
            symbol: underlying symbol
            price: current underlying price
        """
        strike_interval = self.strike_intervals.get(symbol, 50)
        atm_strike = round(price / strike_interval) * strike_interval
        self.logger.debug(f"Updated ATM strike for {symbol}: {atm_strike}")
        
    def get_atm_strike(self, underlying: str) -> Optional[float]:
        """
        Get the current ATM strike for an underlying.
        
        Args:
            underlying: Underlying symbol
            
        Returns:
            Current ATM strike if available, None otherwise
        """
        # Implementation depends on how ATM strikes are stored
        pass
        
    def get_option_symbol(self, underlying: str, strike: float, option_type: str, 
                         expiry_offset: int = 0) -> Optional[str]:
        """
        Get option symbol for given parameters.
        
        Args:
            underlying: Underlying symbol
            strike: Strike price
            option_type: 'CE' or 'PE'
            expiry_offset: Number of expiries from current (0=current)
            
        Returns:
            Option symbol if available, None otherwise
        """
        # Implementation depends on option symbol format
        pass
        
    def get_option_price(self, symbol: str) -> Optional[float]:
        """
        Get current price for an option.
        
        Args:
            symbol: Option symbol
            
        Returns:
            Current price if available, None otherwise
        """
        # Implementation depends on how option prices are stored
        pass
    
    def _get_atm_strike(self, price: float, strike_interval: float) -> float:
        """
        Calculate the At-The-Money (ATM) strike price based on the underlying price and strike interval.
        
        Args:
            price: Current underlying price
            strike_interval: Strike price interval for the underlying
            
        Returns:
            float: The ATM strike price
        """
        return round(price / strike_interval) * strike_interval
    
    def _get_strike_range(self, atm_strike: float, strike_interval: float, range_count: int) -> List[float]:
        """
        Get a range of strikes around the ATM strike.
        
        Args:
            atm_strike: The ATM strike price
            strike_interval: Strike price interval for the underlying
            range_count: Number of strikes to include on each side of ATM
            
        Returns:
            List[float]: List of strike prices
        """
        strikes = []
        for i in range(-range_count, range_count + 1):
            strike = atm_strike + (i * strike_interval)
            strikes.append(strike)
        return strikes
    
    def _get_option_symbol(self, underlying: str, expiry_date: date, strike: float, option_type: str) -> str:
        """       
        Args:
            underlying: The underlying symbol
            expiry_date: Expiry date of the option
            strike: Strike price
            option_type: Option type ('CE' or 'PE')
            
        Returns:
            str: The constructed option symbol
        """
        # Get the correct underlying symbol
        underlying_symbol = self.underlying_mapping.get(underlying, underlying)

        # Determine if this is an underlying or a stock
        # First check if it's in our known underlyings list
        # Then check if it's in NSE_INDICES or BSE_INDICES from constants
        is_underlying = (underlying in NSE_INDICES or 
                        underlying in BSE_INDICES)
        
        # Set the instrument type based on whether it's an underlying or stock
        instrument_type = "OPT" if is_underlying else "FUT"
        
        # Get derivative exchange from underlyings config
        derivative_exchange = self.underlyings[underlying].get('derivative_exchange', Exchange.NFO.value)
        
        if hasattr(self.data_manager, 'market_data_feed') and self.data_manager.market_data_feed:
            return self.data_manager.market_data_feed.construct_trading_symbol(
                symbol=str(underlying_symbol),
                expiry_date=expiry_date,
                instrument_type=instrument_type,
                option_type=option_type,
                strike=strike,
                exchange=str(derivative_exchange)
            )
        else:
            # Fallback to old format if market_data_feed not available
            expiry_str = expiry_date.strftime("%d%b%y").upper()
            return f"{underlying}{expiry_str}{strike}{option_type}"
    
    def _unsubscribe_all_underlying_options(self, underlying: str) -> None:
        """
        Unsubscribe from all options of a given underlying.
        
        Args:
            underlying: The underlying symbol
        """
        if underlying not in self.option_subscriptions:
            return
            
        option_symbols = self.option_subscriptions[underlying].copy()
        for option_symbol in option_symbols:
            self.data_manager.unsubscribe(option_symbol)
            
        self.option_subscriptions[underlying].clear()
        self.logger.info(f"Unsubscribed from all options for {underlying}")
    
    def subscribe_atm_options(self, underlying_symbol: str, expiry_offset: int = 0) -> List[str]:
        """
        Subscribe to ATM options for an underlying.
        
        Args:
            underlying_symbol: The underlying symbol
            expiry_offset: 0 for current expiry, 1 for next expiry, etc.
            
        Returns:
            List[str]: List of subscribed option symbols
        """
        with self.lock:
            if underlying_symbol not in self.underlying_prices:
                self.logger.warning(f"Cannot subscribe to ATM options for {underlying_symbol}: price not available")
                return []
                
            if underlying_symbol not in self.strike_intervals:
                self.logger.warning(f"Strike interval not configured for {underlying_symbol}")
                return []
            
            # Check if data_manager and market_data_feed are available
            if not hasattr(self.data_manager, 'market_data_feed') or not self.data_manager.market_data_feed:
                self.logger.error(f"Cannot subscribe to ATM options: market data feed not available")
                return []
            
            # Get the market data feed
            market_data_feed = self.data_manager.market_data_feed
                
            current_price = self.underlying_prices[underlying_symbol]
            strike_interval = self.strike_intervals[underlying_symbol]
            atm_range = self.atm_ranges.get(underlying_symbol, 5)
            
            # Calculate ATM strike
            atm_strike = self._get_atm_strike(current_price, strike_interval)
            self.current_atm_strikes[underlying_symbol] = atm_strike
            
            underlying = self.underlying_mapping.get(underlying_symbol, underlying_symbol)            
            expiry_dates = market_data_feed.get_expiry_dates(underlying)
            if not expiry_dates:
                self.logger.error(f"No expiry dates found for {underlying_symbol}")
                return []
            
            self.logger.debug(f"Expiry dates for {underlying_symbol}: {expiry_dates}")

            # Select the appropriate expiry based on offset
            if expiry_offset >= len(expiry_dates):
                self.logger.warning(f"Expiry offset {expiry_offset} out of range for {underlying_symbol}, using last available expiry")
                expiry_date = expiry_dates[-1]
            else:
                expiry_date = expiry_dates[expiry_offset]
            
            # Get range of strikes around ATM
            strikes = self._get_strike_range(atm_strike, strike_interval, atm_range)
            
            # Create or get the set of option subscriptions for this underlying
            if underlying_symbol not in self.option_subscriptions:
                self.option_subscriptions[underlying_symbol] = set()
            
            # Prepare option instruments to subscribe
            option_instruments = []
            for strike in strikes:
                self.logger.debug(f"Subscribing to {underlying_symbol} {strike} {expiry_date}")
                for option_type in ['CE', 'PE']:  # Call and Put options
                    # Get the broker-specific option symbol
                    option_symbol = self._get_option_symbol(
                        underlying=underlying_symbol,
                        expiry_date=expiry_date,
                        strike=strike,
                        option_type=option_type
                    )

                    self.logger.debug(f"Option symbol: {option_symbol}")                    
                    if not option_symbol:
                        self.logger.warning(f"Could not get symbol for {underlying_symbol} {strike} {option_type} {expiry_date}")
                        continue
                    
                    # Only add if not already subscribed
                    if option_symbol not in self.option_subscriptions[underlying_symbol]:
                        derivative_exchange = self.underlyings[underlying_symbol].get('derivative_exchange', Exchange.NFO.value)
                        exchange_enum = derivative_exchange if isinstance(derivative_exchange, Exchange) else Exchange[derivative_exchange]
                        instrument = Instrument(
                            symbol=option_symbol,
                            exchange=exchange_enum,
                            asset_class=AssetClass.OPTIONS,
                            instrument_type=InstrumentType.OPTION,
                            strike=strike,
                            option_type=option_type
                        )
                        option_instruments.append(instrument)
            
            # Subscribe to all new option instruments at once
            if option_instruments:
                subscription_results = market_data_feed.subscribe_symbols(option_instruments)
                
                # Add successfully subscribed symbols to our tracking set
                for instrument, success in zip(option_instruments, subscription_results.values()):
                    if success:
                        self.option_subscriptions[underlying_symbol].add(instrument.symbol)
                
                # Count the number of successful subscriptions
                subscribed_count = sum(1 for success in subscription_results.values() if success)
                self.logger.info(f"Subscribed to {subscribed_count}/{len(option_instruments)} ATM options for {underlying_symbol}")
                
                # Return the list of successfully subscribed symbols
                return [instrument.symbol for instrument, success in 
                       zip(option_instruments, subscription_results.values()) 
                       if success]
            else:
                self.logger.info(f"No new ATM options to subscribe for {underlying_symbol}")
                return []
    
    def update_underlying_price(self, underlying_symbol: str, price: float) -> None:
        """
        Update the cached underlying price and check if ATM strikes need to be updated.
        
        Args:
            underlying_symbol: The underlying symbol
            price: The current price of the underlying
        """
        with self.lock:
            old_price = self.underlying_prices.get(underlying_symbol)
            self.underlying_prices[underlying_symbol] = price
            
            # Skip if no previous price or strike interval not configured
            if underlying_symbol not in self.strike_intervals:
                self.logger.warning(f"Cannot update ATM for {underlying_symbol}: strike interval not configured")
                return
                
            strike_interval = self.strike_intervals[underlying_symbol]
            
            # Calculate new ATM strike
            new_atm = self._get_atm_strike(price, strike_interval)
            
            # Get old ATM from storage
            old_atm = self.current_atm_strikes.get(underlying_symbol)
            
            # If this is the first price update, just set the ATM strike and try to subscribe
            if old_price is None:
                self.logger.info(f"First price received for {underlying_symbol}: {price}, ATM strike: {new_atm}")
                self.current_atm_strikes[underlying_symbol] = new_atm
                
                # Initial subscription to ATM options
                self.subscribe_atm_options(underlying_symbol)
                return
                
            # If ATM strike has changed, update option subscriptions
            if old_atm is None or old_atm != new_atm:
                self.logger.info(f"ATM strike for {underlying_symbol} changed from {old_atm} to {new_atm}")
                
                # Update the stored ATM strike
                self.current_atm_strikes[underlying_symbol] = new_atm
                
                # Update option subscriptions based on new ATM strike
                self.update_atm_options(underlying_symbol)
    
    def update_atm_options(self, underlying_symbol: str) -> None:
        """
        Update option subscriptions for an underlying based on the current price.
        Unsubscribes from out-of-range options and subscribes to new in-range options.
        
        Args:
            underlying_symbol: The underlying symbol
        """
        with self.lock:
            if underlying_symbol not in self.underlying_prices:
                self.logger.warning(f"Cannot update ATM options for {underlying_symbol}: price not available")
                return
                
            if underlying_symbol not in self.current_atm_strikes:
                self.logger.warning(f"Cannot update ATM options: ATM strike not set for {underlying_symbol}")
                return
                
            # Check if data_manager and market_data_feed are available
            if not hasattr(self.data_manager, 'market_data_feed') or not self.data_manager.market_data_feed:
                self.logger.error(f"Cannot update ATM options: market data feed not available")
                return
                
            # Get the market data feed
            market_data_feed = self.data_manager.market_data_feed
                
            current_price = self.underlying_prices[underlying_symbol]
            strike_interval = self.strike_intervals[underlying_symbol]
            atm_range = self.atm_ranges.get(underlying_symbol, 5)
            
            # Get current ATM strike (already calculated in update_underlying_price)
            new_atm_strike = self.current_atm_strikes[underlying_symbol]
            
            # Get expiry dates
            underlying = self.underlying_mapping.get(underlying_symbol, underlying_symbol)
            expiry_dates = market_data_feed.get_expiry_dates(underlying)
            if not expiry_dates:
                self.logger.error(f"No expiry dates found for {underlying_symbol}")
                return
                
            # Get current expiry (first one in the list)
            current_expiry = expiry_dates[0]
            
            # Get new range of strikes
            new_strikes = set(self._get_strike_range(new_atm_strike, strike_interval, atm_range))
            
            # Skip if we don't have any option subscriptions for this underlying yet
            if underlying_symbol not in self.option_subscriptions:
                self.logger.info(f"No option subscriptions for {underlying_symbol} yet, subscribing to initial ATM options")
                self.subscribe_atm_options(underlying_symbol)
                return
                
            # Analyze current subscriptions to determine which ones to keep and which to unsubscribe
            current_subscribed = self.option_subscriptions[underlying_symbol].copy()
            
            # Prepare option symbols to subscribe and unsubscribe
            option_instruments_to_add = []
            symbols_to_unsubscribe = []

            is_underlying = (underlying in NSE_INDICES or 
                            underlying in BSE_INDICES)
        
            # Set the instrument type based on whether it's an underlying or stock
            instrument_type = InstrumentType.OPTION.value if is_underlying else InstrumentType.FUTURE.value
            
            # Get derivative exchange from underlyings config
            derivative_exchange = self.underlyings[underlying_symbol].get('derivative_exchange', Exchange.NFO.value)

            # First, gather all possible symbols for the new ATM range to detect what's needed
            potential_symbols = []
            for strike in new_strikes:
                for option_type in ['CE', 'PE']:  # Call and Put options
                    # Get the broker-specific option symbol
                    
                    option_symbol = market_data_feed.construct_trading_symbol(
                        index_symbol=str(underlying_symbol),
                        expiry_date=current_expiry,
                        instrument_type=instrument_type,
                        strike=strike,
                        option_type=option_type,
                        exchange=str(derivative_exchange)
                    )
                    
                    if option_symbol:
                        potential_symbols.append(option_symbol)
                        
                        # Create Instrument object if we need to subscribe to it
                        if option_symbol not in current_subscribed:
                            derivative_exchange = self.underlyings[underlying_symbol].get('derivative_exchange', Exchange.NFO.value)
                            exchange_enum = derivative_exchange if isinstance(derivative_exchange, Exchange) else Exchange[derivative_exchange]
                            instrument = Instrument(
                                symbol=option_symbol,
                                exchange=exchange_enum,
                                asset_class=AssetClass.OPTIONS,
                                instrument_type=InstrumentType.OPTION,
                                strike=strike,
                                option_type=option_type
                            )
                            option_instruments_to_add.append(instrument)
            
            # Determine which currently subscribed symbols are out of the new ATM range
            # by comparing with potential_symbols (if it's not in potential_symbols,
            # it's out of the new ATM range)
            potential_set = set(potential_symbols)
            for symbol in current_subscribed:
                if symbol not in potential_set:
                    # Check if we have open positions before unsubscribing (IMPORTANT)
                    should_unsubscribe = True
                    
                    
                    # TODO: Check with position_manager if this option has open positions
                    # If hasattr(self, 'position_manager') and self.position_manager:
                    #    if self.position_manager.has_open_position(symbol):
                    #        should_unsubscribe = False
                    #        self.logger.info(f"Keeping subscription to {symbol} because it has open positions")
                    
                    if should_unsubscribe:
                        symbols_to_unsubscribe.append(symbol)
            
            # Unsubscribe from out-of-range options (if no open positions)
            if symbols_to_unsubscribe:
                self.logger.info(f"Unsubscribing from {len(symbols_to_unsubscribe)} out-of-range options for {underlying_symbol}")
                
                for symbol in symbols_to_unsubscribe:
                    try:
                        derivative_exchange = self.underlyings[underlying_symbol].get('derivative_exchange', Exchange.NFO.value)
                        exchange_enum = derivative_exchange if isinstance(derivative_exchange, Exchange) else Exchange[derivative_exchange]
                        instrument = Instrument(
                            symbol=symbol,
                            exchange=exchange_enum,
                            asset_class=AssetClass.OPTIONS,
                            instrument_type=InstrumentType.OPTION
                        )
                        
                        success = self.data_manager.unsubscribe(instrument)
                        if success:
                            self.option_subscriptions[underlying_symbol].remove(symbol)
                            self.logger.debug(f"Unsubscribed from out-of-range option {symbol}")
                        else:
                            self.logger.warning(f"Failed to unsubscribe from option {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error unsubscribing from {symbol}: {e}")
            
            # Subscribe to new option instruments
            if option_instruments_to_add:
                subscription_results = market_data_feed.subscribe_symbols(option_instruments_to_add)
                
                # Add successfully subscribed symbols to our tracking set
                for instrument, success in zip(option_instruments_to_add, subscription_results.values()):
                    if success:
                        self.option_subscriptions[underlying_symbol].add(instrument.symbol)
                
                # Count the number of successful subscriptions
                subscribed_count = sum(1 for success in subscription_results.values() if success)
                unsubscribed_count = len([s for s in symbols_to_unsubscribe if s not in self.option_subscriptions[underlying_symbol]])
                
                self.logger.info(f"Updated ATM options for {underlying_symbol}: Unsubscribed {unsubscribed_count}, Subscribed {subscribed_count} new options")
    
    def get_option_data(self, option_symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest data for an option.
        
        Args:
            option_symbol: The option symbol
            
        Returns:
            Optional[Dict[str, Any]]: The latest option data or None if not available
        """
        return self.option_data_cache.get(option_symbol)
    
    def update_option_data(self, option_symbol: str, data: Dict[str, Any]) -> None:
        """
        Update the cached data for an option.
        
        Args:
            option_symbol: The option symbol
            data: The option data to cache
        """
        self.option_data_cache[option_symbol] = data
    
    def get_active_option_symbols(self, underlying_symbol: str = None) -> List[str]:
        """
        Get the list of currently subscribed option symbols.
        
        Args:
            underlying_symbol: Optional underlying symbol to filter by
            
        Returns:
            List[str]: List of active option symbols
        """
        if underlying_symbol:
            return list(self.option_subscriptions.get(underlying_symbol, set()))
        
        # Return all option symbols across all underlyings
        all_options = []
        for options in self.option_subscriptions.values():
            all_options.extend(options)
        return all_options
    
    def get_atm_strikes(self) -> Dict[str, float]:
        """
        Get the current ATM strikes for all underlyings.

        Returns:
            Dict[str, float]: Dictionary mapping underlying symbols to ATM strikes
        """
        return self.current_atm_strikes.copy()
    
    def handle_missing_data(self, option_symbol: str, required_data_points: int) -> bool:
        """
        Check if an option has enough historical data points.
        
        Args:
            option_symbol: The option symbol
            required_data_points: Number of data points required
            
        Returns:
            bool: True if enough data is available, False otherwise
        """
        # This would typically check if there are enough candles/ticks for the option
        # For now, we'll just return a simple check based on whether the option is in our cache
        return option_symbol in self.option_data_cache
    
    def is_option_liquid(self, option_symbol: str, min_volume: int = 100, max_spread_pct: float = 1.0) -> bool:
        """
        Check if an option is liquid enough for trading.
        
        Args:
            option_symbol: The option symbol
            min_volume: Minimum volume required
            max_spread_pct: Maximum bid-ask spread as percentage
            
        Returns:
            bool: True if the option is considered liquid, False otherwise
        """
        option_data = self.option_data_cache.get(option_symbol)
        
        if not option_data:
            return False
            
        volume = option_data.get('volume', 0)
        bid = option_data.get('bid', 0)
        ask = option_data.get('ask', 0)
        
        if volume < min_volume:
            return False
            
        if bid <= 0 or ask <= 0:
            return False
            
        spread_pct = (ask - bid) / ((ask + bid) / 2) * 100
        if spread_pct > max_spread_pct:
            return False
            
        return True
    
    def start_tracking(self) -> bool:
        """
        Start tracking configured underlyings and their ATM options.
        This should be called after configuration to begin monitoring.
        
        Returns:
            bool: True if tracking started successfully
        """
        try:
            self.logger.info("Starting underlying and option tracking")
            
            # Subscribe to all configured underlyings
            for underlying_symbol in self.strike_intervals.keys():
                if not self.subscribe_underlying(underlying_symbol):
                    self.logger.error(f"Failed to subscribe to underlying {underlying_symbol}")
                    continue
                    
                # Subscribe to initial ATM options
                self.subscribe_atm_options(underlying_symbol)
                
            self.logger.info("Underlying and option tracking started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting underlying tracking: {e}")
            return False 

