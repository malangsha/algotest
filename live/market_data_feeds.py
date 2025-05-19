import threading
import time
import logging
import json
import random
import math
import websocket
import requests
import uuid
import os
import csv
import pyotp
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional, Callable, Union, Tuple, Set
from abc import ABC, abstractmethod
from enum import Enum
from NorenRestApiPy.NorenApi import NorenApi

from models.instrument import Instrument, AssetClass
from utils.constants import MarketDataType, EventType, InstrumentType, OptionType, Exchange
from models.events import MarketDataEvent, OrderEvent
from utils.exceptions import MarketDataException
from core.event_manager import EventManager
from core.logging_manager import get_logger
from live.feeds.finvasia.symbol_cache import SymbolCache

class MarketDataFeedBase(ABC):
    """Base abstract class for market data feeds."""
    def __init__(self, instruments: List[Instrument], callback: Callable):
        self.logger = get_logger("live.market_data_feed_base")
        self.instruments = instruments if instruments else []
        # Store instruments in a way that allows quick lookup by symbol/exchange
        self._instrument_lookup: Dict[Tuple[str, str], Instrument] = {}
        if instruments:
             for inst in instruments:
                  if inst.symbol and inst.exchange:
                       self._instrument_lookup[(inst.symbol, inst.exchange)] = inst

        self.callback = callback
        self.logger = get_logger(self.__class__.__name__)
        self.is_running = False

    def get_instrument(self, symbol: str, exchange: str) -> Optional[Instrument]:
         """ Retrieve an instrument object by symbol and exchange """
         return self._instrument_lookup.get((symbol, exchange))

    def add_instrument(self, instrument: Instrument):
         """ Add an instrument to the feed's tracking list """
         if not isinstance(instrument, Instrument):
              self.logger.error("Attempted to add invalid instrument object.")
              return
         if instrument.symbol and instrument.exchange:
              key = (instrument.symbol, instrument.exchange)
              if key not in self._instrument_lookup:
                   self.instruments.append(instrument)
                   self._instrument_lookup[key] = instrument
                   # self.logger.info(f"Added instrument {key} to feed tracking.")
              else:
                   # Optionally update the existing instrument object
                   # self._instrument_lookup[key] = instrument
                   self.logger.debug(f"Instrument {key} already tracked.")
         else:
              self.logger.warning(f"Cannot add instrument without symbol and exchange: {instrument}")


    def remove_instrument(self, instrument: Instrument):
         """ Remove an instrument from the feed's tracking list """
         if not isinstance(instrument, Instrument):
              self.logger.error("Attempted to remove invalid instrument object.")
              return
         if instrument.symbol and instrument.exchange:
              key = (instrument.symbol, instrument.exchange)
              if key in self._instrument_lookup:
                   del self._instrument_lookup[key]
                   # Also remove from the list
                   self.instruments = [inst for inst in self.instruments if inst != instrument]
                   self.logger.info(f"Removed instrument {key} from feed tracking.")
              else:
                   self.logger.debug(f"Instrument {key} not found for removal.")
         else:
              self.logger.warning(f"Cannot remove instrument without symbol and exchange: {instrument}")


    @abstractmethod
    def start(self): pass
    @abstractmethod
    def stop(self): pass
    @abstractmethod
    def subscribe(self, instrument: Instrument): pass
    @abstractmethod
    def unsubscribe(self, instrument: Instrument): pass

class SimulatedFeed(MarketDataFeedBase):
    """Simulated market data feed for testing and development."""

    def __init__(self, instruments: List[Instrument] = None, callback: Callable = None, **kwargs):
        """
        Initialize the simulated feed.

        Args:
            instruments: List of instruments to simulate
            callback: Callback function for market data events
            **kwargs: Additional configuration parameters
        """
        super().__init__(instruments, callback)
        self.logger = get_logger("live.simulated_feed")
        
        # Initialize price tracking
        self.prices = {}
        self.underlying_prices = {}
        self.underlying_volatilities = {}
        self.last_ohlc_minute_map = {}
        
        # Initialize initial prices from settings
        self.initial_prices = kwargs.get('initial_prices', {})
        
        # Initialize price data for each instrument
        for instrument in instruments:
            symbol = instrument.symbol
            initial_price = self.initial_prices.get(symbol, random.uniform(10, 1000))
            self.underlying_prices[symbol] = initial_price
            self.underlying_volatilities[symbol] = random.uniform(0.001, 0.05)
            
            # Initialize price data structure
            self.prices[symbol] = {
                'current': initial_price,
                'previous': initial_price,
                'bid': initial_price * 0.999,
                'ask': initial_price * 1.001,
                'volume': 0,
                'tick_volume': 0,
                'open': initial_price,
                'high': initial_price,
                'low': initial_price,
                'close': initial_price,
                'timestamp': time.time()
            }
        
        # Simulation parameters
        self.tick_interval = kwargs.get('tick_interval', 1.0)
        self.volatility = kwargs.get('volatility', 0.02)
        self.price_jump_probability = kwargs.get('price_jump_probability', 0.01)
        self.max_price_jump = kwargs.get('max_price_jump', 0.05)
        
        # Simulation state
        self.is_running = False
        self.simulation_thread = None

    def start(self):
        """Start the simulated feed."""
        if self.is_running:
            self.logger.warning("Simulated feed is already running")
            return

        self.is_running = True
        self.simulation_thread = threading.Thread(target=self._simulation_loop)
        self.simulation_thread.daemon = True
        self.simulation_thread.start()
        self.logger.info("Simulated feed started")

    def stop(self):
        """Stop the simulated feed."""
        if not self.is_running:
            self.logger.warning("Simulated feed is not running")
            return

        self.is_running = False
        if self.simulation_thread and self.simulation_thread.is_alive():
            self.simulation_thread.join(timeout=2.0)
            if self.simulation_thread.is_alive():
                self.simulation_thread._stop()
                self.simulation_thread = None

        # Clear all data structures
        self.prices.clear()
        self.underlying_prices.clear()
        self.underlying_volatilities.clear()
        self.last_ohlc_minute_map.clear()
        self.logger.info("Simulated feed stopped")

    def _simulation_loop(self):
        """Main simulation loop that periodically updates prices and sends events."""
        while self.is_running:
            try:
                time.sleep(self.tick_interval)
                if not self.is_running:  # Check again after sleep
                    break
                self._update_underlying_prices()
                if not self.is_running:  # Check after price update
                    break
                self._send_market_data_events()
            except Exception as e:
                self.logger.error(f"Error in simulation loop: {str(e)}")
                break  # Exit loop on error

    def _update_underlying_prices(self):
        """Update prices for all underlying assets."""
        for symbol, price in self.underlying_prices.items():
            try:
                base_volatility = self.underlying_volatilities.get(symbol, self.volatility)
                
                # Regular price movement - random walk with drift
                volatility = base_volatility * price
                drift = 0.0001 * price
                price_change = random.normalvariate(drift, volatility)
                
                # Apply price change
                new_price = max(0.01, price + price_change)
                
                # Update prices dictionary
                self.prices[symbol]['previous'] = price
                self.prices[symbol]['current'] = new_price
                
                # Update bid/ask
                spread = new_price * 0.001
                self.prices[symbol]['bid'] = new_price - spread
                self.prices[symbol]['ask'] = new_price + spread
                
                # Update volume
                vol_change = abs(price_change) / price
                self.prices[symbol]['volume'] += int(random.normalvariate(1000, 500) * (1 + 10 * vol_change))
                self.prices[symbol]['tick_volume'] = int(random.normalvariate(10, 5) * (1 + 10 * vol_change))
                
                # Update OHLC
                current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
                if symbol not in self.last_ohlc_minute_map or self.last_ohlc_minute_map[symbol] != current_minute:
                    self.last_ohlc_minute_map[symbol] = current_minute
                    self.prices[symbol]['open'] = new_price
                    self.prices[symbol]['high'] = new_price
                    self.prices[symbol]['low'] = new_price
                else:
                    self.prices[symbol]['high'] = max(self.prices[symbol]['high'], new_price)
                    self.prices[symbol]['low'] = min(self.prices[symbol]['low'], new_price)
                
                self.prices[symbol]['close'] = new_price
                self.underlying_prices[symbol] = new_price
                
            except Exception as e:
                self.logger.error(f"Error updating prices for {symbol}: {str(e)}")
                continue

    def _send_market_data_events(self):
        """Send market data events for all subscribed instruments."""
        for instrument in self.instruments:
            symbol = instrument.symbol
            if symbol not in self.prices:
                continue
                
            price_data = self.prices[symbol]
            market_data = {
                'symbol': symbol,
                'exchange': instrument.exchange,
                'last_price': price_data['current'],
                'bid': price_data['bid'],
                'ask': price_data['ask'],
                'volume': price_data['volume'],
                'timestamp': int(time.time() * 1000)
            }
            
            event = MarketDataEvent(
                event_type=EventType.MARKET_DATA,
                timestamp=int(time.time() * 1000),
                instrument=instrument,
                data=market_data,
                data_type=MarketDataType.TICK
            )
            
            if self.callback:
                try:
                    self.callback(event)
                except Exception as e:
                     self.logger.error(f"Error in market data callback for {symbol}: {e}", exc_info=True)

    def subscribe(self, instrument: Instrument):
        """Subscribe to market data for an instrument."""
        if instrument not in self.instruments:
            self.instruments.append(instrument)
            symbol = instrument.symbol
            initial_price = self.initial_prices.get(symbol, 100.0)
            
            self.prices[symbol] = {
                'current': initial_price,
                'previous': initial_price,
                'bid': initial_price * 0.999,
                'ask': initial_price * 1.001,
                'volume': 0,
                'tick_volume': 0,
                'open': initial_price,
                'high': initial_price,
                'low': initial_price,
                'close': initial_price,
                'timestamp': time.time()
            }
            
            self.underlying_prices[symbol] = initial_price
            self.underlying_volatilities[symbol] = self.volatility
            self.logger.info(f"Added {symbol} to simulation")

    def unsubscribe(self, instrument: Instrument):
        """Unsubscribe from an instrument."""
        if instrument in self.instruments:
            self.instruments.remove(instrument)
            symbol = instrument.symbol
            
            # Clean up all price data for this instrument
            if symbol in self.prices:
                del self.prices[symbol]
            if symbol in self.underlying_prices:
                del self.underlying_prices[symbol]
            if symbol in self.underlying_volatilities:
                del self.underlying_volatilities[symbol]
            if symbol in self.last_ohlc_minute_map:
                del self.last_ohlc_minute_map[symbol]
                
            self.logger.info(f"Unsubscribed from instrument: {symbol}")

    def generate_crossover_scenario(self, symbol: str, duration: int = 100, crossover_at: int = 50, trend_strength: float = 0.01):
        """
        Generate a specific price scenario to cause a moving average crossover.
        
        Args:
            symbol: The symbol to generate scenario for
            duration: Total number of data points to generate
            crossover_at: At which point the trend should change (index)
            trend_strength: How strong the trend should be (percentage change per tick)
        """
        if symbol not in self.prices:
            self.logger.warning(f"Symbol {symbol} not found in prices")
            return
            
        # Get current price
        current_price = self.prices[symbol]['current']
        
        # Simulate a trend reversal that will cause a MA crossover
        # First downtrend, then uptrend
        prices = []
        
        # Initialize with current price
        price = current_price
        
        # Generate scenario
        for i in range(duration):
            # Before crossover point - downtrend
            if i < crossover_at:
                # Downtrend with some noise
                change = -trend_strength * price * (1 + 0.5 * random.normalvariate(0, 1))
            else:
                # After crossover point - uptrend
                change = trend_strength * price * (1 + 0.5 * random.normalvariate(0, 1))
                
            # Apply change
            price = max(0.01, price + change)
            prices.append(price)
            
            # Update price data
            self.prices[symbol]['previous'] = self.prices[symbol]['current']
            self.prices[symbol]['current'] = price
            
            # Update bid/ask
            spread = price * 0.001  # 0.1% spread
            self.prices[symbol]['bid'] = price - spread
            self.prices[symbol]['ask'] = price + spread
            
            # Update OHLC
            # If this is the first price in a series, it's the open
            if i == 0 or (i % 5 == 0):  # Create a new bar every 5 ticks for faster MA calculation
                self.prices[symbol]['open'] = price
                self.prices[symbol]['high'] = price
                self.prices[symbol]['low'] = price
            else:
                # Update high and low
                self.prices[symbol]['high'] = max(self.prices[symbol]['high'], price)
                self.prices[symbol]['low'] = min(self.prices[symbol]['low'], price)
            
            # Always update close
            self.prices[symbol]['close'] = price
            
            # Update volume
            self.prices[symbol]['volume'] += int(random.normalvariate(1000, 500))
            self.prices[symbol]['tick_volume'] = int(random.normalvariate(10, 5))
            
            # Update timestamp
            self.prices[symbol]['timestamp'] = time.time()
            
            # Send a market data event for this update
            for instrument in self.instruments:
                if instrument.symbol == symbol:
                    self._send_market_data_events()
                    time.sleep(self.tick_interval)  # Wait between events
                    break
                    
        self.logger.info(f"Generated crossover scenario for {symbol}: {len(prices)} price points")
        return prices

    # Add a new method that can be called to trigger a specific test scenario
    def trigger_test_scenario(self, scenario_type: str = "moving_average_crossover"):
        """
        Trigger a specific test scenario.
        
        Args:
            scenario_type: Type of scenario to trigger
        """
        if scenario_type == "moving_average_crossover":
            # For each instrument, generate a crossover scenario
            for instrument in self.instruments:
                self.generate_crossover_scenario(instrument.symbol)
                self.logger.info(f"Triggered MA crossover scenario for {instrument.symbol}")
        else:
            self.logger.warning(f"Unknown scenario type: {scenario_type}")

    def _convert_tick_to_market_data(self, tick):
        """
        Convert Finvasia tick format to our standard market data format.
        
        Args:
            tick: Finvasia tick data
            
        Returns:
            dict: Standardized market data
        """
        try:
            market_data = {}
            
            # Extract basic price data
            if 'lp' in tick:
                market_data[MarketDataType.LAST_PRICE.value] = float(tick['lp'])
                
            # Extract bid/ask
            if 'bp' in tick and 'sp' in tick:
                market_data[MarketDataType.BID.value] = float(tick['bp'])
                market_data[MarketDataType.ASK.value] = float(tick['sp'])
                
            # Extract volumes
            if 'v' in tick:
                market_data[MarketDataType.VOLUME.value] = float(tick['v'])
                
            # Extract OHLC if available
            ohlc_data = {}
            if 'o' in tick:
                ohlc_data['open'] = float(tick['o'])
            if 'h' in tick:
                ohlc_data['high'] = float(tick['h'])
            if 'l' in tick:
                ohlc_data['low'] = float(tick['l'])
            if 'c' in tick:
                ohlc_data['close'] = float(tick['c'])
                
            # Only add OHLC if we have at least one value
            if ohlc_data:
                market_data[MarketDataType.OHLC.value] = ohlc_data
                
            # Add timestamp
            market_data[MarketDataType.TIMESTAMP.value] = time.time()
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error converting tick to market data: {str(e)}")
            self.logger.debug(f"Raw tick data: {tick}")
            return None

# --- Updated FinvasiaFeed Class ---
class FinvasiaFeed(MarketDataFeedBase):
    """
    (Integrated) Finvasia WebSocket feed implementation.
    Uses actual Instrument, MarketDataEvent, MarketDataType, EventType definitions.
    """

    def __init__(
        self,
        instruments: List[Instrument],
        callback: Callable[[MarketDataEvent], None],
        user_id: str,
        password: str,
        api_key: str,
        imei: str = None,
        twofa: str = None,
        vc: str = None,
        api_url: str = "https://api.shoonya.com/NorenWClientTP/",
        ws_url: str = "wss://api.shoonya.com/NorenWSTP/",
        debug: bool = False
    ):
        self.logger = get_logger("live.finvasia_feed")
        if debug:
            self.logger.setLevel(logging.DEBUG)

        self._stop_reconnect_event = threading.Event()
        self._reconnect_thread = None
        
        # State tracking
        self.is_connected = False
        self.is_authenticated = False
        self.session_token = None
        
        # Reconnection attributes
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5  # Default, can be overridden by params if needed
        self._reconnect_delay = 5     # Default
        
        # Subscription tracking
        self._subscribed_tokens: set = set()
        self._token_to_symbol: dict = {}
        self._symbol_to_token: dict = {}

        # Thread locks
        self._auth_lock = threading.Lock()
        self._sub_lock = threading.Lock()

        super().__init__(instruments, callback)
        
        self.logger = get_logger("live.finvasia_feed")
        if debug:
            self.logger.setLevel(logging.DEBUG)
            
        # Store credentials
        self.user_id = user_id
        self.password = password
        self.twofa_key = twofa
        self.vendor_code = vc
        self.app_key = api_key
        self.imei = imei
        
        # Initialize API client
        self.api = NorenApi(host=api_url, websocket=ws_url)
        
        # Initialize symbol cache
        self.symbol_cache = SymbolCache()

        self.logger.info("FinvasiaFeed Class initialized")
    
    @property
    def is_running(self):
        """Indicates if the feed is actively connected and authenticated."""
        return self.is_connected and self.is_authenticated
    
    @is_running.setter
    def is_running(self, value: bool):
        """
        Setter for the is_running property.
        Allows control over the feed's state (start/stop) and handles
        assignments from base class during initialization.
        """
        # Logger might not be fully configured if this is called very early from base class,
        # but it should exist due to earlier initialization.
        self.logger.info(f"Attempt to set is_running to: {value}. Current actual state (getter): {self.is_running}")

        if not isinstance(value, bool):
            self.logger.warning(f"is_running can only be set to a boolean value. Received: {value}")
            return

        if value: # Trying to set to True
            if not self.is_running: 
                self.logger.info("is_running being set to True. Calling start() to initiate connection.")            
                self.start() 
            else:
                self.logger.info("is_running is already True (actual state). No action taken by setter.")
        else:             
            if self.is_running:
                self.logger.info("is_running being set to False. Calling stop() to terminate connection.")
                self.stop()
            else:
                self.logger.info("is_running is already False (actual state). No action taken by setter.")

    def start(self):
        """Start the feed and connect to Finvasia."""
        self.logger.info("Starting FinvasiaFeed...")
        self._stop_reconnect_event.clear() # Allow reconnections

        if not self.is_authenticated:
            if not self.authenticate():
                self.logger.error("Failed to authenticate. Cannot start feed.")
                return False # Indicate failure to start
                
        if not self.is_connected:
            if not self.connect(): # connect() will now handle its own logic for connection status
                self.logger.error("Failed to connect to WebSocket during initial start.")
                # _on_close might be triggered by connect() failure, initiating retries if appropriate
                return False # Indicate failure to start

        # If connect was successful, it would have set is_connected and called _on_open.
        # _on_open handles resubscription. If it's an initial start, _subscribed_tokens is empty.
        # For initial subscriptions based on self.instruments:
        if self.is_connected:
            self.logger.info("Initial subscription to provided instruments...")
            # Create a copy for iteration, as self.instruments might be modified by add_instrument
            instruments_to_subscribe = list(self.instruments)
            for instrument in instruments_to_subscribe:
                self.subscribe(instrument) # This uses the updated subscribe method
            self.logger.info("FinvasiaFeed started.")
            return True
        else:
            self.logger.warning("FinvasiaFeed started but initial connection failed. Reconnection might be in progress if enabled.")
            return False # Or True, depending on desired behavior if initial connect fails but retries are active
    
    def stop(self):
        """Stop the feed, disconnect, and prevent further reconnections."""
        self.logger.info("Stopping FinvasiaFeed...")
        self._stop_reconnect_event.set() # Signal any reconnection attempts to stop

        if self._reconnect_thread and self._reconnect_thread.is_alive():
            self.logger.info("Waiting for active reconnect thread to terminate...")
            self._reconnect_thread.join(timeout=self._reconnect_delay + 2.0) # Wait for it to finish or timeout

        self.disconnect() # Call the disconnect method to clean up
        self.logger.info("FinvasiaFeed stopped.")
        return True
    
    def authenticate(self) -> bool:
        """Authenticate with Finvasia API."""
        with self._auth_lock:
            if self.is_authenticated:
                return True
                
            try:
                # Generate TOTP
                twofa_token = pyotp.TOTP(self.twofa_key).now() if self.twofa_key else ""
                
                # Login
                response = self.api.login(
                    userid=self.user_id,
                    password=self.password,
                    twoFA=twofa_token,
                    vendor_code=self.vendor_code,
                    api_secret=self.app_key,
                    imei=self.imei
                )
                
                if isinstance(response, dict) and response.get('stat') == 'Ok':
                    self.session_token = response.get('susertoken')
                    self.is_authenticated = True
                    self.logger.info("Authentication successful")
                    return True
                    
                self.logger.error(f"Authentication failed: {response}")
                return False
                
            except Exception as e:
                self.logger.error(f"Authentication error: {e}")
                return False
    
    def connect(self) -> bool:
        """
        Connect to Finvasia WebSocket feed.
        Ensures previous connection is closed and handles connection timeout.
        """
        if self._stop_reconnect_event.is_set():
            self.logger.info("Connect called but feed is stopping. Aborting connection attempt.")
            return False

        if not self.is_authenticated:
            with self._auth_lock: # Ensure thread-safe authentication check/attempt
                if not self.is_authenticated: # Double check after acquiring lock
                    self.logger.info("Authentication required before connecting. Attempting to authenticate...")
                    if not self.authenticate(): # authenticate() is assumed to be thread-safe or called within lock
                        self.logger.error("Authentication failed. Cannot connect.")
                        return False
        
        self.logger.debug("Attempting to close any existing WebSocket connection before initiating new one.")
        try:
            self.api.close_websocket() # Crucial to prevent "socket already opened"
        except Exception as e:
            self.logger.warning(f"Exception while pre-closing websocket: {e}")
        time.sleep(0.5) # Brief pause for cleanup

        try:
            # Setup WebSocket callbacks
            self.api.on_disconnect = self._on_disconnect
            self.api.on_error = self._on_error
            self.api.on_open = self._on_open
            self.api.on_close = self._on_close # This is important
            self.api.on_ticks = self._on_market_data
            # self.api.on_order_update = self._on_order_update # If you handle order updates

            self.logger.info("Starting WebSocket connection with NorenApi...")
            self.is_connected = False # Reset connection status before attempting
            self.api.start_websocket(
                order_update_callback=self._on_order_update if hasattr(self, '_on_order_update') else None,
                subscribe_callback=self._on_market_data,
                socket_open_callback=self._on_open,
                socket_close_callback=self._on_close
                # error_callback=self._on_error # Check NorenApi documentation if it supports this
            )
            
            # Wait for connection status to be updated by _on_open
            start_time = time.time()
            connection_timeout = 15  # seconds
            self.logger.debug(f"Waiting up to {connection_timeout}s for WebSocket connection to establish...")
            
            while not self.is_connected and (time.time() - start_time) < connection_timeout:
                if self._stop_reconnect_event.is_set():
                    self.logger.info("Connection attempt aborted as feed is stopping.")
                    self.api.close_websocket() # Ensure cleanup
                    return False
                time.sleep(0.1)
                
            if not self.is_connected:
                self.logger.error(f"WebSocket connection timeout after {connection_timeout} seconds. _on_open was not called.")
                self.api.close_websocket() # Attempt to clean up the failed connection
                # _on_close might be called by NorenApi if handshake fails, triggering reconnect.
                # If not, we might want to trigger it manually or call _attempt_reconnect here.
                # For now, rely on NorenApi's _on_close or a subsequent _attempt_reconnect call.
                return False
            
            self.logger.info("WebSocket connection process initiated successfully. _on_open confirmed connection.")
            # _reconnect_attempts are reset in _on_open
            return True

        except Exception as e:
            self.logger.error(f"Exception during WebSocket connection: {e}", exc_info=True)
            self.api.close_websocket() # Ensure cleanup on exception
            self.is_connected = False  # Update state
            # This exception might also lead to _on_close being called by the library.
            # If not, consider calling _attempt_reconnect here after a small delay.
            # self._attempt_reconnect() # Or let _on_close handle it if it's called.
            return False
    
    def subscribe(self, instrument: Instrument): # instrument: Instrument
        """Subscribe to market data for an instrument. Uses full 'exchange|token' string."""
        if not self.is_connected:
            self.logger.warning(f"Cannot subscribe to {instrument.symbol}: WebSocket not connected. Attempting to connect...")
            if not self.connect(): # Try to connect first
                 self.logger.error(f"Failed to connect. Subscription for {instrument.symbol} aborted.")
                 return False              
        
        with self._sub_lock:
            try:
                # Assuming instrument.exchange is an Enum or has a .value attribute for string representation
                exchange_str = instrument.exchange.value if isinstance(instrument.exchange, Exchange) else str(instrument.exchange)                                
                symbol_info = self.symbol_cache.lookup_symbol(instrument.symbol, exchange_str)              
                if not symbol_info or 'token' not in symbol_info:
                    self.logger.warning(f"Symbol not found in cache: {instrument.symbol}, Cannot subscribe")
                    return False
                    
                token = symbol_info['token']
                if token in self._subscribed_tokens:
                    subscription_str = f"{exchange_str}|{token}"
                    self.logger.debug(f"Already subscribed to {subscription_str} ({instrument.symbol}).")
                    return True               
                    
                response = self.api.subscribe([f"{exchange_str}|{token}"])
                # self.logger.debug(f"Subscription response: {response}")                
                # if not isinstance(response, dict) or response.get('stat') != 'Ok':
                #     self.logger.error(f"Subscription failed: {response}")
                #     return False
                    
                # Update tracking
                self._subscribed_tokens.add(token)
                self._token_to_symbol[token] = instrument.symbol
                self._symbol_to_token[instrument.symbol] = token
                
                self.logger.info(f"Subscribed to {instrument.symbol}")
                return True
            
            except Exception as e:
                self.logger.error(f"Exception during subscription to {instrument.symbol}: {e}", exc_info=True)
                return False
    
    def unsubscribe(self, instrument: Instrument):
        """Unsubscribe from market data for an instrument. Uses full 'exchange|token' string."""
        if not self.is_connected:
            self.logger.warning(f"Cannot unsubscribe from {instrument.symbol}: WebSocket not connected.")
            # Unsubscription doesn't strictly need an active connection if we're just updating our state,
            # but the API call would fail.
            return True # Or False, depending on desired strictness
        
        with self._sub_lock:
            try:
                token = self._symbol_to_token.get(instrument.symbol)
                if not token:
                    return True
                    
                # Unsubscribe from symbol
                response = self.api.unsubscribe([f"{instrument.exchange}|{token}"])
                if not isinstance(response, dict) or response.get('stat') != 'Ok':
                    self.logger.error(f"Unsubscription failed: {response}")
                    return False
                    
                # Update tracking
                self._subscribed_tokens.discard(token)
                self._token_to_symbol.pop(token, None)
                self._symbol_to_token.pop(instrument.symbol, None)
                
                self.logger.info(f"Unsubscribed from {instrument.symbol}")
                return True
                
            except Exception as e:
                self.logger.error(f"Unsubscription error: {e}")
                return False
    
    def _on_disconnect(self):
        """Handle WebSocket disconnect callback specifically from NorenApi (if different from _on_close)."""
        self.logger.warning(f"WebSocket _on_disconnect (NorenApi specific) received. Current connected state: {self.is_connected}")

        if self.is_connected: 
            self.logger.warning("WebSocket disconnected unexpectedly (was connected) via _on_disconnect.")
        
        self.is_connected = False

        if self._stop_reconnect_event.is_set():
            self.logger.info("Reconnection suppressed via _on_disconnect as feed is stopping.")
            return

        self._attempt_reconnect()
        
    def _on_error(self, error):
        """Handle WebSocket error."""
        self.logger.error(f"WebSocket error: {error}")
        
    def _on_open(self):
        """Handle WebSocket open event."""
        self.logger.info("WebSocket connection opened successfully.")
        self.is_connected = True
        self._reconnect_attempts = 0 # Reset reconnect counter on successful connection
        self.is_authenticated = True

        # Resubscribe to all tracked instruments
        if self._subscribed_tokens:
            self.logger.info(f"Resubscribing to {len(self._subscribed_tokens)} existing instrument strings after (re)connection...")
            with self._sub_lock:
                tokens_to_resubscribe = list(self._subscribed_tokens)
                if tokens_to_resubscribe:
                    self.logger.debug(f"Attempting to resubscribe to: {tokens_to_resubscribe}")
                    response = self.api.subscribe(tokens_to_resubscribe)
                    self.logger.debug(f"Resubscription API response: {response}")                    
        else:
            self.logger.info("No existing subscriptions to resubscribe to on WebSocket open.")
    
    def _on_close(self, code=None, reason=None):
        """Handle WebSocket close event. This is called by the underlying websocket library."""
        self.logger.warning(f"WebSocket _on_close received. Code: {code}, Reason: '{reason}'. Current connected state: {self.is_connected}")
        
        was_connected = self.is_connected
        self.is_connected = False

        if self._stop_reconnect_event.is_set():
            self.logger.info("Reconnection suppressed as feed is stopping or has been stopped.")
            return        
        
        if was_connected:
            self.logger.warning(f"WebSocket connection lost (was connected). Code: {code}, Reason: '{reason}'")
        else:
            # This could happen if connect() fails and _on_close is called before _on_open.
            self.logger.warning(f"WebSocket failed to establish connection or closed prematurely. Code: {code}, Reason: '{reason}'")
        
        # The 504 Gateway Timeout will trigger this.
        # NorenApi might also trigger close for other reasons.
        self._attempt_reconnect()    
        
    def _attempt_reconnect(self):
        """Manages the logic and scheduling of reconnection attempts."""
        if self._stop_reconnect_event.is_set():
            self.logger.info("Reconnect attempt skipped: feed stopping.")
            return

        if self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            # Exponential backoff, capped at 60 seconds
            delay = min(self._reconnect_delay * (2 ** (self._reconnect_attempts - 1)), 60)
            
            self.logger.info(f"Attempting WebSocket reconnection in {delay:.2f} seconds (attempt {self._reconnect_attempts}/{self._max_reconnect_attempts}).")
            
            # Ensure only one reconnect thread is active
            if self._reconnect_thread and self._reconnect_thread.is_alive():
                self.logger.debug("Previous reconnect thread still alive. Not starting a new one yet.")
                return 

            self._reconnect_thread = threading.Thread(target=self._run_reconnect, args=(delay,))
            self._reconnect_thread.daemon = True # Allow program to exit even if thread is running
            self._reconnect_thread.start()
        else:
            self.logger.error(f"Maximum reconnect attempts ({self._max_reconnect_attempts}) reached. Will not try to reconnect automatically.")
            # Optionally, notify someone or take other actions for permanent failure.

    def _run_reconnect(self, delay: float):
        """Worker method for reconnection, executed in a separate thread."""
        try:
            self.logger.debug(f"Reconnect thread started, sleeping for {delay:.2f} seconds.")
            
            if self._stop_reconnect_event.wait(timeout=delay): 
                self.logger.info("Reconnect cancelled during sleep as feed is stopping.")
                return
                
            if self._stop_reconnect_event.is_set(): 
                self.logger.info("Reconnect cancelled after sleep as feed is stopping.")
                return

            self.logger.info("Executing reconnection...")
            
            # Re-authenticate if not authenticated. Connection might have dropped session.
            # Or, Finvasia might require re-login after certain types of disconnects.
            # This depends on API behavior. For now, assume connect() handles auth if needed.
            # if not self.is_authenticated:
            #    self.logger.info("Authentication lost or not established. Re-authenticating before reconnect.")
            #    if not self.authenticate():
            #        self.logger.error("Re-authentication failed during reconnect sequence.")
            #        # Potentially trigger another _attempt_reconnect or give up
            #        return
            if not self.connect(): 
                self.logger.error("Reconnection attempt failed (connect call returned False).")
                # connect() failure might trigger _on_close, which calls _attempt_reconnect again.
                # This is okay as _reconnect_attempts counter will manage it.
            else:
                # If connect() is successful, _on_open is called, which resets _reconnect_attempts.
                self.logger.info("Reconnection attempt initiated by connect() call. Waiting for _on_open confirmation.")
        except Exception as e:
            self.logger.error(f"Exception in _run_reconnect thread: {e}", exc_info=True)
        finally:
            # Ensure thread object is cleared if it's this thread finishing
            if self._reconnect_thread is threading.current_thread():
                self._reconnect_thread = None

    def _on_market_data(self, ticks):
        """Handle market data ticks."""
        if not isinstance(ticks, list):
            ticks = [ticks]
            
        for tick in ticks:
            try:
                token = tick.get('tk')
                if not token:
                    continue
                    
                # self.logger.debug(f"tick: {tick}")
                symbol = self._token_to_symbol.get(token)
                if not symbol:
                    continue
                    
                # Find the instrument object
                exchange = tick.get('e')
                if isinstance(exchange, str):
                    exchange = Exchange[exchange]

                instrument = self.get_instrument(symbol, exchange)
                if not instrument:
                    self.logger.error(f"Instrument not found for {symbol}")
                    return
                
                market_data = self._convert_tick_to_market_data(tick)             
                if not market_data:
                    return
                
                #self.logger.debug(f"market_data: {market_data}")
                
                # Create market data event
                event = MarketDataEvent(
                    instrument=instrument,
                    data=market_data,
                    timestamp=market_data.get('TIMESTAMP', time.time()),
                    event_type=EventType.MARKET_DATA
                )
                
                # Call the callback
                self.callback(event)
                
            except Exception as e:
                self.logger.error(f"Error processing tick: {e}")
        
    def _on_order_update(self, order_data):
        """Handle order updates."""
        try:
            # Create order event
            event = OrderEvent(
                order_id=order_data.get('norenordno'),
                status=order_data.get('status'),
                data=order_data,
                timestamp=datetime.now()
            )
            
            # Call the callback
            self.callback(event)
            
        except Exception as e:
            self.logger.error(f"Error processing order update: {e}")
    
    def _convert_tick_to_market_data(self, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """ Convert Finvasia tick format to standard dict using MarketDataType enum values. """
        market_data = {}
        try:
            # --- Symbol and Exchange Info ---
            # if 't' in tick: market_data['symbol'] = tick['t']
            if 'e' in tick: market_data['exchange'] = tick['e']
            if 'tk' in tick: market_data['token'] = tick['tk']
            if 'ts' in tick: market_data['symbol'] = tick['ts']

            # --- Price/Quote Data ---
            if 'lp' in tick: market_data[MarketDataType.LAST_PRICE.value] = float(tick['lp'])
            if 'pc' in tick: market_data['percent_change'] = float(tick['pc'])
            if 'ch' in tick: market_data['change'] = float(tick['c'])
            if 'ft' in tick: market_data['feed_time'] = tick['ft']
            
            # Finvasia uses bp1/sp1 for best bid/ask
            if 'bp1' in tick: market_data[MarketDataType.BID.value] = float(tick['bp1'])
            if 'sp1' in tick: market_data[MarketDataType.ASK.value] = float(tick['sp1'])
            if 'bq1' in tick: market_data[MarketDataType.BID_QUANTITY.value] = int(tick['bq1'])
            if 'sq1' in tick: market_data[MarketDataType.ASK_QUANTITY.value] = int(tick['sq1'])           

            # --- Volume/OI Data ---
            if 'v' in tick: market_data[MarketDataType.VOLUME.value] = int(tick['v'])
            if 'oi' in tick: market_data[MarketDataType.OPEN_INTEREST.value] = int(tick['oi'])
            if 'poi' in tick: market_data[MarketDataType.CHANGE_OI.value] = int(tick['poi'])

            # --- OHLC Data ---
            ohlc_data = {}
            if 'o' in tick: ohlc_data[MarketDataType.OPEN.value] = float(tick['o'])
            if 'h' in tick: ohlc_data[MarketDataType.HIGH.value] = float(tick['h'])
            if 'l' in tick: ohlc_data[MarketDataType.LOW.value] = float(tick['l'])
            if 'c' in tick: ohlc_data[MarketDataType.CLOSE.value] = float(tick['c'])
            if ohlc_data: market_data[MarketDataType.OHLC.value] = ohlc_data

            # --- Additional Fields ---
            if 'ap' in tick: market_data['average_price'] = float(tick['ap'])
            if 'pp' in tick: market_data['precision'] = int(tick['pp'])
            if 'ls' in tick: market_data['lot_size'] = int(tick['ls'])
            if 'ti' in tick: market_data['tick_size'] = float(tick['ti'])

            # --- Timestamp ---
            # Prefer feed time 'ft' if available, fallback to 'tk_ts', then system time
            ts_val = tick.get('ft') or tick.get('tk_ts')
            if ts_val:
                try: market_data[MarketDataType.TIMESTAMP.value] = float(ts_val) # Assuming epoch seconds
                except (ValueError, TypeError): market_data[MarketDataType.TIMESTAMP.value] = time.time()
            else: market_data[MarketDataType.TIMESTAMP.value] = time.time()
            
            # Add system timestamp
            market_data['received_timestamp'] = datetime.now().isoformat()

            # --- Validation ---
            has_price = (MarketDataType.LAST_PRICE.value in market_data or
                         (MarketDataType.BID.value in market_data and MarketDataType.ASK.value in market_data) or
                         MarketDataType.OHLC.value in market_data)
            if not has_price:
                 # self.logger.debug(f"Discarding tick - no essential price data found: {tick}")
                 return None

            return market_data

        except (ValueError, TypeError, KeyError) as e:
            self.logger.error(f"Error converting tick: {e}", exc_info=False)
            self.logger.debug(f"Raw tick data causing error: {tick}")
            return None
        except Exception as e:
             self.logger.error(f"Unexpected error converting tick: {e}", exc_info=True)
             self.logger.debug(f"Raw tick data causing error: {tick}")
             return None
    
    def disconnect(self):
        """Gracefully disconnects from Finvasia feed and cleans up resources."""
        self.logger.info("Disconnecting from Finvasia feed...")
        self._stop_reconnect_event.set()

        try:
            # Unsubscribe from all symbols
            if self._subscribed_tokens:
                with self._sub_lock:
                    tokens_to_unsubscribe = list(self._subscribed_tokens)
                    if tokens_to_unsubscribe:
                        self.logger.info(f"Unsubscribing from {len(tokens_to_unsubscribe)} instrument strings during disconnect.")
                        # Check if API allows unsubscribe when not connected, or if it needs connection.
                        # If API requires connection, this might only be effective if called while connected.
                        if self.is_connected: # Only try to tell API if connected
                           self.api.unsubscribe(tokens_to_unsubscribe)
                    # Clear local tracking regardless of API success
                    self._subscribed_tokens.clear()
                    self._token_to_symbol.clear()
                    self._symbol_to_token.clear()
            
            # Stop WebSocket            
            if self.is_connected:
                self.logger.info("Closing WebSocket connection via NorenApi...")
                self.api.close_websocket() # This should handle NorenApi's internal state            
                self.is_connected = False # Explicitly set state

            # Clear authentication
            if self.is_authenticated:
                self.logger.info("Logging out from Finvasia API.")
                try:
                    logout_response = self.api.logout()
                    self.logger.debug(f"Logout API response: {logout_response}")
                except Exception as e:
                    self.logger.error(f"Exception during logout: {e}")
            
            self.is_authenticated = False
            self.session_token = None
                
            self.logger.info("Successfully disconnected and cleaned up Finvasia feed.")
            
        except Exception as e:
            self.logger.error(f"Error during disconnect procedure: {e}", exc_info=True)
        finally:
            # Ensure states are reset
            self.is_connected = False
            self.is_authenticated = False
            self.logger.debug("FinvasiaFeed disconnect final state: is_connected=False, is_authenticated=False")
    
    def __del__(self):
        """Cleanup on object destruction."""
        # Ensure logger exists or use print for __del__ if logger might be gone
        log_func = self.logger.info if hasattr(self, 'logger') and self.logger else print
        log_func("FinvasiaFeed instance being deleted. Ensuring disconnection.")
        
        # Call stop() only if essential attributes for it are present
        # Check for _stop_reconnect_event as a proxy for successful enough __init__
        if hasattr(self, '_stop_reconnect_event'):
            self.stop()
        else:
            log_func("FinvasiaFeed __del__: Bypassing stop() due to likely partial initialization.")
            # Minimal cleanup if stop() cannot be called:
            if hasattr(self, 'api') and self.api:
                try:
                    log_func("FinvasiaFeed __del__: Attempting direct api.close_websocket()")
                    self.api.close_websocket()
                except Exception as e:
                    log_func(f"FinvasiaFeed __del__: Error in direct websocket close: {e}")

class MarketDataFeed:
    """
    Unified interface for different market data feed implementations.
    Supports different broker feeds through a common interface.
    """
    
    def __init__(self, feed_type: str, broker: Any, instruments: List[Instrument] = [], event_manager: EventManager = None, settings: Dict = None):
        """
        Initialize a market data feed of the specified type.

        Args:
            feed_type: Type of feed (websocket, rest, simulated)
            broker: Broker instance
            instruments: List of instruments to subscribe to
            event_manager: EventManager instance
            settings: Dictionary containing feed-specific settings
        """
        self.logger = get_logger("live.market_data_feed")        
        self.feed_type = feed_type.lower()
        self.broker = broker
        self.instruments = instruments
        self.feed = None
        self.event_manager = event_manager
        self.settings = settings if settings is not None else {}
        
        # Initialize subscribed symbols cache
        self._subscribed_symbols = set()
        
        # Register with event manager
        self._register_with_event_manager()

    def _register_with_event_manager(self):
        """Register with event manager to publish market data events."""
        # No need to subscribe to anything here since we're the publisher
        self.logger.info("Registered with Event Manager for publishing market data events")

    def start(self):
        """Start the market data feed."""
        if self.feed:
            self.feed.start()
            return
        
        # Log the instruments we're going to subscribe to
        self.logger.info(f"Starting market data feed for {len(self.instruments)} instruments:")
        for instrument in self.instruments:
            self.logger.info(f"  - {instrument.exchange}:{instrument.symbol}")
        
        if self.feed_type == 'simulated':
            # Get the appropriate parameters for SimulatedFeed
            websocket_url = self.settings.get('websocket_url', '')  
            api_key = self.settings.get('api_key', '')
            client_id = self.settings.get('client_id', '')
            password = self.settings.get('password', '')
            # Pass all settings to SimulatedFeed constructor
            # SimulatedFeed needs to accept **kwargs or specific args from settings
            
            try:
                self.feed = SimulatedFeed(
                    instruments=self.instruments,
                    callback=self._handle_market_data,
                    **self.settings # Pass all settings directly
                )
                self.logger.info("Created SimulatedFeed with appropriate parameters")
            except Exception as e:
                self.logger.error(f"Failed to create SimulatedFeed: {e}")
                raise RuntimeError(f"Failed to create SimulatedFeed: {e}")

        elif self.feed_type == 'finvasia_ws':
            # Extract Finvasia-specific settings from broker config
            user_id = self.settings.get('user_id')
            password = self.settings.get('password')
            api_key = self.settings.get('api_key')
            imei = self.settings.get('imei', '')
            twofa = self.settings.get('twofa', '')
            vc = self.settings.get('vc', '')
            api_url = self.settings.get('api_url', "https://api.shoonya.com/NorenWClientTP/")
            ws_url = self.settings.get('websocket_url', "wss://api.shoonya.com/NorenWSTP/")
            debug = self.settings.get('debug', False)
            
            # Validate required parameters
            if not user_id or not password or not api_key:
                self.logger.error("Missing required Finvasia credentials. Need user_id, password, and api_key.")
                return            
            
            # Create the FinvasiaFeed instance
            self.feed = FinvasiaFeed(
                instruments=self.instruments,
                callback=self._handle_market_data,
                user_id=user_id,
                password=password,
                api_key=api_key,
                imei=imei,
                twofa=twofa,
                vc=vc,
                api_url=api_url,
                ws_url=ws_url,
                debug=debug
            )

            # Persistence is configured using settings passed
            if self.settings.get("enable_persistence", False) and hasattr(self.feed, 'enable_persistence'):
                persistence_path = self.settings.get("persistence_path", "./data/ticks")
                persistence_interval = self.settings.get("persistence_interval", 300)
                self.logger.info(f"Enabling persistence for Finvasia feed. Path: {persistence_path}, Interval: {persistence_interval}s")
                self.feed.enable_persistence(
                    enabled=True,
                    interval=persistence_interval,
                    path=persistence_path
                )
            
            # Set maximum ticks in memory if configured
            max_ticks = self.settings.get('max_ticks_in_memory', 10000)
            if hasattr(self.feed, 'set_max_ticks_in_memory'):
                self.feed.set_max_ticks_in_memory(max_ticks)

        else:
            raise ValueError(f"Unsupported feed type: {self.feed_type}")

        # Start the feed immediately after creation
        if self.feed:
            self.feed.start()
        else:
            self.logger.error(f"Failed to create feed of type {self.feed_type}")

    def stop(self):
        """Stop the market data feed."""
        if self.feed:
            self.feed.stop()

    def _handle_market_data(self, event: MarketDataEvent):
        """
        Handle incoming market data events.
        
        Args:
            event: Market data event containing tick data
        """
        try:
             # Properly classify the data type based on content
            data = event.data

            # Determine the most specific data type
            if MarketDataType.OHLC in data:
                # This is bar/candle data
                event.data_type = MarketDataType.BAR
            elif MarketDataType.LAST_PRICE in data and (MarketDataType.BID in data or MarketDataType.ASK in data):
                # This has bid/ask so it's a quote
                event.data_type = MarketDataType.QUOTE
            elif MarketDataType.LAST_PRICE in data:
                # This is a simple tick/trade
                event.data_type = MarketDataType.TICK

            # Forward to broker for processing
            if hasattr(self.broker, 'on_market_data'):
                self.broker.on_market_data(event)

            # Log event at debug level
            # self.logger.debug(f"Received market data event: {event}")

            # Forward to event manager for system-wide distribution         
            if hasattr(self, 'event_manager') and self.event_manager:
                # self.logger.debug(f"Distributing market data event to event manager: {event}")
                self.event_manager.publish(event)
            elif hasattr(self, 'callback') and callable(self.callback):
                # self.logger.debug(f"Distributing market data event via callback: {event}")
                self.callback(event)

        except Exception as e:
            self.logger.error(f"Error handling market data event: {e}", exc_info=True)

    def subscribe(self, instrument: Instrument):
        """
        Subscribe to market data for an instrument.
        Args:
            instrument: Instrument to subscribe to
        """
        if not self.feed:
            self.logger.warning(f"Cannot subscribe to {instrument.symbol}: Feed not initialized")
            return False
            
        # Check if already subscribed
        cache_key = f"{instrument.exchange.value}:{instrument.symbol}"
        if cache_key in self._subscribed_symbols:
            self.logger.debug(f"Symbol {instrument.symbol} already subscribed")
            return True
            
        try:
            # Add to instruments list if not present
            if instrument not in self.instruments:
                self.logger.info(f"Adding instrument to list: {instrument.symbol}")
                self.feed.add_instrument(instrument)
                
            # Subscribe through the feed
            self.logger.info(f"Subscribing to instrument: {instrument.symbol}")
            success = self.feed.subscribe(instrument)
            
            if success:
                # Add to subscribed symbols cache
                self._subscribed_symbols.add(cache_key)
                self.logger.info(f"Successfully subscribed to {instrument.symbol}")
            else:
                self.logger.error(f"Failed to subscribe to {instrument.symbol}")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error subscribing to {instrument.symbol}: {e}")
            return False

    def unsubscribe(self, instrument: Instrument):
        """
        Unsubscribe from market data for an instrument.

        Args:
            instrument: Instrument to unsubscribe from
        """
        if not self.feed:
            self.logger.warning(f"Cannot unsubscribe from {instrument.symbol}: Feed not initialized")
            return False
            
        try:
            # Unsubscribe through the feed
            self.logger.info(f"Unsubscribing from instrument: {instrument.symbol}")
            success = self.feed.unsubscribe(instrument)

            if success:
                self.logger.info(f"Successfully unsubscribed from {instrument.symbol}")

                self.feed.remove_instrument(instrument)

                # Remove from subscribed symbols cache
                cache_key = f"{instrument.exchange.value}:{instrument.symbol}"
                self._subscribed_symbols.discard(cache_key)
            else:
                self.logger.error(f"Failed to unsubscribe from {instrument.symbol}")

            return success
            
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {instrument.symbol}: {e}")
            return False

    def construct_trading_symbol(self, symbol: str, 
                                 strike: float, option_type: str, 
                                 expiry_date: str,
                                 instrument_type: str,
                                 exchange: str) -> str:
        """
        Get the broker-specific option symbol for the given parameters
        
        Args:
            symbol: The underlying symbol (e.g., 'NIFTY')
            strike: Strike price
            option_type: Option type ('CE' or 'PE')
            expiry_date: Expiry date
            
        Returns:
            str: The broker-specific option symbol
        """       
        
        if not self.feed:
            self.logger.error("Feed not initialized, cannot get symbol")
            return ""
            
        # For Finvasia feed, use the symbol cache
        if hasattr(self.feed, 'symbol_cache'):
            try:
                option_symbol = self.feed.symbol_cache.construct_trading_symbol(
                    name=symbol,
                    expiry_date_str=expiry_date,
                    instrument_type=instrument_type,
                    option_type=option_type,
                    strike=strike,
                    exchange=exchange)
                
                if option_symbol:
                    self.logger.info(f"Constructed option symbol: {option_symbol}")
                    return option_symbol
                    
                self.logger.warning(f"Option not found in cache: {symbol} {expiry_date} {strike} {option_type}")
                return ""
            except Exception as e:
                self.logger.error(f"Error constructing option symbol: {e}")
                return ""
        else:
            # Generic fallback format if no special cache/lookup is available
            expiry_str = expiry_date.strftime("%d%b%y").upper()
            return f"{symbol}{expiry_str}{strike}{option_type}"
            
    def get_expiry_dates(self, symbol: str) -> List[date]:
        """
        Get available expiry dates for an underlying
        
        Args:
            underlying_symbol: The underlying symbol (e.g., 'NIFTY')
            
        Returns:
            List[date]: List of available expiry dates
        """        
        
        if not self.feed:
            self.logger.error("Feed not initialized, cannot get expiry dates")
            return []
            
        # For Finvasia feed, use the symbol cache
        if hasattr(self.feed, 'symbol_cache'):
            try:
                # Get expiry dates from symbol cache
                expiry_dates = self.feed.symbol_cache.get_combined_expiry_dates(symbol)

                # self.logger.debug(f"Expiry dates for {symbol}: {expiry_dates}")
                if expiry_dates:
                    return sorted(expiry_dates)  # Return sorted expiries
                    
                self.logger.warning(f"No expiry dates found for {symbol}")
                
                # Fallback to utility function if no expiries found in cache
                from utils.expiry_utils import get_next_expiry
                next_expiry = get_next_expiry(symbol)
                return [next_expiry] if next_expiry else []
                
            except Exception as e:
                self.logger.error(f"Error getting expiry dates: {e}")
                
                # Fallback to utility function
                from utils.expiry_utils import get_next_expiry
                next_expiry = get_next_expiry(symbol)
                return [next_expiry] if next_expiry else []
        else:
            # Fallback to utility function if no symbol cache
            from utils.expiry_utils import get_next_expiry
            next_expiry = get_next_expiry(symbol)
            return [next_expiry] if next_expiry else []
            
    def subscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        """
        Subscribe to multiple instruments at once
        
        Args:
            instruments: List of instruments to subscribe to
            
        Returns:
            Dict[str, bool]: Dictionary mapping instrument symbols to subscription success status
        """       
        
        if not self.feed:
            self.logger.error("Feed not initialized, cannot subscribe to instruments")
            return {instrument.symbol: False for instrument in instruments}
            
        results = {}
        
        # For each instrument, call subscribe method
        for instrument in instruments:
            try:
                # Use existing subscribe method
                success = self.subscribe(instrument)
                results[instrument.symbol] = success
                
                if not success:
                    self.logger.warning(f"Failed to subscribe to {instrument.symbol} on {instrument.exchange}")
                
            except Exception as e:
                self.logger.error(f"Error subscribing to {instrument.symbol}: {e}")
                results[instrument.symbol] = False
                
        # Log summary
        success_count = sum(1 for v in results.values() if v)
        self.logger.info(f"Subscribed to {success_count}/{len(instruments)} instruments")
        
        return results
    
    def unsubscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        """
        Unsubscribe to multiple instruments at once
        
        Args:
            instruments: List of instruments to unsubscribe from
            
        Returns:
            Dict[str, bool]: Dictionary mapping instrument symbols to unsubscription success status
        """       
        
        if not self.feed:
            self.logger.error("Feed not initialized, cannot unsubscribe to instruments")
            return {instrument.symbol: False for instrument in instruments}
            
        results = {}
        
        # For each instrument, call subscribe method
        for instrument in instruments:
            try:
                # Use existing subscribe method
                success = self.unsubscribe(instrument)
                results[instrument.symbol] = success
                
                if not success:
                    self.logger.warning(f"Failed to unsubscribe to {instrument.symbol} on {instrument.exchange}")
                
            except Exception as e:
                self.logger.error(f"Error unsubscribing to {instrument.symbol}: {e}")
                results[instrument.symbol] = False
                
        # Log summary
        success_count = sum(1 for v in results.values() if v)
        self.logger.info(f"Unsubscribed to {success_count}/{len(instruments)} instruments")
        
        return results

# Example usage
# --- Example Usage (Updated) ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, # DEBUG for more detail
        format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s'
    )

    # --- Credentials ---
    FINVASIA_USER_ID = os.environ.get("FINVASIA_USER_ID", "YOUR_USER_ID")
    FINVASIA_PASSWORD = os.environ.get("FINVASIA_PASSWORD", "YOUR_PASSWORD")
    FINVASIA_API_KEY = os.environ.get("FINVASIA_API_KEY", "YOUR_API_KEY")
    FINVASIA_TWOFA = os.environ.get("FINVASIA_TWOFA", "YOUR_TOTP_SECRET")
    FINVASIA_VC = os.environ.get("FINVASIA_VC", "YOUR_VENDOR_CODE")

    # --- Dynamic Symbol Finding (Example) ---
    initial_instruments = []
    try:
        if not all([FINVASIA_USER_ID != "YOUR_USER_ID", FINVASIA_TWOFA != "YOUR_TOTP_SECRET"]):
             raise ValueError("Credentials not set for dynamic symbol search.")

        # Temp API login for search
        temp_api = NorenApi(host="https://api.shoonya.com/NorenWClientTP/", websocket="")
        login_resp = temp_api.login(FINVASIA_USER_ID, FINVASIA_PASSWORD, pyotp.TOTP(FINVASIA_TWOFA).now(), FINVASIA_VC, FINVASIA_API_KEY, str(uuid.uuid4()))

        nifty_option_symbol = None
        if login_resp and login_resp.get('stat') == 'Ok':
             # Search for near-month NIFTY CE options
             search_resp = temp_api.searchscrip(exchange='NFO', searchtext='NIFTY CE')
             if search_resp and search_resp.get('stat') == 'Ok' and search_resp.get('values'):
                  today_str = datetime.now().strftime('%Y%m%d')
                  future_options = []
                  for item in search_resp['values']:
                       # Finvasia expiry format might be DDMMMYYYY
                       exd_str = item.get('exd')
                       tsym = item.get('tsym')
                       optt = item.get('optt')
                       if exd_str and tsym and optt == 'CE':
                            try:
                                 exd_dt = datetime.strptime(exd_str, '%d%b%Y') # Adjust format if needed
                                 if exd_dt.strftime('%Y%m%d') >= today_str:
                                      # Add strike price for sorting proximity
                                      item['strike'] = float(item.get('strprc', 0))
                                      future_options.append(item)
                            except (ValueError, TypeError):
                                 logging.warning(f"Could not parse expiry {exd_str} for {tsym}")

                  if future_options:
                       future_options.sort(key=lambda x: datetime.strptime(x['exd'], '%d%b%Y')) # Sort by date
                       # Add logic here to find ATM/near-ATM strike if needed
                       nifty_option_symbol = future_options[0]['tsym'] # Take nearest expiry
                       logging.info(f"Dynamically found Nifty Option symbol: {nifty_option_symbol}")
             else: logging.warning("Failed to search for Nifty option symbols.")
        else: logging.warning(f"Temp login failed: {login_resp.get('emsg') if login_resp else 'No Response'}")

        # Create Instrument object
        if nifty_option_symbol:
             # Get details to populate Instrument better
             details = temp_api.searchscrip(exchange='NFO', searchtext=nifty_option_symbol)
             inst_details = {}
             if details and details.get('stat') == 'Ok' and details.get('values'):
                  inst_details = details['values'][0]

             nifty_option = Instrument(
                 symbol=nifty_option_symbol,
                 exchange=Exchange.NFO.value, # Use string value
                 instrument_type=InstrumentType.OPTION,
                 asset_class=AssetClass.OPTIONS,
                 lot_size=float(inst_details.get('ls', 50)), # Get lot size if available
                 tick_size=float(inst_details.get('ti', 0.05)), # Get tick size
                 strike_price=float(inst_details.get('strprc', 0)),
                 option_type=OptionType.CALL, # Since we searched for CE
                 expiry_date=inst_details.get('exd') # Store expiry
             )
             initial_instruments.append(nifty_option)
        else:
             logging.warning("Using fallback static option symbol - VERIFY THIS IS CORRECT!")
             fallback_symbol = "NIFTY 18APR2024 22500 CE" # !! MUST BE VALID !!
             nifty_option = Instrument(symbol=fallback_symbol, exchange=Exchange.NFO.value, instrument_type=InstrumentType.OPTION)
             initial_instruments.append(nifty_option)

    except ImportError:
         logging.error("pyotp library not found. Please install it (`pip install pyotp`). Cannot run example.")
         initial_instruments = [] # Prevent feed start without pyotp
    except Exception as e:
         logging.error(f"Error during dynamic symbol finding: {e}", exc_info=True)
         # Optionally add static fallback here if dynamic fails critically
         initial_instruments = []


    # --- Callback Function ---
    def market_data_handler(event: MarketDataEvent):
        """Handles incoming market data events."""
        symbol = event.instrument.symbol
        # Use enum values (uppercase strings) to access data
        data = event.data
        dtype = event.data_type # MarketDataType enum member

        log_msg = f"[{dtype.value}] {symbol}: " # Use enum value
        parts = []
        # Access data using MarketDataType.XXX.value
        if MarketDataType.LAST_PRICE.value in data: parts.append(f"LTP={data[MarketDataType.LAST_PRICE.value]:.2f}")
        if MarketDataType.BID.value in data: parts.append(f"Bid={data[MarketDataType.BID.value]:.2f}")
        if MarketDataType.ASK.value in data: parts.append(f"Ask={data[MarketDataType.ASK.value]:.2f}")
        if MarketDataType.VOLUME.value in data: parts.append(f"Vol={data[MarketDataType.VOLUME.value]}")
        if MarketDataType.OPEN_INTEREST.value in data: parts.append(f"OI={data[MarketDataType.OPEN_INTEREST.value]}")
        if MarketDataType.OHLC.value in data:
             ohlc = data[MarketDataType.OHLC.value]
             parts.append(f"O={ohlc.get(MarketDataType.OPEN.value, 'N/A')} "
                          f"H={ohlc.get(MarketDataType.HIGH.value, 'N/A')} "
                          f"L={ohlc.get(MarketDataType.LOW.value, 'N/A')} "
                          f"C={ohlc.get(MarketDataType.CLOSE.value, 'N/A')}")
        logging.info(log_msg + ", ".join(parts))

    # --- Initialize & Run FinvasiaFeed ---
    if not all([FINVASIA_USER_ID != "YOUR_USER_ID", FINVASIA_PASSWORD != "YOUR_PASSWORD", FINVASIA_API_KEY != "YOUR_API_KEY", FINVASIA_TWOFA != "YOUR_TOTP_SECRET"]):
         logging.error("Please set Finvasia credentials in environment variables or script before running.")
    elif not initial_instruments:
         logging.error("No initial instruments found or created. Cannot start feed.")
    else:
         feed = None
         try:
             feed = FinvasiaFeed(
                 instruments=initial_instruments,
                 callback=market_data_handler,
                 user_id=FINVASIA_USER_ID, password=FINVASIA_PASSWORD, api_key=FINVASIA_API_KEY,
                 twofa=FINVASIA_TWOFA, vc=FINVASIA_VC,
                 debug=False # Set True for very detailed logs
             )
             feed.enable_persistence(enabled=True, path="./market_data_ticks", interval=60)
             feed.set_max_ticks_in_memory(5000)
             feed.start() # Start the feed

             if feed.is_running:
                 logging.info("Market data feed running. Press Ctrl+C to stop.")
                 # Add dynamic subscriptions or other interactions here
                 time.sleep(15)
                 if feed.is_running:
                      logging.info("--- Adding RELIANCE subscription ---")
                      reliance_inst = Instrument(symbol="RELIANCE", exchange=Exchange.NSE.value, instrument_type=InstrumentType.EQUITY)
                      feed.subscribe(reliance_inst)
                 time.sleep(15)
                 if feed.is_running:
                      logging.info(f"--- Status: {feed.get_subscription_status()} ---")
                      logging.info(f"--- Stats: {feed.get_tick_statistics()} ---")

                 # Keep main thread alive while feed runs
                 while feed.is_running: time.sleep(1)

         except KeyboardInterrupt:
             logging.info("KeyboardInterrupt received. Stopping feed...")
         except Exception as main_e:
             logging.error(f"An error occurred during feed execution: {main_e}", exc_info=True)
         finally:
             if feed and feed.is_running:
                  feed.stop()
             logging.info("Market data feed stopped.")