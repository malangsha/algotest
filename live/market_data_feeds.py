"""
Market data feed implementations for live trading.

This module provides various market data feed implementations for live trading,
including simulated feeds for testing and real-time feeds for production.
The feeds are responsible for subscribing to market data and forwarding it to
the trading engine via callbacks.

The module has been updated to support shared session management with brokers
when both are using the same provider (e.g., Finvasia).
"""

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
import pytz
import pyotp
import threading
from datetime import datetime, timedelta, date, time as dt_time
from typing import List, Dict, Any, Optional, Callable, Union, Tuple, Set
from abc import ABC, abstractmethod
from enum import Enum
from NorenRestApiPy.NorenApi import NorenApi

from models.instrument import Instrument, AssetClass
from models.events import MarketDataEvent, OrderEvent, ExecutionEvent
from utils.constants import ( MarketDataType, EventType, InstrumentType, 
                             OptionType, Exchange, OrderStatus, OrderSide, 
                             OrderType, EventSource)
from utils.exceptions import MarketDataException
from utils.event_utils import EventUtils
from core.event_manager import EventManager
from core.logging_manager import get_logger
from live.feeds.finvasia.symbol_cache import SymbolCache
from utils.tick_source import ScriptedTickSource

class MarketDataFeedBase(ABC):
    """Base abstract class for market data feeds."""
    def __init__(self, instruments: List[Instrument], callback: Callable, event_manager: Optional[EventManager] = None):
        self.logger = get_logger("live.market_data_feed_base")
        self.instruments = instruments if instruments else []
        
        # Store instruments in a way that allows quick lookup by symbol/exchange
        self._instrument_lookup: Dict[Tuple[str, str], Instrument] = {}
        if instruments:
             for inst in instruments:
                  if inst.symbol and inst.exchange:
                       self._instrument_lookup[(inst.symbol, inst.exchange)] = inst

        self.callback = callback
        self.event_manager = event_manager # Store event_manager
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
    """
    A high-fidelity simulated market data feed that generates ticks for indices
    based on config and derives option prices in real-time.
    """

    def __init__(self,
                instruments: Optional[List[Instrument]] = None,
                callback: Optional[Callable[[Union[MarketDataEvent, OrderEvent, ExecutionEvent]], None]] = None,
                event_manager: Optional[EventManager] = None,
                **kwargs
        ):    
        super().__init__(instruments, callback, event_manager)        
        
        # --- Configuration & State ---
        self._tick_interval = kwargs.get("tick_interval", 0.5)
        self._settings = kwargs.get("indices", [])        
        self.index_configs: Dict[str, Dict] = {
            index['name']: index for index in self._settings
        }    
        self.logger.info(f"Loaded {len(self.index_configs)} index configurations for simulation.")

        # --- Simulation Components ---
        self._tick_sources: Dict[str, ScriptedTickSource] = {}
        self.underlying_to_options: Dict[str, Set[Instrument]] = {name: set() for name in self.index_configs}
        self.last_prices: Dict[str, float] = {
            name: config['spot_price'] for name, config in self.index_configs.items()
        }
        
        self.timezone = pytz.timezone('Asia/Kolkata')
        self.symbol_cache = SymbolCache()
        self._simulation_clock: Optional[float] = None
        
        # --- Event Handling ---
        if self.event_manager:
            self.logger.info("Registering for SIMULATED_ORDER_UPDATE and MARKET_DATA events.")
            self.event_manager.subscribe(EventType.SIMULATED_ORDER_UPDATE, self._on_order_event, component_name="SimulatedFeed")
            self.event_manager.subscribe(EventType.MARKET_DATA, self._on_underlying_tick, component_name="SimulatedFeed")            
        else:
            self.logger.warning("EventManager not provided. Cannot process orders or generate option prices.")

    def _initialize_simulation_clock(self):
        """Initializes the simulation clock to a realistic market start time."""
        now_ist = datetime.now(self.timezone)
        market_open_time = dt_time(9, 15)

        # Start simulation on today's date if before market close, otherwise next weekday
        sim_date = now_ist.date()
        if now_ist.time() > dt_time(15, 30):
            sim_date += timedelta(days=1)

        # Advance to the next weekday if the calculated date is a weekend
        while sim_date.weekday() >= 5: # 5 = Saturday, 6 = Sunday
            sim_date += timedelta(days=1)       

        start_datetime = self.timezone.localize(datetime.combine(sim_date, market_open_time))
        self._simulation_clock = start_datetime.timestamp()

    def start(self) -> bool:
        """Creates and starts a tick source for each configured index."""
        if self.is_running:
            self.logger.warning("Simulated feed is already running.")
            return True

        self._initialize_simulation_clock()
        if self._simulation_clock:
            self.logger.info(f"Tick source started. Simulation clock starts at: {datetime.fromtimestamp(self._simulation_clock, self.timezone).isoformat()}")

        self.logger.info("Starting SimulatedFeed...")
        if not self.index_configs:
            self.logger.error("No index configurations in settings. Cannot start simulation.")
            return False

        for name, config in self.index_configs.items():
            try:
                source = ScriptedTickSource(
                    event_manager=self.event_manager,
                    scenario=config.get("scenario", "random_walk"),
                    initial_price=config.get("spot_price", 100),
                    tick_interval=self._tick_interval,  # Corrected here
                )
                source.start()
                self._tick_sources[name] = source
                self.logger.info(f"Started tick source for '{name}' with scenario '{config['scenario']}'.")
            except Exception as e:
                self.logger.error(f"Failed to start tick source for '{name}': {e}", exc_info=True)
                self.stop()
                return False       
        
        self.is_running = True
        return True

    def stop(self) -> bool:
        """Stops all running tick sources."""
        if not self.is_running: return True
        self.logger.info("Stopping SimulatedFeed...")
        for source in self._tick_sources.values():
            source.stop()
        self._tick_sources.clear()
        self.is_running = False
        self.logger.info("SimulatedFeed stopped.")
        return True

    def _get_underlying_name(self, instrument: Instrument) -> Optional[str]:
        """Determines the underlying index name for an instrument."""
        for index_name in self.index_configs:
            if instrument.symbol == index_name: return index_name
            simple_index_name = index_name.split(' ')[0]
            if instrument.symbol.startswith(simple_index_name): return index_name
        self.logger.warning(f"Could not determine underlying for instrument: {instrument.symbol}")
        return None

    def subscribe(self, instrument: Instrument) -> bool:
        """Subscribes to an instrument, starting ticks for its underlying index."""
        if not self.is_running: return False

        underlying_name = self._get_underlying_name(instrument)
        if not underlying_name: return False

        source = self._tick_sources.get(underlying_name)
        if not source: return False

        if instrument.asset_class == AssetClass.OPTIONS:
            self.underlying_to_options[underlying_name].add(instrument)
            self.logger.info(f"Tracking option {instrument.symbol} under {underlying_name}.")

        underlying_instrument = Instrument(
            symbol=underlying_name, 
            exchange=self.index_configs[underlying_name]['exchange'], 
            asset_class=AssetClass.INDEX
        )
        source.subscribe(underlying_instrument)
        
        self.add_instrument(instrument)
        return True

    def unsubscribe(self, instrument: Instrument) -> bool:
        """Unsubscribes from an instrument and stops underlying ticks if no longer needed."""
        underlying_name = self._get_underlying_name(instrument)
        if not underlying_name: return False
            
        if instrument.asset_class == AssetClass.OPTIONS:
            self.underlying_to_options[underlying_name].discard(instrument)
            self.logger.info(f"Stopped tracking option {instrument.symbol}.")
        
        is_index_itself_subscribed = any(
            inst.symbol == underlying_name for inst in self.instruments if inst != instrument
        )
        
        if not self.underlying_to_options[underlying_name] and not is_index_itself_subscribed:
             source = self._tick_sources.get(underlying_name)
             if source:
                underlying_instrument = Instrument(symbol=underlying_name, exchange=self.index_configs[underlying_name]['exchange'], asset_class=AssetClass.INDEX)
                source.unsubscribe(underlying_instrument)
                self.logger.info(f"Unsubscribed from underlying '{underlying_name}' as no more instruments depend on it.")

        self.remove_instrument(instrument)
        return True

    def _on_underlying_tick(self, event: MarketDataEvent):
        """Listens to index ticks and generates derived ticks for subscribed options."""
        instrument = event.instrument
        if instrument.asset_class != AssetClass.INDEX or instrument.symbol not in self._tick_sources:
            return
        
        underlying_name = instrument.symbol
        new_spot_price = event.data.get(MarketDataType.LAST_PRICE.value)
        if new_spot_price is None: return

        price_change = new_spot_price - self.last_prices.get(underlying_name, new_spot_price)
        self.last_prices[underlying_name] = new_spot_price
        self.last_prices[instrument.symbol] = new_spot_price # Also store by exact symbol

        for option_instrument in self.underlying_to_options.get(underlying_name, []):
            self._generate_and_publish_option_tick(option_instrument, new_spot_price, price_change)
            
    def _generate_and_publish_option_tick(self, option: Instrument, spot_price: float, change: float):
        """Calculates a simulated option price and publishes a MarketDataEvent."""
        # Simplified option pricing using delta
        moneyness = spot_price - option.strike
        if option.option_type == OptionType.PUT: moneyness *= -1

        delta = 0.5 + (moneyness / spot_price) * 5
        delta = max(0.01, min(0.99, delta))
        if option.option_type == OptionType.PUT: delta -= 1

        prev_price = self.last_prices.get(option.symbol, max(0.05, spot_price - option.strike if option.option_type == OptionType.CALL else option.strike - spot_price))
        new_price = max(0.05, prev_price + (change * delta))
        self.last_prices[option.symbol] = new_price

        tick_data = {
            MarketDataType.TIMESTAMP.value: self._simulation_clock,
            MarketDataType.LAST_PRICE.value: new_price,
            MarketDataType.BID.value: new_price * 0.995,
            MarketDataType.ASK.value: new_price * 1.005,
        }

        event = MarketDataEvent(
            event_type=EventType.MARKET_DATA, timestamp=tick_data[MarketDataType.TIMESTAMP.value],
            instrument=option, data=tick_data, data_type=MarketDataType.TICK
        )
        # self.logger.debug(f"MarketDataEvent: {event}")
        if self.callback: self.callback(event)
        
    def _on_order_event(self, event: OrderEvent):
        """Handles simulated order updates to mimic broker execution."""
        self.logger.info(f"Received simulated order event for Order ID: {event.order_id}")
        
        status = OrderStatus.FILLED
        exec_type = "FILL"
        last_price = self.last_prices.get(event.instrument.symbol)

        if event.order_type == OrderType.LIMIT and last_price:
            if (event.side == OrderSide.BUY and event.price < last_price) or \
               (event.side == OrderSide.SELL and event.price > last_price):
                status = OrderStatus.OPEN
                exec_type = "BROKER_OPEN"
        
        self.logger.info(f"Simulating execution for Order ID {event.order_id}: Status={status}")
        
        is_filled = status == OrderStatus.FILLED
        exec_event = ExecutionEvent(
            order_id=event.order_id, broker_order_id=event.broker_order_id or f"sim-{uuid.uuid4()}",
            symbol=event.instrument.symbol, exchange=event.instrument.exchange, side=event.side,
            quantity=event.quantity, order_type=event.order_type, status=status,
            price=event.price, trigger_price=event.trigger_price,
            execution_id=f"sim-exec-{uuid.uuid4()}", execution_time=time.time(), execution_type=exec_type,
            cumulative_filled_quantity=event.quantity if is_filled else 0,
            average_price=event.price if is_filled else 0,
            last_filled_quantity=event.quantity if is_filled else 0,
            last_filled_price=event.price if is_filled else 0,
            timestamp=time.time(), event_type=EventType.EXECUTION, source=EventSource.MARKET_DATA_FEED
        )

        if self.event_manager: self.event_manager.publish(exec_event)

    def subscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        """Subscribes to a list of instruments."""
        return {inst.instrument_id: self.subscribe(inst) for inst in instruments}

    def unsubscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        """Unsubscribes from a list of instruments."""
        return {inst.instrument_id: self.unsubscribe(inst) for inst in instruments}

# --- FinvasiaFeed Class  ---
class FinvasiaFeed(MarketDataFeedBase):
    """
    Finvasia WebSocket feed implementation with session manager support.
    
    This class has been updated to support shared session management with the broker
    when both are using the same provider (Finvasia).
    """
    def __init__(
        self,
        instruments: List[Instrument],
        callback: Callable[[Union[MarketDataEvent, OrderEvent, ExecutionEvent]], None],
        event_manager: Optional[EventManager] = None,
        user_id: str = None,
        password: str = None,
        api_key: str = None,
        imei: str = None,
        twofa: str = None,
        vc: str = None,
        api_url: str = "https://api.shoonya.com/NorenWClientTP/",
        ws_url: str = "wss://api.shoonya.com/NorenWSTP/",
        debug: bool = False,
        session_manager = None  # New parameter for session manager
    ):
        # super().__init__(instruments, callback, event_manager) 
        # self.logger = get_logger("live.finvasia_feed")
        # if debug:
        #    self.logger.setLevel(logging.DEBUG)

        self.logger = get_logger("live.finvasia_feed")
        if debug:
            self.logger.setLevel(logging.DEBUG)

        self._stop_reconnect_event = threading.Event()
        self._reconnect_thread = None
        
        # assign event manager
        self.event_manager = event_manager

        # State tracking
        self.is_connected = False
        self.is_authenticated = False
        self.session_token = None
        
        # Session manager support
        self._session_manager = session_manager
        if session_manager:
            self.logger.info("Using shared session manager for authentication")
        
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

        super().__init__(instruments, callback, self.event_manager)
        self.logger = get_logger("live.finvasia_feed")
        
        
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

        # Register for session manager callbacks if provided
        if self._session_manager and hasattr(self._session_manager, 'register_refresh_callback'):
            self._session_manager.register_refresh_callback(self._on_session_refresh)
            self.logger.info("Registered for session refresh callbacks")

        self.logger.info("FinvasiaFeed Class initialized")
    
    def _on_session_refresh(self, session):
        """
        Callback for session refresh events from the session manager.
        
        Args:
            session: Updated session object
        """
        self.logger.info("Session refresh callback received")
        
        # Update session token
        with self._auth_lock:
            self.session_token = session.token
            self.user_id = session.user_id
            self.password = session.password
            self.is_authenticated = True
        
        # Reconnect WebSocket with new token if currently connected
        if self.is_connected:
            self.logger.info("Reconnecting WebSocket with new session token")
            self.disconnect()
            time.sleep(1)  # Brief pause
            self.connect()
    
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
        """
        Authenticate with Finvasia API.
        
        If a session manager is provided, use it for authentication.
        Otherwise, authenticate directly with the API.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        with self._auth_lock:
            if self.is_authenticated:
                return True
            
            try:
               # If using session manager, get session from there
                if self._session_manager:
                    session = self._session_manager.get_session()
                    if session and session.token and session.user_id and session.password:
                        self.session_token = session.token
                        self.user_id = session.user_id # Ensure feed has correct user_id
                        self.password = session.password # Ensure feed has correct password
                        self.is_authenticated = True
                        self.logger.info("Authentication successful using session manager")
                        return True
                    else:
                        self.logger.error("Failed to get valid session from session manager")
                        return False
                
                # Otherwise, authenticate directly
                self.logger.info("Attempting direct authentication...")
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
                    self.logger.info("Authentication successful with direct API login")
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
            self.api.on_close = self._on_close 
            self.api.on_ticks = self._on_market_data            

            self.logger.info("Starting WebSocket connection with NorenApi...")
            self.is_connected = False # Reset connection status before attempting            
            
            session_set_successfully = False
            if self._session_manager:
                session = self._session_manager.get_session()
                if session and session.user_id and session.password and session.token:
                    self.logger.info(f"Setting session for user {session.user_id} using Session Manager")
                    self.api.set_session(userid=session.user_id, password=session.password, usertoken=session.token)
                    session_set_successfully = True
                else:
                    self.logger.error("Failed to get complete session details from Session Manager. Cannot set session.")
            else:
                # Use credentials stored directly in the feed instance
                if self.user_id and self.password and self.session_token:
                    self.logger.info(f"Setting session for user {self.user_id} using direct credentials")
                    self.api.set_session(userid=self.user_id, password=self.password, usertoken=self.session_token)
                    session_set_successfully = True
                else:
                     self.logger.error("Missing credentials or session token for direct session setting. Cannot set session.")
            
            if not session_set_successfully:
                return False 
            
            self.api.start_websocket(
                order_update_callback=self._on_order_update,
                subscribe_callback=self._on_market_data,
                socket_open_callback=self._on_open,
                socket_close_callback=self._on_close                
            )
            
            # Wait for connection status to be updated by _on_open
            start_time = time.time()
            connection_timeout = 15  # seconds
            self.logger.debug(f"Waiting up to {connection_timeout}s for WebSocket connection to establish...")
            
            while not self.is_connected and (time.time() - start_time) < connection_timeout:
                if self._stop_reconnect_event.is_set():
                    self.logger.info("Connection attempt aborted as feed is stopping.")
                    self.api.close_websocket()
                    return False
                time.sleep(0.1)
                
            if not self.is_connected:
                self.logger.error(f"WebSocket connection timeout after {connection_timeout} seconds. _on_open was not called.")
                self.api.close_websocket()                
                return False
            
            self.logger.info("WebSocket connection process initiated successfully. _on_open confirmed connection.")
            # _reconnect_attempts are reset in _on_open
            return True

        except Exception as e:
            self.logger.error(f"Exception during WebSocket connection: {e}", exc_info=True)
            self.api.close_websocket()
            self.is_connected = False             
            return False
    
    def disconnect(self):
        """Disconnect from the WebSocket feed."""
        try:
            self.api.close_websocket()
        except Exception as e:
            self.logger.warning(f"Error during WebSocket disconnect: {e}")
        
        self.is_connected = False
        self.logger.info("WebSocket disconnected")
    
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
            
        # Attempt reconnection
        self._attempt_reconnect()
    
    def _on_error(self, error):
        """Handle WebSocket error callback."""
        self.logger.error(f"WebSocket error: {error}")
        
        # Check if error indicates session expiry
        if isinstance(error, str) and ('session' in error.lower() and ('expired' in error.lower() or 'invalid' in error.lower())):
            self.logger.warning("Session appears to be expired based on WebSocket error. Attempting to refresh.")
            self._refresh_session()
    
    def _on_open(self):
        """Handle WebSocket open callback."""
        self.logger.info("WebSocket connection opened")
        self.is_connected = True
        self._reconnect_attempts = 0  # Reset reconnection counter on successful connection
        
        # Resubscribe to all previously subscribed instruments
        if self._subscribed_tokens:
            self.logger.info(f"Resubscribing to {len(self._subscribed_tokens)} instruments")
            with self._sub_lock:
                subscriptions = []
                for token in self._subscribed_tokens:
                    symbol = self._token_to_symbol.get(token)
                    if symbol:
                        for instrument in self.instruments:
                            if instrument.symbol == symbol:
                                exchange_str = instrument.exchange.value if isinstance(instrument.exchange, Exchange) else str(instrument.exchange)
                                subscriptions.append(f"{exchange_str}|{token}")
                                break
                
                if subscriptions:
                    try:
                        self.api.subscribe(subscriptions)
                        self.logger.info(f"Resubscribed to {len(subscriptions)} instruments")
                    except Exception as e:
                        self.logger.error(f"Error during resubscription: {e}")
    
    def _on_close(self, code=None, reason=None):
        """Handle WebSocket close callback."""
        self.logger.warning(f"WebSocket connection closed: code={code}, reason={reason}")
        self.is_connected = False
        
        if self._stop_reconnect_event.is_set():
            self.logger.info("Reconnection suppressed via _on_close as feed is stopping.")
            return
            
        # Attempt reconnection
        self._attempt_reconnect()
    
    def _on_market_data(self, ticks):
        """
        Handle market data callback from WebSocket.
        
        Args:
            ticks: List of tick data from Finvasia
        """
        if not isinstance(ticks, list):
            ticks = [ticks]
            
        for tick in ticks:
            try:
                # self.logger.debug(f"tick: {tick}")

                # Extract token and exchange
                token = tick.get('tk')
                # exchange = tick.get('e')
                
                if not token:
                    continue
                    
                # Get symbol from token
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
                    
                # Convert tick to market data
                market_data = self._convert_tick_to_market_data(tick)
                if not market_data:
                    continue

                # self.logger.debug(f"market_data: {market_data}")
                    
                # Create event
                event = MarketDataEvent(
                    event_type=EventType.MARKET_DATA,
                    timestamp=market_data.get('TIMESTAMP', time.time()),
                    instrument=instrument,
                    data=market_data,
                    data_type=MarketDataType.TICK
                )
                
                # Send event to callback
                if self.callback:
                    self.callback(event)
                    
            except Exception as e:
                self.logger.error(f"Error processing market data: {e}")
    
    def _on_order_update(self, order_data: Dict[str, Any]):
        self.logger.debug(f"Raw Order update from Finvasia WS: {order_data}")
        if not self.event_manager:
            self.logger.error("EventManager not available in FinvasiaFeed. Cannot publish ExecutionEvent for order updates.")
            return

        try:
            self.logger.debug(f"MDF order update, order_data: {order_data}")
            api_status = order_data.get('status', '').upper()
            broker_order_id = order_data.get('norenordno')            
            
            if not broker_order_id:
                self.logger.warning(f"Order update missing 'norenordno' (broker_order_id): {order_data}")
                return

            # Validate broker order ID
            if not EventUtils.validate_broker_order_id(broker_order_id):
                self.logger.error(f"Invalid broker order ID format: {broker_order_id}")
                return
    
            # Common fields for ExecutionEvent
            event_time = time.time()
            exec_id = f"exec-{uuid.uuid4()}"
            parsed_symbol = order_data.get('tsym', 'UNKNOWN_SYMBOL')
            parsed_exchange_str = order_data.get('exch', 'NSE').upper()
            order_instrument_id = f"{parsed_exchange_str}:{parsed_symbol}"
            order_id = order_data.get('remarks', '').partition('=')[2] or None

            try:
                parsed_exchange = Exchange(parsed_exchange_str)
            except ValueError:
                self.logger.warning(f"Unknown exchange '{parsed_exchange_str}' in order update. Defaulting to NSE.")
                parsed_exchange = Exchange.NSE

            trantype = order_data.get('trantype', '')
            parsed_side = OrderSide.BUY if trantype == 'B' else (OrderSide.SELL if trantype == 'S' else None)
            
            prctyp = order_data.get('prctyp', '')
            parsed_order_type = None
            if prctyp == 'MKT': parsed_order_type = OrderType.MARKET
            elif prctyp == 'LMT': parsed_order_type = OrderType.LIMIT    
            elif prctyp == 'SL-LMT': parsed_order_type = OrderType.SL
            elif prctyp == 'SL-MKT': parsed_order_type = OrderType.SL_M
            
            parsed_quantity = float(order_data.get('qty', 0.0))
            parsed_price = float(order_data.get('prc', 0.0))
            parsed_trigger_price = float(order_data.get('trgprc', 0.0)) if order_data.get('trgprc') else None

            if api_status == 'REJECTED':
                exec_type='BROKER_REJECT'
                rejection_reason = order_data.get('rejreason', 'Unknown rejection reason from broker')
                self.logger.info(f"Broker Order ID {broker_order_id} is {api_status}. Reason: {rejection_reason}")                
                self.logger.info(f"Simulating the OrderFILL on BROKER REJECT")
                
                # exec_event = ExecutionEvent(
                #     order_id=order_id, 
                #     broker_order_id=broker_order_id, symbol=order_instrument_id,
                #     exchange=parsed_exchange, side=parsed_side, quantity=parsed_quantity,
                #     order_type=parsed_order_type, status=OrderStatus.REJECTED, price=parsed_price,
                #     trigger_price=parsed_trigger_price, rejection_reason=rejection_reason,
                #     execution_id=exec_id, execution_time=event_time, execution_type=exec_type,
                #     filled_quantity=0.0, average_price=0.0, timestamp=event_time,
                #     event_type=EventType.EXECUTION, source=EventSource.MARKET_DATA_FEED
                # )

                filled_qty = 75
                avg_price = parsed_price
                if parsed_exchange == Exchange.BFO:
                    filled_qty = 20

                exec_event = ExecutionEvent(
                    order_id=order_id,
                    broker_order_id=broker_order_id, symbol=order_instrument_id,
                    exchange=parsed_exchange, side=parsed_side, quantity=parsed_quantity, # Original order quantity
                    order_type=parsed_order_type, status=OrderStatus.FILLED, price=parsed_price, # Original order price
                    trigger_price=parsed_trigger_price,
                    execution_id=exec_id, execution_time=event_time, execution_type="FILL",
                    cumulative_filled_quantity=filled_qty, # Cumulative filled for this order
                    average_price=avg_price, # Average fill price for this order
                    last_filled_quantity=filled_qty, # Assuming 'COMPLETE' means one fill event for the full quantity or remaining
                    last_filled_price=avg_price,     # Use average price as last fill price for simplicity here
                    commission=float(order_data.get('brkVal', 0.0)), # Example: if broker value is commission
                    timestamp=event_time,
                    event_type=EventType.EXECUTION, source=EventSource.MARKET_DATA_FEED
                )

                self.event_manager.publish(exec_event)
                self.logger.info(f"Published REJECTED ExecutionEvent for Broker Order ID: {broker_order_id}")

            elif api_status == 'COMPLETE': # This means FILLED
                filled_qty = float(order_data.get('fillshares', parsed_quantity)) # If fillshares not present, assume full qty for COMPLETE
                avg_price = float(order_data.get('avgprc', parsed_price)) # Use order price if avgprc not present
                
                self.logger.info(f"Broker Order ID {broker_order_id} COMPLETE (FILLED). Qty: {filled_qty}, AvgPx: {avg_price}")
                
                exec_event = ExecutionEvent(
                    order_id=order_id,
                    broker_order_id=broker_order_id, symbol=order_instrument_id,
                    exchange=parsed_exchange, side=parsed_side, quantity=parsed_quantity, # Original order quantity
                    order_type=parsed_order_type, status=OrderStatus.FILLED, price=parsed_price, # Original order price
                    trigger_price=parsed_trigger_price,
                    execution_id=exec_id, execution_time=event_time, execution_type="FILL",
                    cumulative_filled_quantity=filled_qty, # Cumulative filled for this order
                    average_price=avg_price, # Average fill price for this order
                    last_filled_quantity=filled_qty, # Assuming 'COMPLETE' means one fill event for the full quantity or remaining
                    last_filled_price=avg_price,     # Use average price as last fill price for simplicity here
                    commission=float(order_data.get('brkVal', 0.0)), # Example: if broker value is commission
                    timestamp=event_time,
                    event_type=EventType.EXECUTION, source=EventSource.MARKET_DATA_FEED
                )                
                self.event_manager.publish(exec_event)
                self.logger.info(f"Published FILLED ExecutionEvent for Broker Order ID: {broker_order_id}")
            
            elif api_status == 'OPEN' or 'PENDING' in api_status:
                # For OPEN or PENDING statuses, we might want to publish an ExecutionEvent
                # to confirm the order is now active or pending at the broker.
                current_status = OrderStatus.OPEN if api_status == 'OPEN' else OrderStatus.PENDING
                exec_type = "BROKER_OPEN" if current_status == OrderStatus.OPEN else "BROKER_PENDING"

                self.logger.info(f"Broker Order ID {broker_order_id} is {api_status}. Publishing {exec_type} ExecutionEvent.")
                exec_event = ExecutionEvent(
                    order_id=order_id, 
                    broker_order_id=broker_order_id, symbol=order_instrument_id,
                    exchange=parsed_exchange, side=parsed_side, quantity=parsed_quantity,
                    order_type=parsed_order_type, status=current_status, price=parsed_price,
                    trigger_price=parsed_trigger_price,
                    execution_id=exec_id, execution_time=event_time, execution_type=exec_type,
                    timestamp=event_time,
                    event_type=EventType.EXECUTION, source=EventSource.MARKET_DATA_FEED
                )                 
                self.event_manager.publish(exec_event)
                self.logger.info(f"Published OPEN or ExecutionEvent for Broker Order ID: {broker_order_id}")

            else:
                self.logger.debug(f"Processing other order update for Broker Order ID {broker_order_id}: {order_data}")

        except Exception as e:
            self.logger.error(f"Error processing order update from WebSocket: {e}", exc_info=True)
            self.logger.debug(f"Problematic order_data: {order_data}")
    
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
        
    def _attempt_reconnect(self):
        """Attempt to reconnect to the WebSocket feed."""
        if self._stop_reconnect_event.is_set():
            self.logger.info("Reconnection suppressed as feed is stopping.")
            return
            
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            self.logger.info("Reconnection already in progress.")
            return
            
        self._reconnect_thread = threading.Thread(target=self._reconnect_loop)
        self._reconnect_thread.daemon = True
        self._reconnect_thread.start()
    
    def _reconnect_loop(self):
        """Reconnection loop that attempts to reconnect with exponential backoff."""
        while not self._stop_reconnect_event.is_set() and self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            delay = min(30, self._reconnect_delay * (2 ** (self._reconnect_attempts - 1)))
            
            self.logger.info(f"Reconnection attempt {self._reconnect_attempts}/{self._max_reconnect_attempts} in {delay} seconds")
            
            # Wait for delay
            start_time = time.time()
            while time.time() - start_time < delay:
                if self._stop_reconnect_event.is_set():
                    self.logger.info("Reconnection aborted as feed is stopping.")
                    return
                time.sleep(0.1)
            
            # Check if session needs refresh
            if not self.is_authenticated or self._should_refresh_session():
                self.logger.info("Refreshing session before reconnection attempt")
                if not self._refresh_session():
                    self.logger.error("Session refresh failed. Continuing with reconnection attempt.")
            
            # Attempt to connect
            if self.connect():
                self.logger.info("Reconnection successful")
                return
                
            self.logger.warning(f"Reconnection attempt {self._reconnect_attempts} failed")
            
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            self.logger.error(f"Maximum reconnection attempts ({self._max_reconnect_attempts}) reached. Giving up.")
    
    def _should_refresh_session(self):
        """Check if session should be refreshed."""
        # For simplicity, always refresh on reconnection
        return True
    
    def _refresh_session(self):
        """
        Refresh the session token.
        
        If using session manager, refresh through it.
        Otherwise, re-authenticate directly.
        
        Returns:
            bool: True if refresh successful, False otherwise
        """
        try:
            # If using session manager, refresh through it
            if self._session_manager:
                self.logger.info("Attempting session refresh via Session Manager...")
                session = self._session_manager.refresh_session()
                if session and session.token:
                    # Update feed's credentials from the refreshed session
                    with self._auth_lock:
                        self.session_token = session.token
                        self.user_id = session.user_id
                        self.password = session.password
                        self.is_authenticated = True
                    self.logger.info("Session refreshed through session manager")
                    return True
                else:
                    self.logger.error("Failed to refresh session through session manager")
                    return False
            
            # Otherwise, re-authenticate directly
            self.logger.info("Attempting direct re-authentication...")
            return self.authenticate()
            
        except Exception as e:
            self.logger.error(f"Error refreshing session: {e}", exc_info=True)
            return False
    
class MarketDataFeed:
    """
    Factory class for creating market data feeds.
    
    This class creates and manages market data feeds based on the specified feed type.
    It has been updated to support shared session management with brokers when both
    are using the same provider (e.g., Finvasia).
    """
    
    def __init__(self, feed_type: str, broker=None, instruments: List[Instrument] = None, 
                 event_manager: EventManager = None, settings: Dict[str, Any] = None):
        """
        Initialize the market data feed.
        
        Args:
            feed_type: Type of feed to create (e.g., 'simulated', 'finvasia')
            broker: Broker instance for integration
            instruments: List of instruments to subscribe to
            event_manager: Event manager for publishing events
            settings: Additional settings for the feed
        """
        self.logger = get_logger("live.market_data_feed")
        self.feed_type = feed_type.lower()
        self.broker = broker
        self.instruments = instruments or []
        self.event_manager = event_manager
        self.settings = settings or {}
        self.feed = None      
        
        # Initialize subscribed symbols cache
        self._subscribed_symbols = set()       
        
    def _create_feed(self):
        """Create the appropriate feed based on feed_type."""
        try:
            if self.feed_type == 'simulated':
                self.feed = self._create_simulated_feed()
            elif 'finvasia' in self.feed_type:
                self.feed = self._create_finvasia_feed()
            else:
                self.logger.error(f"Unknown feed type: {self.feed_type}")
                raise ValueError(f"Unknown feed type: {self.feed_type}")
                
            self.logger.info(f"Created {self.feed_type} feed")
            
        except Exception as e:
            self.logger.error(f"Error creating feed: {e}")
            raise
            
    def _create_simulated_feed(self):
        """Create a simulated feed."""
        return SimulatedFeed(
            instruments=self.instruments,
            callback=self._on_market_data,
            event_manager=self.event_manager,
            **self.settings
        )
        
    def _create_finvasia_feed(self):
        """
        Create a Finvasia feed.
        
        If a session manager is provided in settings, use it for authentication.
        Otherwise, use credentials from settings or broker.
        """
        # Check if session manager is provided
        session_manager = self.settings.get('session_manager')
        if session_manager:
            self.logger.info("Using shared session manager for Finvasia feed")
            
            return FinvasiaFeed(
                instruments=self.instruments,
                callback=self._on_market_data,
                event_manager=self.event_manager,
                user_id=self.settings.get('user_id'),
                password=self.settings.get('password'),
                api_key=self.settings.get('api_key'),
                imei=self.settings.get('imei'),
                twofa=self.settings.get('twofa'),
                vc=self.settings.get('vc'),
                api_url=self.settings.get('api_url', 'https://api.shoonya.com/NorenWClientTP/'),
                ws_url=self.settings.get('websocket_url', 'wss://api.shoonya.com/NorenWSTP/'),
                debug=self.settings.get('debug', False),
                session_manager=session_manager
            )
        
        # Otherwise, use credentials from settings or broker
        user_id = self.settings.get('user_id') or (self.broker.user_id if hasattr(self.broker, 'user_id') else None)
        password = self.settings.get('password') or (self.broker.password if hasattr(self.broker, 'password') else None)
        api_key = self.settings.get('api_key') or (self.broker.api_key if hasattr(self.broker, 'api_key') else None)
        twofa = self.settings.get('twofa') or (self.broker.twofa if hasattr(self.broker, 'twofa') else None)
        vc = self.settings.get('vc') or (self.broker.vc if hasattr(self.broker, 'vc') else None)
        imei = self.settings.get('imei') or (self.broker.imei if hasattr(self.broker, 'imei') else None)
        api_url = self.settings.get('api_url') or (self.broker.api_url if hasattr(self.broker, 'api_url') else 'https://api.shoonya.com/NorenWClientTP/')
        ws_url = self.settings.get('websocket_url') or (self.broker.ws_url if hasattr(self.broker, 'ws_url') else 'wss://api.shoonya.com/NorenWSTP/')
        
        self.logger.info("Using direct authentication for Finvasia feed")
        
        return FinvasiaFeed(
            instruments=self.instruments,
            callback=self._on_market_data,
            event_manager=self.event_manager,
            user_id=user_id,
            password=password,
            api_key=api_key,
            imei=imei,
            twofa=twofa,
            vc=vc,
            api_url=api_url,
            ws_url=ws_url,
            debug=self.settings.get('debug', False)
        )
        
    def _on_market_data(self, event: MarketDataEvent):
        try:
            if isinstance(event, MarketDataEvent):
                data = event.data
                if MarketDataType.OHLC in data: event.data_type = MarketDataType.BAR
                elif MarketDataType.LAST_PRICE in data and (MarketDataType.BID in data or MarketDataType.ASK in data):
                    event.data_type = MarketDataType.QUOTE
                elif MarketDataType.LAST_PRICE in data: event.data_type = MarketDataType.TICK
            elif not (isinstance(event, OrderEvent) or isinstance(event, ExecutionEvent)):
                self.logger.warning(f"Received unexpected event type: {type(event)}")
                return

            if self.event_manager:
                self.event_manager.publish(event)
            else: 
                self.logger.warning("No EventManager in MarketDataFeed factory to publish event.")
        except Exception as e:
            self.logger.error(f"Error handling market data/order event: {e}", exc_info=True)
    
    def start(self):
        """Start the feed."""
        if self.feed:
            return self.feed.start()
        
        self._create_feed()
        if self.feed:
            return self.feed.start()

        return False
        
    def stop(self):
        """Stop the feed."""
        if self.feed:
            return self.feed.stop()
        return False
        
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

    def construct_trading_symbol(self, symbol: str, strike: float, option_type: str, 
                                 expiry_date_str: str, instrument_type_str: str, 
                                 exchange_str: str) -> str: 
        if not self.feed or not hasattr(self.feed, 'symbol_cache'):
            self.logger.error("Feed not initialized or does not support symbol construction (no symbol_cache).")
            try:
                dt_expiry = datetime.strptime(expiry_date_str, '%Y-%m-%d').strftime("%d%b%y").upper()
                return f"{symbol.upper()}{dt_expiry}{int(strike) if strike.is_integer() else strike}{option_type.upper()}"
            except Exception: return ""
        try:
            constructed_sym = self.feed.symbol_cache.construct_trading_symbol(
                name=symbol, expiry_date_str=expiry_date_str,
                instrument_type=instrument_type_str, option_type=option_type,
                strike=strike, exchange=exchange_str
            )
            if constructed_sym:
                #self.logger.info(f"Constructed option symbol via FinvasiaFeed cache: {constructed_sym}")
                return constructed_sym
            self.logger.warning(f"Option not found in FinvasiaFeed cache: {symbol} {expiry_date_str} {strike} {option_type}")
            return ""
        except Exception as e:
            self.logger.error(f"Error constructing option symbol via FinvasiaFeed: {e}", exc_info=True)
            return ""
            
    def get_symbol_info(self, trading_symbol: str, exchange: Union[str, Enum]) -> Optional[Dict[str, float]]:
        """
        Get symbol information including lotsize and tick_size.

        Args:
            trading_symbol: The trading symbol to lookup
            exchange: Exchange name (string or Enum)

        Returns:
            Dict with 'lotsize' and 'tick_size' keys, or None if error occurred
            Empty dict if symbol not found but no error
        """
        if not self.feed or not hasattr(self.feed, 'symbol_cache'):
            self.logger.error("Feed not initialized or does not support symbol lookup (no symbol_cache).")
            return None

        exchange_str = exchange.value if isinstance(exchange, Enum) else str(exchange)

        if not (hasattr(self.feed, 'symbol_cache') and self.feed.symbol_cache):
            self.logger.warning("Symbol cache not available in the current feed to retrieve symbol info.")
            return None

        try:
            symbol_info = self.feed.symbol_cache.lookup_symbol(trading_symbol, exchange_str.upper())

            if not symbol_info:
                self.logger.warning(f"Symbol info not found in cache for: {trading_symbol} on exchange: {exchange_str}")
                return {}

            result = {}

            # Handle lotsize
            lotsize_val = symbol_info.get('lotsize')
            if lotsize_val is not None and lotsize_val != '':
                try:
                    result['lotsize'] = float(lotsize_val)
                    # self.logger.debug(f"Retrieved lot size for {trading_symbol}: {result['lotsize']}")
                except ValueError:
                    self.logger.error(f"Could not convert lot size '{lotsize_val}' to float for {trading_symbol}")
                    result['lotsize'] = 1.0  # Default fallback
            else:
                self.logger.warning(f"Lot size is None or empty for {trading_symbol}. Using default.")
                result['lotsize'] = 1.0

            # Handle tick_size
            tick_size_val = symbol_info.get('tick_size') or symbol_info.get('ticksize')  # Handle both possible keys
            if tick_size_val is not None and tick_size_val != '':
                try:
                    result['ticksize'] = float(tick_size_val)
                    # self.logger.debug(f"Retrieved tick size for {trading_symbol}: {result['ticksize']}")
                except ValueError:
                    self.logger.error(f"Could not convert tick size '{tick_size_val}' to float for {trading_symbol}")
                    result['ticksize'] = 0.01  # Default tick size for Indian markets
            else:
                self.logger.warning(f"Tick size is None or empty for {trading_symbol}. Using default.")
                result['ticksize'] = 0.01  # Default tick size

            return result
        
        except Exception as e:
            self.logger.error(f"Error getting symbol info for {trading_symbol} from cache: {e}", exc_info=True)
            return None
    
    def get_lotsize(self, trading_symbol: str, exchange: Union[str, Enum]) -> Optional[float]:
        if not self.feed or not hasattr(self.feed, 'symbol_cache'):
            self.logger.error("Feed not initialized or does not support get_expiry_dates (no symbol_cache).")
            return []
        exchange_str = exchange.value if isinstance(exchange, Enum) else str(exchange)
        if hasattr(self.feed, 'symbol_cache') and self.feed.symbol_cache:
            try:
                symbol_info = self.feed.symbol_cache.lookup_symbol(trading_symbol, exchange_str.upper())
                if symbol_info and 'lotsize' in symbol_info:
                    lotsize_val = symbol_info['lotsize']
                    if lotsize_val is not None and lotsize_val != '': # Check for None or empty string
                        try:
                            lotsize_float = float(lotsize_val)
                            self.logger.debug(f"Retrieved lot size for {trading_symbol} on {exchange_str}: {lotsize_float}")
                            return lotsize_float
                        except ValueError:
                            self.logger.error(f"Could not convert lot size '{lotsize_val}' to float for {trading_symbol}")
                            return None # Indicates conversion error
                    else:
                        self.logger.warning(f"Lot size is None or empty for {trading_symbol} in cache.")
                        return 1.0 # Default to 1.0 if lotsize is explicitly None or empty in cache
                else:
                    self.logger.warning(f"Lot size not found in cache for symbol: {trading_symbol} on exchange: {exchange_str}. Defaulting to 1.0.")
                    return 1.0 # Default to 1.0 if not found
            except Exception as e:
                self.logger.error(f"Error getting lot size for {trading_symbol} from cache: {e}", exc_info=True)
                return None # Indicates error during lookup
        else:
            self.logger.warning("Symbol cache not available in the current feed to retrieve lot size. Cannot determine lot size.")
            return None # Indicates cache not available
        
    def get_expiry_dates(self, symbol: str) -> List[date]: 
        if not self.feed or not hasattr(self.feed, 'symbol_cache'):
            self.logger.error("Feed not initialized or does not support get_expiry_dates (no symbol_cache).")
            return []
        try:
            expiry_dates = self.feed.symbol_cache.get_combined_expiry_dates(symbol)
            if expiry_dates: return sorted(expiry_dates)
            self.logger.warning(f"No expiry dates found for {symbol} in FinvasiaFeed cache.")
            return []
        except Exception as e:
            self.logger.error(f"Error getting expiry dates via FinvasiaFeed: {e}", exc_info=True)
            return []
            
    def subscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        if not self.feed:
            self.logger.error("Feed not initialized, cannot subscribe to instruments")
            return {inst.instrument_id: False for inst in instruments}
        results = {}
        for instrument_obj in instruments:
            results[instrument_obj.instrument_id] = self.subscribe(instrument_obj) 
        success_count = sum(1 for v in results.values() if v)
        self.logger.info(f"Subscription request summary: {success_count}/{len(instruments)} successful.")
        return results
    
    def unsubscribe_symbols(self, instruments: List[Instrument]) -> Dict[str, bool]:
        if not self.feed:
            self.logger.error("Feed not initialized, cannot unsubscribe instruments")
            return {inst.instrument_id: False for inst in instruments}
        results = {}
        for instrument_obj in instruments:
            results[instrument_obj.instrument_id] = self.unsubscribe(instrument_obj) 
        success_count = sum(1 for v in results.values() if v)
        self.logger.info(f"Unsubscription request summary: {success_count}/{len(instruments)} successful.")
        return results

