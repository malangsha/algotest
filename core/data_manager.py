import os
import json
import pandas as pd
import numpy as np
import threading
import time
import queue
import pickle
import hashlib
import concurrent

from typing import Dict, List, Optional, Any, Set, Tuple, Union
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from collections import defaultdict
from enum import Enum

from utils.constants import MarketDataType, Exchange, InstrumentType, SYMBOL_MAPPINGS
from utils.timeframe_manager import TimeframeManager
from models.instrument import Instrument, AssetClass
from models.events import Event, EventType, MarketDataEvent, BarEvent
from core.event_manager import EventManager
from core.logging_manager import get_logger
from utils.greeks_calculator import OptionsGreeksCalculator

class DataManager:
    """
    Central manager for all market data needs across the framework.
    Stores, processes, and provides access to market data across different timeframes.
    Optimized for low latency and high throughput.
    """

    def __init__(self, config: Dict[str, Any], event_manager: EventManager, broker: Any):
        """
        Initialize the Market Data Manager with enhanced performance features.

        Args:
            config: Configuration dictionary
            event_manager: Event Manager instance for publishing/subscribing
            broker: Broker instance for market data
        """
        self.logger = get_logger("core.data_manager")
        self.config = config
        self.event_manager = event_manager
        self.broker = broker
        
        # Market data feed reference
        self.market_data_feed = None

        # Extract configuration
        data_config = config.get('market_data', {})
        self.persistence_enabled = data_config.get('persistence', {}).get('enabled', True)  # Default to True
        self.persistence_path = data_config.get('persistence', {}).get('path', './data')
        self.persistence_interval = data_config.get('persistence', {}).get('interval', 60)  # Reduced to 60 seconds
        self.use_cache = data_config.get('use_cache', True)
        self.cache_limit = data_config.get('cache_limit', 10000)  # per symbol
        self.tfm_cache_limit = data_config.get('timeframe_manager_cache_limit', 10000)
        self.use_redis = data_config.get('use_redis', False)
        self.redis_client = None
        
        # Low latency optimizations
        self.thread_pool_size = data_config.get('thread_pool_size', 4)
        self.processing_queue_size = data_config.get('processing_queue_size', 10000)
        self.batch_processing = data_config.get('batch_processing', True)
        self.batch_size = data_config.get('batch_size', 200)
        
        # Greek calculation configuration
        self.calculate_greeks = data_config.get('calculate_greeks', True)
        self.greeks_risk_free_rate = data_config.get('greeks_risk_free_rate', 0.05)
        self.default_volatility = data_config.get('default_volatility', 0.20) 
        
        # Enhanced data structures for tick, 1m and other timeframe data
        # Using dictionaries for O(1) lookups instead of lists where possible
        self.data_store = {}  # Symbol -> Dictionary of market data points with timestamp as key
        self.last_data = {}   # Symbol -> Latest market data
        self.ohlc_data = {}   # Symbol -> OHLC data frames
        
        # In-memory caches for performance
        self.tick_cache = {}  # Symbol -> Recent ticks (circular buffer)
        self.tick_cache_size = 1000  # Maximum ticks to cache per symbol

        # Separate storage for 1-minute data for efficient persistence
        self.one_minute_data = {}  # Symbol -> DataFrame of 1-minute bars with Greeks and OI       
             
        # Initialize storage
        self.one_minute_data = {}  # Symbol -> DataFrame
        self.last_tick_data = {}  # Symbol -> Dict
        self.data_locks = {}  # Symbol -> Lock
        self.global_lock = threading.RLock()
        
        # --- Threading & Concurrency ---
        self.data_locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)
        self.global_lock = threading.RLock()
        self.tick_queue = queue.Queue(maxsize=self.processing_queue_size)
        self._shutdown_event = threading.Event()
        self.persistence_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='PersistenceWorker')
        self.tick_processing_executor = ThreadPoolExecutor(max_workers=self.thread_pool_size, thread_name_prefix='TickProcessor')
        
        # Store instrument objects for easy lookup
        self.instruments: Dict[str, Instrument] = {} # symbol_key -> Instrument
        # Store the *converted* tick data (using MarketDataType keys)
        self.last_tick_data: Dict[str, Dict[str, Any]] = {} # symbol_key -> latest converted tick dict
        # Store last cumulative volume for calculating tick volume from feeds like Finvasia
        self.last_cumulative_volume: Dict[str, int] = {} # symbol_key -> last value of cumulative volume field
        
        # Initialize timeframe management
        self.timeframe_manager = TimeframeManager(max_bars_in_memory=self.tfm_cache_limit)
        self.timeframe_subscriptions = {}
        for timeframe in TimeframeManager.VALID_TIMEFRAMES:
            self.timeframe_subscriptions[timeframe] = {}
            
        # {symbol_key: {timeframe: set(strategy_ids)}}
        self.symbol_subscribers: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

        # Persistence mechanism with improved threading
        self._persistence_thread = None
        self.last_persistence_time = {}  # Track last persistence by symbol
        
        if self.persistence_enabled:
            self._setup_persistence()
            
        # schedular thread
        self._scheduler_thread = None    

        # Redis client if enabled
        self.setup_redis(data_config)

        # Register with Event Manager
        self._register_event_handlers()
        
        # Start tick processing thread
        self.tick_processor_thread = threading.Thread(target=self._process_tick_queue, daemon=True)
        self.tick_processor_thread.start()
        
        # Track active subscriptions
        self.active_subscriptions: Set[str] = set() 
        
        self.logger.info(f"DataManager initialized. Persistence: {self.persistence_enabled}. TFM Cache: {self.tfm_cache_limit} bars.")
        
    def _setup_persistence(self):
        """Setup the persistence directory structure and mechanism"""
        try:
            # Create main data directory
            os.makedirs(self.persistence_path, exist_ok=True)
            os.makedirs(os.path.join(self.persistence_path, 'bars'), exist_ok=True)
            
            # Start persistence thread
            self._persistence_thread = threading.Thread(target=self._persistence_loop, daemon=True)
            self._persistence_thread.start()
            
            self.logger.info(f"Persistence setup complete at {self.persistence_path}")
        except Exception as e:
            self.logger.error(f"Error setting up persistence: {str(e)}")
            self.persistence_enabled = False

    def setup_redis(self, data_config):
        """Setup Redis connection if enabled"""
        self.redis_client = None
        if self.use_redis:
            try:
                import redis
                redis_config = data_config.get('redis', {})
                self.redis_client = redis.Redis(
                    host=redis_config.get('host', 'localhost'),
                    port=redis_config.get('port', 6379),
                    db=redis_config.get('db', 0),
                    decode_responses=True,  # Always decode for easier processing
                    socket_timeout=1.0,  # Low timeout for fast failure detection
                    socket_connect_timeout=1.0,
                    health_check_interval=30
                )
                self.logger.info("Redis connection established")
            except ImportError:
                self.logger.error("Redis package not installed. Please install 'redis' package.")
                self.use_redis = False
            except Exception as e:
                self.logger.error(f"Failed to connect to Redis: {str(e)}")
                self.use_redis = False

    def _register_event_handlers(self):
        """Register to receive market data events from the event manager."""
        # Register for market data events
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._on_market_data,
            component_name="DataManager"
        )
        
        # Subscribe to system events
        self.event_manager.subscribe(
            EventType.SYSTEM,
            self._handle_system_event,
            component_name="DataManager"
        )

        # Subscribe to custom events
        self.event_manager.subscribe(
            EventType.CUSTOM,
            self._handle_custom_event,
            component_name="DataManager"
        )      
          
        self.logger.info("Registered for market data and timeframe events with Event Manager")

    def _get_lock_for_symbol(self, symbol: str) -> threading.RLock:
        """Get a lock specific to a symbol to minimize contention"""
        with self.global_lock:
            if symbol not in self.data_locks:
                self.data_locks[symbol] = threading.RLock()
            return self.data_locks[symbol]

    def _get_symbol_key(self, instrument: Instrument) -> str:
        """ Creates a unique key for an instrument (e.g., 'NSE:RELIANCE') """
        if not instrument:
            self.logger.error("Cannot generate symbol key: instrument is None")
            return "INVALID:INSTRUMENT"
        
        if not hasattr(instrument, 'symbol') or not instrument.symbol:
            self.logger.error(f"Cannot generate symbol key: instrument has no symbol attribute or empty symbol")
            return "INVALID:NOSYMBOL"
        
        if not hasattr(instrument, 'exchange') or not instrument.exchange:
            self.logger.error(f"Cannot generate symbol key: {instrument.symbol} has no exchange attribute or empty exchange")
            return f"INVALID:{instrument.symbol}"
        
        # Ensure exchange is a string value
        exchange_val = instrument.exchange.value if isinstance(instrument.exchange, Enum) else str(instrument.exchange)
        return f"{exchange_val}:{instrument.symbol}"
    
    def _on_market_data(self, event: Event):
        """
        Handle incoming market data events by placing them in a queue for processing.
        This reduces blocking time in the event handler.

        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent):
            self.logger.warning(f"Received non-market data event: {type(event)}")
            return
        
        # Store instrument object if not already stored
        instrument = event.instrument
        if not instrument:
            self.logger.warning(f"Received market data event without valid instrument: {event}")
            return
        
        symbol_key = self._get_symbol_key(instrument)
        if symbol_key.startswith("INVALID:"):
            self.logger.warning(f"Cannot process market data event with invalid symbol key {symbol_key}")
            return
        
        if symbol_key not in self.instruments:
            with self.global_lock:
                # Double check after acquiring lock
                if symbol_key not in self.instruments:
                    self.instruments[symbol_key] = event.instrument

        # Add symbol_key to data for downstream convenience
        # This might be redundant if the key is already derived from instrument,
        # but ensures it's present in the dict passed around.
        event.data['symbol_key'] = symbol_key

        try:
            # Put event in queue for processing
            self.tick_queue.put(event, block=False)
        except queue.Full:
            self.logger.warning("Tick processing queue full, dropping market data event")

    def _process_tick_queue(self):
        """Continuously processes market data events from the queue."""
        self.logger.info("Tick processing thread started.")
        batch = []
        last_batch_time = time.monotonic()
        while not self._shutdown_event.is_set():
            try:
                while len(batch) < self.batch_size:
                    try:
                        event = self.tick_queue.get_nowait()
                        if event is None: self._shutdown_event.set(); break
                        batch.append(event)
                        self.tick_queue.task_done()
                    except queue.Empty: break
                if self._shutdown_event.is_set(): break
                if batch:
                    self._process_market_data_batch(batch)
                    batch = []
                    last_batch_time = time.monotonic()
                else: time.sleep(0.001) # Prevent busy-waiting
            except Exception as e:
                self.logger.error(f"Error in tick processing loop: {e}", exc_info=True)
                batch = []
                time.sleep(0.1)
        # Process remaining after shutdown
        if batch: self._process_market_data_batch(batch)
        while not self.tick_queue.empty():
             try:
                  event = self.tick_queue.get_nowait()
                  if event and event.data.get('symbol_key'): self._process_market_data_batch([event])
                  self.tick_queue.task_done()
             except queue.Empty: break
             except Exception as e: self.logger.error(f"Error processing final ticks: {e}")
        self.logger.info("Tick processing thread stopped.")
    
    def _process_market_data_batch(self, batch: List[MarketDataEvent]):
        """Processes a batch of market data events, grouping by symbol."""
        symbol_event_groups = defaultdict(list)
        for event in batch:
            if not event.instrument:
                self.logger.warning(f"Received market data event without instrument: {event}")
                continue
            symbol_key = event.data.get('symbol_key') # Key should be added in _on_market_data
            if symbol_key: symbol_event_groups[symbol_key].append(event)
        
        # If timeout is consistently a problem, consider making it configurable
        timeout = self.config.get('symbol_processing_timeout', 3.0)  # Increased from 1.0
        futures = [self.tick_processing_executor.submit(self._process_symbol_events, key, events)
                   for key, events in symbol_event_groups.items()]
        for future in futures:
            try:
                future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                self.logger.error(f"Symbol processing timed out after {timeout}s")
            except Exception as e:
                # Log the full exception details with traceback
                self.logger.error(f"Error processing symbol events batch future: {str(e)}", exc_info=True)

    
    def _process_symbol_events(self, symbol_key: str, events: List[MarketDataEvent]):
        """Processes all events for a single symbol sequentially."""
        with self.data_locks[symbol_key]: # Lock specific to this symbol
            for event in events:
                try:
                    self._process_single_market_data_event(symbol_key, event)
                except Exception as e:
                    self.logger.error(f"Error processing single event for {symbol_key}: {e}", exc_info=True)
    
    def _process_single_market_data_event(self, symbol_key: str, event: MarketDataEvent):
        """
        Processes one market data event:
        1. Calculates tick volume from cumulative volume if necessary.
        2. Updates last tick data store.
        3. Passes standardized tick to TimeframeManager.
        4. Handles completed bars (Greeks calculation, event publishing).
        Assumes event.data already contains converted data using MarketDataType keys.
        """
        try:
            instrument = self.instruments.get(symbol_key)
            if not instrument: return

            converted_tick_data = event.data
            
            # Store the converted tick data
            self.last_tick_data[symbol_key] = converted_tick_data

            # --- Calculate Tick Volume from Cumulative (if applicable) ---
            # The feed handler (_convert_tick_to_market_data) puts cumulative volume
            # into MarketDataType.VOLUME.value. We need to calculate the delta here.
            tick_for_tfm = converted_tick_data.copy() # Create a copy to modify for TFM
            current_cumulative_v = converted_tick_data.get(MarketDataType.VOLUME.value)
            tick_volume = 0.0 # Default volume
            
            if current_cumulative_v is not None:
                try:
                    current_v_int = int(current_cumulative_v)
                    last_v_int = self.last_cumulative_volume.get(symbol_key, 0)

                    if current_v_int >= last_v_int: # Normal case or first tick
                        tick_volume = float(current_v_int - last_v_int)
                    else: # Volume reset detected
                        self.logger.warning(f"Cumulative volume reset detected for {symbol_key}. Current: {current_v_int}, Last: {last_v_int}. Using current as tick volume.")
                        tick_volume = float(current_v_int)

                    self.last_cumulative_volume[symbol_key] = current_v_int
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Could not process cumulative volume for {symbol_key}: {e}. Raw: {current_cumulative_v}. Using volume 0.")
                    tick_volume = 0.0
            
            # Update the dictionary being passed to TimeframeManager with the calculated tick volume
            tick_for_tfm[MarketDataType.VOLUME.value] = tick_volume

            # --- Tick Processing via TimeframeManager ---
            # Ensure necessary fields for TimeframeManager are present
            if MarketDataType.LAST_PRICE.value in tick_for_tfm and MarketDataType.TIMESTAMP.value in tick_for_tfm:
                try:
                    # Pass the modified tick data (with calculated volume) to TimeframeManager
                    completed_bars_info = self.timeframe_manager.process_tick(symbol_key, tick_for_tfm)

                    # --- Handle Completed Bars ---
                    for timeframe, is_completed in completed_bars_info.items():
                        if is_completed:
                            # Retrieve the newly completed bar
                            bars_df = self.timeframe_manager.get_bars(symbol_key, timeframe, limit=1)
                            if bars_df is not None and not bars_df.empty:
                                # Convert the bar row to a dictionary
                                bar_data = bars_df.iloc[-1].to_dict()
                                greeks_data = None
                                implied_vol = None

                                # Calculate Greeks if applicable
                                if self.calculate_greeks and instrument.instrument_type == InstrumentType.OPTION:
                                    greeks_result = self._calculate_option_greeks_for_bar(instrument, bar_data)
                                    if greeks_result:
                                        greeks_data, implied_vol = greeks_result
                                        # Add greeks and IV to the bar dictionary itself for BarEvent
                                        bar_data.update(greeks_data)
                                        bar_data['implied_volatility'] = implied_vol

                                self.logger.debug(f"bar_data: {bar_data}")
                                # --- Publish Standard BarEvent ---
                                self._publish_bar_event(
                                    symbol_key=symbol_key,
                                    timeframe=timeframe,
                                    bar_data=bar_data, # Contains OHLCVOI + Greeks/IV if calculated
                                    instrument=instrument
                                )

                except Exception as e:
                     self.logger.error(f"Error processing tick via TimeframeManager for {symbol_key}: {e}. Tick for TFM: {tick_for_tfm}", exc_info=True)
            else:
                self.logger.warning(f"Skipping tick for {symbol_key} due to missing price or timestamp in converted data: {tick_for_tfm}")
        except Exception as e:
            self.logger.error(f"Error processing market data for {symbol_key}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def _publish_bar_event(self, symbol_key: str, timeframe: str, bar_data: Dict[str, Any],
                           instrument: Instrument):
        """
        Checks subscriptions and publishes a standard BarEvent for a completed bar.
        (Replaces _publish_timeframe_bar_event)

        Args:
            symbol_key: The unique symbol key (e.g., 'NSE:RELIANCE').
            timeframe: The timeframe of the completed bar (e.g., '1m', '5m').
            bar_data: The completed bar data as a dictionary (OHLCVOI + optional Greeks/IV).
            instrument: The Instrument object corresponding to the symbol_key.
        """
        # Check if any strategy is subscribed to this specific symbol and timeframe
        subscribers = self.symbol_subscribers.get(symbol_key, {}).get(timeframe)

        if subscribers:
            # Extract Greeks and OI from bar_data if they exist (they were added during calculation)
            greeks = {k: bar_data.get(k) for k in ['delta', 'gamma', 'theta', 'vega', 'rho'] if k in bar_data}
            open_interest = bar_data.get('open_interest')
            implied_vol = bar_data.get('implied_volatility')

            try:
                # Ensure timestamp is an integer if required by event definition
                bar_timestamp = int(bar_data['timestamp'])

                # Create the standard BarEvent
                bar_event = BarEvent(
                    event_type=EventType.BAR,
                    timestamp=bar_timestamp,
                    instrument=instrument,
                    timeframe=timeframe,
                    # Pass individual OHLCV values to BarEvent constructor
                    open_price=bar_data.get('open', 0.0),
                    high_price=bar_data.get('high', 0.0),
                    low_price=bar_data.get('low', 0.0),
                    close_price=bar_data.get('close', 0.0),
                    volume=bar_data.get('volume', 0.0),
                    bar_start_time=bar_timestamp, # Use bar timestamp as start time
                    # Pass optional attributes separately if needed by BarEvent definition
                    # (Assuming BarEvent doesn't automatically take greeks/oi/iv from a dict)
                    # greeks=greeks if any(v is not None for v in greeks.values()) else None,
                    # open_interest=open_interest,
                    # implied_volatility=implied_vol
                )
                # Add greeks and OI to the event object directly if BarEvent supports it
                if hasattr(bar_event, 'greeks'):
                    bar_event.greeks = greeks if any(v is not None for v in greeks.values()) else None
                if hasattr(bar_event, 'open_interest'):
                     bar_event.open_interest = open_interest
                if hasattr(bar_event, 'implied_volatility'):
                     bar_event.implied_volatility = implied_vol
                # Alternatively, could add them to a generic 'data' field if BarEvent has one


                # Publish the event - EventManager routes it
                self.event_manager.publish(bar_event)
                # self.logger.debug(f"Published BarEvent for {symbol_key} @ {timeframe} to {len(subscribers)} subscribers.")

            except KeyError as ke:
                 self.logger.error(f"Missing key in bar_data for {symbol_key}@{timeframe}: {ke}. Bar Data: {bar_data}")
            except Exception as e:
                 self.logger.error(f"Error creating/publishing BarEvent for {symbol_key}@{timeframe}: {e}", exc_info=True)

    def _calculate_option_greeks_for_bar(self, instrument: Instrument, bar_data: Dict[str, Any]) -> Optional[Tuple[Dict[str, float], float]]:
        """Calculate option Greeks using data from a completed bar."""
        if not self.calculate_greeks or not isinstance(instrument, Instrument) or instrument.instrument_type != InstrumentType.OPTION:
            return None
        
        if instrument.symbol in SYMBOL_MAPPINGS:
            self.logger.debug(f"Skipping greek calculation for: {instrument.symbol}")
            return None
        
        self.logger.debug(f"Instrument: {instrument}")
        try:
            option_price = float(bar_data['close'])
            timestamp = float(bar_data['timestamp'])
            
            # 1. Get Underlying Price
            symbol_key = getattr(instrument, 'symbol_key', None)
            if not symbol_key:
                 symbol_key = getattr(instrument, 'instrument_id', None)
                 if not symbol_key:
                    instrument_sym = getattr(instrument, 'symbol', None)
                    if instrument_sym and instrument.exchange:
                        exch = instrument.exchange.value if isinstance(instrument.exchange, Enum) else str(instrument.exchange)
                        symbol_key = f"{exch}:{instrument_sym}"
                    else: self.logger.warning(f"Cannot determine underlying for option {instrument.symbol}"); return None
            underlying_price = self._get_last_price(symbol_key) # Uses converted data
            if underlying_price is None: self.logger.warning(f"No underlying price for {symbol_key}"); return None

            # 2. Get Time to Expiry
            expiry_dt = getattr(instrument, 'expiry_date', None) # Expecting datetime object
            if not isinstance(expiry_dt, datetime):
                 if isinstance(expiry_dt, str): # Attempt parsing common formats
                      for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%Y-%m-%dT%H:%M:%S'):
                           try: expiry_dt = datetime.strptime(expiry_dt, fmt); break
                           except ValueError: pass
                      else: self.logger.error(f"Cannot parse expiry date '{expiry_dt}' for {instrument.symbol}"); return None
                 else: self.logger.error(f"Invalid expiry date type ({type(expiry_dt)}) for {instrument.symbol}"); return None

            current_dt = datetime.fromtimestamp(timestamp)
            time_to_expiry_years = max(0, (expiry_dt - current_dt).total_seconds() / (365.25 * 86400))

            # 3. Get Strike Price
            strike_price = getattr(instrument, 'strike', None)
            if strike_price is None: self.logger.error(f"Missing strike for option {instrument.symbol}"); return None
            strike_price = float(strike_price)

            # 4. Volatility (Use default or implement IV calculation)
            # TODO: Implement Implied Volatility calculation
            volatility = self.default_volatility

            # 5. Risk-Free Rate
            risk_free_rate = self.greeks_risk_free_rate

            # 6. Call Calculation Function (from greeks_calculator.py)
            # Assuming calculate_greeks returns dict or raises error
            greeks_dict = OptionsGreeksCalculator.calculate_greeks(
                option_type=instrument.option_type,
                underlying_price=underlying_price,
                strike_price=strike_price,
                time_to_expiry=time_to_expiry_years,
                risk_free_rate=risk_free_rate,
                volatility=volatility # Pass default/historical vol as IV guess
            )
            # Assuming IV calculation would happen inside or alongside calculate_greeks
            implied_vol = volatility # Placeholder: Use input vol as IV for now

            if greeks_dict:
                 greeks_dict['time_to_expiry_years'] = round(time_to_expiry_years, 6)
                 return greeks_dict, implied_vol # Return tuple (greeks, iv)
            else:
                 self.logger.warning(f"Greeks calculation failed for {instrument.symbol}")
                 return None

        except Exception as e:
            self.logger.error(f"Error calculating greeks for {instrument.symbol} bar: {e}", exc_info=True)
            return None

    def _get_last_price(self, symbol_key: str) -> Optional[float]:
        """Gets the last known price from last_tick_data (converted)."""
        converted_tick = self.last_tick_data.get(symbol_key)
        if converted_tick:
            # Use MarketDataType enum value as the key
            price = converted_tick.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                try: return float(price)
                except (ValueError, TypeError): pass # Logged elsewhere if conversion fails
        return None

    # --- Persistence ---
    def _persistence_loop(self):
        """Background thread for periodic data persistence."""
        self.logger.info("Persistence loop started.")
        while not self._shutdown_event.wait(self.persistence_interval):
            try:
                symbols_to_persist = list(self.instruments.keys())
                if not symbols_to_persist: continue
                self.logger.debug(f"Starting periodic persistence check for {len(symbols_to_persist)} symbols...")
                start_time = time.monotonic()
                futures = []
                now = time.time()
                for symbol_key in symbols_to_persist:
                    if now - self.last_persistence_time.get(symbol_key, 0) >= self.persistence_interval:
                         # Primarily persist 1m data, adjust if others needed
                         future = self.persistence_executor.submit(self._persist_symbol_data, symbol_key, '1m')
                         futures.append(future)
                         self.last_persistence_time[symbol_key] = now
                submitted_count = len(futures)
                for future in futures:
                    try: future.result(timeout=60)
                    except Exception as e: self.logger.error(f"Persistence task error: {e}", exc_info=True)
                elapsed = time.monotonic() - start_time
                if submitted_count > 0:
                     self.logger.info(f"Persistence cycle completed in {elapsed:.2f}s for {submitted_count} tasks.")
            except Exception as e:
                self.logger.error(f"Error in persistence loop: {e}", exc_info=True)
                time.sleep(60)
        self.logger.info("Persistence loop stopped.")

    def _persist_symbol_data(self, symbol_key: str, timeframe: str):
        """Persists bar data for a symbol/timeframe from TimeframeManager."""
        if not self.persistence_enabled: return
        try:
            df = self.timeframe_manager.get_bars(symbol_key, timeframe) # Gets cached/buffered data
            if df is None or df.empty: return

            df = df.reindex(columns=TimeframeManager.BAR_COLUMNS).copy()
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            parts = symbol_key.split(':', 1)
            exchange = parts[0] if len(parts) > 1 else 'UNKNOWN_EX'
            pure_symbol = parts[1] if len(parts) > 1 else symbol_key
            safe_symbol = "".join(c if c.isalnum() else "_" for c in pure_symbol)

            date_groups = df.groupby(df['datetime'].dt.date)
            saved_count = 0
            for date, date_df in date_groups:
                date_str = date.strftime('%Y-%m-%d')
                persist_dir = os.path.join(self.persistence_path, 'bars', date_str, exchange, timeframe)
                os.makedirs(persist_dir, exist_ok=True)
                file_path = os.path.join(persist_dir, f"{safe_symbol}.parquet")
                # Use timestamp as index for saving
                save_df = date_df.set_index('timestamp').drop(columns=['datetime'], errors='ignore')
                try:
                    if os.path.exists(file_path):
                        existing_df = pd.read_parquet(file_path)
                        combined_df = pd.concat([existing_df, save_df])
                        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                        combined_df.sort_index(inplace=True)
                        combined_df.to_parquet(file_path, index=True)
                    else:
                        save_df.to_parquet(file_path, index=True)
                    saved_count += len(save_df)
                except Exception as e: self.logger.error(f"Error saving parquet {file_path}: {e}")
            # if saved_count > 0: self.logger.debug(f"Persisted {saved_count} {timeframe} bars for {symbol_key}.")
        except Exception as e: self.logger.error(f"Error persisting {timeframe} data for {symbol_key}: {e}", exc_info=True)
 
    def _store_in_redis(self, key: str, data: Dict, timestamp: float):
        """
        Store market data in Redis for real-time access.
        
        Args:
            key: Symbol key
            data: Market data to store
            timestamp: Event timestamp
        """
        try:
            if not self.redis_client:
                return
                
            # Store latest tick data
            latest_key = f"md:latest:{key}"
            self.redis_client.hset(latest_key, mapping={
                'timestamp': timestamp,
                'data': json.dumps(data)
            })
            
            # For efficiency, expire this key after 1 day
            self.redis_client.expire(latest_key, 86400)
            
            # If OHLC data, store in a separate structure
            if MarketDataType.OHLC.value in data:
                ohlc = data[MarketDataType.OHLC.value]
                timeframe = ohlc.get('timeframe', '1m')
                
                # Store in sorted set for time-based lookup
                ohlc_key = f"md:ohlc:{key}:{timeframe}"
                ohlc_data = json.dumps(ohlc)
                self.redis_client.zadd(ohlc_key, {ohlc_data: timestamp})
                
                # Trim to keep only recent data
                self.redis_client.zremrangebyrank(ohlc_key, 0, -self.cache_limit-1)
                self.redis_client.expire(ohlc_key, 86400)
                
        except Exception as e:
            self.logger.error(f"Error storing data in Redis: {str(e)}")
            # Disable Redis if error persists
            if hasattr(self, '_redis_errors'):
                self._redis_errors += 1
                if self._redis_errors > 10:
                    self.logger.error("Too many Redis errors, disabling Redis")
                    self.use_redis = False
            else:
                self._redis_errors = 1

    # --- Data Retrieval ---
    def get_historical_data(self, symbol_key: str, timeframe: str,
                            n_bars: Optional[int] = None,
                            start_time: Optional[Union[datetime, float]] = None,
                            end_time: Optional[Union[datetime, float]] = None) -> Optional[pd.DataFrame]:
        """Get historical bars, checking memory then persistence."""
        df = self.timeframe_manager.get_bars(symbol_key, timeframe)
        needs_persistence_load = False
        start_ts = start_time.timestamp() if isinstance(start_time, datetime) else float(start_time) if start_time else None

        if df is None: needs_persistence_load = True
        elif start_ts and not df.empty and df.index.min() > start_ts: needs_persistence_load = True
        # Add check if n_bars requires more data than available in memory
        elif n_bars is not None and n_bars > 0 and (df is None or len(df) < n_bars):
             needs_persistence_load = True


        if needs_persistence_load and self.persistence_enabled:
             persistent_df = self._load_historical_data_from_storage(symbol_key, timeframe, start_time, end_time)
             if persistent_df is not None:
                  if df is not None:
                       # Combine, ensuring consistent columns (preferring TFM columns)
                       combined_df = pd.concat([persistent_df.reindex(columns=df.columns), df])
                       df = combined_df[~combined_df.index.duplicated(keep='last')].sort_index()
                  else: df = persistent_df

        if df is None or df.empty: return None
        if not df.index.is_monotonic_increasing: df.sort_index(inplace=True)

        # Apply filters
        if start_ts: df = df[df.index >= start_ts]
        if end_time:
            end_ts = end_time.timestamp() if isinstance(end_time, datetime) else float(end_time)
            df = df[df.index <= end_ts]
        if n_bars is not None and n_bars > 0: df = df.iloc[-n_bars:]
        elif n_bars == 0: return pd.DataFrame(columns=df.columns)

        return df.copy() if not df.empty else None

    def _load_historical_data_from_storage(self, symbol_key: str, timeframe: str,
                                           start_time: Optional[Union[datetime, float]] = None,
                                           end_time: Optional[Union[datetime, float]] = None) -> Optional[pd.DataFrame]:
        """Loads historical data from persistent parquet files."""
        if not self.persistence_enabled: return None
        dfs = []
        try:
            end_dt = datetime.fromtimestamp(end_time) if isinstance(end_time, (int, float)) else end_time if isinstance(end_time, datetime) else datetime.now()
            start_dt = datetime.fromtimestamp(start_time) if isinstance(start_time, (int, float)) else start_time if isinstance(start_time, datetime) else end_dt - timedelta(days=30) # Default lookback

            parts = symbol_key.split(':', 1)
            exchange = parts[0] if len(parts) > 1 else 'UNKNOWN_EX'
            pure_symbol = parts[1] if len(parts) > 1 else symbol_key
            safe_symbol = "".join(c if c.isalnum() else "_" for c in pure_symbol)

            current_date = start_dt.date()
            while current_date <= end_dt.date():
                date_str = current_date.strftime('%Y-%m-%d')
                file_path = os.path.join(self.persistence_path, 'bars', date_str, exchange, timeframe, f"{safe_symbol}.parquet")
                if os.path.exists(file_path):
                    try:
                        daily_df = pd.read_parquet(file_path)
                        daily_df.index = daily_df.index.astype(float) # Ensure index is float timestamp
                        dfs.append(daily_df)
                    except Exception as e: self.logger.error(f"Error reading parquet {file_path}: {e}")
                current_date += timedelta(days=1)

            if not dfs: return None
            combined_df = pd.concat(dfs).sort_index()
            # Reindex to standard columns, adding missing ones with NaN
            combined_df = combined_df.reindex(columns=TimeframeManager.BAR_COLUMNS)

            # Filter precisely by timestamp
            start_ts = start_time.timestamp() if isinstance(start_time, datetime) else float(start_time) if start_time else None
            end_ts = end_time.timestamp() if isinstance(end_time, datetime) else float(end_time) if end_time else None
            if start_ts: combined_df = combined_df[combined_df.index >= start_ts]
            if end_ts: combined_df = combined_df[combined_df.index <= end_ts]

            return combined_df if not combined_df.empty else None
        except Exception as e:
            self.logger.error(f"Error loading historical data for {symbol_key} {timeframe}: {e}", exc_info=True)
            return None

    def get_current_bar(self, symbol_key: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Gets the currently forming bar from TimeframeManager."""
        return self.timeframe_manager.get_current_bar(symbol_key, timeframe)

    def get_latest_tick(self, symbol_key: str) -> Optional[Dict[str, Any]]:
         """Gets the most recent *converted* tick data stored."""
         return self.last_tick_data.get(symbol_key)
    
    def _handle_system_event(self, event: Event):
        """
        Handle system events.
        
        Args:
            event: System event
        """
        # Check for shutdown events, connection status changes, etc.
        pass
    
    def _handle_custom_event(self, event: Event):
        """
        Handle custom events.
        
        Args:
            event: Custom event
        """
        # Handle any custom events that might be relevant to data management
        pass
    
    def subscribe(self, symbol: str | Instrument, exchange: str | Exchange = None) -> bool:
        """
        Subscribe to updates for a symbol.
        Primarily used by OptionManager and other components that need to track specific symbols.

        Args:
            symbol: Symbol to subscribe to (can be a string or Instrument object)
            exchange: Optional exchange identifier (can be string or Exchange enum)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert string symbol to Instrument if needed
            if isinstance(symbol, str):
                if not exchange:
                    exchange = 'NSE'  # Default to NSE if no exchange specified
                # Handle both string and Exchange enum types
                exchange_str = exchange.value if isinstance(exchange, Exchange) else exchange
                if isinstance(exchange, str):
                    exchange = Exchange[exchange]
                
                # Create instrument with proper type and asset class
                instrument = Instrument(
                    symbol=symbol,
                    exchange=exchange,
                    instrument_type=InstrumentType.INDEX if symbol in ['NIFTY INDEX', 'NIFTY BANK', 'SENSEX'] else InstrumentType.EQUITY,
                    asset_class=AssetClass.INDEX if symbol in ['NIFTY INDEX', 'NIFTY BANK', 'SENSEX'] else AssetClass.EQUITY,
                    instrument_id=f"{exchange_str}:{symbol}" 
                )
            else:
                instrument = symbol
                
            self.logger.info(f"Subscribing to instrument: {instrument.symbol} {f'on {instrument.exchange}' if instrument.exchange else ''}")            

            # If we have a market data feed, use it
            if hasattr(self, 'market_data_feed') and self.market_data_feed:
                return self.market_data_feed.subscribe(instrument)
            
            self.logger.warning(f"No subscription mechanism available for {instrument.symbol}. Symbol will be tracked internally.")
            return True
            
        except Exception as e:
            self.logger.error(f"Error subscribing to {symbol}: {str(e)}")
            return False
    
    def unsubscribe(self, symbol: str, exchange: str = None) -> bool:
        """
        Unsubscribe from updates for a symbol.
        Primarily used by OptionManager and other components that need to track specific symbols.

        Args:
            symbol: Symbol to unsubscribe from
            exchange: Optional exchange identifier

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Unsubscribing from symbol: {symbol} {f'on {exchange}' if exchange else ''}")
            
            # If we have a broker with unsubscribe capability, use it
            if self.broker and hasattr(self.broker, 'unsubscribe'):
                return self.broker.unsubscribe(symbol, exchange)
            
            # If we have a market data feed, use it
            if hasattr(self, 'market_data_feed') and self.market_data_feed:
                if exchange:
                    return self.market_data_feed.unsubscribe(symbol, exchange)
                else:
                    return self.market_data_feed.unsubscribe(symbol)
            
            # No unsubscription mechanism available, but return success
            self.logger.warning(f"No unsubscription mechanism available for {symbol}.")
            return True
            
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {symbol}: {str(e)}")
            return False
    
    # --- Subscription Management ---
    def subscribe_to_timeframe(self, instrument: Instrument, timeframe: str, strategy_id: str) -> bool:
        """Subscribe a strategy to a specific instrument and timeframe."""
        symbol_key = self._get_symbol_key(instrument)
        if not symbol_key: return False
        if timeframe not in TimeframeManager.VALID_TIMEFRAMES or timeframe == 'tick':
            self.logger.error(f"Invalid timeframe for subscription: {timeframe}")
            return False

        with self.global_lock:
            first_subscription_for_symbol = symbol_key not in self.active_subscriptions
            # Add strategy subscription first
            # self.timeframe_subscriptions[timeframe][strategy_id].add(symbol_key) # This structure seems redundant if we use symbol_subscribers
            self.symbol_subscribers[symbol_key][timeframe].add(strategy_id)

            # If it's the first time any strategy needs this symbol, subscribe at broker
            if first_subscription_for_symbol:
                 # If we have a market data feed, use it
                if hasattr(self, 'market_data_feed') \
                    and self.market_data_feed        \
                    and self.market_data_feed.subscribe(instrument):
                    self.active_subscriptions.add(symbol_key)
                    
                    # Store instrument object if first time subscribing
                    if symbol_key not in self.instruments:
                        self.instruments[symbol_key] = instrument
                    self.logger.info(f"Subscribed to market data feed for {symbol_key} (Triggered by {strategy_id} for {timeframe})")
                else: # Broker subscription failed, roll back strategy subscription
                    # del self.timeframe_subscriptions[timeframe][strategy_id] # Remove if structure removed
                    self.symbol_subscribers[symbol_key][timeframe].discard(strategy_id)
                    if not self.symbol_subscribers[symbol_key][timeframe]: del self.symbol_subscribers[symbol_key][timeframe]
                    if not self.symbol_subscribers[symbol_key]: del self.symbol_subscribers[symbol_key]
                    self.logger.error(f"Failed broker subscription for {symbol_key}. Cannot add {strategy_id} for {timeframe}.")
                    return False

        self.logger.info(f"Strategy '{strategy_id}' subscribed to {symbol_key} on timeframe {timeframe}")
        return True

    def unsubscribe_from_timeframe(self, instrument: Instrument, timeframe: str, strategy_id: str) -> bool:
        """Unsubscribe a strategy from a specific instrument and timeframe."""
        symbol_key = self._get_symbol_key(instrument)
        if not symbol_key or timeframe not in TimeframeManager.VALID_TIMEFRAMES: 
            return False

        removed = False
        with self.global_lock:
            # Remove strategy subscription from symbol_subscribers
            if symbol_key in self.symbol_subscribers and timeframe in self.symbol_subscribers[symbol_key]:
                self.symbol_subscribers[symbol_key][timeframe].discard(strategy_id)
                if not self.symbol_subscribers[symbol_key][timeframe]:
                    del self.symbol_subscribers[symbol_key][timeframe]
                removed = True

            # Check if any strategy still needs this symbol on any timeframe
            if symbol_key in self.symbol_subscribers and not self.symbol_subscribers[symbol_key]:
                # No strategy needs this symbol anymore
                del self.symbol_subscribers[symbol_key]
                if symbol_key in self.active_subscriptions:
                   if self.market_data_feed.unsubscribe(instrument):
                       self.active_subscriptions.discard(symbol_key)
                       # Clean up other data for this symbol
                       if symbol_key in self.instruments: del self.instruments[symbol_key]
                       if symbol_key in self.last_tick_data: del self.last_tick_data[symbol_key]
                       if symbol_key in self.last_cumulative_volume: del self.last_cumulative_volume[symbol_key]
                       # Reset TimeframeManager data for this symbol
                       self.timeframe_manager.reset(symbol=symbol_key)
                       self.logger.info(f"Unsubscribed from broker feed for {symbol_key} and cleaned up data.")
                   else: self.logger.error(f"Failed broker unsubscribe for {symbol_key}.")

        if removed: 
            self.logger.info(f"Strategy '{strategy_id}' unsubscribed from {symbol_key} on {timeframe}")
        return removed
    
    def get_latest_data(self, symbol: str, n: int = 1) -> Optional[List[Dict]]:
        """
        Get the latest n data points for a symbol.
        
        Args:
            symbol: Symbol to get data for
            n: Number of data points to get (default 1)
            
        Returns:
            List of data points or None if not available
        """
        try:
            # Check if this is a compound key
            if ":" not in symbol and symbol in self.last_data:
                key = symbol
            else:
                # Try different exchanges if not found
                exchanges = ["NSE", "BSE"]  # Add more exchanges as needed
                key = None
                
                for exchange in exchanges:
                    test_key = f"{exchange}:{symbol}"
                    if test_key in self.last_data:
                        key = test_key
                        break
                        
                if key is None:
                    return None
                    
            # Return latest data
            if key in self.tick_cache:
                ticks = self.tick_cache[key][-n:]
                return [data for _, data in ticks]
            elif key in self.last_data:
                return [self.last_data[key]]
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting latest data for {symbol}: {str(e)}")
            return None
        
    def load_historical_data_for_backtest(self, symbols: List[str], start_date: str, end_date: str, 
                                        timeframe: str = '1m') -> Dict[str, pd.DataFrame]:
        """
        Load historical data for backtesting.
        
        Args:
            symbols: List of symbols to load data for
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            timeframe: Timeframe to load
            
        Returns:
            Dictionary of symbol -> DataFrame
        """
        result = {}
        
        try:
            # Convert to datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            start_timestamp = int(start_dt.timestamp() * 1000)
            end_timestamp = int(end_dt.timestamp() * 1000)
            
            # Load data for each symbol
            for symbol in symbols:
                df = self.get_historical_data(symbol, timeframe, n_bars=0, 
                                            start_time=start_timestamp, end_time=end_timestamp)
                
                if df is not None and not df.empty:
                    result[symbol] = df
                    
            return result
            
        except Exception as e:
            self.logger.error(f"Error loading historical data for backtest: {str(e)}")
            return result
    
    def get_vwap(self, symbol: str, timeframe: str = '1m', session_start: Optional[str] = None) -> Optional[float]:
        """
        Calculate Volume-Weighted Average Price (VWAP).
        
        Args:
            symbol: Symbol to calculate VWAP for
            timeframe: Timeframe to use
            session_start: Optional session start time in HH:MM format
            
        Returns:
            VWAP value or None if not available
        """
        try:
            # Get historical data
            df = self.get_historical_data(symbol, timeframe, n_bars=0)
            
            if df is None or df.empty:
                return None
                
            # Filter by session start if specified
            if session_start:
                try:
                    session_hour, session_minute = map(int, session_start.split(':'))
                    
                    # Convert timestamps to datetime for filtering
                    df['datetime'] = pd.to_datetime(df.index, unit='ms')
                    
                    # Filter only data from session start today
                    today = pd.Timestamp.now().date()
                    session_start_ts = pd.Timestamp(today, hour=session_hour, minute=session_minute)
                    
                    df = df[df['datetime'] >= session_start_ts]
                    
                    # If no data after filtering, return None
                    if df.empty:
                        return None
                        
                    # Remove temporary column
                    df.drop('datetime', axis=1, inplace=True)
                    
                except Exception as e:
                    self.logger.error(f"Error filtering by session start: {str(e)}")
                    
            # Calculate VWAP
            df['vwap'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum() / df['volume'].cumsum()
            
            # Return latest VWAP
            return df['vwap'].iloc[-1]
            
        except Exception as e:
            self.logger.error(f"Error calculating VWAP for {symbol}: {str(e)}")
            return None
    
    def get_pivot_points(self, symbol: str, method: str = 'standard') -> Dict[str, float]:
        """
        Calculate pivot points for a symbol.
        
        Args:
            symbol: Symbol to calculate pivot points for
            method: Pivot calculation method ('standard', 'fibonacci', 'camarilla', 'woodie')
            
        Returns:
            Dictionary of pivot points
        """
        try:
            # Get daily data
            df = self.get_historical_data(symbol, '1d', n_bars=2)
            
            if df is None or df.empty or len(df) < 1:
                return {}
                
            # Get previous day's data
            prev_day = df.iloc[-1]
            high = prev_day['high']
            low = prev_day['low']
            close = prev_day['close']
            
            result = {}
            
            # Calculate pivot points based on method
            if method == 'standard':
                pivot = (high + low + close) / 3
                r1 = 2 * pivot - low
                r2 = pivot + (high - low)
                r3 = high + 2 * (pivot - low)
                s1 = 2 * pivot - high
                s2 = pivot - (high - low)
                s3 = low - 2 * (high - pivot)
                
                result = {
                    'pivot': pivot,
                    'r1': r1,
                    'r2': r2,
                    'r3': r3,
                    's1': s1,
                    's2': s2,
                    's3': s3
                }
                
            elif method == 'fibonacci':
                pivot = (high + low + close) / 3
                r1 = pivot + 0.382 * (high - low)
                r2 = pivot + 0.618 * (high - low)
                r3 = pivot + (high - low)
                s1 = pivot - 0.382 * (high - low)
                s2 = pivot - 0.618 * (high - low)
                s3 = pivot - (high - low)
                
                result = {
                    'pivot': pivot,
                    'r1': r1,
                    'r2': r2,
                    'r3': r3,
                    's1': s1,
                    's2': s2,
                    's3': s3
                }
                
            elif method == 'camarilla':
                r1 = close + 1.1 * (high - low) / 12
                r2 = close + 1.1 * (high - low) / 6
                r3 = close + 1.1 * (high - low) / 4
                r4 = close + 1.1 * (high - low) / 2
                s1 = close - 1.1 * (high - low) / 12
                s2 = close - 1.1 * (high - low) / 6
                s3 = close - 1.1 * (high - low) / 4
                s4 = close - 1.1 * (high - low) / 2
                
                result = {
                    'r1': r1,
                    'r2': r2,
                    'r3': r3,
                    'r4': r4,
                    's1': s1,
                    's2': s2,
                    's3': s3,
                    's4': s4
                }
                
            elif method == 'woodie':
                pivot = (high + low + 2 * close) / 4
                r1 = 2 * pivot - low
                r2 = pivot + (high - low)
                s1 = 2 * pivot - high
                s2 = pivot - (high - low)
                
                result = {
                    'pivot': pivot,
                    'r1': r1,
                    'r2': r2,
                    's1': s1,
                    's2': s2
                }
                
            # Round to 2 decimal places
            return {k: round(v, 2) for k, v in result.items()}
            
        except Exception as e:
            self.logger.error(f"Error calculating pivot points for {symbol}: {str(e)}")
            return {}
    
    def update_timeframe_aggregation(self):
        """Update higher timeframe aggregations from 1-minute data"""
        try:
            current_time = int(time.time() * 1000)
            
            # Process each symbol
            for symbol in list(self.ohlc_data.keys()):
                # Skip if 1-minute data doesn't exist
                if symbol not in self.ohlc_data or '1m' not in self.ohlc_data[symbol]:
                    continue
                    
                # Get 1-minute data
                df_1m = self.ohlc_data[symbol]['1m'].copy()
                
                # Skip if not enough data
                if len(df_1m) < 2:
                    continue
                    
                # Convert index to datetime for resampling
                df_1m.reset_index(inplace=True)
                df_1m['datetime'] = pd.to_datetime(df_1m['timestamp'], unit='ms')
                df_1m.set_index('datetime', inplace=True)
                
                # Aggregate to higher timeframes
                for timeframe, resample_rule in [
                    ('5m', '5min'),
                    ('15m', '15min'),
                    ('30m', '30min'),
                    ('1h', '1H'),
                    ('4h', '4H'),
                    ('1d', 'D')
                ]:
                    # Skip timeframes no one is subscribed to
                    if (symbol not in self.symbol_subscribers or 
                        timeframe not in self.symbol_subscribers.get(symbol, {})):
                        continue
                        
                    # Resample to this timeframe
                    agg_dict = {
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }
                    
                    # Add open interest if available
                    if 'open_interest' in df_1m.columns:
                        agg_dict['open_interest'] = 'last'
                        
                    # Add Greeks if available
                    for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                        if greek in df_1m.columns:
                            agg_dict[greek] = 'last'
                            
                    # Resample
                    df_tf = df_1m.resample(resample_rule).agg(agg_dict)
                    
                    # Skip if no data
                    if df_tf.empty:
                        continue
                        
                    # Convert back to original format
                    df_tf.reset_index(inplace=True)
                    df_tf['timestamp'] = df_tf['datetime'].astype(int) // 10**6
                    df_tf.drop('datetime', axis=1, inplace=True)
                    df_tf.set_index('timestamp', inplace=True)
                    
                    # Initialize storage for this timeframe if needed
                    if timeframe not in self.ohlc_data[symbol]:
                        self.ohlc_data[symbol][timeframe] = pd.DataFrame()
                        
                    # Only keep complete bars (except the most recent one)
                    df_complete = df_tf.iloc[:-1].copy() if len(df_tf) > 1 else pd.DataFrame()
                    
                    # Store in memory
                    if not df_complete.empty:
                        old_df = self.ohlc_data[symbol][timeframe]
                        
                        # Get current bar timestamps to check for updates
                        existing_timestamps = set()
                        if not old_df.empty:
                            existing_timestamps = set(old_df.index)
                            
                        # Get new bars
                        new_df = df_complete[~df_complete.index.isin(existing_timestamps)]
                        
                        # Update memory storage
                        if not new_df.empty:
                            self.ohlc_data[symbol][timeframe] = pd.concat([old_df, new_df])
                            self.ohlc_data[symbol][timeframe] = self.ohlc_data[symbol][timeframe].iloc[-self.cache_limit:]
                            
                            # Publish events for new bars
                            for idx, row in new_df.iterrows():
                                bar_data = row.to_dict()
                                bar_data['timestamp'] = int(idx)
                                
                                # Extract Greeks if present
                                greeks = {}
                                for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                                    if greek in bar_data:
                                        greeks[greek] = bar_data[greek]
                                        
                                # Get instrument if available
                                instrument = None
                                if symbol in self.instruments:
                                    instrument = self.instruments[symbol]
                                    
                                # Publish event
                                self._publish_timeframe_bar_event(
                                    symbol, 
                                    timeframe, 
                                    bar_data, 
                                    greeks=greeks if greeks else None,
                                    open_interest=bar_data.get('open_interest'),
                                    instrument=instrument
                                )
                                    
        except Exception as e:
            self.logger.error(f"Error updating timeframe aggregation: {str(e)}")
            
    def _recovery_from_persistent_storage(self):
        """
        Recover data from persistent storage on startup
        """
        try:
            if not self.persistence_enabled:
                return
                
            self.logger.info("Recovering data from persistent storage...")
            
            # Get current date for recovery
            current_date = datetime.now().date()
            yesterday = current_date - timedelta(days=1)
            
            # Format dates for file paths
            today_str = current_date.strftime('%Y-%m-%d')
            yesterday_str = yesterday.strftime('%Y-%m-%d')
            
            # Directories to check
            check_dates = [today_str, yesterday_str]
            
            # Recover for active subscriptions
            for symbol in self.active_subscriptions:
                try:
                    recovered = False
                    
                    for date_str in check_dates:
                        # Try to load from CSV
                        file_path = os.path.join(self.data_dir, date_str, f"{symbol}_1m.csv")
                        
                        if os.path.exists(file_path):
                            df = pd.read_csv(file_path, index_col='timestamp')
                            
                            # Initialize storage if needed
                            if symbol not in self.ohlc_data:
                                self.ohlc_data[symbol] = {}
                                
                            if '1m' not in self.ohlc_data[symbol]:
                                self.ohlc_data[symbol]['1m'] = pd.DataFrame()
                                
                            # Only take most recent data up to max cache limit
                            if len(df) > self.cache_limit:
                                df = df.iloc[-self.cache_limit:]
                                
                            self.ohlc_data[symbol]['1m'] = df
                            self.one_minute_data[symbol] = df.copy()
                            
                            self.logger.info(f"Recovered {len(df)} bars for {symbol} from {file_path}")
                            recovered = True
                            break
                    
                    if not recovered:
                        self.logger.info(f"No data found for {symbol} in persistent storage")
                        
                except Exception as e:
                    self.logger.error(f"Error recovering data for {symbol}: {str(e)}")
                    
            # Update higher timeframes from recovered 1-minute data
            self.update_timeframe_aggregation()
            
            self.logger.info("Data recovery completed")
            
        except Exception as e:
            self.logger.error(f"Error in data recovery: {str(e)}")
    
    def optimize_memory_usage(self):
        """
        Optimize memory usage by cleaning up unused data
        """
        try:
            # Get current time
            current_time = int(time.time() * 1000)
            
            # Find symbols without active subscriptions
            inactive_symbols = []
            
            for symbol in self.ohlc_data:
                if symbol not in self.active_subscriptions:
                    # Check subscription timestamp to ensure it's been inactive for some time
                    last_active = self.last_subscription_time.get(symbol, 0)
                    
                    # If inactive for more than 1 hour, mark for cleanup
                    if current_time - last_active > 3600 * 1000:
                        inactive_symbols.append(symbol)
                        
            # Clean up inactive symbols
            for symbol in inactive_symbols:
                # Remove from memory
                if symbol in self.ohlc_data:
                    del self.ohlc_data[symbol]
                    
                if symbol in self.one_minute_data:
                    del self.one_minute_data[symbol]
                    
                if symbol in self.tick_cache:
                    del self.tick_cache[symbol]
                    
                if symbol in self.last_data:
                    del self.last_data[symbol]
                    
                self.logger.info(f"Cleaned up unused data for {symbol}")
                
            # Force garbage collection
            import gc
            gc.collect()
            
        except Exception as e:
            self.logger.error(f"Error optimizing memory usage: {str(e)}")
      
    def setup_scheduled_tasks(self):
        """Setup scheduled tasks for the data manager"""
        try:
            # Schedule memory optimization task
            import schedule
            
            # Run memory optimization every hour
            schedule.every(1).hour.do(self.optimize_memory_usage)
            
            # Update higher timeframes every minute
            schedule.every(1).minutes.do(self.update_timeframe_aggregation)
            
            # Create scheduler thread
            def scheduler_loop():
                while not self._shutdown_event.is_set():
                    schedule.run_pending()
                    time.sleep(1)
                    
            self._scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
            self._scheduler_thread.start()
            
        except ImportError:
            self.logger.warning("Schedule package not available, scheduled tasks will not run")
        except Exception as e:
            self.logger.error(f"Error setting up scheduled tasks: {str(e)}")
    
    def get_instrument(self, symbol_key: str) -> Optional[Instrument]:
         """ Retrieve the Instrument object for a given symbol key. """
         return self.instruments.get(symbol_key)
    
    def close(self):
        """Shutdown the data manager"""
        try:
            self.logger.info("Shutting down data manager...")
            
            # Signal shutdown
            self._shutdown_event.set()
            
            # Shutdown thread pool
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=True)
                self.logger.info("Thread pool shutdown completed")
            
            # Wait for threads to finish
            if hasattr(self, '_processor_thread') and self._processor_thread:
                self._processor_thread.join(timeout=5)
                
            if hasattr(self, '_persistence_thread') and self._persistence_thread:
                self._persistence_thread.join(timeout=5)
                
            if hasattr(self, '_scheduler_thread') and self._scheduler_thread:
                self._scheduler_thread.join(timeout=5)
                
            # Close Redis connection if open
            if hasattr(self, 'redis_client') and self.redis_client:
                self.redis_client.close()
                
            self.logger.info("Data manager shut down successfully")
            
        except Exception as e:
            self.logger.error(f"Error shutting down data manager: {str(e)}")
    
    def get_option_implied_volatility_surface(self, underlying_symbol: str) -> Optional[Dict]:
        """
        Calculate implied volatility surface for options of an underlying.
        
        Args:
            underlying_symbol: Underlying symbol
            
        Returns:
            Dictionary with implied volatility surface data or None if not available
        """
        try:
            # Get option chain
            chain_df = self.get_option_chain_data(underlying_symbol)
            
            if chain_df is None or chain_df.empty:
                return None
                
            # Extract implied volatility data
            surface_data = {
                'strikes': [],
                'expiries': [],
                'call_iv': [],
                'put_iv': []
            }
            
            # Find columns with implied volatility
            call_iv_col = next((col for col in chain_df.columns if 'call' in col and 'implied_vol' in col), None)
            put_iv_col = next((col for col in chain_df.columns if 'put' in col and 'implied_vol' in col), None)
            
            if not call_iv_col or not put_iv_col:
                return None
                
            # For each expiry date
            expiry_dates = chain_df['expiry_date'].unique() if 'expiry_date' in chain_df.columns else []
            
            for expiry in expiry_dates:
                expiry_df = chain_df[chain_df['expiry_date'] == expiry]
                
                for _, row in expiry_df.iterrows():
                    strike = row['strike_price']
                    call_iv = row.get(call_iv_col)
                    put_iv = row.get(put_iv_col)
                    
                    if strike and (call_iv is not None or put_iv is not None):
                        surface_data['strikes'].append(strike)
                        surface_data['expiries'].append(expiry)
                        surface_data['call_iv'].append(call_iv if call_iv is not None else float('nan'))
                        surface_data['put_iv'].append(put_iv if put_iv is not None else float('nan'))
                        
            return surface_data
            
        except Exception as e:
            self.logger.error(f"Error calculating IV surface: {str(e)}")
            return None

    def get_or_create_instrument(self, symbol_str: str) -> Optional[Instrument]:
        """
        Get an existing instrument or create a new one for the given symbol string.
        
        Args:
            symbol_str: String representation of the symbol (possibly with exchange prefix)
    
        Returns:
            Instrument object or None if creation fails
        """
        # Check if this is already a full symbol key with exchange
        if ':' in symbol_str:
            symbol_key = symbol_str
            parts = symbol_str.split(':', 1)
            exchange_str = parts[0]
            symbol_name = parts[1]
        else:
            # Use default exchange if not specified
            exchange_str = self.config.get('market', {}).get('default_exchange', 'NSE')
            symbol_name = symbol_str
            symbol_key = f"{exchange_str}:{symbol_name}"
        
        # Check if we already have this instrument
        if symbol_key in self.instruments:
            return self.instruments[symbol_key]
        
        try:
            # Convert exchange string to enum if possible
            try:
                exchange = Exchange[exchange_str] if exchange_str else None
            except (KeyError, ValueError):
                exchange = exchange_str
                
            # Determine instrument type (basic heuristic)
            if 'CE' in symbol_name or 'PE' in symbol_name:
                instrument_type = InstrumentType.OPTION
                asset_class = AssetClass.OPTIONS
            elif symbol_name in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX'] or 'INDEX' in symbol_name:
                instrument_type = InstrumentType.INDEX
                asset_class = AssetClass.INDEX
            else:
                instrument_type = InstrumentType.EQUITY
                asset_class = AssetClass.EQUITY
                
            # Create and store the instrument
            instrument = Instrument(
                symbol=symbol_name,
                exchange=exchange,
                instrument_type=instrument_type,
                asset_class=asset_class,
                instrument_id=symbol_key
            )
            
            self.instruments[symbol_key] = instrument
            self.logger.info(f"Created instrument for {symbol_key}")
            return instrument
            
        except Exception as e:
            self.logger.error(f"Failed to create instrument for {symbol_str}: {e}")
            return None

# --- Placeholder EventManager ---
class MockEventManager:
    def subscribe(self, event_type, handler, component_name): print(f"Subscribed {component_name} to {event_type}")
    def publish(self, event):
        print(f"Published Event: Type={event.event_type}, Symbol={getattr(event, 'symbol', 'N/A')}, TF={getattr(event, 'timeframe', 'N/A')}, Data={getattr(event, 'bar_data', getattr(event, 'data', {}))}")
# --- End Placeholders ---

# Example Usage Snippet
if __name__ == '__main__':
    # Mock EventManager and Broker for testing
    event_manager = MockEventManager()
    class MockBroker:
        def subscribe_market_data(self, instrument): print(f"Broker subscribed to {instrument.symbol}"); return True
        def unsubscribe_market_data(self, instrument): print(f"Broker unsubscribed from {instrument.symbol}"); return True

    mock_config = {
        'market_data': {
            'persistence': {'enabled': False}, # Disable persistence for simple test
            'calculate_greeks': True,
            'greeks_risk_free_rate': 0.06,
            'default_volatility': 0.22
        }
    }
    broker = MockBroker()

    # Initialize DataManager
    data_manager = DataManager(mock_config, event_manager, broker)

    # Example instrument (using token as per Finvasia logs)
    nifty_idx = Instrument(
        symbol="NIFTY", # Symbol for reference
        token="26000",  # Token used in Finvasia feed
        exchange=Exchange.NSE,
        instrument_type=InstrumentType.INDEX,
        asset_class=AssetClass.INDEX,
        instrument_id="NSE:26000" # Consistent ID
    )
    nifty_key = data_manager._get_symbol_key(nifty_idx) # Should produce "NSE:26000"

    nifty_opt = Instrument(
        symbol="NIFTY25MAY...", # Full symbol if available
        token="38605",
        exchange=Exchange.NFO,
        instrument_type=InstrumentType.OPTION,
        asset_class=AssetClass.OPTIONS,
        instrument_id="NFO:38605",        
        strike=24500, # Example strike
        expiry_date=datetime(2025, 5, 29), # Example expiry
        option_type="CALL", # Example type
        symbol_key="NSE:26000" # Link to underlying
    )
    opt_key = data_manager._get_symbol_key(nifty_opt) # Should produce "NFO:38605"


    # Subscribe strategies
    data_manager.subscribe_to_timeframe(nifty_idx, '1m', 'StrategyA')
    data_manager.subscribe_to_timeframe(nifty_opt, '1m', 'StrategyB') # Subscribe option strategy


    # Simulate MarketDataEvents (Ticks - converted format)
    print("\n--- Simulating Ticks (Converted Format) ---")
    start_ts = time.time()

    # Tick 1 for Nifty Index
    tick1_nifty = MarketDataEvent(
        event_type=EventType.MARKET_DATA,
        instrument=nifty_idx,
        timestamp=start_ts + 5,
        data={
            MarketDataType.LAST_PRICE.value: 24269.90,
            MarketDataType.TIMESTAMP.value: start_ts + 5,
            # Volume for index is often 0 or not applicable for bar volume
            MarketDataType.VOLUME.value: 0, # Feed handler puts cumulative here
            'symbol_key': nifty_key # Added by _on_market_data
        }
    )
    data_manager._on_market_data(tick1_nifty)
    time.sleep(0.01)

    # Tick 1 for Nifty Option
    tick1_opt = MarketDataEvent(
        event_type=EventType.MARKET_DATA,
        instrument=nifty_opt,
        timestamp=start_ts + 6,
        data={
            MarketDataType.LAST_PRICE.value: 208.90,
            MarketDataType.TIMESTAMP.value: start_ts + 6,
            MarketDataType.VOLUME.value: 64032750, # Cumulative volume from feed
            MarketDataType.OPEN_INTEREST.value: 500000, # Example OI
            'symbol_key': opt_key
        }
    )
    data_manager._on_market_data(tick1_opt)
    time.sleep(0.01)

    # Tick 2 for Nifty Index (completes 1m bar if start_ts was near minute start)
    tick2_nifty = MarketDataEvent(
        event_type=EventType.MARKET_DATA,
        instrument=nifty_idx,
        timestamp=start_ts + 65,
        data={
            MarketDataType.LAST_PRICE.value: 24275.00,
            MarketDataType.TIMESTAMP.value: start_ts + 65,
            MarketDataType.VOLUME.value: 0,
            'symbol_key': nifty_key
        }
    )
    data_manager._on_market_data(tick2_nifty)
    time.sleep(0.01)

    # Tick 2 for Nifty Option (completes 1m bar)
    tick2_opt = MarketDataEvent(
        event_type=EventType.MARKET_DATA,
        instrument=nifty_opt,
        timestamp=start_ts + 70,
        data={
            MarketDataType.LAST_PRICE.value: 210.00,
            MarketDataType.TIMESTAMP.value: start_ts + 70,
            MarketDataType.VOLUME.value: 64035150, # Increased cumulative volume
            MarketDataType.OPEN_INTEREST.value: 500500, # Changed OI
            'symbol_key': opt_key
        }
    )
    data_manager._on_market_data(tick2_opt)
    time.sleep(0.1) # Allow time for processing


    # Wait for queue processing (in real app, threads run continuously)
    print("\nWaiting for queue processing...")
    # In a real system, you wouldn't join the queue like this.
    # Instead, you'd query data or rely on published events.
    # For this example, we simulate waiting.
    time.sleep(1) # Give threads time to process


    print("\n--- Querying Data ---")
    # Get current bar (might be empty if last tick completed a bar)
    current_1m_nifty = data_manager.get_current_bar(nifty_key, '1m')
    print(f"Current 1m bar for {nifty_key}: {current_1m_nifty}")
    current_1m_opt = data_manager.get_current_bar(opt_key, '1m')
    print(f"Current 1m bar for {opt_key}: {current_1m_opt}")


    # Get historical bars
    hist_1m_nifty = data_manager.get_historical_data(nifty_key, '1m', n_bars=5)
    print(f"\nHistorical 1m bars for {nifty_key}:\n{hist_1m_nifty}")

    hist_1m_opt = data_manager.get_historical_data(opt_key, '1m', n_bars=5)
    print(f"\nHistorical 1m bars for {opt_key}:\n{hist_1m_opt}")


    # Get latest tick (converted format)
    latest_tick_nifty = data_manager.get_latest_tick(nifty_key)
    print(f"\nLatest Converted Tick for {nifty_key}: {latest_tick_nifty}")
    latest_tick_opt = data_manager.get_latest_tick(opt_key)
    print(f"\nLatest Converted Tick for {opt_key}: {latest_tick_opt}")


    # Shutdown
    print("\n--- Shutting Down ---")
    try:
        data_manager.close()
    except Exception as e:
        print(f"Error during shutdown: {e}")