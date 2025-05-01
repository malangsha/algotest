import logging
import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from datetime import datetime, timedelta
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
import pickle
import hashlib

from utils.constants import MarketDataType, Exchange, InstrumentType, Timeframe
from utils.timeframe_manager import TimeframeManager
from models.instrument import Instrument, AssetClass
from models.events import Event, EventType, MarketDataEvent, BarEvent, TimeframeEventType, TimeframeBarEvent
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
        self.use_redis = data_config.get('use_redis', False)
        self.redis_client = None
        
        # Low latency optimizations
        self.thread_pool_size = data_config.get('thread_pool_size', 4)
        self.processing_queue_size = data_config.get('processing_queue_size', 10000)
        self.batch_processing = data_config.get('batch_processing', True)
        self.batch_size = data_config.get('batch_size', 100)
        
        # Greek calculation configuration
        self.calculate_greeks = data_config.get('calculate_greeks', True)
        self.greeks_risk_free_rate = data_config.get('greeks_risk_free_rate', 0.05)
        
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
        
        # Initialize thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=self.thread_pool_size)
        
        # Initialize processing queue and thread
        self.tick_queue = queue.Queue(maxsize=self.processing_queue_size)
        self._shutdown_event = threading.Event()
        
        # Store instrument objects for easy lookup
        self.instruments: Dict[str, Instrument] = {} # symbol_key -> Instrument
        
        # Initialize subscription tracking
        self.timeframe_subscriptions = {}
        for timeframe in TimeframeManager.VALID_TIMEFRAMES:
            self.timeframe_subscriptions[timeframe] = {}
            
        # Additional index for reverse lookup (symbol -> strategies)
        # This helps with efficient event distribution
        self.symbol_subscribers = {}  # {symbol: {timeframe: set(strategy_ids)}}

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
        self.active_subscriptions = set()
        
        self.logger.info("Enhanced Data Manager initialized with low latency optimizations")
        
    def _setup_persistence(self):
        """Setup the persistence directory structure and mechanism"""
        try:
            # Create main data directory
            os.makedirs(self.persistence_path, exist_ok=True)
            
            # Create subdirectories for different data types
            os.makedirs(os.path.join(self.persistence_path, 'ticks'), exist_ok=True)
            os.makedirs(os.path.join(self.persistence_path, 'bars'), exist_ok=True)
            os.makedirs(os.path.join(self.persistence_path, 'greeks'), exist_ok=True)
            
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
            self.instruments[symbol_key] = instrument

        try:
            # Put event in queue for processing
            self.tick_queue.put(event, block=False)
        except queue.Full:
            self.logger.warning("Tick processing queue full, dropping market data event")

    def _process_tick_queue(self):
        """Process market data events from the queue"""
        batch = []
        last_process_time = time.time()
        
        while True:
            try:
                # Get next event from queue with timeout
                try:
                    event = self.tick_queue.get(timeout=0.001)
                    batch.append(event)
                except queue.Empty:
                    # Process batch if we have any events or it's been a while
                    if batch and (len(batch) >= self.batch_size or 
                                  time.time() - last_process_time > 0.01):
                        self._process_market_data_batch(batch)
                        batch = []
                        last_process_time = time.time()
                    continue
                
                # Process batch if it's full
                if len(batch) >= self.batch_size:
                    self._process_market_data_batch(batch)
                    batch = []
                    last_process_time = time.time()
                    
            except Exception as e:
                self.logger.error(f"Error in tick processing thread: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                # Clear batch on error
                batch = []
                time.sleep(0.01)  # Brief pause on error
    
    def _process_market_data_batch(self, batch: List[MarketDataEvent]):
        """Process market data events in a batch for improved performance.
        
        Args:
            batch: List of market data events
        """
        # Group events by symbol to process together
        symbol_events = {}
        for event in batch:
            if event.instrument:
                symbol_key = self._get_symbol_key(event.instrument)
                if symbol_key not in symbol_events:
                    symbol_events[symbol_key] = []
                symbol_events[symbol_key].append(event)
            else:
                self.logger.warning(f"Received market data event without instrument: {event}")

        # Process each symbol's events in parallel
        with ThreadPoolExecutor(max_workers=min(len(symbol_events), self.thread_pool_size)) as executor:
            futures = [executor.submit(self._process_symbol_events, key, events) 
                       for key, events in symbol_events.items()]
            
            # Wait for all processing to complete
            for future in futures:
                try:
                    future.result()  # Check for exceptions
                except Exception as e:
                    self.logger.error(f"Error processing symbol events: {e}")
    
    def _process_symbol_events(self, key: str, events: List[MarketDataEvent]):
        """
        Process all events for a specific symbol.
        
        Args:
            key: Symbol key (including exchange if applicable)
            events: List of events for this symbol
        """
        try:
            # Get symbol-specific lock
            lock = self._get_lock_for_symbol(key)
            
            # Process events sequentially for this symbol
            with lock:
                for event in events:
                    self._process_single_market_data_event(key, event)
        except Exception as e:
            self.logger.error(f"Error processing events for {key}: {str(e)}")
    
    def _process_single_market_data_event(self, key: str, event: MarketDataEvent):
        """
        Process a single market data event.
        
        Args:
            key: Symbol key
            event: Market data event
        """
        try:
            # Extract event data
            instrument = event.instrument
            data = event.data
            timestamp = event.timestamp
            
            # Convert TIMESTAMP to lowercase timestamp if present in data
            if 'TIMESTAMP' in data:
                data['timestamp'] = data['TIMESTAMP']
            
            # Initialize data structures if needed
            if key not in self.data_store:
                self.data_store[key] = {}
            if key not in self.tick_cache:
                self.tick_cache[key] = []
            
            # Store the latest data
            self.last_data[key] = data
            
            # Add to historical data store with timestamp as key for O(1) lookup
            self.data_store[key][timestamp] = data
            
            # Also keep in tick cache for quick access (circular buffer)
            self.tick_cache[key].append((timestamp, data))
            if len(self.tick_cache[key]) > self.tick_cache_size:
                self.tick_cache[key].pop(0)
            
            # Enforce overall cache limit if needed
            if self.use_cache and len(self.data_store[key]) > self.cache_limit:
                # Remove oldest entries
                oldest_keys = sorted(self.data_store[key].keys())[:len(self.data_store[key]) - self.cache_limit]
                for old_key in oldest_keys:
                    del self.data_store[key][old_key]
            
            # Handle bar/OHLC data
            if (event.data_type == MarketDataType.BAR or
                MarketDataType.OHLC.value in data):
                self._update_ohlc(key, data, timestamp, instrument)
            
            # Process tick data through the timeframe manager if price is available
            if event.data_type == MarketDataType.TRADE and 'price' in data:
                # Extract OI if available
                open_interest = data.get('open_interest', 0)
                
                tick_data = {
                    'timestamp': timestamp,
                    'price': float(data['price']),
                    'volume': float(data.get('volume', 0)),
                    'open_interest': float(open_interest)
                }
                
                # Process this tick in the timeframe manager
                completed_bars = self.timeframe_manager.process_tick(key, tick_data)
                
                # For each completed bar, publish a bar event to subscribers
                for timeframe, is_completed in completed_bars.items():
                    if is_completed:
                        # Get the completed bar
                        bars_df = self.timeframe_manager.get_bars(key, timeframe, 1)
                        if bars_df is not None and not bars_df.empty:
                            # Get the latest bar
                            bar = bars_df.iloc[-1].to_dict()
                            
                            # Calculate Greeks for options if needed
                            greeks_data = None
                            if self.calculate_greeks and instrument.instrument_type == InstrumentType.OPTION:
                                greeks_data = self._calculate_option_greeks(instrument, bar['close'], timestamp)
                                
                                # Store Greeks with the bar data
                                if greeks_data:
                                    if key not in self.greeks_data:
                                        self.greeks_data[key] = {}
                                    self.greeks_data[key][timestamp] = greeks_data
                            
                            # Publish bar event to interested strategies with Greeks data
                            self._publish_timeframe_bar_event(key, timeframe, bar, greeks_data, open_interest, instrument)

                            # If this is a 1-minute bar, store it specially
                            if timeframe == '1m':
                                self._store_one_minute_bar(key, bar, greeks_data, open_interest)
            
            # Store in Redis if enabled
            if self.use_redis and self.redis_client:
                self._store_in_redis(key, data, timestamp)
                
        except Exception as e:
            self.logger.error(f"Error processing market data for {key}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _calculate_option_greeks(self, instrument: Instrument, price: float, timestamp: float) -> Optional[Dict[str, float]]:
        """
        Calculate option Greeks for an option instrument.
        
        Args:
            instrument: Option instrument
            price: Current option price
            timestamp: Current timestamp
            
        Returns:
            Optional[Dict[str, float]]: Dictionary of Greeks or None if calculation failed
        """
        if not instrument or instrument.instrument_type != InstrumentType.OPTION:
            return None
            
        try:
            # Get option details
            option_type = instrument.option_type
            strike_price = instrument.strike
            expiry_date = instrument.expiry_date
            
            # Get underlying symbol
            underlying_symbol = getattr(instrument, 'underlying', None)
            if not underlying_symbol:
                self.logger.debug(f"Underlying symbol not available for {instrument.symbol}")
                return None
                
            # Get underlying price
            underlying_price = self._get_underlying_price(underlying_symbol)
            if not underlying_price:
                self.logger.debug(f"Underlying price not available for {underlying_symbol}")
                return None
                
            # Calculate time to expiry
            expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
            current_dt = datetime.fromtimestamp(timestamp / 1000.0)
            time_diff = expiry_dt - current_dt
            time_to_expiry = max(0.0, time_diff.total_seconds() / (365 * 24 * 60 * 60))
            
            if time_to_expiry <= 0:
                return None
                
            # Get volatility (could be improved with more accurate IV calculation)
            implied_volatility = 0.2  # Placeholder
            
            risk_free_rate = self.greeks_risk_free_rate  # Placeholder
            
            # Calculate Greeks
            greeks = OptionsGreeksCalculator.calculate_greeks(
                option_type=option_type,
                underlying_price=underlying_price,
                strike_price=strike_price,
                time_to_expiry=time_to_expiry,
                risk_free_rate=risk_free_rate,
                implied_volatility=implied_volatility
            )
            
            return greeks
            
        except Exception as e:
            self.logger.error(f"Error calculating Greeks: {str(e)}")
            return None
    
    def _get_underlying_price(self, underlying_symbol: str) -> Optional[float]:
        """
        Get the price of an underlying symbol.
        
        Args:
            underlying_symbol: Underlying symbol
            
        Returns:
            Optional[float]: Underlying price or None if not available
        """
        # First check the last tick data
        if underlying_symbol in self.last_tick_data:
            data = self.last_tick_data[underlying_symbol]
            price = data.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                return float(price)
                
            # Try bid/ask midpoint
            bid = data.get(MarketDataType.BID.value)
            ask = data.get(MarketDataType.ASK.value)
            if bid is not None and ask is not None:
                return (float(bid) + float(ask)) / 2
                
        # If not in tick data, check minute bars
        if underlying_symbol in self.one_minute_data:
            df = self.one_minute_data[underlying_symbol]
            if not df.empty:
                return float(df.iloc[-1]['close'])
                
        return None
    
    def _store_one_minute_bar(self, key: str, bar: Dict, greeks: Optional[Dict] = None, open_interest: Optional[float] = None):
        """
        Store a 1-minute bar with associated Greeks and OI.
        
        Args:
            key: Symbol key
            bar: Bar data dictionary
            greeks: Optional Greeks data
            open_interest: Optional open interest
        """
        try:
            # Initialize if needed
            if key not in self.one_minute_data:
                self.one_minute_data[key] = pd.DataFrame(
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                             'open_interest', 'delta', 'gamma', 'theta', 'vega', 'rho']
                )
                self.one_minute_data[key].set_index('timestamp', inplace=True)
            
            # Use current timestamp in milliseconds if not provided
            current_timestamp = int(time.time() * 1000)
            
            # Create bar data with Greeks and OI
            bar_data = {
                'timestamp': bar.get('timestamp', current_timestamp),
                'open': bar.get('open', 0),
                'high': bar.get('high', 0),
                'low': bar.get('low', 0),
                'close': bar.get('close', 0),
                'volume': bar.get('volume', 0),
                'open_interest': open_interest if open_interest is not None else 0
            }
            
            # Handle TIMESTAMP from market data if present
            if 'TIMESTAMP' in bar:
                # Ensure timestamp is a large enough number to represent current time
                # A timestamp from 1970 would be very small compared to current timestamps
                ts = bar['TIMESTAMP']
                if isinstance(ts, (int, float)) and ts > 1000000000000:  # Reasonable timestamp after 2001
                    bar_data['timestamp'] = ts
                else:
                    bar_data['timestamp'] = current_timestamp
            
            # Add Greeks if available
            if greeks:
                bar_data.update({
                    'delta': greeks.get('delta', 0),
                    'gamma': greeks.get('gamma', 0),
                    'theta': greeks.get('theta', 0),
                    'vega': greeks.get('vega', 0),
                    'rho': greeks.get('rho', 0)
                })
            else:
                # Add empty Greeks
                bar_data.update({
                    'delta': 0,
                    'gamma': 0,
                    'theta': 0,
                    'vega': 0,
                    'rho': 0
                })
            
            # Add to DataFrame
            new_row = pd.DataFrame([bar_data])
            new_row.set_index('timestamp', inplace=True)
            
            # Concatenate with existing data
            self.one_minute_data[key] = pd.concat([self.one_minute_data[key], new_row])
            
            # Check if we need to persist this 1-minute data
            # If it's been more than the persistence interval since the last persistence for this symbol
            current_time = time.time()
            last_persist_time = self.last_persistence_time.get(key, 0)
            
            if current_time - last_persist_time > self.persistence_interval:
                # Schedule persistence for this symbol
                self.thread_pool.submit(self._persist_one_minute_data, key)
                self.last_persistence_time[key] = current_time
                
        except Exception as e:
            self.logger.error(f"Error storing 1-minute bar for {key}: {str(e)}")

    def _publish_timeframe_bar_event(self, symbol: str, timeframe: str, bar_data: Dict[str, Any], 
                                greeks: Optional[Dict] = None, open_interest: Optional[float] = None,
                                instrument: Optional[Instrument] = None):
        """
        Publish a bar event for a specific timeframe.
        
        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar_data: Bar data
            greeks: Optional Greeks data
            open_interest: Optional open interest
            instrument: Optional instrument object
        """
        try:
            # Check if anyone is subscribed to this timeframe for this symbol
            if symbol not in self.symbol_subscribers or timeframe not in self.symbol_subscribers[symbol]:
                return
            
            # If instrument is not provided, try to get it
            if instrument is None and hasattr(self, 'instruments'):
                instrument = self.instruments.get(symbol)
            
            # The TimeframeBarEvent requires event_type in __init__ even though it will be overridden
            # We pass EventType.BAR here as a placeholder - it will be replaced internally by the class
            bar_event = TimeframeBarEvent(                
                event_type=EventType.BAR,  # This will be overridden by the timeframe-based mapping
                timestamp=int(bar_data['timestamp']),
                instrument=instrument,
                timeframe=timeframe,
                bar_data=bar_data,
                greeks=greeks,
                open_interest=open_interest
            )
            
            # Publish the event
            if hasattr(self, 'event_manager'):
                self.event_manager.publish(bar_event)
                
        except Exception as e:
            self.logger.error(f"Error publishing timeframe bar event: {str(e)}")
    
    def _update_ohlc(self, symbol: str, data: Dict, timestamp: float, instrument: Optional[Instrument] = None):
        """
        Update OHLC data from a market data event with enhanced support for Greeks and OI.
    
        Args:
            symbol: Symbol of the instrument
            data: Market data dictionary
            timestamp: Event timestamp
            instrument: Optional instrument object
        """
        try:
            # Initialize OHLC storage for this symbol if needed
            if symbol not in self.ohlc_data:
                self.ohlc_data[symbol] = {}
    
            # Check if we have OHLC data in the event
            if MarketDataType.OHLC.value in data:
                ohlc = data[MarketDataType.OHLC.value]
                timeframe = ohlc.get('timeframe', '1m')
    
                # Create DataFrame if it doesn't exist for this timeframe
                if timeframe not in self.ohlc_data[symbol]:
                    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
                    
                    # Add columns for Greeks if this is an option
                    if instrument and instrument.instrument_type == InstrumentType.OPTION:
                        columns.extend(['delta', 'gamma', 'theta', 'vega', 'rho'])
                        
                    self.ohlc_data[symbol][timeframe] = pd.DataFrame(columns=columns)
                    self.ohlc_data[symbol][timeframe].set_index('timestamp', inplace=True)
    
                # Add the bar to the DataFrame
                bar = {
                    'timestamp': timestamp,
                    'open': float(ohlc.get('OPEN', 0)),
                    'high': float(ohlc.get('HIGH', 0)),
                    'low': float(ohlc.get('LOW', 0)),
                    'close': float(ohlc.get('CLOSE', 0)),
                    'volume': float(ohlc.get('VOLUME', 0)),
                    'open_interest': float(ohlc.get('OPEN_INTEREST', 0))
                }
                
                # Map TIMESTAMP to lowercase timestamp if present in data
                if 'TIMESTAMP' in ohlc:
                    bar['timestamp'] = float(ohlc['TIMESTAMP'])
                    
                # Calculate Greeks for options if needed
                if self.calculate_greeks and instrument and instrument.instrument_type == InstrumentType.OPTION:
                    greeks = self._calculate_option_greeks(instrument, bar['close'], timestamp)
                    if greeks:
                        bar.update({
                            'delta': greeks.get('delta', 0),
                            'gamma': greeks.get('gamma', 0),
                            'theta': greeks.get('theta', 0),
                            'vega': greeks.get('vega', 0),
                            'rho': greeks.get('rho', 0)
                        })
    
                # Add bar to DataFrame
                new_row = pd.DataFrame([bar])
                new_row.set_index('timestamp', inplace=True)
    
                # Concatenate with existing data
                self.ohlc_data[symbol][timeframe] = pd.concat(
                    [self.ohlc_data[symbol][timeframe], new_row]
                ).iloc[-self.cache_limit:]
                
                # If this is a 1-minute bar, store it specially and consider persistence
                if timeframe == '1m':
                    self._store_one_minute_bar(symbol, bar, 
                                            greeks if 'delta' in bar else None,
                                            bar.get('open_interest'))
                
                # Publish a timeframe-specific bar event
                self._publish_timeframe_bar_event(symbol, timeframe, bar, 
                                            greeks if 'delta' in bar else None,
                                            bar.get('open_interest'),
                                            instrument)
    
        except Exception as e:
            self.logger.error(f"Error updating OHLC data for {symbol}: {str(e)}")
    
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
    
    def _persistence_loop(self):
        """Background thread for periodic data persistence"""
        self.logger.info("Starting persistence loop")
        while True:
            try:
                # Sleep for the persistence interval
                time.sleep(self.persistence_interval)
                
                # Persist data for all symbols that haven't been persisted recently
                current_time = time.time()
                symbols_to_persist = []
                
                with self.global_lock:
                    for symbol in self.one_minute_data:
                        last_persist_time = self.last_persistence_time.get(symbol, 0)
                        if current_time - last_persist_time > self.persistence_interval:
                            symbols_to_persist.append(symbol)
                            self.last_persistence_time[symbol] = current_time
                
                # Persist in parallel using thread pool
                futures = []
                for symbol in symbols_to_persist:
                    futures.append(self.thread_pool.submit(self._persist_one_minute_data, symbol))
                    
                # Wait for all to complete (optional)
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error in persistence task: {str(e)}")
                        
            except Exception as e:
                self.logger.error(f"Error in persistence loop: {str(e)}")
                time.sleep(5)  # Sleep longer on error
    
    def _persist_one_minute_data(self, symbol: str):
        """
        Persist 1-minute bar data for a specific symbol.
        
        Args:
            symbol: Symbol to persist data for
        """
        try:
            if not self.persistence_enabled or symbol not in self.one_minute_data:
                return
                
            # Make a copy of the data to avoid lock contention
            with self._get_lock_for_symbol(symbol):
                df = self.one_minute_data[symbol].copy()
                
            if df.empty:
                return
                
            # Convert TIMESTAMP to lowercase timestamp if present
            if 'TIMESTAMP' in df.columns:
                df['timestamp'] = df['TIMESTAMP']
                df = df.drop(columns=['TIMESTAMP'])
                
            # Convert timestamp index to datetime
            df.reset_index(inplace=True)
            
            # Ensure timestamps are valid by replacing any very old dates with current time
            current_ms = int(time.time() * 1000)
            # Threshold for "old" timestamps (e.g., before 2020)
            threshold = 1577836800000  # Jan 1, 2020 in milliseconds
            
            # Replace invalid timestamps with current time
            if 'timestamp' in df.columns:
                df.loc[df['timestamp'] < threshold, 'timestamp'] = current_ms
            
            # Convert to datetime
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Get the date range in the dataframe
            min_date = df['datetime'].min().strftime('%Y-%m-%d')
            max_date = df['datetime'].max().strftime('%Y-%m-%d')
            
            # Get exchange from symbol if applicable
            exchange = "DEFAULT"
            if ":" in symbol:
                exchange, pure_symbol = symbol.split(":", 1)
            else:
                pure_symbol = symbol
                
            # Group by date and save separate files
            date_groups = df.groupby(df['datetime'].dt.date)
            
            for date, date_df in date_groups:
                date_str = date.strftime('%Y-%m-%d')
                
                # Create directory if it doesn't exist
                date_dir = os.path.join(self.persistence_path, 'bars', date_str)
                os.makedirs(date_dir, exist_ok=True)
                
                # File path
                file_path = os.path.join(date_dir, f"{exchange}_{pure_symbol}.parquet")
                
                # Check if file exists and append or create as needed
                if os.path.exists(file_path):
                    existing_df = pd.read_parquet(file_path)
                    
                    # Combine and deduplicate
                    date_df.set_index('timestamp', inplace=True)
                    combined_df = pd.concat([existing_df, date_df])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    
                    # Sort by timestamp
                    combined_df.sort_index(inplace=True)
                    
                    # Save back to file
                    combined_df.to_parquet(file_path)
                else:
                    # Just save this dataframe
                    date_df.set_index('timestamp', inplace=True)
                    date_df.to_parquet(file_path)
                    
            self.logger.debug(f"Persisted 1-minute data for {symbol} from {min_date} to {max_date}")
            
        except Exception as e:
            self.logger.error(f"Error persisting data for {symbol}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
    
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

    def subscribe_market_data(self, instrument: Instrument, strategy_id: str = None) -> bool:
        """
        Subscribe to market data for an instrument.
        
        Args:
            instrument: Instrument to subscribe to
            strategy_id: Optional ID of the strategy subscribing
            
        Returns:
            Boolean indicating success
        """
        try:
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)
            
            # Create symbol key
            key = f"{exchange}:{symbol}" if exchange else symbol
            
            # Add to active subscriptions
            self.active_subscriptions.add(key)
            
            # Add strategy subscription if provided
            if strategy_id:
                if key not in self.symbol_subscribers:
                    self.symbol_subscribers[key] = {}
                
                # Subscribe to all timeframes by default
                for timeframe in TimeframeManager.VALID_TIMEFRAMES:
                    if timeframe not in self.symbol_subscribers[key]:
                        self.symbol_subscribers[key][timeframe] = set()
                    
                    self.symbol_subscribers[key][timeframe].add(strategy_id)
                    
                    # Update reverse lookup
                    if timeframe not in self.timeframe_subscriptions:
                        self.timeframe_subscriptions[timeframe] = {}
                    if strategy_id not in self.timeframe_subscriptions[timeframe]:
                        self.timeframe_subscriptions[timeframe][strategy_id] = set()
                    
                    self.timeframe_subscriptions[timeframe][strategy_id].add(key)
            
            # Connect to broker market data feed if not already connected
            if self.broker and hasattr(self.broker, 'subscribe_market_data'):
                self.broker.subscribe_market_data(instrument)
                self.logger.info(f"Subscribed to market data for {key}")
                return True
            else:
                self.logger.warning(f"Broker doesn't support market data subscription for {key}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error subscribing to market data: {str(e)}")
            return False
    
    def unsubscribe_market_data(self, instrument: Instrument, strategy_id: str = None) -> bool:
        """
        Unsubscribe from market data for an instrument.
        
        Args:
            instrument: Instrument to unsubscribe from
            strategy_id: Optional ID of the strategy unsubscribing
            
        Returns:
            Boolean indicating success
        """
        try:
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)
            
            # Create symbol key
            key = f"{exchange}:{symbol}" if exchange else symbol
            
            # Remove strategy subscription if provided
            if strategy_id:
                if key in self.symbol_subscribers:
                    for timeframe in self.symbol_subscribers[key]:
                        if strategy_id in self.symbol_subscribers[key][timeframe]:
                            self.symbol_subscribers[key][timeframe].remove(strategy_id)
                            
                        # If no more subscribers for this timeframe, remove it
                        if not self.symbol_subscribers[key][timeframe]:
                            del self.symbol_subscribers[key][timeframe]
                            
                    # If no more timeframes, remove symbol
                    if not self.symbol_subscribers[key]:
                        del self.symbol_subscribers[key]
                        
                    # Update reverse lookup
                    for timeframe in self.timeframe_subscriptions:
                        if strategy_id in self.timeframe_subscriptions[timeframe]:
                            if key in self.timeframe_subscriptions[timeframe][strategy_id]:
                                self.timeframe_subscriptions[timeframe][strategy_id].remove(key)
                                
                            # If no more symbols, remove strategy
                            if not self.timeframe_subscriptions[timeframe][strategy_id]:
                                del self.timeframe_subscriptions[timeframe][strategy_id]
            
            # Check if anyone is still subscribed
            still_subscribed = False
            for timeframe_subs in self.symbol_subscribers.get(key, {}).values():
                if timeframe_subs:
                    still_subscribed = True
                    break
                    
            # If no one is subscribed, remove from active subscriptions
            if not still_subscribed:
                if key in self.active_subscriptions:
                    self.active_subscriptions.remove(key)
                    
                # Unsubscribe from broker
                if self.broker and hasattr(self.broker, 'unsubscribe_market_data'):
                    self.broker.unsubscribe_market_data(instrument)
                    self.logger.info(f"Unsubscribed from market data for {key}")
                    
            return True
                
        except Exception as e:
            self.logger.error(f"Error unsubscribing from market data: {str(e)}")
            return False
    
    def subscribe_to_timeframe(self, instrument: Instrument | str, timeframe: str, strategy_id: str) -> bool:
        """
        Subscribe to market data for a specific instrument and timeframe.

        Args:
            instrument: Instrument to subscribe to (can be Instrument object or string)
            timeframe: Timeframe to subscribe to
            strategy_id: ID of the strategy requesting the subscription

        Returns:
            bool: True if subscription was successful
        """
        try:
            # Convert string to Instrument if needed
            if isinstance(instrument, str):
                instrument_obj = self.get_or_create_instrument(instrument)
                if not instrument_obj:
                    self.logger.error(f"Cannot subscribe to timeframe: failed to create instrument for {instrument} (strategy: {strategy_id}, timeframe: {timeframe})")
                    return False
                instrument = instrument_obj

            if not instrument:
                self.logger.error(f"Cannot subscribe to timeframe: instrument is None (strategy: {strategy_id}, timeframe: {timeframe})")
                return False
                
            if not hasattr(instrument, 'symbol') or not instrument.symbol:
                self.logger.error(f"Cannot subscribe to timeframe: instrument has no symbol attribute (strategy: {strategy_id}, timeframe: {timeframe})")
                return False
                
            if not hasattr(instrument, 'exchange') or not instrument.exchange:
                self.logger.error(f"Cannot subscribe to timeframe: {instrument.symbol} has no exchange attribute (strategy: {strategy_id}, timeframe: {timeframe})")
                return False
        
            if timeframe not in TimeframeManager.VALID_TIMEFRAMES:
                self.logger.error(f"Invalid timeframe for subscription: {timeframe}")
                return False

            symbol_key = self._get_symbol_key(instrument)
            if symbol_key.startswith("INVALID:"):
                self.logger.error(f"Cannot subscribe {strategy_id} to {timeframe}: invalid symbol key {symbol_key}")
                return False
        
            # Store instrument object if not already stored
            if symbol_key not in self.instruments:
                self.instruments[symbol_key] = instrument

            # Ensure we have an active subscription to the instrument
            if symbol_key not in self.active_subscriptions:
                # Subscribe to the instrument
                if self.market_data_feed and hasattr(self.market_data_feed, 'subscribe'):
                    success = self.market_data_feed.subscribe(instrument)
                    if not success:
                        self.logger.error(f"Failed to subscribe to {symbol_key}")
                        return False
                self.active_subscriptions.add(symbol_key)
                self.logger.info(f"Subscribed to {symbol_key}")

            # Add to timeframe subscriptions
            if timeframe not in self.timeframe_subscriptions:
                self.timeframe_subscriptions[timeframe] = {}
            
            if strategy_id not in self.timeframe_subscriptions[timeframe]:
                self.timeframe_subscriptions[timeframe][strategy_id] = set()
                
            self.timeframe_subscriptions[timeframe][strategy_id].add(symbol_key)
            
            # Update symbol_subscribers for reverse lookup
            if symbol_key not in self.symbol_subscribers:
                self.symbol_subscribers[symbol_key] = {}
                
            if timeframe not in self.symbol_subscribers[symbol_key]:
                self.symbol_subscribers[symbol_key][timeframe] = set()
                
            self.symbol_subscribers[symbol_key][timeframe].add(strategy_id)
            
            self.logger.info(f"Subscribed {strategy_id} to {symbol_key} on timeframe {timeframe}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error subscribing to timeframe: {str(e)}")
            return False

    def unsubscribe_from_timeframe(self, instrument: Instrument, timeframe: str, strategy_id: str) -> bool:
        """
        Unsubscribe from market data for a specific instrument and timeframe.

        Args:
            instrument: Instrument to unsubscribe from
            timeframe: Timeframe to unsubscribe from
            strategy_id: ID of the strategy

        Returns:
            bool: True if unsubscription was successful
        """
        try:
            symbol_key = self._get_symbol_key(instrument)
            
            # Remove from timeframe subscriptions
            if (timeframe in self.timeframe_subscriptions and 
                strategy_id in self.timeframe_subscriptions[timeframe]):
                
                self.timeframe_subscriptions[timeframe][strategy_id].discard(symbol_key)
                
                # If strategy has no more symbols for this timeframe, remove it
                if not self.timeframe_subscriptions[timeframe][strategy_id]:
                    del self.timeframe_subscriptions[timeframe][strategy_id]
                    
                # If timeframe has no more strategies, remove it
                if not self.timeframe_subscriptions[timeframe]:
                    del self.timeframe_subscriptions[timeframe]
            
            # Update symbol_subscribers
            if (symbol_key in self.symbol_subscribers and 
                timeframe in self.symbol_subscribers[symbol_key]):
                
                self.symbol_subscribers[symbol_key][timeframe].discard(strategy_id)
                
                # If no more strategies for this timeframe, remove it
                if not self.symbol_subscribers[symbol_key][timeframe]:
                    del self.symbol_subscribers[symbol_key][timeframe]
                    
                # If symbol has no more timeframes, remove it
                if not self.symbol_subscribers[symbol_key]:
                    del self.symbol_subscribers[symbol_key]
                    
                    # Check if we should unsubscribe from the feed
                    should_unsubscribe = True
                    for tf_subs in self.timeframe_subscriptions.values():
                        for strat_symbols in tf_subs.values():
                            if symbol_key in strat_symbols:
                                should_unsubscribe = False
                                break
                                
                    if should_unsubscribe and symbol_key in self.active_subscriptions:
                        if self.market_data_feed and hasattr(self.market_data_feed, 'unsubscribe'):
                            self.market_data_feed.unsubscribe(instrument)
                        self.active_subscriptions.discard(symbol_key)
                        self.logger.info(f"Unsubscribed from {symbol_key}")
            
            self.logger.info(f"Unsubscribed {strategy_id} from {symbol_key} on timeframe {timeframe}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error unsubscribing from timeframe: {str(e)}")
            return False
    
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
    
    def get_historical_data(self, symbol: str, timeframe: str = '1m', 
                        n_bars: int = 100, start_time: Optional[float] = None, 
                        end_time: Optional[float] = None) -> Optional[pd.DataFrame]:
        """
        Get historical bar data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to get data for
            timeframe: Timeframe to get data for (1m, 5m, etc.)
            n_bars: Number of bars to get
            start_time: Optional start time (timestamp in ms)
            end_time: Optional end time (timestamp in ms)
            
        Returns:
            DataFrame of historical data or None if not available
        """
        try:
            # Check if this is a compound key
            if ":" not in symbol:
                # Try different exchanges if needed
                exchanges = ["NSE", "BSE"]  # Prioritize Indian exchanges
                found_key = None
                
                for exchange in exchanges:
                    test_key = f"{exchange}:{symbol}"
                    if test_key in self.ohlc_data and timeframe in self.ohlc_data[test_key]:
                        found_key = test_key
                        break
                        
                if not found_key and symbol in self.ohlc_data and timeframe in self.ohlc_data[symbol]:
                    found_key = symbol
            else:
                found_key = symbol
                
            if not found_key:
                # Check if data is in persistence
                return self._load_historical_data_from_storage(symbol, timeframe, start_time, end_time, n_bars)
                
            # Get data from in-memory store
            df = self.ohlc_data[found_key][timeframe].copy()
            
            # Apply time filters
            if start_time:
                df = df[df.index >= start_time]
                
            if end_time:
                df = df[df.index <= end_time]
                
            # Limit to n_bars
            if n_bars > 0:
                df = df.iloc[-n_bars:]
                
            return df
                
        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}:{timeframe}: {str(e)}")
            return self._load_historical_data_from_storage(symbol, timeframe, start_time, end_time, n_bars)
    
    def _load_historical_data_from_storage(self, symbol: str, timeframe: str = '1m',
                                        start_time: Optional[float] = None, 
                                        end_time: Optional[float] = None,
                                        n_bars: int = 100) -> Optional[pd.DataFrame]:
        """
        Load historical data from persistent storage.
        
        Args:
            symbol: Symbol to get data for
            timeframe: Timeframe to get data for
            start_time: Optional start time (timestamp in ms)
            end_time: Optional end time (timestamp in ms)
            n_bars: Number of bars to get
            
        Returns:
            DataFrame of historical data or None if not available
        """
        try:
            if not self.persistence_enabled or timeframe != '1m':
                return None
                
            # Get exchange from symbol if applicable
            exchange = "DEFAULT"
            if ":" in symbol:
                exchange, pure_symbol = symbol.split(":", 1)
            else:
                pure_symbol = symbol
                
            # Convert timestamps to dates
            if start_time:
                start_date = datetime.fromtimestamp(start_time / 1000).date()
            else:
                # Default to last 7 days
                start_date = (datetime.now() - timedelta(days=7)).date()
                
            if end_time:
                end_date = datetime.fromtimestamp(end_time / 1000).date()
            else:
                end_date = datetime.now().date()
                
            # Get list of dates in range
            date_range = []
            current_date = start_date
            while current_date <= end_date:
                date_range.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
                
            # Load data for each date
            dfs = []
            
            for date_str in date_range:
                file_path = os.path.join(self.persistence_path, 'bars', date_str, 
                                    f"{exchange}_{pure_symbol}.parquet")
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_parquet(file_path)
                        dfs.append(df)
                    except Exception as e:
                        self.logger.error(f"Error reading parquet file {file_path}: {str(e)}")
                        continue
                        
            if not dfs:
                return None
                
            # Combine all dataframes
            combined_df = pd.concat(dfs)
            
            # Apply time filters if needed
            if start_time:
                combined_df = combined_df[combined_df.index >= start_time]
                
            if end_time:
                combined_df = combined_df[combined_df.index <= end_time]
                
            # Sort by timestamp
            combined_df.sort_index(inplace=True)
            
            # Limit to n_bars
            if n_bars > 0:
                combined_df = combined_df.iloc[-n_bars:]
                
            return combined_df
                
        except Exception as e:
            self.logger.error(f"Error loading historical data for {symbol}: {str(e)}")
            return None
    
    def calculate_option_greeks_for_instruments(self, instruments: List[Instrument]) -> Dict[str, Dict[str, float]]:
        """
        Calculate Greeks for a list of option instruments.
        
        Args:
            instruments: List of option instruments
            
        Returns:
            Dictionary of instrument symbol -> Greeks data
        """
        results = {}
        
        for instrument in instruments:
            if instrument.instrument_type != InstrumentType.OPTION:
                continue
                
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)
            key = f"{exchange}:{symbol}" if exchange else symbol
            
            # Get latest price
            price = None
            if key in self.last_data and 'price' in self.last_data[key]:
                price = float(self.last_data[key]['price'])
            elif key in self.ohlc_data and '1m' in self.ohlc_data[key] and not self.ohlc_data[key]['1m'].empty:
                price = float(self.ohlc_data[key]['1m'].iloc[-1]['close'])
                
            if price is None:
                self.logger.warning(f"No price data available for {key}")
                continue
                
            # Calculate Greeks
            greeks = self._calculate_option_greeks(instrument, price, time.time() * 1000)
            
            if greeks:
                results[key] = greeks
                    
        return results
    
    def get_option_chain_data(self, underlying_symbol: str, 
                         expiry_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Get option chain data for an underlying symbol.
        
        Args:
            underlying_symbol: Underlying symbol
            expiry_date: Optional expiry date (YYYY-MM-DD)
            
        Returns:
            DataFrame with option chain data or None if not available
        """
        try:
            # This assumes we have subscribed to all options for this underlying
            # Find all options for this underlying
            options_data = []
            
            # Check all active subscriptions
            for key in self.active_subscriptions:
                # Extract exchange and symbol
                exchange = None
                if ":" in key:
                    exchange, symbol = key.split(":", 1)
                else:
                    symbol = key
                    
                # Check if this is an option for our underlying
                if (symbol.startswith(underlying_symbol) or 
                    (hasattr(self.ohlc_data.get(key, {}), 'underlying_symbol') and 
                    self.ohlc_data[key].underlying_symbol == underlying_symbol)):
                    
                    # Get option details
                    option_type = None
                    strike_price = None
                    option_expiry = None
                    
                    # Try to extract from symbol
                    # This is broker-specific and might need adjustment
                    if 'CE' in symbol:
                        option_type = 'CALL'
                    elif 'PE' in symbol:
                        option_type = 'PUT'
                        
                    # For Indian options like NIFTY22JUN17500CE
                    # Extract strike and expiry from symbol
                    if option_type:
                        # Specific to Indian options format
                        parts = symbol.replace('CE', '').replace('PE', '').split('/')
                        if len(parts) > 1:
                            try:
                                strike_price = float(parts[-1])
                            except:
                                pass
                                
                    # Get latest price and Greeks
                    price = None
                    greeks = None
                    oi = 0
                    
                    if key in self.last_data:
                        data = self.last_data[key]
                        price = data.get('price', data.get('close', None))
                        oi = data.get('open_interest', 0)
                    elif key in self.ohlc_data and '1m' in self.ohlc_data[key] and not self.ohlc_data[key]['1m'].empty:
                        last_bar = self.ohlc_data[key]['1m'].iloc[-1]
                        price = last_bar['close']
                        oi = last_bar.get('open_interest', 0)
                        
                        # Check for Greeks
                        if 'delta' in last_bar:
                            greeks = {
                                'delta': last_bar['delta'],
                                'gamma': last_bar['gamma'],
                                'theta': last_bar['theta'],
                                'vega': last_bar['vega'],
                                'rho': last_bar['rho']
                            }
                    
                    # Skip if no price
                    if price is None:
                        continue
                        
                    # Add to list
                    option_data = {
                        'symbol': symbol,
                        'exchange': exchange,
                        'option_type': option_type,
                        'strike_price': strike_price,
                        'expiry_date': option_expiry,
                        'price': price,
                        'open_interest': oi
                    }
                    
                    # Add Greeks if available
                    if greeks:
                        option_data.update(greeks)
                        
                    options_data.append(option_data)
            
            if not options_data:
                return None
                
            # Create DataFrame
            df = pd.DataFrame(options_data)
            
            # Filter by expiry date if specified
            if expiry_date and 'expiry_date' in df.columns:
                df = df[df['expiry_date'] == expiry_date]
                
            # Organize by option type and strike price
            if not df.empty and 'strike_price' in df.columns and 'option_type' in df.columns:
                # Sort by strike price
                df = df.sort_values(by='strike_price')
                
                # If we have both CALL and PUT options, pivot the data
                if set(df['option_type'].unique()) == {'CALL', 'PUT'}:
                    # Create a more readable option chain format
                    call_data = df[df['option_type'] == 'CALL'].set_index('strike_price')
                    put_data = df[df['option_type'] == 'PUT'].set_index('strike_price')
                    
                    # Rename columns to avoid conflicts
                    call_columns = {col: f'call_{col}' for col in call_data.columns if col != 'strike_price'}
                    put_columns = {col: f'put_{col}' for col in put_data.columns if col != 'strike_price'}
                    
                    call_data = call_data.rename(columns=call_columns)
                    put_data = put_data.rename(columns=put_columns)
                    
                    # Combine into single DataFrame
                    chain_df = pd.concat([call_data, put_data], axis=1)
                    
                    # Ensure strike price is included
                    chain_df.reset_index(inplace=True)
                    
                    return chain_df
            
            return df
                
        except Exception as e:
            self.logger.error(f"Error getting option chain data for {underlying_symbol}: {str(e)}")
            return None
    
    def _calculate_option_greeks(self, instrument: Instrument, current_price: float, timestamp: float) -> Dict[str, float]:
        """
        Calculate option Greeks for an instrument.
        
        Args:
            instrument: Option instrument
            current_price: Current price of the option
            timestamp: Current timestamp
            
        Returns:
            Dictionary of Greeks values
        """
        try:
            if not instrument or instrument.instrument_type != InstrumentType.OPTION:
                return {}
            
            # Extract option details
            strike_price = getattr(instrument, 'strike', 0)
            if not strike_price:
                return {}
                
            # Get expiry as days to expiry
            expiry_date = getattr(instrument, 'expiry_date', None)
            if not expiry_date:
                return {}
                
            # Convert expiry date to datetime
            if isinstance(expiry_date, str):
                try:
                    # For Indian options with expiry format DD-MM-YYYY
                    expiry_dt = datetime.strptime(expiry_date, '%d-%m-%Y')
                except:
                    try:
                        # Alternative format YYYY-MM-DD
                        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
                    except:
                        self.logger.error(f"Unable to parse expiry date: {expiry_date}")
                        return {}
            else:
                expiry_dt = expiry_date
                
            # Calculate days to expiry
            current_dt = datetime.fromtimestamp(timestamp / 1000)
            days_to_expiry = (expiry_dt - current_dt).days + (expiry_dt - current_dt).seconds / 86400
            
            if days_to_expiry <= 0:
                return {}
            
            # Convert to years
            time_to_expiry = days_to_expiry / 365.0
            
            # Get option type
            option_type = getattr(instrument, 'option_type', None)
            if not option_type:
                # Try to infer from symbol
                symbol = instrument.symbol
                if 'CE' in symbol:
                    option_type = 'CALL'
                elif 'PE' in symbol:
                    option_type = 'PUT'
                else:
                    return {}
            
            # Get underlying price
            underlying_symbol = getattr(instrument, 'underlying_symbol', None)
            underlying_price = None
            
            if underlying_symbol:
                # Try different exchanges
                exchanges = ["NSE", "BSE"]
                for exchange in exchanges:
                    key = f"{exchange}:{underlying_symbol}"
                    if key in self.last_data:
                        underlying_data = self.last_data[key]
                        underlying_price = underlying_data.get('price', underlying_data.get('close', None))
                        break
                
                # Try without exchange
                if underlying_price is None and underlying_symbol in self.last_data:
                    underlying_data = self.last_data[underlying_symbol]
                    underlying_price = underlying_data.get('price', underlying_data.get('close', None))
            
            # If we still don't have underlying price, try to estimate from the option price
            if underlying_price is None:
                # This is a very rough estimate for Indian options
                if option_type == 'CALL':
                    underlying_price = strike_price + current_price
                else:
                    underlying_price = strike_price - current_price
            
            # Default parameters for Indian market
            # Risk-free rate (approximate based on Indian Treasury yield)
            risk_free_rate = self.greeks_risk_free_rate
            
            # Volatility - use a default or calculate from historical data
            volatility = 0.25  # 25% annual volatility as default
            
            # Try to get historical volatility if available
            if underlying_symbol:
                try:
                    # Look for underlying historical data
                    historical_data = self.get_historical_data(underlying_symbol, timeframe='1d', n_bars=30)
                    if historical_data is not None and len(historical_data) > 5:
                        # Calculate log returns
                        returns = np.log(historical_data['close'] / historical_data['close'].shift(1)).dropna()
                        # Calculate annualized volatility
                        volatility = returns.std() * np.sqrt(252)  # 252 trading days in a year
                except Exception as e:
                    self.logger.debug(f"Error calculating historical volatility: {str(e)}")
            
            # Calculate Greeks using Black-Scholes model
            # Import required for calculations
            from scipy.stats import norm
            
            # Helper functions for Black-Scholes
            def bs_call_price(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
            
            def bs_put_price(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
            
            def bs_call_delta(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                return norm.cdf(d1)
            
            def bs_put_delta(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                return norm.cdf(d1) - 1
            
            def bs_gamma(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                return norm.pdf(d1) / (S * sigma * np.sqrt(T))
            
            def bs_vega(S, K, T, r, sigma):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                return S * np.sqrt(T) * norm.pdf(d1) / 100  # Divided by 100 for percentage
            
            def bs_theta(S, K, T, r, sigma, option_type):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                if option_type == 'CALL':
                    return (-S * sigma * norm.pdf(d1) / (2 * np.sqrt(T)) - 
                            r * K * np.exp(-r * T) * norm.cdf(d2)) / 365
                else:  # PUT
                    return (-S * sigma * norm.pdf(d1) / (2 * np.sqrt(T)) + 
                            r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365
            
            def bs_rho(S, K, T, r, sigma, option_type):
                d1 = (np.log(S/K) + (r + sigma**2/2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                if option_type == 'CALL':
                    return K * T * np.exp(-r * T) * norm.cdf(d2) / 100
                else:  # PUT
                    return -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
            
            # Calculate Greeks
            delta = bs_call_delta(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility) \
                    if option_type == 'CALL' else bs_put_delta(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility)
            
            gamma = bs_gamma(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility)
            vega = bs_vega(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility)
            theta = bs_theta(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility, option_type)
            rho = bs_rho(underlying_price, strike_price, time_to_expiry, risk_free_rate, volatility, option_type)
            
            # Return Greeks
            return {
                'delta': round(delta, 4),
                'gamma': round(gamma, 4),
                'theta': round(theta, 4),
                'vega': round(vega, 4),
                'rho': round(rho, 4),
                'implied_vol': round(volatility, 4),
                'time_to_expiry': round(time_to_expiry, 4)
            }
            
        except ImportError:
            self.logger.error("scipy package not available for Greeks calculation")
            return {}
        except Exception as e:
            self.logger.error(f"Error calculating Greeks: {str(e)}")
            return {}
    
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
    
    def calculate_technical_indicators(self, symbol: str, timeframe: str = '1d',
                                    indicators: List[str] = None) -> Optional[pd.DataFrame]:
        """
        Calculate technical indicators for a symbol.
        
        Args:
            symbol: Symbol to calculate indicators for
            timeframe: Timeframe to use
            indicators: List of indicators to calculate
            
        Returns:
            DataFrame with technical indicators or None if not available
        """
        try:
            # Default indicators if none specified
            if not indicators:
                indicators = ['sma_20', 'ema_20', 'rsi_14', 'macd', 'bollinger_bands']
                
            # Get historical data
            df = self.get_historical_data(symbol, timeframe)
            
            if df is None or df.empty:
                return None
                
            # Try to import TA-Lib
            try:
                import talib
                has_talib = True
            except ImportError:
                self.logger.warning("TA-Lib not available, using custom implementations")
                has_talib = False
                
            # Create result DataFrame
            result = df.copy()
            
            # Calculate requested indicators
            for indicator in indicators:
                parts = indicator.lower().split('_')
                indicator_type = parts[0]
                
                if indicator_type == 'sma':
                    # Simple Moving Average
                    period = int(parts[1]) if len(parts) > 1 else 20
                    
                    if has_talib:
                        result[f'sma_{period}'] = talib.SMA(result['close'].values, timeperiod=period)
                    else:
                        result[f'sma_{period}'] = result['close'].rolling(window=period).mean()
                        
                elif indicator_type == 'ema':
                    # Exponential Moving Average
                    period = int(parts[1]) if len(parts) > 1 else 20
                    
                    if has_talib:
                        result[f'ema_{period}'] = talib.EMA(result['close'].values, timeperiod=period)
                    else:
                        result[f'ema_{period}'] = result['close'].ewm(span=period, adjust=False).mean()
                        
                elif indicator_type == 'rsi':
                    # Relative Strength Index
                    period = int(parts[1]) if len(parts) > 1 else 14
                    
                    if has_talib:
                        result[f'rsi_{period}'] = talib.RSI(result['close'].values, timeperiod=period)
                    else:
                        # Custom RSI implementation
                        delta = result['close'].diff()
                        gain = delta.where(delta > 0, 0)
                        loss = -delta.where(delta < 0, 0)
                        
                        avg_gain = gain.rolling(window=period).mean()
                        avg_loss = loss.rolling(window=period).mean()
                        
                        rs = avg_gain / avg_loss
                        result[f'rsi_{period}'] = 100 - (100 / (1 + rs))
                        
                elif indicator_type == 'macd':
                    # Moving Average Convergence Divergence
                    fast_period = int(parts[1]) if len(parts) > 1 else 12
                    slow_period = int(parts[2]) if len(parts) > 2 else 26
                    signal_period = int(parts[3]) if len(parts) > 3 else 9
                    
                    if has_talib:
                        macd, signal, hist = talib.MACD(
                            result['close'].values,
                            fastperiod=fast_period,
                            slowperiod=slow_period,
                            signalperiod=signal_period
                        )
                        result['macd_line'] = macd
                        result['macd_signal'] = signal
                        result['macd_hist'] = hist
                    else:
                        # Custom MACD implementation
                        result['ema_fast'] = result['close'].ewm(span=fast_period, adjust=False).mean()
                        result['ema_slow'] = result['close'].ewm(span=slow_period, adjust=False).mean()
                        result['macd_line'] = result['ema_fast'] - result['ema_slow']
                        result['macd_signal'] = result['macd_line'].ewm(span=signal_period, adjust=False).mean()
                        result['macd_hist'] = result['macd_line'] - result['macd_signal']
                        
                elif indicator_type == 'bollinger':
                    # Bollinger Bands
                    period = int(parts[2]) if len(parts) > 2 else 20
                    std_dev = float(parts[3]) if len(parts) > 3 else 2.0
                    
                    if has_talib:
                        upper, middle, lower = talib.BBANDS(
                            result['close'].values,
                            timeperiod=period,
                            nbdevup=std_dev,
                            nbdevdn=std_dev
                        )
                        result[f'bb_upper_{period}'] = upper
                        result[f'bb_middle_{period}'] = middle
                        result[f'bb_lower_{period}'] = lower
                    else:
                        # Custom Bollinger Bands implementation
                        result[f'bb_middle_{period}'] = result['close'].rolling(window=period).mean()
                        result[f'bb_std_{period}'] = result['close'].rolling(window=period).std()
                        result[f'bb_upper_{period}'] = result[f'bb_middle_{period}'] + (result[f'bb_std_{period}'] * std_dev)
                        result[f'bb_lower_{period}'] = result[f'bb_middle_{period}'] - (result[f'bb_std_{period}'] * std_dev)
                
                elif indicator_type == 'atr':
                    # Average True Range
                    period = int(parts[1]) if len(parts) > 1 else 14
                    
                    if has_talib:
                        result[f'atr_{period}'] = talib.ATR(
                            result['high'].values,
                            result['low'].values,
                            result['close'].values,
                            timeperiod=period
                        )
                    else:
                        # Custom ATR implementation
                        tr1 = result['high'] - result['low']
                        tr2 = abs(result['high'] - result['close'].shift())
                        tr3 = abs(result['low'] - result['close'].shift())
                        
                        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                        result[f'atr_{period}'] = tr.rolling(window=period).mean()
                
                elif indicator_type == 'stoch':
                    # Stochastic Oscillator
                    k_period = int(parts[1]) if len(parts) > 1 else 14
                    d_period = int(parts[2]) if len(parts) > 2 else 3
                    
                    if has_talib:
                        slowk, slowd = talib.STOCH(
                            result['high'].values,
                            result['low'].values,
                            result['close'].values,
                            fastk_period=k_period,
                            slowk_period=d_period,
                            slowd_period=d_period
                        )
                        result[f'stoch_k_{k_period}'] = slowk
                        result[f'stoch_d_{k_period}'] = slowd
                    else:
                        # Custom Stochastic Oscillator implementation
                        period_low = result['low'].rolling(window=k_period).min()
                        period_high = result['high'].rolling(window=k_period).max()
                        
                        result[f'stoch_k_{k_period}'] = 100 * ((result['close'] - period_low) / (period_high - period_low))
                        result[f'stoch_d_{k_period}'] = result[f'stoch_k_{k_period}'].rolling(window=d_period).mean()
                        
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {str(e)}")
            return None
    
    def get_market_depth(self, symbol: str) -> Optional[Dict]:
        """
        Get market depth (order book) for a symbol.
        
        Args:
            symbol: Symbol to get market depth for
            
        Returns:
            Dictionary with bids and asks or None if not available
        """
        try:
            if not self.use_redis:
                return None
                
            # Get order book from Redis if available
            key = f"orderbook:{symbol}"
            
            if not self.redis_client.exists(key):
                return None
                
            # Get order book data
            order_book = self.redis_client.hgetall(key)
            
            if not order_book:
                return None
                
            # Parse order book
            bids = []
            asks = []
            
            for level_key, value in order_book.items():
                if isinstance(level_key, bytes):
                    level_key = level_key.decode('utf-8')
                    
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                    
                if level_key.startswith('bid:'):
                    level = int(level_key.split(':')[1])
                    price, quantity = map(float, value.split(':'))
                    bids.append((price, quantity, level))
                    
                elif level_key.startswith('ask:'):
                    level = int(level_key.split(':')[1])
                    price, quantity = map(float, value.split(':'))
                    asks.append((price, quantity, level))
                    
            # Sort bids and asks
            bids.sort(key=lambda x: (-x[0], x[2]))  # Sort bids by price desc, level asc
            asks.sort(key=lambda x: (x[0], x[2]))   # Sort asks by price asc, level asc
            
            # Return formatted result
            return {
                'bids': [{'price': price, 'quantity': quantity} for price, quantity, _ in bids],
                'asks': [{'price': price, 'quantity': quantity} for price, quantity, _ in asks]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting market depth for {symbol}: {str(e)}")
            return None
    
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
    
    def get_top_gainers_losers(self, exchange: str = "NSE", n: int = 5) -> Dict[str, List[Dict]]:
        """
        Get top gainers and losers for an exchange.
        
        Args:
            exchange: Exchange to get data for (NSE, BSE)
            n: Number of symbols to return
            
        Returns:
            Dictionary with top gainers and losers
        """
        try:
            results = {
                'gainers': [],
                'losers': []
            }
            
            # Get all symbols for this exchange
            symbols = []
            for key in self.ohlc_data:
                if key.startswith(f"{exchange}:"):
                    symbols.append(key)
                    
            if not symbols:
                return results
                
            # Calculate daily changes
            changes = []
            
            for symbol in symbols:
                if '1d' not in self.ohlc_data.get(symbol, {}):
                    continue
                    
                df = self.ohlc_data[symbol]['1d']
                
                if df.empty or len(df) < 2:
                    continue
                    
                # Get latest and previous close
                latest = df.iloc[-1]
                prev = df.iloc[-2]
                
                close = latest['close']
                prev_close = prev['close']
                
                # Calculate percent change
                pct_change = ((close - prev_close) / prev_close) * 100
                
                symbol_name = symbol.split(':', 1)[1] if ':' in symbol else symbol
                
                changes.append({
                    'symbol': symbol_name,
                    'price': close,
                    'change': close - prev_close,
                    'pct_change': pct_change
                })
                
            # Sort by percent change
            changes.sort(key=lambda x: x['pct_change'], reverse=True)
            
            # Get top gainers
            results['gainers'] = changes[:n]
            
            # Get top losers
            results['losers'] = sorted(changes, key=lambda x: x['pct_change'])[:n]
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error getting top gainers/losers: {str(e)}")
            return {'gainers': [], 'losers': []}
    
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
