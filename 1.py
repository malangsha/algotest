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

from utils.constants import MarketDataType, Exchange, InstrumentType
from utils.timeframe_manager import TimeframeManager
from models.instrument import Instrument, AssetClass
from models.events import Event, EventType, MarketDataEvent, BarEvent
from core.event_manager import EventManager
from core.logging_manager import get_logger
from utils.options_utils import calculate_greeks

# New Event Types for specific timeframes
class TimeframeEventType(Enum):
    BAR_1M = "BAR_1M"
    BAR_5M = "BAR_5M"
    BAR_15M = "BAR_15M"
    BAR_30M = "BAR_30M"
    BAR_1H = "BAR_1H"
    BAR_4H = "BAR_4H"
    BAR_1D = "BAR_1D"

@dataclass
class TimeframeBarEvent(Event):
    """Event for completed timeframe bars"""
    timeframe: str
    instrument: Instrument
    bar_data: Dict[str, Any]
    greeks: Optional[Dict[str, float]] = None
    open_interest: Optional[float] = None

    @property
    def event_type(self) -> EventType:
        # Map timeframe to corresponding event type
        timeframe_map = {
            '1m': TimeframeEventType.BAR_1M,
            '5m': TimeframeEventType.BAR_5M,
            '15m': TimeframeEventType.BAR_15M,
            '30m': TimeframeEventType.BAR_30M,
            '1h': TimeframeEventType.BAR_1H,
            '4h': TimeframeEventType.BAR_4H,
            '1d': TimeframeEventType.BAR_1D
        }
        return timeframe_map.get(self.timeframe, EventType.BAR)


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

        # Separate storage for 1-minute data for efficient persistence
        self.one_minute_data = {}  # Symbol -> DataFrame of 1-minute bars with Greeks and OI

        # Greek data storage
        self.greeks_data = {}  # Symbol -> Dictionary of timestamp -> Greeks data

        # In-memory caches for performance
        self.tick_cache = {}  # Symbol -> Recent ticks (circular buffer)
        self.tick_cache_size = 1000  # Maximum ticks to cache per symbol

        # Queues for processing
        self.tick_queue = queue.Queue(maxsize=self.processing_queue_size)

        # For thread safety with fine-grained locking
        self.data_locks = {}  # Symbol -> Lock
        self.global_lock = threading.RLock()

        # Thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=self.thread_pool_size)

        # Timeframe management with enhanced functionality
        max_bars = data_config.get('max_bars', 1000)
        self.timeframe_manager = TimeframeManager(max_bars=max_bars)

        # Strategy timeframe subscriptions - optimized structure
        # {timeframe: {strategy_id: set(symbols)}}
        self.timeframe_subscriptions = {}
        for timeframe in TimeframeManager.VALID_TIMEFRAMES:
            self.timeframe_subscriptions[timeframe] = {}

        # Additional index for reverse lookup (symbol -> strategies)
        # This helps with efficient event distribution
        self.symbol_subscribers = {}  # {symbol: {timeframe: set(strategy_ids)}}

        # Persistence mechanism with improved threading
        self.persistence_thread = None
        self.last_persistence_time = {}  # Track last persistence by symbol

        if self.persistence_enabled:
            self._setup_persistence()

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
            self.persistence_thread = threading.Thread(target=self._persistence_loop, daemon=True)
            self.persistence_thread.start()

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

        # Register for specific timeframe events if needed
        for tf_event in TimeframeEventType:
            self.event_manager.subscribe(
                tf_event,
                self._handle_timeframe_event,
                component_name="DataManager"
            )

        self.logger.info("Registered for market data and timeframe events with Event Manager")

    def _get_lock_for_symbol(self, symbol: str) -> threading.RLock:
        """Get a lock specific to a symbol to minimize contention"""
        with self.global_lock:
            if symbol not in self.data_locks:
                self.data_locks[symbol] = threading.RLock()
            return self.data_locks[symbol]

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
        """
        Process a batch of market data events.

        Args:
            batch: List of market data events
        """
        # Group events by symbol for more efficient processing
        symbol_events = {}
        for event in batch:
            instrument = event.instrument
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)

            # Create a key that uniquely identifies this instrument
            key = f"{exchange}:{symbol}" if exchange else symbol

            if key not in symbol_events:
                symbol_events[key] = []
            symbol_events[key].append(event)

        # Process each symbol's events in parallel
        futures = []
        for key, events in symbol_events.items():
            futures.append(self.thread_pool.submit(self._process_symbol_events, key, events))

        # Wait for all to complete (optional, can be made fully async)
        for future in futures:
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"Error processing symbol events: {str(e)}")

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
                MarketDataType.OHLC in data):
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

                            # If this is a 1-minute bar, store it specially
                            if timeframe == '1m':
                                self._store_one_minute_bar(key, bar, greeks_data, open_interest)

                            # Publish bar event to interested strategies with Greeks data
                            self._publish_timeframe_bar_event(key, timeframe, bar, greeks_data, open_interest, instrument)

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
            Dictionary of Greeks or None if calculation failed
        """
        try:
            # Get option parameters from instrument
            option_type = getattr(instrument, 'option_type', None)
            strike_price = getattr(instrument, 'strike_price', None)
            expiry_date = getattr(instrument, 'expiry_date', None)

            if not all([option_type, strike_price, expiry_date]):
                return None

            # Get underlying price - need to implement this based on your system design
            underlying_symbol = getattr(instrument, 'underlying_symbol', None)
            underlying_exchange = getattr(instrument, 'underlying_exchange', None)

            if not underlying_symbol:
                return None

            underlying_key = f"{underlying_exchange}:{underlying_symbol}" if underlying_exchange else underlying_symbol
            underlying_price = self._get_underlying_price(underlying_key)

            if not underlying_price:
                return None

            # Calculate time to expiry in years
            now = datetime.fromtimestamp(timestamp / 1000)  # Convert to seconds
            if isinstance(expiry_date, str):
                expiry = datetime.strptime(expiry_date, "%Y-%m-%d")
            elif isinstance(expiry_date, (int, float)):
                expiry = datetime.fromtimestamp(expiry_date)
            else:
                expiry = expiry_date

            time_to_expiry = (expiry - now).total_seconds() / (365.25 * 24 * 60 * 60)

            if time_to_expiry <= 0:
                return None

            # Get estimated volatility - in a real system, you'd have a volatility model
            implied_volatility = 0.3  # Placeholder, should be calculated or provided

            # Calculate Greeks
            greeks = calculate_greeks(
                option_type=option_type,
                underlying_price=underlying_price,
                strike_price=strike_price,
                time_to_expiry=time_to_expiry,
                risk_free_rate=self.greeks_risk_free_rate,
                implied_volatility=implied_volatility
            )

            return greeks
        except Exception as e:
            self.logger.error(f"Error calculating Greeks: {str(e)}")
            return None

    def _get_underlying_price(self, underlying_key: str) -> Optional[float]:
        """
        Get the latest price for an underlying instrument.

        Args:
            underlying_key: Key of the underlying instrument

        Returns:
            Latest price or None if not available
        """
        try:
            # Check if we have the latest data for this symbol
            if underlying_key in self.last_data:
                data = self.last_data[underlying_key]
                if 'price' in data:
                    return float(data['price'])
                elif 'close' in data:
                    return float(data['close'])

            # If not in last_data, check OHLC data
            if underlying_key in self.ohlc_data:
                for timeframe in self.ohlc_data[underlying_key]:
                    df = self.ohlc_data[underlying_key][timeframe]
                    if not df.empty:
                        return float(df.iloc[-1]['close'])

            return None
        except Exception as e:
            self.logger.error(f"Error getting underlying price for {underlying_key}: {str(e)}")
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

            # Create bar data with Greeks and OI
            bar_data = {
                'timestamp': bar.get('timestamp', int(time.time() * 1000)),
                'open': bar.get('open', 0),
                'high': bar.get('high', 0),
                'low': bar.get('low', 0),
                'close': bar.get('close', 0),
                'volume': bar.get('volume', 0),
                'open_interest': open_interest if open_interest is not None else 0
            }

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
        Publish a timeframe-specific bar event for interested strategies.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar_data: Bar data to include in the event
            greeks: Optional Greeks data
            open_interest: Optional open interest
            instrument: Original instrument object
        """
        # Create a proper instrument object if not provided
        if instrument is None:
            # Extract exchange if embedded in symbol
            exchange = None
            if ":" in symbol:
                exchange, pure_symbol = symbol.split(":", 1)
            else:
                pure_symbol = symbol

            # Create basic instrument
            instrument = Instrument(symbol=pure_symbol, exchange=exchange)

        # Create TimeframeBarEvent
        bar_event = TimeframeBarEvent(
            timestamp=int(bar_data.get('timestamp', time.time() * 1000)),
            timeframe=timeframe,
            instrument=instrument,
            bar_data=bar_data,
            greeks=greeks,
            open_interest=open_interest
        )

        # Publish the event to all subscribers
        self.event_manager.publish(bar_event)

        # Log at debug level
        self.logger.debug(f"Published {timeframe} bar for {symbol}: {bar_data}")

    def _handle_timeframe_event(self, event):
        """
        Handle timeframe-specific events.

        Args:
            event: Timeframe event
        """
        # Implementation to handle inbound timeframe events if needed
        pass

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
            if MarketDataType.OHLC in data:
                ohlc = data[MarketDataType.OHLC]
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
                    'open': float(ohlc.get('open', 0)),
                    'high': float(ohlc.get('high', 0)),
                    'low': float(ohlc.get('low', 0)),
                    'close': float(ohlc.get('close', 0)),
                    'volume': float(ohlc.get('volume', 0)),
                    'open_interest': float(ohlc.get('open_interest', 0))
                }

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
            if MarketDataType.OHLC in data:
                ohlc = data[MarketDataType.OHLC]
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

            # Convert timestamp index to datetime
            df.reset_index(inplace=True)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Create directory structure based on date
            # Format: ./data/bars/YYYY-MM-DD/EXCHANGE_SYMBOL.parquet

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

    def subscribe_to_timeframe(self, instrument: Instrument, timeframe: str, strategy_id: str) -> bool:
        """
        Subscribe a strategy to receive bar events for a specific timeframe.

        Args:
            instrument: Instrument to subscribe to
            timeframe: Timeframe to subscribe to
            strategy_id: ID of the strategy subscribing

        Returns:
            Boolean indicating success
        """
        try:
            if timeframe not in TimeframeManager.VALID_TIMEFRAMES:
                self.logger.error(f"Invalid timeframe: {timeframe}")
                return False

            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)

            # Create symbol key
            key = f"{exchange}:{symbol}" if exchange else symbol

            # First ensure market data subscription exists
            self.subscribe_market_data(instrument, strategy_id)

            # Add timeframe subscription
            if key not in self.symbol_subscribers:
                self.symbol_subscribers[key] = {}

            if timeframe not in self.symbol_subscribers[key]:
                self.symbol_subscribers[key][timeframe] = set()

            self.symbol_subscribers[key][timeframe].add(strategy_id)

            # Update reverse lookup
            if timeframe not in self.timeframe_subscriptions:
                self.timeframe_subscriptions[timeframe] = {}

            if strategy_id not in self.timeframe_subscriptions[timeframe]:
                self.timeframe_subscriptions[timeframe][strategy_id] = set()

            self.timeframe_subscriptions[timeframe][strategy_id].add(key)

            self.logger.info(f"Strategy {strategy_id} subscribed to {timeframe} for {key}")
            return True

        except Exception as e:
            self.logger.error(f"Error subscribing to timeframe: {str(e)}")
            return False

    def unsubscribe_from_timeframe(self, instrument: Instrument, timeframe: str, strategy_id: str) -> bool:
        """
        Unsubscribe a strategy from a specific timeframe.

        Args:
            instrument: Instrument to unsubscribe from
            timeframe: Timeframe to unsubscribe from
            strategy_id: ID of the strategy unsubscribing

        Returns:
            Boolean indicating success
        """
        try:
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)

            # Create symbol key
            key = f"{exchange}:{symbol}" if exchange else symbol

            # Remove timeframe subscription
            if (key in self.symbol_subscribers and
                timeframe in self.symbol_subscribers[key] and
                strategy_id in self.symbol_subscribers[key][timeframe]):

                self.symbol_subscribers[key][timeframe].remove(strategy_id)

                # If no more subscribers for this timeframe, remove it
                if not self.symbol_subscribers[key][timeframe]:
                    del self.symbol_subscribers[key][timeframe]

                # If no more timeframes, remove symbol
                if not self.symbol_subscribers[key]:
                    del self.symbol_subscribers[key]

                    # Check if we need to unsubscribe from market data
                    if key in self.active_subscriptions:
                        # Unsubscribe if no one else is using this symbol
                        should_unsubscribe = True
                        for tf, strategies in self.symbol_subscribers.get(key, {}).items():
                            if strategies:
                                should_unsubscribe = False
                                break

                        if should_unsubscribe:
                            self.unsubscribe_market_data(instrument)

                # Update reverse lookup
                if (timeframe in self.timeframe_subscriptions and
                    strategy_id in self.timeframe_subscriptions[timeframe] and
                    key in self.timeframe_subscriptions[timeframe][strategy_id]):

                    self.timeframe_subscriptions[timeframe][strategy_id].remove(key)

                    # If no more symbols, remove strategy
                    if not self.timeframe_subscriptions[timeframe][strategy_id]:
                        del self.timeframe_subscriptions[timeframe][strategy_id]

                self.logger.info(f"Strategy {strategy_id} unsubscribed from {timeframe} for {key}")
                return True
            else:
                self.logger.warning(f"No subscription found for {strategy_id} on {timeframe} for {key}")
                return False

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
            risk_free_rate = 0.06

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

    def _publish_timeframe_bar_event(self, symbol: str, timeframe: str, bar: Dict,
                                greeks: Optional[Dict] = None,
                                open_interest: Optional[float] = None,
                                instrument: Optional[Instrument] = None):
        """
        Publish a bar event for a specific timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar: Bar data
            greeks: Optional Greeks data
            open_interest: Optional open interest
            instrument: Optional instrument object
        """
        try:
            # Check if anyone is subscribed to this timeframe for this symbol
            if symbol not in self.symbol_subscribers or timeframe not in self.symbol_subscribers[symbol]:
                return

            # Create event data
            event_data = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': bar['timestamp'],
                'open': bar['open'],
                'high': bar['high'],
                'low': bar['low'],
                'close': bar['close'],
                'volume': bar['volume']
            }

            # Add Greeks if available
            if greeks:
                event_data['greeks'] = greeks

            # Add open interest if available
            if open_interest is not None:
                event_data['open_interest'] = open_interest

            # Add instrument details if available
            if instrument:
                event_data['instrument_type'] = instrument.instrument_type
                if instrument.instrument_type == InstrumentType.OPTION:
                    event_data['strike_price'] = getattr(instrument, 'strike', 0)
                    event_data['option_type'] = getattr(instrument, 'option_type', None)
                    event_data['expiry_date'] = getattr(instrument, 'expiry_date', None)

            # Create and publish event
            event = Event(
                event_type=f"MARKET_DATA_{timeframe.upper()}_BAR",
                data=event_data,
                timestamp=bar['timestamp']
            )

            # Publish to event bus
            self.event_bus.publish(event)

            # Notify subscribers directly
            for strategy_id in self.symbol_subscribers[symbol][timeframe]:
                # Create strategy-specific event
                strategy_event = Event(
                    event_type=f"STRATEGY_{strategy_id}_MARKET_DATA_{timeframe.upper()}_BAR",
                    data=event_data,
                    timestamp=bar['timestamp']
                )
                self.event_bus.publish(strategy_event)

        except Exception as e:
            self.logger.error(f"Error publishing timeframe bar event: {str(e)}")

    def _store_one_minute_bar(self, symbol: str, bar: Dict,
                            greeks: Optional[Dict] = None,
                            open_interest: Optional[float] = None):
        """
        Store a 1-minute bar for persistence.

        Args:
            symbol: Symbol of the instrument
            bar: Bar data
            greeks: Optional Greeks data
            open_interest: Optional open interest
        """
        try:
            # Create a copy of the bar data
            bar_data = bar.copy()

            # Add Greeks if available
            if greeks:
                for key, value in greeks.items():
                    bar_data[key] = value

            # Add open interest if available
            if open_interest is not None:
                bar_data['open_interest'] = open_interest

            # Convert to DataFrame row
            timestamp = bar_data.pop('timestamp')

            # Initialize storage for this symbol if needed
            if symbol not in self.one_minute_data:
                columns = list(bar_data.keys())
                self.one_minute_data[symbol] = pd.DataFrame(columns=columns)
                self.one_minute_data[symbol].index.name = 'timestamp'

            # Add to DataFrame
            new_row = pd.DataFrame([bar_data], index=[timestamp])

            # Update with lock to prevent race conditions
            with self._get_lock_for_symbol(symbol):
                self.one_minute_data[symbol] = pd.concat([self.one_minute_data[symbol], new_row])

                # Keep only the latest bars to limit memory usage
                self.one_minute_data[symbol] = self.one_minute_data[symbol].iloc[-self.cache_limit:]

        except Exception as e:
            self.logger.error(f"Error storing 1-minute bar: {str(e)}")

    def _get_lock_for_symbol(self, symbol: str):
        """
        Get a lock for a specific symbol.

        Args:
            symbol: Symbol to get lock for

        Returns:
            Lock object
        """
        # Create lock if it doesn't exist
        if symbol not in self.symbol_locks:
            self.symbol_locks[symbol] = threading.RLock()

        return self.symbol_locks[symbol]

    def on_market_data(self, event: Event):
        """
        Process market data event.

        Args:
            event: Market data event
        """
        try:
            data = event.data
            symbol = data.get('symbol')
            timestamp = event.timestamp

            if not symbol:
                self.logger.warning("Market data event missing symbol")
                return

            # Get instrument if available
            instrument = data.get('instrument')

            # Update last data
            self.last_data[symbol] = data

            # Update tick cache if enabled
            if self.cache_ticks:
                if symbol not in self.tick_cache:
                    self.tick_cache[symbol] = deque(maxlen=self.tick_cache_size)

                self.tick_cache[symbol].append((timestamp, data))

            # Store in Redis if enabled
            if self.use_redis:
                self._store_in_redis(symbol, data, timestamp)

            # Update OHLC data if present
            self._update_ohlc(symbol, data, timestamp, instrument)

        except Exception as e:
            self.logger.error(f"Error processing market data event: {str(e)}")

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
                    df_tf.set_index
