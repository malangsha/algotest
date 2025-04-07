import logging
import os
import json
import pandas as pd
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
import threading
import time

from utils.constants import MarketDataType
from utils.timeframe_manager import TimeframeManager
from models.instrument import Instrument
from models.events import Event, EventType, MarketDataEvent, BarEvent
from core.event_manager import EventManager

class DataManager:
    """
    Central manager for all market data needs across the framework.
    Stores, processes, and provides access to market data across different timeframes.
    """
    
    def __init__(self, config: Dict[str, Any], event_manager: EventManager, broker: Any):
        """
        Initialize the Market Data Manager.
        
        Args:
            config: Configuration dictionary
            event_manager: Event Manager instance for publishing/subscribing
            broker: Broker instance for market data
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.event_manager = event_manager
        self.broker = broker

        # Extract configuration
        data_config = config.get('market_data', {})
        self.persistence_enabled = data_config.get('persistence', {}).get('enabled', False)
        self.persistence_path = data_config.get('persistence', {}).get('path', './data')
        self.persistence_interval = data_config.get('persistence', {}).get('interval', 300)  # seconds
        self.use_cache = data_config.get('use_cache', True)
        self.cache_limit = data_config.get('cache_limit', 10000)  # per symbol
        self.use_redis = data_config.get('use_redis', False)
        
        # Data storage
        self.data_store = {}  # Symbol -> List of market data points
        self.last_data = {}   # Symbol -> Latest market data
        self.ohlc_data = {}   # Symbol -> OHLC data frames
        
        # For thread safety
        self.data_lock = threading.RLock()
        
        # Timeframe management
        max_bars = data_config.get('max_bars', 1000)
        self.timeframe_manager = TimeframeManager(max_bars=max_bars)
        
        # Strategy timeframe subscriptions
        # {timeframe: {strategy_id: set(symbols)}}
        self.timeframe_subscriptions = {}
        for timeframe in TimeframeManager.VALID_TIMEFRAMES:
            self.timeframe_subscriptions[timeframe] = {}
        
        # Persistence mechanism
        self.persistence_thread = None
        if self.persistence_enabled:
            os.makedirs(self.persistence_path, exist_ok=True)
            self.persistence_thread = threading.Thread(target=self._persistence_loop, daemon=True)
            self.persistence_thread.start()
            
        # Redis client if enabled
        self.redis_client = None
        if self.use_redis:
            try:
                import redis
                redis_config = data_config.get('redis', {})
                self.redis_client = redis.Redis(
                    host=redis_config.get('host', 'localhost'),
                    port=redis_config.get('port', 6379),
                    db=redis_config.get('db', 0)
                )
                self.logger.info("Redis connection established")
            except ImportError:
                self.logger.error("Redis package not installed. Please install 'redis' package.")
                self.use_redis = False
            except Exception as e:
                self.logger.error(f"Failed to connect to Redis: {str(e)}")
                self.use_redis = False
        
        # Register with Event Manager
        self._register_event_handlers()
        self.logger.info("Data Manager initialized")
    
    def _register_event_handlers(self):
        """Register to receive market data events from the event manager."""
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
        self.logger.info("Registered for market data events with Event Manager")       

    def _on_market_data(self, event: Event):
        """
        Handle incoming market data events.
        
        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent):
            self.logger.warning(f"Received non-market data event: {type(event)}")
            return
            
        try:
            # Process and store the data
            instrument = event.instrument
            data = event.data
            timestamp = event.timestamp
            
            symbol = instrument.symbol
            exchange = getattr(instrument, 'exchange', None)
            
            # Create a key that uniquely identifies this instrument
            key = f"{exchange}:{symbol}" if exchange else symbol
            
            with self.data_lock:
                # Store the latest data
                self.last_data[key] = data
                
                # Add to historical data store with appropriate size limit
                if key not in self.data_store:
                    self.data_store[key] = []
                
                # Add the data point
                self.data_store[key].append({
                    'timestamp': timestamp,
                    'data': data
                })
                
                # Enforce cache limit if needed
                if self.use_cache and len(self.data_store[key]) > self.cache_limit:
                    self.data_store[key] = self.data_store[key][-self.cache_limit:]
                
                # Determine what kind of data we have and publish appropriate events
                # First, check for tick data (individual trades)
                # Next, check for bar/OHLC data
                if (event.data_type == MarketDataType.BAR or 
                    MarketDataType.OHLC in data):
                    # Update OHLC data
                    self._update_ohlc(key, data, timestamp)
                
                # Process tick data through the timeframe manager if price is available
                if event.data_type == MarketDataType.TRADE and 'price' in data:
                    tick_data = {
                        'timestamp': timestamp,
                        'price': float(data['price']),
                        'volume': float(data.get('volume', 0))
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
                                
                                # Publish bar event to interested strategies
                                self._publish_bar_event(key, timeframe, bar)
                
            # Store in Redis if enabled
            if self.use_redis and self.redis_client:
                self._store_in_redis(key, data, timestamp)
                
        except Exception as e:
            self.logger.error(f"Error processing market data: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _publish_bar_event(self, symbol: str, timeframe: str, bar_data: Dict[str, Any]):
        """
        Publish a BarEvent for interested strategies.
        
        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar_data: Bar data to include in the event
        """
        # Check if any strategies are subscribed to this symbol and timeframe
        if timeframe not in self.timeframe_subscriptions:
            return
            
        # Extract exchange if embedded in symbol
        exchange = None
        if ":" in symbol:
            exchange, pure_symbol = symbol.split(":", 1)
        else:
            pure_symbol = symbol
            
        # Create instrument object
        instrument = Instrument(symbol=pure_symbol, exchange=exchange)
        
        # Create BarEvent
        bar_event = BarEvent(
            timestamp=int(bar_data.get('timestamp', time.time() * 1000)),
            instrument=instrument,
            timeframe=timeframe,
            open_price=bar_data.get('open', 0),
            high_price=bar_data.get('high', 0),
            low_price=bar_data.get('low', 0),
            close_price=bar_data.get('close', 0),
            volume=bar_data.get('volume', 0)
        )
        
        # Publish the event to all subscribers
        self.event_manager.publish(bar_event)
        
        # Log at debug level
        self.logger.debug(f"Published {timeframe} bar for {symbol}: {bar_data}")
    
    def subscribe_to_timeframe(self, strategy_id: str, symbol: str, timeframe: str = 'tick'):
        """
        Subscribe a strategy to receive market data for a specific symbol and timeframe.
        
        Args:
            strategy_id: ID of the strategy subscribing
            symbol: Symbol to subscribe to
            timeframe: Timeframe to subscribe to (default: tick)
            
        Returns:
            bool: True if subscription was successful
        """
        if timeframe not in TimeframeManager.VALID_TIMEFRAMES:
            self.logger.error(f"Invalid timeframe: {timeframe}")
            return False
            
        with self.data_lock:
            # Initialize if needed
            if strategy_id not in self.timeframe_subscriptions[timeframe]:
                self.timeframe_subscriptions[timeframe][strategy_id] = set()
                
            # Add the symbol to this strategy's subscriptions
            self.timeframe_subscriptions[timeframe][strategy_id].add(symbol)
            
            self.logger.info(f"Strategy {strategy_id} subscribed to {symbol} data on {timeframe} timeframe")
            return True
    
    def unsubscribe_from_timeframe(self, strategy_id: str, symbol: str = None, timeframe: str = None):
        """
        Unsubscribe a strategy from market data.
        
        Args:
            strategy_id: ID of the strategy to unsubscribe
            symbol: Optional symbol to unsubscribe from. If None, unsubscribes from all symbols.
            timeframe: Optional timeframe to unsubscribe from. If None, unsubscribes from all timeframes.
        """
        with self.data_lock:
            if timeframe is not None:
                if timeframe in self.timeframe_subscriptions:
                    if strategy_id in self.timeframe_subscriptions[timeframe]:
                        if symbol is not None:
                            self.timeframe_subscriptions[timeframe][strategy_id].discard(symbol)
                            self.logger.info(f"Strategy {strategy_id} unsubscribed from {symbol} on {timeframe}")
                        else:
                            self.timeframe_subscriptions[timeframe].pop(strategy_id, None)
                            self.logger.info(f"Strategy {strategy_id} unsubscribed from all symbols on {timeframe}")
            else:
                # Unsubscribe from all timeframes
                for tf in self.timeframe_subscriptions:
                    if strategy_id in self.timeframe_subscriptions[tf]:
                        if symbol is not None:
                            self.timeframe_subscriptions[tf][strategy_id].discard(symbol)
                        else:
                            self.timeframe_subscriptions[tf].pop(strategy_id, None)
                self.logger.info(f"Strategy {strategy_id} unsubscribed from {'all symbols' if symbol is None else symbol} on all timeframes")
    
    def get_subscribed_symbols(self, strategy_id: str, timeframe: str = None) -> Dict[str, Set[str]]:
        """
        Get the symbols a strategy is subscribed to.
        
        Args:
            strategy_id: ID of the strategy
            timeframe: Optional timeframe to filter by
            
        Returns:
            Dictionary mapping timeframes to sets of subscribed symbols
        """
        result = {}
        
        with self.data_lock:
            if timeframe is not None:
                if timeframe in self.timeframe_subscriptions and strategy_id in self.timeframe_subscriptions[timeframe]:
                    result[timeframe] = set(self.timeframe_subscriptions[timeframe][strategy_id])
            else:
                for tf in self.timeframe_subscriptions:
                    if strategy_id in self.timeframe_subscriptions[tf]:
                        result[tf] = set(self.timeframe_subscriptions[tf][strategy_id])
                        
        return result

    def _handle_system_event(self, event: Event):
        """
        Handle system events from the event manager.
        
        Args:
            event: System event
        """
        try:
            # Extract event details
            system_event_type = getattr(event, 'system_event_type', None)
            message = getattr(event, 'message', '')
            severity = getattr(event, 'severity', 'INFO')
            data = getattr(event, 'data', {})
            
            self.logger.debug(f"Received system event: {system_event_type} - {message}")
            
            # Handle different system event types
            if system_event_type == 'start':
                # System is starting up
                self.logger.info("System starting - preparing data manager")
                # Any initialization that should happen on system start
                
            elif system_event_type == 'shutdown':
                # System is shutting down
                self.logger.info("System shutting down - persisting data")
                # Save any pending data before shutdown
                if self.persistence_enabled:
                    self._persist_all_data()
            
            elif system_event_type == 'heartbeat':
                # Regular heartbeat event
                pass  # Nothing specific to do for heartbeats
                
            else:
                # Handle other system events as needed
                self.logger.debug(f"Unhandled system event type: {system_event_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling system event: {str(e)}")
    
    def _handle_custom_event(self, event: Event):
        """
        Handle custom events from the event manager.
        
        Args:
            event: Custom event
        """
        try:
            # Check if this is a data-related custom event
            event_subtype = getattr(event, 'subtype', None)
            
            if event_subtype == 'data_request':
                # Handle data request
                request_type = getattr(event, 'request_type', None)
                symbol = getattr(event, 'symbol', None)
                timeframe = getattr(event, 'timeframe', 'tick')
                limit = getattr(event, 'limit', 100)
                
                if request_type == 'historical_data':
                    # Request for historical data
                    data = self.get_historical_data(symbol, getattr(event, 'exchange', None), 
                                             getattr(event, 'start_time', None),
                                             getattr(event, 'end_time', None),
                                             limit)
                    
                    # Create response event
                    from models.events import CustomEvent
                    response = CustomEvent(
                        subtype='data_response',
                        request_id=getattr(event, 'request_id', None),
                        data=data
                    )
                    self.event_manager.publish(response)
                    
                elif request_type == 'ohlc':
                    # Request for OHLC data
                    data = self.get_ohlc(symbol, getattr(event, 'exchange', None), 
                               timeframe, limit)
                    
                    # Create response event
                    from models.events import CustomEvent
                    response = CustomEvent(
                        subtype='data_response',
                        request_id=getattr(event, 'request_id', None),
                        data=data.to_dict() if hasattr(data, 'to_dict') else data
                    )
                    self.event_manager.publish(response)
            
        except Exception as e:
            self.logger.error(f"Error handling custom event: {str(e)}")
    
    def _update_ohlc(self, symbol: str, data: Dict, timestamp: float):
        """
        Update OHLC data from a market data event.
        
        Args:
            symbol: Symbol of the instrument
            data: Market data dictionary
            timestamp: Event timestamp
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
                    self.ohlc_data[symbol][timeframe] = pd.DataFrame(
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                    )
                    self.ohlc_data[symbol][timeframe].set_index('timestamp', inplace=True)
                
                # Add the bar to the DataFrame
                bar = {
                    'timestamp': timestamp,
                    'open': float(ohlc.get('open', 0)),
                    'high': float(ohlc.get('high', 0)),
                    'low': float(ohlc.get('low', 0)),
                    'close': float(ohlc.get('close', 0)),
                    'volume': float(ohlc.get('volume', 0))
                }
                
                # Add bar to DataFrame
                new_row = pd.DataFrame([bar])
                new_row.set_index('timestamp', inplace=True)
                
                # Concatenate with existing data
                self.ohlc_data[symbol][timeframe] = pd.concat(
                    [self.ohlc_data[symbol][timeframe], new_row]
                ).iloc[-self.cache_limit:]
                
        except Exception as e:
            self.logger.error(f"Error updating OHLC data: {str(e)}")
    
    def _store_in_redis(self, symbol: str, data: Dict, timestamp: float):
        """
        Store market data in Redis.
        
        Args:
            symbol: Symbol of the instrument
            data: Market data dictionary
            timestamp: Event timestamp
        """
        try:
            # Store last price in Redis
            if 'price' in data:
                key = f"marketdata:{symbol}:last_price"
                value = str(data['price'])
                self.redis_client.set(key, value)
                
                # Also store with timestamp
                key = f"marketdata:{symbol}:tick:{int(timestamp)}"
                self.redis_client.hmset(key, {
                    'price': str(data.get('price', 0)),
                    'volume': str(data.get('volume', 0)),
                    'timestamp': str(int(timestamp))
                })
                # Set expiry (1 day)
                self.redis_client.expire(key, 86400)
                
            # Store OHLC data if available
            if MarketDataType.OHLC in data:
                ohlc = data[MarketDataType.OHLC]
                timeframe = ohlc.get('timeframe', '1m')
                key = f"marketdata:{symbol}:ohlc:{timeframe}:{int(timestamp)}"
                
                self.redis_client.hmset(key, {
                    'open': str(ohlc.get('open', 0)),
                    'high': str(ohlc.get('high', 0)),
                    'low': str(ohlc.get('low', 0)),
                    'close': str(ohlc.get('close', 0)),
                    'volume': str(ohlc.get('volume', 0)),
                    'timestamp': str(int(timestamp))
                })
                # Set expiry (30 days)
                self.redis_client.expire(key, 2592000)
                
        except Exception as e:
            self.logger.error(f"Error storing in Redis: {str(e)}")
            
    def _persistence_loop(self):
        """Background thread for periodic data persistence."""
        while True:
            time.sleep(self.persistence_interval)
            if self.persistence_enabled:
                self._persist_all_data()
    
    def _persist_all_data(self):
        """Persist all market data to disk."""
        try:
            for symbol in self.ohlc_data:
                self._persist_symbol_data(symbol)
        except Exception as e:
            self.logger.error(f"Error persisting all data: {str(e)}")
    
    def _persist_symbol_data(self, symbol: str):
        """
        Persist market data for a specific symbol.
        
        Args:
            symbol: Symbol to persist data for
        """
        try:
            # Create directory if needed
            symbol_path = os.path.join(self.persistence_path, symbol)
            os.makedirs(symbol_path, exist_ok=True)
            
            # Persist last data
            if symbol in self.last_data:
                last_data_path = os.path.join(symbol_path, "last_data.json")
                with open(last_data_path, 'w') as f:
                    # Create a serializable version with timestamp
                    serializable_data = {
                        'timestamp': time.time(),
                        'data': self.last_data[symbol]
                    }
                    json.dump(serializable_data, f, default=str)
            
            # Persist OHLC data
            if symbol in self.ohlc_data:
                for timeframe, df in self.ohlc_data[symbol].items():
                    if not df.empty:
                        ohlc_path = os.path.join(symbol_path, f"ohlc_{timeframe}.csv")
                        df.to_csv(ohlc_path)
            
            # Also persist timeframe manager data if available
            for timeframe in TimeframeManager.VALID_TIMEFRAMES:
                if timeframe == 'tick':
                    continue
                    
                df = self.timeframe_manager.get_bars(symbol, timeframe)
                if df is not None and not df.empty:
                    tm_path = os.path.join(symbol_path, f"tm_{timeframe}.csv")
                    df.to_csv(tm_path)
                    
            self.logger.debug(f"Persisted data for {symbol}")
            
        except Exception as e:
            self.logger.error(f"Error persisting data for {symbol}: {str(e)}")
    
    def get_latest_data(self, symbol: str, exchange: str = None) -> Optional[Dict]:
        """
        Get the latest market data for a symbol.
        
        Args:
            symbol: Symbol to get data for
            exchange: Optional exchange
            
        Returns:
            Latest market data or None if not available
        """
        key = f"{exchange}:{symbol}" if exchange else symbol
        
        with self.data_lock:
            return self.last_data.get(key)
    
    def get_historical_data(self, symbol: str, exchange: str = None, 
                      start_time: Optional[float] = None, 
                      end_time: Optional[float] = None, 
                      limit: int = 100) -> List[Dict]:
        """
        Get historical market data for a symbol.
        
        Args:
            symbol: Symbol to get data for
            exchange: Optional exchange
            start_time: Optional start time (unix timestamp)
            end_time: Optional end time (unix timestamp)
            limit: Maximum number of data points to return
            
        Returns:
            List of historical data points
        """
        key = f"{exchange}:{symbol}" if exchange else symbol
        
        with self.data_lock:
            if key not in self.data_store:
                return []
                
            data = self.data_store[key]
            
            # Filter by time if specified
            if start_time is not None:
                data = [d for d in data if d['timestamp'] >= start_time]
                
            if end_time is not None:
                data = [d for d in data if d['timestamp'] <= end_time]
                
            # Apply limit
            if limit and len(data) > limit:
                data = data[-limit:]
                
            return data
    
    def get_ohlc(self, symbol: str, exchange: str = None, 
           timeframe: str = '1m', limit: int = 100) -> pd.DataFrame:
        """
        Get OHLC data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to get data for
            exchange: Optional exchange
            timeframe: Timeframe of the OHLC data (e.g., '1m', '5m', '1h')
            limit: Maximum number of bars to return
            
        Returns:
            DataFrame of OHLC data or empty DataFrame if not available
        """
        key = f"{exchange}:{symbol}" if exchange else symbol
        
        with self.data_lock:
            # First check the timeframe manager
            tm_data = self.timeframe_manager.get_bars(key, timeframe, limit)
            if tm_data is not None and not tm_data.empty:
                return tm_data
            
            # Fall back to stored OHLC data
            if key in self.ohlc_data and timeframe in self.ohlc_data[key]:
                df = self.ohlc_data[key][timeframe]
                if not df.empty:
                    if limit and len(df) > limit:
                        return df.iloc[-limit:]
                    return df
            
            # Return empty DataFrame if no data found
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    
    def get_instrument_details(self, symbol: str, exchange: str = None) -> Dict:
        """
        Get details for an instrument.
        
        Args:
            symbol: Symbol to get details for
            exchange: Optional exchange
            
        Returns:
            Dictionary of instrument details
        """
        # Implementation depends on how instrument details are stored
        # This is a placeholder that would be implemented to fetch instrument details
        # from the broker or a local database
        return {'symbol': symbol, 'exchange': exchange}
    
    def get_available_timeframes(self, symbol: str, exchange: str = None) -> List[str]:
        """
        Get a list of timeframes that have data for a given symbol.
        
        Args:
            symbol: Symbol to check
            exchange: Optional exchange
            
        Returns:
            List of available timeframes
        """
        key = f"{exchange}:{symbol}" if exchange else symbol
        
        with self.data_lock:
            # Check timeframe manager first
            tm_timeframes = self.timeframe_manager.get_available_timeframes(key)
            
            # Also check stored OHLC data
            ohlc_timeframes = []
            if key in self.ohlc_data:
                ohlc_timeframes = list(self.ohlc_data[key].keys())
                
            # Combine and deduplicate
            return list(set(tm_timeframes + ohlc_timeframes))
    
    def stop(self):
        """Stop the data manager and perform cleanup."""
        if self.persistence_enabled:
            self._persist_all_data()
            
        self.logger.info("Data Manager stopped")