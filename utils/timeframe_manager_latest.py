import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta, time as dt_time
import threading
import time
import pytz
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

from core.logging_manager import get_logger
from utils.constants import MarketDataType


class SessionType(Enum):
    """Market session types for Indian markets"""
    REGULAR = "regular"
    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"
    MUHURAT = "muhurat"
    CLOSED = "closed"


@dataclass
class SessionTiming:
    """Session timing definition"""
    start_time: dt_time
    end_time: dt_time
    session_type: SessionType
    name: str


class MarketSessionManager:
    """Manages Indian market sessions for NSE and BSE"""
    
    def __init__(self):
        self.timezone = pytz.timezone('Asia/Kolkata')
        self.sessions = {
            'NSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "NSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "NSE Regular"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "NSE Post-Market"),
            ],
            'BSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "BSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "BSE Regular"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "BSE Post-Market"),
            ]
        }
        
        # Market holidays (sample - should be updated from exchange calendar)
        self.market_holidays = set([
            '2024-01-26',  # Republic Day
            '2024-03-08',  # Holi
            '2024-03-29',  # Good Friday
            # Add more holidays as needed
        ])
    
    def get_session_type(self, timestamp: float, exchange: str = 'NSE') -> SessionType:
        """Get the market session type for a given timestamp"""
        try:
            dt = datetime.fromtimestamp(timestamp, self.timezone)
            date_str = dt.strftime('%Y-%m-%d')
            
            # Check if it's a market holiday
            if date_str in self.market_holidays:
                return SessionType.CLOSED
            
            # Check if it's a weekend
            if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
                return SessionType.CLOSED
            
            current_time = dt.time()
            
            # Check sessions for the exchange
            for session in self.sessions.get(exchange, self.sessions['NSE']):
                if session.start_time <= current_time <= session.end_time:
                    return session.session_type
            
            return SessionType.CLOSED
            
        except Exception:
            return SessionType.CLOSED
    
    def is_market_open(self, timestamp: float, exchange: str = 'NSE') -> bool:
        """Check if the market is open for regular trading"""
        session_type = self.get_session_type(timestamp, exchange)
        return session_type == SessionType.REGULAR
    
    def get_market_day_start(self, timestamp: float, exchange: str = 'NSE') -> float:
        """Get the start of the market day (9:15 AM IST) for a given timestamp"""
        dt = datetime.fromtimestamp(timestamp, self.timezone)
        market_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
        return market_start.timestamp()


class DataValidator:
    """Validates market data for quality and circuit breaker rules"""
    
    def __init__(self):
        self.logger = get_logger("utils.data_validator")
        # Circuit breaker limits (5%, 10%, 20% for different scrips)
        self.circuit_limits = {
            'GROUP_A': 0.20,  # 20% circuit limit
            'GROUP_B': 0.10,  # 10% circuit limit
            'DEFAULT': 0.05   # 5% circuit limit
        }
        
        # Price history for validation
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.lock = threading.RLock()
    
    def validate_tick(self, symbol: str, market_data: Dict[str, Any], 
                     last_close: Optional[float] = None) -> Tuple[bool, str]:
        """
        Validate a tick for data quality issues
        
        Returns:
            Tuple[bool, str]: (is_valid, reason_if_invalid)
        """
        price = market_data.get(MarketDataType.LAST_PRICE.value)
        volume = market_data.get(MarketDataType.VOLUME.value, 0)
        
        if price is None or price <= 0:
            return False, "Invalid price"
        
        if volume < 0:
            return False, "Invalid volume"
        
        # Circuit breaker check if we have last close
        if last_close and last_close > 0:
            price_change_pct = abs(price - last_close) / last_close
            limit = self.circuit_limits.get(self._get_scrip_group(symbol), 
                                           self.circuit_limits['DEFAULT'])
            
            if price_change_pct > limit:
                return False, f"Circuit breaker violation: {price_change_pct:.2%} > {limit:.2%}"
        
        # Volume spike detection
        with self.lock:
            if symbol in self.price_history:
                recent_prices = list(self.price_history[symbol])
                if len(recent_prices) >= 10:
                    avg_price = sum(recent_prices[-10:]) / 10
                    if abs(price - avg_price) / avg_price > 0.1:  # 10% deviation
                        self.logger.warning(f"Significant price deviation for {symbol}: "
                                          f"current={price}, avg={avg_price:.2f}")
            
            self.price_history[symbol].append(price)
        
        return True, ""
    
    def _get_scrip_group(self, symbol: str) -> str:
        """Determine scrip group for circuit breaker limits"""
        # Simple heuristic - in production, this should be from market data
        if 'NSE:' in symbol and any(x in symbol for x in ['RELIANCE', 'TCS', 'INFY', 'HDFC']):
            return 'GROUP_A'
        return 'DEFAULT'


class PerformanceMonitor:
    """Monitors performance metrics for the TimeframeManager"""
    
    def __init__(self):
        self.metrics = {
            'bars_completed': defaultdict(int),
            'ticks_processed': defaultdict(int),
            'processing_latency': defaultdict(list),
            'memory_usage': defaultdict(int),
            'errors': defaultdict(int),
        }
        self.lock = threading.RLock()
        self.start_time = time.time()
    
    def record_bar_completion(self, symbol: str, timeframe: str):
        """Record a bar completion event"""
        with self.lock:
            self.metrics['bars_completed'][f"{symbol}:{timeframe}"] += 1
    
    def record_tick_processing(self, symbol: str, processing_time: float):
        """Record tick processing time"""
        with self.lock:
            self.metrics['ticks_processed'][symbol] += 1
            self.metrics['processing_latency'][symbol].append(processing_time)
            # Keep only last 1000 measurements
            if len(self.metrics['processing_latency'][symbol]) > 1000:
                self.metrics['processing_latency'][symbol] = \
                    self.metrics['processing_latency'][symbol][-1000:]
    
    def record_error(self, symbol: str, error_type: str):
        """Record an error event"""
        with self.lock:
            self.metrics['errors'][f"{symbol}:{error_type}"] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get performance statistics"""
        with self.lock:
            stats = {
                'uptime_seconds': time.time() - self.start_time,
                'total_bars_completed': sum(self.metrics['bars_completed'].values()),
                'total_ticks_processed': sum(self.metrics['ticks_processed'].values()),
                'total_errors': sum(self.metrics['errors'].values()),
            }
            
            # Calculate average latency per symbol
            latency_stats = {}
            for symbol, latencies in self.metrics['processing_latency'].items():
                if latencies:
                    latency_stats[symbol] = {
                        'avg_ms': np.mean(latencies) * 1000,
                        'max_ms': np.max(latencies) * 1000,
                        'min_ms': np.min(latencies) * 1000,
                        'p95_ms': np.percentile(latencies, 95) * 1000 if len(latencies) > 20 else 0
                    }
            stats['latency_by_symbol'] = latency_stats
            
            return stats


class TimeframeManager:
    """
    Enhanced TimeframeManager with market session awareness, improved thread safety,
    data validation, and performance monitoring for Indian markets.
    """

    # Supported timeframes with Indian market focus
    SUPPORTED_TIMEFRAMES: Dict[str, int] = {
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '4h': 14400, '1d': 86400,
    }

    BAR_COLUMNS: List[str] = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest']

    def __init__(self, 
                 max_bars_in_memory: int = 10000,
                 enable_market_session_validation: bool = True,
                 enable_data_validation: bool = True,
                 enable_performance_monitoring: bool = True,
                 default_exchange: str = 'NSE'):
        """
        Initialize the Enhanced TimeframeManager.

        Args:
            max_bars_in_memory: Maximum bars to keep in memory per symbol/timeframe
            enable_market_session_validation: Enable market session checks
            enable_data_validation: Enable data quality validation
            enable_performance_monitoring: Enable performance metrics collection
            default_exchange: Default exchange (NSE or BSE)
        """
        self.logger = get_logger("utils.enhanced_timeframe_manager")
        self.max_bars_in_memory = max_bars_in_memory
        self.default_exchange = default_exchange
        
        # Initialize components
        self.market_session_manager = MarketSessionManager() if enable_market_session_validation else None
        self.data_validator = DataValidator() if enable_data_validation else None
        self.performance_monitor = PerformanceMonitor() if enable_performance_monitoring else None
        
        # Thread-safe data structures with fine-grained locking
        self.completed_bars_buffer: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.max_bars_in_memory))
        )
        self.bars_df_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        self.current_bars: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        self.last_tick_time: Dict[str, float] = {}
        self.last_close_prices: Dict[str, float] = {}  # For circuit breaker validation
        
        # Separate locks for different data structures
        self.current_bars_lock = threading.RLock()
        self.completed_bars_lock = threading.RLock()
        self.cache_lock = threading.RLock()
        self.general_lock = threading.RLock()
        
        # Symbol metadata cache for performance
        self.symbol_metadata: Dict[str, Dict[str, Any]] = {}
        self.symbol_metadata_lock = threading.RLock()
        
        # Market session cache for performance optimization
        self.session_cache: Dict[str, Dict[str, Any]] = {}  # exchange -> {is_open, next_transition, last_check}
        self.session_cache_lock = threading.RLock()
        self.session_cache_validity = 300  # Cache validity in seconds (5 minutes max)

        
        # Memory monitoring
        self.memory_warning_threshold = max_bars_in_memory * 0.8
        
        self.logger.info(f"Enhanced TimeframeManager initialized. "
                        f"Market validation: {enable_market_session_validation}, "
                        f"Data validation: {enable_data_validation}, "
                        f"Performance monitoring: {enable_performance_monitoring}")

    def _is_market_open_cached(self, timestamp_sec: float, exchange: str) -> bool:
        """
        Check if market is open using cached session state for performance.
        Only calls the full market session check when crossing session boundaries.
        """
        if not self.market_session_manager:
            return True
            
        with self.session_cache_lock:
            cache_entry = self.session_cache.get(exchange)
            current_time = timestamp_sec
            
            # Check if we need to refresh the cache
            needs_refresh = (
                cache_entry is None or
                current_time >= cache_entry.get('next_transition', 0) or
                current_time - cache_entry.get('last_check', 0) > self.session_cache_validity
            )
            
            if needs_refresh:
                # Refresh cache by checking actual market status
                is_open = self.market_session_manager.is_market_open(timestamp_sec, exchange)
                session_type = self.market_session_manager.get_session_type(timestamp_sec, exchange)
                
                # Calculate next transition time
                next_transition = self._calculate_next_session_transition(timestamp_sec, exchange)
                
                # Update cache
                self.session_cache[exchange] = {
                    'is_open': is_open,
                    'session_type': session_type,
                    'next_transition': next_transition,
                    'last_check': current_time
                }
                
                return is_open
            else:
                # Use cached result
                return cache_entry['is_open']
    
    def _calculate_next_session_transition(self, timestamp_sec: float, exchange: str) -> float:
        """Calculate the next time the market session will change"""
        try:
            dt = datetime.fromtimestamp(timestamp_sec, self.market_session_manager.timezone)
            
            # For simplicity, check every hour boundary during market days
            # In production, this could be more precise based on actual session timings
            next_hour = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            return next_hour.timestamp()
        except Exception:
            # Fallback: check again in 1 hour
            return timestamp_sec + 3600

    def ensure_timeframe_tracked(self, symbol: str, timeframe: str) -> None:
        """Ensure that a specific symbol and timeframe combination is tracked"""
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for symbol '{symbol}'")
            return
        
        with self.current_bars_lock:
            # Initialize data structures if they don't exist
            if symbol not in self.current_bars:
                self.current_bars[symbol] = {}
            
            if timeframe not in self.current_bars[symbol]:
                self.logger.info(f"Started tracking '{timeframe}' for symbol '{symbol}'")
        
        with self.completed_bars_lock:
            _ = self.completed_bars_buffer[symbol][timeframe]  # Initialize if needed
        
        with self.cache_lock:
            _ = self.bars_df_cache[symbol]  # Initialize if needed

    def process_tick(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, bool]:
        """
        Process a market data tick with enhanced validation and monitoring.
        
        Args:
            symbol: Symbol identifier (e.g., 'NSE:RELIANCE')
            market_data: Market data dictionary
            
        Returns:
            Dictionary mapping timeframes to completion status
        """
        start_time = time.time()
        
        try:
            # Extract and validate basic data
            price = market_data.get(MarketDataType.LAST_PRICE.value)
            timestamp_sec = market_data.get(MarketDataType.TIMESTAMP.value)
            volume = market_data.get(MarketDataType.VOLUME.value, 0.0)
            open_interest = market_data.get(MarketDataType.OPEN_INTEREST.value)
            
            # Handle missing price using bid/ask
            if price is None:
                bid = market_data.get(MarketDataType.BID.value)
                ask = market_data.get(MarketDataType.ASK.value)
                if bid and ask and bid > 0 and ask > 0:
                    price = (bid + ask) / 2.0
                    self.logger.debug(f"Using mid-price {price:.2f} for {symbol}")
                else:
                    self.logger.warning(f"No valid price for {symbol}")
                    if self.performance_monitor:
                        self.performance_monitor.record_error(symbol, "invalid_price")
                    return {}
            
            # Handle missing timestamp
            if timestamp_sec is None:
                timestamp_sec = time.time()
                self.logger.warning(f"Using system time for {symbol}")
            
            # Type conversion with error handling
            try:
                price = float(price)
                timestamp_sec = float(timestamp_sec)
                volume = float(volume) if volume is not None else 0.0
                open_interest = float(open_interest) if open_interest is not None else None
            except (ValueError, TypeError) as e:
                self.logger.error(f"Type conversion error for {symbol}: {e}")
                if self.performance_monitor:
                    self.performance_monitor.record_error(symbol, "type_conversion")
                return {}
            
            # Market session validation
            if self.market_session_manager:
                exchange = self._extract_exchange_from_symbol(symbol)                
                if not self._is_market_open_cached(timestamp_sec, exchange):
                    session_type = self.session_cache.get(exchange, {}).get('session_type', 'unknown')
                    self.logger.debug(f"Market closed for {symbol}, session: {session_type}")
                    return {}
            
            # Data quality validation
            if self.data_validator:
                last_close = self.last_close_prices.get(symbol)
                is_valid, reason = self.data_validator.validate_tick(symbol, market_data, last_close)
                if not is_valid:
                    self.logger.warning(f"Invalid tick for {symbol}: {reason}")
                    if self.performance_monitor:
                        self.performance_monitor.record_error(symbol, "validation_failed")
                    return {}
            
            # Out-of-order tick detection
            with self.general_lock:
                if symbol in self.last_tick_time:
                    time_diff = timestamp_sec - self.last_tick_time[symbol]
                    if time_diff < -0.5:  # Allow small negative differences
                        self.logger.debug(f"Out-of-order tick for {symbol}: {time_diff:.3f}s")
                        return {}
                    if time_diff > 300:  # Log significant gaps
                        self.logger.info(f"Time gap for {symbol}: {time_diff:.2f}s")
                
                self.last_tick_time[symbol] = timestamp_sec
                # Update last close price for validation
                self.last_close_prices[symbol] = price
            
            # Process tick for each tracked timeframe
            completed_bars_status = {}
            
            with self.current_bars_lock:
                if symbol not in self.current_bars:
                    return {}
                
                for timeframe in list(self.current_bars[symbol].keys()):
                    tf_seconds = self.SUPPORTED_TIMEFRAMES.get(timeframe)
                    if tf_seconds is None:
                        continue
                    
                    # Get market-aware bar start time
                    bar_start_timestamp = self._get_market_aware_bar_start(
                        timestamp_sec, tf_seconds, symbol
                    )
                    
                    current_bar = self.current_bars[symbol].get(timeframe)
                    
                    if current_bar is None:
                        # Initialize first bar
                        self.current_bars[symbol][timeframe] = self._create_new_bar(
                            bar_start_timestamp, price, volume, open_interest
                        )
                        completed_bars_status[timeframe] = False
                        
                    elif bar_start_timestamp > current_bar['timestamp']:
                        # Complete current bar and start new one
                        with self.completed_bars_lock:
                            self.completed_bars_buffer[symbol][timeframe].append(current_bar)
                        
                        completed_bars_status[timeframe] = True
                        
                        # Start new bar
                        self.current_bars[symbol][timeframe] = self._create_new_bar(
                            bar_start_timestamp, price, volume, open_interest
                        )
                        
                        # Invalidate cache
                        with self.cache_lock:
                            self.bars_df_cache[symbol].pop(timeframe, None)
                        
                        # Record completion in monitor
                        if self.performance_monitor:
                            self.performance_monitor.record_bar_completion(symbol, timeframe)
                            
                    elif bar_start_timestamp == current_bar['timestamp']:
                        # Update current bar
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume
                        if open_interest is not None:
                            current_bar['open_interest'] = open_interest
                        completed_bars_status[timeframe] = False
                        
                    else:
                        # Out-of-order tick for this timeframe
                        self.logger.warning(f"Out-of-order tick for {symbol} [{timeframe}]")
                        completed_bars_status[timeframe] = False
            
            # Record performance metrics
            if self.performance_monitor:
                processing_time = time.time() - start_time
                self.performance_monitor.record_tick_processing(symbol, processing_time)
            
            return completed_bars_status
            
        except Exception as e:
            self.logger.error(f"Error processing tick for {symbol}: {e}", exc_info=True)
            if self.performance_monitor:
                self.performance_monitor.record_error(symbol, "processing_error")
            return {}

    def _extract_exchange_from_symbol(self, symbol: str) -> str:
        """Extract exchange from symbol (e.g., 'NSE:RELIANCE' -> 'NSE')"""
        if ':' in symbol:
            return symbol.split(':', 1)[0]
        return self.default_exchange

    def _get_market_aware_bar_start(self, tick_timestamp: float, 
                                   timeframe_seconds: int, symbol: str) -> float:
        """
        Calculate market-aware bar start timestamp with timezone support.
        For daily bars, aligns to market day start (9:15 AM IST).
        """
        if timeframe_seconds <= 0:
            return tick_timestamp
        
        # For daily bars, align to market day start
        if timeframe_seconds >= 86400:  # Daily or longer
            if self.market_session_manager:
                exchange = self._extract_exchange_from_symbol(symbol)
                return self.market_session_manager.get_market_day_start(tick_timestamp, exchange)
        
        # For intraday bars, use standard alignment
        return float((int(tick_timestamp) // timeframe_seconds) * timeframe_seconds)

    def _create_new_bar(self, timestamp: float, price: float, 
                       volume: float, open_interest: Optional[float]) -> Dict[str, Any]:
        """Create a new bar dictionary with validation"""
        return {
            'timestamp': timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': max(0, volume),  # Ensure non-negative volume
            'open_interest': open_interest
        }

    def _build_dataframe_from_buffer(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Build DataFrame from completed bars buffer with error handling"""
        with self.completed_bars_lock:
            buffer = self.completed_bars_buffer[symbol][timeframe]
            if not buffer:
                return None
            
            try:
                # Convert buffer to DataFrame
                data = []
                for bar in buffer:
                    # Validate bar data before adding
                    if self._validate_bar_data(bar):
                        data.append(bar)
                
                if not data:
                    return None
                
                df = pd.DataFrame(data, columns=self.BAR_COLUMNS)
                
                # Convert timestamp to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=True, drop=False)
                
                # Sort by timestamp (though should already be ordered)
                df.sort_index(inplace=True)
                
                # Handle any data type issues
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Fill any NaN values in open_interest
                df['open_interest'] = df['open_interest'].fillna(method='ffill')
                
                self.logger.debug(f"Built DataFrame for {symbol} [{timeframe}] with {len(df)} bars")
                return df
                
            except Exception as e:
                self.logger.error(f"Error building DataFrame for {symbol} [{timeframe}]: {e}", 
                                exc_info=True)
                return None

    def _validate_bar_data(self, bar: Dict[str, Any]) -> bool:
        """Validate bar data for consistency"""
        try:
            # Check required fields
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in bar or bar[field] is None:
                    return False
            
            # Check OHLC consistency
            if not (bar['low'] <= bar['open'] <= bar['high'] and 
                   bar['low'] <= bar['close'] <= bar['high']):
                return False
            
            # Check non-negative volume
            if bar['volume'] < 0:
                return False
            
            return True
            
        except Exception:
            return False

    def get_bars(self, symbol: str, timeframe: str, 
                limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Get historical bars with enhanced caching and validation.
        
        Args:
            symbol: Symbol identifier
            timeframe: Timeframe identifier  
            limit: Optional limit on number of bars to return
            
        Returns:
            DataFrame of OHLCV bars or None if no data
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for {symbol}")
            return None
        
        # Check cache first
        with self.cache_lock:
            cached_df = self.bars_df_cache.get(symbol, {}).get(timeframe)
            if cached_df is not None:
                df_to_return = cached_df
            else:
                # Build from buffer
                df_to_return = self._build_dataframe_from_buffer(symbol, timeframe)
                if df_to_return is not None:
                    # Cache the result
                    if symbol not in self.bars_df_cache:
                        self.bars_df_cache[symbol] = {}
                    self.bars_df_cache[symbol][timeframe] = df_to_return
        
        # Handle empty result
        if df_to_return is None or df_to_return.empty:
            empty_df = pd.DataFrame(columns=self.BAR_COLUMNS)
            empty_df.set_index(pd.to_datetime([]), inplace=True)
            return empty_df
        
        # Apply limit if specified
        if limit is not None and limit > 0:
            return df_to_return.iloc[-limit:].copy()
        elif limit == 0:
            empty_df = pd.DataFrame(columns=df_to_return.columns)
            empty_df.index = pd.to_datetime([])
            return empty_df
        else:
            return df_to_return.copy()

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Get the currently forming bar for a symbol and timeframe"""
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            return None
        
        with self.current_bars_lock:
            current_bar = self.current_bars.get(symbol, {}).get(timeframe)
            return current_bar.copy() if current_bar else None

    def reset(self, symbol: Optional[str] = None) -> None:
        """Reset stored data for a symbol or all symbols"""
        locks = [self.current_bars_lock, self.completed_bars_lock, 
                self.cache_lock, self.general_lock]
        
        for lock in locks:
            lock.acquire()
        
        try:
            if symbol:
                # Reset data for specific symbol
                self.completed_bars_buffer.pop(symbol, None)
                self.bars_df_cache.pop(symbol, None)
                self.current_bars.pop(symbol, None)
                self.last_tick_time.pop(symbol, None)
                self.last_close_prices.pop(symbol, None)
                
                with self.symbol_metadata_lock:
                    self.symbol_metadata.pop(symbol, None)
                
                self.logger.info(f"Reset data for symbol: {symbol}")
            else:
                # Reset all data
                self.completed_bars_buffer.clear()
                self.bars_df_cache.clear()
                self.current_bars.clear()
                self.last_tick_time.clear()
                self.last_close_prices.clear()
                
                with self.symbol_metadata_lock:
                    self.symbol_metadata.clear()
                
                # Reset session cache
                with self.session_cache_lock:
                    self.session_cache.clear()
            
                self.logger.info("Reset all TimeframeManager data")
        finally:
            for lock in reversed(locks):
                lock.release()

    def get_performance_statistics(self) -> Dict[str, Any]:
        """Get performance statistics if monitoring is enabled"""
        if self.performance_monitor:
            return self.performance_monitor.get_statistics()
        return {}

    def get_memory_usage_info(self) -> Dict[str, int]:
        """Get memory usage information"""
        memory_info = {}
        
        with self.completed_bars_lock:
            for symbol, timeframes in self.completed_bars_buffer.items():
                for timeframe, buffer in timeframes.items():
                    key = f"{symbol}:{timeframe}"
                    memory_info[key] = len(buffer)
        
        # Check for memory warnings
        for key, count in memory_info.items():
            if count > self.memory_warning_threshold:
                self.logger.warning(f"High memory usage for {key}: {count} bars")
        
        return memory_info

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """Get available timeframes for a symbol"""
        available = set()
        
        with self.completed_bars_lock:
            if symbol in self.completed_bars_buffer:
                available.update(self.completed_bars_buffer[symbol].keys())
        
        with self.current_bars_lock:
            if symbol in self.current_bars:
                available.update(self.current_bars[symbol].keys())
        
        return sorted([tf for tf in available if tf in self.SUPPORTED_TIMEFRAMES])

    def get_tracked_symbols(self) -> List[str]:
        """Get all tracked symbols"""
        symbols = set()
        
        with self.completed_bars_lock:
            symbols.update(self.completed_bars_buffer.keys())
        
        with self.current_bars_lock:
            symbols.update(self.current_bars.keys())
        
        return sorted(list(symbols))

    def get_tracked_symbols_and_timeframes(self) -> Dict[str, List[str]]:
        """Get mapping of symbols to their tracked timeframes"""
        result = {}
        
        for symbol in self.get_tracked_symbols():
            timeframes = self.get_available_timeframes(symbol)
            if timeframes:
                result[symbol] = timeframes
        
        return result

    def stop_tracking_timeframe(self, symbol: str, timeframe: str) -> None:
        """Stop tracking a specific timeframe for a symbol and clear its data"""
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for {symbol}")
            return
        
        locks = [self.current_bars_lock, self.completed_bars_lock, self.cache_lock]
        
        for lock in locks:
            lock.acquire()
        
        try:
            # Remove current bar data
            if symbol in self.current_bars and timeframe in self.current_bars[symbol]:
                del self.current_bars[symbol][timeframe]
                
                # Clean up symbol entry if no timeframes left
                if not self.current_bars[symbol]:
                    del self.current_bars[symbol]
            
            # Remove completed bars buffer
            if symbol in self.completed_bars_buffer and timeframe in self.completed_bars_buffer[symbol]:
                del self.completed_bars_buffer[symbol][timeframe]
                
                # Clean up symbol entry if no timeframes left
                if not self.completed_bars_buffer[symbol]:
                    del self.completed_bars_buffer[symbol]
            
            # Remove cached DataFrame
            if symbol in self.bars_df_cache and timeframe in self.bars_df_cache[symbol]:
                del self.bars_df_cache[symbol][timeframe]
                
                # Clean up symbol entry if no timeframes left
                if not self.bars_df_cache[symbol]:
                    del self.bars_df_cache[symbol]
            
            # Clean up metadata if no data left for symbol
            if (symbol not in self.current_bars and 
                symbol not in self.completed_bars_buffer):
                with self.symbol_metadata_lock:
                    self.symbol_metadata.pop(symbol, None)
                self.last_tick_time.pop(symbol, None)
                self.last_close_prices.pop(symbol, None)
            
            self.logger.info(f"Stopped tracking '{timeframe}' for symbol '{symbol}'")
            
        finally:
            for lock in reversed(locks):
                lock.release()

    def get_market_session_info(self, timestamp: float, symbol: str) -> Dict[str, Any]:
        """Get market session information for a given timestamp and symbol"""
        if not self.market_session_manager:
            return {'session_type': 'unknown', 'is_open': True}
        
        exchange = self._extract_exchange_from_symbol(symbol)
        session_type = self.market_session_manager.get_session_type(timestamp, exchange)
        is_open = self.market_session_manager.is_market_open(timestamp, exchange)
        
        return {
            'session_type': session_type.value,
            'is_open': is_open,
            'exchange': exchange,
            'timestamp': timestamp
        }
    
    def set_circuit_breaker_limits(self, symbol_group_limits: Dict[str, float]) -> None:
        """Update circuit breaker limits for different symbol groups"""
        if self.data_validator:
            self.data_validator.circuit_limits.update(symbol_group_limits)
            self.logger.info(f"Updated circuit breaker limits: {symbol_group_limits}")
    
    def add_market_holiday(self, date_str: str) -> None:
        """Add a market holiday date (YYYY-MM-DD format)"""
        if self.market_session_manager:
            self.market_session_manager.market_holidays.add(date_str)
            self.logger.info(f"Added market holiday: {date_str}")
    
    def get_data_quality_report(self, symbol: str) -> Dict[str, Any]:
        """Get data quality report for a symbol"""
        if not self.data_validator:
            return {}
        
        report = {
            'symbol': symbol,
            'price_history_count': 0,
            'validation_issues': []
        }
        
        with self.data_validator.lock:
            if symbol in self.data_validator.price_history:
                report['price_history_count'] = len(self.data_validator.price_history[symbol])
                
                # Calculate price statistics
                prices = list(self.data_validator.price_history[symbol])
                if prices:
                    report['price_stats'] = {
                        'min': min(prices),
                        'max': max(prices),
                        'mean': sum(prices) / len(prices),
                        'latest': prices[-1] if prices else None
                    }
        
        return report
    
    def force_bar_completion(self, symbol: str, timeframe: str) -> bool:
        """
        Force completion of the current bar for a symbol and timeframe.
        Useful for end-of-day processing or testing.
        
        Returns:
            bool: True if a bar was completed, False otherwise
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            return False
        
        with self.current_bars_lock:
            if symbol not in self.current_bars or timeframe not in self.current_bars[symbol]:
                return False
            
            current_bar = self.current_bars[symbol][timeframe]
            
            # Move current bar to completed bars
            with self.completed_bars_lock:
                self.completed_bars_buffer[symbol][timeframe].append(current_bar)
            
            # Clear current bar
            del self.current_bars[symbol][timeframe]
            
            # Invalidate cache
            with self.cache_lock:
                self.bars_df_cache[symbol].pop(timeframe, None)
            
            # Record completion in monitor
            if self.performance_monitor:
                self.performance_monitor.record_bar_completion(symbol, timeframe)
            
            self.logger.info(f"Forced completion of bar for {symbol} [{timeframe}]")
            return True
    
    def get_timeframe_status(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        """Get status information for all timeframes of a symbol"""
        status = {}
        
        for timeframe in self.get_available_timeframes(symbol):
            tf_status = {
                'timeframe': timeframe,
                'has_current_bar': False,
                'completed_bars_count': 0,
                'last_update': None
            }
            
            # Check current bar
            with self.current_bars_lock:
                if symbol in self.current_bars and timeframe in self.current_bars[symbol]:
                    tf_status['has_current_bar'] = True
                    current_bar = self.current_bars[symbol][timeframe]
                    tf_status['current_bar_start'] = current_bar['timestamp']
                    tf_status['current_price'] = current_bar['close']
            
            # Check completed bars
            with self.completed_bars_lock:
                if symbol in self.completed_bars_buffer and timeframe in self.completed_bars_buffer[symbol]:
                    buffer = self.completed_bars_buffer[symbol][timeframe]
                    tf_status['completed_bars_count'] = len(buffer)
                    if buffer:
                        tf_status['last_completed_bar'] = buffer[-1]['timestamp']
            
            # Last update time
            tf_status['last_update'] = self.last_tick_time.get(symbol)
            
            status[timeframe] = tf_status
        
        return status
    
    def export_data_to_dict(self) -> Dict[str, Any]:
        """Export all timeframe data to a dictionary format for serialization"""
        export_data = {
            'metadata': {
                'export_timestamp': time.time(),
                'max_bars_in_memory': self.max_bars_in_memory,
                'default_exchange': self.default_exchange
            },
            'symbols': {}
        }
        
        for symbol in self.get_tracked_symbols():
            symbol_data = {
                'timeframes': {},
                'last_tick_time': self.last_tick_time.get(symbol),
                'last_close_price': self.last_close_prices.get(symbol)
            }
            
            for timeframe in self.get_available_timeframes(symbol):
                timeframe_data = {
                    'current_bar': None,
                    'completed_bars': []
                }
                
                # Export current bar
                current_bar = self.get_current_bar(symbol, timeframe)
                if current_bar:
                    timeframe_data['current_bar'] = current_bar
                
                # Export completed bars
                with self.completed_bars_lock:
                    if symbol in self.completed_bars_buffer and timeframe in self.completed_bars_buffer[symbol]:
                        timeframe_data['completed_bars'] = list(self.completed_bars_buffer[symbol][timeframe])
                
                symbol_data['timeframes'][timeframe] = timeframe_data
            
            export_data['symbols'][symbol] = symbol_data
        
        return export_data
    
    def import_data_from_dict(self, import_data: Dict[str, Any]) -> None:
        """Import timeframe data from a dictionary format"""
        if 'symbols' not in import_data:
            self.logger.error("Invalid import data format: missing 'symbols' key")
            return
        
        # Clear existing data
        self.reset()
        
        for symbol, symbol_data in import_data['symbols'].items():
            # Restore last tick time and close price
            if 'last_tick_time' in symbol_data and symbol_data['last_tick_time']:
                self.last_tick_time[symbol] = symbol_data['last_tick_time']
            
            if 'last_close_price' in symbol_data and symbol_data['last_close_price']:
                self.last_close_prices[symbol] = symbol_data['last_close_price']
            
            # Restore timeframe data
            if 'timeframes' in symbol_data:
                for timeframe, timeframe_data in symbol_data['timeframes'].items():
                    # Ensure timeframe is tracked
                    self.ensure_timeframe_tracked(symbol, timeframe)
                    
                    # Restore completed bars
                    if 'completed_bars' in timeframe_data:
                        with self.completed_bars_lock:
                            self.completed_bars_buffer[symbol][timeframe].extend(
                                timeframe_data['completed_bars']
                            )
                    
                    # Restore current bar
                    if 'current_bar' in timeframe_data and timeframe_data['current_bar']:
                        with self.current_bars_lock:
                            self.current_bars[symbol][timeframe] = timeframe_data['current_bar']
        
        self.logger.info(f"Imported data for {len(import_data['symbols'])} symbols")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - could be used for cleanup"""
        if exc_type is not None:
            self.logger.error(f"Exception in TimeframeManager context: {exc_type.__name__}: {exc_val}")
        return False
    
    def __del__(self):
        """Destructor - cleanup resources"""
        try:
            if hasattr(self, 'logger'):
                self.logger.info("Enhanced TimeframeManager instance being destroyed")
        except:
            pass
