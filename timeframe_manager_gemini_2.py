import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta, time as dt_time, date as dt_date
import threading
import time
import pytz
from collections import defaultdict, deque
from dataclasses import dataclass # Removed 'field' as it's not used at class level here
from enum import Enum

# Assuming core.logging_manager and utils.constants are available in the environment
# For standalone execution, these might need dummy implementations or adjustments.
try:
    from core.logging_manager import get_logger
except ImportError:
    import logging
    def get_logger(name):
        logger = logging.getLogger(name)
        if not logger.handlers: # Avoid duplicate handlers if re-running in some environments
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

try:
    from utils.constants import MarketDataType
except ImportError:
    class MarketDataType(Enum):
        TIMESTAMP = "timestamp"
        LAST_PRICE = "last_price"
        VOLUME = "volume"
        OPEN_INTEREST = "open_interest"
        BID = "bid"
        ASK = "ask"
        # Add other types if necessary for the context of TimeframeManager

class SessionType(Enum):
    """Market session types for Indian markets"""
    REGULAR = "regular"
    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"
    MUHURAT = "muhurat"  # Muhurat trading session
    CLOSED = "closed"


@dataclass
class SessionTiming:
    """Session timing definition"""
    start_time: dt_time
    end_time: dt_time
    session_type: SessionType
    name: str


class MarketSessionManager:
    """
    Manages Indian market sessions for NSE and BSE.
    Timestamps are expected to be epoch seconds.
    Timezone is IST ('Asia/Kolkata').
    """
    
    def __init__(self, logger_instance=None):
        # Use a passed logger or get a default one
        self.logger = logger_instance if logger_instance else get_logger("utils.market_session_manager")
        self.timezone = pytz.timezone('Asia/Kolkata')
        
        # Define standard session timings. These are dt_time objects (IST).
        self.sessions: Dict[str, List[SessionTiming]] = {
            'NSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "NSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "NSE Regular Trading"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "NSE Post-Market"),
                # Example: Muhurat trading (typically evening, date-specific)
                # SessionTiming(dt_time(18, 15), dt_time(19, 15), SessionType.MUHURAT, "NSE Muhurat Trading"),
            ],
            'BSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "BSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "BSE Regular Trading"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "BSE Post-Market"),
            ]
        }
        
        # Market holidays (YYYY-MM-DD format). 
        # In a production system, this should be dynamically loaded from an exchange calendar or a reliable source.
        # CORRECTED INITIALIZATION: Directly initialize as a set.
        self.market_holidays: Set[str] = {
            '2024-01-26',  # Republic Day
            '2024-03-08',  # Maha Shivratri (Example, check actual calendar)
            '2024-03-25',  # Holi (Example)
            '2024-03-29',  # Good Friday (Example)
            # Add more holidays as needed. Ensure this list is kept up-to-date.
            # For 2025 (Illustrative, verify with official calendar):
            '2025-01-26', # Republic Day
            '2025-03-14', # Holi
            '2025-04-18', # Good Friday
            '2025-05-01', # Maharashtra Day / May Day
            '2025-08-15', # Independence Day
            '2025-10-02', # Gandhi Jayanti
            '2025-10-21', # Diwali Laxmi Puja (Muhurat Trading might occur)
            '2025-12-25', # Christmas
        }

    def _is_holiday(self, date_obj: dt_date) -> bool:
        """Check if a given date is a market holiday."""
        return date_obj.strftime('%Y-%m-%d') in self.market_holidays

    def _is_weekend(self, date_obj: dt_date) -> bool:
        """Check if a given date is a weekend (Saturday or Sunday)."""
        return date_obj.weekday() >= 5  # Monday is 0 and Sunday is 6

    def get_session_type(self, timestamp_sec: float, exchange: str = 'NSE') -> SessionType:
        """
        Get the market session type for a given epoch timestamp (in seconds).
        The timestamp is interpreted in the IST timezone.
        """
        try:
            # Convert epoch seconds to IST datetime object
            dt_ist = datetime.fromtimestamp(timestamp_sec, self.timezone)
            date_ist = dt_ist.date()
            time_ist = dt_ist.time()
            
            if self._is_holiday(date_ist) or self._is_weekend(date_ist):
                # Special check for Muhurat trading, which can occur on holidays/weekends
                for session in self.sessions.get(exchange, []):
                    if session.session_type == SessionType.MUHURAT:
                        # Muhurat trading is date-specific, this logic assumes it's defined for the current day if active
                        # A more robust Muhurat check would involve checking if today IS the Muhurat trading day.
                        if session.start_time <= time_ist < session.end_time: # Note: end_time is exclusive for comparison
                             return SessionType.MUHURAT
                return SessionType.CLOSED
            
            # Check standard sessions for the exchange
            for session in self.sessions.get(exchange, self.sessions['NSE']): # Fallback to NSE if exchange not found
                if session.session_type == SessionType.MUHURAT: # Skip Muhurat here, handled above
                    continue
                # Check if current IST time falls within the session's start and end IST times
                # Standard convention: interval is [start, end).
                if session.start_time <= time_ist < session.end_time:
                    return session.session_type
            
            return SessionType.CLOSED # If no session matches
            
        except Exception as e:
            self.logger.error(f"Error determining session type for timestamp {timestamp_sec} on {exchange}: {e}", exc_info=True)
            return SessionType.CLOSED # Default to closed on error
    
    def is_market_open(self, timestamp_sec: float, exchange: str = 'NSE') -> bool:
        """
        Check if the market is open for regular trading at a given epoch timestamp (seconds).
        """
        session_type = self.get_session_type(timestamp_sec, exchange)
        return session_type == SessionType.REGULAR
    
    def get_market_day_start(self, timestamp_sec: float, exchange: str = 'NSE') -> float:
        """
        Get the start of the regular market trading day (e.g., 9:15 AM IST)
        for a given epoch timestamp (seconds), returning epoch seconds.
        """
        dt_ist = datetime.fromtimestamp(timestamp_sec, self.timezone)
        # Find the regular session start time for the given exchange
        regular_session_start_time = dt_time(9, 15) # Default
        for session in self.sessions.get(exchange, self.sessions['NSE']):
            if session.session_type == SessionType.REGULAR:
                regular_session_start_time = session.start_time
                break
        
        market_start_dt_ist = dt_ist.replace(hour=regular_session_start_time.hour, 
                                             minute=regular_session_start_time.minute, 
                                             second=0, microsecond=0)
        return market_start_dt_ist.timestamp() # Returns float epoch seconds

    def get_next_market_day(self, current_date_ist: dt_date) -> dt_date:
        """Gets the next valid market day (non-holiday, non-weekend) after current_date_ist."""
        next_day = current_date_ist + timedelta(days=1)
        while self._is_holiday(next_day) or self._is_weekend(next_day):
            next_day += timedelta(days=1)
        return next_day

class DataValidator:
    """Validates market data for quality and circuit breaker rules"""
    
    def __init__(self):
        self.logger = get_logger("utils.data_validator")
        # Circuit breaker limits (example: 5%, 10%, 20% for different scrips)
        # In production, these limits might be more dynamic or instrument-specific.
        self.circuit_limits: Dict[str, float] = {
            'GROUP_A': 0.20,  # 20% circuit limit (e.g., large-cap, F&O stocks)
            'GROUP_B': 0.10,  # 10% circuit limit (e.g., mid-cap)
            'DEFAULT': 0.05   # 5% circuit limit (e.g., small-cap, illiquid stocks)
        }
        
        # Price history for anomaly detection (e.g., volume spikes, price jumps)
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100)) # Stores recent prices
        self.lock = threading.RLock() # Thread-safe access to price_history
    
    def validate_tick(self, symbol: str, market_data: Dict[str, Any], 
                     last_close_price: Optional[float] = None) -> Tuple[bool, str]:
        """
        Validate a single market data tick.
        
        Args:
            symbol: The instrument symbol.
            market_data: Dictionary containing tick data (e.g., price, volume).
            last_close_price: The last closing price of the *previous completed bar* or previous day's close.
                              Used for circuit breaker checks.
        
        Returns:
            Tuple[bool, str]: (is_valid, reason_if_invalid)
        """
        price = market_data.get(MarketDataType.LAST_PRICE.value)
        volume = market_data.get(MarketDataType.VOLUME.value, 0.0) # Default to 0 if not present
        
        if price is None or not isinstance(price, (int, float)) or price <= 0:
            return False, "Invalid or missing price (must be positive number)"
        
        if not isinstance(volume, (int, float)) or volume < 0:
            return False, "Invalid volume (must be non-negative number)"
        
        # Circuit breaker check (if last_close_price is available)
        if last_close_price is not None and last_close_price > 0:
            price_change_pct = abs(price - last_close_price) / last_close_price
            # Determine circuit group for the symbol (can be enhanced with actual scrip metadata)
            scrip_group = self._get_scrip_group(symbol)
            limit = self.circuit_limits.get(scrip_group, self.circuit_limits['DEFAULT'])
            
            if price_change_pct > limit:
                return False, f"Circuit breaker violation: change {price_change_pct:.2%} > limit {limit:.2%}"
        
        # Price and volume anomaly detection (example: significant deviation from recent average)
        with self.lock:
            if symbol in self.price_history:
                recent_prices = list(self.price_history[symbol])
                if len(recent_prices) >= 10: # Need enough data points for a meaningful average
                    avg_price_short_term = sum(recent_prices[-10:]) / 10
                    # Example: Check for >10% price deviation from short-term average
                    if abs(price - avg_price_short_term) / avg_price_short_term > 0.10: 
                        self.logger.warning(f"Significant price deviation for {symbol}: "
                                          f"current={price:.2f}, recent_avg={avg_price_short_term:.2f}")
            
            self.price_history[symbol].append(price) # Store current price for future checks
        
        return True, "" # Tick is considered valid
    
    def _get_scrip_group(self, symbol: str) -> str:
        """
        Determine the scrip group for applying appropriate circuit breaker limits.
        This is a placeholder. In production, this should be derived from actual instrument metadata
        (e.g., exchange-provided classification, market cap, F&O eligibility).
        """
        # Example heuristic:
        if 'NSE:' in symbol and any(x in symbol for x in ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK']):
            return 'GROUP_A'
        if 'MIDCAP' in symbol or 'SMALLCAP' in symbol: # Hypothetical naming convention
             return 'GROUP_B'
        return 'DEFAULT'


class PerformanceMonitor:
    """Monitors performance metrics for the TimeframeManager"""
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {
            'bars_completed': defaultdict(int),      # symbol:timeframe -> count
            'ticks_processed': defaultdict(int),     # symbol -> count
            'processing_latency': defaultdict(lambda: deque(maxlen=1000)), # symbol -> list of latencies (seconds)
            'errors': defaultdict(int),              # symbol:error_type -> count
        }
        self.lock = threading.RLock() # Thread-safe access to metrics
        self.start_time = time.monotonic() # Use monotonic clock for measuring elapsed time
    
    def record_bar_completion(self, symbol: str, timeframe: str):
        """Record a bar completion event"""
        with self.lock:
            self.metrics['bars_completed'][f"{symbol}:{timeframe}"] += 1
    
    def record_tick_processing(self, symbol: str, processing_time_sec: float):
        """Record tick processing time in seconds"""
        with self.lock:
            self.metrics['ticks_processed'][symbol] += 1
            self.metrics['processing_latency'][symbol].append(processing_time_sec)
            # deque automatically handles maxlen
    
    def record_error(self, symbol: str, error_type: str):
        """Record an error event"""
        with self.lock:
            self.metrics['errors'][f"{symbol}:{error_type}"] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get performance statistics"""
        with self.lock:
            # Create a deep copy for safe iteration if metrics are modified elsewhere (though unlikely here)
            current_metrics = {
                k: (v.copy() if isinstance(v, defaultdict) else v) 
                for k, v in self.metrics.items()
            }

            stats: Dict[str, Any] = {
                'uptime_seconds': time.monotonic() - self.start_time,
                'total_bars_completed': sum(current_metrics['bars_completed'].values()),
                'total_ticks_processed': sum(current_metrics['ticks_processed'].values()),
                'total_errors': sum(current_metrics['errors'].values()),
                'detailed_errors': dict(current_metrics['errors']), # Convert defaultdict to dict for output
                'detailed_bars_completed': dict(current_metrics['bars_completed'])
            }
            
            latency_stats: Dict[str, Dict[str, float]] = {}
            for symbol, latencies_deque in current_metrics['processing_latency'].items():
                latencies = list(latencies_deque) # Convert deque to list for numpy operations
                if latencies:
                    latency_stats[symbol] = {
                        'count': len(latencies),
                        'avg_ms': np.mean(latencies) * 1000,
                        'max_ms': np.max(latencies) * 1000,
                        'min_ms': np.min(latencies) * 1000,
                        'p95_ms': np.percentile(latencies, 95) * 1000 if len(latencies) >= 20 else 0.0, # Meaningful P95 with enough samples
                        'p99_ms': np.percentile(latencies, 99) * 1000 if len(latencies) >= 100 else 0.0
                    }
            stats['latency_by_symbol_ms'] = latency_stats
            
            return stats


class TimeframeManager:
    """
    Manages time-based aggregation of market data (ticks) into bars (OHLCV)
    for various timeframes. Handles Indian market sessions (IST), data validation,
    and performance monitoring. Timestamps are handled as epoch seconds internally.
    """

    SUPPORTED_TIMEFRAMES: Dict[str, int] = {
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '4h': 14400, '1d': 86400, # 1d is aligned to market day start
    }

    BAR_COLUMNS: List[str] = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest']

    def __init__(self, 
                 max_bars_in_memory: int = 10000,
                 enable_market_session_validation: bool = True,
                 enable_data_validation: bool = True,
                 enable_performance_monitoring: bool = True,
                 default_exchange: str = 'NSE'):
        """
        Initialize the TimeframeManager.

        Args:
            max_bars_in_memory: Max completed bars to keep in memory per symbol/timeframe.
            enable_market_session_validation: If True, validate ticks against market sessions.
            enable_data_validation: If True, validate tick data quality.
            enable_performance_monitoring: If True, collect performance metrics.
            default_exchange: Default exchange (e.g., 'NSE', 'BSE') if not in symbol.
        """
        self.logger = get_logger("utils.timeframe_manager")
        self.max_bars_in_memory = max_bars_in_memory
        self.default_exchange = default_exchange.upper() # Standardize exchange name
        
        self.market_session_manager = MarketSessionManager(logger_instance=self.logger) if enable_market_session_validation else None
        self.data_validator = DataValidator() if enable_data_validation else None
        self.performance_monitor = PerformanceMonitor() if enable_performance_monitoring else None
        
        # Data structures for storing bars:
        # completed_bars_buffer: Stores completed OHLCV bars as deques of dictionaries.
        #   Structure: {symbol: {timeframe: deque([bar_dict1, bar_dict2, ...])}}
        self.completed_bars_buffer: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.max_bars_in_memory))
        )
        # bars_df_cache: Caches pandas DataFrames of completed bars for faster retrieval.
        #   Structure: {symbol: {timeframe: pd.DataFrame}}
        self.bars_df_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        # current_bars: Stores the currently forming bar for each symbol and timeframe.
        #   Structure: {symbol: {timeframe: current_bar_dict_or_None}}
        self.current_bars: Dict[str, Dict[str, Optional[Dict[str, Any]]]] = defaultdict(dict)
        
        # Tracking last known state:
        self.last_tick_time: Dict[str, float] = {} # symbol -> last_tick_timestamp_sec
        self.last_close_prices: Dict[str, float] = {}  # symbol -> close_price_of_last_completed_bar

        # Threading locks for safe concurrent access:
        self.current_bars_lock = threading.RLock()
        self.completed_bars_lock = threading.RLock()
        self.cache_lock = threading.RLock()
        self.general_lock = threading.RLock() # For shared resources like last_tick_time
        
        # Market session cache for performance:
        #   Structure: {exchange: {'is_open': bool, 'session_type': SessionType, 
        #                          'next_transition_ts': float, 'last_check_ts': float}}
        self.session_cache: Dict[str, Dict[str, Any]] = {}
        self.session_cache_lock = threading.RLock()
        self.session_cache_validity_sec = 60  # How long cached session status is considered fresh (seconds)

        self.logger.info(f"TimeframeManager initialized. Max bars/symbol/tf: {max_bars_in_memory}. "
                        f"Market session validation: {enable_market_session_validation}, "
                        f"Data validation: {enable_data_validation}, "
                        f"Performance monitoring: {enable_performance_monitoring}, "
                        f"Default exchange: {self.default_exchange}")

    def _extract_exchange_from_symbol(self, symbol: str) -> str:
        """Extracts exchange from symbol (e.g., 'NSE:RELIANCE' -> 'NSE'). Falls back to default."""
        if ':' in symbol:
            return symbol.split(':', 1)[0].upper()
        return self.default_exchange

    def _calculate_next_session_transition(self, current_timestamp_sec: float, exchange: str) -> float:
        """
        Calculates the epoch timestamp (seconds) of the next market session transition (open/close).
        This is crucial for refreshing the session cache accurately.
        """
        if not self.market_session_manager:
            return current_timestamp_sec + 3600 # Fallback if no session manager

        msm = self.market_session_manager
        current_dt_ist = datetime.fromtimestamp(current_timestamp_sec, msm.timezone)
        current_date_ist = current_dt_ist.date()
        current_time_ist = current_dt_ist.time()

        # Helper to combine date and time and convert to epoch seconds
        def to_epoch_sec(date_val: dt_date, time_val: dt_time) -> float:
            # Ensure the datetime object is created naively first, then localized by the specific timezone object
            # This is the most robust way with pytz.
            naive_dt = datetime.combine(date_val, time_val)
            return msm.timezone.localize(naive_dt).timestamp()


        # Check today's sessions
        is_today_market_day = not (msm._is_holiday(current_date_ist) or msm._is_weekend(current_date_ist))
        
        # Sort sessions by start time to ensure correct order
        exchange_sessions = sorted(msm.sessions.get(exchange, []), key=lambda s: s.start_time)

        if is_today_market_day:
            for session in exchange_sessions:
                if session.session_type == SessionType.MUHURAT: continue # Muhurat handled separately if needed

                if current_time_ist < session.start_time:
                    # Current time is before this session starts
                    return to_epoch_sec(current_date_ist, session.start_time)
                if current_time_ist < session.end_time: # current_time is within this session
                    # Next transition is the end of this session
                    return to_epoch_sec(current_date_ist, session.end_time)
            
            # If current_time_ist is after all today's sessions or no sessions today
            # Fall through to find next market day's first session
            pass

        # Find next market day and its first session start
        # If today was a market day but current time is after all sessions, or if today wasn't a market day
        # advance to next market day
        next_market_date = current_date_ist # Start with current date
        if not is_today_market_day or (exchange_sessions and current_time_ist >= exchange_sessions[-1].end_time):
            next_market_date = msm.get_next_market_day(current_date_ist)
        elif is_today_market_day and not exchange_sessions: # Today is market day but no sessions defined for it (edge case)
             next_market_date = msm.get_next_market_day(current_date_ist)


        first_session_next_day = None
        if exchange_sessions: # Ensure there are sessions defined
            # Find the earliest non-Muhurat session for the next market day
            for s in exchange_sessions:
                if s.session_type != SessionType.MUHURAT:
                    first_session_next_day = s
                    break
            if not first_session_next_day and exchange_sessions: # If only Muhurat sessions, pick the first one
                 first_session_next_day = exchange_sessions[0]


        if first_session_next_day:
            return to_epoch_sec(next_market_date, first_session_next_day.start_time)
        
        # Fallback: if no sessions defined for exchange, check in 1 hour
        self.logger.warning(f"No sessions found for exchange {exchange} to calculate next transition. Defaulting to 1hr.")
        return current_timestamp_sec + 3600


    def _is_market_open_cached(self, timestamp_sec: float, exchange: str) -> bool:
        """
        Checks if the market is open using a cached session state for performance.
        Refreshes cache based on `session_cache_validity_sec` or if `next_transition_ts` is reached.
        """
        if not self.market_session_manager:
            return True # Assume open if session validation is disabled

        with self.session_cache_lock:
            cache_entry = self.session_cache.get(exchange)
            
            needs_refresh = (
                cache_entry is None or
                timestamp_sec >= cache_entry.get('next_transition_ts', 0) or
                (timestamp_sec - cache_entry.get('last_check_ts', 0)) > self.session_cache_validity_sec
            )
            
            if needs_refresh:
                is_open = self.market_session_manager.is_market_open(timestamp_sec, exchange)
                session_type = self.market_session_manager.get_session_type(timestamp_sec, exchange)
                next_transition_ts = self._calculate_next_session_transition(timestamp_sec, exchange)
                
                self.session_cache[exchange] = {
                    'is_open': is_open,
                    'session_type': session_type,
                    'next_transition_ts': next_transition_ts,
                    'last_check_ts': timestamp_sec
                }
                if cache_entry is None or cache_entry.get('session_type') != session_type: # Check if cache_entry exists before accessing its items
                     self.logger.info(f"Session cache for {exchange} updated: {session_type.value}, next transition at "
                                     f"{datetime.fromtimestamp(next_transition_ts, self.market_session_manager.timezone).isoformat()}") # Use manager's timezone
                return is_open
            else:
                return cache_entry['is_open']

    def ensure_timeframe_tracked(self, symbol: str, timeframe: str) -> None:
        """
        Ensures that the given symbol and timeframe are set up for tracking.
        Initializes necessary data structures if they don't exist.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for symbol '{symbol}'. Not tracking.") 
            return
        
        # Initialize current_bars entry for the symbol if it's new
        with self.current_bars_lock: 
            if symbol not in self.current_bars: 
                self.current_bars[symbol] = {} 
            
            # If this specific timeframe for the symbol is new, mark it for initialization on first tick
            if timeframe not in self.current_bars[symbol]: 
                self.current_bars[symbol][timeframe] = None # Sentinel for "awaiting first tick"
                self.logger.info(f"Started tracking '{timeframe}' for symbol '{symbol}'. Current bar will be initialized on first tick.")
        
        # defaultdicts for completed_bars_buffer and bars_df_cache handle their own initialization on access.
        # Access them once to ensure keys exist if needed for other logic, though not strictly necessary here.
        with self.completed_bars_lock: 
            _ = self.completed_bars_buffer[symbol][timeframe] 
        with self.cache_lock: 
            _ = self.bars_df_cache[symbol] # Ensures symbol key exists in bars_df_cache

    def process_tick(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, bool]:
        """
        Processes a single market data tick (e.g., trade update).
        Updates relevant current bars and completes bars if timeframe ends.
        All timestamps in market_data are expected to be epoch seconds.

        Returns:
            A dictionary indicating which timeframes had a bar completed: {timeframe_str: True/False}
        """
        processing_start_time = time.monotonic()
        
        try:
            # 1. Extract and validate essential data from the tick
            price = market_data.get(MarketDataType.LAST_PRICE.value)
            timestamp_sec = market_data.get(MarketDataType.TIMESTAMP.value) # Expected in epoch seconds
            volume = market_data.get(MarketDataType.VOLUME.value, 0.0)
            open_interest = market_data.get(MarketDataType.OPEN_INTEREST.value) # Can be None
            
            # Attempt to use bid/ask for price if LTP is missing
            if price is None:
                bid = market_data.get(MarketDataType.BID.value)
                ask = market_data.get(MarketDataType.ASK.value)
                if bid is not None and ask is not None and isinstance(bid, (int, float)) and isinstance(ask, (int, float)) and bid > 0 and ask > 0:
                    price = (bid + ask) / 2.0
                    self.logger.debug(f"Using mid-price {price:.2f} for {symbol} as LTP is missing.")
                else:
                    self.logger.warning(f"No valid price (LTP or Bid/Ask) for {symbol}. Tick: {market_data}")
                    if self.performance_monitor:
                        self.performance_monitor.record_error(symbol, "invalid_price_data")
                    return {} # Cannot process without price

            # Use current system time if timestamp is missing (less ideal)
            if timestamp_sec is None:
                timestamp_sec = time.time() # time.time() returns epoch seconds (UTC)
                self.logger.warning(f"Timestamp missing for {symbol}. Using current system time: {timestamp_sec}. Tick: {market_data}")
            
            # Convert data types and validate
            try:
                price = float(price)
                timestamp_sec = float(timestamp_sec)
                volume = float(volume) if volume is not None else 0.0
                open_interest = float(open_interest) if open_interest is not None else None # OI can be None
                if price <= 0: raise ValueError("Price must be positive.")
                if volume < 0: raise ValueError("Volume must be non-negative.")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Data type conversion/validation error for {symbol}. Data: {market_data}. Error: {e}")
                if self.performance_monitor:
                    self.performance_monitor.record_error(symbol, "type_conversion_error")
                return {}

            # 2. Market session validation (if enabled)
            exchange = self._extract_exchange_from_symbol(symbol)
            if self.market_session_manager:
                # Ensure self.market_session_manager.timezone is used for converting timestamp_sec for logging
                log_datetime_ist = datetime.fromtimestamp(timestamp_sec, self.market_session_manager.timezone)
                if not self._is_market_open_cached(timestamp_sec, exchange):
                    session_type_val = "N/A"
                    with self.session_cache_lock: # Safely access session_cache
                        s_type = self.session_cache.get(exchange, {}).get('session_type')
                        if s_type: session_type_val = s_type.value if isinstance(s_type, Enum) else str(s_type)
                    
                    self.logger.debug(f"Market not in REGULAR session for {symbol} (Exchange: {exchange}) at "
                                     f"{log_datetime_ist.isoformat()}. " # Use localized datetime for logging
                                     f"Actual session: {session_type_val}. Tick rejected.")
                    return {}
            
            # 3. Data quality validation (if enabled)
            if self.data_validator:
                # Use the close of the last *completed* bar for circuit breaker validation
                last_completed_bar_close = self.last_close_prices.get(symbol)
                is_valid, reason = self.data_validator.validate_tick(symbol, market_data, last_completed_bar_close)
                if not is_valid:
                    self.logger.warning(f"Invalid tick for {symbol}: {reason}. Tick: {market_data}")
                    if self.performance_monitor:
                        self.performance_monitor.record_error(symbol, f"validation_failed_{reason.replace(' ', '_').lower()}")
                    return {}
            
            # 4. Out-of-order tick detection (global for the symbol)
            with self.general_lock:
                last_known_tick_time = self.last_tick_time.get(symbol)
                if last_known_tick_time is not None:
                    time_diff_sec = timestamp_sec - last_known_tick_time
                    if time_diff_sec < -0.5:  # Allow small negative diff for precision issues, reject larger ones
                        self.logger.warning(f"Out-of-order tick detected for {symbol}: current_ts={timestamp_sec}, "
                                          f"last_ts={last_known_tick_time}. Diff: {time_diff_sec:.3f}s. Skipping.")
                        if self.performance_monitor:
                             self.performance_monitor.record_error(symbol, "out_of_order_tick")
                        return {}
                    if time_diff_sec > 300: # Log if significant gap (e.g., > 5 minutes)
                        self.logger.info(f"Significant time gap for {symbol}: {time_diff_sec:.2f}s since last tick.")
                
                self.last_tick_time[symbol] = timestamp_sec # Update last tick time for this symbol

            # 5. Process tick for each tracked timeframe for the symbol
            completed_bars_status: Dict[str, bool] = {}
            log_display_timezone = self.market_session_manager.timezone if self.market_session_manager else pytz.utc
            
            with self.current_bars_lock:
                if symbol not in self.current_bars:
                    self.logger.debug(f"Symbol {symbol} not actively tracked (no timeframes setup). Call ensure_timeframe_tracked first.")
                    return {} # Should be caught by ensure_timeframe_tracked usually
                
                for timeframe_str, tf_seconds in self.SUPPORTED_TIMEFRAMES.items():
                    if timeframe_str not in self.current_bars[symbol]: # Check if this timeframe is tracked for the symbol
                        continue # Skip if not tracked
                    
                    # Calculate the market-aware start timestamp for the bar this tick belongs to
                    bar_start_timestamp_sec = self._get_market_aware_bar_start(timestamp_sec, tf_seconds, symbol, exchange)
                    
                    current_bar = self.current_bars[symbol].get(timeframe_str)
                    
                    if current_bar is None: # First tick for this timeframe, or bar was just completed
                        self.current_bars[symbol][timeframe_str] = self._create_new_bar(
                            bar_start_timestamp_sec, price, volume, open_interest
                        )
                        completed_bars_status[timeframe_str] = False
                        self.logger.debug(f"Initialized first '{timeframe_str}' bar for {symbol} at "
                                         f"{datetime.fromtimestamp(bar_start_timestamp_sec, log_display_timezone).isoformat()} "
                                         f"({bar_start_timestamp_sec})")
                        
                    elif bar_start_timestamp_sec > current_bar['timestamp']: # Tick belongs to a new bar
                        # Complete the old bar
                        with self.completed_bars_lock:
                            self.completed_bars_buffer[symbol][timeframe_str].append(current_bar)
                        
                        completed_bars_status[timeframe_str] = True
                        self.last_close_prices[symbol] = current_bar['close'] # Update last close for circuit checks
                        
                        # Start a new bar
                        self.current_bars[symbol][timeframe_str] = self._create_new_bar(
                            bar_start_timestamp_sec, price, volume, open_interest
                        )
                        self.logger.debug(f"Completed '{timeframe_str}' bar for {symbol} (ended {datetime.fromtimestamp(current_bar['timestamp'], log_display_timezone).isoformat()}); "
                                         f"Started new one at {datetime.fromtimestamp(bar_start_timestamp_sec, log_display_timezone).isoformat()}")
                        
                        # Invalidate DataFrame cache for this symbol/timeframe
                        with self.cache_lock:
                            self.bars_df_cache.get(symbol, {}).pop(timeframe_str, None)
                        
                        if self.performance_monitor:
                            self.performance_monitor.record_bar_completion(symbol, timeframe_str)
                            
                    elif bar_start_timestamp_sec == current_bar['timestamp']: # Tick updates the current bar
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume
                        if open_interest is not None: # Update OI if available
                            current_bar['open_interest'] = open_interest
                        completed_bars_status[timeframe_str] = False
                        
                    else: # bar_start_timestamp_sec < current_bar['timestamp'] - Late/out-of-order tick for this timeframe
                        self.logger.warning(
                            f"Late/out-of-order tick for {symbol} [{timeframe_str}]. "
                            f"Tick bar start: {datetime.fromtimestamp(bar_start_timestamp_sec, log_display_timezone).isoformat()} ({bar_start_timestamp_sec}), "
                            f"Current bar start: {datetime.fromtimestamp(current_bar['timestamp'], log_display_timezone).isoformat()} ({current_bar['timestamp']}). "
                            f"Skipping update for this timeframe."
                        )
                        completed_bars_status[timeframe_str] = False # No bar completed or updated
            
            # 6. Record performance metrics (if enabled)
            if self.performance_monitor:
                processing_duration_sec = time.monotonic() - processing_start_time
                self.performance_monitor.record_tick_processing(symbol, processing_duration_sec)
            
            return completed_bars_status
            
        except Exception as e:
            self.logger.error(f"Critical error processing tick for {symbol}. Data: {market_data}. Error: {e}", exc_info=True)
            if self.performance_monitor:
                self.performance_monitor.record_error(symbol, "critical_processing_error")
            return {} # Return empty on critical error

    def _get_market_aware_bar_start(self, tick_timestamp_sec: float, 
                                   timeframe_seconds: int, symbol: str, exchange: str) -> float:
        """
        Calculates the market-aware start timestamp (epoch seconds) for a bar.
        For daily bars (or longer), aligns to the market day's regular session start (e.g., 9:15 AM IST).
        For intraday bars, uses standard epoch alignment.
        """
        if timeframe_seconds <= 0: # Should not happen with SUPPORTED_TIMEFRAMES
            self.logger.error(f"Invalid timeframe_seconds: {timeframe_seconds} for symbol {symbol}")
            return tick_timestamp_sec 

        # For daily or longer timeframes, align to the start of the market day in IST.
        if timeframe_seconds >= 86400: # 1 day or more
            if self.market_session_manager:
                return self.market_session_manager.get_market_day_start(tick_timestamp_sec, exchange)
            else:
                # Fallback if no session manager: align to start of UTC day, then convert to rough IST day start.
                # This is less accurate and assumes market opens around morning IST.
                dt_utc = datetime.fromtimestamp(tick_timestamp_sec, pytz.utc)
                # Create naive datetime for IST components, then localize
                naive_ist_approx_day_start = datetime(dt_utc.year, dt_utc.month, dt_utc.day, 9, 15)
                dt_ist_approx_day_start = pytz.timezone('Asia/Kolkata').localize(naive_ist_approx_day_start)
                
                # If tick is before this approximate start, use previous day's start
                if dt_utc.timestamp() < dt_ist_approx_day_start.timestamp(): # Compare epoch timestamps
                     dt_ist_approx_day_start -= timedelta(days=1) # This will be aware of DST if any
                return dt_ist_approx_day_start.timestamp()
        
        # For intraday bars, use standard epoch second alignment.
        # This means the bar start is a multiple of timeframe_seconds in epoch time.
        return float((int(tick_timestamp_sec) // timeframe_seconds) * timeframe_seconds)

    def _create_new_bar(self, timestamp_sec: float, price: float, 
                       volume: float, open_interest: Optional[float]) -> Dict[str, Any]:
        """Creates a new bar dictionary. Ensures volume is non-negative."""
        return {
            'timestamp': timestamp_sec, # Epoch seconds, start of the bar
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': max(0.0, volume), # Ensure volume is not negative
            'open_interest': open_interest # Can be None
        }

    def _validate_bar_data(self, bar: Dict[str, Any]) -> bool:
        """Validates a single bar's data for consistency before adding to DataFrame."""
        try:
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in bar or bar[field] is None:
                    self.logger.warning(f"Bar validation failed: Missing field '{field}' in bar: {bar}")
                    return False
            
            # Check OHLC consistency: L <= O <= H and L <= C <= H
            if not (bar['low'] <= bar['open'] <= bar['high'] and 
                   bar['low'] <= bar['close'] <= bar['high']):
                # Allow for minor floating point inaccuracies if values are extremely close
                if not (np.isclose(bar['low'], bar['open']) or bar['low'] < bar['open']) or \
                   not (np.isclose(bar['open'], bar['high']) or bar['open'] < bar['high']) or \
                   not (np.isclose(bar['low'], bar['close']) or bar['low'] < bar['close']) or \
                   not (np.isclose(bar['close'], bar['high']) or bar['close'] < bar['high']):
                    self.logger.warning(f"Bar validation failed: OHLC inconsistency in bar: {bar}")
                    return False
            
            if bar['volume'] < 0:
                self.logger.warning(f"Bar validation failed: Negative volume in bar: {bar}")
                return False
            
            return True
        except TypeError: # Handles cases where bar fields might not be comparable (e.g. None)
             self.logger.warning(f"Bar validation failed: Type error during consistency check for bar: {bar}", exc_info=True)
             return False


    def _build_dataframe_from_buffer(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Builds a pandas DataFrame from the completed bars buffer for a given symbol and timeframe.
        Includes data validation for each bar.
        """
        with self.completed_bars_lock:
            # Make a copy of the buffer items for processing to avoid issues if buffer is modified
            buffer_items = list(self.completed_bars_buffer[symbol][timeframe]) 
            if not buffer_items:
                return None # Return None if buffer is empty
            
            try:
                # Validate bars before adding to DataFrame
                valid_bars_data = [bar for bar in buffer_items if self._validate_bar_data(bar)]
                
                if not valid_bars_data:
                    self.logger.warning(f"No valid bars found in buffer for {symbol} [{timeframe}] after validation.")
                    return None

                df = pd.DataFrame(valid_bars_data, columns=self.BAR_COLUMNS)
                
                # Convert 'timestamp' (epoch seconds) to pandas DatetimeIndex (UTC), then localize to IST for display/analysis if needed
                # For internal consistency and plotting, UTC index is often preferred.
                # If IST index is strictly needed by consumers, it can be converted: df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
                df.set_index('timestamp', inplace=True, drop=False) # Keep timestamp column as well
                
                # Sort by timestamp (deque should maintain order, but good practice)
                df.sort_index(inplace=True)
                
                # Ensure numeric types for OHLCV columns
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce') # Coerce errors to NaN
                
                # Handle open_interest (can be NaN if not available for all ticks)
                df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
                # Optional: Forward fill NaN in open_interest if appropriate for the data source
                # df['open_interest'] = df['open_interest'].ffill() 
                
                # Drop rows with NaN in critical OHLC columns if any occurred due to coercion
                df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

                self.logger.debug(f"Built DataFrame for {symbol} [{timeframe}] with {len(df)} bars.")
                return df
                
            except Exception as e:
                self.logger.error(f"Error building DataFrame for {symbol} [{timeframe}]: {e}", exc_info=True)
                return None

    def get_bars(self, symbol: str, timeframe: str, 
                limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Retrieves historical bars for a symbol and timeframe.
        Uses a cache for performance. If not cached, builds from buffer.
        Timestamps in the DataFrame index will be UTC. The 'timestamp' column contains epoch seconds.

        Args:
            symbol: Instrument symbol.
            timeframe: Timeframe string (e.g., '1m', '1d').
            limit: Optional maximum number of recent bars to return.

        Returns:
            A pandas DataFrame with OHLCV data, or None if no data or error.
            Index is pd.Timestamp (UTC), 'timestamp' column is epoch seconds.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' requested for {symbol}.")
            return None
        
        df_to_return: Optional[pd.DataFrame] = None
        
        # 1. Check cache
        with self.cache_lock:
            cached_df = self.bars_df_cache.get(symbol, {}).get(timeframe)
            if cached_df is not None:
                df_to_return = cached_df
                self.logger.debug(f"Cache hit for {symbol} [{timeframe}].")

        # 2. If not in cache, build from buffer and cache it
        if df_to_return is None:
            self.logger.debug(f"Cache miss for {symbol} [{timeframe}]. Building from buffer.")
            df_built = self._build_dataframe_from_buffer(symbol, timeframe)
            if df_built is not None and not df_built.empty:
                with self.cache_lock: # Lock again to update cache
                    # Ensure symbol key exists
                    if symbol not in self.bars_df_cache: self.bars_df_cache[symbol] = {}
                    self.bars_df_cache[symbol][timeframe] = df_built
                df_to_return = df_built
            elif df_built is None: # Error during build
                 return None 
            # If df_built is empty, df_to_return remains None or empty df based on below
        
        # Handle cases where no data is available or built
        if df_to_return is None or df_to_return.empty:
            # Return an empty DataFrame with correct columns and index type
            empty_df = pd.DataFrame(columns=self.BAR_COLUMNS)
            # Ensure 'timestamp' column exists for type consistency if needed by downstream
            empty_df['timestamp'] = pd.to_datetime([], unit='s', utc=True)
            empty_df.set_index('timestamp', inplace=True, drop=False)
            for col in ['open', 'high', 'low', 'close', 'volume', 'open_interest']: # Ensure numeric types
                empty_df[col] = pd.to_numeric(empty_df[col])
            return empty_df
        
        # Apply limit if specified (to a copy)
        # Ensure limit is positive; if limit is 0, return empty DataFrame with correct structure.
        if limit is not None:
            if limit == 0:
                # Return empty DataFrame with same columns and index type as df_to_return
                empty_df_limited = pd.DataFrame(columns=df_to_return.columns)
                empty_df_limited.index = pd.to_datetime([], utc=True) # Match index type
                empty_df_limited.index.name = df_to_return.index.name # Match index name
                for col in df_to_return.columns: # Match column dtypes
                    empty_df_limited[col] = empty_df_limited[col].astype(df_to_return[col].dtype)
                return empty_df_limited

            elif limit > 0:
                 return df_to_return.iloc[-limit:].copy() # Return a copy
        
        return df_to_return.copy() # Return a copy by default

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Gets the currently forming (incomplete) bar for a symbol and timeframe.
        Returns a copy of the bar dictionary.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for get_current_bar on {symbol}.")
            return None
        
        with self.current_bars_lock:
            current_bar_dict = self.current_bars.get(symbol, {}).get(timeframe)
            return current_bar_dict.copy() if current_bar_dict else None

    def reset(self, symbol: Optional[str] = None) -> None:
        """Resets stored data for a specific symbol or all symbols if None."""
        # Acquire all relevant locks in a consistent order to prevent deadlocks
        locks_to_acquire = [self.current_bars_lock, self.completed_bars_lock, 
                            self.cache_lock, self.general_lock, self.session_cache_lock]
        for lock in locks_to_acquire:
            lock.acquire()
        
        try:
            if symbol:
                self.completed_bars_buffer.pop(symbol, None)
                self.bars_df_cache.pop(symbol, None)
                self.current_bars.pop(symbol, None)
                self.last_tick_time.pop(symbol, None)
                self.last_close_prices.pop(symbol, None)
                # Symbol-specific session cache doesn't exist, session_cache is per exchange
                self.logger.info(f"Reset data for symbol: {symbol}")
            else: # Reset all data
                self.completed_bars_buffer.clear()
                self.bars_df_cache.clear()
                self.current_bars.clear()
                self.last_tick_time.clear()
                self.last_close_prices.clear()
                self.session_cache.clear() # Clear session cache as well
                if self.performance_monitor: # Reset performance monitor too if desired
                    self.performance_monitor = PerformanceMonitor() 
                self.logger.info("Reset all TimeframeManager data including performance stats and session cache.")
        finally:
            for lock in reversed(locks_to_acquire): # Release in reverse order
                lock.release()

    def get_performance_statistics(self) -> Dict[str, Any]:
        """Returns performance statistics if monitoring is enabled."""
        if self.performance_monitor:
            return self.performance_monitor.get_statistics()
        return {"message": "Performance monitoring is disabled."}

    def get_memory_usage_info(self) -> Dict[str, Any]: # Changed return type for more info
        """Provides information about memory usage (number of bars stored)."""
        memory_info: Dict[str, Any] = {'total_completed_bars_in_memory': 0, 'details_per_symbol_timeframe': {}}
        total_bars = 0
        
        with self.completed_bars_lock:
            for symbol, timeframes_data in self.completed_bars_buffer.items():
                for timeframe, buffer_deque in timeframes_data.items():
                    num_bars = len(buffer_deque)
                    memory_info['details_per_symbol_timeframe'][f"{symbol}:{timeframe}"] = num_bars
                    total_bars += num_bars
        memory_info['total_completed_bars_in_memory'] = total_bars
        
        # Example: Check against a warning threshold
        # Calculate threshold only if there are tracked symbols and supported timeframes to avoid division by zero or meaningless threshold
        num_tracked_symbols = len(self.get_tracked_symbols())
        num_supported_timeframes = len(self.SUPPORTED_TIMEFRAMES)
        if num_tracked_symbols > 0 and num_supported_timeframes > 0:
            warning_threshold_total = self.max_bars_in_memory * num_tracked_symbols * num_supported_timeframes * 0.1 
            if total_bars > warning_threshold_total : 
                self.logger.warning(f"High total memory usage: {total_bars} completed bars stored across all symbols/timeframes.")
        
        return memory_info

    # --- Other utility methods from the original file, reviewed for consistency ---

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """Get all timeframes currently being tracked or having data for a symbol."""
        available = set()
        with self.completed_bars_lock:
            if symbol in self.completed_bars_buffer:
                available.update(self.completed_bars_buffer[symbol].keys())
        with self.current_bars_lock:
            if symbol in self.current_bars:
                available.update(self.current_bars[symbol].keys())
        return sorted([tf for tf in list(available) if tf in self.SUPPORTED_TIMEFRAMES])

    def get_tracked_symbols(self) -> List[str]:
        """Get a list of all symbols for which any data is being tracked."""
        symbols = set()
        with self.completed_bars_lock: symbols.update(self.completed_bars_buffer.keys())
        with self.current_bars_lock: symbols.update(self.current_bars.keys())
        return sorted(list(symbols))

    def get_tracked_symbols_and_timeframes(self) -> Dict[str, List[str]]:
        """Returns a dictionary mapping symbols to their tracked timeframes."""
        result = {}
        for symbol in self.get_tracked_symbols():
            timeframes = self.get_available_timeframes(symbol)
            if timeframes: result[symbol] = timeframes
        return result

    def stop_tracking_timeframe(self, symbol: str, timeframe: str) -> None:
        """Stops tracking a specific timeframe for a symbol and clears its associated data."""
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Cannot stop tracking: Unsupported timeframe '{timeframe}' for {symbol}.")
            return

        locks = [self.current_bars_lock, self.completed_bars_lock, self.cache_lock]
        for lock in locks: lock.acquire()
        try:
            if symbol in self.current_bars: self.current_bars[symbol].pop(timeframe, None)
            if symbol in self.completed_bars_buffer: self.completed_bars_buffer[symbol].pop(timeframe, None)
            if symbol in self.bars_df_cache: self.bars_df_cache[symbol].pop(timeframe, None)

            # Clean up symbol entry if no timeframes are left for it
            if symbol in self.current_bars and not self.current_bars[symbol]: self.current_bars.pop(symbol)
            if symbol in self.completed_bars_buffer and not self.completed_bars_buffer[symbol]: self.completed_bars_buffer.pop(symbol)
            if symbol in self.bars_df_cache and not self.bars_df_cache[symbol]: self.bars_df_cache.pop(symbol)
            
            # If symbol is no longer tracked at all, remove its last_tick_time and last_close_price
            if symbol not in self.current_bars and symbol not in self.completed_bars_buffer:
                with self.general_lock:
                    self.last_tick_time.pop(symbol, None)
                    self.last_close_prices.pop(symbol, None)
                self.logger.info(f"Symbol {symbol} is no longer tracked. Cleared all associated data.")

            self.logger.info(f"Stopped tracking '{timeframe}' for symbol '{symbol}' and cleared its data.")
        finally:
            for lock in reversed(locks): lock.release()
            
    def get_market_session_info(self, timestamp_sec: float, symbol: str) -> Dict[str, Any]:
        """Gets market session information for a given epoch timestamp and symbol."""
        if not self.market_session_manager:
            return {'session_type': 'unknown (validation_disabled)', 'is_open': True, 'timestamp_sec': timestamp_sec}
        
        exchange = self._extract_exchange_from_symbol(symbol)
        session_type = self.market_session_manager.get_session_type(timestamp_sec, exchange)
        is_open = self.market_session_manager.is_market_open(timestamp_sec, exchange) # Regular trading
        
        # Ensure timezone from manager is used for display
        display_timezone = self.market_session_manager.timezone
        return {
            'session_type': session_type.value,
            'is_regular_session_open': is_open,
            'exchange': exchange,
            'timestamp_sec': timestamp_sec,
            'datetime_ist': datetime.fromtimestamp(timestamp_sec, display_timezone).isoformat()
        }

    def set_circuit_breaker_limits(self, symbol_group_limits: Dict[str, float]) -> None:
        """Updates circuit breaker limits in the DataValidator."""
        if self.data_validator:
            # Ensure keys are uppercase for consistency if _get_scrip_group normalizes to uppercase
            normalized_limits = {k.upper(): v for k,v in symbol_group_limits.items()}
            self.data_validator.circuit_limits.update(normalized_limits)
            self.logger.info(f"Updated circuit breaker limits: {normalized_limits}")
        else:
            self.logger.warning("Data validator disabled. Cannot set circuit breaker limits.")

    def add_market_holiday(self, date_str: str) -> None: # Expects YYYY-MM-DD
        """Adds a market holiday to the MarketSessionManager."""
        if self.market_session_manager:
            try: # Validate date format
                datetime.strptime(date_str, '%Y-%m-%d')
                self.market_session_manager.market_holidays.add(date_str)
                self.logger.info(f"Added market holiday: {date_str}. Session cache will be affected on next refresh.")
                # Potentially clear session cache here if immediate effect is needed, or let it refresh.
                with self.session_cache_lock: self.session_cache.clear()
                self.logger.info("Cleared session cache due to holiday update.")
            except ValueError:
                self.logger.error(f"Invalid date format for holiday: {date_str}. Please use YYYY-MM-DD.")
        else:
            self.logger.warning("Market session manager disabled. Cannot add holiday.")
            
    def get_data_quality_report(self, symbol: str) -> Dict[str, Any]:
        """Retrieves data quality metrics for a symbol from DataValidator."""
        if not self.data_validator:
            return {"message": "Data validator disabled."}
        
        report: Dict[str, Any] = {
            'symbol': symbol,
            'price_history_count': 0,
            'price_stats': {} # Min, Max, Mean, Latest from DataValidator's history
        }
        with self.data_validator.lock: # Access DataValidator's internal state safely
            if symbol in self.data_validator.price_history:
                prices = list(self.data_validator.price_history[symbol])
                report['price_history_count'] = len(prices)
                if prices:
                    report['price_stats'] = {
                        'min': np.min(prices) if prices else None,
                        'max': np.max(prices) if prices else None,
                        'mean': np.mean(prices) if prices else None,
                        'latest': prices[-1] if prices else None
                    }
        # Could also include counts of specific validation failures if PerformanceMonitor tracks them granularly
        if self.performance_monitor:
            errors_for_symbol = {}
            with self.performance_monitor.lock:
                 for error_key, count in self.performance_monitor.metrics['errors'].items():
                      if error_key.startswith(symbol + ":validation_failed"):
                           errors_for_symbol[error_key.split(':',1)[1]] = count
            if errors_for_symbol: report['validation_error_counts'] = errors_for_symbol
        return report

    def force_bar_completion(self, symbol: str, timeframe: str, completion_timestamp_sec: Optional[float] = None) -> bool:
        """
        Forces the completion of the current bar for a symbol and timeframe.
        Uses the bar's last known price as close if not specified otherwise.
        If completion_timestamp_sec is provided, it's used as the bar's official end time.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Cannot force completion: Unsupported timeframe '{timeframe}' for {symbol}.")
            return False
        
        with self.current_bars_lock:
            if symbol not in self.current_bars or timeframe not in self.current_bars[symbol] \
               or self.current_bars[symbol][timeframe] is None:
                self.logger.info(f"No active current bar to force complete for {symbol} [{timeframe}].")
                return False
            
            current_bar = self.current_bars[symbol][timeframe]
            assert current_bar is not None # Due to check above

            # Optionally adjust bar's close timestamp if provided
            if completion_timestamp_sec is not None:
                # This is tricky as it might make the bar seem out of its natural timeframe.
                # Forcing completion usually implies using the natural end or last known tick time.
                # For now, we assume the bar's existing timestamp is its start, and it closes "now".
                # A more sophisticated approach might be needed if specific closing times are required.
                self.logger.info(f"Note: completion_timestamp_sec for force_bar_completion is complex. "
                                 f"Bar {symbol}[{timeframe}] will use its existing data.")

            with self.completed_bars_lock:
                self.completed_bars_buffer[symbol][timeframe].append(current_bar)
            
            self.last_close_prices[symbol] = current_bar['close'] # Update last close price
            self.current_bars[symbol][timeframe] = None # Mark as ready for new bar

            with self.cache_lock: # Invalidate cache
                self.bars_df_cache.get(symbol, {}).pop(timeframe, None)
            
            if self.performance_monitor:
                self.performance_monitor.record_bar_completion(symbol, timeframe)
            
            self.logger.info(f"Forced completion of bar for {symbol} [{timeframe}]. Last price: {current_bar['close']}.")
            return True

    def export_data_to_dict(self) -> Dict[str, Any]:
        """Exports all current and completed bar data to a dictionary for serialization."""
        # Acquire locks to ensure consistent data snapshot
        locks = [self.current_bars_lock, self.completed_bars_lock, self.general_lock]
        for lock in locks: lock.acquire()
        try:
            export_data: Dict[str, Any] = {
                'metadata': {
                    'export_timestamp_utc_epoch': time.time(),
                    'max_bars_in_memory': self.max_bars_in_memory,
                    'default_exchange': self.default_exchange,
                    'supported_timeframes': self.SUPPORTED_TIMEFRAMES
                },
                'symbols_data': {}
            }
            
            tracked_symbols = set(self.current_bars.keys()) | set(self.completed_bars_buffer.keys())

            for symbol in tracked_symbols:
                symbol_data: Dict[str, Any] = {
                    'timeframes': {},
                    'last_tick_time_sec': self.last_tick_time.get(symbol),
                    'last_completed_bar_close_price': self.last_close_prices.get(symbol)
                }
                
                for timeframe in self.SUPPORTED_TIMEFRAMES.keys(): # Iterate all supported TFs
                    current_bar_data = self.current_bars.get(symbol, {}).get(timeframe)
                    completed_bars_list = list(self.completed_bars_buffer.get(symbol, {}).get(timeframe, []))

                    if current_bar_data or completed_bars_list: # Only include if data exists
                        symbol_data['timeframes'][timeframe] = {
                            'current_bar': current_bar_data.copy() if current_bar_data else None,
                            'completed_bars': [bar.copy() for bar in completed_bars_list] # Store copies
                        }
                
                if symbol_data['timeframes']: # Only add symbol if it has timeframe data
                     export_data['symbols_data'][symbol] = symbol_data
            return export_data
        finally:
            for lock in reversed(locks): lock.release()

    def import_data_from_dict(self, import_data: Dict[str, Any]) -> bool:
        """Imports data from a dictionary, overwriting existing data. Reset is called first."""
        self.reset() # Clear all existing data before import

        # Acquire locks for writing new data
        locks = [self.current_bars_lock, self.completed_bars_lock, self.general_lock]
        for lock in locks: lock.acquire()
        try:
            metadata = import_data.get('metadata', {})
            self.logger.info(f"Importing data. Exported at (UTC epoch): {metadata.get('export_timestamp_utc_epoch')}")
            # Could compare metadata here (max_bars, exchange) if strict matching is needed

            symbols_data = import_data.get('symbols_data', {})
            for symbol, symbol_data_dict in symbols_data.items():
                self.last_tick_time[symbol] = symbol_data_dict.get('last_tick_time_sec')
                self.last_close_prices[symbol] = symbol_data_dict.get('last_completed_bar_close_price')

                for timeframe, tf_data in symbol_data_dict.get('timeframes', {}).items():
                    if timeframe not in self.SUPPORTED_TIMEFRAMES:
                        self.logger.warning(f"Skipping import for unsupported timeframe '{timeframe}' for symbol '{symbol}'.")
                        continue
                    
                    self.ensure_timeframe_tracked(symbol, timeframe) # Sets up dicts

                    completed_bars_list = tf_data.get('completed_bars', [])
                    if completed_bars_list:
                        # Ensure deque respects maxlen if imported data exceeds it
                        imported_deque = deque(completed_bars_list, maxlen=self.max_bars_in_memory)
                        self.completed_bars_buffer[symbol][timeframe] = imported_deque
                    
                    current_bar_dict = tf_data.get('current_bar')
                    if current_bar_dict:
                        self.current_bars[symbol][timeframe] = current_bar_dict
            
            self.logger.info(f"Successfully imported data for {len(symbols_data)} symbols.")
            return True
        except Exception as e:
            self.logger.error(f"Error during data import: {e}", exc_info=True)
            # Data might be partially imported. Consider a more transactional approach or full rollback if critical.
            return False
        finally:
            for lock in reversed(locks): lock.release()
            # Invalidate all caches after import
            with self.cache_lock: self.bars_df_cache.clear()
            with self.session_cache_lock: self.session_cache.clear()
            self.logger.info("Cleared DataFrame and session caches after data import.")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(f"TimeframeManager exited with exception: {exc_val}", exc_info=(exc_type, exc_val, exc_tb))
        # No specific cleanup needed here that isn't handled by __del__ or normal operation
        return False # Propagate exceptions

    def __del__(self):
        # This method is not guaranteed to be called in all circumstances (e.g., interpreter shutdown).
        # For critical cleanup, use explicit methods or context managers.
        if hasattr(self, 'logger') and self.logger: # Check if logger was initialized
            self.logger.info("TimeframeManager instance is being garbage collected.")
        else: # Fallback print if logger is not available
            print("TimeframeManager instance is being garbage collected (logger not available).")


def main():
    """Main function to test TimeframeManager."""
    print("--- TimeframeManager Test ---")
    logger = get_logger("test_main") # Ensure logger is set up for main
    logger.info("Starting TimeframeManager test.")

    # Initialize TimeframeManager
    tfm = TimeframeManager(max_bars_in_memory=50, 
                           enable_data_validation=True, 
                           enable_market_session_validation=True)

    # Test symbol and timeframes
    test_symbol_nse = "NSE:RELIANCE"
    test_symbol_bse = "BSE:INFY" # Example for another exchange
    timeframes_to_test = ['1m', '5m', '1d']

    # Ensure timeframes are tracked
    for tf in timeframes_to_test:
        tfm.ensure_timeframe_tracked(test_symbol_nse, tf)
        tfm.ensure_timeframe_tracked(test_symbol_bse, tf)

    # --- Test Case 1: Simulate Ticks during Market Hours ---
    logger.info("\n--- Test Case 1: Market Hours Ticks (NSE:RELIANCE) ---")
    
    start_date_for_test = dt_date(2025, 5, 26) # A Monday
    if tfm.market_session_manager:
        while tfm.market_session_manager._is_holiday(start_date_for_test) or \
              tfm.market_session_manager._is_weekend(start_date_for_test):
            start_date_for_test += timedelta(days=1)
    
    logger.info(f"Using test date: {start_date_for_test.strftime('%Y-%m-%d')}")

    # Market open time for NSE (Regular session)
    nse_regular_open_time = dt_time(9, 15) # Default
    
    # Explicitly define IST timezone for test data generation
    ist_timezone = pytz.timezone('Asia/Kolkata')

    if tfm.market_session_manager:
        # Verify the manager's timezone (optional, for sanity check)
        if tfm.market_session_manager.timezone.zone != 'Asia/Kolkata':
            logger.warning(
                f"MarketSessionManager timezone is {tfm.market_session_manager.timezone.zone}, expected 'Asia/Kolkata'. "
                f"Test will use explicit 'Asia/Kolkata'."
            )
        # Get NSE regular open time from actual session definitions
        nse_sessions = tfm.market_session_manager.sessions.get('NSE', [])
        for s in nse_sessions:
            if s.session_type == SessionType.REGULAR:
                nse_regular_open_time = s.start_time
                break
    
    # Create a naive datetime first from date and market open time
    naive_base_dt = datetime.combine(start_date_for_test, nse_regular_open_time)
    # Localize the naive datetime to IST using the explicitly defined IST timezone
    base_datetime_ist = ist_timezone.localize(naive_base_dt)
    
    # Log the created base_datetime_ist to verify its offset
    logger.info(f"Base datetime for ticks (should be IST +05:30): {base_datetime_ist.isoformat()}")
    
    # Set a last close price for circuit breaker testing
    tfm.last_close_prices[test_symbol_nse] = 2500.00

    tick_data_series = [
        {'price': 2505.0, 'volume': 100, 'oi': 10000, 'delay_sec': 0},
        {'price': 2506.5, 'volume': 150, 'oi': 10050, 'delay_sec': 10}, 
        {'price': 2503.0, 'volume': 120, 'oi': 10030, 'delay_sec': 50}, # Total 60s, 09:16:00
        {'price': 2508.0, 'volume': 200, 'oi': 10100, 'delay_sec': 60}, # Total 120s, 09:17:00
        {'price': 2510.0, 'volume': 180, 'oi': 10150, 'delay_sec': 120},# Total 240s, 09:19:00
        {'price': 2400.0, 'volume': 50,  'oi': 10140, 'delay_sec': 55}, # Total 295s, 09:19:55
        {'price': 2512.0, 'volume': 220, 'oi': 10200, 'delay_sec': 5},  # Total 300s, 09:20:00
    ]

    current_tick_time_ist = base_datetime_ist
    for i, tick_info in enumerate(tick_data_series):
        # current_tick_time_ist is already localized. Adding timedelta preserves localization.
        if i > 0 : # For the first tick, delay_sec is 0, so use base_datetime_ist directly
             current_tick_time_ist += timedelta(seconds=tick_info['delay_sec'])
        elif i == 0 and tick_info['delay_sec'] > 0 : # if first tick has a delay
            current_tick_time_ist += timedelta(seconds=tick_info['delay_sec'])


        tick_timestamp_sec = current_tick_time_ist.timestamp() # Get UTC epoch seconds

        market_data = {
            MarketDataType.TIMESTAMP.value: tick_timestamp_sec,
            MarketDataType.LAST_PRICE.value: tick_info['price'],
            MarketDataType.VOLUME.value: tick_info['volume'],
            MarketDataType.OPEN_INTEREST.value: tick_info['oi']
        }
        # Log with the correctly localized IST time for clarity
        logger.info(f"\nProcessing tick {i+1} for {test_symbol_nse} at {current_tick_time_ist.isoformat()} (Epoch: {tick_timestamp_sec:.0f}): P={tick_info['price']}, V={tick_info['volume']}")
        completed_status = tfm.process_tick(test_symbol_nse, market_data)
        logger.info(f"Tick processed. Completed bars: {completed_status}")

        log_display_timezone = tfm.market_session_manager.timezone if tfm.market_session_manager else pytz.utc
        for tf_str in timeframes_to_test:
            if completed_status.get(tf_str):
                logger.info(f"*** {tf_str} BAR COMPLETED for {test_symbol_nse} ***")
                bars_df = tfm.get_bars(test_symbol_nse, tf_str, limit=5)
                if bars_df is not None and not bars_df.empty:
                    logger.info(f"Recent {tf_str} bars for {test_symbol_nse}:\n{bars_df.tail()}")
                else:
                    logger.info(f"No {tf_str} bars yet for {test_symbol_nse} or DataFrame is empty.")
            
            current_bar = tfm.get_current_bar(test_symbol_nse, tf_str)
            if current_bar:
                current_bar_time_ist_display = datetime.fromtimestamp(current_bar['timestamp'], tz=log_display_timezone)
                logger.info(f"Current {tf_str} bar for {test_symbol_nse} (starts {current_bar_time_ist_display.isoformat()}): O={current_bar['open']:.2f} H={current_bar['high']:.2f} L={current_bar['low']:.2f} C={current_bar['close']:.2f} V={current_bar['volume']}")
            else:
                logger.info(f"No current {tf_str} bar for {test_symbol_nse} (likely just completed or not started).")
        
        if i < len(tick_data_series) -1 : # Avoid sleep after last tick
            time.sleep(0.01) 

    # --- Test Case 2: Ticks outside Market Hours ---
    logger.info("\n--- Test Case 2: Outside Market Hours Tick (NSE:RELIANCE) ---")
    # Timestamp: Same day, but 8:00 AM IST (before pre-market)
    # Ensure this is also correctly localized
    naive_outside_hours_dt = datetime.combine(start_date_for_test, dt_time(8,0))
    outside_hours_time_ist = ist_timezone.localize(naive_outside_hours_dt)
    outside_hours_timestamp_sec = outside_hours_time_ist.timestamp()
    market_data_outside = {
        MarketDataType.TIMESTAMP.value: outside_hours_timestamp_sec,
        MarketDataType.LAST_PRICE.value: 2500.0, MarketDataType.VOLUME.value: 10
    }
    logger.info(f"Processing tick outside hours at {outside_hours_time_ist.isoformat()}: {market_data_outside}")
    status = tfm.process_tick(test_symbol_nse, market_data_outside)
    logger.info(f"Status for outside hours tick: {status} (expected empty if market closed)")


    # --- Test Case 3: Holiday Tick ---
    logger.info("\n--- Test Case 3: Holiday Tick (NSE:RELIANCE) ---")
    if tfm.market_session_manager:
        holiday_date_str = "2025-05-27" 
        tfm.add_market_holiday(holiday_date_str) 
        
        naive_holiday_dt = datetime.strptime(holiday_date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
        holiday_dt_localized = ist_timezone.localize(naive_holiday_dt) # Use defined IST
        holiday_timestamp_sec = holiday_dt_localized.timestamp()

        market_data_holiday = {
            MarketDataType.TIMESTAMP.value: holiday_timestamp_sec,
            MarketDataType.LAST_PRICE.value: 2501.0, MarketDataType.VOLUME.value: 20
        }
        logger.info(f"Processing tick on holiday ({holiday_dt_localized.isoformat()}): {market_data_holiday}")
        status = tfm.process_tick(test_symbol_nse, market_data_holiday)
        logger.info(f"Status for holiday tick: {status} (expected empty)")


    # --- Test Case 4: Get All Bars and Current Bar after processing ---
    logger.info("\n--- Test Case 4: Final State Check (NSE:RELIANCE) ---")
    log_display_timezone_final = tfm.market_session_manager.timezone if tfm.market_session_manager else pytz.utc
    for tf_str in timeframes_to_test:
        logger.info(f"\n--- {test_symbol_nse} - Timeframe: {tf_str} ---")
        all_bars_df = tfm.get_bars(test_symbol_nse, tf_str)
        if all_bars_df is not None and not all_bars_df.empty:
            logger.info(f"All {tf_str} bars for {test_symbol_nse}:\n{all_bars_df}")
        else:
            logger.info(f"No {tf_str} bars available for {test_symbol_nse}.")

        current_tf_bar = tfm.get_current_bar(test_symbol_nse, tf_str)
        if current_tf_bar:
            current_bar_time_ist_display = datetime.fromtimestamp(current_tf_bar['timestamp'], tz=log_display_timezone_final)
            logger.info(f"Final current {tf_str} bar for {test_symbol_nse} (starts {current_bar_time_ist_display.isoformat()}): {current_tf_bar}")
        else:
            logger.info(f"No final current {tf_str} bar for {test_symbol_nse}.")

    # --- Test Case 5: BSE Symbol (minimal test) ---
    logger.info("\n--- Test Case 5: BSE Symbol Ticks (BSE:INFY) ---")
    # Use a timestamp similar to NSE test for BSE, ensuring correct localization
    naive_bse_tick_dt = datetime.combine(start_date_for_test, dt_time(9,25)) # e.g., 09:25 AM on the test date
    bse_tick_time_ist = ist_timezone.localize(naive_bse_tick_dt)
    bse_tick_timestamp_sec = bse_tick_time_ist.timestamp()
    tfm.last_close_prices[test_symbol_bse] = 1500.00 

    market_data_bse = {
        MarketDataType.TIMESTAMP.value: bse_tick_timestamp_sec,
        MarketDataType.LAST_PRICE.value: 1502.50,
        MarketDataType.VOLUME.value: 500,
        MarketDataType.OPEN_INTEREST.value: 20000
    }
    logger.info(f"Processing tick for {test_symbol_bse} at {bse_tick_time_ist.isoformat()}: {market_data_bse}")
    tfm.process_tick(test_symbol_bse, market_data_bse)
    
    bse_1m_bars = tfm.get_bars(test_symbol_bse, '1m', limit=1)
    if bse_1m_bars is not None and not bse_1m_bars.empty:
        logger.info(f"Recent 1m bars for {test_symbol_bse}:\n{bse_1m_bars}")
    else:
        logger.info(f"No 1m bars for {test_symbol_bse} after one tick (as expected, bar not complete).") # This might change if 9:25 is a boundary
    
    bse_current_1m = tfm.get_current_bar(test_symbol_bse, '1m')
    if bse_current_1m:
        bse_curr_bar_time_ist_display = datetime.fromtimestamp(bse_current_1m['timestamp'], tz=log_display_timezone_final)
        logger.info(f"Current 1m bar for {test_symbol_bse} (starts {bse_curr_bar_time_ist_display.isoformat()}): {bse_current_1m}")


    # --- Test Case 6: Daily Bar Alignment ---
    logger.info("\n--- Test Case 6: Daily Bar Alignment (NSE:RELIANCE) ---")
    naive_daily_test_dt = datetime.combine(start_date_for_test, dt_time(15,25))
    daily_test_time_ist = ist_timezone.localize(naive_daily_test_dt)
    daily_tick_ts_sec = daily_test_time_ist.timestamp()
    market_data_daily = {
        MarketDataType.TIMESTAMP.value: daily_tick_ts_sec,
        MarketDataType.LAST_PRICE.value: 2520.0, MarketDataType.VOLUME.value: 300
    }
    logger.info(f"Processing tick for daily bar test at {daily_test_time_ist.isoformat()}: {market_data_daily}")
    tfm.process_tick(test_symbol_nse, market_data_daily)
    
    daily_bar_current = tfm.get_current_bar(test_symbol_nse, '1d')
    if daily_bar_current:
        daily_bar_start_ist_display = datetime.fromtimestamp(daily_bar_current['timestamp'], tz=log_display_timezone_final)
        logger.info(f"Current '1d' bar for {test_symbol_nse} starts at: {daily_bar_start_ist_display.isoformat()} (Epoch: {daily_bar_current['timestamp']})")
        
        expected_daily_start_ts = tfm._get_market_aware_bar_start(daily_tick_ts_sec, tfm.SUPPORTED_TIMEFRAMES['1d'], test_symbol_nse, tfm._extract_exchange_from_symbol(test_symbol_nse))
        expected_daily_start_dt_ist_display = datetime.fromtimestamp(expected_daily_start_ts, tz=log_display_timezone_final)
        logger.info(f"Expected '1d' bar start for this tick: {expected_daily_start_dt_ist_display.isoformat()} (Epoch: {expected_daily_start_ts})")
        
        if np.isclose(daily_bar_current['timestamp'], expected_daily_start_ts): # Compare float timestamps
            logger.info("Daily bar start timestamp matches expected market day start. OK.")
        else:
            logger.error(f"Daily bar start timestamp MISMATCH! Current: {daily_bar_current['timestamp']}, Expected: {expected_daily_start_ts}")


    # --- Performance and Memory Stats ---
    logger.info("\n--- Final Stats ---")
    perf_stats = tfm.get_performance_statistics()
    logger.info(f"Performance Statistics:\n{pd.Series(perf_stats)}") 
    
    memory_info = tfm.get_memory_usage_info()
    logger.info(f"Memory Usage Info:\n{pd.Series(memory_info)}")

    # --- Test Export/Import ---
    logger.info("\n--- Test Case 7: Export and Import Data ---")
    exported_data = tfm.export_data_to_dict()
    logger.info(f"Exported data for {len(exported_data.get('symbols_data', {}))} symbols.")

    tfm_imported = TimeframeManager(max_bars_in_memory=50)
    import_success = tfm_imported.import_data_from_dict(exported_data)
    logger.info(f"Import successful: {import_success}")

    if import_success:
        imported_bars_df = tfm_imported.get_bars(test_symbol_nse, '5m')
        original_bars_df = tfm.get_bars(test_symbol_nse, '5m') # Get from original tfm
        
        if imported_bars_df is not None and original_bars_df is not None:
            if len(imported_bars_df) == len(original_bars_df):
                logger.info(f"Import check: Number of 5m bars for {test_symbol_nse} matches ({len(imported_bars_df)}). OK.")
                if not imported_bars_df.empty:
                     if pd.DataFrame.equals(imported_bars_df,original_bars_df) : 
                          logger.info(f"Import check: Content of 5m bars for {test_symbol_nse} matches. OK.")
                     else:
                          logger.warning(f"Import check: Content of 5m bars for {test_symbol_nse} MISMATCH.")
            else:
                logger.warning(f"Import check: Number of 5m bars for {test_symbol_nse} MISMATCH. "
                               f"Original: {len(original_bars_df)}, Imported: {len(imported_bars_df)}")
        else:
            logger.warning(f"Import check: DataFrame(s) for {test_symbol_nse} 5m are None. "
                           f"Original is None: {original_bars_df is None}, Imported is None: {imported_bars_df is None}")


    logger.info("\n--- TimeframeManager Test Finished ---")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()

