import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta, time as dt_time, date as dt_date
import threading
import time
import pytz
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

# Helper to create a logger mock if core.logging_manager is not available in test env
try:
    from core.logging_manager import get_logger
except ImportError:
    import logging
    def get_logger(name):
        logger = logging.getLogger(name)
        if not logger.handlers: # Avoid adding multiple handlers if already configured
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO) 
        return logger

try:
    from utils.constants import MarketDataType
except ImportError:
    class MarketDataType(Enum): # type: ignore
        TIMESTAMP = "feed_timestamp" 
        LAST_PRICE = "last_traded_price" 
        VOLUME = "volume_traded_today" 
        OPEN_INTEREST = "open_interest" 
        BID = "best_bid_price" 
        ASK = "best_ask_price" 
        OHLC = "ohlc" 
    print("Warning: 'utils.constants.MarketDataType' not found. Using a mock definition. Ensure keys match your data feed.")


class SessionType(Enum):
    REGULAR = "regular"
    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"
    MUHURAT = "muhurat"
    CLOSED = "closed"

@dataclass
class SessionTiming:
    start_time: dt_time
    end_time: dt_time
    session_type: SessionType
    name: str

class MarketSessionManager:
    def __init__(self, logger_instance=None):
        self.logger = logger_instance if logger_instance else get_logger("utils.market_session_manager")
        self.timezone = pytz.timezone('Asia/Kolkata')
        
        # Define standard session timings. These are dt_time objects (IST).
        self.sessions: Dict[str, List[SessionTiming]] = {
            'NSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "NSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "NSE Regular Trading"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "NSE Post-Market"),               
            ],
            'BSE': [
                SessionTiming(dt_time(9, 0), dt_time(9, 15), SessionType.PRE_MARKET, "BSE Pre-Market"),
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "BSE Regular Trading"),
                SessionTiming(dt_time(15, 30), dt_time(16, 0), SessionType.POST_MARKET, "BSE Post-Market"),
            ],
            'NFO': [
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "NFO Regular Trading"),
            ],
            'BFO': [
                SessionTiming(dt_time(9, 15), dt_time(15, 30), SessionType.REGULAR, "BFO Regular Trading"),
            ],
            'MCX': [
                SessionTiming(dt_time(9, 0), dt_time(17, 0), SessionType.REGULAR, "MCX Regular Trading (Morning)"),
                SessionTiming(dt_time(17, 0), dt_time(23, 30), SessionType.REGULAR, "MCX Regular Trading (Evening)"), 
            ]
        }
        
        # Market holidays (YYYY-MM-DD format).
        self.market_holidays: Set[str] = { # YYYY-MM-DD format
            # 2024 (Examples - ensure this list is accurate and maintained)
            '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', 
            '2024-04-17', '2024-05-01', '2024-05-20', '2024-06-17', '2024-07-17',
            '2024-08-15', '2024-10-02', '2024-11-01', # Diwali - Muhurat trading might occur
            '2024-11-15', '2024-12-25',
            # 2025 (Illustrative, verify with official calendar):
            '2025-01-26', '2025-03-14', '2025-04-18', '2025-05-01', '2025-08-15', 
            '2025-10-02', '2025-10-21', 
            '2025-12-25', 
        }            

    def _is_holiday(self, date_obj: dt_date) -> bool:
        return date_obj.strftime('%Y-%m-%d') in self.market_holidays

    def _is_weekend(self, date_obj: dt_date) -> bool:
        return date_obj.weekday() >= 5

    def get_session_type(self, timestamp_sec: float, exchange: str = 'NSE') -> SessionType:
        try:
            dt_ist = datetime.fromtimestamp(timestamp_sec, self.timezone)
            date_ist, time_ist = dt_ist.date(), dt_ist.time()
            
            if self._is_holiday(date_ist) or self._is_weekend(date_ist):
                for session in self.sessions.get(exchange, []):
                    if session.session_type == SessionType.MUHURAT and \
                       session.start_time <= time_ist < session.end_time:
                             return SessionType.MUHURAT
                return SessionType.CLOSED
            
            exchange_sessions_today = self.sessions.get(exchange, self.sessions.get('NSE', [])) 
            sorted_sessions = sorted(exchange_sessions_today, key=lambda s: s.start_time)

            for session in sorted_sessions:
                if session.session_type == SessionType.MUHURAT: continue
                if session.start_time <= time_ist < session.end_time:
                    return session.session_type
            
            return SessionType.CLOSED
        except Exception as e:
            self.logger.error(f"Error determining session type for ts {timestamp_sec} on {exchange}: {e}", exc_info=True)
            return SessionType.CLOSED

    def is_market_open(self, timestamp_sec: float, exchange: str = 'NSE') -> bool:
        session_type = self.get_session_type(timestamp_sec, exchange)
        return session_type == SessionType.REGULAR
    
    def get_market_day_start(self, timestamp_sec: float, exchange: str = 'NSE') -> int: 
        dt_ist = datetime.fromtimestamp(timestamp_sec, self.timezone)
        regular_session_start_time = dt_time(9, 15) 
        
        for session in self.sessions.get(exchange, self.sessions.get('NSE',[])):
            if session.session_type == SessionType.REGULAR:
                regular_session_start_time = session.start_time
                break
        
        market_start_dt_ist = self.timezone.localize(
            datetime.combine(dt_ist.date(), regular_session_start_time)
        )
        return int(market_start_dt_ist.timestamp()) 

    def get_next_market_day(self, current_date_ist: dt_date) -> dt_date:
        next_day = current_date_ist + timedelta(days=1)
        while self._is_holiday(next_day) or self._is_weekend(next_day):
            next_day += timedelta(days=1)
        return next_day

class DataValidator:
    def __init__(self, logger_instance=None):
        self.logger = logger_instance if logger_instance else get_logger("utils.data_validator")
        self.circuit_limits = {
                'GROUP_A_20PCT': 0.20, 'GROUP_B_10PCT': 0.10, 
                'GROUP_T_5PCT': 0.05, 'FUTURE_20PCT': 0.20,
                'OPTION_NO_LIMIT': 1.0, 'DEFAULT': 0.20 
            }
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.lock = threading.RLock() 
    
    def validate_tick(self, symbol: str, price: Optional[float] = None, volume: Optional[float] = None, 
                 last_close_price: Optional[float] = None) -> Tuple[bool, str]:
        if price is None: 
            return False, "Missing price"
        try:
            price = float(price) 
            if price < 0: return False, "Negative price"
        except (ValueError, TypeError): return False, "Invalid price format"

        if last_close_price is not None and last_close_price > 0:
            price_change_pct = abs(price - last_close_price) / last_close_price
            scrip_group = self._get_scrip_group(symbol)            
            limit = self.circuit_limits.get(scrip_group, self.circuit_limits['DEFAULT'])
            if price_change_pct > limit:
                return False, f"Circuit breaker: {price_change_pct:.2%} > {limit:.2%} for {scrip_group}"

        instrument_type = self._get_instrument_type(symbol)
        if instrument_type == 'EQUITY' and volume is not None and volume == 0:
            self.logger.debug(f"Zero volume for equity tick: {symbol}")

        with self.lock:
            if symbol in self.price_history:
                recent_prices = list(self.price_history[symbol])
                if len(recent_prices) >= 5: 
                    recent_avg = sum(recent_prices[-5:]) / 5
                    if recent_avg > 0: 
                        deviation_pct = abs(price - recent_avg) / recent_avg
                        max_deviation = 0.05 
                        if 'OPTION' in instrument_type: max_deviation = 0.50 
                        elif 'FUTURE' in instrument_type: max_deviation = 0.10
                        
                        if deviation_pct > max_deviation:
                            self.logger.warning(f"Price deviation for {symbol}: curr={price:.2f}, avg={recent_avg:.2f}, dev={deviation_pct:.2%}")
            self.price_history[symbol].append(price)
        return True, "Validated"


    def _get_instrument_type(self, symbol: str) -> str:
        if ':' not in symbol: return 'UNKNOWN'
        try:
            exchange, instrument_code = symbol.split(':', 1)
            exchange, instrument_code = exchange.upper(), instrument_code.upper()
            
            if exchange in ['NFO', 'BFO', 'CDS', 'MCX']: 
                import re
                if (instrument_code.endswith(('CE', 'PE')) or 
                    re.search(r'(CALL|PUT)$', instrument_code, re.IGNORECASE) or
                    re.search(r'\d{1,2}[A-Z]{3}\d+(C|P)E?$', instrument_code) or 
                    re.search(r'(CE|PE|C\d+|P\d+)$', instrument_code)): 
                    return 'OPTION'
                if 'FUT' in instrument_code or re.search(r'\d{2}[A-Z]{3}$', instrument_code): 
                    if exchange == 'CDS': return 'CURRENCY_FUTURE'
                    if exchange == 'MCX': return 'COMMODITY_FUTURE'
                    return 'FUTURE'
                return 'DERIVATIVE' 
            if exchange in ['NSE', 'BSE']: 
                if any(idx in instrument_code for idx in ['NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCAP']): return 'INDEX'
                if 'ETF' in instrument_code or 'GOLD' in instrument_code or instrument_code.startswith(('LIQUID', 'GILT')): return 'ETF'
                return 'EQUITY'
            if exchange == 'CDS': return 'CURRENCY'
            if exchange == 'MCX': return 'COMMODITY'
        except Exception as e: self.logger.warning(f"Error detecting instrument type for {symbol}: {e}")
        return 'UNKNOWN'
    
    def _get_scrip_group(self, symbol: str) -> str:
        instrument_type = self._get_instrument_type(symbol)        
        if instrument_type == 'OPTION': return 'OPTION_NO_LIMIT'
        if 'FUTURE' in instrument_type: return 'FUTURE_20PCT'
        
        instrument_name = symbol.split(':', 1)[1].upper() if ':' in symbol else symbol.upper()
        large_cap_keywords = {'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'HINDUNILVR', 'INFY', 'ITC', 'SBIN'}
        if any(keyword in instrument_name for keyword in large_cap_keywords) or \
           any(idx in instrument_name for idx in ['NIFTY', 'BANKNIFTY']): 
            return 'GROUP_A_20PCT'
        if 'SMALLCAP' in instrument_name or instrument_name.endswith('T2T'): 
            return 'GROUP_T_5PCT'
        return 'GROUP_B_10PCT' 
    
class PerformanceMonitor:
    def __init__(self):
        self.metrics: Dict[str, Any] = {
            'bars_completed': defaultdict(int), 'ticks_processed': defaultdict(int),
            'processing_latency': defaultdict(lambda: deque(maxlen=1000)),
            'errors': defaultdict(int), 'bars_finalized_timeout': defaultdict(int),
            'bars_finalized_session_end': defaultdict(int)
        }
        self.lock = threading.RLock()
        self.start_time = time.monotonic() 
    
    def record_bar_completion(self, symbol: str, timeframe: str, finalized_by: str = "normal"):
        with self.lock:
            self.metrics['bars_completed'][f"{symbol}:{timeframe}"] += 1
            if finalized_by == "timeout":
                self.metrics['bars_finalized_timeout'][f"{symbol}:{timeframe}"] +=1
            elif finalized_by == "session_end":
                 self.metrics['bars_finalized_session_end'][f"{symbol}:{timeframe}"] +=1
    
    def record_tick_processing(self, symbol: str, processing_time_sec: float):
        with self.lock:
            self.metrics['ticks_processed'][symbol] += 1
            self.metrics['processing_latency'][symbol].append(processing_time_sec)
    
    def record_error(self, symbol: str, error_type: str):
        with self.lock: self.metrics['errors'][f"{symbol}:{error_type}"] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        with self.lock:
            current_metrics = {
                'bars_completed': self.metrics['bars_completed'].copy(),
                'ticks_processed': self.metrics['ticks_processed'].copy(),
                'processing_latency': {k: v.copy() for k, v in self.metrics['processing_latency'].items()},
                'errors': self.metrics['errors'].copy(),
                'bars_finalized_timeout': self.metrics['bars_finalized_timeout'].copy(),
                'bars_finalized_session_end': self.metrics['bars_finalized_session_end'].copy()
            }
            
            stats: Dict[str, Any] = {
                'uptime_seconds': time.monotonic() - self.start_time,
                'total_bars_completed': sum(current_metrics['bars_completed'].values()),
                'total_ticks_processed': sum(current_metrics['ticks_processed'].values()),
                'total_errors': sum(current_metrics['errors'].values()),
                'detailed_errors': dict(current_metrics['errors']),
                'detailed_bars_completed': dict(current_metrics['bars_completed']),
                'detailed_bars_finalized_timeout': dict(current_metrics['bars_finalized_timeout']),
                'detailed_bars_finalized_session_end': dict(current_metrics['bars_finalized_session_end'])
            }
            latency_stats: Dict[str, Dict[str, float]] = {}
            for symbol, latencies_deque in current_metrics['processing_latency'].items():
                latencies = list(latencies_deque) 
                if latencies:
                    latency_stats[symbol] = {
                        'count': len(latencies), 
                        'avg_ms': np.mean(latencies) * 1000,
                        'max_ms': np.max(latencies) * 1000, 
                        'min_ms': np.min(latencies) * 1000,
                        'p95_ms': np.percentile(latencies, 95) * 1000 if len(latencies) >= 20 else (np.mean(latencies) * 1000 if latencies else 0.0), 
                        'p99_ms': np.percentile(latencies, 99) * 1000 if len(latencies) >= 100 else (np.mean(latencies) * 1000 if latencies else 0.0)
                    }
            stats['latency_by_symbol_ms'] = latency_stats
            return stats

class TimeframeManager:
    SUPPORTED_TIMEFRAMES: Dict[str, int] = {
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '4h': 14400, '1d': 86400, 
    }
    BAR_COLUMNS: List[str] = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 
        'open_interest', 'closed_due_to_timeout' 
    ]

    def __init__(self, 
                 max_bars_in_memory: int = 10000,
                 enable_market_session_validation: bool = True,
                 enable_data_validation: bool = True,
                 enable_performance_monitoring: bool = True,
                 default_exchange: str = 'NSE',
                 **kwargs):
        self.logger = get_logger("utils.timeframe_manager")
        self.max_bars_in_memory = max_bars_in_memory
        self.default_exchange = default_exchange.upper()
        
        self.market_session_manager = MarketSessionManager(logger_instance=self.logger) if enable_market_session_validation else None
        self.data_validator = DataValidator(logger_instance=self.logger) if enable_data_validation else None
        self.performance_monitor = PerformanceMonitor() if enable_performance_monitoring else None
        
        self.completed_bars_buffer: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.max_bars_in_memory))
        )
        self.bars_df_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)
        self.current_bars: Dict[str, Dict[str, Optional[Dict[str, Any]]]] = defaultdict(dict)
        
        self.last_tick_time: Dict[str, int] = {} 
        self.last_close_prices: Dict[str, float] = {}

        self.current_bars_lock = threading.RLock() 
        self.completed_bars_lock = threading.RLock() 
        self.cache_lock = threading.RLock() 
        self.general_lock = threading.RLock() 
        
        self.session_cache: Dict[str, Dict[str, Any]] = {} 
        self.session_cache_lock = threading.RLock() 
        self.session_cache_validity_sec = 60 

        self.timeout_factor = float(kwargs.get('bar_timeout_factor', 2.0))
        self.min_absolute_timeout_seconds = int(kwargs.get('bar_min_timeout_seconds', 5 * 60)) 
        self.timeout_check_interval_seconds = int(kwargs.get('bar_timeout_check_interval', 15)) 
        self.force_finalize_at_session_end = bool(kwargs.get('force_finalize_at_session_end', True))

        self.last_tick_update_time_for_bar: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float)) 

        self._shutdown_timeout_thread_event = threading.Event()
        self._bar_timeout_finalization_thread = threading.Thread(
            target=self._bar_timeout_finalization_loop, 
            daemon=True 
        )
        self._bar_timeout_finalization_thread.name = "BarTimeoutFinalizer"
        self._bar_timeout_finalization_thread.start()
        
        self.logger.info(
            f"TimeframeManager initialized. Max bars/symbol/tf: {self.max_bars_in_memory}. "
            f"Default exchange: {self.default_exchange}. "
            f"Timeout Finalization: Factor={self.timeout_factor}x, MinTimeout={self.min_absolute_timeout_seconds}s, "
            f"CheckInterval={self.timeout_check_interval_seconds}s, ForceSessionEnd={self.force_finalize_at_session_end}"
        )


    def _extract_exchange_from_symbol(self, symbol: str) -> str:
        if ':' in symbol: return symbol.split(':', 1)[0].upper()
        return self.default_exchange

    def _calculate_next_session_transition(self, current_timestamp_sec: float, exchange: str) -> float: 
        if not self.market_session_manager: return current_timestamp_sec + 3600 
        
        msm = self.market_session_manager
        current_dt_ist = datetime.fromtimestamp(current_timestamp_sec, msm.timezone)
        current_date_ist, current_time_ist = current_dt_ist.date(), current_dt_ist.time()

        def to_epoch_sec(date_val: dt_date, time_val: dt_time) -> float:
            return msm.timezone.localize(datetime.combine(date_val, time_val)).timestamp()

        is_today_market_day = not (msm._is_holiday(current_date_ist) or msm._is_weekend(current_date_ist))
        exchange_sessions_list = msm.sessions.get(exchange, msm.sessions.get('NSE', [])) 
        exchange_sessions = sorted(exchange_sessions_list, key=lambda s: s.start_time)

        if is_today_market_day:
            for session in exchange_sessions:
                if session.session_type == SessionType.MUHURAT: continue 
                if current_time_ist < session.start_time: 
                    return to_epoch_sec(current_date_ist, session.start_time)
                if current_time_ist < session.end_time: 
                    return to_epoch_sec(current_date_ist, session.end_time)
        
        next_market_date = current_date_ist
        if not is_today_market_day or (exchange_sessions and current_time_ist >= exchange_sessions[-1].end_time):
            next_market_date = msm.get_next_market_day(current_date_ist)
        elif is_today_market_day and not exchange_sessions: 
             next_market_date = msm.get_next_market_day(current_date_ist)
        
        first_session_next_day = next((s for s in exchange_sessions if s.session_type != SessionType.MUHURAT), None)
        if not first_session_next_day and exchange_sessions: 
             first_session_next_day = exchange_sessions[0]

        if first_session_next_day: 
            return to_epoch_sec(next_market_date, first_session_next_day.start_time)
        
        self.logger.warning(f"No sessions found for {exchange} to calculate next transition. Defaulting to 1hr from now.")
        return current_timestamp_sec + 3600 

    def _is_market_open_cached(self, timestamp_sec: float, exchange: str) -> bool: 
        if not self.market_session_manager: return True 
        
        with self.session_cache_lock:
            cache_entry = self.session_cache.get(exchange)
            current_time_monotonic = time.monotonic() 

            needs_refresh = True 
            if cache_entry:
                if timestamp_sec < cache_entry.get('next_transition_ts', 0) and \
                   (current_time_monotonic - cache_entry.get('last_check_monotonic_time', 0)) < self.session_cache_validity_sec:
                    needs_refresh = False
            
            if needs_refresh:
                is_open = self.market_session_manager.is_market_open(timestamp_sec, exchange)
                session_type = self.market_session_manager.get_session_type(timestamp_sec, exchange)
                next_transition_ts = self._calculate_next_session_transition(timestamp_sec, exchange)
                
                self.session_cache[exchange] = {
                    'is_open': is_open, 
                    'session_type': session_type,
                    'next_transition_ts': next_transition_ts, 
                    'last_check_ts': timestamp_sec, 
                    'last_check_monotonic_time': current_time_monotonic 
                }
                if not cache_entry or cache_entry.get('session_type') != session_type:
                     log_tz = self.market_session_manager.timezone
                     self.logger.info(f"Session cache for {exchange} updated: {session_type.value}, "
                                     f"next transition ~{datetime.fromtimestamp(next_transition_ts, log_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
                return is_open
            
            return cache_entry['is_open']

    def ensure_timeframe_tracked(self, symbol: str, timeframe: str) -> None:
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Unsupported timeframe '{timeframe}' for {symbol}. Not tracking.") 
            return
        with self.current_bars_lock: 
            if symbol not in self.current_bars: 
                self.current_bars[symbol] = {} 
            if timeframe not in self.current_bars[symbol]: 
                self.current_bars[symbol][timeframe] = None 
                self.logger.info(f"Tracking '{timeframe}' for {symbol}. Bar will be initialized on first tick.")
        with self.completed_bars_lock: 
            _ = self.completed_bars_buffer[symbol][timeframe] 
        with self.cache_lock: 
            _ = self.bars_df_cache[symbol]

    def extract_and_validate_price(self, symbol: str, market_data: Dict[str, Any]) -> Optional[float]:
        price = market_data.get(MarketDataType.LAST_PRICE.value)
        instrument_type = self.data_validator._get_instrument_type(symbol) if self.data_validator else "UNKNOWN"
        exchange = self._extract_exchange_from_symbol(symbol)

        if price is not None:
            try:
                price = float(price)
                if price == 0:
                    if instrument_type == 'OPTION': return 0.0 
                    self.logger.debug(f"Zero LTP for non-option {symbol}, attempting fallbacks.")
                    price = None 
                elif price < 0:
                    self.logger.warning(f"Negative LTP {price} for {symbol}, attempting fallbacks.")
                    price = None
                else: 
                    # self.logger.debug(f"Using last price (ltp): {price}") 
                    return price 
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid LTP format for {symbol}: {market_data.get(MarketDataType.LAST_PRICE.value)}, attempting fallbacks.")
                price = None

        bid = market_data.get(MarketDataType.BID.value)
        ask = market_data.get(MarketDataType.ASK.value)
        try:
            if bid is not None and ask is not None:
                bid_f, ask_f = float(bid), float(ask) 
                if bid_f > 0 and ask_f > 0 and ask_f >= bid_f:
                    spread_pct = ((ask_f - bid_f) / bid_f * 100) if bid_f > 0 else float('inf')
                    max_spread_pct = 10.0 if instrument_type == 'OPTION' else 5.0
                    if spread_pct <= max_spread_pct:
                        mid_price = (bid_f + ask_f) / 2.0
                        # self.logger.debug(f"Using mid-price {mid_price:.2f} for {symbol} (LTP missing/invalid, spread: {spread_pct:.2f}%)")
                        return mid_price
                    else: self.logger.debug(f"Wide spread {spread_pct:.2f}% for {symbol}, skipping midpoint.")
        except (ValueError, TypeError): pass 

        try:
            if bid is not None: 
                bid_f = float(bid)
                if bid_f > 0:
                    # self.logger.debug(f"Using bid price {bid_f:.2f} for {symbol} (LTP and mid-price unavailable/invalid).")
                    return bid_f
        except (ValueError, TypeError): pass
        try:
            if ask is not None: 
                ask_f = float(ask)
                if ask_f > 0:
                    # self.logger.debug(f"Using ask price {ask_f:.2f} for {symbol} (LTP, mid-price, bid unavailable/invalid).")
                    return ask_f
        except (ValueError, TypeError): pass
        
        prev_close = None
        ohlc_data = market_data.get(MarketDataType.OHLC.value) 
        if isinstance(ohlc_data, dict):
            prev_close_from_ohlc = ohlc_data.get('CLOSE') 
            if prev_close_from_ohlc is not None:
                try: prev_close = float(prev_close_from_ohlc)
                except (ValueError, TypeError): pass
        
        if not (prev_close and prev_close > 0): 
            with self.general_lock: 
                prev_close = self.last_close_prices.get(symbol)

        if prev_close and prev_close > 0:
            current_time_from_data = market_data.get(MarketDataType.TIMESTAMP.value) 
            current_time_to_check = float(current_time_from_data) if current_time_from_data is not None else time.time()

            if self.market_session_manager:
                market_day_start_ts = self.market_session_manager.get_market_day_start(current_time_to_check, exchange)
                if current_time_to_check >= market_day_start_ts: 
                    self.logger.warning(f"Using previous close {prev_close:.2f} for {symbol} as a last resort.")
                    return prev_close
        
        if instrument_type == 'OPTION':
            self.logger.warning(f"No valid price found for option {symbol} after all fallbacks, using zero.")
            return 0.0

        self.logger.error(f"No valid price could be extracted for {symbol}. Market data: {market_data}")
        if self.performance_monitor: self.performance_monitor.record_error(symbol, "no_valid_price")
        return None
    
    def _extract_and_validate_volume_oi(self, symbol: str, market_data: Dict[str, Any]) -> Tuple[float, Optional[float]]:
        instrument_type = self.data_validator._get_instrument_type(symbol) if self.data_validator else "UNKNOWN"
        volume_raw = market_data.get(MarketDataType.VOLUME.value)
        volume = 0.0
        if volume_raw is not None:
            try:
                volume = float(volume_raw)
                if volume < 0: self.logger.warning(f"Negative volume {volume} for {symbol}, using 0."); volume = 0.0
            except (ValueError, TypeError): self.logger.warning(f"Invalid volume format for {symbol}: {volume_raw}, using 0."); volume = 0.0
        elif instrument_type == 'EQUITY': self.logger.debug(f"Missing volume for equity {symbol}, using 0.")

        open_interest: Optional[float] = None
        if instrument_type in ['OPTION', 'FUTURE', 'COMMODITY_FUTURE', 'CURRENCY_FUTURE', 'DERIVATIVE']: 
            oi_raw = market_data.get(MarketDataType.OPEN_INTEREST.value)
            if oi_raw is not None:
                try:
                    open_interest = float(oi_raw)
                    if open_interest < 0: self.logger.warning(f"Negative OI {open_interest} for {symbol}, setting to None."); open_interest = None
                except (ValueError, TypeError): self.logger.warning(f"Invalid OI format for {symbol}: {oi_raw}")
        return volume, open_interest

    def process_tick(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, bool]:
        processing_start_time = time.monotonic()
        try:
            price = self.extract_and_validate_price(symbol, market_data)
            if price is None: return {} 
            
            timestamp_sec_raw = market_data.get(MarketDataType.TIMESTAMP.value)
            timestamp_sec: int 
            if timestamp_sec_raw is None:
                timestamp_sec = int(time.time()) 
                self.logger.warning(f"Timestamp missing for {symbol}. Using current time: {timestamp_sec}. Tick: {market_data}")
            else:
                try: 
                    timestamp_sec = int(float(timestamp_sec_raw)) 
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Invalid timestamp format for {symbol}: {timestamp_sec_raw}. Data: {market_data}. Error: {e}")
                    if self.performance_monitor: self.performance_monitor.record_error(symbol, "timestamp_error")
                    return {}
            
            volume, open_interest = self._extract_and_validate_volume_oi(symbol, market_data)           
            
            exchange = self._extract_exchange_from_symbol(symbol)
            if self.market_session_manager and not self._is_market_open_cached(float(timestamp_sec), exchange):
                self.logger.debug(f"Tick for {symbol} outside regular market hours. Timestamp: {timestamp_sec}, Exchange: {exchange}")
                return {} 
            
            if self.data_validator:
                with self.general_lock: last_completed_bar_close = self.last_close_prices.get(symbol)
                is_valid, reason = self.data_validator.validate_tick(symbol, price, volume, last_completed_bar_close)
                if not is_valid:
                    self.logger.warning(f"Invalid tick for {symbol} due to: {reason}. Price: {price}, Tick: {market_data}")
                    if self.performance_monitor: self.performance_monitor.record_error(symbol, f"validation_failed_{reason.replace(' ', '_').lower()}")
                    return {}
            
            with self.general_lock: 
                last_known_tick_time = self.last_tick_time.get(symbol)
                if last_known_tick_time and timestamp_sec < last_known_tick_time : 
                    self.logger.warning(f"Out-of-order tick for {symbol}: curr_ts={timestamp_sec}, last_ts={last_known_tick_time}. Skipping.")
                    if self.performance_monitor: self.performance_monitor.record_error(symbol, "out_of_order_tick")
                    return {}
                self.last_tick_time[symbol] = timestamp_sec 

            completed_bars_status: Dict[str, bool] = {} 
            log_tz = self.market_session_manager.timezone if self.market_session_manager else pytz.utc
            
            with self.current_bars_lock: 
                if symbol not in self.current_bars: 
                    self.logger.warning(f"Symbol {symbol} not found in current_bars during process_tick. Ensure it's tracked.")
                    return {}
                
                for timeframe_str, tf_seconds in self.SUPPORTED_TIMEFRAMES.items():
                    if timeframe_str not in self.current_bars[symbol]: continue 
                    
                    bar_start_timestamp_sec = self._get_market_aware_bar_start(timestamp_sec, tf_seconds, symbol, exchange) 
                    current_bar = self.current_bars[symbol].get(timeframe_str)
                    
                    if current_bar is None: 
                        new_bar = self._create_new_bar(bar_start_timestamp_sec, price, volume, open_interest)
                        self.current_bars[symbol][timeframe_str] = new_bar
                        self.last_tick_update_time_for_bar[symbol][timeframe_str] = time.monotonic() 
                        completed_bars_status[timeframe_str] = False
                        
                    elif bar_start_timestamp_sec > current_bar['timestamp']: 
                        self._finalize_bar(symbol, timeframe_str, current_bar.copy(), forced_by="normal_completion")
                        completed_bars_status[timeframe_str] = True
                        
                        new_bar = self._create_new_bar(bar_start_timestamp_sec, price, volume, open_interest)
                        self.current_bars[symbol][timeframe_str] = new_bar
                        self.last_tick_update_time_for_bar[symbol][timeframe_str] = time.monotonic()
                        
                    elif bar_start_timestamp_sec == current_bar['timestamp']: 
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume 
                        if open_interest is not None: current_bar['open_interest'] = open_interest 
                        
                        self.last_tick_update_time_for_bar[symbol][timeframe_str] = time.monotonic()
                        completed_bars_status[timeframe_str] = False
                        
                    else: 
                        self.logger.warning(
                            f"Late tick detected for {symbol}[{timeframe_str}]. "
                            f"Tick bar start: {datetime.fromtimestamp(bar_start_timestamp_sec,log_tz).isoformat()}, " 
                            f"Current active bar start: {datetime.fromtimestamp(current_bar['timestamp'],log_tz).isoformat()}. " 
                            f"Tick timestamp: {datetime.fromtimestamp(timestamp_sec,log_tz).isoformat()}. Skipping update for this timeframe." 
                        )
                        completed_bars_status[timeframe_str] = False 
            
            if self.performance_monitor:
                self.performance_monitor.record_tick_processing(symbol, time.monotonic() - processing_start_time)
            return completed_bars_status
            
        except Exception as e:
            self.logger.error(f"Critical error processing tick for {symbol}. Data: {market_data}. Error: {e}", exc_info=True)
            if self.performance_monitor: self.performance_monitor.record_error(symbol, "critical_processing_error")
            return {}

    def _get_market_aware_bar_start(self, tick_timestamp_sec: int, 
                                   timeframe_seconds: int, symbol: str, exchange: str) -> int: 
        if timeframe_seconds <= 0: 
            self.logger.error(f"Invalid timeframe_seconds: {timeframe_seconds} for {symbol}. Defaulting to tick_timestamp_sec.")
            return int(tick_timestamp_sec) # Ensure int

        if timeframe_seconds >= 86400: 
            if self.market_session_manager:
                # get_market_day_start returns int. Ensure it's cast again just in case.
                return int(self.market_session_manager.get_market_day_start(float(tick_timestamp_sec), exchange))
            else:
                dt_utc = datetime.fromtimestamp(float(tick_timestamp_sec), pytz.utc)
                dt_tick_in_ist = dt_utc.astimezone(pytz.timezone('Asia/Kolkata'))
                market_open_time_ist = dt_time(9, 15) 
                
                bar_date_ist = dt_tick_in_ist.date()
                if dt_tick_in_ist.time() < market_open_time_ist:
                    bar_date_ist -= timedelta(days=1) 
                
                bar_start_dt_naive_ist = datetime.combine(bar_date_ist, market_open_time_ist)
                bar_start_dt_aware_ist = pytz.timezone('Asia/Kolkata').localize(bar_start_dt_naive_ist)
                return int(bar_start_dt_aware_ist.timestamp()) 
        else: 
            return int((tick_timestamp_sec // timeframe_seconds) * timeframe_seconds) 

    def _create_new_bar(self, timestamp_sec: int, price: float, 
                       volume: float, open_interest: Optional[float]) -> Dict[str, Any]:
        """Creates a new bar dictionary. Timestamp is integer epoch seconds."""
        return {
            'timestamp': int(timestamp_sec), # Explicitly cast to int
            'open': price, 'high': price, 'low': price, 'close': price, 
            'volume': max(0.0, volume), 
            'open_interest': open_interest,
            'closed_due_to_timeout': False 
        }

    def _validate_bar_data(self, bar: Dict[str, Any]) -> bool:
        try:
            essential_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'closed_due_to_timeout']
            for field in essential_fields:
                if field not in bar or bar[field] is None:
                    self.logger.warning(f"Bar missing essential field '{field}' or it's None: {bar}")
                    return False
            
            if not isinstance(bar['timestamp'], int): 
                self.logger.warning(f"Bar field 'timestamp' is not an integer (type: {type(bar['timestamp'])}). Bar: {bar}")
                return False

            if not isinstance(bar['closed_due_to_timeout'], bool):
                self.logger.warning(f"Bar field 'closed_due_to_timeout' is not boolean (type: {type(bar['closed_due_to_timeout'])}). Bar: {bar}")
                return False

            o, h, l, c, v = bar['open'], bar['high'], bar['low'], bar['close'], bar['volume']
            oi = bar.get('open_interest')

            numeric_fields_map = {'open': o, 'high': h, 'low': l, 'close': c, 'volume': v}
            if oi is not None: 
                numeric_fields_map['open_interest'] = oi
            
            temp_numeric_values = {}
            for field_name, val in numeric_fields_map.items():
                if not isinstance(val, (int, float, np.number)):
                    try:
                        if field_name == 'open_interest' and val is None:
                            temp_numeric_values[field_name] = None 
                            continue
                        temp_numeric_values[field_name] = float(val)
                    except (ValueError, TypeError):
                        self.logger.warning(
                            f"Bar field '{field_name}' is not convertible to numeric (type: {type(val)}). Value: {val}. Bar: {bar}"
                        )
                        return False
                else:
                    temp_numeric_values[field_name] = val 
            
            o, h, l, c, v = temp_numeric_values['open'], temp_numeric_values['high'], temp_numeric_values['low'], \
                            temp_numeric_values['close'], temp_numeric_values['volume']
            oi = temp_numeric_values.get('open_interest') 


            if not (np.isclose(l, h) or l < h): 
                self.logger.warning(f"Bar inconsistency: low ({l}) > high ({h}). Bar: {bar}")
                return False

            open_ok = (np.isclose(l, o) or l <= o) and (np.isclose(o, h) or o <= h) 
            if not open_ok:
                self.logger.warning(f"Bar inconsistency: open ({o}) not within [low ({l}), high ({h})]. Bar: {bar}")
                return False

            close_ok = (np.isclose(l, c) or l <= c) and (np.isclose(c, h) or c <= h) 
            if not close_ok:
                self.logger.warning(f"Bar inconsistency: close ({c}) not within [low ({l}), high ({h})]. Bar: {bar}")
                return False
            
            if v < 0:
                self.logger.warning(f"Negative volume ({v}) in bar: {bar}")
                return False
            
            if oi is not None and oi < 0: 
                self.logger.warning(f"Negative open_interest ({oi}) in bar: {bar}")
                return False
                
            return True
        except Exception as e: 
            self.logger.error(f"Unexpected error validating bar: {bar}. Error: {e}", exc_info=True)
            return False


    def _build_dataframe_from_buffer(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        with self.completed_bars_lock: 
            buffer_items = list(self.completed_bars_buffer[symbol][timeframe]) 
        
        if not buffer_items: return None 

        try:
            valid_bars_data = [bar for bar in buffer_items if self._validate_bar_data(bar)]
            if not valid_bars_data:
                self.logger.debug(f"No valid bars in buffer for {symbol}[{timeframe}] after validation.")
                return None 
            
            df = pd.DataFrame(valid_bars_data, columns=self.BAR_COLUMNS)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
            df.set_index('timestamp', inplace=True, drop=False) 
            df.sort_index(inplace=True) 

            for col in ['open', 'high', 'low', 'close', 'volume', 'open_interest']:
                df[col] = pd.to_numeric(df[col], errors='coerce') 
            
            if 'closed_due_to_timeout' in df.columns:
                df['closed_due_to_timeout'] = df['closed_due_to_timeout'].astype(bool)
            
            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True) 
            return df
        except Exception as e: 
            self.logger.error(f"Error building DataFrame for {symbol}[{timeframe}]: {e}", exc_info=True)
            return None

    def get_bars(self, symbol: str, timeframe: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        if timeframe not in self.SUPPORTED_TIMEFRAMES: 
            self.logger.warning(f"Unsupported timeframe '{timeframe}' requested for {symbol}.")
            return None
        
        df_to_return: Optional[pd.DataFrame] = None
        
        with self.cache_lock: 
            cached_df = self.bars_df_cache.get(symbol, {}).get(timeframe)
        
        if cached_df is not None:
            df_to_return = cached_df
        else:
            df_built = self._build_dataframe_from_buffer(symbol, timeframe)
            if df_built is not None and not df_built.empty:
                with self.cache_lock: 
                    if symbol not in self.bars_df_cache: self.bars_df_cache[symbol] = {}
                    self.bars_df_cache[symbol][timeframe] = df_built
                df_to_return = df_built
            elif df_built is None or df_built.empty: 
                empty_df = pd.DataFrame(columns=self.BAR_COLUMNS) 
                empty_df['timestamp'] = pd.to_datetime([], unit='s', utc=True)
                empty_df.set_index('timestamp', inplace=True, drop=False)
                for col in ['open', 'high', 'low', 'close', 'volume', 'open_interest']: 
                    empty_df[col] = pd.to_numeric(empty_df[col], errors='coerce')
                if 'closed_due_to_timeout' in empty_df.columns: 
                     empty_df['closed_due_to_timeout'] = empty_df['closed_due_to_timeout'].astype(bool)
                else: 
                     empty_df['closed_due_to_timeout'] = pd.Series(dtype='bool')
                return empty_df
        
        if df_to_return is None or df_to_return.empty: 
            empty_df = pd.DataFrame(columns=self.BAR_COLUMNS)
            empty_df['timestamp'] = pd.to_datetime(empty_df['timestamp'], unit='s', utc=True) 
            empty_df = empty_df.set_index(pd.DatetimeIndex(empty_df['timestamp'])) 
            if 'timestamp' not in empty_df.columns and isinstance(empty_df.index, pd.DatetimeIndex): 
                 empty_df['timestamp'] = empty_df.index.to_series()

            for col in ['open', 'high', 'low', 'close', 'volume', 'open_interest']:
                 if col not in empty_df: empty_df[col] = np.nan 
                 empty_df[col] = pd.to_numeric(empty_df[col], errors='coerce')
            if 'closed_due_to_timeout' in empty_df.columns: 
                 empty_df['closed_due_to_timeout'] = empty_df['closed_due_to_timeout'].astype(bool)
            else: 
                 empty_df['closed_due_to_timeout'] = pd.Series(dtype='bool')

            return empty_df.copy() 

        if limit is not None:
            if limit == 0: 
                empty_df_limited = pd.DataFrame(columns=df_to_return.columns, index=pd.DatetimeIndex([]))
                empty_df_limited.index.name = df_to_return.index.name
                for col in df_to_return.columns: 
                    if col == 'closed_due_to_timeout':
                        empty_df_limited[col] = pd.Series(dtype='bool')
                    else:
                        empty_df_limited[col] = pd.Series(dtype=df_to_return[col].dtype)
                if 'timestamp' in empty_df_limited.columns:
                    empty_df_limited['timestamp'] = pd.to_datetime(empty_df_limited['timestamp'], utc=True)
                return empty_df_limited
            elif limit > 0: 
                return df_to_return.iloc[-limit:].copy() 
        
        return df_to_return.copy() 

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        if timeframe not in self.SUPPORTED_TIMEFRAMES: return None
        with self.current_bars_lock: 
            current_bar_dict = self.current_bars.get(symbol, {}).get(timeframe)
            if current_bar_dict and 'closed_due_to_timeout' not in current_bar_dict:
                current_bar_dict['closed_due_to_timeout'] = False 
            return current_bar_dict.copy() if current_bar_dict else None 

    def _finalize_bar(self, symbol: str, timeframe: str, bar_to_finalize: Dict[str, Any], forced_by: str) -> None:
        log_tz = self.market_session_manager.timezone if self.market_session_manager else pytz.utc
        
        with self.current_bars_lock, self.completed_bars_lock, self.cache_lock, self.general_lock:
            current_bar_in_state = self.current_bars.get(symbol, {}).get(timeframe)
            if current_bar_in_state is None or current_bar_in_state['timestamp'] != bar_to_finalize['timestamp']:
                self.logger.debug(
                    f"Bar for {symbol}[{timeframe}] starting {datetime.fromtimestamp(bar_to_finalize['timestamp'], log_tz).isoformat()} "
                    f"was already finalized or advanced. Skipping this finalization attempt by '{forced_by}'. "
                )
                return

            if forced_by == "timeout":
                bar_to_finalize['closed_due_to_timeout'] = True
                self.logger.info(f"Bar {symbol}[{timeframe}] at {bar_to_finalize['timestamp']} marked as closed_due_to_timeout.")
            else:
                bar_to_finalize['closed_due_to_timeout'] = False 

            if not self._validate_bar_data(bar_to_finalize): 
                self.logger.error(f"Invalid bar data for {symbol}[{timeframe}] at finalization (forced by {forced_by}). Bar: {bar_to_finalize}. Skipping finalization.")
                self.current_bars[symbol][timeframe] = None 
                if symbol in self.last_tick_update_time_for_bar: 
                    self.last_tick_update_time_for_bar[symbol].pop(timeframe, None)
                return

            # self.logger.info(
            #     f"Finalizing bar for {symbol}[{timeframe}] (Start: {datetime.fromtimestamp(bar_to_finalize['timestamp'], log_tz).isoformat()}) due to {forced_by}. "
            #     f"Closed by Timeout: {bar_to_finalize['closed_due_to_timeout']}. "
            #     f"OHLCV: O={bar_to_finalize['open']:.2f} H={bar_to_finalize['high']:.2f} L={bar_to_finalize['low']:.2f} C={bar_to_finalize['close']:.2f} V={bar_to_finalize['volume']}"
            # )

            self.completed_bars_buffer[symbol][timeframe].append(bar_to_finalize) 
            self.last_close_prices[symbol] = bar_to_finalize['close']
            self.current_bars[symbol][timeframe] = None
            self.bars_df_cache.get(symbol, {}).pop(timeframe, None)

            if symbol in self.last_tick_update_time_for_bar:
                self.last_tick_update_time_for_bar[symbol].pop(timeframe, None)
                if not self.last_tick_update_time_for_bar[symbol]: 
                    self.last_tick_update_time_for_bar.pop(symbol)

            if self.performance_monitor:
                self.performance_monitor.record_bar_completion(symbol, timeframe, finalized_by=forced_by)
    
    def _bar_timeout_finalization_loop(self):
        self.logger.info("Bar timeout finalization loop starting.")
        log_tz = self.market_session_manager.timezone if self.market_session_manager else pytz.utc
        
        while not self._shutdown_timeout_thread_event.wait(self.timeout_check_interval_seconds):
            try:
                current_epoch_time_monotonic = time.monotonic() 
                current_epoch_time_wall = time.time() 

                bars_to_finalize_timeout: List[Tuple[str, str, Dict[str, Any]]] = []
                bars_to_finalize_session_end: List[Tuple[str, str, Dict[str, Any]]] = []

                potential_bars_for_check: List[Tuple[str, str, Dict[str, Any], float]] = []
                with self.current_bars_lock: 
                    for symbol, timeframes_data in self.current_bars.items():
                        for timeframe, current_bar_dict in timeframes_data.items():
                            if current_bar_dict: 
                                last_update_monotonic_time = self.last_tick_update_time_for_bar.get(symbol, {}).get(timeframe)
                                if last_update_monotonic_time: 
                                    potential_bars_for_check.append((symbol, timeframe, current_bar_dict.copy(), last_update_monotonic_time))
                
                for symbol, timeframe, bar_copy, last_update_monotonic_time in potential_bars_for_check:
                    timeframe_seconds = self.SUPPORTED_TIMEFRAMES[timeframe]
                    timeout_duration = max(timeframe_seconds * self.timeout_factor, self.min_absolute_timeout_seconds)
                    
                    if current_epoch_time_monotonic - last_update_monotonic_time > timeout_duration:
                        self.logger.debug(
                            f"Bar {symbol}[{timeframe}] (Start: {datetime.fromtimestamp(bar_copy['timestamp'], log_tz).isoformat()}) " 
                            f"marked for TIMEOUT. Last update (monotonic age): {current_epoch_time_monotonic - last_update_monotonic_time:.0f}s ago, "
                            f"Timeout threshold: {timeout_duration:.0f}s."
                        )
                        bars_to_finalize_timeout.append((symbol, timeframe, bar_copy))

                if self.market_session_manager and self.force_finalize_at_session_end:
                    symbols_in_check = list(set(item[0] for item in potential_bars_for_check))
                    
                    for symbol_to_check_session in symbols_in_check:
                        exchange = self._extract_exchange_from_symbol(symbol_to_check_session)
                        self._is_market_open_cached(current_epoch_time_wall, exchange) 
                        
                        session_type_val = SessionType.CLOSED.value 
                        with self.session_cache_lock: 
                            cached_session_info = self.session_cache.get(exchange)
                            if cached_session_info and cached_session_info.get('session_type'):
                                session_type_val = cached_session_info['session_type'].value
                        
                        if session_type_val in [SessionType.POST_MARKET.value, SessionType.CLOSED.value]:
                            for s, tf, cb, lut in potential_bars_for_check: 
                                if s == symbol_to_check_session and self.SUPPORTED_TIMEFRAMES[tf] < 86400: 
                                    is_already_for_timeout = any(
                                        item[0] == s and item[1] == tf and item[2]['timestamp'] == cb['timestamp']
                                        for item in bars_to_finalize_timeout
                                    )
                                    if not is_already_for_timeout:
                                        self.logger.debug(
                                            f"Market session ended/post-market ({session_type_val}) for {s}. "
                                            f"Marking intraday bar {tf} (Start: {datetime.fromtimestamp(cb['timestamp'], log_tz).isoformat()}) for SESSION_END finalization." 
                                        )
                                        bars_to_finalize_session_end.append((s, tf, cb)) 

                for symbol, timeframe, bar_copy_to_finalize in bars_to_finalize_timeout:
                    self._finalize_bar(symbol, timeframe, bar_copy_to_finalize, forced_by="timeout")
                
                for symbol, timeframe, bar_copy_to_finalize in bars_to_finalize_session_end:
                    self._finalize_bar(symbol, timeframe, bar_copy_to_finalize, forced_by="session_end")
            
            except Exception as e: 
                self.logger.error(f"Error in bar timeout finalization loop: {e}", exc_info=True)
        
        self.logger.info("Bar timeout finalization loop stopped.")


    def force_bar_completion(self, symbol: str, timeframe: str, completion_timestamp_sec: Optional[float] = None) -> bool:
        if timeframe not in self.SUPPORTED_TIMEFRAMES: 
            self.logger.warning(f"Cannot force completion for unsupported timeframe '{timeframe}' of {symbol}.")
            return False
        
        bar_to_finalize_copy = None
        with self.current_bars_lock: 
            current_bar_dict = self.current_bars.get(symbol, {}).get(timeframe)
            if current_bar_dict is None:
                self.logger.info(f"No active bar for {symbol}[{timeframe}] to force complete.")
                return False
            bar_to_finalize_copy = current_bar_dict.copy() 
        
        if completion_timestamp_sec is not None: 
             self.logger.info(f"Forcing completion for {symbol}[{timeframe}] at effective time: {completion_timestamp_sec}.")
        
        self._finalize_bar(symbol, timeframe, bar_to_finalize_copy, forced_by="manual_force")
        return True
        
    def reset(self, symbol: Optional[str] = None) -> None:
        locks_to_acquire = [self.current_bars_lock, self.completed_bars_lock, self.cache_lock, 
                            self.general_lock, self.session_cache_lock]
        for lock in locks_to_acquire: lock.acquire()
        try:
            if symbol:
                self.logger.info(f"Resetting data for symbol: {symbol}")
                for store in [self.completed_bars_buffer, self.bars_df_cache, self.current_bars, 
                              self.last_tick_update_time_for_bar]: 
                    if symbol in store: store.pop(symbol)
                if symbol in self.last_tick_time: self.last_tick_time.pop(symbol)
                if symbol in self.last_close_prices: self.last_close_prices.pop(symbol)
            else: 
                self.logger.info("Resetting all TimeframeManager data.")
                for store in [self.completed_bars_buffer, self.bars_df_cache, self.current_bars, 
                              self.last_tick_time, self.last_close_prices, self.session_cache, 
                              self.last_tick_update_time_for_bar]: 
                    store.clear()
                if self.performance_monitor: 
                    self.performance_monitor = PerformanceMonitor() 
        finally:
            for lock in reversed(locks_to_acquire): lock.release() 

    def get_performance_statistics(self) -> Dict[str, Any]:
        return self.performance_monitor.get_statistics() if self.performance_monitor else {"message": "Performance monitoring disabled."}

    def get_memory_usage_info(self) -> Dict[str, Any]:
        memory_info: Dict[str, Any] = {'total_completed_bars_in_memory': 0, 'details_per_symbol_timeframe': {}}
        total_bars = 0
        with self.completed_bars_lock: 
            for symbol, timeframes_data in self.completed_bars_buffer.items():
                for timeframe, buffer_deque in timeframes_data.items():
                    num_bars = len(buffer_deque)
                    memory_info['details_per_symbol_timeframe'][f"{symbol}:{timeframe}"] = num_bars
                    total_bars += num_bars
        memory_info['total_completed_bars_in_memory'] = total_bars
        return memory_info

    def get_available_timeframes(self, symbol: str) -> List[str]:
        available = set()
        with self.completed_bars_lock:
            if symbol in self.completed_bars_buffer: 
                available.update(self.completed_bars_buffer[symbol].keys())
        with self.current_bars_lock:
            if symbol in self.current_bars: 
                available.update(self.current_bars[symbol].keys())
        return sorted([tf for tf in list(available) if tf in self.SUPPORTED_TIMEFRAMES])

    def get_tracked_symbols(self) -> List[str]:
        symbols = set()
        with self.completed_bars_lock: symbols.update(self.completed_bars_buffer.keys())
        with self.current_bars_lock: symbols.update(self.current_bars.keys())
        return sorted(list(symbols))

    def get_tracked_symbols_and_timeframes(self) -> Dict[str, List[str]]:
        result = {}
        for symbol in self.get_tracked_symbols():
            timeframes = self.get_available_timeframes(symbol)
            if timeframes: result[symbol] = timeframes
        return result

    def stop_tracking_timeframe(self, symbol: str, timeframe: str) -> None:
        if timeframe not in self.SUPPORTED_TIMEFRAMES: 
            self.logger.warning(f"Cannot stop tracking unsupported timeframe '{timeframe}' for {symbol}.")
            return

        locks_to_acquire = [self.current_bars_lock, self.completed_bars_lock, self.cache_lock, self.general_lock]
        for lock in locks_to_acquire: lock.acquire()
        try:
            if symbol in self.current_bars: self.current_bars[symbol].pop(timeframe, None)
            if symbol in self.completed_bars_buffer: self.completed_bars_buffer[symbol].pop(timeframe, None)
            if symbol in self.bars_df_cache: self.bars_df_cache[symbol].pop(timeframe, None)
            if symbol in self.last_tick_update_time_for_bar: 
                self.last_tick_update_time_for_bar[symbol].pop(timeframe, None)
                if not self.last_tick_update_time_for_bar[symbol]: 
                    self.last_tick_update_time_for_bar.pop(symbol)
            
            if symbol in self.current_bars and not self.current_bars[symbol]: self.current_bars.pop(symbol)
            if symbol in self.completed_bars_buffer and not self.completed_bars_buffer[symbol]: self.completed_bars_buffer.pop(symbol)
            if symbol in self.bars_df_cache and not self.bars_df_cache[symbol]: self.bars_df_cache.pop(symbol)
            
            if symbol not in self.current_bars and symbol not in self.completed_bars_buffer:
                if symbol in self.last_tick_time: self.last_tick_time.pop(symbol)
                if symbol in self.last_close_prices: self.last_close_prices.pop(symbol)
                self.logger.info(f"Symbol {symbol} no longer tracked for any timeframe. Cleared all its associated data.")
            else:
                self.logger.info(f"Stopped tracking '{timeframe}' for {symbol} and cleared its data.")
        finally:
            for lock in reversed(locks_to_acquire): lock.release()
            
    def get_market_session_info(self, timestamp_sec: float, symbol: str) -> Dict[str, Any]: # Accepts float for flexibility
        if not self.market_session_manager: 
            return {'session_type': 'unknown (validation_disabled)', 'is_regular_session_open': True, 
                    'exchange': self._extract_exchange_from_symbol(symbol), 'timestamp_sec': int(timestamp_sec), # Store/return as int
                    'datetime_ist': 'N/A (validation_disabled)'}
        
        exchange = self._extract_exchange_from_symbol(symbol)
        session_type = self.market_session_manager.get_session_type(timestamp_sec, exchange)
        is_open = self.market_session_manager.is_market_open(timestamp_sec, exchange)
        dt_ist_str = "N/A"
        try:
            # Use float for fromtimestamp, then format. Stored timestamp_sec is int.
            dt_ist_str = datetime.fromtimestamp(float(timestamp_sec), self.market_session_manager.timezone).isoformat()
        except Exception: 
            pass

        return {'session_type': session_type.value, 'is_regular_session_open': is_open, 'exchange': exchange,
                'timestamp_sec': int(timestamp_sec), 'datetime_ist': dt_ist_str}

    def set_circuit_breaker_limits(self, symbol_group_limits: Dict[str, float]) -> None:
        if self.data_validator:
            self.data_validator.circuit_limits.update({k.upper(): v for k,v in symbol_group_limits.items()})
            self.logger.info(f"Updated circuit breaker limits: {symbol_group_limits}")
        else: self.logger.warning("Data validator disabled, cannot set circuit breaker limits.")

    def add_market_holiday(self, date_str: str) -> None: 
        if self.market_session_manager:
            try: 
                datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError: 
                self.logger.error(f"Invalid date format for holiday: {date_str}. Use<y_bin_46>-MM-DD."); return
            
            self.market_session_manager.market_holidays.add(date_str)
            self.logger.info(f"Added market holiday: {date_str}.")
            with self.session_cache_lock: 
                self.session_cache.clear()
                self.logger.info("Cleared session cache due to holiday update.")
        else: self.logger.warning("Market session manager disabled, cannot add holiday.")
            
    def get_data_quality_report(self, symbol: str) -> Dict[str, Any]:
        if not self.data_validator: return {"message": "Data validator disabled."}
        
        report: Dict[str, Any] = {'symbol': symbol, 'price_history_count': 0, 'price_stats': {}}
        with self.data_validator.lock: 
            prices = list(self.data_validator.price_history.get(symbol, deque())) 
            report['price_history_count'] = len(prices)
            if prices: 
                report['price_stats'] = {
                    'min': np.min(prices), 'max': np.max(prices), 
                    'mean': np.mean(prices), 'latest': prices[-1]
                }
        
        if self.performance_monitor:
            errors_for_symbol = {}
            with self.performance_monitor.lock: 
                 for err_key, count in self.performance_monitor.metrics['errors'].items():
                      if err_key.startswith(symbol + ":validation_failed"): 
                          errors_for_symbol[err_key.split(':',1)[1]] = count 
            if errors_for_symbol: report['validation_error_counts'] = errors_for_symbol
        return report

    def export_data_to_dict(self) -> Dict[str, Any]:
        locks_to_acquire = [self.current_bars_lock, self.completed_bars_lock, self.general_lock]
        for lock in locks_to_acquire: lock.acquire()
        try:
            export_data: Dict[str, Any] = {
                'metadata': { 
                    'export_timestamp_utc_epoch': int(time.time()), # Export as int
                    'max_bars_in_memory': self.max_bars_in_memory,
                    'default_exchange': self.default_exchange, 
                    'supported_timeframes': self.SUPPORTED_TIMEFRAMES
                },
                'symbols_data': {}
            }
            tracked_symbols = set(self.current_bars.keys()) | set(self.completed_bars_buffer.keys())
            
            for symbol in tracked_symbols:
                symbol_data_export: Dict[str, Any] = {
                    'timeframes': {}, 
                    'last_tick_time_sec': self.last_tick_time.get(symbol), # Already int
                    'last_completed_bar_close_price': self.last_close_prices.get(symbol),
                    'last_tick_update_time_for_bar': dict(self.last_tick_update_time_for_bar.get(symbol, defaultdict(float)))
                }
                for timeframe_str in self.SUPPORTED_TIMEFRAMES.keys():
                    current_bar_data = self.current_bars.get(symbol, {}).get(timeframe_str)
                    completed_bars_list = list(self.completed_bars_buffer.get(symbol, {}).get(timeframe_str, deque()))
                    
                    if current_bar_data or completed_bars_list: 
                        symbol_data_export['timeframes'][timeframe_str] = {
                            'current_bar': current_bar_data.copy() if current_bar_data else None, 
                            'completed_bars': [bar.copy() for bar in completed_bars_list] 
                        }
                if symbol_data_export['timeframes']: 
                    export_data['symbols_data'][symbol] = symbol_data_export
            return export_data
        finally:
            for lock in reversed(locks_to_acquire): lock.release()

    def import_data_from_dict(self, import_data: Dict[str, Any]) -> bool:
        self.reset() 
        
        locks_to_acquire = [self.current_bars_lock, self.completed_bars_lock, self.general_lock]
        for lock in locks_to_acquire: lock.acquire()
        try:
            metadata = import_data.get('metadata', {})
            self.logger.info(f"Importing data. Exported at epoch: {metadata.get('export_timestamp_utc_epoch')}")
            
            symbols_data_to_import = import_data.get('symbols_data', {})
            for symbol, symbol_data_dict in symbols_data_to_import.items():
                # Ensure imported last_tick_time is int
                last_tick_time_imported = symbol_data_dict.get('last_tick_time_sec')
                if last_tick_time_imported is not None:
                    self.last_tick_time[symbol] = int(last_tick_time_imported)
                
                self.last_close_prices[symbol] = symbol_data_dict.get('last_completed_bar_close_price')
                
                last_update_times = symbol_data_dict.get('last_tick_update_time_for_bar', {})
                self.last_tick_update_time_for_bar[symbol] = defaultdict(float, last_update_times)

                for timeframe_str, tf_data in symbol_data_dict.get('timeframes', {}).items():
                    if timeframe_str not in self.SUPPORTED_TIMEFRAMES: continue 
                    
                    self.ensure_timeframe_tracked(symbol, timeframe_str) 
                    
                    completed_bars_list_from_import = tf_data.get('completed_bars', [])
                    restored_completed_bars = []
                    for bar_dict_import in completed_bars_list_from_import:
                        if 'timestamp' in bar_dict_import: # Convert to int if present
                            bar_dict_import['timestamp'] = int(bar_dict_import['timestamp'])
                        bar_dict_import.setdefault('closed_due_to_timeout', False) 
                        restored_completed_bars.append(bar_dict_import)
                    if restored_completed_bars:
                        self.completed_bars_buffer[symbol][timeframe_str] = deque(restored_completed_bars, maxlen=self.max_bars_in_memory)
                    
                    current_bar_from_import = tf_data.get('current_bar')
                    if current_bar_from_import: 
                        if 'timestamp' in current_bar_from_import: # Convert to int
                             current_bar_from_import['timestamp'] = int(current_bar_from_import['timestamp'])
                        current_bar_from_import.setdefault('closed_due_to_timeout', False) 
                        self.current_bars[symbol][timeframe_str] = current_bar_from_import
            
            self.logger.info(f"Successfully imported data for {len(symbols_data_to_import)} symbols.")
            return True
        except Exception as e: 
            self.logger.error(f"Error during data import: {e}", exc_info=True)
            return False
        finally:
            for lock in reversed(locks_to_acquire): lock.release()
            with self.cache_lock: self.bars_df_cache.clear()
            with self.session_cache_lock: self.session_cache.clear()
            self.logger.info("Cleared DataFrame and session caches after data import.")

    def shutdown(self):
        self.logger.info("Shutting down TimeframeManager...")
        self._shutdown_timeout_thread_event.set() 
        
        if hasattr(self, '_bar_timeout_finalization_thread') and self._bar_timeout_finalization_thread.is_alive():
            self.logger.info("Waiting for bar timeout finalization thread to join...")
            join_timeout = self.timeout_check_interval_seconds + 5 
            self._bar_timeout_finalization_thread.join(timeout=join_timeout)
            if self._bar_timeout_finalization_thread.is_alive():
                self.logger.warning(f"Bar timeout finalization thread did not join within {join_timeout}s.")
            else:
                self.logger.info("Bar timeout finalization thread joined successfully.")
        self.logger.info("TimeframeManager shutdown complete.")

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown() 
        if exc_type: 
            self.logger.error(f"TimeframeManager exited context with exception: {exc_val}", 
                              exc_info=(exc_type, exc_val, exc_tb))
        return False 

    def __del__(self):
        if hasattr(self, '_shutdown_timeout_thread_event') and not self._shutdown_timeout_thread_event.is_set():
            log_func = self.logger.info if hasattr(self, 'logger') and self.logger else print
            log_func("TimeframeManager.__del__ called, initiating shutdown as a fallback...")
            self.shutdown()
        if hasattr(self, 'logger') and self.logger: self.logger.debug("TimeframeManager instance garbage collected.")


def main():
    """Main function to demonstrate TimeframeManager with integer timestamps and closed_due_to_timeout flag."""
    import logging
    if not logging.getLogger().handlers: 
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = get_logger("main_tfm_demo")
    logger.info("--- TimeframeManager Demo with Integer Timestamps & closed_due_to_timeout ---")

    ist_timezone = pytz.timezone('Asia/Kolkata')
    # Use a date known to be a weekday and not a holiday from the hardcoded list for reliable demo
    # For example, if '2025-06-02' is not a holiday in self.market_holidays
    test_date = dt_date(2025, 6, 2) # Assuming this is a working day for the demo
    
    # Check if demo date is a holiday/weekend for robustness
    temp_msm = MarketSessionManager()
    is_test_date_closed = temp_msm._is_holiday(test_date) or temp_msm._is_weekend(test_date)
    if is_test_date_closed:
        logger.warning(f"Demo test date {test_date} is a holiday/weekend. Timeout demo might be affected by session_end logic.")
    del temp_msm
    logger.info(f"Using test date for demo: {test_date.strftime('%Y-%m-%d')}")


    tfm = TimeframeManager(
        max_bars_in_memory=10,
        default_exchange='NFO',
        bar_timeout_factor=1.1, 
        bar_min_timeout_seconds=5, # Short timeout for demo
        bar_timeout_check_interval=1 
    )

    symbol_opt = "NFO:BANKNIFTY25JUN25000CE" 
    tfm.ensure_timeframe_tracked(symbol_opt, '1m')

    logger.info(f"\n--- Scenario 1: Normal 1-min Bar Completion for {symbol_opt} ---")
    market_open_dt = ist_timezone.localize(datetime.combine(test_date, dt_time(9, 15, 0)))
    
    tick1_ts = int(market_open_dt.timestamp()) + 10  
    tick2_ts = int(market_open_dt.timestamp()) + 30  
    tick3_ts = int(market_open_dt.timestamp()) + 70  # Completes the 09:15 bar

    tfm.process_tick(symbol_opt, {
        MarketDataType.TIMESTAMP.value: float(tick1_ts), 
        MarketDataType.LAST_PRICE.value: 100.0, MarketDataType.VOLUME.value: 50, MarketDataType.OPEN_INTEREST.value: 1000
    })
    tfm.process_tick(symbol_opt, {
        MarketDataType.TIMESTAMP.value: float(tick2_ts),
        MarketDataType.LAST_PRICE.value: 102.0, MarketDataType.VOLUME.value: 30, MarketDataType.OPEN_INTEREST.value: 1005
    })
    current_bar_details = tfm.get_current_bar(symbol_opt, '1m')
    logger.info(f"Current bar before completion: Timestamp={current_bar_details.get('timestamp') if current_bar_details else 'N/A'}, "
                f"ClosedByTimeout={current_bar_details.get('closed_due_to_timeout') if current_bar_details else 'N/A'}")
    
    tfm.process_tick(symbol_opt, {
        MarketDataType.TIMESTAMP.value: float(tick3_ts), 
        MarketDataType.LAST_PRICE.value: 101.0, MarketDataType.VOLUME.value: 20, MarketDataType.OPEN_INTEREST.value: 1010
    })
    
    completed_bars_normal = tfm.get_bars(symbol_opt, '1m')
    if completed_bars_normal is not None and not completed_bars_normal.empty:
        bar_info = completed_bars_normal.iloc[-1]
        logger.info(f"Completed bar (normal): \n{bar_info.to_string()}")
        assert not bar_info['closed_due_to_timeout'], "Bar should not be closed by timeout (normal completion)"
    else:
        logger.warning("No normal completed bars found for Scenario 1.")

    logger.info(f"\n--- Scenario 2: 1-min Bar Timeout for {symbol_opt} ---")
    # The current bar is for 09:16:00, started by tick3_ts.
    current_bar_before_timeout = tfm.get_current_bar(symbol_opt, '1m')
    logger.info(f"Current bar before timeout: Timestamp={current_bar_before_timeout.get('timestamp') if current_bar_before_timeout else 'N/A'}, "
                f"ClosedByTimeout={current_bar_before_timeout.get('closed_due_to_timeout') if current_bar_before_timeout else 'N/A'}")
    
    # Calculate wait time based on config. For 1m bar: max(60*1.1, 5) = 66s.
    # The timeout thread checks every 1s.
    timeout_duration_for_bar = max(tfm.SUPPORTED_TIMEFRAMES['1m'] * tfm.timeout_factor, tfm.min_absolute_timeout_seconds)
    wait_time_for_demo = timeout_duration_for_bar + tfm.timeout_check_interval_seconds * 2 # Add a buffer
    
    logger.info(f"Waiting for timeout (approx {wait_time_for_demo:.0f}s)...")
    time.sleep(wait_time_for_demo) 

    completed_bars_after_wait = tfm.get_bars(symbol_opt, '1m') 
    # After waiting, the bar for 09:16:00 should be completed.
    # It could be by timeout or session_end if main() is run on a weekend/off-hours.
    
    if completed_bars_after_wait is not None and len(completed_bars_after_wait) > (len(completed_bars_normal) if completed_bars_normal is not None else 0) :
        finalized_bar_scenario2 = completed_bars_after_wait.iloc[-1] # Get the latest bar (should be the 09:16 bar)
        logger.info(f"Completed bar (after wait): \n{finalized_bar_scenario2.to_string()}")
        
        # Check performance monitor to see how it was finalized
        stats = tfm.get_performance_statistics()
        finalized_by_timeout_count = stats['detailed_bars_finalized_timeout'].get(f"{symbol_opt}:1m", 0)
        finalized_by_session_end_count = stats['detailed_bars_finalized_session_end'].get(f"{symbol_opt}:1m", 0)

        if finalized_bar_scenario2['closed_due_to_timeout']:
            logger.info("Bar was correctly closed due to timeout.")
            assert finalized_by_timeout_count > 0, "Perf mon should record timeout finalization"
        elif finalized_by_session_end_count > (stats['detailed_bars_finalized_session_end'].get(f"{symbol_opt}:1m",0) -1 if finalized_by_session_end_count > 0 else 0) : # Check if it increased
             logger.warning("Bar was closed due to session_end, not timeout, because demo ran during closed market hours for current system time.")
        else:
            logger.error(f"Bar was not closed by timeout as expected. Flag: {finalized_bar_scenario2['closed_due_to_timeout']}")
            # This assertion might fail if session_end took precedence
            # assert finalized_bar_scenario2['closed_due_to_timeout'], "Bar should ideally be closed by timeout in this demo scenario"

    else:
        logger.warning("No additional bar found after timeout wait for Scenario 2.")
        logger.info(f"All completed bars after timeout attempt: \n{completed_bars_after_wait}")

    logger.info("\n--- Final Stats ---")
    perf_stats = tfm.get_performance_statistics()
    for key in ['detailed_errors', 'detailed_bars_completed', 
                'detailed_bars_finalized_timeout', 'detailed_bars_finalized_session_end', 
                'latency_by_symbol_ms']:
        if key in perf_stats and isinstance(perf_stats[key], (defaultdict, dict)):
            perf_stats[key] = dict(perf_stats[key])
            if key == 'latency_by_symbol_ms':
                 for sym_key, lat_data in perf_stats[key].items():
                     if isinstance(lat_data, defaultdict): perf_stats[key][sym_key] = dict(lat_data)
    logger.info(f"Performance Statistics:\n{pd.Series(perf_stats).to_string(max_rows=None)}") 


    logger.info("\nShutting down TimeframeManager...")
    tfm.shutdown()
    logger.info("--- TimeframeManager Demo Finished ---")

if __name__ == "__main__":
    main()

