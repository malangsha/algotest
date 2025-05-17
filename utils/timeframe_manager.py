import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import threading
import time
from collections import defaultdict, deque

from core.logging_manager import get_logger
from utils.constants import MarketDataType

class TimeframeManager:
    """
    Manages the conversion of tick data to various timeframes (e.g., 1m, 5m, 1h)
    and provides mechanisms to store and retrieve this bar data. 
    Optimized for low latency by minimizing DataFrame operations during tick processing.
    It uses a buffer for completed bars and converts to DataFrame on demand.

    Key Features:
    - Converts ticks to OHLCV bars for multiple timeframes.
    - Caches DataFrames for quick retrieval.
    - Thread-safe operations.
    - Handles potential timestamp gaps and out-of-order ticks.
    """

    # Defines all supported timeframes and their duration in seconds.
    # This is the single source of truth for timeframe validation and processing.
    # SUPPORTED_TIMEFRAMES: Dict[str, int] = {
    #     # Tick data is not aggregated, so duration is 0, but listed for completeness.
    #     # Process_tick will skip aggregation for timeframes with 0 second duration.
    #     # 'tick': 0, 
    #     '1s': 1, '5s': 5, '10s': 10, '30s': 30,
    #     '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
    #     '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
    #     '1d': 86400,        # Daily
    #     '1w': 604800,       # Weekly
    #     # '1M': 2592000,    # Monthly (approx. 30 days) - commented out due to variable length
    # }
    
    SUPPORTED_TIMEFRAMES: Dict[str, int] = {
        # Tick data is not aggregated, so duration is 0, but listed for completeness.
        # Process_tick will skip aggregation for timeframes with 0 second duration.
        # 'tick': 0, 
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '4h': 14400, '1d': 86400,
    }

    # Standard columns for the OHLCV bar DataFrames.
    BAR_COLUMNS: List[str] = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest']

    def __init__(self, max_bars_in_memory: int = 10000):
        """
        Initializes the TimeframeManager.

        Args:
            max_bars_in_memory: Maximum number of completed bars to keep in memory
                                per symbol per timeframe (in a deque) before on-demand
                                conversion to a DataFrame. This helps balance memory usage
                                and the frequency of DataFrame construction.
        """
        self.logger = get_logger("utils.timeframe_manager")
        self.max_bars_in_memory = max_bars_in_memory

        # Stores completed OHLCVOI bars as a list of dicts before DataFrame conversion.
        # Structure: {symbol_key: {timeframe_str: deque[Dict[str, Any]]}}
        self.completed_bars_buffer: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=self.max_bars_in_memory)))

        # Caches the DataFrame representation of bars for quick retrieval.
        # Structure: {symbol_key: {timeframe_str: pd.DataFrame}}
        self.bars_df_cache: Dict[str, Dict[str, pd.DataFrame]] = defaultdict(dict)

        # Stores the current, actively forming (incomplete) bar for each symbol and timeframe.
        # Structure: {symbol_key: {timeframe_str: Dict[str, Any]}}
        self.current_bars: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)

        # Reentrant lock for ensuring thread safety during concurrent access to shared data structures.
        self.lock = threading.RLock()

        # Tracks the last processed tick timestamp for each symbol to detect gaps or out-of-order data.
        self.last_tick_time: Dict[str, float] = {}

        self.logger.info(f"TimeframeManager initialized. Max bars in memory buffer: {self.max_bars_in_memory}. Supported timeframes: {list(self.SUPPORTED_TIMEFRAMES.keys())}")

    def ensure_timeframe_tracked(self, symbol: str, timeframe: str) -> None:
        """
        Ensures that the TimeframeManager is set up to track a specific symbol and timeframe.
        Initializes internal data structures if they don't already exist for the given combination.
        This method is typically called by DataManager when a strategy subscribes to a new timeframe.

        Args:
            symbol: The symbol of the instrument (e.g., 'NSE:RELIANCE').
            timeframe: The timeframe identifier (e.g., '1m', '5m').
        """
        with self.lock:
            if timeframe not in self.SUPPORTED_TIMEFRAMES:
                self.logger.warning(f"Attempted to ensure tracking for an unsupported timeframe: '{timeframe}' for symbol '{symbol}'. Supported: {list(self.SUPPORTED_TIMEFRAMES.keys())}")
                return
            
            # Accessing defaultdict entries ensures they are initialized if not present.
            _ = self.completed_bars_buffer[symbol][timeframe]
            _ = self.bars_df_cache[symbol] # Ensures the symbol key exists in the cache dict.
            
            if timeframe not in self.current_bars.get(symbol, {}):
                self.logger.info(f"Timeframe '{timeframe}' is now actively prepared for tracking for symbol '{symbol}'. First bar will be created upon data arrival.")
            else:
                self.logger.debug(f"Timeframe '{timeframe}' is already being tracked for symbol '{symbol}'.")

    def process_tick(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, bool]:
        """
        Processes a single market data tick and updates bars for all relevant, tracked timeframes.
        This method is designed for low latency and avoids costly DataFrame operations.

        Args:
            symbol: Symbol of the instrument (e.g., 'NSE:RELIANCE') or a unique token ID.
            market_data: A dictionary containing market data for the tick.
                         Expected keys (from utils.constants.MarketDataType):
                         - LAST_PRICE (float): Last traded price.
                         - TIMESTAMP (float): Unix timestamp in seconds (with milliseconds if available).
                         - VOLUME (float, optional): Volume of the trade.
                         - OPEN_INTEREST (float, optional): Open interest data.
                         - BID (float, optional): Current best bid price.
                         - ASK (float, optional): Current best ask price.

        Returns:
            A dictionary mapping timeframe strings (e.g., '1m', '5m') to a boolean value.
            True indicates that a bar for that timeframe was completed with this tick, False otherwise.
        """
        price = market_data.get(MarketDataType.LAST_PRICE.value)
        timestamp_sec = market_data.get(MarketDataType.TIMESTAMP.value)
        volume = market_data.get(MarketDataType.VOLUME.value, 0.0)
        open_interest = market_data.get(MarketDataType.OPEN_INTEREST.value)

        if price is None:
            bid = market_data.get(MarketDataType.BID.value)
            ask = market_data.get(MarketDataType.ASK.value)
            if bid is not None and ask is not None and bid > 0 and ask > 0: # Ensure bid/ask are valid
                price = (bid + ask) / 2.0
                self.logger.debug(f"Using mid-price {price:.2f} for {symbol} as last_price is unavailable.")
            else:
                self.logger.warning(f"No valid price (LTP or Bid/Ask) for {symbol}. Tick: {market_data}. Skipping processing.")
                return {}

        if timestamp_sec is None:
            self.logger.warning(f"No timestamp for {symbol}. Tick: {market_data}. Using current system time.")
            timestamp_sec = time.time()

        try:
            price = float(price)
            timestamp_sec = float(timestamp_sec)
            volume = float(volume) if volume is not None else 0.0
            open_interest = float(open_interest) if open_interest is not None else None # OI can be None
        except (ValueError, TypeError) as e:
            self.logger.error(f"Type conversion error for market data of {symbol}. Data: {market_data}. Error: {e}. Skipping tick.")
            return {}

        if symbol in self.last_tick_time:
            time_diff = timestamp_sec - self.last_tick_time[symbol]
            if time_diff < -0.5: # Allow for small negative diff due to precision or minor reordering
                # self.logger.warning(f"Out-of-order tick detected for {symbol}: current_ts={timestamp_sec}, last_ts={self.last_tick_time[symbol]}. Diff: {time_diff:.3f}s. Skipping tick.")
                return {}
            if time_diff > 300: # Log significant gaps (e.g., > 5 minutes)
                self.logger.info(f"Significant time gap detected for {symbol}: {time_diff:.2f} seconds since last tick.")
        self.last_tick_time[symbol] = timestamp_sec

        completed_bars_status: Dict[str, bool] = {}

        with self.lock:
            # Iterate over only the timeframes for which the symbol is currently being tracked for bar formation.
            # This requires knowing which (symbol, timeframe) pairs are active, typically managed by DataManager
            # and communicated via ensure_timeframe_tracked. For now, we iterate all SUPPORTED_TIMEFRAMES
            # and rely on current_bars[symbol] being populated only for active ones, or ensure_timeframe_tracked
            # having prepped them.
            # A more optimized approach might be to iterate self.current_bars[symbol].keys() if that's guaranteed
            # to contain all *subscribed* timeframes for the symbol.
            # However, ensure_timeframe_tracked might be called before any tick arrives, so current_bars[symbol][tf] might not exist yet.
            # Thus, iterating SUPPORTED_TIMEFRAMES and checking if current_bars[symbol][tf] exists or needs init is safer.

            for timeframe, tf_seconds in self.SUPPORTED_TIMEFRAMES.items():
                if tf_seconds == 0: # Skip pseudo-timeframes like 'tick' if it were included
                    continue

                # Check if this specific timeframe is actually being tracked for this symbol
                # This check is important if not all SUPPORTED_TIMEFRAMES are active for every symbol.
                # If ensure_timeframe_tracked is always called prior, this check might be redundant
                # if self.current_bars[symbol] or self.completed_bars_buffer[symbol] accurately reflect active tracking.
                # For robustness, let's assume we only process if the symbol/timeframe is somewhat initialized.
                # This is implicitly handled by checking `timeframe not in self.current_bars[symbol]` later.

                bar_start_timestamp = self._get_bar_start_timestamp(timestamp_sec, tf_seconds)

                current_bar_for_tf = self.current_bars[symbol].get(timeframe)

                if current_bar_for_tf is None:
                    # This is the first tick for this symbol/timeframe combination, or after a reset.
                    # Or, this timeframe was just added via ensure_timeframe_tracked.
                    self.current_bars[symbol][timeframe] = self._create_new_bar(bar_start_timestamp, price, volume, open_interest)
                    completed_bars_status[timeframe] = False
                    self.logger.debug(f"Initialized first '{timeframe}' bar for {symbol} at {datetime.fromtimestamp(bar_start_timestamp)} ({bar_start_timestamp})")
                else:
                    if bar_start_timestamp > current_bar_for_tf['timestamp']:
                        # Current bar is completed.
                        self.completed_bars_buffer[symbol][timeframe].append(current_bar_for_tf)
                        completed_bars_status[timeframe] = True
                        self.logger.debug(f"Completed '{timeframe}' bar for {symbol}: {current_bar_for_tf}")

                        # Start a new bar.
                        self.current_bars[symbol][timeframe] = self._create_new_bar(bar_start_timestamp, price, volume, open_interest)
                        self.logger.debug(f"Started new '{timeframe}' bar for {symbol} at {datetime.fromtimestamp(bar_start_timestamp)}")

                        # Invalidate the DataFrame cache for this timeframe as new data is available.
                        if timeframe in self.bars_df_cache.get(symbol, {}):
                            del self.bars_df_cache[symbol][timeframe]
                            # self.logger.debug(f"Invalidated DataFrame cache for {symbol} [{timeframe}].")

                    elif bar_start_timestamp == current_bar_for_tf['timestamp']:
                        # Tick belongs to the current bar; update it.
                        current_bar_for_tf['high'] = max(current_bar_for_tf['high'], price)
                        current_bar_for_tf['low'] = min(current_bar_for_tf['low'], price)
                        current_bar_for_tf['close'] = price
                        current_bar_for_tf['volume'] += volume
                        if open_interest is not None: # Update OI only if the tick provides it.
                            current_bar_for_tf['open_interest'] = open_interest
                        completed_bars_status[timeframe] = False
                    else:
                        # Tick timestamp is older than the current bar's start time for this timeframe.
                        # This implies an out-of-order tick that wasn't caught by the global last_tick_time check,
                        # or a logic flaw. Log this occurrence.
                        self.logger.warning(f"Tick timestamp {timestamp_sec} (bar start {bar_start_timestamp}) is older than current '{timeframe}' bar's start {current_bar_for_tf['timestamp']} for {symbol}. Skipping update for this timeframe.")
                        completed_bars_status[timeframe] = False
        return completed_bars_status

    def _create_new_bar(self, timestamp: float, price: float, volume: float, open_interest: Optional[float]) -> Dict[str, Any]:
        """Helper to create a new bar dictionary."""
        return {
            'timestamp': timestamp, # Bar start timestamp
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'open_interest': open_interest
        }

    def _get_bar_start_timestamp(self, tick_timestamp: float, timeframe_seconds: int) -> float:
        """
        Calculates the standardized start timestamp for a bar interval.
        Aligns the timestamp to the beginning of the interval (e.g., for 5m, 09:17:32 becomes 09:15:00).

        Args:
            tick_timestamp: Timestamp of the tick (in seconds, unix epoch).
            timeframe_seconds: Duration of the timeframe in seconds.

        Returns:
            The calculated start timestamp for the bar (unix epoch seconds).
        """
        if timeframe_seconds <= 0:
            self.logger.error(f"Invalid timeframe_seconds ({timeframe_seconds}) for bar timestamp calculation. Returning tick_timestamp.")
            return tick_timestamp 
        return float((int(tick_timestamp) // timeframe_seconds) * timeframe_seconds)

    def _build_dataframe_from_buffer(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Converts the deque buffer of completed bars into a Pandas DataFrame.
        This is called internally when DataFrame access is needed and the cache is stale.

        Args:
            symbol: Symbol of the instrument.
            timeframe: Timeframe identifier.

        Returns:
            A Pandas DataFrame containing the bars, sorted by timestamp, or None if no data.
        """
        with self.lock:
            buffer = self.completed_bars_buffer[symbol][timeframe]
            if not buffer:
                self.logger.debug(f"No completed bars in buffer for {symbol} [{timeframe}] to build DataFrame.")
                return None
            try:
                df = pd.DataFrame(list(buffer), columns=self.BAR_COLUMNS)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') # Convert to datetime objects
                df.set_index('timestamp', inplace=True, drop=False) # Keep timestamp as a column too
                # df.sort_index(inplace=True) # Deque stores in order, but explicit sort is safer if assumptions change.
                                            # However, list(buffer) preserves order.
                self.logger.debug(f"Built DataFrame for {symbol} [{timeframe}] with {len(df)} bars from buffer.")
                return df
            except Exception as e:
                self.logger.error(f"Error converting buffer to DataFrame for {symbol} [{timeframe}]: {e}", exc_info=True)
                return None

    def get_bars(self, symbol: str, timeframe: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Retrieves historical bars for a specific symbol and timeframe as a DataFrame.
        Uses a cache; if cache is miss or stale (implicitly handled by deletion in process_tick),
        it rebuilds the DataFrame from the internal buffer.

        Args:
            symbol: Symbol of the instrument (e.g., 'NSE:RELIANCE').
            timeframe: Timeframe identifier (e.g., '1m', '5m').
            limit: Optional. If provided, returns only the 'limit' most recent bars.

        Returns:
            A Pandas DataFrame of OHLCVOI bars, with 'timestamp' as a datetime index and also a column.
            Returns None if no data is available for the symbol/timeframe.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Requested bars for unsupported timeframe: '{timeframe}' for symbol '{symbol}'.")
            return None

        with self.lock:
            cached_df = self.bars_df_cache.get(symbol, {}).get(timeframe)
            if cached_df is not None:
                self.logger.debug(f"Cache hit for {symbol} [{timeframe}].")
                df_to_return = cached_df
            else:
                self.logger.debug(f"Cache miss for {symbol} [{timeframe}]. Building from buffer.")
                df_to_return = self._build_dataframe_from_buffer(symbol, timeframe)
                if df_to_return is not None:
                    self.bars_df_cache[symbol][timeframe] = df_to_return # Update cache
                else:
                    # No data in buffer, return empty DataFrame with correct columns
                    self.logger.debug(f"No bars available for {symbol} [{timeframe}] after attempting build.")
                    return pd.DataFrame(columns=self.BAR_COLUMNS).set_index(pd.to_datetime([]))

            if df_to_return is None or df_to_return.empty:
                 return pd.DataFrame(columns=self.BAR_COLUMNS).set_index(pd.to_datetime([]))

            # Apply limit if requested
            if limit is not None and limit > 0:
                return df_to_return.iloc[-limit:].copy()
            elif limit == 0:
                return pd.DataFrame(columns=df_to_return.columns, index=pd.to_datetime([]))
            else: # limit is None or < 0 (interpreted as no limit)
                return df_to_return.copy()

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Gets the currently forming (incomplete) bar for a symbol and timeframe.

        Args:
            symbol: Symbol of the instrument.
            timeframe: Timeframe identifier.

        Returns:
            A copy of the current incomplete bar data (dictionary), or None if no such bar exists.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            self.logger.warning(f"Requested current bar for unsupported timeframe: '{timeframe}' for symbol '{symbol}'.")
            return None
        with self.lock:
            current_bar_data = self.current_bars.get(symbol, {}).get(timeframe)
            return current_bar_data.copy() if current_bar_data else None

    def reset(self, symbol: Optional[str] = None) -> None:
        """
        Resets stored bar data, optionally for a specific symbol or for all symbols.

        Args:
            symbol: Optional. If provided, resets data only for this symbol.
                    If None (default), resets data for all tracked symbols.
        """
        with self.lock:
            if symbol:
                if symbol in self.completed_bars_buffer: del self.completed_bars_buffer[symbol]
                if symbol in self.bars_df_cache: del self.bars_df_cache[symbol]
                if symbol in self.current_bars: del self.current_bars[symbol]
                if symbol in self.last_tick_time: del self.last_tick_time[symbol]
                log_msg = f"Reset TimeframeManager data for symbol: {symbol}"
            else:
                self.completed_bars_buffer.clear()
                self.bars_df_cache.clear()
                self.current_bars.clear()
                self.last_tick_time.clear()
                log_msg = "Reset all TimeframeManager data for all symbols."
        self.logger.info(log_msg)

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """
        Returns a sorted list of timeframes for which data (either current or completed)
        is being actively tracked or buffered for a given symbol.
        """
        with self.lock:
            available = set()
            if symbol in self.completed_bars_buffer:
                available.update(self.completed_bars_buffer[symbol].keys())
            if symbol in self.current_bars:
                available.update(self.current_bars[symbol].keys())
            return sorted(list(tf for tf in available if tf in self.SUPPORTED_TIMEFRAMES))

    def get_tracked_symbols(self) -> List[str]:
        """
        Returns a sorted list of all symbols for which any timeframe data is being tracked
        (i.e., present in current_bars or completed_bars_buffer).
        """
        with self.lock:
            tracked = set(self.completed_bars_buffer.keys()) | set(self.current_bars.keys())
            return sorted(list(tracked))

    def get_tracked_symbols_and_timeframes(self) -> Dict[str, List[str]]:
        """
        Returns a dictionary mapping symbol keys to lists of timeframes being tracked for each symbol.
        This is used by the DataManager for persistence operations.
        
        Returns:
            Dict[str, List[str]]: A dictionary with symbol keys as keys and lists of timeframes as values.
                                 Example: {'NSE:RELIANCE': ['1m', '5m'], 'NSE:INFY': ['1m', '15m']}
        """
        result = {}
        with self.lock:
            # Get all tracked symbols
            symbols = set(self.completed_bars_buffer.keys()) | set(self.current_bars.keys())
            
            # For each symbol, get its tracked timeframes
            for symbol in symbols:
                timeframes = self.get_available_timeframes(symbol)
                if timeframes:  # Only add symbols with at least one timeframe
                    result[symbol] = timeframes
                    
            self.logger.debug(f"Retrieved {len(result)} symbols with active timeframe tracking")
        return result
    
    def stop_tracking_timeframe(self, symbol: str, timeframe: str) -> None:
        """
        Stops tracking a specific timeframe for a symbol and clears all its associated data.
        This is typically called when DataManager indicates no more subscribers for this symbol/timeframe.

        Args:
            symbol: The symbol of the instrument.
            timeframe: The timeframe identifier.
        """
        with self.lock:
            removed_data = False
            if symbol in self.completed_bars_buffer and timeframe in self.completed_bars_buffer[symbol]:
                del self.completed_bars_buffer[symbol][timeframe]
                removed_data = True
            if symbol in self.current_bars and timeframe in self.current_bars[symbol]:
                del self.current_bars[symbol][timeframe]
                removed_data = True
            if symbol in self.bars_df_cache and timeframe in self.bars_df_cache.get(symbol, {}):
                del self.bars_df_cache[symbol][timeframe]
                removed_data = True

            if removed_data:
                self.logger.info(f"Stopped tracking and cleared data for {symbol} [{timeframe}].")
            else:
                self.logger.info(f"No data found to clear for {symbol} [{timeframe}] during stop_tracking_timeframe call.")

            # Clean up symbol-level entries if they become empty
            if symbol in self.completed_bars_buffer and not self.completed_bars_buffer[symbol]:
                del self.completed_bars_buffer[symbol]
            if symbol in self.current_bars and not self.current_bars[symbol]:
                del self.current_bars[symbol]
            if symbol in self.bars_df_cache and not self.bars_df_cache[symbol]:
                del self.bars_df_cache[symbol]
            
            # If the symbol is no longer tracked for any timeframe, clear its last_tick_time entry.
            if not self.get_available_timeframes(symbol): # Check if any timeframes are left for this symbol
                 if symbol in self.last_tick_time:
                    del self.last_tick_time[symbol]
                    self.logger.info(f"Cleared last_tick_time for completely untracked symbol {symbol}.")

