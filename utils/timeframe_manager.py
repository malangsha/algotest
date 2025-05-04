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
    Manages the conversion of tick data to various timeframes (1m, 5m, 15m, 1h, 1d, etc.)
    and provides mechanisms to store and retrieve this data. Optimized for low latency
    by minimizing DataFrame operations during tick processing.

    Standard Timeframes:
    'tick': Raw tick data (not aggregated)
    '1s', '5s', '10s', '30s': Seconds
    '1m', '3m', '5m', '15m', '30m': Minutes
    '1h', '2h', '4h', '6h', '8h', '12h': Hours
    '1d': Daily (aligned to UTC day start by default, can be adjusted)
    '1w': Weekly
    '1M': Monthly (approx 30 days)
    """

    # Standard timeframes supported and their duration in seconds
    VALID_TIMEFRAMES_1 = {
        'tick': 0,
        '1s': 1, '5s': 5, '10s': 10, '30s': 30,
        '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
        '1d': 86400,
        '1w': 604800,
        '1M': 2592000, # Approximation
    }

     # Standard timeframes supported and their duration in seconds
    VALID_TIMEFRAMES = {                
        '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
        '1h': 3600, '4h': 14400, # Approximation
    }

    # Columns for the DataFrame storage
    BAR_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest']

    def __init__(self, max_bars_in_memory: int = 10000):
        """
        Initialize the TimeframeManager.

        Args:
            max_bars_in_memory: Maximum number of completed bars to keep in memory
                                per symbol per timeframe before converting to DataFrame.
                                Helps balance memory usage and DataFrame conversion frequency.
        """
        self.logger = get_logger("utils.timeframe_manager")
        self.max_bars_in_memory = max_bars_in_memory # Controls max size of the temporary list

        # Data structure to store completed OHLCVOI bars before DataFrame conversion
        # {symbol: {timeframe: deque[Dict]}} - Using deque for efficient appends
        self.completed_bars_buffer = defaultdict(lambda: defaultdict(lambda: deque(maxlen=self.max_bars_in_memory)))

        # Data structure to store the final DataFrame representation
        # {symbol: {timeframe: pd.DataFrame}}
        self.bars_df_cache = defaultdict(dict)

        # Current (incomplete) open bars for each symbol and timeframe
        # {symbol: {timeframe: Dict}}
        self.current_bars = defaultdict(dict)

        # Lock for thread safety during updates
        self.lock = threading.RLock()

        # Last timestamp processed for each symbol (used for gap detection)
        self.last_tick_time = {}

        self.logger.info(f"TimeframeManager initialized with max_bars_in_memory={max_bars_in_memory}")

    def process_tick(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, bool]:
        """
        Process a single tick and update bars for all relevant timeframes.
        Designed for low latency by avoiding DataFrame operations here.

        Args:
            symbol: Symbol of the instrument (e.g., 'NSE:RELIANCE') or token ID
            market_data: Market data dictionary converted from Finvasia format.
                Expected to contain:
                - MarketDataType.LAST_PRICE.value (float): Last traded price
                - MarketDataType.TIMESTAMP.value (float): Unix timestamp in seconds
                - MarketDataType.VOLUME.value (float, optional): Trade volume
                - MarketDataType.OPEN_INTEREST.value (float, optional): Open interest

        Returns:
            Dictionary mapping timeframes (e.g., '1m', '5m') to a boolean
            indicating if a bar for that timeframe was completed with this tick.
        """
        # --- Input Validation and Data Extraction ---
        price = market_data.get(MarketDataType.LAST_PRICE.value)
        timestamp = market_data.get(MarketDataType.TIMESTAMP.value)  # Expecting seconds
        volume = market_data.get(MarketDataType.VOLUME.value, 0)  # Default to 0 if not provided
        open_interest = market_data.get(MarketDataType.OPEN_INTEREST.value)  # Optional

        # If price is not available but bid/ask are, use mid price
        if price is None:
            bid = market_data.get(MarketDataType.BID.value)
            ask = market_data.get(MarketDataType.ASK.value)
            if bid is not None and ask is not None:
                price = (bid + ask) / 2
                self.logger.debug(f"Using mid price {price} for {symbol} as last_price is not available")

        if price is None:
            self.logger.warning(f"No price data available for {symbol}: {market_data}. Missing last_price and bid/ask.")
            return {}

        if timestamp is None:
            self.logger.warning(f"No timestamp for {symbol}: {market_data}. Using current time.")
            timestamp = time.time()

        try:
            price = float(price)
            timestamp = float(timestamp)
            volume = float(volume) if volume is not None else 0
            open_interest = float(open_interest) if open_interest is not None else None
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error converting market data types for {symbol}: {market_data}. Error: {e}")
            return {}

        # --- Timestamp Gap Detection ---
        if symbol in self.last_tick_time:
            time_diff = timestamp - self.last_tick_time[symbol]
            if time_diff > 60:  # Log gaps larger than 60 seconds
                self.logger.warning(f"Time gap detected for {symbol}: {time_diff:.2f} seconds.")
            elif time_diff < 0:
                self.logger.warning(f"Out-of-order tick detected for {symbol}: current={timestamp}, last={self.last_tick_time[symbol]}. Skipping.")
                return {}  # Skip out-of-order ticks

        self.last_tick_time[symbol] = timestamp

        # --- Bar Update Logic ---
        completed_bars_status = {}  # Track which timeframes completed a bar

        with self.lock:
            for timeframe, seconds in self.VALID_TIMEFRAMES.items():
                if seconds == 0:  # Skip 'tick' timeframe
                    continue

                bar_start_timestamp = self._get_bar_timestamp(timestamp, seconds)

                # --- Initialize or Update Bar ---
                if timeframe not in self.current_bars[symbol]:
                    # First tick for this symbol/timeframe combination
                    self.current_bars[symbol][timeframe] = self._create_new_bar(
                        bar_start_timestamp, price, volume, open_interest
                    )
                    completed_bars_status[timeframe] = False
                    self.logger.debug(f"Initialized first {timeframe} bar for {symbol} at {bar_start_timestamp}")

                else:
                    current_bar = self.current_bars[symbol][timeframe]

                    if bar_start_timestamp > current_bar['timestamp']:
                        # --- Current Bar Completed ---
                        # Store the completed bar in the buffer
                        self.completed_bars_buffer[symbol][timeframe].append(current_bar)
                        completed_bars_status[timeframe] = True
                        self.logger.debug(f"Completed {timeframe} bar for {symbol}: {current_bar}")

                        # --- Start New Bar ---
                        self.current_bars[symbol][timeframe] = self._create_new_bar(
                            bar_start_timestamp, price, volume, open_interest
                        )
                        self.logger.debug(f"Started new {timeframe} bar for {symbol} at {bar_start_timestamp}")

                        # Invalidate the DataFrame cache for this timeframe as new data is available
                        if timeframe in self.bars_df_cache.get(symbol, {}):
                            del self.bars_df_cache[symbol][timeframe]

                    elif bar_start_timestamp == current_bar['timestamp']:
                        # --- Update Existing Bar ---
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume  # Accumulate volume
                        # Update OI only if the tick provides it, otherwise keep the last known value
                        if open_interest is not None:
                            current_bar['open_interest'] = open_interest
                        completed_bars_status[timeframe] = False
                    else:
                        # This case (bar_start_timestamp < current_bar['timestamp'])
                        # should theoretically not happen if time_diff check works,
                        # but log if it does.
                        self.logger.error(f"Timestamp calculation error for {symbol} {timeframe}. "
                                          f"Tick time: {timestamp}, Bar start: {bar_start_timestamp}, "
                                          f"Current bar time: {current_bar['timestamp']}")
                        completed_bars_status[timeframe] = False

        return completed_bars_status

    def _create_new_bar(self, timestamp: float, price: float, volume: float, open_interest: Optional[float]) -> Dict[str, Any]:
        """Helper to create a new bar dictionary."""
        return {
            'timestamp': timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'open_interest': open_interest  # Store initial OI (can be None)
        }

    def _get_bar_timestamp(self, tick_timestamp: float, timeframe_seconds: int) -> float:
        """
        Calculate the start timestamp for a bar based on the timeframe.
        Aligns the timestamp to the beginning of the interval.
        E.g., for 5m timeframe, 09:17:32 becomes 09:15:00.

        Args:
            tick_timestamp: Timestamp of the tick in seconds (unix time).
            timeframe_seconds: Number of seconds in the timeframe.

        Returns:
            Start timestamp (unix time in seconds) for the bar.
        """
        if timeframe_seconds <= 0:
            return tick_timestamp  # Should not happen for valid timeframes

        # Integer division floors the result, effectively rounding down
        return float((int(tick_timestamp) // timeframe_seconds) * timeframe_seconds)

    def _build_dataframe_from_buffer(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Converts the deque buffer of completed bars into a Pandas DataFrame.
        This is called internally when DataFrame access is needed.

        Args:
            symbol: Symbol of the instrument.
            timeframe: Timeframe identifier.

        Returns:
            A Pandas DataFrame containing the bars, or None if no data.
        """
        with self.lock:
            buffer = self.completed_bars_buffer[symbol][timeframe]
            if not buffer:
                return None

            # Convert deque of dictionaries to DataFrame
            try:
                df = pd.DataFrame(list(buffer), columns=self.BAR_COLUMNS)
                # Ensure timestamp is float for consistency, then set as index
                df['timestamp'] = df['timestamp'].astype(float)
                df.set_index('timestamp', inplace=True, drop=False)  # Keep timestamp column
                df.sort_index(inplace=True)  # Ensure chronological order
                return df
            except Exception as e:
                self.logger.error(f"Error converting buffer to DataFrame for {symbol} {timeframe}: {e}")
                return None

    def get_bars(self, symbol: str, timeframe: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Get historical bars for a specific symbol and timeframe as a DataFrame.
        Builds the DataFrame from the buffer if it's not cached or invalidated.

        Args:
            symbol: Symbol of the instrument (e.g., 'NSE:RELIANCE').
            timeframe: Timeframe identifier (e.g., '1m', '5m').
            limit: Optional limit on the number of most recent bars to return.

        Returns:
            Pandas DataFrame of OHLCVOI bars, or None if no data is available.
            The DataFrame index is the bar's start timestamp (unix seconds).
        """
        with self.lock:
            # Check cache first
            if timeframe in self.bars_df_cache.get(symbol, {}):
                df = self.bars_df_cache[symbol][timeframe]
            else:
                # Build DataFrame from buffer if not cached
                df = self._build_dataframe_from_buffer(symbol, timeframe)
                if df is not None:
                    # Store in cache
                    if symbol not in self.bars_df_cache:
                        self.bars_df_cache[symbol] = {}
                    self.bars_df_cache[symbol][timeframe] = df
                else:
                    # No data in buffer
                    return None  # Return None if DataFrame couldn't be built

            # Apply limit if requested
            if limit is not None and limit > 0:
                # Ensure limit doesn't exceed available rows
                actual_limit = min(limit, len(df))
                if actual_limit > 0:
                    return df.iloc[-actual_limit:].copy()  # Return tail
                else:
                    return pd.DataFrame(columns=df.columns)  # Return empty DF with same columns
            elif limit == 0:
                return pd.DataFrame(columns=df.columns)  # Return empty DF if limit is 0
            else:
                return df.copy()  # Return full DataFrame copy

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the currently forming (incomplete) bar for a symbol and timeframe.

        Args:
            symbol: Symbol of the instrument.
            timeframe: Timeframe identifier.

        Returns:
            Dictionary with current incomplete bar data (OHLCVOI), or None if none exists.
        """
        with self.lock:
            # Return a copy to prevent external modification
            return self.current_bars.get(symbol, {}).get(timeframe, {}).copy() or None

    def reset(self, symbol: Optional[str] = None):
        """
        Reset stored bar data, optionally for a specific symbol only.

        Args:
            symbol: Optional symbol to reset. If None, resets all data for all symbols.
        """
        with self.lock:
            if symbol:
                # Clear data for the specific symbol
                if symbol in self.completed_bars_buffer:
                    del self.completed_bars_buffer[symbol]
                if symbol in self.bars_df_cache:
                    del self.bars_df_cache[symbol]
                if symbol in self.current_bars:
                    del self.current_bars[symbol]
                if symbol in self.last_tick_time:
                    del self.last_tick_time[symbol]
                log_msg = f"Reset timeframe data for symbol: {symbol}"
            else:
                # Clear all data
                self.completed_bars_buffer.clear()
                self.bars_df_cache.clear()
                self.current_bars.clear()
                self.last_tick_time.clear()
                log_msg = "Reset all timeframe data"

        self.logger.info(log_msg)

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """
        Get a list of timeframes for which any bar data (completed or current) exists.

        Args:
            symbol: Symbol to check.

        Returns:
            List of available timeframe identifiers for this symbol.
        """
        with self.lock:
            # Combine keys from buffer and current bars
            buffered_tfs = set(self.completed_bars_buffer.get(symbol, {}).keys())
            current_tfs = set(self.current_bars.get(symbol, {}).keys())
            available_tfs = list(buffered_tfs.union(current_tfs))
            return sorted(available_tfs, key=lambda tf: self.VALID_TIMEFRAMES.get(tf, float('inf')))


# Example Usage (for testing purposes)
if __name__ == "__main__":
    # Setup basic logging for the example
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    tf_manager = TimeframeManager(max_bars_in_memory=5)  # Keep small buffer for demo
    symbol = "NSE:26000"  # Using format "exchange:token"

    # Simulate Finvasia-like market data events
    start_time = time.time()
    market_data_events = [
        # Bar 1 (starts at t+0)
        {
            MarketDataType.TIMESTAMP.value: start_time + 5,
            MarketDataType.LAST_PRICE.value: 24269.90,
            MarketDataType.VOLUME.value: 100,
            "exchange": "NSE",
            "token": "26000"
        },
        {
            MarketDataType.TIMESTAMP.value: start_time + 15,
            MarketDataType.LAST_PRICE.value: 24269.80,
            MarketDataType.VOLUME.value: 150,
            "exchange": "NSE",
            "token": "26000"
        },
        {
            MarketDataType.TIMESTAMP.value: start_time + 30,
            MarketDataType.LAST_PRICE.value: 24268.80,
            MarketDataType.VOLUME.value: 200,
            "exchange": "NSE",
            "token": "26000"
        },
        # Bar 2 (starts at t+60)
        {
            MarketDataType.TIMESTAMP.value: start_time + 65,
            MarketDataType.LAST_PRICE.value: 24269.90,
            MarketDataType.VOLUME.value: 120,
            "exchange": "NSE",
            "token": "26000"
        },
        {
            MarketDataType.TIMESTAMP.value: start_time + 70,
            MarketDataType.LAST_PRICE.value: 24269.45,
            MarketDataType.VOLUME.value: 80,
            "exchange": "NSE",
            "token": "26000"
        },
    ]

    logger.info("Simulating market data events...")
    for market_data in market_data_events:
        completed = tf_manager.process_tick(symbol, market_data)
        logger.debug(f"Processed tick at {market_data[MarketDataType.TIMESTAMP.value]:.0f}. "
                    f"Price: {market_data[MarketDataType.LAST_PRICE.value]}. "
                    f"Completed bars: {completed}")
        time.sleep(0.01)  # Small delay

    logger.info("\n--- Results ---")

    # Get current (incomplete) 1m bar
    current_1m = tf_manager.get_current_bar(symbol, '1m')
    logger.info(f"Current 1m Bar: {current_1m}")

    # Get completed 1m bars as DataFrame
    bars_1m_df = tf_manager.get_bars(symbol, '1m')
    logger.info(f"\nCompleted 1m Bars DataFrame:\n{bars_1m_df}")

    # Check available timeframes
    available = tf_manager.get_available_timeframes(symbol)
    logger.info(f"\nAvailable timeframes for {symbol}: {available}")

