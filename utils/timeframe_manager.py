import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import threading
import time
from collections import defaultdict

class TimeframeManager:
    """
    Manages the conversion of tick data to various timeframes (1m, 5m, 15m, 1h, 1d, etc.)
    and provides mechanisms to store and retrieve this data.
    """

    # Standard timeframes supported
    VALID_TIMEFRAMES = {
        'tick': 0,  # Raw tick data
        '1s': 1,    # 1 second
        '5s': 5,    # 5 seconds
        '10s': 10,  # 10 seconds
        '30s': 30,  # 30 seconds
        '1m': 60,   # 1 minute
        '3m': 180,  # 3 minutes
        '5m': 300,  # 5 minutes
        '15m': 900, # 15 minutes
        '30m': 1800, # 30 minutes
        '1h': 3600, # 1 hour
        '2h': 7200, # 2 hours
        '4h': 14400, # 4 hours
        '6h': 21600, # 6 hours
        '8h': 28800, # 8 hours
        '12h': 43200, # 12 hours
        '1d': 86400, # 1 day (24 hours)
        '1w': 604800, # 1 week
        '1M': 2592000, # 1 month (approx 30 days)
    }

    def __init__(self, max_bars: int = 1000):
        """
        Initialize the TimeframeManager.

        Args:
            max_bars: Maximum number of bars to keep per symbol per timeframe
        """
        self.logger = logging.getLogger("utils.timeframe_manager")
        self.max_bars = max_bars

        # Data structure to store OHLCV bars for each symbol and timeframe
        # {symbol: {timeframe: DataFrame}}
        self.bars = defaultdict(dict)

        # Current open bars for each symbol and timeframe
        # {symbol: {timeframe: Dict}}
        self.current_bars = defaultdict(dict)

        # Lock for thread safety
        self.lock = threading.RLock()

        # Last timestamp processed for each symbol (used for gap detection)
        self.last_tick_time = {}

        self.logger.info(f"TimeframeManager initialized with max_bars={max_bars}")

    def process_tick(self, symbol: str, tick: Dict[str, Any]) -> Dict[str, bool]:
        """
        Process a tick and update bars for all timeframes.

        Args:
            symbol: Symbol of the instrument
            tick: Tick data dictionary with price, volume, timestamp

        Returns:
            Dictionary mapping timeframes to boolean indicating if bar was completed
        """
        # Extract data from tick
        price = tick.get('price', 0.0)
        volume = tick.get('volume', 0.0)
        timestamp = tick.get('timestamp', 0.0) / 1000  # Convert to seconds if in milliseconds

        self.logger.debug(f"Processing tick for {symbol}: price={price:.2f}, volume={volume}, timestamp={timestamp}")

        # Check for timestamp gaps
        if symbol in self.last_tick_time:
            gap = timestamp - self.last_tick_time[symbol]
            if gap > 10:  # Gap of more than 10 seconds
                self.logger.warning(f"Detected time gap of {gap:.2f} seconds for {symbol}")

        # Update last tick time
        self.last_tick_time[symbol] = timestamp

        with self.lock:
            # Dictionary to track which timeframes completed a bar
            completed_bars = {}

            # Update bars for each timeframe
            for timeframe, seconds in self.VALID_TIMEFRAMES.items():
                if timeframe == 'tick':
                    continue  # Skip tick data as it's not aggregated

                # Check if we need to create a new bar or update existing one
                bar_timestamp = self._get_bar_timestamp(timestamp, seconds)

                # Initialize if this symbol+timeframe doesn't exist yet
                if timeframe not in self.current_bars[symbol]:
                    self.logger.debug(f"Creating first bar for {symbol} on {timeframe} timeframe at timestamp {bar_timestamp}")
                    self.current_bars[symbol][timeframe] = {
                        'timestamp': bar_timestamp,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': volume
                    }
                    completed_bars[timeframe] = False
                else:
                    current_bar = self.current_bars[symbol][timeframe]

                    # Check if this tick belongs to a new bar
                    if bar_timestamp > current_bar['timestamp']:
                        # Store completed bar
                        self.logger.info(f"BAR COMPLETED: {symbol} {timeframe} - O:{current_bar['open']:.2f} H:{current_bar['high']:.2f} L:{current_bar['low']:.2f} C:{current_bar['close']:.2f} V:{current_bar['volume']:.0f}")
                        self._store_completed_bar(symbol, timeframe, current_bar)
                        completed_bars[timeframe] = True

                        # Create new bar
                        self.current_bars[symbol][timeframe] = {
                            'timestamp': bar_timestamp,
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': volume
                        }
                        self.logger.debug(f"Creating new bar for {symbol} on {timeframe} timeframe at timestamp {bar_timestamp}")
                    else:
                        # Update existing bar
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume
                        completed_bars[timeframe] = False

            return completed_bars

    def _get_bar_timestamp(self, tick_timestamp: float, timeframe_seconds: int) -> float:
        """
        Calculate the start timestamp for a bar given a tick timestamp and timeframe in seconds.

        Args:
            tick_timestamp: Timestamp of the tick in seconds (unix time)
            timeframe_seconds: Number of seconds in the timeframe

        Returns:
            Start timestamp for the bar that contains this tick
        """
        if timeframe_seconds == 0:
            return tick_timestamp

        # Round down to the nearest timeframe interval
        return (int(tick_timestamp) // timeframe_seconds) * timeframe_seconds

    def _store_completed_bar(self, symbol: str, timeframe: str, bar: Dict[str, Any]) -> None:
        """
        Store a completed bar in the appropriate timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar: Bar data dictionary
        """
        # Initialize DataFrame if it doesn't exist
        if timeframe not in self.bars[symbol]:
            self.bars[symbol][timeframe] = pd.DataFrame(
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            self.bars[symbol][timeframe].set_index('timestamp', inplace=True)

        # Add the new bar
        new_row = pd.DataFrame([bar])
        new_row.set_index('timestamp', inplace=True)

        # Fix the FutureWarning by correctly concatenating the DataFrames
        if not self.bars[symbol][timeframe].empty:
            # If we already have data, concatenate the new row
            self.bars[symbol][timeframe] = pd.concat(
                [self.bars[symbol][timeframe], new_row],
                ignore_index=False
            )
        else:
            # If DataFrame is empty, just use the new row
            self.bars[symbol][timeframe] = new_row

    def get_bars(self, symbol: str, timeframe: str, limit: int = None) -> Optional[pd.DataFrame]:
        """
        Get historical bars for a specific symbol and timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bars to retrieve
            limit: Optional limit on the number of bars to return (default: all available)

        Returns:
            DataFrame of OHLCV bars or None if not available
        """
        with self.lock:
            if symbol not in self.bars or timeframe not in self.bars[symbol]:
                return None

            df = self.bars[symbol][timeframe]
            if limit:
                return df.iloc[-limit:]
            return df.copy()

    def get_current_bar(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the current (incomplete) bar for a symbol and timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar

        Returns:
            Dictionary with current bar data or None if not available
        """
        with self.lock:
            if symbol not in self.current_bars or timeframe not in self.current_bars[symbol]:
                return None
            return self.current_bars[symbol][timeframe].copy()

    def reset(self, symbol: str = None):
        """
        Reset stored data, optionally for a specific symbol only.

        Args:
            symbol: Optional symbol to reset. If None, resets all data.
        """
        with self.lock:
            if symbol:
                if symbol in self.bars:
                    del self.bars[symbol]
                if symbol in self.current_bars:
                    del self.current_bars[symbol]
                if symbol in self.last_tick_time:
                    del self.last_tick_time[symbol]
            else:
                self.bars.clear()
                self.current_bars.clear()
                self.last_tick_time.clear()

        self.logger.info(f"Reset timeframe data for {'all symbols' if symbol is None else symbol}")

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """
        Get a list of timeframes that have data for a given symbol.

        Args:
            symbol: Symbol to check

        Returns:
            List of available timeframes for this symbol
        """
        with self.lock:
            if symbol not in self.bars:
                return []
            return list(self.bars[symbol].keys())
