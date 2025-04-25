"""
Multi-timeframe strategy base class.

This module provides a base class for strategies that operate on multiple timeframes.
"""

import logging
from typing import Dict, List, Optional, Set, Any
from datetime import datetime

from strategies.base_strategy import OptionStrategy
from strategies.strategy_registry import StrategyRegistry
from models.events import MarketDataEvent, BarEvent
from utils.constants import SignalType, Timeframe

@StrategyRegistry.register('multi_timeframe_strategy')
class MultiTimeframeStrategy(OptionStrategy):
    """
    Base class for strategies that analyze data across multiple timeframes.
    
    This class provides functionality for:
    - Managing data from multiple timeframes
    - Synchronizing analysis across timeframes
    - Generating signals based on multi-timeframe analysis
    """
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.logger = logging.getLogger("strategies.multi_timeframe_strategy")
        
        # Configure timeframes
        self.timeframes: Set[str] = set(config.get('timeframes', ['1m', '5m', '15m']))
        self.primary_timeframe = config.get('primary_timeframe', '5m')
        
        # Validate timeframes
        if self.primary_timeframe not in self.timeframes:
            self.timeframes.add(self.primary_timeframe)
            
        # Data storage for each timeframe
        self.data_by_timeframe: Dict[str, Dict[str, List[Dict]]] = {
            tf: {} for tf in self.timeframes
        }
        
        # Maximum number of bars to store per timeframe
        self.max_bars = config.get('max_bars', 100)
        
    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get required options for this strategy.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping index symbols to lists of option requirements.
            Each option requirement is a dict with keys: strike, option_type, expiry_offset
        """
        return {}  # Base multi-timeframe strategy doesn't use any options
        
    def on_market_data(self, event: MarketDataEvent) -> None:
        """
        Process market data events.
        
        Args:
            event: Market data event containing price information
        """
        symbol = event.get('symbol')
        if not symbol:
            return
            
        # Update data for each timeframe
        for timeframe in self.timeframes:
            if symbol not in self.data_by_timeframe[timeframe]:
                self.data_by_timeframe[timeframe][symbol] = []
                
            # Add new data point
            self.data_by_timeframe[timeframe][symbol].append({
                'timestamp': event['timestamp'],
                'price': event['price']
            })
            
            # Trim old data
            if len(self.data_by_timeframe[timeframe][symbol]) > self.max_bars:
                self.data_by_timeframe[timeframe][symbol] = \
                    self.data_by_timeframe[timeframe][symbol][-self.max_bars:]
                    
    def on_bar(self, event: BarEvent) -> None:
        """
        Process bar events.
        
        Args:
            event: Bar event containing OHLCV data
        """
        symbol = event.get('symbol')
        timeframe = event.get('timeframe')
        
        if not symbol or not timeframe or timeframe not in self.timeframes:
            return
            
        # Process bar data for the specific timeframe
        self._process_timeframe_data(symbol, timeframe, event)
        
    def _process_timeframe_data(self, symbol: str, timeframe: str, data: Dict) -> None:
        """
        Process data for a specific timeframe.
        
        Args:
            symbol: Symbol being processed
            timeframe: Timeframe of the data
            data: Bar data to process
        """
        # Implement in child class
        pass
        
    def get_data(self, symbol: str, timeframe: str) -> Optional[List[Dict]]:
        """
        Get stored data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to get data for
            timeframe: Timeframe to get data for
            
        Returns:
            List of data points if available, None otherwise
        """
        return self.data_by_timeframe.get(timeframe, {}).get(symbol)
        
    def on_stop(self) -> None:
        """Clean up when strategy is stopped."""
        # Clear all stored data
        for timeframe in self.timeframes:
            self.data_by_timeframe[timeframe].clear()
        self.logger.info("Multi-timeframe strategy stopped") 