"""
Test strategy for development and testing purposes.
"""

import logging
from typing import Dict, Any, List

from strategies.base_strategy import OptionStrategy
from strategies.strategy_registry import StrategyRegistry
from models.events import MarketDataEvent, BarEvent
from utils.constants import SignalType

@StrategyRegistry.register('test_strategy')
class TestStrategy(OptionStrategy):
    """Simple test strategy for development purposes."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = logging.getLogger("strategies.test_strategy")
        
    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get required options for this strategy.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping index symbols to lists of option requirements.
            Each option requirement is a dict with keys: strike, option_type, expiry_offset
        """
        return {}  # Test strategy doesn't use any options
        
    def on_market_data(self, event: MarketDataEvent) -> None:
        """Process market data events."""
        self.logger.debug(f"Received market data: {event}")
        
    def on_bar(self, event: BarEvent) -> None:
        """Process bar events."""
        self.logger.debug(f"Received bar: {event}")
        
    def on_stop(self) -> None:
        """Clean up when strategy is stopped."""
        self.logger.info("Test strategy stopped")
