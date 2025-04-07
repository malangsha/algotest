import logging
from datetime import datetime
from typing import List, Dict, Any

from models.events import MarketDataEvent, OrderEvent, Event
from strategies.strategy import Strategy
from utils.constants import SignalType
from models.instrument import Instrument


class TestStrategy(Strategy):
    """
    Simple test strategy that alternates between BUY/SELL signals on each new bar.
    Generates all supported event types for testing purposes.
    """

    def __init__(self, name: str, strategy_id: str, instruments: List[Instrument],
                 parameters: Dict[str, Any] = None, market_data=None, event_manager=None,
                 config: Dict = None, **kwargs):
        """
        Initialize the TestStrategy.

        Args:
            name: Strategy name
            strategy_id: Unique identifier for the strategy
            instruments: List of instruments to trade
            parameters: Strategy parameters
            market_data: Market data manager
            event_manager: Event manager
            config: Full strategy configuration
            **kwargs: Additional parameters
        """
        # Create a config dictionary for the base class
        full_config = {
            'id': strategy_id,
            'name': name,
            'instruments': instruments
        }
        if config:
            full_config.update(config)
            
        # Initialize the parent class
        super().__init__(full_config, market_data, None, event_manager)
        
        self.logger = logging.getLogger(__name__)
        self.signal_counter = 0
        self.last_action = None
        self.parameters = parameters or {}

        # Test parameters
        self.test_symbols = [instr.symbol for instr in self.instruments]
        self.position_size = self.parameters.get('position_size', 100)
        self.alternate_signals = self.parameters.get('alternate_signals', True)

        self.logger.info(f"Initialized TestStrategy for symbols: {self.test_symbols}")

    def on_market_data(self, event: MarketDataEvent):
        """Generate test signals on each market data event"""
        if not self.is_running:
            return

        instrument = event.instrument
        if instrument.symbol not in self.test_symbols:
            return

        # Alternate signals if configured
        if self.alternate_signals:
            signal_type = SignalType.ENTRY if self.signal_counter % 2 == 0 else SignalType.EXIT
        else:
            signal_type = SignalType.ENTRY  # Always entry

        # Create test signal
        self.generate_signal(
            signal_type=signal_type,
            instrument=instrument,
            price=event.data.close if hasattr(event.data, 'close') else event.data.get('close'),  # Handle different data formats
            quantity=self.position_size,
            notes=f"Test signal {self.signal_counter}"
        )

        self.signal_counter += 1
        self.last_action = signal_type.name

    def on_order_event(self, event: Event):
        """Log all order events"""
        self.logger.info(f"Received order event: {event}")

    def on_execution_event(self, event: Event):
        """Log all execution events"""
        self.logger.info(f"Received execution event: {event}")

    def get_state(self) -> Dict[str, Any]:
        """Add test-specific metrics to state"""
        state = super().get_state()
        state.update({
            "signal_counter": self.signal_counter,
            "last_action": self.last_action
        })
        return state
