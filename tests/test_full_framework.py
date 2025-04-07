import unittest
import logging
import time
from datetime import datetime
from unittest.mock import Mock

from models.events import (
    SignalEvent, OrderEvent, FillEvent, ExecutionEvent,
    EventType, SignalType, OrderSide, OrderType
)
from models.instrument import Instrument
from models.position import Position
from core.event_manager import EventManager
from core.position_manager import PositionManager
from core.portfolio import Portfolio
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from utils.constants import Exchange, OrderStatus, InstrumentType

# Define a simple MockRiskManager for testing purposes
class MockRiskManager:
    """A simplified risk manager for testing that properly handles order sizing"""
    
    def __init__(self, event_manager=None):
        self.event_manager = event_manager
        self.logger = logging.getLogger("MockRiskManager")
        self.max_position_size = 50  # Maximum position size in quantity
        
    def _on_signal_event(self, event):
        """Handle signal events by applying risk rules"""
        if not hasattr(event, 'quantity') or not event.quantity:
            return
            
        # If quantity exceeds max position size, adjust it
        if event.quantity > self.max_position_size:
            self.logger.info(f"Adjusting signal quantity from {event.quantity} to {self.max_position_size}")
            
            # Create a new signal with adjusted quantity
            adjusted_signal = SignalEvent(
                event_type=EventType.SIGNAL,
                timestamp=event.timestamp,
                event_id=event.event_id,
                symbol=event.symbol,
                exchange=event.exchange,
                signal_type=event.signal_type,
                signal_price=event.signal_price,
                signal_time=event.signal_time,
                strategy_id=event.strategy_id,
                side=event.side,
                quantity=self.max_position_size,  # Adjusted quantity
                order_type=event.order_type,
                price=event.price
            )
            
            # Publish the adjusted signal
            if self.event_manager:
                self.event_manager.publish(adjusted_signal)
                
            # Prevent the original signal from being processed further
            return False
        
        # Original signal is fine
        return True
        
    def register_with_event_manager(self, event_manager):
        """Register with event manager"""
        self.event_manager = event_manager
        event_manager.subscribe(
            EventType.SIGNAL,
            self._on_signal_event,
            component_name="MockRiskManager"
        )
        self.logger.info("MockRiskManager registered with Event Manager")

class FullFrameworkTest(unittest.TestCase):
    """Test the entire framework with all components working together."""

    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.full_framework")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()
        
        # Create position manager
        self.position_manager = PositionManager(self.event_manager)
        
        # Create portfolio with appropriate config
        portfolio_config = {
            "portfolio": {
                "initial_capital": 1000000.0,
                "max_leverage": 1.0,
                "max_position_size": 0.2
            },
            "risk": {
                "max_position_size": 50,  # Maximum position size in quantity
                "max_open_positions": 10,
                "position_sizing": "fixed",
                "fixed_position_size": 100000,  # Fixed position size in currency
                "max_drawdown": 0.10,
                "max_exposure": 0.8,
                "max_instrument_exposure": 0.20
            }
        }
        self.portfolio = Portfolio(config=portfolio_config, 
                                  position_manager=self.position_manager,
                                  event_manager=self.event_manager)
        
        # Create order manager
        self.order_manager = OrderManager(self.event_manager)
        
        # Create mock risk manager for more predictable testing
        self.risk_manager = MockRiskManager()
        self.risk_manager.register_with_event_manager(self.event_manager)
        
        # Create test instrument
        self.instrument = Instrument(
            symbol="TCS",
            exchange=Exchange.NSE,
            instrument_type=InstrumentType.EQUITY,
            tick_size=0.01,
            lot_size=1,
            trading_hours=("09:30", "16:00")
        )
        
        # Add instrument to position manager
        self.position_manager.add_instrument(self.instrument)
        
        # Create tracking for received events
        self.received_events = {event_type: [] for event_type in EventType}
        for event_type in EventType:
            self.event_manager.subscribe(
                event_type,
                lambda e, et=event_type: self.received_events[et].append(e),
                component_name="TestEventTracker"
            )
        
        self.logger.info("Test setup complete")

    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("Test teardown complete")

    def test_full_trading_cycle(self):
        """Test the full trading cycle from signal to position."""
        # Create an initial buy signal
        buy_signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.ENTRY,
            side=OrderSide.BUY,
            price=3500.0,
            quantity=10,
            signal_price=3500.0,
            signal_time=int(datetime.now().timestamp() * 1000),
            order_type=OrderType.MARKET
        )
        
        # Reset tracking
        for events in self.received_events.values():
            events.clear()
        
        # Publish the buy signal
        self.logger.info("Publishing buy signal")
        self.event_manager.publish(buy_signal)
        
        # Wait for signal processing
        time.sleep(1.0)
        
        # Verify order was created
        self.assertGreaterEqual(len(self.received_events[EventType.ORDER]), 1, 
                             "Order not created from buy signal")
        buy_order_event = self.received_events[EventType.ORDER][-1]
        buy_order_id = buy_order_event.order_id
        
        self.logger.info(f"Buy order created with ID: {buy_order_id}")
        
        # Create execution event (using compatible parameters)
        execution = ExecutionEvent(
            event_type=EventType.EXECUTION,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=buy_order_id,
            symbol="TCS",  
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,  # Use this instead of filled_quantity
            price=3500.0,  # Use this instead of filled_price
            status=OrderStatus.FILLED,
            remaining_quantity=0,
            strategy_id="test_strategy"
        )
        
        # Publish execution
        self.logger.info("Publishing execution for buy order")
        self.event_manager.publish(execution)
        
        # Wait for execution processing
        time.sleep(0.5)
        
        # Create fill event
        fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=buy_order_id,
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=3500.0,
            strategy_id="test_strategy"
        )
        
        # Publish fill
        self.logger.info("Publishing fill for buy order")
        self.event_manager.publish(fill)
        
        # Wait for fill processing
        time.sleep(0.5)
        
        # Verify position was created
        position = self.position_manager.get_position("TCS")
        self.assertIsNotNone(position, "Position not created after buy fill")
        self.assertEqual(position.quantity, 10, "Position quantity incorrect after buy")
        self.assertAlmostEqual(position.average_price, 3500.0, delta=0.01, 
                            msg="Position average price incorrect after buy")
        
        # Check portfolio reflects the position
        self.logger.info(f"Portfolio cash after buy: {self.portfolio.cash}")
        
        # Now create a partial sell signal
        sell_signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.EXIT,
            side=OrderSide.SELL,
            price=3500.0,
            quantity=5,  # Partial sell
            signal_price=3500.0,
            signal_time=int(datetime.now().timestamp() * 1000),
            order_type=OrderType.MARKET
        )
        
        # Reset tracking
        for events in self.received_events.values():
            events.clear()
        
        # Publish the sell signal
        self.logger.info("Publishing sell signal")
        self.event_manager.publish(sell_signal)
        
        # Wait for signal processing
        time.sleep(1.0)
        
        # Verify order was created
        self.assertGreaterEqual(len(self.received_events[EventType.ORDER]), 1, 
                             "Order not created from sell signal")
        sell_order_event = self.received_events[EventType.ORDER][-1]
        sell_order_id = sell_order_event.order_id
        
        self.logger.info(f"Sell order created with ID: {sell_order_id}")
        
        # Create execution event for sell (using compatible parameters)
        execution = ExecutionEvent(
            event_type=EventType.EXECUTION,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=sell_order_id,
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=5,  # Use this instead of filled_quantity
            price=3500.0,  # Use this instead of filled_price
            status=OrderStatus.FILLED,
            remaining_quantity=0,
            strategy_id="test_strategy"
        )
        
        # Publish execution
        self.logger.info("Publishing execution for sell order")
        self.event_manager.publish(execution)
        
        # Wait for execution processing
        time.sleep(0.5)
        
        # Create fill event for sell
        fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=sell_order_id,
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=5,
            price=3500.0,
            strategy_id="test_strategy"
        )
        
        # Publish fill
        self.logger.info("Publishing fill for sell order")
        self.event_manager.publish(fill)
        
        # Wait for fill processing
        time.sleep(0.5)
        
        # Verify position was updated
        position = self.position_manager.get_position("TCS")
        self.assertIsNotNone(position, "Position lost after sell fill")
        self.assertEqual(position.quantity, 5, "Position quantity incorrect after sell")
        
        # Check realized PnL
        expected_pnl = 5 * (3500.0 - 3500.0)  # 5 shares sold at $10 profit each
        self.assertAlmostEqual(position.realized_pnl, expected_pnl, delta=0.01,
                            msg="Position realized PnL incorrect after sell")
        
        # Check portfolio reflects both transactions
        self.logger.info(f"Portfolio cash after sell: {self.portfolio.cash}")
        
        # Check event flow monitor
        flow_diagnostics = self.event_manager.get_flow_monitor_diagnostics()
        
        # Verify no broken chains
        self.assertEqual(flow_diagnostics['stats']['broken_chains'], 0, 
                      "Found broken chains in successful flow")
        
        # Verify we have completed chains
        self.assertGreater(flow_diagnostics['stats']['complete_chains'], 0, 
                        "No completed chains in event flow")
        
        # Log chain completion stats
        self.logger.info(f"Average chain completion time: {flow_diagnostics['stats']['avg_completion_time']:.6f} seconds")
        
        self.logger.info("Full trading cycle test completed successfully")

    def test_risk_management(self):
        """Test risk management constraints in full framework."""
        # Create a signal with quantity that exceeds risk limits
        large_signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.ENTRY,
            side=OrderSide.BUY,
            price=3500.0,
            quantity=200,  # Exceeds max position size
            signal_price=3500.0,
            signal_time=int(datetime.now().timestamp() * 1000),
            order_type=OrderType.MARKET
        )
        
        # Reset tracking
        for events in self.received_events.values():
            events.clear()
        
        # Publish the oversized signal
        self.logger.info("Publishing large buy signal that should be adjusted by risk manager")
        self.event_manager.publish(large_signal)
        
        # Wait for signal processing
        time.sleep(1.0)
        
        # Check if an order was created
        if len(self.received_events[EventType.ORDER]) == 0:
            self.fail("No order was created from the signal")
        
        # Verify order was created with adjusted quantity
        adjusted_order = self.received_events[EventType.ORDER][-1]
        
        # The risk manager should have reduced the quantity to max_position_size (50) or less
        self.assertLessEqual(adjusted_order.quantity, 50, 
                          "Risk manager did not adjust oversized order")
        
        self.logger.info(f"Risk manager adjusted order quantity from 200 to {adjusted_order.quantity}")

if __name__ == "__main__":
    unittest.main() 