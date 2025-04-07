import unittest
import logging
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from core.event_manager import EventManager
from core.execution_handler import ExecutionHandler
from core.position_manager import PositionManager
from models.events import EventType, OrderEvent, ExecutionEvent, FillEvent
from models.instrument import Instrument
from models.order import Order, OrderStatus, OrderType
from utils.constants import Exchange, OrderSide
from brokers.broker_interface import BrokerInterface

class MockBroker:
    """Mock broker for testing."""
    
    def __init__(self):
        """Initialize the mock broker."""
        self.orders = {}
        
    def place_order(self, order):
        """
        Place an order with the mock broker.
        
        Args:
            order: Order to place
            
        Returns:
            str: Broker order ID
        """
        # Store order
        self.orders[order.order_id] = {
            "order": order,
            "status": OrderStatus.OPEN
        }
        
        # Return broker order ID
        return f"broker-{order.order_id}"
        
    def cancel_order(self, order_id):
        """
        Cancel an order with the mock broker.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            bool: True if cancellation successful, False otherwise
        """
        if order_id in self.orders:
            self.orders[order_id]["status"] = OrderStatus.CANCELLED
            return True
        return False
        
    def modify_order(self, order_id, modified_order):
        """
        Modify an order with the mock broker.
        
        Args:
            order_id: ID of the order to modify
            modified_order: New order details
            
        Returns:
            bool: True if modification successful, False otherwise
        """
        if order_id in self.orders:
            self.orders[order_id]["order"] = modified_order
            return True
        return False
        
    def get_order_status(self, order_id):
        """
        Get the status of an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            OrderStatus: Status of the order
        """
        if order_id in self.orders:
            return self.orders[order_id]["status"]
        return OrderStatus.REJECTED
        
    def simulate_execution(self, order_id, quantity, price, status=OrderStatus.FILLED, commission=0.0):
        """
        Simulate an execution.
        
        Args:
            order_id: Order ID
            quantity: Quantity executed
            price: Execution price
            status: Order status after execution
            commission: Commission for the execution
            
        Returns:
            dict: Execution details
        """
        if order_id in self.orders:
            order = self.orders[order_id]["order"]
            self.orders[order_id]["status"] = status
            
            execution = {
                "order_id": order_id,
                "symbol": order.symbol,
                "exchange": order.exchange,
                "side": order.side,
                "quantity": quantity,
                "price": price,
                "status": status,
                "execution_id": f"exec-{order_id}-{int(time.time())}",
                "broker_order_id": f"broker-{order_id}",
                "commission": commission
            }
            
            return execution
        return None

class ExecutionHandlerTest(unittest.TestCase):
    """Test the ExecutionHandler component."""
    
    def setUp(self):
        """Set up the test environment."""
        self.logger = logging.getLogger("tests.execution_handler")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)
        
        # Mock Event Manager and Broker Interface
        self.event_manager = MagicMock(spec=EventManager)
        self.broker_interface = MagicMock(spec=BrokerInterface)
        self.broker_interface.is_connected.return_value = True # Assume connected initially
        
        # Create Execution Handler
        self.execution_handler = ExecutionHandler(
            event_manager=self.event_manager,
            broker_interface=self.broker_interface
        )
        
        # Create position manager
        self.position_manager = PositionManager(self.event_manager)
        
        # Create instruments for testing
        self.aapl = Instrument(
            symbol="AAPL",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )
        
        self.msft = Instrument(
            symbol="TCS",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )
        
        # Add instruments to position manager
        self.position_manager.add_instrument(self.aapl)
        self.position_manager.add_instrument(self.msft)
        
        # Set up tracking for events
        self.order_events = []
        self.execution_events = []
        self.fill_events = []
        
        self.event_manager.subscribe(
            EventType.ORDER,
            lambda event: self.order_events.append(event),
            component_name="TestOrderTracker"
        )
        
        self.event_manager.subscribe(
            EventType.EXECUTION,
            lambda event: self.execution_events.append(event),
            component_name="TestExecutionTracker"
        )
        
        self.event_manager.subscribe(
            EventType.FILL,
            lambda event: self.fill_events.append(event),
            component_name="TestFillTracker"
        )
        
        self.logger.info("ExecutionHandler test setup complete")
        
    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("ExecutionHandler test teardown complete")
        
    def create_order_event(self, symbol, side, order_type, quantity, price=None, order_id=None):
        """Helper to create an order event."""
        if order_id is None:
            order_id = f"order-{int(datetime.now().timestamp() * 1000)}"
            
        order = OrderEvent(
            event_type=EventType.ORDER,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=order_id,
            symbol=symbol,
            exchange=Exchange.NSE,
            side=side,
            quantity=quantity,
            price=price,
            order_type=order_type,
            status=OrderStatus.OPEN,
            strategy_id="test_strategy"
        )
        
        return order
        
    def test_initialization(self):
        """Test that the handler initializes and subscribes correctly."""
        # Check if subscribe was called on the event manager for ORDER events
        self.event_manager.subscribe.assert_called_with(
            EventType.ORDER,
            self.execution_handler._on_order_event,
            component_name="ExecutionHandler"
        )
        
    def test_submit_order_on_pending_event(self):
        """Test that a PENDING order event triggers place_order on the broker."""
        # Create a PENDING order event (simulating output from OrderManager)
        order_event = OrderEvent(
            event_type=EventType.ORDER,
            timestamp=int(time.time() * 1000),
            order_id="test_order_123",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=100,
            price=150.0,
            order_type=OrderType.LIMIT,
            status=OrderStatus.PENDING, # Status indicating it should be submitted
            strategy_id="test_strategy"
        )
        
        # Simulate the event manager calling the handler
        self.execution_handler._on_order_event(order_event)
        
        # Verify broker_interface.place_order was called
        self.broker_interface.place_order.assert_called_once()
        call_args = self.broker_interface.place_order.call_args[0]
        passed_order = call_args[0]
        
        # Verify the details of the order passed to the broker
        self.assertIsInstance(passed_order, Order)
        self.assertEqual(passed_order.order_id, "test_order_123")
        self.assertEqual(passed_order.instrument_id, "AAPL")
        self.assertEqual(passed_order.quantity, 100)
        self.assertEqual(passed_order.side, OrderSide.BUY.value) # Check string value
        self.assertEqual(passed_order.price, 150.0)
        self.assertEqual(passed_order.order_type, OrderType.LIMIT.value)
        
    def test_cancel_order_on_pending_cancel_event(self):
        """Test that a PENDING_CANCEL order event triggers cancel_order on the broker."""
        # Create a PENDING_CANCEL order event
        order_event = OrderEvent(
            event_type=EventType.ORDER,
            timestamp=int(time.time() * 1000),
            order_id="test_order_456",
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=50,
            order_type=OrderType.LIMIT,
            status=OrderStatus.CANCEL_PENDING,  # Changed from PENDING_CANCEL
            strategy_id="test_strategy"
        )
        
        # Simulate the event manager calling the handler
        self.execution_handler._on_order_event(order_event)
        
        # Verify broker_interface.cancel_order was called
        self.broker_interface.cancel_order.assert_called_once_with("test_order_456")
        
    def test_ignore_other_statuses(self):
        """Test that events with other statuses (e.g., FILLED) are ignored by the handler."""
        # Create a FILLED order event
        order_event = OrderEvent(
            event_type=EventType.ORDER,
            timestamp=int(time.time() * 1000),
            order_id="test_order_789",
            symbol="INFY",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            order_type=OrderType.MARKET,
            status=OrderStatus.FILLED, # Status that should be ignored for submission/cancellation
            strategy_id="test_strategy"
        )
        
        # Simulate the event manager calling the handler
        self.execution_handler._on_order_event(order_event)
        
        # Verify broker methods were NOT called
        self.broker_interface.place_order.assert_not_called()
        self.broker_interface.cancel_order.assert_not_called()
        self.broker_interface.modify_order.assert_not_called()
        
    # TODO: Add test for PENDING_REPLACE status triggering modify_order
    # TODO: Add test for broker connection failure handling

if __name__ == "__main__":
    unittest.main() 