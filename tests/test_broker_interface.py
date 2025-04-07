import unittest
import logging
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock

from core.event_manager import EventManager
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

class TestBrokerInterface(BrokerInterface):
    """Test implementation of BrokerInterface."""
    def __init__(self, broker=None, event_manager=None):
        super().__init__(broker, event_manager)

class BrokerInterfaceTest(unittest.TestCase):
    """Test the BrokerInterface."""

    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.broker_interface")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Create mock broker
        self.mock_broker = MockBroker()

        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()

        # Create broker interface with mock broker
        self.broker_interface = TestBrokerInterface(
            broker=self.mock_broker,
            event_manager=self.event_manager
        )

        # Track events
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

        self.logger.info("BrokerInterface test setup complete")

    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("BrokerInterface test teardown complete")

if __name__ == "__main__":
    unittest.main()
