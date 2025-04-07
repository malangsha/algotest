import unittest
import logging
import time
from datetime import datetime
from unittest.mock import MagicMock

from core.event_manager import EventManager
from core.paper_trading_simulator import PaperTradingSimulator
from brokers.broker_interface import BrokerInterface
from models.events import EventType, MarketDataEvent, FillEvent
from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument
from utils.constants import MarketDataType, OrderSide

class PaperTradingSimulatorTest(unittest.TestCase):
    """Test the PaperTradingSimulator component."""

    def setUp(self):
        """Set up the test environment."""
        self.logger = logging.getLogger("tests.paper_trading_simulator")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

        # Mock Event Manager and Broker Interface
        self.event_manager = MagicMock(spec=EventManager)
        self.broker_interface = MagicMock(spec=BrokerInterface)

        # Set up mock broker attributes for paper trading
        self.broker_interface.paper_trading = True
        self.broker_interface.simulated_orders = {}
        self.broker_interface.next_sim_fill_id = 1

        # Mock the get_pending_paper_orders method
        def mock_get_pending():
            return { 
                oid: o for oid, o in self.broker_interface.simulated_orders.items()
                if o.status in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]
            }
        self.broker_interface.get_pending_paper_orders = MagicMock()
        self.broker_interface.get_pending_paper_orders.side_effect = mock_get_pending

        # Create Paper Trading Simulator
        self.simulator = PaperTradingSimulator(
            event_manager=self.event_manager,
            broker_interface=self.broker_interface
        )

        self.logger.info("PaperTradingSimulator test setup complete")

    def test_initialization(self):
        """Test that the simulator initializes and subscribes correctly."""
        self.event_manager.subscribe.assert_called_with(
            EventType.MARKET_DATA,
            self.simulator._on_market_data,
            component_name="PaperTradingSimulator"
        )

    def test_market_order_fill(self):
        """Test that a MARKET order gets filled on the next market data tick."""
        # Add a pending market order to the mock broker
        order = Order(
            instrument_id="AAPL",
            quantity=10,
            side=OrderSide.BUY.value,
            order_type=OrderType.MARKET,
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # Create a market data event
        market_event = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 155.0,
                MarketDataType.BID: 154.95,
                MarketDataType.ASK: 155.05
            }
        )

        # Simulate the event manager calling the handler
        self.simulator._on_market_data(market_event)

        # Verify a FillEvent was published
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event = call_args[0]

        self.assertIsInstance(fill_event, FillEvent)
        self.assertEqual(fill_event.order_id, order_id)
        self.assertEqual(fill_event.symbol, "AAPL")
        self.assertEqual(fill_event.quantity, 10)
        self.assertEqual(fill_event.side, OrderSide.BUY)
        self.assertEqual(fill_event.price, 155.05)

        # Verify the order status was updated in the mock broker
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].filled_quantity, 10)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, 155.05)

    def test_buy_limit_order_fill(self):
        """Test that a BUY LIMIT order fills when price drops below limit."""
        order = Order(
            instrument_id="AAPL",
            quantity=5,
            side=OrderSide.BUY.value,
            order_type=OrderType.LIMIT,
            price=150.0,
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # 1. Market data above limit - should not fill
        market_event_above = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={ MarketDataType.LAST_PRICE: 150.50, MarketDataType.ASK: 150.55 }
        )
        self.simulator._on_market_data(market_event_above)
        self.event_manager.publish.assert_not_called() # No fill yet
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PENDING)

        # 2. Market data at/below limit - should fill
        market_event_below = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={ MarketDataType.LAST_PRICE: 149.90, MarketDataType.ASK: 149.95 }
        )
        self.simulator._on_market_data(market_event_below)
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event = call_args[0]

        self.assertEqual(fill_event.order_id, order_id)
        self.assertEqual(fill_event.quantity, 5)
        # Fill price should be the ask (since it's lower than limit) or limit price
        expected_fill_price = min(149.95, 150.0)
        self.assertEqual(fill_event.price, expected_fill_price)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, expected_fill_price)

    def test_sell_limit_order_fill(self):
        """Test that a SELL LIMIT order fills when price rises above limit."""
        order = Order(
            instrument_id="MSFT",
            quantity=8,
            side=OrderSide.SELL.value,
            order_type=OrderType.LIMIT,
            price=200.0,
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # 1. Market data below limit - should not fill
        market_event_below = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="MSFT"),
            data={ MarketDataType.LAST_PRICE: 199.50, MarketDataType.BID: 199.45 }
        )
        self.simulator._on_market_data(market_event_below)
        self.event_manager.publish.assert_not_called()

        # 2. Market data at/above limit - should fill
        market_event_above = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="MSFT"),
            data={ MarketDataType.LAST_PRICE: 200.10, MarketDataType.BID: 200.05 }
        )
        self.simulator._on_market_data(market_event_above)
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event = call_args[0]

        self.assertEqual(fill_event.order_id, order_id)
        self.assertEqual(fill_event.quantity, 8)
        # Fill price should be the bid (since it's higher than limit) or limit price
        expected_fill_price = max(200.05, 200.0)
        self.assertEqual(fill_event.price, expected_fill_price)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, expected_fill_price)

    def test_no_fill_if_not_paper_trading(self):
        """Test that no fills occur if the broker is not in paper trading mode."""
        # Set broker to non-paper trading mode
        self.broker_interface.paper_trading = False

        # Add a pending market order
        order = Order(
            instrument_id="AAPL",
            quantity=10,
            side=OrderSide.BUY.value,
            order_type=OrderType.MARKET,
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # Create a market data event
        market_event = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={ MarketDataType.LAST_PRICE: 155.0 }
        )

        # Simulate the event manager calling the handler
        self.simulator._on_market_data(market_event)

        # Verify no FillEvent was published
        self.event_manager.publish.assert_not_called()
        # Verify order status remains PENDING
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PENDING)

    def test_partial_fill(self):
        """Test that an order can be filled in multiple parts."""
        # Add a pending market order to the mock broker
        order = Order(
            instrument_id="AAPL",
            quantity=10,
            side=OrderSide.BUY.value,
            order_type=OrderType.MARKET,
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # First market data event - partial fill of 4 shares
        market_event1 = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 155.0,
                MarketDataType.BID: 154.95,
                MarketDataType.ASK: 155.05,
                MarketDataType.VOLUME: 4  # Only 4 shares available
            }
        )

        # Simulate the event manager calling the handler
        self.simulator._on_market_data(market_event1)

        # Verify first FillEvent was published
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event1 = call_args[0]

        self.assertIsInstance(fill_event1, FillEvent)
        self.assertEqual(fill_event1.order_id, order_id)
        self.assertEqual(fill_event1.symbol, "AAPL")
        self.assertEqual(fill_event1.quantity, 4)  # First fill of 4 shares
        self.assertEqual(fill_event1.side, OrderSide.BUY)
        self.assertEqual(fill_event1.price, 155.05)

        # Verify the order status was updated to PARTIALLY_FILLED
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].filled_quantity, 4)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, 155.05)

        # Reset the mock for the next call
        self.event_manager.publish.reset_mock()

        # Second market data event - fill remaining 6 shares
        market_event2 = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 155.5,
                MarketDataType.BID: 155.45,
                MarketDataType.ASK: 155.55,
                MarketDataType.VOLUME: 6  # Remaining 6 shares available
            }
        )

        # Simulate the event manager calling the handler again
        self.simulator._on_market_data(market_event2)

        # Verify second FillEvent was published
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event2 = call_args[0]

        self.assertIsInstance(fill_event2, FillEvent)
        self.assertEqual(fill_event2.order_id, order_id)
        self.assertEqual(fill_event2.symbol, "AAPL")
        self.assertEqual(fill_event2.quantity, 6)  # Second fill of 6 shares
        self.assertEqual(fill_event2.side, OrderSide.BUY)
        self.assertEqual(fill_event2.price, 155.55)

        # Verify the order status was updated to FILLED
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].filled_quantity, 10)
        # Average price should be weighted average of both fills
        expected_avg_price = (4 * 155.05 + 6 * 155.55) / 10
        self.assertAlmostEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, expected_avg_price)

    def test_stop_order_fill(self):
        """Test that a STOP order gets filled when the stop price is hit."""
        # Add a pending stop order to the mock broker
        order = Order(
            instrument_id="AAPL",
            quantity=10,
            side=OrderSide.BUY.value,
            order_type=OrderType.STOP,
            stop_price=160.0,  # Stop price
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # 1. Market data below stop price - should not fill
        market_event_below = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 159.0,
                MarketDataType.BID: 158.95,
                MarketDataType.ASK: 159.05
            }
        )
        self.simulator._on_market_data(market_event_below)
        self.event_manager.publish.assert_not_called()  # No fill yet
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PENDING)

        # 2. Market data at/above stop price - should fill
        market_event_above = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 160.10,
                MarketDataType.BID: 160.05,
                MarketDataType.ASK: 160.15
            }
        )
        self.simulator._on_market_data(market_event_above)
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event = call_args[0]

        self.assertEqual(fill_event.order_id, order_id)
        self.assertEqual(fill_event.quantity, 10)
        # Fill price should be the ask since it's a buy order
        self.assertEqual(fill_event.price, 160.15)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, 160.15)

    def test_stop_limit_order_fill(self):
        """Test that a STOP-LIMIT order gets filled when the stop price is hit and limit price is met."""
        # Add a pending stop-limit order to the mock broker
        order = Order(
            instrument_id="AAPL",
            quantity=10,
            side=OrderSide.BUY.value,
            order_type=OrderType.STOP_LIMIT,
            price=160.0,  # Limit price
            stop_price=165.0,  # Stop price
            strategy_id="test_strat"
        )
        order_id = order.order_id  # Get the generated order_id
        order.status = OrderStatus.PENDING  # Set status to PENDING
        self.broker_interface.simulated_orders[order_id] = order

        # 1. Market data below stop price - should not fill
        market_event_below = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 164.0,
                MarketDataType.BID: 163.95,
                MarketDataType.ASK: 164.05
            }
        )
        self.simulator._on_market_data(market_event_below)
        self.event_manager.publish.assert_not_called()  # No fill yet
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PENDING)

        # 2. Market data at stop price but above limit - should not fill
        market_event_stop_above_limit = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 165.10,
                MarketDataType.BID: 165.05,
                MarketDataType.ASK: 165.15
            }
        )
        self.simulator._on_market_data(market_event_stop_above_limit)
        self.event_manager.publish.assert_not_called()  # No fill yet
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.PENDING)

        # 3. Market data at stop price and at/below limit - should fill
        market_event_stop_at_limit = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(time.time() * 1000),
            instrument=Instrument(symbol="AAPL"),
            data={
                MarketDataType.LAST_PRICE: 165.0,  # At stop price
                MarketDataType.BID: 159.95,  # Below limit price
                MarketDataType.ASK: 159.95  # Below limit price
            }
        )
        self.simulator._on_market_data(market_event_stop_at_limit)
        self.event_manager.publish.assert_called_once()
        call_args = self.event_manager.publish.call_args[0]
        fill_event = call_args[0]

        self.assertEqual(fill_event.order_id, order_id)
        self.assertEqual(fill_event.quantity, 10)
        # Fill price should be the limit price since it's a limit order
        self.assertEqual(fill_event.price, 160.0)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].status, OrderStatus.FILLED)
        self.assertEqual(self.broker_interface.simulated_orders[order_id].average_fill_price, 160.0)

    # TODO: Add tests for STOP and STOP_LIMIT orders

if __name__ == '__main__':
    unittest.main() 