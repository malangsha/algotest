#!/usr/bin/env python
"""
Unit tests for the event flow functionality.
This tests the path of events from signal generation through execution.
"""

import unittest
import time
import logging
from datetime import datetime

from core.event_manager import EventManager
from core.order_manager import OrderManager
from core.position_manager import PositionManager
from models.events import (
    Event, EventType, SignalEvent, OrderEvent, FillEvent, PositionEvent
)
from utils.constants import OrderSide, SignalType, OrderType, Exchange

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class EventFlowTest(unittest.TestCase):
    """Test the event flow functionality from signal to position."""

    def setUp(self):
        """Set up the test environment."""
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)

        # Create position manager
        self.position_manager = PositionManager()

        # Connect components to event manager
        mock_risk_manager = MockRiskManager()
        self.order_manager = OrderManager(
            event_manager=self.event_manager,
            risk_manager=mock_risk_manager
        )
        self.position_manager.register_with_event_manager(self.event_manager)

        # Track received events by type for validation
        self.received_events = {
            EventType.SIGNAL: [],
            EventType.ORDER: [],
            EventType.FILL: [],
            EventType.POSITION: []
        }

        # Set up event tracking
        for event_type in self.received_events.keys():
            self.event_manager.subscribe(
                event_type,
                lambda event, et=event_type: self.received_events[et].append(event),
                f"Test{event_type.value}Listener"
            )

        # Start the event manager
        self.event_manager.start()

        # Wait for initialization
        time.sleep(0.2)

    def tearDown(self):
        """Clean up after tests."""
        self.event_manager.stop()
        time.sleep(0.2)  # Wait for shutdown

    def test_signal_to_order_flow(self):
        """Test the flow from signal to order."""
        # Create and publish a signal
        signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.ENTRY,
            signal_price=150.0,
            side=OrderSide.BUY,
            quantity=10,
            order_type=OrderType.MARKET
        )

        # Clear tracking
        for events in self.received_events.values():
            events.clear()

        # Publish the signal
        self.event_manager.publish(signal)

        # Wait for processing
        time.sleep(0.5)

        # Verify signal was received
        self.assertEqual(len(self.received_events[EventType.SIGNAL]), 1, "Signal not received")

        # Verify order was created
        self.assertGreater(len(self.received_events[EventType.ORDER]), 0, "Order not created from signal")

        # Verify order details
        order_event = self.received_events[EventType.ORDER][0]
        self.assertEqual(order_event.symbol, "TCS", "Order symbol doesn't match signal")
        self.assertEqual(order_event.side, OrderSide.BUY, "Order side doesn't match signal")
        self.assertEqual(order_event.quantity, 10, "Order quantity doesn't match signal")

        return order_event  # Return for use in other tests

    def test_order_to_fill_flow(self):
        """Test the flow from order to fill."""
        # First create an order via signal
        order_event = self.test_signal_to_order_flow()

        # Clear tracking except orders
        for event_type, events in self.received_events.items():
            if event_type != EventType.ORDER:
                events.clear()

        # Create and publish fill for the order
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=order_event.order_id,
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=150.0,
            fill_time=int(datetime.now().timestamp() * 1000)
        )

        # Publish the fill
        self.event_manager.publish(fill_event)

        # Wait for processing
        time.sleep(0.5)

        # Verify fill was received
        self.assertEqual(len(self.received_events[EventType.FILL]), 1, "Fill not received")

        # Return for use in other tests
        return order_event, self.received_events[EventType.FILL][0]

    def test_fill_to_position_flow(self):
        """Test the complete flow from fill to position update."""
        # First create an order and fill
        order_event, fill_event = self.test_order_to_fill_flow()

        # Wait for position update
        time.sleep(0.5)

        # Verify position was updated
        self.assertGreater(len(self.received_events[EventType.POSITION]), 0, "Position not updated from fill")

        # Verify position details
        position_event = self.received_events[EventType.POSITION][0]
        self.assertEqual(position_event.symbol, "TCS", "Position symbol doesn't match fill")
        self.assertEqual(position_event.quantity, 10, "Position quantity doesn't match fill")

        # Verify position is in position manager
        position = self.position_manager.get_position("TCS")
        self.assertIsNotNone(position, "Position not found in position manager")
        self.assertEqual(position.quantity, 10, "Position quantity incorrect in manager")

        return position_event

    def test_complete_flow_with_multiple_fills(self):
        """Test a complete flow with multiple fills that update position."""
        # First create a position
        position_event = self.test_fill_to_position_flow()

        # Clear tracking
        for events in self.received_events.values():
            events.clear()

        # Create a sell signal
        signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.EXIT,
            signal_price=160.0,
            side=OrderSide.SELL,
            quantity=5,  # Partial sell
            order_type=OrderType.MARKET
        )

        # Publish the signal
        self.event_manager.publish(signal)

        # Wait for order creation
        time.sleep(0.5)

        # Verify order was created
        self.assertGreater(len(self.received_events[EventType.ORDER]), 0, "Sell order not created")
        sell_order = self.received_events[EventType.ORDER][0]

        # Create fill for the sell order
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=sell_order.order_id,
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=5,
            price=160.0,
            fill_time=int(datetime.now().timestamp() * 1000)
        )

        # Publish the fill
        self.event_manager.publish(fill_event)

        # Wait for position update
        time.sleep(0.5)

        # Verify position was updated
        self.assertGreater(len(self.received_events[EventType.POSITION]), 0, "Position not updated after sell")

        # Verify position details
        position_event = self.received_events[EventType.POSITION][0]
        self.assertEqual(position_event.symbol, "TCS", "Position symbol incorrect")
        self.assertEqual(position_event.quantity, 5, "Position quantity not updated correctly")

        # Verify position manager state
        position = self.position_manager.get_position("TCS")
        self.assertIsNotNone(position, "Position not found in position manager")
        self.assertEqual(position.quantity, 5, "Position quantity incorrect in manager after sell")

    def test_event_flow_monitor(self):
        """Test that the event flow monitor tracks event chains correctly."""
        # Run a complete event flow
        self.test_fill_to_position_flow()

        # Get flow monitor diagnostics
        flow_diagnostics = self.event_manager.get_flow_monitor_diagnostics()

        # Verify stats
        self.assertIn('stats', flow_diagnostics, "Flow monitor diagnostics missing stats")
        stats = flow_diagnostics['stats']

        # We should have completed chains
        self.assertGreater(stats['complete_chains'], 0, "No completed chains in flow monitor")

        # And should not have broken chains if everything worked
        self.assertEqual(stats['broken_chains'], 0, "Broken chains detected in flow monitor")
        
    def test_invalid_signal_validation(self):
        """Test that invalid signals are detected by the flow monitor."""
        # Create an invalid signal (missing required fields)
        invalid_signal = {
            "event_type": EventType.SIGNAL,
            "timestamp": int(datetime.now().timestamp() * 1000),
            "strategy_id": "test_strategy",
            "symbol": "TCS",
            # Missing signal_type, side, price, and quantity
        }
        
        # Create Event object
        from models.events import SignalEvent
        signal = SignalEvent(**invalid_signal)
        
        # Publish the invalid signal
        self.event_manager.publish(signal)
        
        # Wait for processing
        time.sleep(0.5)
        
        # Check flow monitor stats
        flow_diagnostics = self.event_manager.get_flow_monitor_diagnostics()
        stats = flow_diagnostics['stats']
        
        # Should have detected a broken chain due to validation error
        self.assertGreater(stats['broken_chains'], 0, 
                          "Flow monitor did not detect invalid signal as broken chain")
        
        # Check broken chains details
        broken_chains = flow_diagnostics['broken_chains']
        self.assertGreater(len(broken_chains), 0, "No broken chains recorded")
        
        # At least one broken chain should be for our signal
        found_signal_error = False
        for chain in broken_chains:
            if (chain.get('parent_event') == EventType.SIGNAL and 
                'missing_fields' in chain.get('message', '')):
                found_signal_error = True
                break
                
        self.assertTrue(found_signal_error, "No broken chain for invalid signal found")

    def test_partial_flow_completion(self):
        """Test partial completion of event flow (signal to order only)."""
        # Create a valid signal
        signal = SignalEvent(
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            strategy_id="test_strategy",
            symbol="TCS",
            exchange=Exchange.NSE,
            signal_type=SignalType.ENTRY,
            side=OrderSide.BUY,
            price=150.0,
            quantity=10,
            signal_price=150.0,
            order_type=OrderType.MARKET
        )
        
        # Clear tracking
        for events in self.received_events.values():
            events.clear()
            
        # Publish the signal
        self.event_manager.publish(signal)
        
        # Wait for processing
        time.sleep(0.5)
        
        # Verify order was created
        self.assertEqual(len(self.received_events[EventType.ORDER]), 1, "Order not created from signal")
        
        # Get flow monitor diagnostics
        flow_diagnostics = self.event_manager.get_flow_monitor_diagnostics()
        
        # Check for completed signal to order chain
        completed_chains = flow_diagnostics['completed_chains']
        active_chains = flow_diagnostics['active_chains']
        
        # Should have a completed signal to order chain
        found_completed_signal_to_order = False
        for chain in completed_chains:
            if (chain.get('parent_event') == EventType.SIGNAL and
                len(chain.get('events_seen', [])) == 2 and
                EventType.ORDER in chain.get('events_seen', [])):
                found_completed_signal_to_order = True
                break
                
        self.assertTrue(found_completed_signal_to_order, 
                       "No completed signal to order chain found")
        
        # Should have active order to fill chains
        order_chains_active = False
        for chain_id, chain in active_chains.items():
            if (chain.get('parent_event') == EventType.ORDER and
                EventType.FILL in chain.get('expected_events', [])):
                order_chains_active = True
                break
                
        self.assertTrue(order_chains_active, 
                       "No active order to fill chains found")

    def test_timeout_detection(self):
        """Test detection of timed-out event chains."""
        # Create a valid order event that won't be followed by a fill
        from models.events import OrderEvent
        order = OrderEvent(
            event_type=EventType.ORDER,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="timeout_test_order",
            symbol="TCS",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=150.0,
            strategy_id="test_strategy",
            order_type=OrderType.MARKET
        )
        
        # Clear tracking
        for events in self.received_events.values():
            events.clear()
            
        # Publish the order
        self.event_manager.publish(order)
        
        # Access the flow monitor directly to reduce timeout for this test
        flow_monitor = self.event_manager.flow_monitor
        
        # Find the chain for our order and set a short timeout
        for flow_id, flow_data in flow_monitor.active_flow_ids.items():
            if (flow_data.get('metadata', {}).get('order_id') == "timeout_test_order"):
                flow_data['timeout'] = time.time() + 0.5  # Short timeout
        
        # Wait for timeout
        time.sleep(1.0)
        
        # Get flow monitor diagnostics
        flow_diagnostics = self.event_manager.get_flow_monitor_diagnostics()
        stats = flow_diagnostics['stats']
        
        # Should have detected a broken chain due to timeout
        self.assertGreater(stats['broken_chains'], 0, 
                          "Flow monitor did not detect timed-out chain")
        
        # Check broken chains details
        broken_chains = flow_diagnostics['broken_chains']
        self.assertGreater(len(broken_chains), 0, "No broken chains recorded")
        
        # At least one broken chain should be for our order
        found_timeout_error = False
        for chain in broken_chains:
            if (chain.get('parent_event') == EventType.ORDER and
                'timeout_test_order' in str(chain.get('metadata', {}))):
                found_timeout_error = True
                break
                
        self.assertTrue(found_timeout_error, "No broken chain for timed-out order found")


class MockRiskManager:
    """Mock risk manager for testing."""

    def __init__(self):
        self.should_allow = True

    def check_signal(self, signal):
        """Always allow signals."""
        return self.should_allow

    def set_allow(self, allow):
        """Set whether signals should be allowed."""
        self.should_allow = allow


if __name__ == "__main__":
    unittest.main()
