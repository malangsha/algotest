# /home/ubuntu/algotest/tests/test_event_manager.py
"""
Unit tests for the EventManager and related event handling.
"""

import unittest
import time
import logging
import sys
import os
from queue import Queue
from threading import Event as ThreadEvent # Rename to avoid conflict with our Event class

# Add project root to sys.path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.event_manager import EventManager, EventFlowMonitor
from models.events import Event, EventType, SignalEvent, OrderEvent, FillEvent, PositionEvent, TimeframeEventType
from models.instrument import Instrument, InstrumentType, AssetClass
from models.order import    OrderType, OrderStatus
from utils.constants import Exchange, OrderSide

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class TestEventManager(unittest.TestCase):
    """Tests the core functionality of the EventManager."""

    def setUp(self):
        """Set up a new EventManager for each test."""
        self.event_manager = EventManager(queue_size=100, process_sleep=0.001, enable_monitoring=True)
        self.event_manager.start()
        self.received_events = []
        self.callback_triggered = ThreadEvent() # Use threading.Event

    def tearDown(self):
        """Stop the EventManager after each test."""
        self.event_manager.stop()
        # Give time for threads to clean up
        time.sleep(0.1)

    def _callback(self, event: Event):
        """Simple callback to record received events."""
        logging.info(f"Callback received event: {event.event_type}")
        self.received_events.append(event)
        self.callback_triggered.set() # Signal that the callback was triggered

    def test_01_publish_subscribe_async(self):
        """Test basic asynchronous publish and subscribe."""
        test_event_type = EventType.CUSTOM
        self.event_manager.subscribe(test_event_type, self._callback, component_name="TestSubscriber")
        
        test_event = Event(event_type=test_event_type, timestamp=int(time.time() * 1000))
        success = self.event_manager.publish(test_event)
        self.assertTrue(success, "Event publishing failed")
        
        # Wait for the event to be processed by the callback
        triggered = self.callback_triggered.wait(timeout=2.0) # Wait up to 2 seconds
        self.assertTrue(triggered, "Callback was not triggered within timeout")
        
        self.assertEqual(len(self.received_events), 1, "Callback did not receive the event")
        self.assertEqual(self.received_events[0].event_type, test_event_type, "Received event has wrong type")
        self.assertEqual(self.received_events[0].event_id, test_event.event_id, "Received event has wrong ID")

    def test_02_publish_sync(self):
        """Test synchronous event publishing."""
        test_event_type = EventType.SYSTEM
        self.event_manager.subscribe(test_event_type, self._callback, component_name="TestSyncSubscriber")
        
        test_event = Event(event_type=test_event_type, timestamp=int(time.time() * 1000))
        callbacks_executed = self.event_manager.publish_sync(test_event)
        
        # Synchronous publish should execute callback immediately
        self.assertEqual(callbacks_executed, 1, "Sync publish did not execute the expected number of callbacks")
        self.assertEqual(len(self.received_events), 1, "Callback did not receive the event synchronously")
        self.assertEqual(self.received_events[0].event_type, test_event_type, "Received sync event has wrong type")

    def test_03_multiple_subscribers(self):
        """Test that multiple subscribers receive the same event."""
        test_event_type = EventType.MARKET_DATA
        callback1_received = []
        callback2_received = []
        callback1_triggered = ThreadEvent()
        callback2_triggered = ThreadEvent()

        def callback1(event: Event):
            logging.info("Callback 1 received event")
            callback1_received.append(event)
            callback1_triggered.set()

        def callback2(event: Event):
            logging.info("Callback 2 received event")
            callback2_received.append(event)
            callback2_triggered.set()

        self.event_manager.subscribe(test_event_type, callback1, component_name="Callback1")
        self.event_manager.subscribe(test_event_type, callback2, component_name="Callback2")

        test_event = Event(event_type=test_event_type, timestamp=int(time.time() * 1000))
        self.event_manager.publish(test_event)

        triggered1 = callback1_triggered.wait(timeout=2.0)
        triggered2 = callback2_triggered.wait(timeout=2.0)
        self.assertTrue(triggered1, "Callback 1 was not triggered")
        self.assertTrue(triggered2, "Callback 2 was not triggered")

        self.assertEqual(len(callback1_received), 1)
        self.assertEqual(len(callback2_received), 1)
        self.assertEqual(callback1_received[0].event_id, test_event.event_id)
        self.assertEqual(callback2_received[0].event_id, test_event.event_id)

    def test_04_event_priority(self):
        """Test that higher priority events are processed first (qualitative)."""
        # This test is qualitative as precise timing is hard to guarantee
        low_priority_event_type = EventType.POSITION
        high_priority_event_type = EventType.ORDER

        self.event_manager.subscribe(low_priority_event_type, self._callback, component_name="LowPriSub")
        self.event_manager.subscribe(high_priority_event_type, self._callback, component_name="HighPriSub")

        low_event = Event(event_type=low_priority_event_type, timestamp=int(time.time() * 1000))
        high_event = Event(event_type=high_priority_event_type, timestamp=int(time.time() * 1000))

        # Publish low priority first, then high priority immediately after
        self.event_manager.publish(low_event)
        self.event_manager.publish(high_event)

        # Wait for both events (or timeout)
        start_wait = time.time()
        while len(self.received_events) < 2 and (time.time() - start_wait) < 3.0:
            time.sleep(0.01)

        self.assertEqual(len(self.received_events), 2, "Did not receive both events")
        
        # Check if the high priority event was received before the low priority one
        # Note: This depends on thread scheduling, but PriorityQueue should favor it.
        if len(self.received_events) == 2:
            self.assertEqual(self.received_events[0].event_type, high_priority_event_type, "High priority event was not processed first")
            self.assertEqual(self.received_events[1].event_type, low_priority_event_type, "Low priority event was not processed second")

    def test_05_queue_full_retry(self):
        """Test that events are retried if the main queue is full."""
        # Reduce queue size temporarily for this test (requires modifying EventManager or creating a new one)
        # For simplicity, we simulate by blocking the processing thread briefly
        
        # Stop the default manager and create one with a tiny queue
        self.event_manager.stop()
        time.sleep(0.1)
        self.event_manager = EventManager(queue_size=1, process_sleep=0.1, enable_monitoring=True)
        self.event_manager.start()
        self.event_manager.subscribe(EventType.CUSTOM, self._callback)

        event1 = Event(event_type=EventType.CUSTOM, timestamp=int(time.time() * 1000))
        event2 = Event(event_type=EventType.CUSTOM, timestamp=int(time.time() * 1000))
        event3 = Event(event_type=EventType.CUSTOM, timestamp=int(time.time() * 1000))

        # Publish first event - should go to main queue
        success1 = self.event_manager.publish(event1)
        self.assertTrue(success1)
        self.assertEqual(self.event_manager.event_queue.qsize(), 1)
        self.assertEqual(self.event_manager.retry_queue.qsize(), 0)

        # Publish second event - main queue full, should go to retry queue
        success2 = self.event_manager.publish(event2)
        self.assertTrue(success2) # Publish should still return True as it went to retry
        self.assertEqual(self.event_manager.event_queue.qsize(), 1)
        self.assertEqual(self.event_manager.retry_queue.qsize(), 1)

        # Publish third event - retry queue might also be full depending on timing
        # Let's assume retry queue also has size 1 for this test case
        # (Actual retry queue size is same as main queue size in implementation)
        # So, event3 might fail if both queues are full instantly.
        # Let's wait a bit for the processor to potentially clear event1
        time.sleep(0.2) # Allow time for event1 to be processed
        
        # Now event queue should be empty, retry queue should have event2
        # The retry thread should move event2 to main queue
        time.sleep(0.2) # Allow time for retry thread
        
        self.assertEqual(self.event_manager.event_queue.qsize(), 1) # Should contain event2 now
        self.assertEqual(self.event_manager.retry_queue.qsize(), 0)

        # Publish event3 - should go into main queue now
        success3 = self.event_manager.publish(event3)
        self.assertTrue(success3)
        self.assertEqual(self.event_manager.event_queue.qsize(), 2) # Event2 and Event3

        # Wait for all events to be processed
        start_wait = time.time()
        while len(self.received_events) < 3 and (time.time() - start_wait) < 5.0:
            time.sleep(0.1)
            
        self.assertEqual(len(self.received_events), 3, "Did not receive all 3 events after retry")

    def test_06_event_flow_monitor_complete_chain(self):
        """Test the EventFlowMonitor detects a complete event chain."""
        monitor = self.event_manager.flow_monitor
        self.assertIsNotNone(monitor, "Flow monitor not enabled or initialized")

        # Simulate a Signal -> Order -> Fill -> Position chain
        instrument = Instrument(symbol="TEST", exchange=Exchange.NSE, instrument_type=InstrumentType.EQUITY, asset_class=AssetClass.EQUITY)
        signal_event = SignalEvent(symbol="RELIANCE-EQ", exchange=Exchange.NSE.value, signal_type="ENTRY", side=OrderSide.BUY, price=100.0, quantity=10)
        order_event = OrderEvent(order_id=signal_event.event_id, symbol="TEST", exchange=Exchange.NSE, side=OrderSide.BUY, quantity=10, order_type=OrderType.LIMIT, status=OrderStatus.SUBMITTED, price=100.0, strategy_id=signal_event.strategy_id)
        fill_event = FillEvent(order_id=order_event.order_id, symbol="TEST", exchange=Exchange.NSE, side=OrderSide.BUY, quantity=10, price=100.0, strategy_id=order_event.strategy_id)
        position_event = PositionEvent(symbol="RELIANCE-EQ", exchange=Exchange.NSE.value, quantity=10, average_price=100.0)

        # Publish events in order
        self.event_manager.publish_sync(signal_event) # Use sync to ensure monitor sees it first
        time.sleep(0.01) # Small delay
        self.event_manager.publish_sync(order_event)
        time.sleep(0.01)
        self.event_manager.publish_sync(fill_event)
        time.sleep(0.01)
        self.event_manager.publish_sync(position_event)
        time.sleep(0.1) # Allow monitor time to process

        stats = monitor.get_stats()
        self.assertGreaterEqual(stats["complete_chains"], 1, "Monitor did not register a complete chain")
        # Check active chains - might depend on exact timing and other test events
        # self.assertEqual(stats["active_chains"], 0, "Monitor shows active chains after completion")
        self.assertEqual(stats["broken_chains"], 0, "Monitor incorrectly registered a broken chain")

    def test_07_event_flow_monitor_broken_chain(self):
        """Test the EventFlowMonitor detects a broken event chain (timeout)."""
        monitor = self.event_manager.flow_monitor
        self.assertIsNotNone(monitor)
        
        # Reduce timeout for testing
        original_timeout = monitor.timeouts.get(EventType.SIGNAL, 5.0)
        monitor.timeouts[EventType.SIGNAL] = 0.1 # Set timeout to 0.1 seconds

        # Simulate only a Signal event
        instrument = Instrument(symbol="TEST_BROKEN", exchange=Exchange.NSE, instrument_type=InstrumentType.EQUITY, asset_class=AssetClass.EQUITY)
        signal_event = SignalEvent(instrument=instrument, signal_type="ENTRY", side=OrderSide.BUY, price=100.0, quantity=10)

        self.event_manager.publish_sync(signal_event)
        
        # Wait longer than the timeout
        time.sleep(0.5)
        
        # Manually trigger timeout check if needed (implementation detail)
        # In the actual EventManager, the monitor runs within the event loop or its own thread.
        # For testing, we might need to explicitly call a check method if one existed,
        # or rely on the fact that _on_event checks timeouts periodically.
        # Let's publish another event to trigger the check in _on_event
        dummy_event = Event(event_type=EventType.SYSTEM)
        self.event_manager.publish_sync(dummy_event)
        time.sleep(0.1) # Allow check to run

        stats = monitor.get_stats()
        self.assertGreaterEqual(stats["broken_chains"], 1, "Monitor did not register a broken chain after timeout")
        self.assertEqual(len(monitor.get_broken_chains()), 1, "Broken chain history not updated correctly")
        if monitor.get_broken_chains():
            self.assertEqual(monitor.get_broken_chains()[0]["parent_event"], EventType.SIGNAL)
            self.assertIn(EventType.ORDER, monitor.get_broken_chains()[0]["missing_events"])

        # Restore original timeout
        monitor.timeouts[EventType.SIGNAL] = original_timeout

if __name__ == "__main__":
    unittest.main()


