#!/usr/bin/env python
"""
Unit tests for the event handling system.
This tests the basic functionality of the event manager.
"""

import unittest
import time
from datetime import datetime

from core.event_manager import EventManager
from models.events import Event, EventType

class EventManagerTest(unittest.TestCase):
    """Test the event manager's ability to handle events."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=False)
        self.event_manager.start()
        
        # Set up tracking for events received
        self.received_events = []
        
        # Register a simple listener
        self.event_manager.subscribe(
            EventType.CUSTOM,
            self._on_event,
            component_name="TestListener"
        )
        
        # Wait for initialization
        time.sleep(0.2)
        
    def tearDown(self):
        """Clean up after tests."""
        # Stop event manager
        self.event_manager.stop()
        time.sleep(0.2)  # Wait for threads to stop
    
    def _on_event(self, event):
        """Record received events."""
        self.received_events.append(event)
    
    def test_publish_event(self):
        """Test that events can be published and received."""
        # Create a custom event
        event = Event(
            event_type=EventType.CUSTOM,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        event.test_field = "test_value"
        
        # Publish the event
        self.event_manager.publish(event)
        
        # Wait for event processing
        time.sleep(0.5)
        
        # Check that event was received
        self.assertEqual(len(self.received_events), 1)
        
        # Check event fields
        received = self.received_events[0]
        self.assertEqual(received.event_type, EventType.CUSTOM)
        self.assertEqual(received.test_field, "test_value")
    
    def test_multiple_subscribers(self):
        """Test that multiple subscribers receive the same event."""
        # Set up a second tracker
        second_received = []
        
        # Register a second listener
        self.event_manager.subscribe(
            EventType.CUSTOM,
            lambda event: second_received.append(event),
            component_name="SecondTestListener"
        )
        
        # Create and publish event
        event = Event(
            event_type=EventType.CUSTOM,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        self.event_manager.publish(event)
        
        # Wait for event processing
        time.sleep(0.5)
        
        # Check both listeners received the event
        self.assertEqual(len(self.received_events), 1)
        self.assertEqual(len(second_received), 1)
    
    def test_event_type_filtering(self):
        """Test that subscribers only receive events they're subscribed to."""
        # Set up a second tracker for a different event type
        position_events = []
        
        # Register a listener for position events
        self.event_manager.subscribe(
            EventType.POSITION,
            lambda event: position_events.append(event),
            component_name="PositionTestListener"
        )
        
        # Create and publish different event types
        custom_event = Event(
            event_type=EventType.CUSTOM,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        position_event = Event(
            event_type=EventType.POSITION,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        
        self.event_manager.publish(custom_event)
        self.event_manager.publish(position_event)
        
        # Wait for event processing
        time.sleep(0.5)
        
        # Check correct events were received
        self.assertEqual(len(self.received_events), 1)
        self.assertEqual(len(position_events), 1)
        self.assertEqual(self.received_events[0].event_type, EventType.CUSTOM)
        self.assertEqual(position_events[0].event_type, EventType.POSITION)

if __name__ == "__main__":
    unittest.main() 