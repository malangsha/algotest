import logging
import queue
import threading
import time
import traceback
from typing import Dict, List, Callable, Any, Optional, Type, Set
from datetime import datetime
from utils.constants import EventType, EventPriority, TimeframeEventType
from models.events import Event, EventType, EventValidationError, EventPriority, MarketDataEvent, BarEvent, OrderEvent, FillEvent, PositionEvent, SignalEvent, TimerEvent, SystemEvent, AccountEvent, RiskBreachEvent, ExecutionEvent
from collections import defaultdict
import uuid
import sys

class EventFlowMonitor:
    """
    Monitors event flow to detect breaks or issues in the event chain.
    """
    
    def __init__(self, event_manager):
        """
        Initialize the event flow monitor.
        
        Args:
            event_manager: The event manager to monitor
        """
        self.logger = logging.getLogger("core.event_flow_monitor")
        self.event_manager = event_manager
        
        # Track expected event relationships (what events should follow others)
        self.expected_flows = {
            EventType.SIGNAL: {EventType.ORDER},  # Signals should lead to Orders
            EventType.ORDER: {EventType.EXECUTION, EventType.FILL},  # Orders should lead to Execution and/or Fills
            EventType.FILL: {EventType.POSITION},  # Fills should lead to Position updates
            EventType.POSITION: {EventType.ACCOUNT},  # Position updates should lead to Account updates
            EventType.EXECUTION: {EventType.FILL},  # Executions should lead to Fills
            EventType.BAR: {EventType.SIGNAL},  # Bars should lead to Signals
            TimeframeEventType.BAR_1M: {EventType.SIGNAL},  # 1-minute bars should lead to Signals
            TimeframeEventType.BAR_5M: {EventType.SIGNAL},  # 5-minute bars should lead to Signals
            TimeframeEventType.BAR_15M: {EventType.SIGNAL},  # 15-minute bars should lead to Signals
            TimeframeEventType.BAR_30M: {EventType.SIGNAL},  # 30-minute bars should lead to Signals
            TimeframeEventType.BAR_1H: {EventType.SIGNAL},  # 1-hour bars should lead to Signals
            TimeframeEventType.BAR_4H: {EventType.SIGNAL},  # 4-hour bars should lead to Signals
            TimeframeEventType.BAR_1D: {EventType.SIGNAL}  # 1-day bars should lead to Signals
        }
        
        # Track actual events seen
        self.active_flow_ids = {}  # event_id -> {expected_events, timeout, created_at, metadata}
        
        # Keep a history of completed and broken chains for diagnosis
        self.completed_chains = []  # Stores last 20 successful chains
        self.broken_chains = []  # Stores last 20 broken chains
        
        # Timeouts for expected events (in seconds)
        self.timeouts = {
            EventType.SIGNAL: 5.0,
            EventType.ORDER: 5.0,
            EventType.FILL: 5.0,
            EventType.EXECUTION: 10.0,
            EventType.POSITION: 5.0,
            EventType.ACCOUNT: 5.0,
            EventType.BAR: 5.0,
            TimeframeEventType.BAR_1M: 5.0,
            TimeframeEventType.BAR_5M: 5.0,
            TimeframeEventType.BAR_15M: 5.0,
            TimeframeEventType.BAR_30M: 5.0,
            TimeframeEventType.BAR_1H: 5.0,
            TimeframeEventType.BAR_4H: 5.0,
            TimeframeEventType.BAR_1D: 5.0
        }
        
        # Statistics
        self.stats = {
            "broken_chains": 0,
            "complete_chains": 0,
            "active_chains": 0,
            "warnings": [],
            "chain_completion_times": [],  # List of times taken to complete chains
        }
        
        # Register with event manager to receive all events
        for event_type in EventType:
            event_manager.subscribe(event_type, self._on_event, component_name="EventFlowMonitor")
    
    def _on_event(self, event: Event):
        """
        Process an event through the monitor.
        
        Args:
            event: The event to process
        """
        try:
            # Extract basic info for tracking
            event_id = getattr(event, 'event_id', None)
            event_type = getattr(event, 'event_type', None)
            
            if not event_id or not event_type:
                self.logger.warning(f"Received event with missing ID or type: {event}")
                return
                
            # Collect metadata for diagnostics
            metadata = {}
            for attr in ['symbol', 'order_id', 'strategy_id', 'timestamp']:
                if hasattr(event, attr):
                    metadata[attr] = getattr(event, attr)
            
            # Check for invalid events - simulate a broken chain for invalid signals
            is_valid = True
            if event_type == EventType.SIGNAL:
                # Check for required fields in signal events
                required_fields = ['symbol', 'signal_type', 'side', 'price', 'quantity']
                missing_fields = [field for field in required_fields if not hasattr(event, field) or getattr(event, field) is None]
                if missing_fields:
                    self.logger.warning(f"Invalid signal event missing fields: {missing_fields}")
                    # Create a broken chain record for an invalid signal
                    warning = {
                        "timestamp": datetime.now(),
                        "message": f"Invalid signal event missing fields: {missing_fields}",
                        "flow_id": event_id,
                        "parent_event": event_type,
                        "missing_events": ["VALIDATION"],
                        "events_seen": [event_type],
                        "metadata": metadata
                    }
                    
                    self.broken_chains.append(warning)
                    if len(self.broken_chains) > 20:
                        self.broken_chains.pop(0)
                        
                    self.stats["broken_chains"] += 1
                    self.logger.warning(f"Invalid signal created broken chain: {warning['message']}")
                    is_valid = False
            
            # Start a new chain if this is a starting event (no parent) and is valid
            if is_valid and event_type in self.expected_flows:
                # Add expected follow-ups to tracking
                self.active_flow_ids[event_id] = {
                    "expected_events": self.expected_flows[event_type].copy(),
                    "timeout": time.time() + self.timeouts.get(event_type, 10.0),
                    "created_at": time.time(),
                    "parent_event": event_type,
                    "metadata": metadata,
                    "events_seen": [event_type]
                }
                self.stats["active_chains"] += 1
                
            # Check if this event fulfills any expected events in any chain
            for flow_id, flow_data in list(self.active_flow_ids.items()):
                # If this event is expected in this chain
                if event_type in flow_data["expected_events"]:
                    # Check if this event is related to this chain
                    # (e.g., order_id matches if it's an order or fill event)
                    is_related = True
                    
                    # Check for relationships between events
                    if hasattr(event, 'order_id') and 'order_id' in flow_data['metadata']:
                        if event.order_id != flow_data['metadata']['order_id']:
                            is_related = False
                    
                    # For symbol-based relationships
                    if hasattr(event, 'symbol') and 'symbol' in flow_data['metadata']:
                        if event.symbol != flow_data['metadata']['symbol']:
                            is_related = False
                    
                    if is_related:
                        # Add this event's metadata
                        flow_data["metadata"].update(metadata)
                        
                        # Track that we've seen this event type
                        flow_data["events_seen"].append(event_type)
                        
                        # Remove this event type from expected events
                        flow_data["expected_events"].remove(event_type)
                        
                        # If no more expected events, chain is complete
                        if not flow_data["expected_events"]:
                            completion_time = time.time() - flow_data["created_at"]
                            self.stats["chain_completion_times"].append(completion_time)
                            self.stats["complete_chains"] += 1
                            self.stats["active_chains"] -= 1
                            
                            # Store completed chain for diagnostics
                            completed_chain = {
                                "flow_id": flow_id,
                                "parent_event": flow_data["parent_event"],
                                "created_at": flow_data["created_at"],
                                "completed_at": time.time(),
                                "completion_time": completion_time,
                                "events_seen": flow_data["events_seen"],
                                "metadata": flow_data["metadata"]
                            }
                            
                            # Keep limited history
                            self.completed_chains.append(completed_chain)
                            if len(self.completed_chains) > 20:
                                self.completed_chains.pop(0)
                                
                            del self.active_flow_ids[flow_id]
                            self.logger.debug(f"Event chain completed in {completion_time:.2f} seconds: {flow_data['parent_event']} -> {flow_data['events_seen']}")
                        
            # Check for timed out chains
            current_time = time.time()
            for flow_id, flow_data in list(self.active_flow_ids.items()):
                if current_time > flow_data["timeout"]:
                    # Chain timed out
                    warning = {
                        "timestamp": datetime.now(),
                        "message": f"Event chain broken: {flow_data['parent_event']} didn't lead to {flow_data['expected_events']}",
                        "flow_id": flow_id,
                        "parent_event": flow_data["parent_event"],
                        "missing_events": list(flow_data["expected_events"]),
                        "events_seen": flow_data["events_seen"],
                        "metadata": flow_data["metadata"]
                    }
                    
                    # Keep limited history of warnings
                    self.stats["warnings"].append(warning)
                    if len(self.stats["warnings"]) > 20:
                        self.stats["warnings"].pop(0)
                        
                    # Store broken chain for diagnostics
                    self.broken_chains.append(warning)
                    if len(self.broken_chains) > 20:
                        self.broken_chains.pop(0)
                        
                    self.stats["broken_chains"] += 1
                    self.stats["active_chains"] -= 1
                    del self.active_flow_ids[flow_id]
                    
                    self.logger.warning(f"Event flow chain broken: {warning['message']} (Metadata: {warning['metadata']})")
        except Exception as e:
            self.logger.error(f"Error in event flow monitor: {e}")
            self.logger.error(traceback.format_exc())
    
    def get_broken_chains(self):
        """Get the history of broken chains for diagnostics"""
        return self.broken_chains
    
    def get_completed_chains(self):
        """Get the history of completed chains for diagnostics"""
        return self.completed_chains
    
    def get_active_chains(self):
        """Get the currently active event chains"""
        return self.active_flow_ids
    
    def get_stats(self):
        """Get statistics about event flow"""
        stats = self.stats.copy()
        
        # Calculate average completion time if we have data
        if stats["chain_completion_times"]:
            stats["avg_completion_time"] = sum(stats["chain_completion_times"]) / len(stats["chain_completion_times"])
        else:
            stats["avg_completion_time"] = 0
            
        # Remove the raw list of times from the stats
        stats.pop("chain_completion_times", None)
        
        return stats

class EventManager:
    """
    Central hub for event distribution in the algotrading framework.
    Uses both a queue mechanism for asynchronous processing and direct callbacks.
    """
    
    # Map event types to their priorities
    EVENT_PRIORITIES = {
        EventType.MARKET_DATA: EventPriority.NORMAL,
        EventType.ORDER: EventPriority.HIGH,
        EventType.TRADE: EventPriority.HIGH,
        EventType.POSITION: EventPriority.LOW,
        EventType.STRATEGY: EventPriority.HIGH,
        EventType.SYSTEM: EventPriority.LOW,
        EventType.SIGNAL: EventPriority.HIGH,
        EventType.FILL: EventPriority.HIGH,
        EventType.ACCOUNT: EventPriority.LOW,
        EventType.CUSTOM: EventPriority.HIGH,
        EventType.TIMER: EventPriority.LOW,
        EventType.RISK_BREACH: EventPriority.HIGH,
        EventType.EXECUTION: EventPriority.HIGH,
        EventType.BAR: EventPriority.NORMAL,
        TimeframeEventType.BAR_1M: EventPriority.NORMAL,
        TimeframeEventType.BAR_5M: EventPriority.NORMAL,
        TimeframeEventType.BAR_15M: EventPriority.NORMAL,
        TimeframeEventType.BAR_30M: EventPriority.NORMAL,
        TimeframeEventType.BAR_1H: EventPriority.NORMAL,
        TimeframeEventType.BAR_4H: EventPriority.NORMAL,
        TimeframeEventType.BAR_1D: EventPriority.NORMAL        
    }
    
    def __init__(self, queue_size: int = 5000, process_sleep: float = 0.001, enable_monitoring: bool = True):
        """
        Initialize the Event Manager.
        
        Args:
            queue_size: Maximum size of the event queue
            process_sleep: Sleep time between processing events (seconds)
            enable_monitoring: Whether to enable event flow monitoring
        """
        self.logger = logging.getLogger(__name__)
        
        # Event queue for asynchronous processing
        self.event_queue = queue.PriorityQueue(maxsize=queue_size)
        self._queue_size = queue_size  # Store for reference
        
        # Dict mapping event types to lists of subscriber callbacks
        self._subscribers: Dict[EventType, List[Callable[[Event], None]]] = {}
        for event_type in EventType:
            self._subscribers[event_type] = []
        
        # Track component registrations for debugging
        self._registered_components: Dict[str, List[EventType]] = {}
        
        # Processing thread
        self._processing_thread = None
        self._process_sleep = process_sleep
        self._is_running = False
        
        # Diagnostic counters for tracking event flow
        self._diag_publish_count = 0
        self._diag_process_count = 0
        self._diag_last_sample_time = time.time()
        self._diag_sample_interval = 5  # Log diagnostics every 5 seconds
        
        # Statistics
        self.stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_published_by_type": {event_type: 0 for event_type in EventType},
            "queue_overflow_count": 0,
            "max_queue_size": 0,
            "callbacks_executed": 0,
            "callback_errors": 0,
            "events_by_type": {event_type.value: 0 for event_type in EventType},
            "last_error": None,
            "last_error_time": None,
            "error_count": 0,
            "error_history": [],  # Keep a history of recent errors
            "validation_errors": 0,  # Track validation errors
            "retry_attempts": 0,  # Track retry attempts
            "retry_successes": 0,  # Track successful retries
        }
        
        # Event logger reference (will be set by register_event_logger)
        self.event_logger = None
        
        # Monitor for event flow
        self.flow_monitor = None
        if enable_monitoring:
            self.flow_monitor = EventFlowMonitor(self)

        # Retry queue for events that failed due to queue overflow
        self.retry_queue = queue.Queue(maxsize=queue_size)
        
        # Print event queue information        
        self.logger.info(f"Event queue initialized with max size: {self.event_queue.maxsize}")
        
        # Start diagnostic logging
        threading.Thread(target=self._diagnostic_logging_thread, daemon=True).start()
        
        # Start retry thread
        threading.Thread(target=self._retry_thread, daemon=True).start()

        self._enable_monitoring = enable_monitoring
        self._monitoring_thread = None
        self._stop_monitoring = False
        self._lock = threading.Lock()

    def start(self) -> bool:
        """
        Start the event processing thread.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self._is_running:
            self.logger.warning("Event manager is already running")
            return True
            
        try:
            self._is_running = True
            self._processing_thread = threading.Thread(target=self.process_events, daemon=True)
            self._processing_thread.start()
            self.logger.info("Event manager started")
            return True
        except Exception as e:
            self._is_running = False
            self.logger.error(f"Failed to start event manager: {str(e)}")
            return False

    def stop(self):
        """Stop the event processing thread."""
        if not self._is_running:
            self.logger.warning("Event manager is not running")
            return
            
        self._is_running = False
        
        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5.0)
        
        self.logger.info("Event manager stopped")

    def is_running(self) -> bool:
        """
        Check if the event manager is running.
        
        Returns:
            bool: True if the event manager is running, False otherwise
        """
        return self._is_running

    def _record_error(self, error_message, exception=None):
        """Record an error for monitoring and history"""
        self.stats["error_count"] += 1
        self.stats["last_error"] = error_message
        self.stats["last_error_time"] = datetime.now()
        
        error_entry = {
            "timestamp": datetime.now(),
            "message": error_message,
            "exception": str(exception) if exception else None,
            "traceback": traceback.format_exc() if exception else None
        }
        
        self.stats["error_history"].append(error_entry)
        # Keep only the last 20 errors
        if len(self.stats["error_history"]) > 20:
            self.stats["error_history"].pop(0)
    
    def subscribe(self, event_type: EventType, callback: Callable[[Event], None], component_name: str = None) -> None:
        """
        Subscribe to events of a specific type.
        
        Args:
            event_type: Type of event to subscribe to (from EventType enum)
            callback: Function to call when event occurs
            component_name: Optional name of the subscribing component for logging
        """
        try:
            # Convert string event type to enum if needed
            if isinstance(event_type, str):
                event_type = EventType[event_type]
                
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
                
            if callback not in self._subscribers[event_type]:
                self._subscribers[event_type].append(callback)
                self.logger.debug(f"Added subscriber for {event_type} events{f' from {component_name}' if component_name else ''}")
            else:
                self.logger.debug(f"Callback already subscribed to {event_type} events{f' from {component_name}' if component_name else ''}")

            if component_name:
                if component_name not in self._registered_components:
                    self._registered_components[component_name] = []
                if event_type not in self._registered_components[component_name]:
                    self._registered_components[component_name].append(event_type)    
                                    
        except (KeyError, ValueError) as e:
            self.logger.error(f"Invalid event type: {event_type}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error subscribing to {event_type}: {str(e)}")

    def register_event_logger(self, event_logger):
        """
        Register an event logger to track events more efficiently.
        
        Args:
            event_logger: EventLogger instance to register
            
        Returns:
            bool: True if registration was successful
        """
        if self.event_logger:
            self.logger.warning("An event logger is already registered, replacing it")
            
        self.event_logger = event_logger
        self.logger.info("EventLogger registered with EventManager")
        return True

    def publish(self, event: Event) -> bool:
        """
        Publish an event to the event queue for asynchronous processing.
        
        Args:
            event: Event to publish
            
        Returns:
            bool: True if event was successfully added to queue
        """
        if not hasattr(event, 'event_type'):
            error_msg = f"Invalid event object without event_type: {type(event)}"
            self.logger.error(error_msg)
            self._record_error(error_msg)
            return False
            
        # Validate event if it has a validate method
        try:
            if hasattr(event, 'validate'):
                event.validate()
        except EventValidationError as e:
            # Don't publish invalid events
            error_msg = f"Event validation error: {e} - event will not be published"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            self.stats["validation_errors"] += 1
            return False
        except Exception as e:
            error_msg = f"Error validating event: {e}"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            return False
            
        event_type_value = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
        self.logger.debug(f"Publishing event: {event.event_type}: {event_type_value}")
        
        try:
            # Set event priority based on its type
            event.priority = self.EVENT_PRIORITIES.get(event.event_type, EventPriority.NORMAL)
            priority_value = event.priority.value
            
            # Add to queue for asynchronous processing
            self.event_queue.put((priority_value, event), block=False)
            self.stats["events_published"] += 1
            self._diag_publish_count += 1  # Increment diagnostic counter
            
            # Update events by type counter for both string and enum value
            self.stats["events_by_type"][event_type_value] = self.stats["events_by_type"].get(event_type_value, 0) + 1
            
            # Also track by EventType enum for easier access
            if event.event_type in self.stats["events_published_by_type"]:
                self.stats["events_published_by_type"][event.event_type] += 1
            
            # Track max queue size
            current_size = self.event_queue.qsize()
            self.stats["max_queue_size"] = max(self.stats["max_queue_size"], current_size)
            
            # Log warning if queue is filling up (but not yet full)
            if current_size > self.event_queue.maxsize * 0.8:
                self.logger.warning(f"Event queue is at {current_size}/{self.event_queue.maxsize} capacity ({current_size/self.event_queue.maxsize*100:.1f}%)")
            
            if self._enable_monitoring:
                self.flow_monitor._on_event(event)
            
            return True
            
        except queue.Full:
            # Add to retry queue instead of dropping
            try:
                self.retry_queue.put(event, block=False)
                self.logger.warning(f"Event queue full, event added to retry queue: {event.event_type}")
                return True
            except queue.Full:
                # Both queues are full, now we have to drop
                self.stats["queue_overflow_count"] += 1
                self.logger.error(f"Event queue and retry queue full, event dropped: {event.event_type}")
                
                # Detailed diagnostic logging for queue overflow
                self.logger.error(f"QUEUE OVERFLOW: Queue size={self.event_queue.qsize()}/{self.event_queue.maxsize}, Events published={self.stats['events_published']}, Events processed={self.stats['events_processed']}")
                
                # Log subscriber count for this event type to help diagnose
                if event.event_type in self._subscribers:
                    subscriber_count = len(self._subscribers[event.event_type])
                    self.logger.error(f"Event type {event.event_type} has {subscriber_count} subscribers")
                
                return False
        except Exception as e:
            error_msg = f"Error publishing event: {str(e)}"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            return False

    def _retry_thread(self):
        """Thread that retries publishing events from the retry queue"""
        while True:
            try:
                # Sleep to avoid spinning
                time.sleep(0.1)
                
                # Check if there are events to retry
                if self.retry_queue.empty():
                    continue
                
                # Check if main queue has space
                if self.event_queue.full():
                    continue
                
                # Get event from retry queue
                event = self.retry_queue.get(block=False)
                self.stats["retry_attempts"] += 1
                
                # Try to add to main queue
                try:
                    self.event_queue.put(event, block=False)
                    self.stats["retry_successes"] += 1
                    self.logger.info(f"Successfully retried event: {event.event_type}")
                except queue.Full:
                    # Put back in retry queue
                    try:
                        self.retry_queue.put(event, block=False)
                    except queue.Full:
                        # Both queues are full, drop the event
                        self.stats["queue_overflow_count"] += 1
                        self.logger.error(f"Event dropped during retry: {event.event_type}")
                
                # Mark as done
                self.retry_queue.task_done()
                
            except queue.Empty:
                # No events in retry queue
                pass
            except Exception as e:
                self.logger.error(f"Error in retry thread: {str(e)}")
                self.logger.error(traceback.format_exc())

    def publish_sync(self, event: Event) -> int:
        """
        Publish an event synchronously, directly calling callbacks.
        
        Args:
            event: Event to publish
            
        Returns:
            int: Number of callbacks executed
        """
        if not isinstance(event, Event):
            error_msg = f"Invalid event object: {type(event)}"
            self.logger.error(error_msg)
            self._record_error(error_msg)
            return 0
            
        # Validate event if it has a validate method
        try:
            if hasattr(event, 'validate'):
                event.validate()
        except EventValidationError as e:
            # Don't process invalid events
            error_msg = f"Event validation error in sync publish: {e} - event will not be processed"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            self.stats["validation_errors"] += 1
            return 0
        except Exception as e:
            error_msg = f"Error validating event in sync publish: {e}"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            return 0
            
        # Directly dispatch to callbacks
        return self._dispatch_event(event)

    def process_events(self):
        """Main event processing loop."""
        self.logger.info("Event processing loop started")
        
        while self._is_running:
            try:
                # Process all available events
                processed = self._process_events(max_events=100)
                
                if processed == 0:
                    # If no events were processed, sleep briefly to avoid spinning
                    time.sleep(self._process_sleep)
                    
            except Exception as e:
                self.logger.error(f"Error in event processing loop: {str(e)}")
                self.logger.error(traceback.format_exc())
                self.stats["last_error"] = str(e)
                self.stats["last_error_time"] = datetime.now()
                time.sleep(self._process_sleep)
        
        self.logger.info("Event processing loop stopped")

    def _process_events(self, max_events: int = 100) -> int:
        """
        Process a batch of events from the queue.
        
        Args:
            max_events: Maximum number of events to process
            
        Returns:
            int: Number of events processed
        """
        processed = 0
        
        for _ in range(max_events):
            try:
                # Get event with a small timeout
                priority, event = self.event_queue.get(block=True, timeout=0.001)
                
                # Update event type statistics
                event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
                self.stats["events_by_type"][event_type] += 1
                
                # Update last event time
                if "last_event_times" not in self.stats:
                    self.stats["last_event_times"] = {}
                self.stats["last_event_times"][event_type] = datetime.now()
                
                # Dispatch event to subscribers
                callbacks_executed = self._dispatch_event(event)
                
                # Log if event processing is slow (taking too many callbacks)
                if callbacks_executed > 10:  # Arbitrary threshold for "many callbacks"
                    self.logger.warning(f"Event {event.event_type} processed by {callbacks_executed} callbacks")
                
                # Mark as done
                self.event_queue.task_done()
                processed += 1
                self.stats["events_processed"] += 1
                self._diag_process_count += 1  # Increment diagnostic counter
                
            except queue.Empty:
                # No more events in queue
                break
            except Exception as e:
                self.logger.error(f"Error processing event: {str(e)}")
                self.logger.error(traceback.format_exc())
                self.stats["last_error"] = str(e)
                self.stats["last_error_time"] = datetime.now()
                
        # Periodically log processing statistics if not making progress
        current_size = self.event_queue.qsize()
        if processed == 0 and current_size > 0:
            # No events processed but queue has events - could indicate processing issues
            self.logger.warning(f"Event processing stalled - queue has {current_size} events but none processed in this cycle")
            
        # Log if queue is growing rapidly
        if current_size > self.event_queue.maxsize * 0.5 and processed < max_events * 0.25:
            # Queue is over half full and we're processing at less than 25% capacity
            self.logger.warning(f"Event queue growing: {current_size}/{self.event_queue.maxsize} events, processed only {processed}/{max_events} max events")
            
        return processed

    def _dispatch_event(self, event: Event) -> int:
        """
        Dispatch an event to all subscribers.
        
        Args:
            event: Event to dispatch
            
        Returns:
            int: Number of callbacks executed
        """
        if not hasattr(event, 'event_type'):
            self.logger.error(f"Invalid event object without event_type: {type(event)}")
            return 0
            
        callbacks_executed = 0
        start_time = time.time()
        slow_callbacks = []

        # self.logger.debug(f"Dispatching {event.event_type} (type: {type(event.event_type)})")
        # subscribers = self._subscribers.get(event.event_type, [])
        # self.logger.debug(f"Subscribers for {event.event_type}: {len(subscribers)}")

        # Execute callbacks for this event type
        if event.event_type in self._subscribers:
            for callback in self._subscribers[event.event_type]:
                callback_start = time.time()
                try:
                    
                    # callback_name = callback.__qualname__ if hasattr(callback, "__qualname__") else str(callback)
                    # self.logger.debug(f"Executing callback: {callback_name}")
                    # Execute the callback
                    callback(event)
                    callbacks_executed += 1
                    self.stats["callbacks_executed"] += 1
                    
                    # Track slow callbacks
                    callback_time = time.time() - callback_start
                    if callback_time > 0.01:  # 10ms threshold for "slow" callback
                        callback_name = callback.__name__ if hasattr(callback, '__name__') else str(callback)
                        slow_callbacks.append((callback_name, callback_time))
                        
                except Exception as e:
                    self.stats["callback_errors"] += 1
                    error_msg = f"Error in event callback {callback}: {e}"
                    self.logger.error(error_msg)
                    self.logger.error(traceback.format_exc())
                    self._record_error(error_msg, e)
                    
        # Log if event dispatch was slow
        total_time = time.time() - start_time
        if total_time > 0.05:  # 50ms threshold for "slow" event dispatch
            self.logger.warning(f"Slow event dispatch: {event.event_type} took {total_time:.3f}s for {callbacks_executed} callbacks")
            if slow_callbacks:
                slow_str = ", ".join([f"{name}({time:.3f}s)" for name, time in slow_callbacks])
                self.logger.warning(f"Slow callbacks: {slow_str}")
                
        return callbacks_executed

    def get_subscribers(self, event_type: Optional[EventType] = None) -> Dict[str, List[EventType]]:
        """
        Get the list of subscribers, optionally filtered by event type.
        
        Args:
            event_type: Optional event type to filter by
            
        Returns:
            Dict[str, List[EventType]]: Component name -> list of event types
        """
        if event_type:
            return {
                component: types
                for component, types in self._registered_components.items()
                if event_type in types
            }
        else:
            return self._registered_components.copy()
    
    def get_flow_monitor_diagnostics(self):
        """
        Get diagnostic information from the event flow monitor
        
        Returns:
            dict: Flow monitor diagnostic information
        """
        if not self.flow_monitor:
            return {"status": "disabled"}
            
        return {
            "stats": self.flow_monitor.get_stats(),
            "active_chains": self.flow_monitor.get_active_chains(),
            "broken_chains": self.flow_monitor.get_broken_chains(),
            "completed_chains": self.flow_monitor.get_completed_chains()
        }
        
    def get_error_history(self):
        """
        Get the history of errors
        
        Returns:
            list: Recent error history
        """
        return self.stats.get("error_history", [])

    def get_subscriber_info(self):
        """
        Get detailed information about subscribers
        
        Returns:
            dict: Detailed subscriber information
        """
        subscriber_info = {}
        for event_type, subscribers in self._subscribers.items():
            event_type_value = event_type.value if hasattr(event_type, 'value') else str(event_type)
            subscriber_info[event_type_value] = {
                "count": len(subscribers),
                "components": []
            }
            
            # Find which components are listening for this event type
            for component, subscribed_types in self._registered_components.items():
                if event_type in subscribed_types:
                    subscriber_info[event_type_value]["components"].append(component)
                    
        return subscriber_info
                
    def get_stats(self, include_details: bool = False) -> Dict[str, Any]:
        """
        Get statistics about the event manager.
        
        Args:
            include_details: Whether to include detailed stats
        
        Returns:
            Dict[str, Any]: Dictionary of statistics
        """
        # Add current queue size to stats
        current_stats = self.stats.copy()
        current_stats['current_queue_size'] = self.event_queue.qsize()
        current_stats['current_retry_queue_size'] = self.retry_queue.qsize()
        
        # Remove detailed error history unless requested
        if not include_details:
            current_stats.pop('error_history', None)
        else:
            # Include subscriber information
            current_stats['subscribers'] = self.get_subscriber_info()
        
        # Add monitor stats if available
        if self.flow_monitor:
            current_stats['flow_monitor'] = self.flow_monitor.get_stats()
            
            if include_details:
                # Include recent broken chains
                current_stats['flow_monitor']['recent_broken_chains'] = self.flow_monitor.get_broken_chains()[-5:] if self.flow_monitor.get_broken_chains() else []
            
        return current_stats
    
    def reset_stats(self):
        """Reset statistics counters."""
        for event_type in EventType:
            self.stats["events_by_type"][event_type.value] = 0
            
        self.stats.update({
            "events_published": 0,
            "events_processed": 0,
            "queue_overflow_count": 0,
            "callbacks_executed": 0,
            "callback_errors": 0,
            "validation_errors": 0,
            "retry_attempts": 0,
            "retry_successes": 0
        })
        
        if self.flow_monitor:
            self.flow_monitor.stats.update({
                "broken_chains": 0,
                "complete_chains": 0,
                "warnings": []
            })

    def _diagnostic_logging_thread(self):
        """Thread function for logging diagnostic information"""
        while True:
            time.sleep(self._diag_sample_interval)
            current_time = time.time()
            elapsed_time = current_time - self._diag_last_sample_time
            
            # Calculate rates
            publish_rate = self._diag_publish_count / elapsed_time if elapsed_time > 0 else 0
            process_rate = self._diag_process_count / elapsed_time if elapsed_time > 0 else 0
            
            # Reset counters for the next sample
            self._diag_publish_count = 0
            self._diag_process_count = 0
            self._diag_last_sample_time = current_time
            
            # Log diagnostic information
            self.logger.info(f"Event queue size: {self.event_queue.qsize()}, Retry queue size: {self.retry_queue.qsize()}, Max queue size: {self.stats['max_queue_size']}, Publish rate: {publish_rate:.2f} events/s, Process rate: {process_rate:.2f} events/s")

    def print_event_statistics(self, include_components: bool = True, include_priorities: bool = True):
        """
        Print event statistics in a nicely formatted way.
        
        Args:
            include_components: Whether to include component registration details
            include_priorities: Whether to include event priorities
        """
        try:
            # Get subscriber information
            subscriber_info = self.get_subscriber_info()
            
            # Print header
            print("\n" + "="*80)
            print("EVENT MANAGER STATISTICS")
            print("="*80)
            
            # Print event priorities
            if include_priorities:
                print("\nEvent Priorities:")
                print("-"*40)
                print(f"{'Event Type':<20} | {'Priority':<10} | Description")
                print("-"*40)
                for event_type, priority in self.EVENT_PRIORITIES.items():
                    priority_name = priority.name if hasattr(priority, 'name') else str(priority)
                    print(f"{event_type.value:<20} | {priority_name:<10} | {self._get_priority_description(priority)}")
                print("-"*40)
            
            # Print component registrations
            if include_components:
                print("\nComponent Registrations:")
                print("-"*40)
                
                # First, collect all components and their events
                component_events = defaultdict(list)
                for event_type, info in subscriber_info.items():
                    for component in info["components"]:
                        component_events[component].append(event_type)
                
                if not component_events:
                    print("No components registered")
                else:
                    # Print in a nice format
                    for component, events in sorted(component_events.items()):
                        print(f"\nComponent: {component}")
                        print("Events:")
                        for event in sorted(events):
                            count = subscriber_info[event]["count"]
                            print(f"  - {event:<20} | Subscribers: {count}")
                
                print("-"*40)

                # Print event subscribers
                print("\nEvent Subscribers:")
                print("-"*40)
                
                # Filter out events with no subscribers
                events_with_subscribers = {et: info for et, info in subscriber_info.items() if info["components"]}
                
                if not events_with_subscribers:
                    print("No event subscribers")
                else:
                    # Create a lookup map for string event_type to enum
                    event_type_map = {}
                    for et in EventType:
                        event_type_map[et.value] = et
                        
                    # Also add TimeframeEventType values
                    try:
                        from utils.constants import TimeframeEventType
                        for et in TimeframeEventType:
                            event_type_map[et.value] = et
                    except (ImportError, AttributeError):
                        pass
                    
                    for event_type, info in sorted(events_with_subscribers.items()):
                        print(f"\nEvent: {event_type}")
                        print("Subscribers:")
                        for component in sorted(info["components"]):
                            # Get the actual callback functions for this event type
                            try:
                                # Look up the enum by its value in our map
                                enum_event_type = event_type_map.get(event_type)
                                if enum_event_type:
                                    callbacks = [cb for cb in self._subscribers.get(enum_event_type, []) 
                                            if hasattr(cb, '__name__') and cb.__name__ != '<lambda>']
                                    callback_names = [cb.__name__ for cb in callbacks]
                                    
                                    if callback_names:
                                        print(f"  - {component:<20} | Callbacks: {', '.join(callback_names)}")
                                    else:
                                        print(f"  - {component:<20} | Callbacks: <anonymous>")
                                else:
                                    print(f"  - {component:<20} | Callbacks: <no matching event type>")
                            except Exception as e:
                                # Handle any errors in callback retrieval
                                print(f"  - {component:<20} | Callbacks: <error: {str(e)}>")
                
                print("-"*40)
            
            # Print event counts
            print("\nEvent Counts:")
            print("-"*40)
            print(f"{'Event Type':<20} | {'Count':<10} | {'Last Event Time':<20}")
            print("-"*40)
            
            # Filter out events with zero count
            active_events = {et: count for et, count in self.stats["events_by_type"].items() if count > 0}
            
            if not active_events:
                print("No events processed yet")
            else:
                for event_type, count in sorted(active_events.items()):
                    last_time = self.stats.get("last_event_times", {}).get(event_type, "Never")
                    print(f"{event_type:<20} | {count:<10} | {last_time}")
            
            # Print queue statistics
            print("\nQueue Statistics:")
            print("-"*40)
            print(f"Current Queue Size: {self.event_queue.qsize()}/{self.event_queue.maxsize}")
            print(f"Max Queue Size: {self.stats['max_queue_size']}")
            print(f"Queue Overflows: {self.stats['queue_overflow_count']}")
            print(f"Retry Queue Size: {self.retry_queue.qsize()}")
            print(f"Retry Successes: {self.stats['retry_successes']}/{self.stats['retry_attempts']}")
            
            # Print error statistics
            print("\nError Statistics:")
            print("-"*40)
            print(f"Total Errors: {self.stats['error_count']}")
            print(f"Validation Errors: {self.stats['validation_errors']}")
            print(f"Callback Errors: {self.stats['callback_errors']}")
            
            if 'last_error' in self.stats and self.stats['last_error']:
                print(f"\nLast Error ({self.stats.get('last_error_time', 'Unknown')}):")
                print(f"  {self.stats['last_error']}")
            
            # Print flow monitor statistics if available
            if self.flow_monitor:
                flow_stats = self.flow_monitor.get_stats()
                print("\nEvent Flow Statistics:")
                print("-"*40)
                print(f"Broken Chains: {flow_stats.get('broken_chains', 0)}")
                print(f"Complete Chains: {flow_stats.get('complete_chains', 0)}")
                print(f"Active Chains: {flow_stats.get('active_chains', 0)}")
                if 'avg_completion_time' in flow_stats:
                    print(f"Average Chain Completion Time: {flow_stats['avg_completion_time']:.2f}s")
            
            print("\n" + "="*80)
        except Exception as e:
            self.logger.error(f"Error printing event statistics: {e}")
            import traceback
            traceback.print_exc()

    def _get_priority_description(self, priority):
        """Get a human-readable description of the priority level."""
        descriptions = {
            EventPriority.HIGH: "High priority - processed immediately",
            EventPriority.NORMAL: "Normal priority - processed in order",
            EventPriority.LOW: "Low priority - processed when system is idle"
        }
        return descriptions.get(priority, "Unknown priority level")
