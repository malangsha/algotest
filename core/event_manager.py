import logging
import queue
import threading
import time
import traceback
from typing import Dict, List, Callable, Any, Optional, Type, Set
from datetime import datetime
from utils.constants import EventType, EventPriority, TimeframeEventType
from models.events import Event, EventValidationError, MarketDataEvent, BarEvent, OrderEvent, FillEvent, PositionEvent, SignalEvent, TimerEvent, SystemEvent, AccountEvent, RiskBreachEvent, ExecutionEvent
from collections import defaultdict
import uuid
import sys

# Ensure EventType and EventPriority are correctly imported if they are not part of models.events
# from utils.constants import EventType, EventPriority (if they live here)

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
            EventType.SIGNAL: {EventType.ORDER},
            EventType.ORDER: {EventType.EXECUTION, EventType.FILL},
            EventType.FILL: {EventType.POSITION},
            EventType.POSITION: {EventType.ACCOUNT},
            EventType.EXECUTION: {EventType.FILL}            
        }
        
        self.active_flow_ids: Dict[str, Dict[str, Any]] = {}  # event_id -> {expected_events, timeout, created_at, metadata, parent_event, events_seen}
        
        self.completed_chains: List[Dict[str, Any]] = []
        self.broken_chains: List[Dict[str, Any]] = []
        
        self.timeouts = {
            EventType.SIGNAL: 5.0,
            EventType.ORDER: 5.0, # Reduced from 10 based on logs showing issues sooner
            EventType.FILL: 5.0,
            EventType.EXECUTION: 10.0, # Increased as this might involve broker interaction
            EventType.POSITION: 5.0,
            EventType.ACCOUNT: 5.0            
        }
        
        self.stats = {
            "broken_chains": 0,
            "complete_chains": 0,
            "active_chains": 0,
            "warnings": [],
            "chain_completion_times": [],
        }
        
        for event_type_member in EventType:
            event_manager.subscribe(event_type_member, self._on_event, component_name="EventFlowMonitor")
    
    def _on_event(self, event: Event):
        """
        Process an event through the monitor.
        
        Args:
            event: The event to process
        """
        try:
            event_id = getattr(event, 'event_id', None)
            event_type = getattr(event, 'event_type', None)
            
            if not event_id or not event_type:
                self.logger.warning(f"Received event with missing ID or type: {event}")
                return
                
            metadata = {}
            for attr in ['symbol', 'order_id', 'strategy_id', 'timestamp']:
                if hasattr(event, attr):
                    metadata[attr] = getattr(event, attr)
            
            is_valid = True
            if event_type == EventType.SIGNAL:
                required_fields = ['symbol', 'signal_type', 'side', 'price', 'quantity']
                missing_fields = [field for field in required_fields if not hasattr(event, field) or getattr(event, field) is None]
                if missing_fields:
                    self.logger.warning(f"Invalid signal event missing fields: {missing_fields}")
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
                    if len(self.broken_chains) > 20: self.broken_chains.pop(0)
                    self.stats["broken_chains"] += 1
                    is_valid = False
            
            if is_valid and event_type in self.expected_flows:
                if event_id not in self.active_flow_ids: # Start new chain only if not already active
                    self.active_flow_ids[event_id] = {
                        "expected_events": self.expected_flows[event_type].copy(),
                        "timeout": time.time() + self.timeouts.get(event_type, 10.0),
                        "created_at": time.time(),
                        "parent_event": event_type,
                        "metadata": metadata,
                        "events_seen": [event_type]
                    }
                    self.stats["active_chains"] = len(self.active_flow_ids) # More accurate count
                # else:
                #     self.logger.debug(f"EventFlowMonitor: Event {event_id} ({event_type}) is already part of an active flow. Not starting a new chain.")


            # Check if this event fulfills any expected events in any chain
            # Iterate over a copy of items for safe modification
            for flow_id, flow_data in list(self.active_flow_ids.items()):
                if event_type in flow_data["expected_events"]:
                    is_related = True
                    # Check relationship based on order_id if present in both event and flow metadata
                    event_order_id = getattr(event, 'order_id', None)
                    flow_order_id = flow_data['metadata'].get('order_id', None)

                    if event_order_id and flow_order_id and event_order_id != flow_order_id:
                        # This event has an order_id, but it doesn't match the flow's order_id.
                        # This means it's not related to *this specific* flow, even if types match.
                        is_related = False
                    
                    # Additional check: if the current event_id is the flow_id itself, it's the starting event.
                    # This should not remove itself from expected_events unless it's a self-fulfilling event type (rare).
                    # The current logic adds the event_type to events_seen and removes from expected_events.
                    # This is generally fine if the event is a successor.

                    if is_related:
                        flow_data["metadata"].update(metadata)
                        flow_data["events_seen"].append(event_type)
                        flow_data["expected_events"].remove(event_type)
                        
                        if not flow_data["expected_events"]: # Chain complete
                            completion_time = time.time() - flow_data["created_at"]
                            self.stats["chain_completion_times"].append(completion_time)
                            self.stats["complete_chains"] += 1
                            
                            completed_chain = {
                                "flow_id": flow_id, "parent_event": flow_data["parent_event"],
                                "created_at": flow_data["created_at"], "completed_at": time.time(),
                                "completion_time": completion_time, "events_seen": flow_data["events_seen"],
                                "metadata": flow_data["metadata"]
                            }
                            self.completed_chains.append(completed_chain)
                            if len(self.completed_chains) > 20: self.completed_chains.pop(0)
                            
                            # *** FIX FOR KeyError START ***
                            if flow_id in self.active_flow_ids:
                                del self.active_flow_ids[flow_id]
                            # *** FIX FOR KeyError END ***
                            self.stats["active_chains"] = len(self.active_flow_ids)
                            self.logger.debug(f"Event chain completed for flow_id {flow_id} in {completion_time:.2f} seconds: {flow_data['parent_event']} -> {flow_data['events_seen']}")
            
            # Check for timed out chains
            current_time = time.time()
            for flow_id, flow_data in list(self.active_flow_ids.items()): # Iterate on a copy
                if current_time > flow_data["timeout"]:
                    warning_msg = f"Event chain broken for flow_id {flow_id}: {flow_data['parent_event']} didn't lead to {flow_data['expected_events']}"
                    warning = {
                        "timestamp": datetime.now(), "message": warning_msg,
                        "flow_id": flow_id, "parent_event": flow_data["parent_event"],
                        "missing_events": list(flow_data["expected_events"]),
                        "events_seen": flow_data["events_seen"], "metadata": flow_data["metadata"]
                    }
                    
                    self.stats["warnings"].append(warning)
                    if len(self.stats["warnings"]) > 20: self.stats["warnings"].pop(0)
                    self.broken_chains.append(warning)
                    if len(self.broken_chains) > 20: self.broken_chains.pop(0)
                        
                    self.stats["broken_chains"] += 1
                    
                    # *** FIX FOR KeyError START ***
                    if flow_id in self.active_flow_ids:
                        del self.active_flow_ids[flow_id]
                    # *** FIX FOR KeyError END ***
                    self.stats["active_chains"] = len(self.active_flow_ids)
                    self.logger.warning(f"{warning_msg} (Metadata: {warning['metadata']})")
        
        except Exception as e:
            self.logger.error(f"Error in event flow monitor _on_event for event_id {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            # self.logger.error(traceback.format_exc()) # exc_info=True does this

    def get_broken_chains(self):
        return self.broken_chains
    
    def get_completed_chains(self):
        return self.completed_chains
    
    def get_active_chains(self):
        return self.active_flow_ids
    
    def get_stats(self):
        stats_copy = self.stats.copy()
        if stats_copy["chain_completion_times"]:
            stats_copy["avg_completion_time"] = sum(stats_copy["chain_completion_times"]) / len(stats_copy["chain_completion_times"])
        else:
            stats_copy["avg_completion_time"] = 0
        stats_copy.pop("chain_completion_times", None)
        stats_copy["active_chains"] = len(self.active_flow_ids) # Ensure active_chains is current
        return stats_copy

class EventManager:
    EVENT_PRIORITIES = {
        EventType.MARKET_DATA: EventPriority.NORMAL,
        EventType.ORDER: EventPriority.HIGH,
        # EventType.TRADE: EventPriority.HIGH, # TRADE is not a standard EventType in provided enums, might be custom or part of Fill
        EventType.POSITION: EventPriority.LOW,
        # EventType.STRATEGY: EventPriority.HIGH, # STRATEGY is not standard, perhaps SystemEvent covers this
        EventType.SYSTEM: EventPriority.LOW,
        EventType.SIGNAL: EventPriority.HIGH,
        EventType.FILL: EventPriority.HIGH,
        EventType.ACCOUNT: EventPriority.LOW,
        EventType.CUSTOM: EventPriority.HIGH, # Assuming CUSTOM is a valid EventType
        EventType.TIMER: EventPriority.LOW,
        EventType.RISK_BREACH: EventPriority.HIGH,
        EventType.EXECUTION: EventPriority.HIGH,
        EventType.BAR: EventPriority.NORMAL
    }
    
    def __init__(self, queue_size: int = 5000, process_sleep: float = 0.001, enable_monitoring: bool = True):
        self.logger = logging.getLogger("core.event_manager") # Changed from __name__ to "core.event_manager" for consistency with logs
        
        # Initialize the lock early
        self._lock = threading.Lock()

        self.event_queue = queue.PriorityQueue(maxsize=queue_size)
        self._queue_size = queue_size
        
        self._subscribers: Dict[EventType, List[Callable[[Event], None]]] = defaultdict(list)
        
        self._registered_components: Dict[str, List[EventType]] = defaultdict(list)
        
        self._processing_thread = None
        self._process_sleep = process_sleep
        self._is_running = False
        
        self._diag_publish_count = 0
        self._diag_process_count = 0
        self._diag_last_sample_time = time.time()
        self._diag_sample_interval = 5
        
        self.stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_published_by_type": {event_type: 0 for event_type in EventType},
            "queue_overflow_count": 0,
            "max_queue_size_reached": 0, # Renamed from max_queue_size to avoid confusion with current_max
            "callbacks_executed": 0,
            "callback_errors": 0,
            "events_by_type": {event_type.value: 0 for event_type in EventType}, # Assuming .value is string
            "last_error": None,
            "last_error_time": None,
            "error_count": 0,
            "error_history": [],
            "validation_errors": 0,
            "retry_attempts": 0,
            "retry_successes": 0,
        }
        
        self.event_logger = None
        
        self._enable_monitoring = enable_monitoring # Store the parameter
        self.flow_monitor = None
        if self._enable_monitoring: # Use the stored parameter
            self.flow_monitor = EventFlowMonitor(self)

        self.retry_queue = queue.Queue(maxsize=queue_size)
        
        self.logger.info(f"Event queue initialized with max size: {self.event_queue.maxsize}")
        
        threading.Thread(target=self._diagnostic_logging_thread, daemon=True, name="EventManagerDiagnosticsThread").start()
        threading.Thread(target=self._retry_thread, daemon=True, name="EventManagerRetryThread").start()

    def start(self) -> bool:
        if self._is_running:
            self.logger.warning("Event manager is already running")
            return True
        try:
            self._is_running = True
            self._processing_thread = threading.Thread(target=self.process_events, daemon=True, name="EventManagerProcessingThread")
            self._processing_thread.start()
            self.logger.info("Event manager started")
            return True
        except Exception as e:
            self._is_running = False
            self.logger.error(f"Failed to start event manager: {str(e)}", exc_info=True)
            return False

    def stop(self):
        if not self._is_running:
            self.logger.warning("Event manager is not running")
            return
        self._is_running = False
        if self._processing_thread and self._processing_thread.is_alive():
            try:
                self._processing_thread.join(timeout=5.0)
                if self._processing_thread.is_alive():
                    self.logger.warning("Event processing thread did not stop in time.")
            except Exception as e:
                self.logger.error(f"Error stopping processing thread: {e}", exc_info=True)

        self.logger.info("Event manager stopped")

    def is_running(self) -> bool:
        return self._is_running

    def _record_error(self, error_message, exception=None):
        self.stats["error_count"] += 1
        self.stats["last_error"] = error_message
        self.stats["last_error_time"] = datetime.now()
        error_entry = {
            "timestamp": datetime.now(), "message": error_message,
            "exception": str(exception) if exception else None,
            "traceback": traceback.format_exc() if exception else None
        }
        self.stats["error_history"].append(error_entry)
        if len(self.stats["error_history"]) > 20: self.stats["error_history"].pop(0)
    
    def subscribe(self, event_type: EventType, callback: Callable[[Event], None], component_name: str = None) -> None:
        try:
            if not isinstance(event_type, EventType):
                # Attempt to convert if it's a string representation of EventType enum member
                try:
                    event_type = EventType[str(event_type).upper()]
                except KeyError:
                    self.logger.error(f"Invalid event type string: {event_type}. Cannot subscribe.")
                    return

            with self._lock: # Ensure thread-safe modification of subscribers
                if callback not in self._subscribers[event_type]:
                    self._subscribers[event_type].append(callback)
                    self.logger.debug(f"Added subscriber for {event_type.name} events{f' from {component_name}' if component_name else ''}")
                else:
                    self.logger.debug(f"Callback already subscribed to {event_type.name} events{f' from {component_name}' if component_name else ''}")

                if component_name:
                    if event_type not in self._registered_components[component_name]:
                        self._registered_components[component_name].append(event_type)    
                                    
        except (KeyError, ValueError) as e: # ValueError for invalid enum conversion
            self.logger.error(f"Invalid event type for subscription: {event_type}: {str(e)}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error subscribing to {event_type}: {str(e)}", exc_info=True)

    def register_event_logger(self, event_logger):
        if self.event_logger:
            self.logger.warning("An event logger is already registered, replacing it")
        self.event_logger = event_logger
        self.logger.info("EventLogger registered with EventManager")
        return True

    def publish(self, event: Event) -> bool:
        if not hasattr(event, 'event_type') or not isinstance(event.event_type, EventType):
            error_msg = f"Invalid event object: missing or invalid event_type. Event: {type(event)}, Data: {str(event)[:100]}"
            self.logger.error(error_msg)
            self._record_error(error_msg)
            return False
            
        try:
            if hasattr(event, 'validate') and callable(event.validate):
                event.validate()
        except EventValidationError as e:
            error_msg = f"Event validation error: {e} - event will not be published. Event: {event}"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            self.stats["validation_errors"] += 1
            return False
        except Exception as e: # Catch other validation errors
            error_msg = f"Unexpected error during event validation: {e}. Event: {event}"
            self.logger.error(error_msg, exc_info=True)
            self._record_error(error_msg, e)
            return False
            
        event_type_value = event.event_type.value
        
        try:
            # Set event_id if not already set
            if not getattr(event, 'event_id', None):
                event.event_id = str(uuid.uuid4())
            # Set timestamp if not already set
            if not getattr(event, 'timestamp', None):
                event.timestamp = time.time()

            event_priority_enum = self.EVENT_PRIORITIES.get(event.event_type, EventPriority.NORMAL)
            priority_value = event_priority_enum.value
            
            # Use getattr for priority on event, as it might not always be pre-set
            event_specific_priority = getattr(event, 'priority', None)
            if isinstance(event_specific_priority, EventPriority):
                 priority_value = event_specific_priority.value
            elif event_specific_priority is not None: # If it's an int already
                 priority_value = int(event_specific_priority)


            self.event_queue.put((priority_value, event), block=False)
            self.stats["events_published"] += 1
            self._diag_publish_count += 1
            
            self.stats["events_by_type"][event_type_value] = self.stats["events_by_type"].get(event_type_value, 0) + 1
            if event.event_type in self.stats["events_published_by_type"]:
                self.stats["events_published_by_type"][event.event_type] += 1
            
            current_size = self.event_queue.qsize()
            self.stats["max_queue_size_reached"] = max(self.stats["max_queue_size_reached"], current_size)
            
            if current_size > self.event_queue.maxsize * 0.8 and current_size != self.event_queue.maxsize : # Avoid log when full as retry handles it
                self.logger.warning(f"Event queue is at {current_size}/{self.event_queue.maxsize} capacity ({current_size/self.event_queue.maxsize*100:.1f}%)")
            
            # Pass to flow monitor if enabled and it's a monitored type
            if self.flow_monitor and event.event_type in self.flow_monitor.expected_flows:
                 self.flow_monitor._on_event(event) # Pass to flow monitor if it's a starting event of a flow
            elif self.flow_monitor: # Or if it's any event that could complete a flow
                 # Check if this event type can be part of any flow completion
                 can_complete_flow = any(event.event_type in expected_set for expected_set in self.flow_monitor.expected_flows.values())
                 if can_complete_flow:
                      self.flow_monitor._on_event(event)

            return True
            
        except queue.Full:
            try:
                self.retry_queue.put(event, block=False)
                self.logger.warning(f"Event queue full, event added to retry queue: {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')})")
                return True # Considered published to retry queue
            except queue.Full:
                self.stats["queue_overflow_count"] += 1
                self.logger.error(f"Event queue AND retry queue full, event dropped: {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')})")
                self.logger.error(f"QUEUE OVERFLOW: MainQ={self.event_queue.qsize()}/{self.event_queue.maxsize}, RetryQ={self.retry_queue.qsize()}/{self.retry_queue.maxsize}, Pub={self.stats['events_published']}, Proc={self.stats['events_processed']}")
                if event.event_type in self._subscribers:
                    self.logger.error(f"Event type {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} has {len(self._subscribers[event.event_type])} subscribers")
                return False
        except Exception as e:
            error_msg = f"Error publishing event {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')}): {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self._record_error(error_msg, e)
            return False

    def _retry_thread(self):
        while self._is_running: # Check if manager is running to allow graceful shutdown
            try:
                event_to_retry = self.retry_queue.get(timeout=1) # Timeout to allow checking _is_running
                self.stats["retry_attempts"] += 1
                
                # Try to add to main queue
                try:
                    event_priority_enum = self.EVENT_PRIORITIES.get(event_to_retry.event_type, EventPriority.NORMAL)
                    priority_value = event_priority_enum.value
                    # Check for specific priority on the event itself
                    event_specific_priority = getattr(event_to_retry, 'priority', None)
                    if isinstance(event_specific_priority, EventPriority):
                        priority_value = event_specific_priority.value
                    elif event_specific_priority is not None:
                        priority_value = int(event_specific_priority)

                    self.event_queue.put((priority_value, event_to_retry), block=False) # Non-blocking put
                    self.stats["retry_successes"] += 1
                    self.logger.info(f"Successfully retried event: {event_to_retry.event_type.name if hasattr(event_to_retry.event_type, 'name') else event_to_retry.event_type} (ID: {getattr(event_to_retry, 'event_id', 'N/A')})")
                except queue.Full:
                    # Put back in retry queue if main is still full
                    try:
                        self.retry_queue.put(event_to_retry, block=False) # Non-blocking put back
                        self.logger.warning(f"Main event queue still full during retry for {event_to_retry.event_type.name if hasattr(event_to_retry.event_type, 'name') else event_to_retry.event_type}. Event returned to retry queue.")
                    except queue.Full:
                        self.stats["queue_overflow_count"] += 1 # Counts as overflow if retry queue is also full on put back
                        self.logger.error(f"Event dropped during retry (both queues full): {event_to_retry.event_type.name if hasattr(event_to_retry.event_type, 'name') else event_to_retry.event_type} (ID: {getattr(event_to_retry, 'event_id', 'N/A')})")
                finally:
                    self.retry_queue.task_done() # Mark task as done from the initial get
                
            except queue.Empty:
                # No events in retry queue, or timeout occurred, loop continues if _is_running
                pass
            except Exception as e:
                self.logger.error(f"Error in retry thread: {str(e)}", exc_info=True)
                time.sleep(0.1) # Brief pause on error before retrying loop
        self.logger.info("EventManager retry thread stopped.")


    def publish_sync(self, event: Event) -> int:
        if not isinstance(event, Event) or not hasattr(event, 'event_type') or not isinstance(event.event_type, EventType):
            error_msg = f"Invalid event object for sync publish: {type(event)}"
            self.logger.error(error_msg)
            self._record_error(error_msg)
            return 0
            
        try:
            if hasattr(event, 'validate') and callable(event.validate):
                event.validate()
        except EventValidationError as e:
            error_msg = f"Event validation error in sync publish: {e} - event will not be processed. Event: {event}"
            self.logger.error(error_msg)
            self._record_error(error_msg, e)
            self.stats["validation_errors"] += 1
            return 0
        except Exception as e:
            error_msg = f"Error validating event in sync publish: {e}. Event: {event}"
            self.logger.error(error_msg, exc_info=True)
            self._record_error(error_msg, e)
            return 0
        
        # Set event_id and timestamp if not present
        if not getattr(event, 'event_id', None): event.event_id = str(uuid.uuid4())
        if not getattr(event, 'timestamp', None): event.timestamp = time.time()

        # Pass to flow monitor if applicable
        if self.flow_monitor and event.event_type in self.flow_monitor.expected_flows:
            self.flow_monitor._on_event(event)
        elif self.flow_monitor:
             can_complete_flow = any(event.event_type in expected_set for expected_set in self.flow_monitor.expected_flows.values())
             if can_complete_flow: self.flow_monitor._on_event(event)

        return self._dispatch_event(event)

    def process_events(self):
        self.logger.info("Event processing loop started")
        while self._is_running:
            try:
                processed_count = self._process_batch_of_events(max_events=100) # Process in batches
                if processed_count == 0:
                    time.sleep(self._process_sleep) # Sleep only if no events were processed
            except Exception as e:
                self.logger.error(f"Error in event processing loop: {str(e)}", exc_info=True)
                self._record_error(f"Event loop error: {str(e)}", e)
                time.sleep(1) # Longer sleep on major loop error
        self.logger.info("Event processing loop stopped")

    def _process_batch_of_events(self, max_events: int = 100) -> int:
        processed_in_batch = 0
        for _ in range(max_events):
            try:
                # Get event with a small timeout to prevent blocking indefinitely if queue becomes empty
                priority, event = self.event_queue.get(block=True, timeout=0.001) 
                
                event_type_val = event.event_type.value
                # self.stats["events_by_type"][event_type_val] = self.stats["events_by_type"].get(event_type_val, 0) + 1 # Already counted in publish
                
                if "last_event_times" not in self.stats: self.stats["last_event_times"] = {}
                self.stats["last_event_times"][event_type_val] = datetime.now()
                
                callbacks_executed = self._dispatch_event(event)
                
                if callbacks_executed > 10:
                    self.logger.warning(f"Event {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')}) processed by {callbacks_executed} callbacks")
                
                self.event_queue.task_done()
                processed_in_batch += 1
                self.stats["events_processed"] += 1
                self._diag_process_count += 1
                
            except queue.Empty:
                break # No more events in queue for this batch
            except Exception as e:
                # Log error for specific event processing, but continue batch if possible
                self.logger.error(f"Error processing event (ID: {getattr(event, 'event_id', 'N/A')} if available): {str(e)}", exc_info=True)
                self._record_error(f"Event processing error: {str(e)}", e)
                if 'event' in locals() and hasattr(event, 'event_type'): # Check if event is defined
                    self.logger.error(f"Failed event details: Type={event.event_type.name if hasattr(event.event_type, 'name') else event.event_type}, Content={str(event)[:200]}")
                # self.event_queue.task_done() # Mark as done even if error to prevent stall, or implement dead-letter queue
        
        current_q_size = self.event_queue.qsize()
        if processed_in_batch == 0 and current_q_size > 0:
            self.logger.warning(f"Event processing may be stalled - queue has {current_q_size} events but none processed in this cycle.")
        elif current_q_size > self._queue_size * 0.5 and processed_in_batch < max_events * 0.25:
            self.logger.warning(f"Event queue growing: {current_q_size}/{self._queue_size} events, processed only {processed_in_batch}/{max_events} in batch.")
            
        return processed_in_batch

    def _dispatch_event(self, event: Event) -> int:
        if not hasattr(event, 'event_type') or not isinstance(event.event_type, EventType): # Should be caught by publish
            self.logger.error(f"Invalid event dispatched: {type(event)}")
            return 0
            
        callbacks_executed = 0
        start_time = time.time()
        slow_callbacks = []

        # Use a lock if _subscribers can be modified by other threads (e.g. subscribe/unsubscribe)
        # However, iterating over a list copy is safer if modifications can happen during iteration.
        # For now, assuming subscribe is less frequent than dispatch.
        # A read-write lock would be ideal if performance is critical.
        with self._lock: # Lock to prevent modification of _subscribers during iteration
            subscribers_for_event = list(self._subscribers.get(event.event_type, [])) # Create a copy

        if not subscribers_for_event:
            # self.logger.debug(f"No subscribers for event type {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')})")
            return 0
            
        for callback in subscribers_for_event:
            callback_start_time = time.time()
            try:
                callback(event)
                callbacks_executed += 1
                self.stats["callbacks_executed"] += 1
                
                callback_duration = time.time() - callback_start_time
                if callback_duration > 0.01: # 10ms threshold
                    callback_name = getattr(callback, '__qualname__', getattr(callback, '__name__', str(callback)))
                    slow_callbacks.append((callback_name, callback_duration))
                    
            except Exception as e:
                self.stats["callback_errors"] += 1
                callback_name_for_log = getattr(callback, '__qualname__', getattr(callback, '__name__', str(callback)))
                error_msg = f"Error in event callback {callback_name_for_log} for event {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')}): {e}"
                self.logger.error(error_msg, exc_info=True)
                self._record_error(error_msg, e)
                    
        total_dispatch_time = time.time() - start_time
        if total_dispatch_time > 0.05: # 50ms threshold
            self.logger.warning(f"Slow event dispatch: {event.event_type.name if hasattr(event.event_type, 'name') else event.event_type} (ID: {getattr(event, 'event_id', 'N/A')}) took {total_dispatch_time:.3f}s for {callbacks_executed} callbacks.")
            if slow_callbacks:
                slow_str = ", ".join([f"{name}({cb_time:.3f}s)" for name, cb_time in slow_callbacks])
                self.logger.warning(f"Slow callbacks during dispatch: {slow_str}")
                
        return callbacks_executed

    def get_subscribers(self, event_type: Optional[EventType] = None) -> Dict[str, List[EventType]]:
        with self._lock: # Access _registered_components safely
            if event_type:
                return {
                    component: types
                    for component, types in self._registered_components.items()
                    if event_type in types
                }
            else:
                return self._registered_components.copy() # Return a copy
    
    def get_flow_monitor_diagnostics(self):
        if not self.flow_monitor: return {"status": "disabled"}
        return {
            "stats": self.flow_monitor.get_stats(),
            "active_chains": self.flow_monitor.get_active_chains(),
            "broken_chains": self.flow_monitor.get_broken_chains(),
            "completed_chains": self.flow_monitor.get_completed_chains()
        }
        
    def get_error_history(self):
        return list(self.stats.get("error_history", [])) # Return a copy

    def get_subscriber_info(self):
        subscriber_info = {}
        with self._lock: # Access _subscribers and _registered_components safely
            for event_type, subscribers_list in self._subscribers.items():
                event_type_value = event_type.value if hasattr(event_type, 'value') else str(event_type)
                subscriber_info[event_type_value] = {
                    "count": len(subscribers_list),
                    "components": []
                }
                for component, subscribed_types_list in self._registered_components.items():
                    if event_type in subscribed_types_list:
                        subscriber_info[event_type_value]["components"].append(component)
        return subscriber_info
                
    def get_stats(self, include_details: bool = False) -> Dict[str, Any]:
        current_stats = self.stats.copy() # Make a copy to avoid modifying original during processing
        current_stats['current_queue_size'] = self.event_queue.qsize()
        current_stats['current_retry_queue_size'] = self.retry_queue.qsize()
        
        if not include_details:
            current_stats.pop('error_history', None)
        else:
            current_stats['subscribers'] = self.get_subscriber_info()
            if 'error_history' in current_stats: # Ensure it's a copy if included
                 current_stats['error_history'] = list(current_stats['error_history'])

        if self.flow_monitor:
            flow_stats = self.flow_monitor.get_stats()
            current_stats['flow_monitor'] = flow_stats
            if include_details:
                broken_chains_history = self.flow_monitor.get_broken_chains()
                current_stats['flow_monitor']['recent_broken_chains'] = list(broken_chains_history[-5:]) if broken_chains_history else []
        return current_stats
    
    def reset_stats(self):
        with self._lock: # Ensure thread-safe reset
            for event_type in EventType:
                if hasattr(event_type, 'value'):
                    self.stats["events_by_type"][event_type.value] = 0
            
            self.stats.update({
                "events_published": 0, "events_processed": 0,
                "queue_overflow_count": 0, "callbacks_executed": 0,
                "callback_errors": 0, "validation_errors": 0,
                "retry_attempts": 0, "retry_successes": 0,
                "max_queue_size_reached": 0, "error_count": 0,
                "last_error": None, "last_error_time": None,
                "error_history": []
            })
            # Reset published_by_type correctly
            self.stats["events_published_by_type"] = {et: 0 for et in EventType}

            if self.flow_monitor:
                self.flow_monitor.stats.update({
                    "broken_chains": 0, "complete_chains": 0, "active_chains":0,
                    "warnings": [], "chain_completion_times": []
                })
            self.logger.info("EventManager statistics reset.")


    def _diagnostic_logging_thread(self):
        while self._is_running: # Check _is_running for graceful shutdown
            # Use a longer sleep if not running to avoid tight loop before start/after stop
            if not self._is_running:
                time.sleep(1)
                continue

            time.sleep(self._diag_sample_interval)
            # Ensure thread safety for diagnostic counters if they could be accessed elsewhere,
            # though they are primarily updated by publish and process_batch which run in specific threads.
            # A lock might be overkill if publish/process are single-threaded or internally synchronized for these counters.
            # For simplicity, assuming direct access is okay for these diagnostic-only counters.
            current_time = time.time()
            elapsed_time = current_time - self._diag_last_sample_time
            
            if elapsed_time > 0:
                publish_rate = self._diag_publish_count / elapsed_time
                process_rate = self._diag_process_count / elapsed_time
                
                self._diag_publish_count = 0 # Reset for next interval
                self._diag_process_count = 0
                self._diag_last_sample_time = current_time
                
                self.logger.info(
                    f"EventManager Diag: MainQ={self.event_queue.qsize()}, RetryQ={self.retry_queue.qsize()}, "
                    f"MaxQReached={self.stats.get('max_queue_size_reached',0)}, "
                    f"PubRate={publish_rate:.2f} evt/s, ProcRate={process_rate:.2f} evt/s"
                )
            else: # Avoid division by zero if interval is too short or time hasn't advanced
                self._diag_last_sample_time = current_time # Reset time to prevent large elapsed_time on next run

        self.logger.info("EventManager diagnostic logging thread stopped.")


    def print_event_statistics(self, include_components: bool = True, include_priorities: bool = True):
        try:
            subscriber_info = self.get_subscriber_info()
            
            print("\n" + "="*80 + "\nEVENT MANAGER STATISTICS\n" + "="*80)
            
            if include_priorities:
                print("\nEvent Priorities:\n" + "-"*60 + f"\n{'Event Type':<25} | {'Priority':<15} | Description\n" + "-"*60)
                for event_type, priority in self.EVENT_PRIORITIES.items():
                    priority_name = priority.name if hasattr(priority, 'name') else str(priority)
                    event_type_name = event_type.name if hasattr(event_type, 'name') else str(event_type)
                    print(f"{event_type_name:<25} | {priority_name:<15} | {self._get_priority_description(priority)}")
                print("-"*60)
            
            if include_components:
                print("\nComponent Registrations:\n" + "-"*60)
                component_events = defaultdict(list)
                with self._lock: # Access _registered_components safely
                    for component, types in self._registered_components.items():
                        for et in types:
                             component_events[component].append(et.name if hasattr(et, 'name') else str(et))
                
                if not component_events: print("No components registered")
                else:
                    for component, events in sorted(component_events.items()):
                        print(f"\nComponent: {component}\n  Subscribed Events: {', '.join(sorted(events))}")
                print("-"*60)

                print("\nEvent Subscribers (by Event Type):\n" + "-"*60)
                events_with_subscribers = {
                    et_val: info for et_val, info in subscriber_info.items() if info["components"]
                }
                if not events_with_subscribers: print("No event subscribers found for any event type.")
                else:
                    for event_type_val, info in sorted(events_with_subscribers.items()):
                        print(f"\nEvent: {event_type_val} (Subscribers: {info['count']})")
                        print(f"  Components: {', '.join(sorted(info['components']))}")
                print("-"*60)
            
            print("\nEvent Counts (Processed):\n" + "-"*60 + f"\n{'Event Type':<25} | {'Count':<10} | {'Last Event Time':<25}\n" + "-"*60)
            active_events = {et_val: count for et_val, count in self.stats["events_by_type"].items() if count > 0}
            if not active_events: print("No events processed yet")
            else:
                for event_type_val, count in sorted(active_events.items()):
                    last_time_dt = self.stats.get("last_event_times", {}).get(event_type_val)
                    last_time_str = last_time_dt.strftime('%Y-%m-%d %H:%M:%S') if last_time_dt else "Never"
                    print(f"{event_type_val:<25} | {count:<10} | {last_time_str}")
            
            print("\nQueue Statistics:\n" + "-"*60)
            print(f"Current Main Queue Size: {self.event_queue.qsize()}/{self._queue_size}")
            print(f"Max Main Queue Size Reached: {self.stats['max_queue_size_reached']}")
            print(f"Main Queue Overflows (Dropped): {self.stats['queue_overflow_count']}")
            print(f"Current Retry Queue Size: {self.retry_queue.qsize()}/{self._queue_size}") # Assuming retry queue has same max size
            print(f"Retry Successes: {self.stats['retry_successes']} / Attempts: {self.stats['retry_attempts']}")
            
            print("\nError Statistics:\n" + "-"*60)
            print(f"Total Errors Logged: {self.stats['error_count']}")
            print(f"Event Validation Errors: {self.stats['validation_errors']}")
            print(f"Callback Execution Errors: {self.stats['callback_errors']}")
            if self.stats['last_error']:
                last_err_time_str = self.stats['last_error_time'].strftime('%Y-%m-%d %H:%M:%S') if self.stats['last_error_time'] else "Unknown"
                print(f"\nLast Error ({last_err_time_str}):\n  {self.stats['last_error']}")
            
            if self.flow_monitor:
                flow_stats = self.flow_monitor.get_stats()
                print("\nEvent Flow Statistics:\n" + "-"*60)
                print(f"Broken Chains: {flow_stats.get('broken_chains', 0)}")
                print(f"Complete Chains: {flow_stats.get('complete_chains', 0)}")
                print(f"Active Chains: {flow_stats.get('active_chains', 0)}")
                if 'avg_completion_time' in flow_stats and flow_stats['avg_completion_time'] is not None:
                    print(f"Average Chain Completion Time: {flow_stats['avg_completion_time']:.3f}s")
            
            print("\n" + "="*80)
        except Exception as e:
            self.logger.error(f"Error printing event statistics: {e}", exc_info=True)

    def _get_priority_description(self, priority: EventPriority) -> str:
        descriptions = {
            EventPriority.HIGH: "High priority - processed quickly",
            EventPriority.NORMAL: "Normal priority - processed in order",
            EventPriority.LOW: "Low priority - processed when system is less busy"
        }
        return descriptions.get(priority, "Unknown priority level")


