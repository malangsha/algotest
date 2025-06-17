import uuid
import logging
import traceback
import threading
from typing import Dict, List, Optional, Any, Union, Set # Added Set
from datetime import datetime
from enum import Enum
import time
import collections
import json

from models.order import Order, OrderAction, OrderValidationError, OrderActionType
from models.trade import Trade
from models.order import OrderType as ModelOrderType
from models.events import (OrderEvent, Event, 
                           EventValidationError, 
                           SignalEvent, FillEvent, 
                           ExecutionEvent)
from utils.constants import (EventType, 
                             Exchange, 
                             OrderStatus, OrderSide, 
                             OrderType as ConstantsOrderType)
from utils.exceptions import OrderError, BrokerError
from utils.event_utils import EventUtils
from utils.constants import EventSource, BufferWindowState
from core.order_bufferwindow_manager import BufferWindowManager
from core.logging_manager import get_logger

class OrderProcessingError(Exception):
    """Exception raised when order processing fails"""
    pass

class OrderSubmissionTracker:
    """Tracks order submission attempts to prevent duplicates"""

    def __init__(self):
        self.submitted_orders: Set[str] = set() # Tracks successfully submitted broker_order_ids or internal_ids if broker_id not yet known
        self.submission_attempts: Dict[str, int] = {} # Tracks attempts per internal order_id
        self.last_submission_time: Dict[str, datetime] = {} # Tracks last attempt time per internal order_id
        self.max_attempts = 3
        self.cooldown_seconds = 5 # Cooldown between retries for the same order
        self.logger = logging.getLogger("core.order_submission_tracker")

    def can_submit(self, order_id: str) -> bool:
        """Check if an internal order_id can be submitted (not recently failed too many times)."""
        attempts = self.submission_attempts.get(order_id, 0)
        if attempts >= self.max_attempts:
            last_attempt_time = self.last_submission_time.get(order_id)
            if last_attempt_time and (datetime.now() - last_attempt_time).total_seconds() < self.cooldown_seconds * (attempts - self.max_attempts + 1): # Exponential backoff example
                self.logger.warning(f"Order {order_id} has reached max attempts ({attempts}) and is in cooldown.")
                return False
            # If cooldown passed, reset attempts to allow new try
            # self.submission_attempts[order_id] = 0 # Or allow one more try by not returning False

        last_attempt = self.last_submission_time.get(order_id)
        if last_attempt and (datetime.now() - last_attempt).total_seconds() < self.cooldown_seconds and attempts > 0 : # Apply cooldown only if there was a recent attempt
            self.logger.warning(f"Order {order_id} is in submission cooldown. Last attempt: {last_attempt}, Attempts: {attempts}")
            return False

        # Check if it's already successfully submitted and awaiting terminal state
        if order_id in self.submitted_orders:
             self.logger.warning(f"Order {order_id} is already marked as submitted and awaiting terminal state. Not resubmitting.")
             return False

        return True

    def record_submission_attempt(self, order_id: str):
        """Record a submission attempt for an internal order_id."""
        self.submission_attempts[order_id] = self.submission_attempts.get(order_id, 0) + 1
        self.last_submission_time[order_id] = datetime.now()
        self.logger.info(f"Submission attempt {self.submission_attempts[order_id]} for order {order_id} recorded at {self.last_submission_time[order_id]}.")


    def mark_submitted(self, order_id: str, broker_order_id: Optional[str] = None):
        """Mark internal order_id as successfully submitted to broker."""
        # We track successful submissions by internal_order_id to prevent OM from trying to submit it again.
        self.submitted_orders.add(order_id)
        # Reset attempts counter on successful submission, as it's now broker's responsibility
        self.submission_attempts.pop(order_id, None) # Remove from attempts as it's now submitted
        self.last_submission_time.pop(order_id, None) # No longer relevant for retry cooldown
        self.logger.info(f"Order {order_id} (Broker ID: {broker_order_id or 'N/A'}) marked as successfully submitted.")

    def remove_tracking(self, order_id: str):
        """Remove internal order_id from all tracking (when cancelled/rejected/filled)."""
        self.submitted_orders.discard(order_id)
        self.submission_attempts.pop(order_id, None)
        self.last_submission_time.pop(order_id, None)
        self.logger.info(f"Order {order_id} removed from submission tracking.")

class OrderMappingService:
    """Manages mapping between internal IDs and broker order IDs"""
    
    def __init__(self):
        self._internal_to_broker: Dict[str, str] = {}
        self._broker_to_internal: Dict[str, str] = {}
        self._lock = threading.RLock()
    
    def add_mapping(self, internal_id: str, broker_order_id: str) -> None:
        with self._lock:
            self._internal_to_broker[internal_id] = broker_order_id
            self._broker_to_internal[broker_order_id] = internal_id
    
    def get_internal_id(self, broker_order_id: str) -> Optional[str]:
        with self._lock:
            return self._broker_to_internal.get(broker_order_id)
    
    def get_broker_id(self, internal_id: str) -> Optional[str]:
        with self._lock:
            return self._internal_to_broker.get(internal_id)
    
    def has_mapping(self, internal_id: str) -> bool:
        with self._lock:
            return internal_id in self._internal_to_broker
    
    def remove_mapping(self, internal_id: str) -> None:
        with self._lock:
            broker_id = self._internal_to_broker.pop(internal_id, None)
            if broker_id:
                self._broker_to_internal.pop(broker_id, None)

class OrderManager:
    """Manages order lifecycle, from creation to execution and tracking"""

    def __init__(self, event_manager, risk_manager=None, broker_interface=None):
        self.logger = get_logger("core.order_manager")
        self.event_manager = event_manager
        self.risk_manager = risk_manager
        self.broker_interface = broker_interface # Note: OrderManager typically doesn't directly use broker_interface
        self._buffer_manager = BufferWindowManager()

        self.orders: Dict[str, Order] = {}
        self.pending_orders: Dict[str, Order] = {} # Orders waiting for ExecutionHandler to pick up
        self.filled_orders: Dict[str, Order] = {}
        self.cancelled_orders: Dict[str, Order] = {}
        self.rejected_orders: Dict[str, Order] = {}

        self.submission_tracker = OrderSubmissionTracker()
        self.broker_to_internal_id: Dict[str, str] = {}
        self.order_lifecycle: Dict[str, List[Dict[str, Any]]] = {}

        self.stats = {
            "orders_created": 0, "orders_submitted": 0, "orders_filled": 0,
            "orders_cancelled": 0, "orders_rejected": 0, "orders_modified": 0,
            "errors": 0, "last_error": None, "last_error_time": None,
            "retry_attempts": 0, "retry_successes": 0 # Managed by submission_tracker now
        }
        self.logger.info("Order Manager initializing")

        # --- Order mappping ---
        self._mapping_service = OrderMappingService()
        self._lock = threading.RLock()

        # Add buffer for unmapped events and a dedicated lock
        self._unmapped_events_buffer = collections.deque(maxlen=200)
        self._unmapped_lock = threading.Lock()
        self._dlq_logger = get_logger("dead_letter_queue.executions")
        
        # For debugging/monitoring
        self._processed_events_count = 0
        self._buffered_events_count = 0
        self._expired_windows_count = 0  

        self._register_event_handlers()
        self.logger.info("Order Manager initialized")

    def _register_event_handlers(self):
        self.event_manager.subscribe(EventType.SIGNAL, self._on_signal_event, component_name="OrderManager")
        self.event_manager.subscribe(EventType.ORDER, self._on_order_event, component_name="OrderManager")
        self.event_manager.subscribe(EventType.FILL, self._on_fill_event, component_name="OrderManager")
        self.event_manager.subscribe(EventType.EXECUTION, self._on_execution_event, component_name="OrderManager")
        self.event_manager.subscribe(EventType.RISK_BREACH, self._on_risk_breach_event, component_name="OrderManager")        
        self.logger.info("Registered with Event Manager for relevant events.")

    def _record_error(self, error_msg: str, exception_obj: Optional[Exception] = None):
        self.stats["errors"] += 1
        self.stats["last_error"] = error_msg
        self.stats["last_error_time"] = datetime.now()
        if exception_obj:
            self.logger.error(f"OrderManager Error: {error_msg}", exc_info=exception_obj)
        else:
            self.logger.error(f"OrderManager Error: {error_msg}")


    def _record_order_lifecycle_event(self, order_id: str, status: Union[str, Enum], message: Optional[str] = None):
        if order_id not in self.order_lifecycle:
            self.order_lifecycle[order_id] = []

        status_str = status.value if isinstance(status, Enum) else str(status)

        self.order_lifecycle[order_id].append({
            "timestamp": datetime.now(),
            "status": status_str,
            "message": message or ""
        })
        self.logger.debug(f"Order {order_id} lifecycle: {status_str} - {message or ''}")

    def _validate_state_transition(self, order: Order, target_status: OrderStatus) -> bool:
        current_status = order.status
        valid_transitions = {
            OrderStatus.CREATED: [OrderStatus.PENDING, OrderStatus.SUBMITTING, OrderStatus.REJECTED, OrderStatus.CANCELLED], # Allow cancel/reject from CREATED
            OrderStatus.PENDING: [OrderStatus.SUBMITTING, OrderStatus.PENDING_CANCEL, OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.SUBMITTED], # Added SUBMITTED if EH confirms quickly
            OrderStatus.SUBMITTING: [OrderStatus.SUBMITTED, OrderStatus.REJECTED, OrderStatus.PENDING_CANCEL, OrderStatus.CANCELLED],
            OrderStatus.SUBMITTED: [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                                   OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.PENDING_CANCEL,
                                   OrderStatus.PENDING_REPLACE],
            OrderStatus.OPEN: [OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELLED,
                              OrderStatus.REJECTED, OrderStatus.PENDING_CANCEL, OrderStatus.PENDING_REPLACE],
            OrderStatus.PARTIALLY_FILLED: [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.PENDING_CANCEL, OrderStatus.REJECTED], # Added REJECTED
            OrderStatus.PENDING_CANCEL: [OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED], # If cancel fails, can revert
            OrderStatus.PENDING_REPLACE: [OrderStatus.OPEN, OrderStatus.REJECTED, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED], # If modify fails/succeeds
            OrderStatus.FILLED: [],
            OrderStatus.CANCELLED: [],
            OrderStatus.REJECTED: []
        }
        allowed_transitions = valid_transitions.get(current_status, [])
        is_valid = target_status in allowed_transitions

        if not is_valid:
            self.logger.warning(f"Invalid state transition for order {order.order_id}: {current_status.value} -> {target_status.value}")
        return is_valid

    def _on_signal_event(self, event: SignalEvent):
        try:
            self.logger.debug(f"Received signal event: {event}")
            if not isinstance(event, SignalEvent):
                self.logger.warning(f"Invalid signal event type received: {type(event)}")
                return
            event.validate()

            if self.risk_manager and hasattr(self.risk_manager, 'check_signal'):
                if not self.risk_manager.check_signal(event):
                    self.logger.info(f"Signal {event.event_id} rejected by risk manager for strategy {event.strategy_id}")
                    return

            order = self._create_order_from_signal(event)
            if order:
                self.stats["orders_created"] += 1
                self._record_order_lifecycle_event(order.order_id, "CREATED", f"Created from signal {event.event_id}")
                self.logger.info(f"Order {order.order_id} created from signal {event.event_id}")
                # Instead of publishing CREATE, directly attempt to submit
                self.submit_order(order.order_id)
        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing signal event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling signal event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _on_order_event(self, event: OrderEvent):
        # This method primarily handles externally triggered order actions (e.g., manual cancel/modify)
        # or status updates that might come through OrderEvents (though ExecutionEvents are preferred for broker status).
        try:
            self.logger.debug(f"Received order event: {event}")
            if not isinstance(event, OrderEvent):
                self.logger.warning(f"Invalid order event type received: {type(event)}")
                return
            event.validate()

            order_id = event.order_id
            order = self.orders.get(order_id)

            if not order:
                if event.broker_order_id and event.broker_order_id in self.broker_to_internal_id:
                    internal_id = self.broker_to_internal_id[event.broker_order_id]
                    order = self.orders.get(internal_id)
                    if order: self.logger.info(f"Found order by broker ID mapping: {event.broker_order_id} -> {internal_id}")
                if not order:
                    self.logger.warning(f"OrderManager received OrderEvent for an untracked order_id: {order_id}. Status: {event.status}, Action: {event.action}")
                    return

            # Update local order from the event if it contains new information (less common for OM to act on its own OrderEvents this way)
            # self._update_local_order_from_event(order, event) # Be cautious with this, ExecutionEvents are primary source of truth for status
            action_str = str(event.action).upper() if event.action else None
            event_status_val = event.status.value if isinstance(event.status, Enum) else str(event.status).upper()

            if action_str == OrderActionType.SUBMIT.value:
                if order.status == OrderStatus.CREATED:
                    self.logger.info(f"OrderManager: Received explicit SUBMIT action for CREATED order {order_id}. Proceeding.")
                    self.submit_order(order.order_id)
                elif order.status == OrderStatus.PENDING:
                     self.logger.warning(f"OrderManager: Received SUBMIT action for order {order_id} already PENDING. Ignoring to prevent loop. Submission tracker manages retries.")
                else:
                    self.logger.warning(f"OrderManager: Received SUBMIT action for order {order_id} in status {order.status.value}. Cannot submit.")

            elif action_str == OrderActionType.CANCEL.value:
                if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                    self.cancel_order(order.order_id)
            elif action_str == OrderActionType.MODIFY.value:
                if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                    modify_params = event.metadata or {}
                    self.modify_order(order.order_id, **modify_params)
            # OM generally shouldn't react to status-only OrderEvents it published itself.
            # It should react to ExecutionEvents from the broker (via ExecutionHandler).
            else:
                self.logger.debug(f"OrderManager: OrderEvent for {order_id} with unhandled action/status. Action: '{action_str}', Status: '{event_status_val}'")

        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing order event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling order event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _update_local_order_from_event_data(self, order: Order, event_data: Union[OrderEvent, ExecutionEvent, FillEvent]):
        """Updates local order based on data from any relevant event."""
        if not order or not event_data: return

        new_status_val = getattr(event_data, 'status', None)
        if new_status_val is not None:
            try:
                new_status = OrderStatus(new_status_val.value) if isinstance(new_status_val, Enum) else OrderStatus(str(new_status_val).upper())
                if order.status != new_status:
                    if self._validate_state_transition(order, new_status):
                        self.logger.info(f"Order {order.order_id} status changing from {order.status.value} to {new_status.value} based on {type(event_data).__name__} {getattr(event_data, 'event_id', '')}")
                        order.status = new_status
                    else:
                        self.logger.warning(f"Skipping invalid state transition for order {order.order_id} from event {type(event_data).__name__}: {order.status.value} -> {new_status.value}")

                # Update broker order ID if not already set
                if event_data.broker_order_id and not order.broker_order_id:
                    order.broker_order_id = event_data.broker_order_id

            except ValueError as e:
                self.logger.error(f"Invalid status value '{new_status_val}' in {type(event_data).__name__} for order {order.order_id}: {e}")

        broker_id = getattr(event_data, 'broker_order_id', None)
        if broker_id:
            if order.broker_order_id != broker_id:
                 self.logger.info(f"Updating broker_order_id for order {order.order_id} to {broker_id}")
                 order.broker_order_id = broker_id
            if broker_id not in self.broker_to_internal_id or self.broker_to_internal_id[broker_id] != order.order_id :
                self.broker_to_internal_id[broker_id] = order.order_id
                self.logger.info(f"Mapped broker_order_id {broker_id} to internal_order_id {order.order_id}")

        filled_qty = getattr(event_data, 'filled_quantity', None) # For OrderEvent, ExecutionEvent
        if filled_qty is None and isinstance(event_data, FillEvent): # For FillEvent
            filled_qty = order.filled_quantity + event_data.quantity # FillEvent has incremental quantity

        if filled_qty is not None and filled_qty != order.filled_quantity:
            order.filled_quantity = filled_qty

        avg_price = getattr(event_data, 'average_price', None) # For OrderEvent, ExecutionEvent
        if avg_price is None and isinstance(event_data, FillEvent) and event_data.quantity > 0: # For FillEvent
            if order.filled_quantity > 0: # Recalculate average price
                 total_value = (order.average_fill_price * (order.filled_quantity - event_data.quantity)) + (event_data.price * event_data.quantity)
                 avg_price = total_value / order.filled_quantity
            else: # Should not happen if filled_qty was updated correctly
                 avg_price = event_data.price

        if avg_price is not None and avg_price != order.average_fill_price: # and order.filled_quantity > 0:
            order.average_fill_price = avg_price

        rejection = getattr(event_data, 'rejection_reason', None)
        if rejection and order.rejection_reason != rejection:
            order.rejection_reason = rejection

        event_timestamp = getattr(event_data, 'timestamp', None)
        if event_timestamp:
            try:
                # Assuming event timestamps are in seconds since epoch
                order.last_updated = datetime.fromtimestamp(float(event_timestamp))
            except (ValueError, TypeError) as e:
                self.logger.error(f"Timestamp conversion error for {type(event_data).__name__} {getattr(event_data, 'event_id', '')} (ts: {event_timestamp}): {e}. Using current time.")
                order.last_updated = datetime.now()
        else:
            order.last_updated = datetime.now()

        # Update order.params with event.metadata if present
        metadata = getattr(event_data, 'metadata', None)
        if metadata and isinstance(metadata, dict):
            if order.params is None: order.params = {}
            order.params.update(metadata)

        self.logger.debug(f"Updated local order {order.order_id} from {type(event_data).__name__}. New status: {order.status.value}, Filled: {order.filled_quantity}, AvgPx: {order.average_fill_price}")

    def _handle_rejection(self, order: Order, reason: Optional[str]):
        """Handles an order rejection."""
        target_status = OrderStatus.REJECTED
        if order.status == target_status: # Already rejected
            if order.rejection_reason != reason:
                order.rejection_reason = f"{order.rejection_reason}; {reason}" # Append reason
            self.logger.info(f"Order {order.order_id} already REJECTED. Reason updated: {order.rejection_reason}")
            return

        if not self._validate_state_transition(order, target_status):
            self.logger.warning(f"Cannot transition order {order.order_id} to REJECTED from {order.status.value}")
            return

        order.status = target_status
        order.rejection_reason = reason or "Unknown"
        order.last_updated = datetime.now()

        if order.order_id in self.pending_orders:
            del self.pending_orders[order.order_id]
        self.rejected_orders[order.order_id] = order

        self._record_order_lifecycle_event(order.order_id, "REJECTED", reason)
        self.stats["orders_rejected"] += 1
        self.submission_tracker.remove_tracking(order.order_id) # Critical: remove from tracker on rejection
        self.logger.info(f"Order {order.order_id} REJECTED. Reason: {reason}")
        self._publish_order_event(order, OrderActionType.UPDATE_STATUS.value, status_override=OrderStatus.REJECTED)

    def _handle_expired_windows(self) -> None:
        """Handle expired buffer windows"""
        expired_orders = self._buffer_manager.check_expired_windows()
        for internal_id in expired_orders:
            self._expired_windows_count += 1
            window = self._buffer_manager.get_window(internal_id)
            if window:
                buffered_events = window.get_buffered_events()
                if buffered_events:
                    self.logger.error(f"Buffer window expired for {internal_id} with {len(buffered_events)} unprocessed events")
                    # Optionally, you could still try to process these events
                    # or move them to a dead letter queue
                else:
                    self.logger.warning(f"Buffer window expired for {internal_id} with no buffered events")
            
            # Cleanup expired window
            self._buffer_manager.cleanup_window(internal_id)

    def _on_execution_event(self, event: ExecutionEvent) -> None:
        """Handle execution events with buffer window logic"""
        self.logger.debug(f"Received execution event: {event}")
        if not isinstance(event, ExecutionEvent):
                self.logger.warning(f"Invalid execution event type: {type(event)}")
                return
        event.validate()

        self.logger.debug(f"Execution Event Source: {event.source}")
        with self._lock:
            
            # Check for expired windows first
            self._handle_expired_windows()  
            if event.source == EventSource.EXECUTION_HANDLER:
                self._handle_eh_event(event)
            elif event.source == EventSource.MARKET_DATA_FEED:
                self._handle_mdf_event(event)
            else:             
                self._process_execution_event(event)
                
    def _handle_eh_event(self, event: ExecutionEvent):
        """Handle EH event - establishes mapping and closes buffer window"""
        if not event.broker_order_id:
            self.logger.error(f"EH event missing broker_order_id: {event}")
            return
        
        # Get buffer window
        window = self._buffer_manager.get_window(event.order_id)
        if not window:
            self.logger.warning(f"No buffer window found for EH event: {event.order_id}")
            # Process the event anyway (might be a late EH event)
            self._process_execution_event(event)
            return        
            
        # # Establish mapping
        # internal_id = self._mapping_service.get_internal_id(event.broker_order_id)
        # if EventUtils.is_unmapped_orderid(internal_id):            
        #     self._mapping_service.remove_mapping(internal_id) # remove old mapping from MDF

        # self._mapping_service.add_mapping(event.order_id, event.broker_order_id)
        
        # Store the EH event in the window
        window.expected_eh_event = event
        
        # Process the EH event itself
        self._process_execution_event(event)
        
        # Get buffered events before closing window
        buffered_events = window.get_buffered_events()
        
        # Close the buffer window
        self._buffer_manager.close_window(event.order_id)
        
        # Process buffered events in order
        if buffered_events:
            self.logger.info(f"Processing {len(buffered_events)} buffered events for {event.order_id}")
            for buffered_event in buffered_events:
                # Update the event to use correct internal_id
                buffered_event.order_id = event.order_id
                self._process_execution_event(buffered_event)
        
        self.logger.info(f"Processed EH event and closed buffer window for {event.order_id}")  

    def _handle_mdf_event(self, event: ExecutionEvent):
        """Handle MDF event - buffer if window is active, otherwise process normally"""
        if not event.broker_order_id:
            self.logger.error(f"MDF event missing broker_order_id: {event}")
            return
        
        order_id = getattr(event, "order_id", None)
        if not order_id:
            order_id = event.get('remarks', '').partition('=')[2] or None
            if not order_id:
               self.logger.debug(f"order_id not found in the event received, skipping the event.")
               return             

        # Check if we have a mapping already        
        window = self._buffer_manager.get_window(event.order_id)               
        if window and window.is_active():
            self.logger.warning(f"MDF event for {event.broker_order_id} received while window still active")
            window.add_buffered_event(event)
            self._buffered_events_count += 1
        elif window and window.state == BufferWindowState.CLOSED:
            self._process_execution_event(event)
            self.logger.debug(f"Processed MDF event (window closed) for {event.broker_order_id}")
        else:
            self._process_execution_event(event)
            self.logger.debug(f"Processed MDF event (no window) for {event.broker_order_id}")                   

    def _process_execution_event(self, event: ExecutionEvent):
        try:           

            internal_id = event.order_id
            broker_id = event.broker_order_id            
            order = self.orders.get(internal_id)

            # # Check if this is an unmapped event (placeholder ID starting with "UNMAPPED_")
            # is_unmapped_event = EventUtils.is_unmapped_event(event)
            
            # if internal_id and not is_unmapped_event:                
            #     order = self.orders.get(internal_id)            
            # elif broker_id:
            #     # Attempt to map broker ID to internal ID
            #     resolved_internal_id =self._mapping_service.get_internal_id(broker_id)
            #     if resolved_internal_id:
            #         self.logger.info(f"ExecutionEvent for broker_id {broker_id} mapped to internal_id {resolved_internal_id}.")
            #         order = self.orders.get(resolved_internal_id)
            #         internal_id = resolved_internal_id  # Use the resolved ID
            #     else:
            #         # Mapping not yet available, buffer the event
            #         with self._unmapped_lock:
            #             event.received_at = time.time()  # Add timestamp for retry timeout
            #             self._unmapped_events_buffer.append(event)
            #         self.logger.warning(f"ExecutionEvent for broker_id {broker_id} could not be mapped. Buffering for retry.")
            #         return  # Stop processing this event here

            # Main processing logic for a mapped event
            exec_type = getattr(event, 'execution_type', 'UPDATE')
            exec_status = event.status
            if exec_type == 'BROKER_PENDING' and exec_status == OrderStatus.PENDING:
                self.logger.debug(f"Order notification event, Skipping it for now.")
                return
            
            self._update_local_order_from_event_data(order, event) # Use generic updater
            execution_status = order.status # Status should be updated by _update_local_order_from_event_data          
            self._record_order_lifecycle_event(internal_id, f"EXECUTION_{exec_type}",
                                              f"ExecID: {event.execution_id}, Status: {execution_status.value}, BrokerID: {event.broker_order_id or 'N/A'}")

            if execution_status == OrderStatus.REJECTED:
                # _handle_rejection was likely called by _update_local_order_from_event_data if status changed to REJECTED
                # Ensure submission_tracker.remove_tracking is called
                if order.order_id not in self.submission_tracker.submitted_orders and order.order_id not in self.submission_tracker.submission_attempts:
                     pass # Already removed by _handle_rejection
                else: # Fallback if _handle_rejection wasn't triggered by status change logic
                    self.submission_tracker.remove_tracking(order.order_id)
                self.logger.info(f"Order {order.order_id} processed as REJECTED from ExecutionEvent.")


            elif execution_status == OrderStatus.SUBMITTED:
                if event.broker_order_id:
                    self.submission_tracker.mark_submitted(order.order_id, event.broker_order_id)
                    if order.order_id in self.pending_orders:
                        del self.pending_orders[order.order_id]
                    self.logger.info(f"Order {order.order_id} confirmed SUBMITTED by ExecutionEvent. Broker ID: {event.broker_order_id}")
                else:
                    self.logger.warning(f"ExecutionEvent for order {order.order_id} has status SUBMITTED but no broker_order_id.")


            elif execution_status == OrderStatus.CANCELLED:
                self._handle_cancellation(order, event.text or "Cancelled by broker execution") # _handle_cancellation also calls remove_tracking

            elif execution_status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
                # FillEvent creation should be based on ExecutionEvent's fill details
                if event.last_filled_quantity is not None and event.last_filled_quantity > 0:
                    self._create_fill_from_execution(event, order) # Pass order to get strategy_id if not in event
                if execution_status == OrderStatus.FILLED:
                    self.submission_tracker.remove_tracking(order.order_id)
                    if order.order_id in self.pending_orders: del self.pending_orders[order.order_id]
                    self.filled_orders[order.order_id] = order
                    self.stats["orders_filled"] += 1
                    self.logger.info(f"Order {order.order_id} fully FILLED by ExecutionEvent.")           

        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing execution event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling execution event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)


    def _create_fill_from_execution(self, exec_event: ExecutionEvent, order: Order): # Added order param
        try:
            if not (exec_event.last_filled_quantity is not None and exec_event.last_filled_quantity > 0 and exec_event.last_filled_price is not None):
                self.logger.debug(f"ExecutionEvent {exec_event.execution_id} for order {exec_event.order_id} does not contain new fill data. Skipping FillEvent creation.")
                return

            fill_event = FillEvent(
                timestamp=exec_event.execution_time if exec_event.execution_time is not None else exec_event.timestamp,
                order_id=exec_event.order_id, # Should be internal order_id
                symbol=exec_event.symbol,
                exchange=Exchange(exec_event.exchange.value) if isinstance(exec_event.exchange, Enum) else Exchange(str(exec_event.exchange).upper()),
                side=OrderSide(exec_event.side.value) if isinstance(exec_event.side, Enum) else OrderSide(str(exec_event.side).upper()),
                quantity=exec_event.last_filled_quantity,
                price=exec_event.last_filled_price,
                commission=exec_event.commission or 0.0,
                fill_time=exec_event.execution_time if exec_event.execution_time is not None else exec_event.timestamp,
                strategy_id=order.strategy_id, # Get strategy_id from the order object
                broker_order_id=exec_event.broker_order_id,
                execution_id=exec_event.execution_id,
                event_type=EventType.FILL
            )
            fill_event.validate()
            self.event_manager.publish(fill_event)
            self.logger.info(f"Created FillEvent {fill_event.fill_id} from ExecutionEvent {exec_event.execution_id} for order {exec_event.order_id}")
        except (EventValidationError, OrderValidationError, ValueError) as e:
            self.logger.error(f"FillEvent creation/validation failed (from exec {exec_event.execution_id}): {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Error creating FillEvent from ExecutionEvent {exec_event.execution_id}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _on_fill_event(self, event: FillEvent):
        try:
            self.logger.debug(f"Received fill event: {event}")
            if not isinstance(event, FillEvent):
                self.logger.warning(f"Invalid fill event type: {type(event)}")
                return
            event.validate()

            order_id = event.order_id # Internal system ID
            order = self.orders.get(order_id)

            if not order and event.broker_order_id: # Try mapping via broker_order_id
                internal_id_from_broker = self.broker_to_internal_id.get(event.broker_order_id)
                if internal_id_from_broker:
                    order = self.orders.get(internal_id_from_broker)
                    if order:
                        self.logger.info(f"Found order by broker ID mapping in FillEvent: {event.broker_order_id} -> {internal_id_from_broker}")
                        order_id = internal_id_from_broker
                else:
                     self.logger.warning(f"FillEvent for broker_order_id {event.broker_order_id} has no known internal mapping.")


            if not order:
                self.logger.warning(f"Received FillEvent for unknown order_id: {event.order_id} (Broker ID: {event.broker_order_id or 'N/A'})")
                return

            # Store old filled quantity to see if this fill is new
            old_filled_quantity = order.filled_quantity
            self._update_local_order_from_event_data(order, event) # Updates status, filled_qty, avg_price

            # Check if the fill was actually processed (filled quantity increased)
            if order.filled_quantity > old_filled_quantity or (order.filled_quantity == event.quantity and old_filled_quantity == 0): # Ensure it's a new fill
                self._record_order_lifecycle_event(order_id, f"FILL_PROCESSED (Status: {order.status.value})",
                                                  f"FillID: {event.fill_id}, Qty: {event.quantity}@{event.price}, TotalFilled: {order.filled_quantity}/{order.quantity}")

                if hasattr(event, 'commission') and event.commission is not None:
                     order.total_commission = getattr(order, 'total_commission', 0.0) + event.commission

                if order.status == OrderStatus.FILLED:
                    if order_id in self.pending_orders:
                        del self.pending_orders[order_id]
                    self.filled_orders[order_id] = order
                    self.stats["orders_filled"] += 1
                    self.submission_tracker.remove_tracking(order_id) # Critical: remove from tracker on full fill
                    avg_price_str = f"{order.average_fill_price:.2f}" if order.average_fill_price else 'N/A'
                    self.logger.info(f"Order {order_id} fully FILLED by FillEvent. AvgPx: {avg_price_str}")
                elif order.status == OrderStatus.PARTIALLY_FILLED:
                    self.logger.info(f"Order {order_id} PARTIALLY_FILLED by FillEvent. Filled: {order.filled_quantity}/{order.quantity}, AvgPx: {order.average_fill_price:.2f if order.average_fill_price else 'N/A'}")
                # self._publish_order_event(order, OrderActionType.UPDATE_FILL.value) # Reconsider if needed
            else:
                self.logger.info(f"FillEvent {event.fill_id} for order {order_id} did not result in new filled quantity. Current filled: {order.filled_quantity}")


        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing fill event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling fill event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _on_risk_breach_event(self, event): # RiskBreachEvent (assuming type hint)
        try:
            self.logger.warning(f"Received risk breach event: {event}")
            if not isinstance(event, Event) or event.event_type != EventType.RISK_BREACH:
                self.logger.warning(f"Invalid risk breach event type: {type(event)}")
                return

            symbol_to_act_on = getattr(event, 'symbol', None)
            strategy_to_act_on = getattr(event, 'strategy_id', None)
            orders_to_cancel_ids = []

            # Check active orders (pending, submitted, open, partially_filled)
            active_orders_view = {**self.pending_orders, **{k:v for k,v in self.orders.items() if v.status in [OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]}}

            for order_id, order_obj in list(active_orders_view.items()):
                matches_symbol = (symbol_to_act_on and order_obj.instrument_id == symbol_to_act_on)
                matches_strategy = (strategy_to_act_on and order_obj.strategy_id == strategy_to_act_on)

                should_cancel = False
                if symbol_to_act_on and strategy_to_act_on:
                    if matches_symbol and matches_strategy: should_cancel = True
                elif symbol_to_act_on:
                    if matches_symbol: should_cancel = True
                elif strategy_to_act_on:
                    if matches_strategy: should_cancel = True
                elif not symbol_to_act_on and not strategy_to_act_on: # If event means "cancel all"
                    should_cancel = True


                if should_cancel:
                    orders_to_cancel_ids.append(order_id)

            for order_id_to_cancel in orders_to_cancel_ids:
                try:
                    self.logger.info(f"Attempting to cancel order {order_id_to_cancel} due to risk breach event {getattr(event, 'event_id', 'N/A')}")
                    self.cancel_order(order_id_to_cancel)
                except Exception as e_cancel:
                    self.logger.error(f"Failed to initiate cancellation for order {order_id_to_cancel} during risk breach handling: {e_cancel}")
                    self._record_error(str(e_cancel), e_cancel)
        except Exception as e:
            self.logger.error(f"Error handling risk breach event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e) 

    def _create_order_from_signal(self, signal: SignalEvent) -> Optional[Order]:
        if not (signal.symbol and signal.signal_type and signal.side and signal.quantity is not None and signal.order_type):
            self.logger.error(f"SignalEvent {signal.event_id} is missing required fields for order creation.")
            raise OrderValidationError("SignalEvent missing required fields.")

        order_params = signal.metadata.copy() if signal.metadata else {}
        order_params['signal_price_ref'] = signal.price
        order_params['underlying_price_at_signal'] = signal.signal_price
        if hasattr(signal, 'time_in_force') and signal.time_in_force:
            order_params['time_in_force'] = signal.time_in_force

        try:
            order = self.create_order(
                strategy_id=signal.strategy_id,
                instrument_id=signal.symbol,
                quantity=signal.quantity,
                price=signal.price,
                side=signal.side.value,
                order_type=signal.order_type, # Pass ConstantsOrderType, create_order handles it
                exchange=signal.exchange.value if isinstance(signal.exchange, Enum) else signal.exchange,
                params=order_params
            )
            return order
        except (OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Failed to create order from signal {signal.event_id} due to: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in _create_order_from_signal for signal {signal.event_id}: {e}", exc_info=True)
            return None

    def _publish_order_event(self, order: Order, action: Optional[str] = None, status_override: Optional[OrderStatus] = None):
        if not order: return
        try:
            side_enum = order.side if isinstance(order.side, OrderSide) else OrderSide(str(order.side).upper())
            order_type_for_event = ConstantsOrderType(order.order_type.value) # Map models.order.OrderType to utils.constants.OrderType
            status_for_event = status_override if status_override else order.status
            remarks = (
                f"order_id={order.order_id}"
                if order.order_id
                else EventUtils.create_unmapped_placeholder_id()
            )
            event = OrderEvent(
                order_id=order.order_id,
                symbol=order.instrument_id,
                exchange=order.exchange if isinstance(order.exchange, Exchange) else (Exchange(str(order.exchange).upper()) if order.exchange else None),
                side=side_enum,
                quantity=order.quantity,
                order_type=order_type_for_event,
                status=status_for_event,
                price=order.price,
                trigger_price=getattr(order, 'stop_price', None),
                filled_quantity=order.filled_quantity,
                remaining_quantity=order.remaining_quantity(),
                average_price=order.average_fill_price,
                order_time=order.created_at.timestamp() if order.created_at else time.time(),
                last_update_time=order.last_updated.timestamp() if order.last_updated else time.time(),
                strategy_id=order.strategy_id,
                client_order_id=getattr(order, 'client_order_id', None),
                broker_order_id=order.broker_order_id,
                rejection_reason=getattr(order, 'rejection_reason', None),
                action=action or (status_for_event.value if isinstance(status_for_event, Enum) else str(status_for_event)),
                time_in_force=getattr(order, 'time_in_force', None) or order.params.get('time_in_force'),
                metadata=getattr(order, 'params', None) or {},
                event_type=EventType.ORDER,
                remarks=remarks, 
                timestamp=time.time()
            )
            event.validate()
            self.event_manager.publish(event)
            self.logger.debug(f"Published OrderEvent: ID={order.order_id}, Action={event.action}, Status={event.status.value}")
        except (EventValidationError, OrderValidationError) as e:
            self.logger.error(f"OrderEvent validation failed for order {order.order_id}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Error publishing OrderEvent for order {order.order_id}: {e}", exc_info=True)
            self._record_error(str(e), e)


    def _handle_cancellation(self, order: Order, reason: Optional[str]):
        """Handles an order cancellation."""
        target_status = OrderStatus.CANCELLED
        if order.status == target_status: # Already cancelled
            self.logger.info(f"Order {order.order_id} already CANCELLED.")
            return

        if not self._validate_state_transition(order, target_status):
            # If it's in PENDING_CANCEL, it's a valid final state.
            if order.status == OrderStatus.PENDING_CANCEL:
                 self.logger.info(f"Order {order.order_id} was PENDING_CANCEL, now confirming CANCELLED.")
            else:
                self.logger.warning(f"Cannot transition order {order.order_id} to CANCELLED from {order.status.value}")
                # If cancel request failed at broker, it might revert to previous state. This needs broker feedback.
                # For now, if OM decides it's cancelled, it's cancelled.
                # return # Potentially dangerous to return without setting state if cancellation is confirmed elsewhere

        order.status = target_status
        order.canceled_at = datetime.now()
        order.last_updated = order.canceled_at

        if order.order_id in self.pending_orders:
            del self.pending_orders[order.order_id]
        self.cancelled_orders[order.order_id] = order

        self._record_order_lifecycle_event(order.order_id, "CANCELLED", reason or "Cancelled")
        self.stats["orders_cancelled"] += 1
        self.submission_tracker.remove_tracking(order.order_id) # Critical: remove from tracker
        self.logger.info(f"Order {order.order_id} CANCELLED. Reason: {reason}")
        self._publish_order_event(order, OrderActionType.UPDATE_STATUS.value, status_override=OrderStatus.CANCELLED)


    def create_order(self, strategy_id: str, instrument_id: str, quantity: float, price: Optional[float],
                     side: str, order_type: Union[ConstantsOrderType, ModelOrderType, str],
                     exchange: Optional[Union[Exchange, str]] = None,
                     params: Optional[Dict[str, Any]] = None) -> Optional[Order]:
        try:
            order_side_enum = OrderSide(side.upper()) # Validate and get enum

            final_order_type: ModelOrderType
            if isinstance(order_type, ModelOrderType):
                final_order_type = order_type
            elif isinstance(order_type, ConstantsOrderType):
                final_order_type = ModelOrderType(order_type.value)
            elif isinstance(order_type, str):
                final_order_type = ModelOrderType(order_type.upper())
            else:
                raise OrderValidationError(f"Unsupported order_type type: {type(order_type)}")

            final_exchange_enum: Optional[Exchange] = None
            if isinstance(exchange, Exchange):
                final_exchange_enum = exchange
            elif isinstance(exchange, str):
                final_exchange_enum = Exchange(exchange.upper()) # Validate and get enum
            elif ':' in instrument_id: # Try to infer from instrument_id
                 try:
                    final_exchange_enum = Exchange(instrument_id.split(':', 1)[0].upper())
                 except ValueError:
                    raise OrderValidationError(f"Could not infer valid Exchange from instrument_id {instrument_id}")
            if not final_exchange_enum:
                raise OrderValidationError(f"Exchange is required for instrument {instrument_id}")


            order = Order(
                instrument_id=instrument_id,
                quantity=quantity,
                side=order_side_enum.value, # models.Order expects string "BUY" or "SELL"
                order_type=final_order_type,
                price=price,
                strategy_id=strategy_id,
                exchange=final_exchange_enum.value, # models.Order expects string like "NSE", "BSE"
                params=params or {}
            )

            self.orders[order.order_id] = order
            self._record_order_lifecycle_event(order.order_id, "CREATED", f"Strategy: {strategy_id}, Type: {order.order_type.value}, Qty: {quantity}")
            self.logger.info(f"Created order: {order}")
            return order
        except (OrderValidationError, ValueError) as e:
            self.logger.error(f"Order creation/validation failed: {e}", exc_info=True)
            self._record_error(str(e), e)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error creating order: {e}", exc_info=True)
            self._record_error(str(e), e)
            return None

    def submit_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if not order:
            self.logger.error(f"Cannot submit: Order {order_id} not found.")
            return False
        try:
            if not self.submission_tracker.can_submit(order_id):
                self.logger.warning(f"Order {order_id} cannot be submitted based on submission tracker rules (e.g. max attempts, cooldown, or already submitted).")
                # If it's in submitted_orders set, it means we think it's live. Don't change status.
                if order_id in self.submission_tracker.submitted_orders:
                    return False # Already submitted, do nothing.
                # If it's due to attempts/cooldown, we might set to REJECTED or just log and wait.
                # For now, just prevent submission.
                return False


            if order.status not in [OrderStatus.CREATED, OrderStatus.REJECTED]: # Allow retry for rejected if tracker permits
                if order.status == OrderStatus.PENDING and order_id in self.pending_orders:
                     self.logger.warning(f"Order {order_id} is already PENDING and queued for ExecutionHandler. Submission tracker should prevent resending if already live.")
                     # Re-publish PENDING event only if we are sure it's not a duplicate attempt for a live order
                     # This path is risky; rely on tracker.
                     return False # Avoid re-queuing if already PENDING.
                else:
                    raise OrderProcessingError(f"Cannot submit order {order_id} with status {order.status.value}. Requires CREATED or REJECTED (for retry).")

            self.submission_tracker.record_submission_attempt(order_id) # Record attempt *before* changing state and publishing

            # Transition to PENDING: OM hands off to EH by publishing a PENDING OrderEvent
            if not self._validate_state_transition(order, OrderStatus.PENDING):
                 raise OrderProcessingError(f"Cannot transition order {order_id} from {order.status.value} to PENDING.")

            self._buffer_manager.start_buffer_window(order_id)
            order.status = OrderStatus.PENDING
            order.last_updated = datetime.now()
            self.pending_orders[order.order_id] = order # Add to OM's pending queue
            self._record_order_lifecycle_event(order.order_id, "PENDING", "Queued for submission to broker via ExecutionHandler")
            self._publish_order_event(order, OrderActionType.PENDING.value) # Notify ExecutionHandler

            self.logger.info(f"Order {order_id} moved to PENDING state. Awaiting processing by ExecutionHandler. Attempt: {self.submission_tracker.submission_attempts.get(order_id, 0)}")
            return True

        except (OrderProcessingError, BrokerError) as e:
            self.logger.error(f"Error submitting order {order_id}: {e}")
            if order: self._handle_rejection(order, str(e)) # This will also call submission_tracker.remove_tracking
            self._record_error(str(e), e)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error submitting order {order_id}: {e}", exc_info=True)
            if order: self._handle_rejection(order, f"System error: {str(e)}")
            self._record_error(str(e), e)
            return False


    def modify_order(self, order_id: str, **kwargs) -> bool:
        order = self.orders.get(order_id)
        if not order:
            self.logger.error(f"Cannot modify: Order {order_id} not found.")
            return False
        try:
            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING]: # Allow modify on PENDING too
                raise OrderProcessingError(f"Cannot modify order {order_id} with status {order.status.value}")

            modification_details = {}
            new_quantity = kwargs.get('quantity')
            new_price = kwargs.get('price')
            new_stop_price = kwargs.get('stop_price') # models.Order uses stop_price

            if new_quantity is not None and float(new_quantity) != order.quantity :
                modification_details['quantity'] = float(new_quantity)
            if new_price is not None and float(new_price) != order.price:
                modification_details['price'] = float(new_price)
            if new_stop_price is not None and float(new_stop_price) != getattr(order, 'stop_price', None):
                modification_details['trigger_price'] = float(new_stop_price) # For OrderEvent

            if not modification_details:
                self.logger.warning(f"No changes detected for modify_order {order_id}. Ignoring.")
                return False

            # Optimistically set to PENDING_REPLACE, broker confirmation will follow
            if not self._validate_state_transition(order, OrderStatus.PENDING_REPLACE):
                 raise OrderProcessingError(f"Cannot transition order {order_id} from {order.status.value} to PENDING_REPLACE.")

            order.status = OrderStatus.PENDING_REPLACE
            order.last_updated = datetime.now()
            self._record_order_lifecycle_event(order_id, "PENDING_REPLACE", f"Modification request: {modification_details}")

            # Publish OrderEvent with MODIFY action. ExecutionHandler will pick this up.
            # The event should carry the new values in its fields, and original values in metadata if needed.
            # For simplicity, we assume the OrderEvent fields (price, quantity, trigger_price) are the target values.
            # OrderManager's local order object is not yet updated with these values, only its status.
            # The actual update to price/qty on the 'order' object should happen after broker confirmation via ExecutionEvent.

            temp_event_params = order.params.copy() if order.params else {}
            temp_event_params.update(modification_details) # Add modification details to metadata for EH

            # Create a snapshot of the order *as if* modified for the event
            # This is tricky. The event should signal the *intent* to modify to these new values.
            self._publish_order_event(
                order, # Pass the current order
                OrderActionType.MODIFY.value,
                # The event itself will carry new price/qty if we set them directly on event fields
                # For now, metadata carries the changes. ExecutionHandler needs to extract these.
                # Let's ensure the event carries the *target* values for key fields if possible
                # This means the _publish_order_event needs to be flexible or we construct event manually here.
            )
            # The OrderEvent published should have the *new* price/quantity if they are part of the event's direct fields.
            # Let's assume _publish_order_event takes the order object and the modification_details are in order.params for the event.
            # The ExecutionHandler will use these from the event.

            self.stats["orders_modified"] += 1
            self.logger.info(f"Order {order_id} modification requested: {modification_details}. Status set to PENDING_REPLACE.")
            return True

        except (OrderProcessingError, ValueError) as e:
            self.logger.error(f"Error requesting modification for order {order_id}: {e}")
            # Revert status if it was PENDING_REPLACE but failed locally
            if order and order.status == OrderStatus.PENDING_REPLACE:
                # This needs careful thought: what was the prior status?
                # For now, assume it stays PENDING_REPLACE and broker will reject.
                pass
            self._record_error(str(e), e)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error modifying order {order_id}: {e}", exc_info=True)
            self._record_error(str(e), e)
            return False

    def cancel_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if not order:
            self.logger.error(f"Cannot cancel: Order {order_id} not found.")
            return False
        try:
            # Allow cancellation for CREATED, PENDING, SUBMITTED, OPEN, PARTIALLY_FILLED
            if order.status not in [OrderStatus.CREATED, OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                # If already in a terminal state or pending cancel, log and exit
                if order.status in [OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.PENDING_CANCEL]:
                    self.logger.info(f"Order {order_id} is already in status {order.status.value}. No action needed for cancel request.")
                    return True # Or False if we consider it a "failed" new request
                raise OrderProcessingError(f"Cannot cancel order {order_id} with status {order.status.value}")

            # If order is only CREATED, can cancel locally without broker
            if order.status == OrderStatus.CREATED:
                self._handle_cancellation(order, "Cancelled locally before submission")
                return True

            if not self._validate_state_transition(order, OrderStatus.PENDING_CANCEL):
                 raise OrderProcessingError(f"Cannot transition order {order_id} from {order.status.value} to PENDING_CANCEL.")

            order.status = OrderStatus.PENDING_CANCEL
            order.last_updated = datetime.now()
            self._record_order_lifecycle_event(order_id, "PENDING_CANCEL", "Cancellation requested for broker")
            self._publish_order_event(order, OrderActionType.CANCEL.value) # Notify ExecutionHandler
            self.logger.info(f"Order {order_id} cancellation requested. Status set to PENDING_CANCEL.")
            return True

        except OrderProcessingError as e:
            self.logger.error(f"Error requesting cancellation for order {order_id}: {e}")
            self._record_error(str(e), e)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error cancelling order {order_id}: {e}", exc_info=True)
            self._record_error(str(e), e)
            return False

    def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        order = self.orders.get(order_id)
        if not order and self.broker_to_internal_id: # Check if it's a known broker_id
            for b_id, int_id in self.broker_to_internal_id.items():
                if b_id == order_id: # User might query with broker_id
                    order = self.orders.get(int_id)
                    break
        return order.status if order else None

    def get_order_lifecycle(self, order_id: str) -> List[Dict[str, Any]]:
        order = self.orders.get(order_id) # Check by internal ID first
        if not order and self.broker_to_internal_id:
             for b_id, int_id in self.broker_to_internal_id.items():
                if b_id == order_id: # User might query with broker_id
                    order_id = int_id # Use internal ID for lifecycle lookup
                    break
        return self.order_lifecycle.get(order_id, [])

    def get_stats(self) -> Dict[str, Any]:
        current_stats = self.stats.copy()
        current_stats["current_total_orders"] = len(self.orders)
        current_stats["current_pending_om_queue"] = len(self.pending_orders) # Orders OM has marked PENDING for EH
        current_stats["current_live_tracked_by_submission_tracker"] = len(self.submission_tracker.submitted_orders)
        current_stats["current_attempting_retry_by_submission_tracker"] = len(self.submission_tracker.submission_attempts)

        current_stats["current_filled_orders_dict_len"] = len(self.filled_orders)
        current_stats["current_cancelled_orders_dict_len"] = len(self.cancelled_orders)
        current_stats["current_rejected_orders_dict_len"] = len(self.rejected_orders)
        return current_stats

    def get_order_batch(self, status: Optional[OrderStatus] = None,
                        strategy_id: Optional[str] = None,
                        instrument_id: Optional[str] = None) -> List[Order]:
        result = []
        for order in self.orders.values():
            if status and order.status != status: continue
            if strategy_id and order.strategy_id != strategy_id: continue
            if instrument_id and order.instrument_id != instrument_id: continue
            result.append(order)
        return result
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Retrieve an order by its internal ID."""
        return self.orders.get(order_id)
    
    def get_orders_by_symbol(self, symbol: str) -> Dict[str, Order]:
        """Retrieve all orders for a specific symbol."""
        return {oid: order for oid, order in self.orders.items() if order.symbol == symbol}
    
    def get_active_orders(self) -> Dict[str, Order]:
        """Retrieve all active (non-terminal) orders."""
        active_statuses = {OrderStatus.PENDING_NEW, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}
        return {oid: order for oid, order in self.orders.items() if order.status in active_statuses}   
   

    def stop(self):
        """Stop the reprocessing thread and cleanup."""
        self.logger.info("Stopping OrderManager and its reprocessing thread.")
        self._is_running = False       
        
        self.logger.info("OrderManager stopped successfully.")
    

