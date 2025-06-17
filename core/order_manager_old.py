import uuid
import logging
import traceback
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
import time

from models.order import Order, OrderAction, OrderValidationError, OrderActionType
from models.trade import Trade
from utils.constants import EventType, OrderType as ModelOrderType # This is likely models.order.OrderType
from utils.constants import Exchange, OrderStatus, OrderSide, OrderType as ConstantsOrderType # This is from utils.constants
from utils.exceptions import OrderError, BrokerError
from models.events import OrderEvent, Event, EventValidationError, SignalEvent, FillEvent, ExecutionEvent

class OrderProcessingError(Exception):
    """Exception raised when order processing fails"""
    pass

class OrderManager:
    """Manages order lifecycle, from creation to execution and tracking"""

    def __init__(self, event_manager, risk_manager=None, broker_interface=None):
        self.logger = logging.getLogger("core.order_manager")
        self.event_manager = event_manager
        self.risk_manager = risk_manager
        self.broker_interface = broker_interface
        
        self.orders: Dict[str, Order] = {}
        self.pending_orders: Dict[str, Order] = {}
        self.filled_orders: Dict[str, Order] = {}
        self.cancelled_orders: Dict[str, Order] = {}
        self.rejected_orders: Dict[str, Order] = {}
        
        self.order_lifecycle: Dict[str, List[Dict[str, Any]]] = {}
        
        self.stats = {
            "orders_created": 0, "orders_submitted": 0, "orders_filled": 0,
            "orders_cancelled": 0, "orders_rejected": 0, "orders_modified": 0,
            "errors": 0, "last_error": None, "last_error_time": None,
            "retry_attempts": 0, "retry_successes": 0
        }
        
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

    def _on_signal_event(self, event: SignalEvent):
        try:
            self.logger.debug(f"Received signal event: {event}")
            if not isinstance(event, SignalEvent): # Basic type check
                self.logger.warning(f"Invalid signal event type received: {type(event)}")
                return
            event.validate() # Use built-in validation

            if self.risk_manager and hasattr(self.risk_manager, 'check_signal'):
                if not self.risk_manager.check_signal(event):
                    self.logger.info(f"Signal {event.event_id} rejected by risk manager for strategy {event.strategy_id}")
                    return
            
            order = self._create_order_from_signal(event)
            if order:
                self.stats["orders_created"] += 1
                self._record_order_lifecycle_event(order.order_id, "CREATED", f"Created from signal {event.event_id}")
                self.logger.info(f"Order {order.order_id} created from signal {event.event_id}")
                self._publish_order_event(order, OrderActionType.CREATE.value) # Use enum value
        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing signal event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling signal event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)


    def _on_order_event(self, event: OrderEvent):
        try:
            self.logger.debug(f"Received order event: {event}")
            if not isinstance(event, OrderEvent):
                self.logger.warning(f"Invalid order event type received: {type(event)}")
                return
            event.validate()

            order_id = event.order_id
            order = self.orders.get(order_id)

            if not order:
                if event.status == OrderStatus.CREATED and event.action == OrderActionType.CREATE.value:
                    self.logger.debug(f"OrderEvent for CREATED order {order_id} received. Assuming it was just created by this OM.")
                    # Potentially reconstruct or fetch if necessary, but for now, assume it's handled by submit_order if it gets there.
                else:
                    self.logger.warning(f"OrderManager received OrderEvent for an untracked or unknown order_id: {order_id}. Status: {event.status}, Action: {event.action}")
                    return

            # Update local order from the event IF the event is more recent or provides new info
            # This is crucial if OrderEvents can come from external sources.
            # For now, the primary update logic for order attributes is within specific handlers like _on_execution_event.
            # _update_local_order_from_event is a general updater.
            if order: # Ensure order object exists before updating
                 self._update_local_order_from_event(order, event)


            action_str = str(event.action).upper() if event.action else None
            # Ensure current_order_status is derived from event.status correctly
            current_order_status: OrderStatus
            if isinstance(event.status, OrderStatus):
                current_order_status = event.status
            elif hasattr(event.status, 'value') and isinstance(event.status.value, str):
                try:
                    current_order_status = OrderStatus(str(event.status.value).upper())
                except ValueError:
                    self.logger.error(f"Invalid OrderStatus value in event: {event.status.value} for order {order_id}")
                    return
            else: 
                try:
                    current_order_status = OrderStatus(str(event.status).upper())
                except ValueError:
                    self.logger.error(f"Invalid OrderStatus string in event: {event.status} for order {order_id}")
                    return

            # Re-fetch order as _update_local_order_from_event might have changed its status or other attributes
            order = self.orders.get(order_id) 
            if not order and action_str not in [OrderActionType.CREATE.value]:
                self.logger.warning(f"Order {order_id} not found for action '{action_str}' after update attempt. Ignoring event.")
                return

            if action_str == OrderActionType.CREATE.value:
                if order: 
                    self.submit_order(order.order_id)
                else:
                    self.logger.error(f"Order {order_id} not found in OrderManager despite CREATED action event.")
            elif action_str == OrderActionType.SUBMIT.value:
                 if order: self.submit_order(order.order_id)
            elif action_str == OrderActionType.CANCEL.value:
                if order: self.cancel_order(order.order_id)
            elif action_str == OrderActionType.MODIFY.value:
                if order:
                    modify_params = event.metadata or {} 
                    self.modify_order(order.order_id, **modify_params)
            elif action_str == OrderActionType.PENDING.value or current_order_status == OrderStatus.PENDING:
                if order:
                    self.logger.info(f"OrderManager: Order {order_id} is PENDING. Ensuring it's tracked.")
                    # _update_local_order_from_event already called
                    if order_id not in self.pending_orders: self.pending_orders[order_id] = order
                    self._record_order_lifecycle_event(order_id, "PENDING_ACTION_RECEIVED", f"Received PENDING. Current local status: {order.status.value}")
            elif action_str == OrderActionType.SUBMIT.value or current_order_status == OrderStatus.SUBMITTED: # Corrected from SUBMIT
                 if order:
                    self.logger.info(f"OrderManager: Order {order_id} is SUBMITTED. Updating tracking.")
                    # _update_local_order_from_event already called
                    if order_id in self.pending_orders: del self.pending_orders[order_id]
                    self._record_order_lifecycle_event(order_id, "SUBMITTED_ACTION_RECEIVED", f"Broker Order ID: {order.broker_order_id or 'N/A'}")
            elif current_order_status == OrderStatus.REJECTED:
                if order:
                    # _update_local_order_from_event already called
                    self._handle_rejection(order, event.rejection_reason or "Unknown reason from OrderEvent")
            elif action_str == OrderActionType.UPDATE_STATUS.value:
                 if order and hasattr(event, 'status'):
                      # _update_local_order_from_event already called
                      self.logger.info(f"Order {order.order_id} status updated to {order.status.value} via STATUS_UPDATE action.")
            else:
                self.logger.debug(f"OrderManager: OrderEvent for {order_id} with unhandled action/status. Action: '{action_str}', Status: '{current_order_status.value}'")

        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing order event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e: # Catch specific exceptions like ValueError for enum conversion
            self.logger.error(f"Unexpected error handling order event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _update_local_order_from_event(self, order: Order, event: OrderEvent):
        """Updates the local Order object with information from an OrderEvent."""
        if not order or not event: return

        try:
            new_status = OrderStatus(event.status.value) if isinstance(event.status, Enum) else OrderStatus(str(event.status).upper())
            order.status = new_status
        except ValueError as e:
            self.logger.error(f"Invalid status value '{event.status}' in OrderEvent for order {order.order_id}: {e}")
            # Potentially skip status update or set to UNKNOWN
            return


        order.broker_order_id = event.broker_order_id or order.broker_order_id
        order.filled_quantity = event.filled_quantity if event.filled_quantity is not None else order.filled_quantity
        
        # average_price from OrderEvent maps to average_fill_price in Order model
        order.average_fill_price = event.average_price if event.average_price is not None else order.average_fill_price
        
        order.rejection_reason = event.rejection_reason if event.rejection_reason else 'Unknown'
        
        # FIX: Convert millisecond timestamp from OrderEvent to seconds for datetime.fromtimestamp
        if event.event_type == EventType.ORDER and event.timestamp > (time.time() * 100): # Heuristic: if timestamp is like ms
            try:
                order.last_updated = datetime.fromtimestamp(event.timestamp / 1000.0)
            except ValueError as e: # Handle potential "year is out of range" if division still results in bad ts
                self.logger.error(f"Timestamp conversion error for OrderEvent {event.event_id} (ts: {event.timestamp}): {e}. Using current time.")
                order.last_updated = datetime.now()
        elif event.timestamp: # For other events or if OrderEvent.timestamp is already seconds
             try:
                order.last_updated = datetime.fromtimestamp(event.timestamp)
             except ValueError as e:
                self.logger.error(f"Timestamp conversion error for event {event.event_id} (ts: {event.timestamp}): {e}. Using current time.")
                order.last_updated = datetime.now()
        else:
            order.last_updated = datetime.now()
        
        if hasattr(event, 'metadata') and event.metadata and hasattr(order, 'params'):
            if order.params is None: order.params = {}
            order.params.update(event.metadata)

        self.logger.debug(f"Updated local order {order.order_id} from OrderEvent. New status: {order.status.value}")


    def _handle_rejection(self, order: Order, reason: Optional[str]):
        """Handles an order rejection."""
        order.status = OrderStatus.REJECTED
        order.rejection_reason = reason or "Unknown"
        order.last_updated = datetime.now()
        
        if order.order_id in self.pending_orders:
            del self.pending_orders[order.order_id]
        self.rejected_orders[order.order_id] = order
        
        self._record_order_lifecycle_event(order.order_id, "REJECTED", reason)
        self.stats["orders_rejected"] += 1
        self.logger.info(f"Order {order.order_id} REJECTED. Reason: {reason}")
        self._publish_order_event(order, OrderStatus.REJECTED.value) # Publish status update

    def _on_execution_event(self, event: ExecutionEvent):
        try:
            self.logger.debug(f"Received execution event: {event}")
            if not isinstance(event, ExecutionEvent):
                self.logger.warning(f"Invalid execution event type: {type(event)}")
                return
            event.validate()

            order_id = event.order_id
            order = self.orders.get(order_id)

            if not order:
                self.logger.warning(f"Received ExecutionEvent for unknown order_id: {order_id}")
                return

            self._update_local_order_from_execution_event(order, event)

            execution_status = OrderStatus(event.status.value) if isinstance(event.status, Enum) else OrderStatus(str(event.status).upper())
            exec_type = getattr(event, 'execution_type', 'UPDATE') 
            
            self._record_order_lifecycle_event(order_id, f"EXECUTION_{exec_type}", 
                                              f"ExecID: {event.execution_id}, Status: {execution_status.value}, BrokerID: {event.broker_order_id or 'N/A'}")

            if execution_status == OrderStatus.REJECTED:
                self._handle_rejection(order, event.rejection_reason or f"Rejected by broker execution: {event.text or 'No reason specified'}")
            elif execution_status == OrderStatus.CANCELLED:
                self._handle_cancellation(order, "Cancelled by broker execution")
            elif execution_status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
                if event.last_filled_quantity is not None and event.last_filled_quantity > 0:
                    self._create_fill_from_execution(event) 
                else: 
                    self.logger.info(f"ExecutionEvent indicates status {execution_status.value} for order {order_id} but no specific fill quantity in this event. Waiting for separate FillEvent if applicable.")
            
            # Publish an OrderEvent if the status of the local order object was changed by the ExecutionEvent
            # The _update_local_order_from_execution_event method changes order.status
            # We need to compare the order's status *before* the update with its status *after* the update.
            # For simplicity, if the execution_status implies a change, we publish.
            # A more robust way is to store old_status before calling _update_local_order_from_execution_event.
            # However, _publish_order_event will use the *current* state of 'order'.
            self._publish_order_event(order, OrderActionType.UPDATE_STATUS.value)


        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing execution event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling execution event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _update_local_order_from_execution_event(self, order: Order, event: ExecutionEvent):
        """Updates the local Order object with information from an ExecutionEvent."""
        if not order or not event: return

        try:
            new_status = OrderStatus(event.status.value) if isinstance(event.status, Enum) else OrderStatus(str(event.status).upper())
        except ValueError as e:
            self.logger.error(f"Invalid status value '{event.status}' in ExecutionEvent for order {order.order_id}: {e}")
            return # Skip update if status is invalid

        if order.status != new_status:
            self.logger.info(f"Order {order.order_id} status changing from {order.status.value} to {new_status.value} based on ExecutionEvent {event.execution_id}")
            order.status = new_status

        order.broker_order_id = event.broker_order_id or order.broker_order_id
        
        # ExecutionEvent.timestamp is float (seconds)
        order.last_updated = datetime.fromtimestamp(event.timestamp) if event.timestamp else datetime.now()
        
        order.rejection_reason = event.rejection_reason if event.rejection_reason else 'Unknown'
        
        if event.cumulative_filled_quantity is not None and event.cumulative_filled_quantity > order.filled_quantity:
            order.filled_quantity = event.cumulative_filled_quantity
        
        # FIX: Access event.average_price instead of event.average_filled_price
        if event.average_price is not None and event.cumulative_filled_quantity is not None and event.cumulative_filled_quantity > 0:
            order.average_fill_price = event.average_price # Order model uses average_fill_price

        if hasattr(event, 'metadata') and event.metadata and hasattr(order, 'params'):
            if order.params is None: order.params = {}
            order.params.update(event.metadata)
            
        self.logger.debug(f"Updated local order {order.order_id} from ExecutionEvent. New status: {order.status.value}, Filled Qty: {order.filled_quantity}, AvgPx: {order.average_fill_price}")


    def _create_fill_from_execution(self, exec_event: ExecutionEvent):
        try:
            if not (exec_event.last_filled_quantity is not None and exec_event.last_filled_quantity > 0 and exec_event.last_filled_price is not None):
                self.logger.debug(f"ExecutionEvent {exec_event.execution_id} for order {exec_event.order_id} does not contain new fill data. Skipping FillEvent creation.")
                return

            fill_event = FillEvent(
                timestamp=exec_event.execution_time if exec_event.execution_time is not None else exec_event.timestamp,
                order_id=exec_event.order_id,
                symbol=exec_event.symbol,
                exchange=Exchange(exec_event.exchange.value) if isinstance(exec_event.exchange, Enum) else Exchange(str(exec_event.exchange).upper()),
                side=OrderSide(exec_event.side.value) if isinstance(exec_event.side, Enum) else OrderSide(str(exec_event.side).upper()),
                quantity=exec_event.last_filled_quantity,
                price=exec_event.last_filled_price,
                commission=exec_event.commission or 0.0,
                fill_time=exec_event.execution_time if exec_event.execution_time is not None else exec_event.timestamp,
                strategy_id=exec_event.strategy_id,
                broker_order_id=exec_event.broker_order_id,
                execution_id=exec_event.execution_id,
            )
            fill_event.validate()
            self.event_manager.publish(fill_event)
            self.logger.info(f"Created FillEvent {fill_event.fill_id} from ExecutionEvent {exec_event.execution_id} for order {exec_event.order_id}")
        except (EventValidationError, OrderValidationError, ValueError) as e: # Added ValueError for enum
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

            order_id = event.order_id
            order = self.orders.get(order_id)

            if not order:
                self.logger.warning(f"Received FillEvent for unknown order_id: {order_id}")
                return

            order.update_fill(
                fill_quantity=event.quantity, 
                fill_price=event.price,       
                timestamp=datetime.fromtimestamp(event.fill_time) if event.fill_time else datetime.now()
            )
            if hasattr(event, 'commission') and event.commission is not None: # Check for None
                 order.total_commission = getattr(order, 'total_commission', 0.0) + event.commission

            self._record_order_lifecycle_event(order_id, f"FILL_PROCESSED (Status: {order.status.value})", 
                                              f"FillID: {event.fill_id}, Qty: {event.quantity}@{event.price}, TotalFilled: {order.filled_quantity}/{order.quantity}")

            if order.status == OrderStatus.FILLED:
                if order_id in self.pending_orders: del self.pending_orders[order_id]
                self.filled_orders[order_id] = order
                self.stats["orders_filled"] += 1 
                self.logger.info(f"Order {order_id} fully FILLED. AvgPx: {order.average_fill_price:.2f}")
            elif order.status == OrderStatus.PARTIALLY_FILLED:
                self.logger.info(f"Order {order_id} PARTIALLY_FILLED. Filled: {order.filled_quantity}/{order.quantity}, AvgPx: {order.average_fill_price:.2f}")
            
            self._publish_order_event(order, OrderActionType.UPDATE_FILL.value)

        except (EventValidationError, OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Error processing fill event {getattr(event, 'event_id', 'N/A')}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Unexpected error handling fill event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _on_risk_breach_event(self, event): # RiskBreachEvent (assuming type hint)
        try:
            # Assuming event is already validated if it's a specific type like RiskBreachEvent
            self.logger.warning(f"Received risk breach event: {event}")
            if not isinstance(event, Event) or event.event_type != EventType.RISK_BREACH:
                self.logger.warning(f"Invalid risk breach event type: {type(event)}")
                return

            symbol_to_act_on = getattr(event, 'symbol', None)
            strategy_to_act_on = getattr(event, 'strategy_id', None)
            orders_to_cancel_ids = []

            for order_id, order_obj in list(self.pending_orders.items()): # Iterate over copy for safe removal
                matches_symbol = (symbol_to_act_on and order_obj.instrument_id == symbol_to_act_on)
                matches_strategy = (strategy_to_act_on and order_obj.strategy_id == strategy_to_act_on)
                
                should_cancel = False
                if symbol_to_act_on and strategy_to_act_on:
                    if matches_symbol and matches_strategy: should_cancel = True
                elif symbol_to_act_on:
                    if matches_symbol: should_cancel = True
                elif strategy_to_act_on:
                    if matches_strategy: should_cancel = True
                # Add logic for "all" if needed, e.g. if event has a field like 'scope'='all'

                if should_cancel:
                    orders_to_cancel_ids.append(order_id)
            
            for order_id_to_cancel in orders_to_cancel_ids:
                try:
                    self.cancel_order(order_id_to_cancel) 
                    self.logger.info(f"Cancelled order {order_id_to_cancel} due to risk breach event {getattr(event, 'event_id', 'N/A')}")
                except Exception as e_cancel:
                    self.logger.error(f"Failed to cancel order {order_id_to_cancel} during risk breach handling: {e_cancel}")
                    self._record_error(str(e_cancel), e_cancel)
        except Exception as e:
            self.logger.error(f"Error handling risk breach event {getattr(event, 'event_id', 'N/A')}: {e}", exc_info=True)
            self._record_error(str(e), e)
    
    def _create_order_from_signal(self, signal: SignalEvent) -> Optional[Order]:
        if not (signal.symbol and signal.signal_type and signal.side and signal.quantity is not None and signal.order_type):
            self.logger.error(f"SignalEvent {signal.event_id} is missing required fields for order creation.")
            raise OrderValidationError("SignalEvent missing required fields.")

        # Metadata from signal is crucial for ExecutionHandler
        order_params = signal.metadata.copy() if signal.metadata else {}
        # Add other relevant signal fields to params if needed by broker/execution
        order_params['signal_price_ref'] = signal.price # Price of option at signal time
        order_params['underlying_price_at_signal'] = signal.signal_price # Price of underlying
        if hasattr(signal, 'time_in_force') and signal.time_in_force:
            order_params['time_in_force'] = signal.time_in_force


        try:
            # create_order expects side as string, order_type as ModelOrderType enum (or string convertible to it)
            # signal.order_type is utils.constants.OrderType (FrameworkOrderType)
            # signal.side is utils.constants.OrderSide
            order = self.create_order(
                strategy_id=signal.strategy_id,
                instrument_id=signal.symbol,
                quantity=signal.quantity,
                price=signal.price, # This is the order price (e.g. limit price for option)
                side=signal.side.value, 
                order_type=signal.order_type, # Pass ConstantsOrderType, create_order handles it
                exchange=signal.exchange.value if isinstance(signal.exchange, Enum) else signal.exchange,
                params=order_params # Pass metadata from signal here
            )
            return order
        except (OrderValidationError, OrderProcessingError) as e:
            self.logger.error(f"Failed to create order from signal {signal.event_id} due to: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in _create_order_from_signal for signal {signal.event_id}: {e}", exc_info=True)
            return None

    def _publish_order_event(self, order: Order, action: Optional[str] = None):
        if not order: return
        try:
            # Ensure enums are correctly passed or converted for OrderEvent
            side_enum = order.side if isinstance(order.side, OrderSide) else OrderSide(str(order.side).upper())
            
            # order.order_type is models.order.OrderType (ModelOrderType)
            # OrderEvent expects utils.constants.OrderType (FrameworkOrderType)
            # Assuming they have compatible .value string representations
            order_type_for_event = ConstantsOrderType(order.order_type.value)

            status_for_event = order.status # Should be OrderStatus enum

            event = OrderEvent(
                order_id=order.order_id,
                symbol=order.instrument_id,
                exchange=order.exchange if isinstance(order.exchange, Enum) else (Exchange(str(order.exchange).upper()) if order.exchange else None),
                side=side_enum,
                quantity=order.quantity,
                order_type=order_type_for_event,
                status=status_for_event,
                price=order.price,
                trigger_price=getattr(order, 'stop_price', None), # models.Order uses stop_price
                filled_quantity=order.filled_quantity,
                remaining_quantity=order.remaining_quantity(),
                average_price=order.average_fill_price,
                order_time=order.created_at.timestamp() if order.created_at else time.time(),
                last_update_time=order.last_updated.timestamp() if order.last_updated else time.time(),
                strategy_id=order.strategy_id,
                client_order_id=getattr(order, 'client_order_id', None), # If your Order model has this
                broker_order_id=order.broker_order_id,
                rejection_reason=getattr(order, 'rejection_reason', None), # If your Order model has this
                action=action or (order.status.value if isinstance(order.status, Enum) else str(order.status)),
                time_in_force=getattr(order, 'time_in_force', None),
                metadata=getattr(order, 'params', None) or {}, # Populate metadata from order.params
                event_type=EventType.ORDER, # This is set by Event dataclass default if not overridden
                timestamp=time.time() # Use current time in seconds
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

    def _publish_execution_event(self, order: Order, exec_type: str = "NEW", text: Optional[str] = None):
        if not order: return
        try:
            side_enum = order.side if isinstance(order.side, OrderSide) else OrderSide(str(order.side).upper())
            order_type_for_event = ConstantsOrderType(order.order_type.value) # Map models.order.OrderType to utils.constants.OrderType
            status_for_event = order.status # Should be OrderStatus enum

            exec_event = ExecutionEvent(
                order_id=order.order_id,
                symbol=order.instrument_id,
                exchange=order.exchange if isinstance(order.exchange, Enum) else (Exchange(str(order.exchange).upper()) if order.exchange else None),
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
                broker_order_id=order.broker_order_id,
                rejection_reason=getattr(order, 'rejection_reason', None),
                execution_id=f"exec-{uuid.uuid4()}", # Generate a new one for this event
                execution_time=time.time(),
                execution_type=exec_type,
                text=text,
                metadata=getattr(order, 'params', None) or {}, # Pass metadata
                timestamp=time.time() # Event's own timestamp
            )
            exec_event.validate()
            self.event_manager.publish(exec_event)
            self.logger.debug(f"Published ExecutionEvent: OrderID={order.order_id}, ExecType={exec_type}, Status={status_for_event.value}")
        except (EventValidationError, OrderValidationError) as e:
            self.logger.error(f"ExecutionEvent validation failed for order {order.order_id}: {e}")
            self._record_error(str(e), e)
        except Exception as e:
            self.logger.error(f"Error publishing ExecutionEvent for order {order.order_id}: {e}", exc_info=True)
            self._record_error(str(e), e)

    def _standardize_order_status(self, status_val: Any) -> OrderStatus:
        if isinstance(status_val, OrderStatus): return status_val
        if isinstance(status_val, Enum): status_val = status_val.value # Get string value if it's some other enum
        if isinstance(status_val, str):
            try: return OrderStatus(status_val.upper())
            except ValueError: pass
        self.logger.warning(f"Could not standardize status '{status_val}' (type: {type(status_val)}) to OrderStatus enum. Defaulting to UNKNOWN.")
        return OrderStatus.UNKNOWN


    def _handle_cancellation(self, order: Order, reason: Optional[str]):
        """Handles an order cancellation."""
        order.status = OrderStatus.CANCELLED
        order.canceled_at = datetime.now()
        order.last_updated = order.canceled_at
        
        if order.order_id in self.pending_orders:
            del self.pending_orders[order.order_id]
        self.cancelled_orders[order.order_id] = order
        
        self._record_order_lifecycle_event(order.order_id, "CANCELLED", reason or "Cancelled")
        self.stats["orders_cancelled"] += 1
        self.logger.info(f"Order {order.order_id} CANCELLED. Reason: {reason}")
        self._publish_order_event(order, OrderStatus.CANCELLED.value)


    def create_order(self, strategy_id: str, instrument_id: str, quantity: float, price: Optional[float],
                     side: str, order_type: Union[ConstantsOrderType, ModelOrderType, str], 
                     exchange: Optional[Union[Exchange, str]] = None,
                     params: Optional[Dict[str, Any]] = None) -> Optional[Order]:
        try:
            order_side_str = side.upper()
            if order_side_str not in [s.value for s in OrderSide]:
                raise OrderValidationError(f"Invalid order side: {side}")

            # Convert order_type to ModelOrderType (the one used by models.order.Order)
            final_order_type: ModelOrderType
            if isinstance(order_type, ModelOrderType):
                final_order_type = order_type
            elif isinstance(order_type, ConstantsOrderType): # This is utils.constants.OrderType
                final_order_type = ModelOrderType(order_type.value)
            elif isinstance(order_type, str):
                final_order_type = ModelOrderType(order_type.upper())
            else:
                raise OrderValidationError(f"Unsupported order_type type: {type(order_type)}")

            final_exchange_str: Optional[str] = None
            if isinstance(exchange, Exchange):
                final_exchange_str = exchange.value
            elif isinstance(exchange, str):
                final_exchange_str = exchange.upper()
            
            if not final_exchange_str:
                 # Try to infer from instrument_id if possible, e.g. "NSE:SBIN"
                if ':' in instrument_id:
                    final_exchange_str = instrument_id.split(':', 1)[0].upper()
                if not final_exchange_str:
                    raise OrderValidationError(f"Exchange is required for instrument {instrument_id}")


            order = Order(
                instrument_id=instrument_id,
                quantity=quantity,
                side=order_side_str, # models.Order expects string
                order_type=final_order_type, # models.Order expects its own OrderType enum
                price=price,
                strategy_id=strategy_id,
                exchange=final_exchange_str, # models.Order expects string
                params=params or {} # Pass params (which contains metadata from signal)
            )
            # order.validate() # models.Order.validate is called in its __init__

            self.orders[order.order_id] = order
            self._record_order_lifecycle_event(order.order_id, "CREATED", f"Strategy: {strategy_id}, Type: {order.order_type.value}, Qty: {quantity}")
            self.logger.info(f"Created order: {order}")
            return order
        except (OrderValidationError, ValueError) as e: # ValueError for enum conversion
            self.logger.error(f"Order creation/validation failed: {e}", exc_info=True)
            self._record_error(str(e), e)
            return None # Explicitly return None on failure
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
            if order.status != OrderStatus.CREATED:
                # Allow re-submission if PENDING (e.g. if previous submission attempt failed before broker ack)
                if order.status == OrderStatus.PENDING:
                    self.logger.warning(f"Order {order_id} is already PENDING. Attempting to re-publish PENDING event for ExecutionHandler.")
                else:
                    raise OrderProcessingError(f"Cannot submit order {order_id} with status {order.status.value}")

            order.status = OrderStatus.PENDING
            order.last_updated = datetime.now()
            self.pending_orders[order.order_id] = order
            self._record_order_lifecycle_event(order.order_id, "PENDING", "Queued for submission to broker")
            self._publish_order_event(order, OrderActionType.PENDING.value) # Publish PENDING OrderEvent
            
            # ExecutionHandler will pick up the PENDING OrderEvent and attempt actual submission.
            self.logger.info(f"Order {order_id} moved to PENDING state. Awaiting processing by ExecutionHandler.")
            return True

        except (OrderProcessingError, BrokerError) as e: # BrokerError if OM submits directly
            self.logger.error(f"Error submitting order {order_id}: {e}")
            if order: 
                order.status = OrderStatus.REJECTED
                order.rejection_reason = str(e)
                order.last_updated = datetime.now()
                self._handle_rejection(order, str(e)) 
            self._record_error(str(e), e)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error submitting order {order_id}: {e}", exc_info=True)
            if order:
                order.status = OrderStatus.REJECTED
                order.rejection_reason = f"System error: {str(e)}"
                order.last_updated = datetime.now()
                self._handle_rejection(order, f"System error: {str(e)}")
            self._record_error(str(e), e)
            return False


    def modify_order(self, order_id: str, **kwargs) -> bool:
        order = self.orders.get(order_id)
        if not order:
            self.logger.error(f"Cannot modify: Order {order_id} not found.")
            return False
        try:
            # Check if order is in a modifiable state
            if order.status not in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                raise OrderProcessingError(f"Cannot modify order {order_id} with status {order.status.value}")

            # Store original values if needed for rollback or detailed logging
            # original_values = {k: getattr(order, k) for k in kwargs if hasattr(order, k)}
            
            # Update local order object with new parameters from kwargs
            # This is a preview of changes; actual broker modification is separate.
            # The OrderEvent's metadata should carry these modification details.
            modification_details = {}
            if 'quantity' in kwargs:
                order.quantity = float(kwargs['quantity'])
                modification_details['quantity'] = order.quantity
            if 'price' in kwargs:
                order.price = float(kwargs['price'])
                modification_details['price'] = order.price
            if 'stop_price' in kwargs: # models.Order uses stop_price
                order.stop_price = float(kwargs['stop_price'])
                modification_details['trigger_price'] = order.stop_price # For OrderEvent
            
            # Add other modifiable fields as needed
            order.last_updated = datetime.now()
            
            order.status = OrderStatus.PENDING_REPLACE 
            self._record_order_lifecycle_event(order_id, "PENDING_REPLACE", f"Modification request: {modification_details}")
            
            # Publish OrderEvent with MODIFY action and modification details in metadata
            # ExecutionHandler will use this to call broker's modify_order
            event_metadata = getattr(order, 'params', {}).copy() # Start with existing params
            event_metadata.update(modification_details) # Add specific modifications

            self._publish_order_event(order, OrderActionType.MODIFY.value) 
            self.stats["orders_modified"] += 1 
            self.logger.info(f"Order {order_id} modification requested: {modification_details}. Status set to PENDING_REPLACE.")
            return True

        except (OrderProcessingError, ValueError) as e: 
            self.logger.error(f"Error requesting modification for order {order_id}: {e}")
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
            if order.status not in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                raise OrderProcessingError(f"Cannot cancel order {order_id} with status {order.status.value}")

            order.status = OrderStatus.PENDING_CANCEL
            order.last_updated = datetime.now()
            self._record_order_lifecycle_event(order_id, "PENDING_CANCEL", "Cancellation requested")
            self._publish_order_event(order, OrderActionType.CANCEL.value) 
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
        return order.status if order else None
    
    def get_order_lifecycle(self, order_id: str) -> List[Dict[str, Any]]:
        return self.order_lifecycle.get(order_id, [])
        
    def get_stats(self) -> Dict[str, Any]:
        current_stats = self.stats.copy()
        current_stats["current_total_orders"] = len(self.orders)
        current_stats["current_pending_orders"] = len(self.pending_orders)
        # Correctly count based on actual dictionaries
        current_stats["current_filled_orders"] = len(self.filled_orders)
        current_stats["current_cancelled_orders"] = len(self.cancelled_orders)
        current_stats["current_rejected_orders"] = len(self.rejected_orders)    
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


