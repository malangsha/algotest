import uuid
import logging
import traceback
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
import time

from models.order import Order, OrderStatus, OrderAction, OrderValidationError
from models.trade import Trade
from utils.exceptions import OrderError, BrokerError
from models.events import EventType, OrderEvent, Event, EventValidationError, SignalEvent, FillEvent, ExecutionEvent

class OrderProcessingError(Exception):
    """Exception raised when order processing fails"""
    pass

class OrderManager:
    """Manages order lifecycle, from creation to execution and tracking"""

    def __init__(self, event_manager, risk_manager=None, broker_interface=None):
        """
        Initialize the order manager.
        
        Args:
            event_manager: Event manager for publishing/subscribing to events
            risk_manager: Optional risk manager for checking orders
            broker_interface: Optional broker interface for order execution
        """
        self.logger = logging.getLogger("core.order_manager")
        self.event_manager = event_manager
        self.risk_manager = risk_manager
        self.broker_interface = broker_interface
        
        # Tracking order state
        self.orders = {}  # Order ID -> Order
        self.pending_orders = {}  # Order ID -> Order (only pending ones)
        self.filled_orders = {}  # Order ID -> Order (only filled ones)
        self.cancelled_orders = {}  # Order ID -> Order (only cancelled ones)
        self.rejected_orders = {}  # Order ID -> Order (only rejected ones)
        
        # Order lifecycle tracking
        self.order_lifecycle = {}  # Order ID -> List of {timestamp, status, message}
        
        # Order generation state
        self.next_order_id = 1
        
        # Register with event manager
        self._register_event_handlers()
        
        # Stats for monitoring
        self.stats = {
            "orders_created": 0,
            "orders_submitted": 0,
            "orders_filled": 0,
            "orders_cancelled": 0,
            "orders_rejected": 0,
            "orders_modified": 0,
            "errors": 0,
            "last_error": None,
            "last_error_time": None,
            "retry_attempts": 0,
            "retry_successes": 0
        }
        
        self.logger.info("Order Manager initialized")

    def _register_event_handlers(self):
        """Register to receive events from the event manager."""
        self.event_manager.subscribe(
            EventType.SIGNAL, 
            self._on_signal_event, 
            component_name="OrderManager"
        )
        
        self.event_manager.subscribe(
            EventType.ORDER, 
            self._on_order_event, 
            component_name="OrderManager"
        )
        
        self.event_manager.subscribe(
            EventType.FILL, 
            self._on_fill_event, 
            component_name="OrderManager"
        )
        
        self.event_manager.subscribe(
            EventType.EXECUTION, 
            self._on_execution_event, 
            component_name="OrderManager"
        )
        
        self.event_manager.subscribe(
            EventType.RISK_BREACH, 
            self._on_risk_breach_event, 
            component_name="OrderManager"
        )
        
        self.logger.info("Registered with Event Manager")
        
    def _record_error(self, error):
        """Record an error for monitoring"""
        self.stats["errors"] += 1
        self.stats["last_error"] = str(error)
        self.stats["last_error_time"] = datetime.now()
        self.logger.error(f"Error in OrderManager: {error}")

    def _record_order_lifecycle_event(self, order_id, status, message=None):
        """Record an event in the order lifecycle"""
        if order_id not in self.order_lifecycle:
            self.order_lifecycle[order_id] = []
            
        self.order_lifecycle[order_id].append({
            "timestamp": datetime.now(),
            "status": status,
            "message": message
        })
        
        self.logger.debug(f"Order {order_id} lifecycle: {status} - {message}")

    def _on_signal_event(self, event):
        """Handle signal events from the event manager."""
        try:
            self.logger.debug(f"Received signal event: {event}")
            
            # Validate event
            if not isinstance(event, SignalEvent) and not hasattr(event, 'signal_type'):
                self.logger.warning(f"Invalid signal event format: {event}")
                return
            
            # Process the signal to potentially create an order
            if hasattr(event, 'signal_type') and hasattr(event, 'strategy_id'):
                # Check risk if we have a risk manager
                if self.risk_manager and hasattr(self.risk_manager, 'check_signal'):
                    try:
                        if not self.risk_manager.check_signal(event):
                            self.logger.info(f"Signal rejected by risk manager: {event.strategy_id}")
                            return
                    except Exception as e:
                        self.logger.error(f"Error in risk manager check: {e}")
                        self._record_error(e)
                        # We still proceed to avoid stopping the flow completely
                
                # Convert signal to order
                try:
                    order = self._create_order_from_signal(event)
                    if order:
                        self.stats["orders_created"] += 1
                        self._record_order_lifecycle_event(order.order_id, "CREATED", f"Created from signal {event.event_id}")
                        self.logger.info(f"Order created from signal: {order.order_id}")
                        self._publish_order_event(order, 'CREATED')
                except (OrderValidationError, OrderProcessingError) as e:
                    self.logger.error(f"Failed to create order from signal: {e}")
                    self._record_error(e)
                except Exception as e:
                    self.logger.error(f"Unexpected error creating order from signal: {e}")
                    self.logger.error(traceback.format_exc())
                    self._record_error(e)
        except Exception as e:
            self.logger.error(f"Error handling signal event: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _on_order_event(self, event):
        """Handle order events from the event manager."""
        try:
            self.logger.debug(f"Received order event: {event}")
            
            # Extract order details from the event
            order = None
            order_id = None
            
            # First try to get the order_id
            if hasattr(event, 'order_id'):
                order_id = event.order_id
                
                # Try to get the order from our tracking dictionaries
                if order_id in self.orders:
                    order = self.orders[order_id]
            
            # If we couldn't get the order from order_id, try other methods
            if not order:
                if hasattr(event, 'order'):
                    order = event.order
                elif hasattr(event, 'data') and isinstance(event.data, dict) and 'order' in event.data:
                    order = event.data['order']
            
            # If we have an order but no order_id, get the order_id from the order
            if order and not order_id and hasattr(order, 'order_id'):
                order_id = order.order_id
            
            if not order or not order_id:
                self.logger.warning("Received order event without valid order data")
                return
                
            # Update order tracking
            self.orders[order_id] = order
            
            # Extract action from event
            action = None
            if hasattr(event, 'action'):
                action = event.action
            
            # Process based on action
            if action == 'SUBMIT' or action == 'CREATED':
                try:
                    self.submit_order(order)
                except Exception as e:
                    self.logger.error(f"Error submitting order: {e}")
                    self._record_error(e)
            elif action == 'CANCEL':
                try:
                    self.cancel_order(order_id)
                except Exception as e:
                    self.logger.error(f"Error cancelling order: {e}")
                    self._record_error(e)
            elif action == 'MODIFY':
                try:
                    # Extract modification parameters from event
                    modify_params = {}
                    for param in ['price', 'quantity', 'order_type', 'trigger_price']:
                        if hasattr(event, param):
                            modify_params[param] = getattr(event, param)
                    
                    if modify_params:
                        self.modify_order(order_id, **modify_params)
                    else:
                        self.logger.warning(f"Modify order event without parameters: {event}")
                except Exception as e:
                    self.logger.error(f"Error modifying order: {e}")
                    self._record_error(e)
            elif action == 'REJECTED':
                # Update order status to rejected
                order.status = OrderStatus.REJECTED
                if hasattr(event, 'rejection_reason'):
                    order.rejection_reason = event.rejection_reason
                order.last_updated = datetime.now()
                
                # Move to rejected orders
                self.rejected_orders[order_id] = order
                if order_id in self.pending_orders:
                    del self.pending_orders[order_id]
                
                self._record_order_lifecycle_event(order_id, "REJECTED", order.rejection_reason if hasattr(order, 'rejection_reason') else None)
                self.stats["orders_rejected"] += 1
                self.logger.info(f"Order rejected: {order_id}")
            elif action == 'STATUS_UPDATE':
                # Just update the order status
                if hasattr(event, 'status'):
                    self._update_order_status(order, event.status)
            else:
                self.logger.debug(f"Received order event with unhandled action: {action}")
                
        except Exception as e:
            self.logger.error(f"Error handling order event: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _on_execution_event(self, event):
        """Handle execution events from the event manager."""
        try:
            self.logger.debug(f"Received execution event: {event}")
            
            # Validate execution event
            if not isinstance(event, ExecutionEvent) and not hasattr(event, 'order_id'):
                self.logger.warning(f"Invalid execution event format: {event}")
                return
            
            # Extract order_id
            order_id = event.order_id
            
            # Check if we have this order
            if order_id not in self.orders:
                self.logger.warning(f"Received execution for unknown order: {order_id}")
                return
            
            # Get the order
            order = self.orders[order_id]
            
            # Update order status based on execution
            if hasattr(event, 'status'):
                self._update_order_status(order, event.status)
            
            # Update broker_order_id if provided
            if hasattr(event, 'broker_order_id') and event.broker_order_id:
                order.broker_order_id = event.broker_order_id
            
            # Record the execution in the order lifecycle
            execution_type = getattr(event, 'execution_type', 'UNKNOWN')
            self._record_order_lifecycle_event(order_id, f"EXECUTION_{execution_type}", 
                                              f"Execution ID: {getattr(event, 'execution_id', 'UNKNOWN')}")
            
            # If this is a fill execution, update filled quantity
            if hasattr(event, 'last_filled_quantity') and event.last_filled_quantity:
                # Create a fill event
                self._create_fill_from_execution(event)
                
        except Exception as e:
            self.logger.error(f"Error handling execution event: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _create_fill_from_execution(self, execution_event):
        """Create a fill event from an execution event."""
        try:
            # Only create fill if we have quantity information
            if not hasattr(execution_event, 'last_filled_quantity') or not execution_event.last_filled_quantity:
                return
            
            fill_event = FillEvent(
                event_type=EventType.FILL,
                timestamp=int(time.time() * 1000),
                order_id=execution_event.order_id,
                symbol=execution_event.symbol,
                exchange=getattr(execution_event, 'exchange', None),
                side=execution_event.side,
                quantity=execution_event.last_filled_quantity,
                price=execution_event.last_filled_price,
                commission=getattr(execution_event, 'commission', 0.0),
                fill_time=getattr(execution_event, 'execution_time', int(time.time() * 1000)),
                strategy_id=getattr(execution_event, 'strategy_id', None),
                broker_order_id=getattr(execution_event, 'broker_order_id', None),
                fill_id=f"fill-{uuid.uuid4()}"
            )
            
            # Validate and publish
            try:
                fill_event.validate()
                self.event_manager.publish(fill_event)
                self.logger.info(f"Created fill event from execution: {fill_event.fill_id}")
            except EventValidationError as e:
                self.logger.error(f"Fill event validation failed: {e}")
                self._record_error(e)
            
        except Exception as e:
            self.logger.error(f"Error creating fill from execution: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _on_fill_event(self, event):
        """Handle fill events from the event manager."""
        try:
            self.logger.debug(f"Received fill event: {event}")
            
            # Validate fill event
            if not isinstance(event, FillEvent) and not hasattr(event, 'order_id'):
                self.logger.warning(f"Invalid fill event format: {event}")
                return
            
            # Update order status based on fill
            if hasattr(event, 'order_id'):
                order_id = event.order_id
                if order_id in self.orders:
                    order = self.orders[order_id]
                    
                    # Update order status
                    previous_status = order.status
                    
                    # Update filled quantity
                    if hasattr(event, 'quantity'):
                        try:
                            # Initialize filled_quantity if not present
                            if not hasattr(order, 'filled_quantity') or order.filled_quantity is None:
                                order.filled_quantity = 0
                            
                            # Check for duplicate fills or overfills
                            if hasattr(event, 'fill_id'):
                                # Track fill IDs to prevent duplicates
                                if not hasattr(order, 'fill_ids'):
                                    order.fill_ids = set()
                                
                                if event.fill_id in order.fill_ids:
                                    self.logger.warning(f"Duplicate fill detected: {event.fill_id} for order {order_id}")
                                    return
                                
                                order.fill_ids.add(event.fill_id)
                            
                            # Check if this fill would exceed the order quantity
                            if order.filled_quantity + event.quantity > order.quantity * 1.001:  # Allow 0.1% tolerance
                                self.logger.warning(f"Fill would exceed order quantity: {order.filled_quantity} + {event.quantity} > {order.quantity} for order {order_id}")
                                # We'll still process it but cap at the order quantity
                                event.quantity = order.quantity - order.filled_quantity
                                if event.quantity <= 0:
                                    self.logger.warning(f"Order {order_id} already fully filled, ignoring additional fill")
                                    return
                            
                            # Update filled quantity
                            order.filled_quantity += event.quantity
                            
                            # Update order status based on fill
                            if order.filled_quantity >= order.quantity * 0.999:  # Allow 0.1% tolerance
                                order.status = OrderStatus.FILLED
                                self.stats["orders_filled"] += 1
                                self._record_order_lifecycle_event(order_id, "FILLED", f"Fill ID: {getattr(event, 'fill_id', 'UNKNOWN')}")
                            elif order.filled_quantity > 0:
                                order.status = OrderStatus.PARTIALLY_FILLED
                                self._record_order_lifecycle_event(order_id, "PARTIALLY_FILLED", 
                                                                 f"Fill ID: {getattr(event, 'fill_id', 'UNKNOWN')}, Quantity: {event.quantity}")
                                
                            # Update average fill price
                            if hasattr(event, 'price'):
                                if not hasattr(order, 'average_fill_price') or order.average_fill_price is None:
                                    order.average_fill_price = 0
                                
                                # Calculate new average price
                                total_value = ((order.filled_quantity - event.quantity) * order.average_fill_price) + (event.quantity * event.price)
                                order.average_fill_price = total_value / order.filled_quantity
                                
                            # Update commission if provided
                            if hasattr(event, 'commission') and event.commission:
                                if not hasattr(order, 'total_commission') or order.total_commission is None:
                                    order.total_commission = 0
                                order.total_commission += event.commission
                                
                        except Exception as e:
                            self.logger.error(f"Error updating order {order_id} with fill: {e}")
                            self.logger.error(traceback.format_exc())
                            self._record_error(e)
                    
                    # Move from pending to filled if fully filled
                    if order.status == OrderStatus.FILLED:
                        if order_id in self.pending_orders:
                            del self.pending_orders[order_id]
                        self.filled_orders[order_id] = order
                        
                        # Log the completion
                        self.logger.info(f"Order {order.order_id} filled completely at average price {order.average_fill_price}")
                    elif order.status == OrderStatus.PARTIALLY_FILLED:
                        self.logger.info(f"Order {order.order_id} partially filled: {order.filled_quantity}/{order.quantity} at average price {order.average_fill_price}")
                else:
                    self.logger.warning(f"Received fill for unknown order: {order_id}")
        except Exception as e:
            self.logger.error(f"Error handling fill event: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _on_risk_breach_event(self, event):
        """Handle risk breach events from the event manager."""
        try:
            self.logger.warning(f"Received risk breach event: {event}")
            
            # Handle risk breach - this might require canceling existing orders
            if hasattr(event, 'action'):
                if event.action == 'exit_position' and hasattr(event, 'symbol'):
                    # Cancel all pending orders for this symbol
                    for order_id, order in list(self.pending_orders.items()):
                        instrument_id = getattr(order, 'instrument_id', None)
                        symbol = getattr(order, 'symbol', instrument_id)
                        
                        if symbol == event.symbol:
                            try:
                                self.cancel_order(order_id)
                                self.logger.info(f"Cancelled order {order_id} due to risk breach")
                            except Exception as e:
                                self.logger.error(f"Error cancelling order {order_id}: {e}")
                                self._record_error(e)
                    
                    # If required, create a new order to close the position
                    if hasattr(event, 'position') and event.position != 0:
                        try:
                            # Create liquidation order
                            side = "SELL" if event.position > 0 else "BUY"
                            quantity = abs(event.position)
                            
                            liquidation_order = self.create_order(
                                strategy_id="RISK_MANAGER",
                                instrument_id=event.symbol,
                                quantity=quantity,
                                price=0,  # Market order
                                side=side,
                                order_type="MARKET"
                            )
                            
                            if liquidation_order:
                                self._record_order_lifecycle_event(liquidation_order.order_id, "CREATED", "Created for risk breach liquidation")
                                self.submit_order(liquidation_order)
                                self.logger.info(f"Created liquidation order {liquidation_order.order_id} due to risk breach")
                        except Exception as e:
                            self.logger.error(f"Error creating liquidation order: {e}")
                            self._record_error(e)
        except Exception as e:
            self.logger.error(f"Error handling risk breach event: {e}")
            self.logger.error(traceback.format_exc())
            self._record_error(e)

    def _create_order_from_signal(self, signal):
        """Create an order from a signal event."""
        if not hasattr(signal, 'symbol') or not hasattr(signal, 'signal_type'):
            self.logger.warning("Invalid signal format")
            raise OrderValidationError("Signal missing required fields: symbol or signal_type")
            
        # Extract order details from signal
        symbol = signal.symbol
        strategy_id = getattr(signal, 'strategy_id', 'UNKNOWN')
        signal_type = getattr(signal, 'signal_type', None)
        
        # Determine order side based on signal type and side
        side = None
        
        # First check if 'side' is explicitly provided
        if hasattr(signal, 'side') and signal.side is not None:
            side = signal.side
            # Convert Enum to string if needed
            if hasattr(side, 'value'):
                side = side.value
        
        # If side is not provided, infer from signal_type
        if not side and signal_type:
            if hasattr(signal_type, 'value'):
                signal_type_value = signal_type.value
            else:
                signal_type_value = str(signal_type)
            
            if signal_type_value in ['ENTRY', 'BUY']:
                side = 'BUY'
            elif signal_type_value in ['EXIT', 'SELL']:
                side = 'SELL'
        
        if not side:
            error_msg = f"Cannot determine order side from signal: {signal_type}"
            self.logger.warning(error_msg)
            raise OrderValidationError(error_msg)
            
        # Extract other order parameters
        quantity = getattr(signal, 'quantity', 100)  # Default quantity
        price = getattr(signal, 'price', 0)  # Default to market order
        order_type = getattr(signal, 'order_type', 'MARKET')
        
        # Convert order_type from enum if needed
        if hasattr(order_type, 'value'):
            order_type = order_type.value
        
        # Create the order
        try:
            order = self.create_order(
                strategy_id=strategy_id,
                instrument_id=symbol,
                quantity=quantity,
                price=price,
                side=side,
                order_type=order_type
            )
            
            return order
        except Exception as e:
            error_msg = f"Failed to create order from signal: {e}"
            self.logger.error(error_msg)
            raise OrderProcessingError(error_msg)

    def _publish_order_event(self, order, action=None):
        """Publish an order event to the event system."""
        if not order:
            return
            
        try:
            # Create order event
            side = order.side if hasattr(order, 'side') else None
            
            # Standardize between symbol and instrument_id
            symbol = None
            if hasattr(order, 'symbol'):
                symbol = order.symbol
            elif hasattr(order, 'instrument_id'):
                symbol = order.instrument_id
            
            event = OrderEvent(
                event_type=EventType.ORDER,
                timestamp=int(time.time() * 1000),
                order_id=order.order_id,
                symbol=symbol,
                exchange=getattr(order, 'exchange', None),
                side=side,
                quantity=order.quantity,
                price=getattr(order, 'price', None),
                status=order.status,
                strategy_id=getattr(order, 'strategy_id', None)
            )
            
            if action:
                event.action = action
                
            # Add other order attributes if available
            for attr in ['broker_order_id', 'filled_quantity', 'average_price', 'trigger_price', 'rejection_reason']:
                if hasattr(order, attr) and getattr(order, attr) is not None:
                    setattr(event, attr, getattr(order, attr))
            
            # Validate before publishing
            try:
                event.validate()
                # Publish the event
                self.event_manager.publish(event)
                self.logger.debug(f"Published order event: {order.order_id} - {action or 'UPDATE'}")
            except EventValidationError as e:
                self.logger.error(f"Order event validation failed: {e}")
                self._record_error(e)
                # Don't publish invalid events
            
        except Exception as e:
            self.logger.error(f"Error publishing order event: {e}")
            self._record_error(e)

    def _publish_execution_event(self, order):
        """Publish an execution event when an order is executed."""
        if not order:
            return
            
        try:
            # Standardize between symbol and instrument_id
            symbol = None
            if hasattr(order, 'symbol'):
                symbol = order.symbol
            elif hasattr(order, 'instrument_id'):
                symbol = order.instrument_id
            
            # Create execution event using the proper ExecutionEvent class
            event = ExecutionEvent(
                event_type=EventType.EXECUTION,
                timestamp=int(time.time() * 1000),
                order_id=order.order_id,
                symbol=symbol,
                exchange=getattr(order, 'exchange', None),
                side=order.side,
                quantity=order.quantity,
                price=getattr(order, 'price', None),
                status=order.status,
                strategy_id=getattr(order, 'strategy_id', None),
                execution_id=f"exec-{uuid.uuid4()}",
                execution_time=int(time.time() * 1000),
                execution_type="NEW"
            )
            
            # Add broker_order_id if available
            if hasattr(order, 'broker_order_id') and order.broker_order_id:
                event.broker_order_id = order.broker_order_id
            
            # Validate before publishing
            try:
                event.validate()
                # Publish the event
                self.event_manager.publish(event)
                self.logger.debug(f"Published execution event for order: {order.order_id}")
            except EventValidationError as e:
                self.logger.error(f"Execution event validation failed: {e}")
                self._record_error(e)
                # Don't publish invalid events
                
        except Exception as e:
            self.logger.error(f"Error publishing execution event: {e}")
            self._record_error(e)

    def _standardize_order_status(self, status):
        """Standardize order status to OrderStatus enum."""
        if isinstance(status, OrderStatus):
            return status
            
        if isinstance(status, str):
            try:
                return OrderStatus(status)
            except (ValueError, TypeError):
                pass
                
        # Default to UNKNOWN if we can't convert
        self.logger.warning(f"Could not convert status to OrderStatus enum: {status}")
        return OrderStatus.UNKNOWN

    def _update_order_status(self, order, new_status):
        """Update order status and tracking dictionaries."""
        if not order:
            return
            
        # Standardize status
        new_status = self._standardize_order_status(new_status)
        old_status = self._standardize_order_status(order.status) if hasattr(order, 'status') else OrderStatus.UNKNOWN
        
        # Skip if no change
        if new_status == old_status:
            return
            
        # Update order status
        order.status = new_status
        order.last_updated = datetime.now()
        
        # Record in lifecycle
        self._record_order_lifecycle_event(order.order_id, new_status.name, f"Changed from {old_status.name}")
        
        # Update tracking dictionaries
        if new_status == OrderStatus.FILLED:
            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]
            self.filled_orders[order.order_id] = order
            self.stats["orders_filled"] += 1
        elif new_status == OrderStatus.CANCELLED:
            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]
            self.cancelled_orders[order.order_id] = order
            self.stats["orders_cancelled"] += 1
        elif new_status == OrderStatus.REJECTED:
            if order.order_id in self.pending_orders:
                del self.pending_orders[order.order_id]
            self.rejected_orders[order.order_id] = order
            self.stats["orders_rejected"] += 1
        elif new_status == OrderStatus.PENDING or new_status == OrderStatus.SUBMITTED:
            self.pending_orders[order.order_id] = order
        
        # Publish order update event
        self._publish_order_event(order, 'STATUS_UPDATE')

    def create_order(self, strategy_id: str, instrument_id: str, quantity: float, price: float, 
                    side: str, order_type: Any) -> Order:
        """
        Create a new order.
        
        Args:
            strategy_id: ID of the strategy creating the order
            instrument_id: ID or symbol of the instrument
            quantity: Quantity to trade
            price: Price for limit/stop orders
            side: BUY or SELL
            order_type: Type of order (MARKET, LIMIT, etc.)
            
        Returns:
            Order: The created order object
        
        Raises:
            OrderValidationError: If order parameters are invalid
        """
        try:
            # Convert side/action if needed
            if hasattr(side, 'value'):
                side = side.value
                
            # Convert order_type if needed
            if hasattr(order_type, 'value'):
                order_type = order_type.value
            
            # Create the order
            order = Order(
                instrument_id=instrument_id,
                quantity=quantity,
                side=side,  # Use standardized 'side'
                price=price,
                order_type=order_type,
                strategy_id=strategy_id
            )
            
            # Validate the order
            order.validate()
            
            # Store in tracking dictionaries
            self.orders[order.order_id] = order
            
            # Record in lifecycle
            self._record_order_lifecycle_event(order.order_id, "CREATED", f"Strategy: {strategy_id}, Type: {order_type}")
            
            self.logger.info(f"Created order: {order}")
            return order
        except OrderValidationError as e:
            self.logger.error(f"Order validation failed: {e}")
            self._record_error(e)
            raise
        except Exception as e:
            error_msg = f"Unexpected error creating order: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self._record_error(e)
            raise OrderProcessingError(error_msg)

    def submit_order(self, order: Order) -> bool:
        """
        Submit an order to the broker.
        
        Args:
            order: The order to submit
            
        Returns:
            bool: True if the order was submitted successfully
            
        Raises:
            OrderProcessingError: If order submission fails
        """
        try:
            # Check order status
            current_status = self._standardize_order_status(getattr(order, 'status', None))
                    
            if current_status != OrderStatus.CREATED:
                error_msg = f"Cannot submit order with status {getattr(order, 'status', 'UNKNOWN')}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)

            # Update order status to PENDING
            order.status = OrderStatus.PENDING
            order.last_updated = datetime.now()
            
            # Store in pending orders
            self.pending_orders[order.order_id] = order
            
            # Record in lifecycle
            self._record_order_lifecycle_event(order.order_id, "PENDING", "Submitting to broker")
            
            # Publish order update event
            self._publish_order_event(order, 'PENDING')

            # Submit to broker
            try:
                if self.broker_interface and hasattr(self.broker_interface, 'submit_order'):
                    # Use actual broker interface if available
                    result = self.broker_interface.submit_order(order)
                else:
                    # Simulated broker response
                    result = {'success': True, 'broker_order_id': 'simulated-' + order.order_id}
                
                if not result.get('success', False):
                    raise BrokerError(result.get('error', 'Unknown broker error'))
                
            except BrokerError as e:
                self.logger.error(f"Broker error: {e}")
                order.status = OrderStatus.REJECTED
                order.rejection_reason = str(e)
                order.last_updated = datetime.now()
                
                # Record in lifecycle
                self._record_order_lifecycle_event(order.order_id, "REJECTED", f"Broker error: {e}")
                
                # Publish order update event
                self._publish_order_event(order, 'REJECTED')
                
                error_msg = f"Order rejected by broker: {order.order_id} - {e}"
                self._record_error(error_msg)
                raise OrderProcessingError(error_msg)
            
            # Update order after successful submission
            order.status = OrderStatus.SUBMITTED
            order.broker_order_id = result.get('broker_order_id')
            order.last_updated = datetime.now()
            
            # Record in lifecycle
            self._record_order_lifecycle_event(order.order_id, "SUBMITTED", f"Broker order ID: {order.broker_order_id}")
            
            # Update order in tracking dictionaries
            self.orders[order.order_id] = order
            
            # Publish order update event
            self._publish_order_event(order, 'SUBMITTED')
            
            # Publish execution event
            self._publish_execution_event(order)
            
            self.stats["orders_submitted"] += 1
            self.logger.info(f"Order submitted: {order.order_id}, broker ID: {order.broker_order_id}")
            return True

        except OrderProcessingError:
            # Re-raise OrderProcessingError without wrapping
            raise
        except BrokerError as e:
            # Handle broker errors
            if hasattr(order, 'status'):
                order.status = OrderStatus.REJECTED
                order.rejection_reason = str(e)
                order.last_updated = datetime.now()
                
                # Record in lifecycle
                self._record_order_lifecycle_event(order.order_id, "REJECTED", f"Broker error: {e}")
                
                # Publish order update event
                self._publish_order_event(order, 'REJECTED')
            
            error_msg = f"Broker error submitting order: {e}"
            self.logger.error(error_msg)
            self._record_error(e)
            raise OrderProcessingError(error_msg)
        except Exception as e:
            # Handle unexpected errors
            if hasattr(order, 'status'):
                order.status = OrderStatus.REJECTED
                order.rejection_reason = f"System error: {str(e)}"
                order.last_updated = datetime.now()
                
                # Record in lifecycle
                self._record_order_lifecycle_event(order.order_id, "REJECTED", f"System error: {e}")
                
                # Publish order update event
                self._publish_order_event(order, 'REJECTED')
            
            error_msg = f"Error submitting order: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self._record_error(e)
            raise OrderProcessingError(error_msg)

    def modify_order(self, order_id: str, **kwargs) -> bool:
        """
        Modify an existing order.
        
        Args:
            order_id: The ID of the order to modify
            **kwargs: Parameters to modify (price, quantity, order_type, trigger_price)
            
        Returns:
            bool: True if the order was modified successfully
            
        Raises:
            OrderProcessingError: If modification fails
        """
        try:
            if order_id not in self.orders:
                error_msg = f"Order not found: {order_id}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
        
            order = self.orders[order_id]
        
            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PENDING]:
                error_msg = f"Cannot modify order with status {order.status}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
            
            # Record original values for logging
            original_values = {}
            for param in kwargs:
                if hasattr(order, param):
                    original_values[param] = getattr(order, param)
            
            # Apply modifications
            for param, value in kwargs.items():
                if hasattr(order, param):
                    setattr(order, param, value)
            
            # Update last_updated timestamp
            order.last_updated = datetime.now()
            
            # Submit modification to broker
            try:
                if self.broker_interface and hasattr(self.broker_interface, 'modify_order'):
                    # Use actual broker interface if available
                    result = self.broker_interface.modify_order(order, **kwargs)
                else:
                    # Simulated broker response
                    result = {'success': True}
                
                if not result.get('success', False):
                    # Revert changes
                    for param, value in original_values.items():
                        setattr(order, param, value)
                    raise BrokerError(result.get('error', 'Unknown broker error'))
                
            except BrokerError as e:
                self.logger.error(f"Broker error modifying order: {e}")
                # Revert changes
                for param, value in original_values.items():
                    setattr(order, param, value)
                error_msg = f"Order modification rejected by broker: {order_id} - {e}"
                self._record_error(error_msg)
                raise OrderProcessingError(error_msg)
            
            # Record in lifecycle
            changes = ", ".join([f"{k}: {original_values[k]} -> {kwargs[k]}" for k in kwargs if k in original_values])
            self._record_order_lifecycle_event(order_id, "MODIFIED", f"Changes: {changes}")
            
            # Publish order update event
            self._publish_order_event(order, 'MODIFIED')
            
            self.stats["orders_modified"] += 1
            self.logger.info(f"Order modified: {order_id}, changes: {changes}")
            return True
                
        except OrderProcessingError:
            # Re-raise OrderProcessingError without wrapping
            raise
        except Exception as e:
            error_msg = f"Error modifying order {order_id}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self._record_error(e)
            raise OrderProcessingError(error_msg)

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.
        
        Args:
            order_id: The ID of the order to cancel
            
        Returns:
            bool: True if the order was cancelled successfully
            
        Raises:
            OrderProcessingError: If cancellation fails
        """
        try:
            if order_id not in self.orders:
                error_msg = f"Order not found: {order_id}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
    
            order = self.orders[order_id]
    
            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING]:
                error_msg = f"Cannot cancel order with status {order.status}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
    
            # Submit cancellation to broker
            try:
                if self.broker_interface and hasattr(self.broker_interface, 'cancel_order'):
                    # Use actual broker interface if available
                    result = self.broker_interface.cancel_order(order)
                else:
                    # Simulated cancellation
                    result = True
    
                if not result:
                    raise BrokerError("Broker rejected cancellation request")
                
            except BrokerError as e:
                self.logger.error(f"Broker error cancelling order: {e}")
                error_msg = f"Order cancellation rejected by broker: {order_id} - {e}"
                self._record_error(error_msg)
                raise OrderProcessingError(error_msg)
    
            # Update order status
            order.status = OrderStatus.CANCELLED
            order.last_updated = datetime.now()
            
            # Record in lifecycle
            self._record_order_lifecycle_event(order_id, "CANCELLED", f"Filled quantity: {getattr(order, 'filled_quantity', 0)}")
            
            # Move to cancelled orders
            self.cancelled_orders[order_id] = order
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            
            # Publish order update event
            self._publish_order_event(order, 'CANCELLED')
            
            self.stats["orders_cancelled"] += 1
            self.logger.info(f"Order cancelled: {order_id}")
            return True
                
        except OrderProcessingError:
            # Re-raise OrderProcessingError without wrapping
            raise
        except Exception as e:
            error_msg = f"Error cancelling order {order_id}: {e}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self._record_error(e)
            raise OrderProcessingError(error_msg)
            
    def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """
        Get the current status of an order.
        
        Args:
            order_id: The ID of the order
            
        Returns:
            Optional[OrderStatus]: The order status if found, None otherwise
        """
        if order_id in self.orders:
            return self.orders[order_id].status
        return None
    
    def get_order_lifecycle(self, order_id: str) -> List[Dict[str, Any]]:
        """
        Get the lifecycle events for an order.
        
        Args:
            order_id: The ID of the order
            
        Returns:
            List[Dict[str, Any]]: List of lifecycle events for the order
        """
        if order_id in self.order_lifecycle:
            return self.order_lifecycle[order_id]
        return []
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the OrderManager.
        
        Returns:
            Dict[str, Any]: Dictionary of statistics
        """
        # Add current counts
        self.stats["current_orders"] = len(self.orders)
        self.stats["pending_orders"] = len(self.pending_orders)
        self.stats["filled_orders"] = len(self.filled_orders)
        self.stats["cancelled_orders"] = len(self.cancelled_orders)
        self.stats["rejected_orders"] = len(self.rejected_orders)
        
        return self.stats
    
    def get_order_batch(self, status=None, strategy_id=None, symbol=None) -> List[Order]:
        """
        Get a batch of orders filtered by criteria.
        
        Args:
            status: Optional status to filter by
            strategy_id: Optional strategy ID to filter by
            symbol: Optional symbol to filter by
            
        Returns:
            List[Order]: List of matching orders
        """
        result = []
        
        for order_id, order in self.orders.items():
            # Apply status filter if provided
            if status and getattr(order, 'status', None) != status:
                continue
                
            # Apply strategy filter if provided
            if strategy_id and getattr(order, 'strategy_id', None) != strategy_id:
                continue
                
            # Apply symbol filter if provided
            order_symbol = getattr(order, 'symbol', getattr(order, 'instrument_id', None))
            if symbol and order_symbol != symbol:
                continue
                
            result.append(order)
            
        return result
