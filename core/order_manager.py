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
from models.events import EventType, OrderEvent, Event, EventValidationError, SignalEvent, FillEvent

class OrderProcessingError(Exception):
    """Exception raised when order processing fails"""
    pass

class OrderManager:
    """Manages order lifecycle, from creation to execution and tracking"""

    def __init__(self, event_manager, risk_manager=None):
        """
        Initialize the order manager.
        
        Args:
            event_manager: Event manager for publishing/subscribing to events
            risk_manager: Optional risk manager for checking orders
        """
        self.logger = logging.getLogger("core.order_manager")
        self.event_manager = event_manager
        self.risk_manager = risk_manager
        
        # Tracking order state
        self.orders = {}  # Order ID -> Order
        self.pending_orders = {}  # Order ID -> Order (only pending ones)
        self.filled_orders = {}  # Order ID -> Order (only filled ones)
        self.cancelled_orders = {}  # Order ID -> Order (only cancelled ones)
        
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
            "errors": 0,
            "last_error": None,
            "last_error_time": None
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
            if hasattr(event, 'order'):
                order = event.order
            elif hasattr(event, 'data') and isinstance(event.data, dict) and 'order' in event.data:
                order = event.data['order']
            elif hasattr(event, 'order_id') and event.order_id in self.orders:
                order = self.orders[event.order_id]
            
            if not order:
                self.logger.warning("Received order event without order data")
                return
                
            # Update order tracking
            if hasattr(order, 'order_id'):
                self.orders[order.order_id] = order
                
                # Update specific tracking dictionaries based on status
                if hasattr(order, 'status'):
                    if order.status == OrderStatus.PENDING:
                        self.pending_orders[order.order_id] = order
                    elif order.status == OrderStatus.FILLED:
                        self.filled_orders[order.order_id] = order
                        if order.order_id in self.pending_orders:
                            del self.pending_orders[order.order_id]
                    elif order.status == OrderStatus.CANCELLED:
                        self.cancelled_orders[order.order_id] = order
                        if order.order_id in self.pending_orders:
                            del self.pending_orders[order.order_id]
                
                # If this is a new order that needs to be executed, submit it
                action = None
                if hasattr(event, 'action'):
                    action = event.action
                
                if action == 'SUBMIT' or action == 'CREATED':
                    try:
                        self.submit_order(order)
                    except Exception as e:
                        self.logger.error(f"Error submitting order: {e}")
                        self._record_error(e)
        except Exception as e:
            self.logger.error(f"Error handling order event: {e}")
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
                            if not hasattr(order, 'filled_quantity') or order.filled_quantity is None:
                                order.filled_quantity = 0
                            order.filled_quantity += event.quantity
                            
                            # Update order status based on fill
                            if order.filled_quantity >= order.quantity:
                                order.status = OrderStatus.FILLED
                                self.stats["orders_filled"] += 1
                            elif order.filled_quantity > 0:
                                order.status = OrderStatus.PARTIALLY_FILLED
                                
                            # Update average fill price
                            if hasattr(event, 'price'):
                                if not hasattr(order, 'average_fill_price') or order.average_fill_price is None:
                                    order.average_fill_price = 0
                                
                                # Calculate new average price
                                total_value = ((order.filled_quantity - event.quantity) * order.average_fill_price) + (event.quantity * event.price)
                                order.average_fill_price = total_value / order.filled_quantity
                        except Exception as e:
                            self.logger.error(f"Error updating order {order_id} with fill: {e}")
                            self._record_error(e)
                    
                    # Move from pending to filled if fully filled
                    if order.status == OrderStatus.FILLED:
                        if order.order_id in self.pending_orders:
                            del self.pending_orders[order.order_id]
                        self.filled_orders[order.order_id] = order
                        
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
            
            event = OrderEvent(
                event_type=EventType.ORDER,
                timestamp=int(time.time() * 1000),
                order_id=order.order_id,
                symbol=getattr(order, 'instrument_id', None),
                side=side,
                quantity=order.quantity,
                price=getattr(order, 'price', None),
                status=order.status,
                strategy_id=getattr(order, 'strategy_id', None)
            )
            
            if action:
                event.action = action
                
            # Add other order attributes if available
            for attr in ['broker_order_id', 'filled_quantity', 'average_price']:
                if hasattr(order, attr):
                    setattr(event, attr, getattr(order, attr))
            
            # Validate before publishing
            try:
                event.validate()
            except EventValidationError as e:
                self.logger.warning(f"Order event validation warning: {e}")
                # Continue anyway as this is not fatal
            
            # Publish the event
            self.event_manager.publish(event)
            self.logger.debug(f"Published order event: {order.order_id} - {action or 'UPDATE'}")
        except Exception as e:
            self.logger.error(f"Error publishing order event: {e}")
            self._record_error(e)

    def _publish_execution_event(self, order):
        """Publish an execution event when an order is executed."""
        if not order:
            return
            
        try:
            # Create execution event
            event = Event(
                event_type=EventType.EXECUTION,
                timestamp=int(time.time() * 1000)
            )
            
            # Add order details to the event
            event.order_id = order.order_id
            event.symbol = getattr(order, 'instrument_id', None)
            event.side = order.side
            event.quantity = order.quantity
            event.price = getattr(order, 'price', None)
            event.strategy_id = getattr(order, 'strategy_id', None)
            
            # Publish the event
            self.event_manager.publish(event)
            self.logger.debug(f"Published execution event for order: {order.order_id}")
        except Exception as e:
            self.logger.error(f"Error publishing execution event: {e}")
            self._record_error(e)

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
            current_status = None
            if hasattr(order, 'status'):
                current_status = order.status
                # Convert from string to enum if needed
                if isinstance(current_status, str):
                    try:
                        current_status = OrderStatus(current_status)
                    except (ValueError, TypeError):
                        pass
                    
            if current_status != OrderStatus.CREATED:
                error_msg = f"Cannot submit order with status {getattr(order, 'status', 'UNKNOWN')}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)

            # Update order status to PENDING
            order.status = OrderStatus.PENDING
            order.last_updated = datetime.now()
            
            # Store in pending orders
            self.pending_orders[order.order_id] = order
            
            # Publish order update event
            self._publish_order_event(order, 'PENDING')

            # Publish execution event
            self._publish_execution_event(order)

            # Submit to broker
            # This part depends on your broker implementation
            try:
                # Simulated broker response
                result = {'success': True, 'broker_order_id': 'simulated-' + order.order_id}
                
                if not result.get('success', False):
                    raise BrokerError(result.get('error', 'Unknown broker error'))
                
            except BrokerError as e:
                self.logger.error(f"Broker error: {e}")
                order.status = OrderStatus.REJECTED
                order.rejection_reason = str(e)
                order.last_updated = datetime.now()
                
                # Publish order update event
                self._publish_order_event(order, 'REJECTED')
                
                error_msg = f"Order rejected by broker: {order.order_id} - {e}"
                self._record_error(error_msg)
                raise OrderProcessingError(error_msg)
            
            # Update order after successful submission
            order.status = OrderStatus.SUBMITTED
            order.broker_order_id = result.get('broker_order_id')
            order.last_updated = datetime.now()
            
            # Update order in tracking dictionaries
            self.orders[order.order_id] = order
            
            # Publish order update event
            self._publish_order_event(order, 'SUBMITTED')
            
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
                
                # Publish order update event
                self._publish_order_event(order, 'REJECTED')
            
            error_msg = f"Error submitting order: {e}"
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
    
            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]:
                error_msg = f"Cannot cancel order with status {order.status}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
    
            # For simulation, we'll just update the status
            # In a real system, you would call the broker API
            result = True
    
            if result:
                order.status = OrderStatus.CANCELLED
                order.last_updated = datetime.now()
                
                # Move to cancelled orders
                self.cancelled_orders[order_id] = order
                if order_id in self.pending_orders:
                    del self.pending_orders[order_id]
                
                # Publish order update event
                self._publish_order_event(order, 'CANCELLED')
                
                self.stats["orders_cancelled"] += 1
                self.logger.info(f"Order cancelled: {order_id}")
                return True
            else:
                error_msg = f"Failed to cancel order: {order_id}"
                self.logger.error(error_msg)
                raise OrderProcessingError(error_msg)
                
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
        
        return self.stats
