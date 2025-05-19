"""
Execution handlers that process orders and submit them to appropriate venues.
"""
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from enum import Enum

from core.event_manager import EventManager
from models.events import Event, EventType, OrderEvent
from models.order import Order, OrderStatus, OrderType
from utils.constants import OrderSide
from brokers.broker_interface import BrokerInterface
from core.logging_manager import get_logger


class ExecutionHandler:
    """
    Handles the actual execution of orders by interacting with the broker interface.
    Listens for OrderEvents and translates them into broker actions.
    """
    def __init__(self, event_manager: EventManager, broker_interface: BrokerInterface):
        """
        Initialize the ExecutionHandler.

        Args:
            event_manager: The system's EventManager.
            broker_interface: The interface to the broker.
        """
        self.logger = get_logger(__name__)
        self.event_manager = event_manager
        self.broker_interface = broker_interface
        self._register_event_handlers()
        self.logger.info("Execution Handler initialized")

    def _register_event_handlers(self):
        """Register to receive Order events."""
        self.event_manager.subscribe(
            EventType.ORDER,
            self._on_order_event,
            component_name="ExecutionHandler"
        )
        self.logger.info("ExecutionHandler subscribed to ORDER events")

    def _on_order_event(self, event: OrderEvent):
        """
        Handle incoming OrderEvents from the OrderManager.

        Args:
            event: The OrderEvent.
        """
        self.logger.debug(f"ExecutionHandler received order event: {event.order_id}, Status: {event.status}")

        # Extract necessary information (create an Order object from the event)
        # Note: OrderManager might publish OrderEvents that don't map perfectly to the Order model.
        # We need to ensure compatibility or construct the Order object here.
        order = Order(
            instrument_id=event.symbol,  # Assuming event.symbol holds instrument_id
            quantity=event.quantity,
            side=event.side.value if isinstance(event.side, Enum) else event.side,  # Use string value
            order_type=event.order_type.value if isinstance(event.order_type, Enum) else event.order_type,
            price=event.price,
            strategy_id=event.strategy_id
        )

        # Determine action based on event status or an explicit action field if added later
        # Simple logic: if status is PENDING/CREATED, submit. If PENDING_CANCEL, cancel etc.
        if event.status == OrderStatus.PENDING or event.status == OrderStatus.CREATED: # Assuming OrderManager sets status to PENDING/CREATED initially
            self.logger.info(f"Submitting order {order.order_id} to broker.")
            try:
                # Ensure broker is connected
                if not self.broker_interface.is_connected():
                    self.logger.warning("Broker not connected. Attempting to connect before submitting order.")
                    if not self.broker_interface.connect():
                         self.logger.error(f"Failed to connect to broker. Cannot submit order {order.order_id}.")
                         # Optionally publish a REJECTED order event here
                         return

                broker_order_id = self.broker_interface.place_order(order)

                if broker_order_id:
                    self.logger.info(f"Order {order.order_id} submitted successfully. Broker ID: {broker_order_id}")
                    # Optionally publish an updated OrderEvent with SUBMITTED status and broker ID
                else:
                    self.logger.error(f"Broker failed to place order {order.order_id}. No broker ID received.")
                    # Optionally publish a REJECTED order event here

            except Exception as e:
                self.logger.error(f"Error submitting order {order.order_id} via broker: {e}", exc_info=True)
                # Optionally publish a REJECTED order event here

        elif event.status == OrderStatus.PENDING_CANCEL:
            self.logger.info(f"Cancelling order {order.order_id} with broker.")
            try:
                success = self.broker_interface.cancel_order(order.order_id)
                if success:
                    self.logger.info(f"Order {order.order_id} cancellation request submitted successfully.")
                    # Broker might send an update event, or we publish a CANCELED status event
                else:
                    self.logger.error(f"Broker failed to cancel order {order.order_id}.")
            except Exception as e:
                self.logger.error(f"Error cancelling order {order.order_id} via broker: {e}", exc_info=True)

        elif event.status == OrderStatus.PENDING_REPLACE: # Assuming a status for modification requests
            self.logger.info(f"Modifying order {order.order_id} with broker.")
            try:
                # Need to pass the modified parameters to modify_order
                # The OrderEvent needs to carry the modifications (new price/qty)
                success = self.broker_interface.modify_order(order.order_id, order) # Pass the full order for now
                if success:
                    self.logger.info(f"Order {order.order_id} modification request submitted successfully.")
                else:
                    self.logger.error(f"Broker failed to modify order {order.order_id}.")
            except Exception as e:
                self.logger.error(f"Error modifying order {order.order_id} via broker: {e}", exc_info=True)
        else:
            # Handle other statuses if needed (e.g., logging final states like FILLED/CANCELLED received from broker updates)
            self.logger.debug(f"Ignoring order event {order.order_id} with status {event.status} in ExecutionHandler.")

    def start(self):
        # Placeholder if any background tasks are needed
        self.logger.info("Execution Handler started (no background tasks)")

    def stop(self):
        # Placeholder for cleanup
        self.logger.info("Execution Handler stopped")

class SimulatedExecutionHandler(ExecutionHandler):
    """
    Simulated execution handler for backtesting and demo.
    Processes orders against market data and generates fills.
    """
    
    def __init__(self, market_data_feed, event_manager: EventManager, slippage: float = 0.0):
        """Initialize the simulated execution handler.
        
        Args:
            market_data_feed: The market data feed to get prices from
            event_manager: The event manager to dispatch events
            slippage: The slippage factor to apply to executions (e.g., 0.001 for 0.1%)
        """
        super().__init__(event_manager)
        self.market_data_feed = market_data_feed
        self.slippage = slippage
        self.filled_orders: Dict[str, Order] = {}
        self.cancelled_orders: Dict[str, Order] = {}
        
        # Register for market data events to process orders against
        self.event_manager.subscribe(EventType.MARKET_DATA, self.on_market_data, component_name="SimulatedExecutionHandler")
    
    def submit_order(self, order: Order) -> bool:
        """Submit an order to the simulated execution system.
        
        Args:
            order: The order to submit
            
        Returns:
            bool: True if the order was accepted
        """
        try:
            # Generate a broker order ID (in real systems this would come from the broker)
            order.broker_order_id = f"sim-{uuid.uuid4()}"
            
            # Update order status
            order.status = OrderStatus.PENDING
            
            # Store the order
            self.orders[order.order_id] = order
            
            # Publish the order event
            self._publish_order_event(order, "SUBMITTED")
            
            # For market orders, try to execute immediately
            if order.order_type == OrderType.MARKET:
                self._try_execute_market_order(order)
            
            self.logger.info(f"Order submitted: {order.order_id} - {order.instrument.symbol} {order.direction} {order.quantity}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error submitting order: {str(e)}")
            return False
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order.
        
        Args:
            order_id: The ID of the order to cancel
            
        Returns:
            bool: True if the order was cancelled successfully
        """
        if order_id not in self.orders:
            self.logger.warning(f"Cannot cancel order {order_id} - not found")
            return False
            
        order = self.orders[order_id]
        
        # Only cancel if the order is still pending
        if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED]:
            order.status = OrderStatus.CANCELLED
            
            # Remove from active orders
            self.cancelled_orders[order_id] = order
            del self.orders[order_id]
            
            # Publish cancel event
            self._publish_order_event(order, "CANCELLED")
            
            self.logger.info(f"Order cancelled: {order_id}")
            return True
        else:
            self.logger.warning(f"Cannot cancel order {order_id} - status is {order.status}")
            return False
    
    def on_market_data(self, event: Event) -> None:
        """
        Process market data events to potentially execute pending orders.
        
        Args:
            event: Market data event
        """
        try:
            # Check each pending order against the current market data
            for order_id, order in list(self.orders.items()):
                if order.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED]:
                    # For limit and stop orders, check if conditions are met
                    if order.order_type != OrderType.MARKET:
                        self._try_execute_limit_stop_order(order, event)
            
        except Exception as e:
            self.logger.error(f"Error processing market data for order execution: {str(e)}")
    
    def _try_execute_market_order(self, order: Order) -> None:
        """
        Try to execute a market order immediately using current market data.
        
        Args:
            order: The order to execute
        """
        # Get current market data for the instrument
        symbol = order.instrument.symbol
        if symbol not in self.market_data_feed.prices:
            self.logger.warning(f"No market data available for {symbol}, cannot execute market order")
            return
            
        price_data = self.market_data_feed.prices[symbol]
        
        # Use bid for SELL orders and ask for BUY orders
        if order.direction == "SELL":
            execution_price = price_data.get('bid', price_data.get('current'))
        else:  # BUY
            execution_price = price_data.get('ask', price_data.get('current'))
        
        # Apply slippage
        slippage_factor = 1.0 + (self.slippage if order.direction == "BUY" else -self.slippage)
        execution_price = execution_price * slippage_factor
        
        # Execute the order
        self._execute_order(order, execution_price)
    
    def _try_execute_limit_stop_order(self, order: Order, event: Event) -> None:
        """
        Try to execute a limit or stop order based on current market data.
        
        Args:
            order: The order to potentially execute
            event: Market data event
        """
        if hasattr(event, 'symbol') and event.symbol != order.instrument.symbol:
            return  # Not relevant for this order
            
        current_price = None
        
        # Get current price from the event
        if hasattr(event, 'data'):
            price_data = event.data
            # Use close price or current price as available
            current_price = price_data.get('close', price_data.get('current'))
        
        if current_price is None:
            return  # No price available
            
        should_execute = False
        
        # Check if conditions are met for this order type
        if order.order_type == OrderType.LIMIT:
            # For BUY limit orders, execute when price <= limit price
            if order.direction == "BUY" and current_price <= order.price:
                should_execute = True
            # For SELL limit orders, execute when price >= limit price
            elif order.direction == "SELL" and current_price >= order.price:
                should_execute = True
                
        elif order.order_type == OrderType.STOP:
            # For BUY stop orders, execute when price >= stop price
            if order.direction == "BUY" and current_price >= order.price:
                should_execute = True
            # For SELL stop orders, execute when price <= stop price
            elif order.direction == "SELL" and current_price <= order.price:
                should_execute = True
                
        # Execute if conditions are met
        if should_execute:
            # For stop orders, we execute at market price (with slippage)
            if order.order_type == OrderType.STOP:
                # Apply slippage
                slippage_factor = 1.0 + (self.slippage if order.direction == "BUY" else -self.slippage)
                execution_price = current_price * slippage_factor
            else:
                # For limit orders, we execute at the limit price
                execution_price = order.price
                
            self._execute_order(order, execution_price)
    
    def _execute_order(self, order: Order, price: float) -> None:
        """
        Execute an order at the specified price.
        
        Args:
            order: The order to execute
            price: The execution price
        """
        # Update order
        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_quantity = order.quantity
        order.filled_time = datetime.now()
        
        # Move to filled orders
        self.filled_orders[order.order_id] = order
        if order.order_id in self.orders:
            del self.orders[order.order_id]
        
        # Publish fill event
        self._publish_order_event(order, "FILLED")
        
        self.logger.info(
            f"Order filled: {order.order_id} - {order.instrument.symbol} "
            f"{order.direction} {order.quantity} @ {price:.4f}"
        ) 