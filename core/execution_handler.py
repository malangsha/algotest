import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from enum import Enum
import time

from utils.constants import OrderSide, OrderStatus, OrderType as FrameworkOrderType, Exchange
from utils.constants import EventSource, OrderType as ModelOrderType # Renamed to avoid conflict
from core.event_manager import EventManager
from core.logging_manager import get_logger

from models.events import Event, EventType, OrderEvent, ExecutionEvent
from models.order import Order as ModelOrder
from models.instrument import Instrument
from brokers.broker_interface import BrokerInterface

class ExecutionHandler:
    """
    Handles the actual execution of orders by interacting with the broker interface.
    Listens for OrderEvents and translates them into broker actions.
    """
    def __init__(self, event_manager: EventManager, broker_interface: BrokerInterface):
        self.logger = get_logger(__name__)
        self.event_manager = event_manager
        self.broker_interface = broker_interface        
        self.currently_submitting: Set[str] = set()
        self._register_event_handlers()
        self.logger.info("Execution Handler initialized")

    def _register_event_handlers(self):
        self.event_manager.subscribe(
            EventType.ORDER,
            self._on_order_event,
            component_name="ExecutionHandler"
        )
        self.logger.info("ExecutionHandler subscribed to ORDER events")

    def _on_order_event(self, event: OrderEvent):
        # This event.order_id is the internal system-wide order ID.
        self.logger.debug(f"ExecutionHandler received order event: ID={event.order_id}, Symbol={event.symbol}, Exchange={event.exchange}, Status={event.status}, Side={event.side}, Type={event.order_type}, Action={event.action}")

        try:
            event.validate()
        except Exception as e:
            self.logger.error(f"Invalid OrderEvent received by ExecutionHandler: {e}. Order ID: {event.order_id}. Event: {event}")
            return

        # Standardize enums from event
        try:
            order_side_enum = event.side if isinstance(event.side, OrderSide) else OrderSide(str(event.side).upper())
            broker_side_str = order_side_enum.value

            # Event carries FrameworkOrderType, broker might expect ModelOrderType
            if isinstance(event.order_type, ModelOrderType):
                broker_order_type_enum = event.order_type
            elif isinstance(event.order_type, FrameworkOrderType):
                broker_order_type_enum = ModelOrderType(event.order_type.value.upper())
            elif isinstance(event.order_type, str):
                 broker_order_type_enum = ModelOrderType(event.order_type.upper())
            else:
                raise ValueError(f"Cannot interpret event.order_type: {event.order_type}")

            order_exchange_enum = event.exchange if isinstance(event.exchange, Exchange) else Exchange(str(event.exchange).upper())
            broker_exchange_str = order_exchange_enum.value

            event_status_enum = event.status if isinstance(event.status, OrderStatus) else OrderStatus(str(event.status).upper())
            event_action_str = str(event.action).upper() if event.action else None

        except ValueError as e:
            self.logger.error(f"Invalid enum value in OrderEvent for order {event.order_id}: {e}. Event: {event}")
            # Publish a REJECTED ExecutionEvent if critical info is missing/invalid
            self._publish_execution_event(
                order_id=event.order_id, symbol=event.symbol, exchange_str=str(event.exchange),
                side_enum=None, quantity=event.quantity, order_type_enum=None,
                status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                rejection_reason=f"Invalid event data: {e}", exec_type="REJECTED_EVENT_VALIDATION"
            )
            return

        # --- Order Submission Logic (PENDING status from OrderManager) ---
        if event_status_enum == OrderStatus.PENDING and (event_action_str == "PENDING" or event_action_str == "SUBMIT"): # Explicit PENDING action from OM
            if event.order_id in self.currently_submitting:
                self.logger.warning(f"Order {event.order_id} is already being processed for submission by this ExecutionHandler instance. Ignoring duplicate PENDING event.")
                return
            try:
                self.currently_submitting.add(event.order_id)
                self.logger.info(f"ExecutionHandler: Attempting to submit order {event.order_id} to broker. Details: {event}")

                if not self.broker_interface.is_connected():
                    self.logger.warning("Broker not connected. Attempting to connect before submitting order.")
                    if not self.broker_interface.connect(): # Assuming connect() is synchronous
                        self.logger.error(f"Failed to connect to broker. Cannot submit order {event.order_id}.")
                        self._publish_execution_event(
                            order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                            side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                            status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                            rejection_reason="Broker connection failed", exec_type="REJECTED_SUBMISSION"
                        )
                        return

                # Prepare Instrument object for broker
                # Assuming event.symbol is the full instrument ID like "NSE:SBIN" or just "SBIN" if exchange is separate
                symbol_parts = event.symbol.split(':', 1)
                trading_symbol_for_broker = symbol_parts[-1] # Takes "SBIN" from "NSE:SBIN" or uses "SBIN" if no ":"

                instrument_obj_for_broker = Instrument(
                    instrument_id=event.symbol, # Full ID
                    symbol=trading_symbol_for_broker, # Trading symbol part
                    exchange=broker_exchange_str,
                    # Potentially add other instrument details if needed by broker_interface
                )

                # Prepare broker-specific keyword arguments
                broker_specific_kwargs = {}
                if event.metadata:
                    # BFO Product Type Correction: Use 'C' (NRML) instead of 'I' (MIS) for BFO options
                    product_type_from_meta = event.metadata.get('product_type')
                    if broker_exchange_str == 'BFO' and product_type_from_meta == 'I':
                        self.logger.info(f"Correcting product type from 'I' (Intraday/MIS) to 'C' (NRML/CarryForward) for BFO exchange order {event.order_id}")
                        broker_specific_kwargs['product_type'] = 'C'
                    elif product_type_from_meta:
                        broker_specific_kwargs['product_type'] = product_type_from_meta

                    if 'disclose_quantity' in event.metadata:
                        broker_specific_kwargs['discloseqty'] = event.metadata['disclose_quantity'] # Example for Finvasia

                    tif_from_meta = event.metadata.get('time_in_force')
                    if tif_from_meta:
                        tif_upper = str(tif_from_meta).upper()
                        if tif_upper == 'DAY': broker_specific_kwargs['retention'] = 'DAY' # Example mapping
                        elif tif_upper == 'IOC': broker_specific_kwargs['retention'] = 'IOC'
                        else: self.logger.warning(f"Unsupported time_in_force '{tif_from_meta}' for order {event.order_id}")
                
                # Add client_order_id to broker call if available and broker supports it
                if event.client_order_id:
                    broker_specific_kwargs['client_order_id'] = event.client_order_id

                if event.remarks:
                    broker_specific_kwargs['remarks'] = event.remarks    

                broker_response = self.broker_interface.place_order(
                    instrument=instrument_obj_for_broker,
                    order_type=broker_order_type_enum, # This is models.order.OrderType
                    quantity=int(event.quantity),
                    side=broker_side_str, # "BUY" or "SELL"
                    price=event.price, # Can be None for MARKET orders
                    trigger_price=event.trigger_price, # Can be None
                    **broker_specific_kwargs
                )

                # Process broker response
                if broker_response and isinstance(broker_response, dict):
                    broker_assigned_order_id = broker_response.get('order_id') or broker_response.get('broker_order_id')
                    broker_status_str = str(broker_response.get('status', '')).upper()

                    if broker_assigned_order_id and (broker_status_str in ['PLACED', 'SUBMITTED', 'OPEN', 'PENDING', 'TRIGGER_PENDING']): # Accepted by broker
                        self.logger.info(f"Order {event.order_id} submitted to broker. Broker assigned ID: {broker_assigned_order_id}, Broker Status: {broker_status_str}")

                        self._publish_execution_event(
                            order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                            side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                            status_enum=OrderStatus.SUBMITTED, # System status becomes SUBMITTED
                            price=event.price, trigger_price=event.trigger_price, strategy_id=event.strategy_id,
                            broker_order_id=broker_assigned_order_id, exec_type="NEW_SUBMISSION_ACCEPTED",
                            text=f"Broker status: {broker_status_str}"
                        )
                    elif broker_status_str in ['REJECTED', 'ERROR', 'FAILED']:
                        rejection_reason = broker_response.get('message', broker_response.get('error', 'Broker rejected submission'))
                        self.logger.error(f"Broker rejected order {event.order_id}. Reason: {rejection_reason}. Response: {broker_response}")
                        self._publish_execution_event(
                            order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                            side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                            status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                            rejection_reason=rejection_reason, exec_type="REJECTED_SUBMISSION",
                            broker_order_id=broker_assigned_order_id # Include if broker provides ID even for rejection
                        )
                    else: # Ambiguous response
                        self.logger.warning(f"Ambiguous broker response for order {event.order_id}: {broker_response}. Assuming submission failed for now.")
                        self._publish_execution_event(
                            order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                            side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                            status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                            rejection_reason=f"Ambiguous broker response: {str(broker_response)[:200]}",
                            exec_type="REJECTED_SUBMISSION_AMBIGUOUS",
                            broker_order_id=broker_assigned_order_id
                        )
                else: # No response or unexpected response format
                    self.logger.error(f"Broker failed to place order {event.order_id} or returned unexpected response. Response: {broker_response}")
                    self._publish_execution_event(
                        order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                        side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                        status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                        rejection_reason=f"Broker submission failed/no response: {str(broker_response)[:200]}",
                        exec_type="REJECTED_SUBMISSION_NO_RESPONSE"
                    )

            except Exception as e:
                self.logger.error(f"Error submitting order {event.order_id} via broker: {e}", exc_info=True)
                self._publish_execution_event(
                    order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                    side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                    status_enum=OrderStatus.REJECTED, price=event.price, strategy_id=event.strategy_id,
                    rejection_reason=f"Exception during submission: {str(e)}",
                    exec_type="REJECTED_SUBMISSION_EXCEPTION"
                )
            finally:
                self.currently_submitting.discard(event.order_id)

        # --- Order Cancellation Logic (PENDING_CANCEL status from OrderManager) ---
        elif event_status_enum == OrderStatus.PENDING_CANCEL and event_action_str == "CANCEL":
            if not event.broker_order_id:
                self.logger.error(f"Cannot cancel order {event.order_id}: missing broker_order_id in PENDING_CANCEL event.")
                # Publish ExecutionEvent indicating failure to process cancel
                self._publish_execution_event(
                    order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                    side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                    status_enum=event_status_enum, # Stays PENDING_CANCEL or could be REJECTED_CANCEL_REQUEST
                    price=event.price, strategy_id=event.strategy_id,
                    rejection_reason="Missing broker_order_id for cancellation", exec_type="CANCEL_REQUEST_FAILED"
                )
                return

            self.logger.info(f"ExecutionHandler: Attempting to cancel order {event.order_id} (Broker ID: {event.broker_order_id}) with broker.")
            try:
                # Broker interface's cancel_order might be synchronous or asynchronous.
                # For now, assume it's synchronous and returns success/failure or triggers async updates.
                # The actual confirmation of cancellation will come via broker's execution updates (websocket).
                cancel_response = self.broker_interface.cancel_order(order_id=event.broker_order_id) # Pass broker_order_id

                if cancel_response and isinstance(cancel_response, dict) and str(cancel_response.get('status','')).upper() in ['CANCELLED', 'PENDING_CANCEL', 'SUCCESS', 'OK']: # Adapt to broker's success indicators
                     self.logger.info(f"Order {event.order_id} (Broker ID: {event.broker_order_id}) cancellation request accepted by broker. Response: {cancel_response}")
                     # We don't publish CANCELLED ExecutionEvent here. That should come from broker's feed.
                     # We can publish a CANCEL_REQUESTED_ACK ExecutionEvent if needed.
                     self._publish_execution_event(
                        order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                        side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                        status_enum=OrderStatus.PENDING_CANCEL, # System status remains PENDING_CANCEL until broker confirms
                        price=event.price, strategy_id=event.strategy_id,
                        broker_order_id=event.broker_order_id, exec_type="CANCEL_REQUEST_ACKNOWLEDGED",
                        text=f"Broker ack for cancel: {str(cancel_response)[:100]}"
                     )
                else:
                    self.logger.error(f"Broker failed to accept cancellation for order {event.order_id} (Broker ID: {event.broker_order_id}). Response: {cancel_response}")
                    # Publish an ExecutionEvent indicating cancel request failed at broker
                    self._publish_execution_event(
                        order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                        side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                        status_enum=event_status_enum, # Stays PENDING_CANCEL, OM might retry or mark as failed to cancel
                        price=event.price, strategy_id=event.strategy_id,
                        broker_order_id=event.broker_order_id,
                        rejection_reason=f"Broker rejected cancel request: {str(cancel_response)[:100]}",
                        exec_type="CANCEL_REQUEST_REJECTED_BY_BROKER"
                    )

            except Exception as e:
                self.logger.error(f"Error cancelling order {event.order_id} (Broker ID: {event.broker_order_id}) via broker: {e}", exc_info=True)
                self._publish_execution_event(
                    order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                    side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                    status_enum=event_status_enum, # Stays PENDING_CANCEL
                    price=event.price, strategy_id=event.strategy_id,
                    broker_order_id=event.broker_order_id,
                    rejection_reason=f"Exception during cancel request: {str(e)}",
                    exec_type="CANCEL_REQUEST_EXCEPTION"
                )

        # --- Order Modification Logic (PENDING_REPLACE status from OrderManager) ---
        elif event_status_enum == OrderStatus.PENDING_REPLACE and event_action_str == "MODIFY":
            if not event.broker_order_id:
                self.logger.error(f"Cannot modify order {event.order_id}: missing broker_order_id in PENDING_REPLACE event.")
                self._publish_execution_event(
                    order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                    side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                    status_enum=event_status_enum, # Stays PENDING_REPLACE
                    price=event.price, strategy_id=event.strategy_id,
                    rejection_reason="Missing broker_order_id for modification", exec_type="MODIFY_REQUEST_FAILED"
                )
                return

            # Extract modification parameters from the event.
            # OrderEvent fields like price, quantity, trigger_price should hold the *new* target values.
            # Metadata might also contain them if event fields are not directly updated by OM.
            new_price = event.price # Assumes event.price is the new target price
            new_quantity = event.quantity # Assumes event.quantity is the new target quantity
            new_trigger_price = event.trigger_price # Assumes event.trigger_price is new target

            # Check if metadata has more specific modification details
            if event.metadata:
                new_price = event.metadata.get('price', new_price)
                new_quantity = event.metadata.get('quantity', new_quantity)
                new_trigger_price = event.metadata.get('trigger_price', new_trigger_price)


            self.logger.info(f"ExecutionHandler: Attempting to modify order {event.order_id} (Broker ID: {event.broker_order_id}) with broker. New Px:{new_price}, Qty:{new_quantity}, TrigPx:{new_trigger_price}")
            try:
                modify_response = self.broker_interface.modify_order(
                    order_id=event.broker_order_id, # Broker's order ID
                    price=new_price,
                    quantity=int(new_quantity) if new_quantity is not None else None,
                    trigger_price=new_trigger_price
                    # Potentially other modifiable params from event.metadata
                )
                if modify_response and isinstance(modify_response, dict) and str(modify_response.get('status','')).upper() in ['MODIFIED', 'REPLACED', 'PENDING_REPLACE', 'SUCCESS', 'OK']:
                    self.logger.info(f"Order {event.order_id} (Broker ID: {event.broker_order_id}) modification request accepted by broker. Response: {modify_response}")
                    # Actual MODIFIED status comes from broker execution feed.
                    # Publish ack for modify request.
                    self._publish_execution_event(
                        order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                        side_enum=order_side_enum, quantity=new_quantity, order_type_enum=broker_order_type_enum,
                        status_enum=OrderStatus.PENDING_REPLACE, # System status remains PENDING_REPLACE
                        price=new_price, trigger_price=new_trigger_price, strategy_id=event.strategy_id,
                        broker_order_id=event.broker_order_id, exec_type="MODIFY_REQUEST_ACKNOWLEDGED",
                        text=f"Broker ack for modify: {str(modify_response)[:100]}"
                     )
                else:
                    self.logger.error(f"Broker failed to accept modification for order {event.order_id} (Broker ID: {event.broker_order_id}). Response: {modify_response}")
                    self._publish_execution_event(
                        order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                        side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum, # Original Qty/Px for this event
                        status_enum=event_status_enum, # Stays PENDING_REPLACE
                        price=event.price, strategy_id=event.strategy_id, # Original Px
                        broker_order_id=event.broker_order_id,
                        rejection_reason=f"Broker rejected modify request: {str(modify_response)[:100]}",
                        exec_type="MODIFY_REQUEST_REJECTED_BY_BROKER"
                    )
            except Exception as e:
                self.logger.error(f"Error modifying order {event.order_id} (Broker ID: {event.broker_order_id}) via broker: {e}", exc_info=True)
                self._publish_execution_event(
                    order_id=event.order_id, symbol=event.symbol, exchange_str=broker_exchange_str,
                    side_enum=order_side_enum, quantity=event.quantity, order_type_enum=broker_order_type_enum,
                    status_enum=event_status_enum, # Stays PENDING_REPLACE
                    price=event.price, strategy_id=event.strategy_id,
                    broker_order_id=event.broker_order_id,
                    rejection_reason=f"Exception during modify request: {str(e)}",
                    exec_type="MODIFY_REQUEST_EXCEPTION"
                )
        else:
            self.logger.debug(f"ExecutionHandler: Ignoring OrderEvent for order {event.order_id} with status {event_status_enum.value} and action {event_action_str or 'N/A'}. No specific handler.")


    def _publish_execution_event(self, order_id: str, symbol: str, exchange_str: str,
                                 side_enum: Optional[OrderSide], quantity: float, order_type_enum: Optional[ModelOrderType],
                                 status_enum: OrderStatus, price: Optional[float], strategy_id: Optional[str],
                                 broker_order_id: Optional[str] = None, rejection_reason: Optional[str] = None,
                                 exec_type: str = "UPDATE", text: Optional[str] = None,
                                 trigger_price: Optional[float] = None,
                                 filled_quantity: Optional[float] = None, # Add fill details
                                 average_price: Optional[float] = None,
                                 last_filled_quantity: Optional[float] = None,
                                 last_filled_price: Optional[float] = None,
                                 commission: Optional[float] = None,
                                 execution_id_override: Optional[str] = None, # For specific exec_ids
                                 execution_time_override: Optional[float] = None
                                 ):
        """Helper to create and publish an ExecutionEvent."""
        try:
            # Convert ModelOrderType back to FrameworkOrderType for the event
            framework_order_type = None
            if order_type_enum:
                if isinstance(order_type_enum, ModelOrderType):
                    framework_order_type = FrameworkOrderType(order_type_enum.value)
                elif isinstance(order_type_enum, FrameworkOrderType): # Should not happen if type hint is ModelOrderType
                    framework_order_type = order_type_enum
                else:
                    self.logger.warning(f"Cannot convert order_type {order_type_enum} to FrameworkOrderType for ExecutionEvent.")


            exec_event = ExecutionEvent(
                order_id=order_id, # This is the internal system order ID
                symbol=symbol,
                exchange=Exchange(exchange_str.upper()) if exchange_str else None,
                side=side_enum,
                quantity=quantity,
                order_type=framework_order_type, # Event uses FrameworkOrderType
                status=status_enum, # Event uses OrderStatus
                price=price,
                trigger_price=trigger_price,
                strategy_id=strategy_id,
                broker_order_id=broker_order_id,
                rejection_reason=rejection_reason,
                execution_id=execution_id_override or f"exec-{uuid.uuid4()}",
                execution_time=execution_time_override or time.time(),
                execution_type=exec_type,
                text=text,
                # Fill related fields for ExecutionEvent (can be cumulative or last fill)
                cumulative_filled_quantity=filled_quantity, # Often same as filled_quantity
                average_price=average_price,
                last_filled_quantity=last_filled_quantity,
                last_filled_price=last_filled_price,
                commission=commission,
                event_type=EventType.EXECUTION,
                source=EventSource.EXECUTION_HANDLER,
                timestamp=time.time() # Event's own timestamp
            )
            exec_event.validate()           
            
            self.event_manager.publish(exec_event)
            self.logger.debug(f"Published ExecutionEvent: OrderID={order_id}, ExecType={exec_type}, Status={status_enum.value}, BrokerID={broker_order_id or 'N/A'}")
        except Exception as e:
            self.logger.error(f"Error publishing ExecutionEvent for order {order_id}: {e}", exc_info=True)


    def start(self):
        self.logger.info("Execution Handler started (no background tasks typically)")

    def stop(self):
        self.logger.info("Execution Handler stopped")

# SimulatedExecutionHandler remains largely unchanged from the provided code,
# as the primary fixes were for the main ExecutionHandler and OrderManager.
# If specific fixes are needed for SimulatedExecutionHandler related to these issues,
# they would follow similar patterns (e.g., ensuring correct OrderType mapping).
class SimulatedExecutionHandler(ExecutionHandler):
    """
    Simulated execution handler for backtesting and demo.
    Processes orders against market data and generates fills.
    """

    def __init__(self, event_manager: EventManager, market_data_feed = None, slippage: float = 0.0):
        # For SimulatedExecutionHandler, we don't need a real broker_interface.
        # It acts as its own broker.
        super().__init__(event_manager, broker_interface=self) # Pass self as broker_interface
        self.market_data_feed = market_data_feed
        self.slippage = slippage
        self.internal_orders: Dict[str, ModelOrder] = {} # Tracks orders using ModelOrder for simulation
        # system_to_sim_map: Dict[str, str] = {} # Maps system event.order_id to sim_order.order_id if needed

        if self.market_data_feed:
            # Assuming MarketDataEvent and BarEvent are defined elsewhere
            # from models.events import MarketDataEvent, BarEvent
            # self.event_manager.subscribe(EventType.MARKET_DATA, self.on_market_data, component_name="SimulatedExecutionHandler")
            # self.event_manager.subscribe(EventType.BAR, self.on_market_data, component_name="SimulatedExecutionHandler")
            self.logger.info("SimulatedExecutionHandler subscribed to market data (if events are defined and published).")

        else:
            self.logger.warning("SimulatedExecutionHandler initialized without a market_data_feed. Market order execution might be limited.")

    # --- Implementing BrokerInterface methods for simulation ---
    def is_connected(self) -> bool: return True
    def connect(self) -> bool: return True
    def disconnect(self) -> bool: return True

    def place_order(self, instrument: Instrument, order_type: ModelOrderType, quantity: int,
                   side: str, price: float = None, trigger_price: float = None,
                   client_order_id: Optional[str] = None, # Added to match potential broker call
                   **kwargs) -> Optional[Dict[str, Any]]:
        """
        Simulate placing an order.
        'instrument' is models.Instrument.
        'order_type' is models.order.OrderType (ModelOrderType).
        'side' is string "BUY" or "SELL".
        **kwargs might contain 'strategy_id' or other params from the original OrderEvent's metadata.
        The system_order_id (from the original OrderEvent) should be passed in kwargs if we need to link it.
        """
        system_event_order_id = kwargs.get('system_event_order_id', f"sim-sys-{uuid.uuid4()}") # Get original event.order_id if passed

        self.logger.info(f"SimulatedEH: Received place_order call for SystemID {system_event_order_id}: {instrument.symbol}, {order_type.value}, Qty:{quantity}, Side:{side}, Px:{price}, TrigPx:{trigger_price}")

        # Create an internal ModelOrder for simulation tracking
        sim_internal_order = ModelOrder(
            instrument_id=instrument.instrument_id, # Full ID like "NSE:SBIN"
            quantity=quantity,
            side=side, # "BUY" or "SELL"
            order_type=order_type, # ModelOrderType
            price=price,
            trigger_price=trigger_price,
            exchange=instrument.exchange, # "NSE", "BSE"
            strategy_id=kwargs.get('strategy_id', 'sim_strategy'),
            client_order_id=client_order_id,
            # params = kwargs.get('params') # If other params needed
        )
        # sim_internal_order.order_id is the ID generated by ModelOrder constructor
        sim_broker_order_id = f"sim-broker-{sim_internal_order.order_id}"
        sim_internal_order.broker_order_id = sim_broker_order_id
        sim_internal_order.status = OrderStatus.SUBMITTED # Assume submitted to simulated exchange

        self.internal_orders[sim_internal_order.order_id] = sim_internal_order # Store by its own ID

        self.logger.info(f"Simulated order {sim_internal_order.order_id} (BrokerID: {sim_broker_order_id}) created and marked SUBMITTED for SystemID {system_event_order_id}.")

        # Publish an ExecutionEvent back to the system, using the original system_event_order_id
        self._publish_execution_event(
            order_id=system_event_order_id, # CRITICAL: Use the system's tracking ID from the event
            symbol=instrument.instrument_id,
            exchange_str=instrument.exchange,
            side_enum=OrderSide(side),
            quantity=quantity,
            order_type_enum=order_type, # ModelOrderType
            status_enum=OrderStatus.SUBMITTED,
            price=price,
            trigger_price=trigger_price,
            strategy_id=sim_internal_order.strategy_id,
            broker_order_id=sim_broker_order_id,
            exec_type="NEW_SUBMISSION_ACCEPTED",
            text="Simulated order placed successfully"
        )

        # For market orders, try to fill immediately if market data is available (simplified)
        if order_type == ModelOrderType.MARKET:
            if self.market_data_feed:
                self._try_execute_simulated_order(sim_internal_order, system_event_order_id) # Pass both IDs
            else: # No feed, fill at some assumed price or keep pending
                self.logger.warning(f"Simulated market order {sim_internal_order.order_id} (SystemID {system_event_order_id}) cannot be filled immediately: no market_data_feed.")
                # Optionally, fill with a slight delay or assumed price for testing
                # self._simulate_fill(sim_internal_order, price or 0, quantity, system_event_order_id)


        return {'order_id': sim_broker_order_id, 'status': 'PLACED', 'message': 'Simulated order placed'}


    def cancel_order(self, order_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Simulate cancelling an order. 'order_id' here is the broker_order_id."""
        system_event_order_id = kwargs.get('system_event_order_id', None) # Get original event.order_id if passed

        found_sim_order: Optional[ModelOrder] = None
        sim_order_internal_key = None

        for key, sim_order_obj in self.internal_orders.items():
            if sim_order_obj.broker_order_id == order_id:
                found_sim_order = sim_order_obj
                sim_order_internal_key = key
                break

        if not found_sim_order:
            self.logger.warning(f"SimulatedEH: Cannot cancel order with BrokerID {order_id} - not found.")
            # Publish failure ExecutionEvent if system_event_order_id is known
            if system_event_order_id:
                 self._publish_execution_event(
                    order_id=system_event_order_id, symbol=kwargs.get('symbol', 'UNKNOWN'), exchange_str=kwargs.get('exchange', 'UNKNOWN'),
                    side_enum=None, quantity=0, order_type_enum=None, status_enum=OrderStatus.REJECTED, # Or current status from event
                    price=None, strategy_id=None, broker_order_id=order_id,
                    rejection_reason="Order not found for cancellation", exec_type="CANCEL_REQUEST_FAILED_NOT_FOUND"
                )
            return {'order_id': order_id, 'status': 'REJECTED', 'message': 'Order not found for cancellation'}

        if found_sim_order.status in [OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING_CANCEL]: # Modifiable states
            found_sim_order.status = OrderStatus.CANCELLED
            found_sim_order.canceled_at = datetime.now()
            # del self.internal_orders[sim_order_internal_key] # Or move to a cancelled dict

            self.logger.info(f"Simulated order {found_sim_order.order_id} (BrokerID: {order_id}, SystemID: {system_event_order_id or 'N/A'}) cancelled.")

            self._publish_execution_event(
                order_id=system_event_order_id or found_sim_order.order_id, # Use system ID if available
                symbol=found_sim_order.instrument_id, exchange_str=found_sim_order.exchange,
                side_enum=OrderSide(found_sim_order.side), quantity=found_sim_order.quantity,
                order_type_enum=found_sim_order.order_type, status_enum=OrderStatus.CANCELLED,
                price=found_sim_order.price, strategy_id=found_sim_order.strategy_id,
                broker_order_id=found_sim_order.broker_order_id, exec_type="CANCELLED",
                text="Simulated order cancelled successfully"
            )
            return {'order_id': order_id, 'status': 'CANCELLED', 'message': 'Simulated order cancelled'}
        else:
            self.logger.warning(f"SimulatedEH: Cannot cancel order {found_sim_order.order_id} (BrokerID: {order_id}) - status is {found_sim_order.status.value}.")
            # Publish ExecutionEvent indicating cancel failed due to state
            if system_event_order_id:
                 self._publish_execution_event(
                    order_id=system_event_order_id, symbol=found_sim_order.instrument_id, exchange_str=found_sim_order.exchange,
                    side_enum=OrderSide(found_sim_order.side), quantity=found_sim_order.quantity,
                    order_type_enum=found_sim_order.order_type, status_enum=found_sim_order.status, # Report current status
                    price=found_sim_order.price, strategy_id=found_sim_order.strategy_id, broker_order_id=order_id,
                    rejection_reason=f"Cannot cancel, order in state {found_sim_order.status.value}", exec_type="CANCEL_REQUEST_FAILED_STATE"
                )
            return {'order_id': order_id, 'status': str(found_sim_order.status.value), 'message': f'Cannot cancel order in {found_sim_order.status.value} state.'}


    def modify_order(self, order_id: str, price: float = None, quantity: int = None, trigger_price: float = None, **kwargs) -> Optional[Dict[str, Any]]:
        """Simulate modifying an order. 'order_id' here is the broker_order_id."""
        system_event_order_id = kwargs.get('system_event_order_id', None)

        found_sim_order: Optional[ModelOrder] = None
        for sim_o in self.internal_orders.values():
            if sim_o.broker_order_id == order_id:
                found_sim_order = sim_o
                break
        
        if not found_sim_order:
            self.logger.warning(f"SimulatedEH: Cannot modify order with BrokerID {order_id} - not found.")
            if system_event_order_id:
                 self._publish_execution_event(
                    order_id=system_event_order_id, symbol=kwargs.get('symbol', 'UNKNOWN'), exchange_str=kwargs.get('exchange', 'UNKNOWN'),
                    side_enum=None, quantity=0, order_type_enum=None, status_enum=OrderStatus.REJECTED,
                    price=None, strategy_id=None, broker_order_id=order_id,
                    rejection_reason="Order not found for modification", exec_type="MODIFY_REQUEST_FAILED_NOT_FOUND"
                )
            return {'order_id': order_id, 'status': 'REJECTED', 'message': 'Order not found for modification'}

        if found_sim_order.status in [OrderStatus.SUBMITTED, OrderStatus.OPEN, OrderStatus.PENDING_REPLACE]: # Modifiable states
            if price is not None: found_sim_order.price = price
            if quantity is not None: found_sim_order.quantity = quantity # TODO: Handle partial fills if modifying qty
            if trigger_price is not None: found_sim_order.trigger_price = trigger_price
            
            found_sim_order.status = OrderStatus.OPEN # Assume modification successful, becomes OPEN (or stays OPEN)
            found_sim_order.last_updated = datetime.now()

            self.logger.info(f"Simulated order {found_sim_order.order_id} (BrokerID: {order_id}, SystemID: {system_event_order_id or 'N/A'}) modified. New Px:{price}, Qty:{quantity}, TrigPx:{trigger_price}")

            self._publish_execution_event(
                order_id=system_event_order_id or found_sim_order.order_id,
                symbol=found_sim_order.instrument_id, exchange_str=found_sim_order.exchange,
                side_enum=OrderSide(found_sim_order.side), quantity=found_sim_order.quantity, # New quantity
                order_type_enum=found_sim_order.order_type, status_enum=OrderStatus.OPEN, # Or SUBMITTED if broker uses that for mods
                price=found_sim_order.price, trigger_price=found_sim_order.trigger_price, # New price/trigger
                strategy_id=found_sim_order.strategy_id, broker_order_id=found_sim_order.broker_order_id,
                exec_type="MODIFIED", text="Simulated order modified successfully"
            )
            return {'order_id': order_id, 'status': 'MODIFIED', 'message': 'Simulated order modified'}
        else:
            self.logger.warning(f"SimulatedEH: Cannot modify order {found_sim_order.order_id} (BrokerID: {order_id}) - status is {found_sim_order.status.value}.")
            if system_event_order_id:
                self._publish_execution_event(
                    order_id=system_event_order_id, symbol=found_sim_order.instrument_id, exchange_str=found_sim_order.exchange,
                    side_enum=OrderSide(found_sim_order.side), quantity=found_sim_order.quantity,
                    order_type_enum=found_sim_order.order_type, status_enum=found_sim_order.status,
                    price=found_sim_order.price, strategy_id=found_sim_order.strategy_id, broker_order_id=order_id,
                    rejection_reason=f"Cannot modify, order in state {found_sim_order.status.value}", exec_type="MODIFY_REQUEST_FAILED_STATE"
                )
            return {'order_id': order_id, 'status': str(found_sim_order.status.value), 'message': f'Cannot modify order in {found_sim_order.status.value} state.'}

    def on_market_data(self, event: Event) -> None: # Assuming MarketDataEvent or BarEvent
        if not self.market_data_feed: return # Should not be called if no feed

        # Identify relevant market data (e.g. event.instrument.instrument_id, event.price)
        # For simplicity, this example won't fully implement market data processing for fills.
        # A real implementation would iterate self.internal_orders and check fill conditions.
        self.logger.debug(f"SimulatedEH received market data: {event}")
        for sim_order_id, sim_order in list(self.internal_orders.items()):
            if sim_order.status in [OrderStatus.SUBMITTED, OrderStatus.OPEN]: # Check pending limit/stop orders
                 # This requires a proper market data event structure and logic
                 # self._try_execute_simulated_order(sim_order, system_event_order_id=sim_order.client_order_id or sim_order_id)
                 pass


    def _try_execute_simulated_order(self, sim_order: ModelOrder, system_event_order_id: str, market_price: Optional[float] = None):
        """Simplified execution logic for simulated orders."""
        if sim_order.status not in [OrderStatus.SUBMITTED, OrderStatus.OPEN]:
            return

        exec_price = None
        # For MARKET orders
        if sim_order.order_type == ModelOrderType.MARKET:
            if market_price is not None: # If direct market price is provided (e.g. from a tick)
                exec_price = market_price
            elif self.market_data_feed and hasattr(self.market_data_feed, 'get_latest_tick'):
                tick_data = self.market_data_feed.get_latest_tick(sim_order.instrument_id)
                if tick_data:
                    if sim_order.side == "BUY": exec_price = tick_data.get('ask', tick_data.get('ltp'))
                    else: exec_price = tick_data.get('bid', tick_data.get('ltp'))
            if exec_price is None: # Fallback if no market data
                exec_price = sim_order.price if sim_order.price else (100.0 if sim_order.side == "BUY" else 99.0) # Dummy price
                self.logger.warning(f"Simulated market order {sim_order.order_id} filling at assumed price {exec_price} due to no live market data.")

        # For LIMIT orders
        elif sim_order.order_type == ModelOrderType.LIMIT and market_price is not None:
            if sim_order.side == "BUY" and market_price <= sim_order.price:
                exec_price = min(market_price, sim_order.price)
            elif sim_order.side == "SELL" and market_price >= sim_order.price:
                exec_price = max(market_price, sim_order.price)
        
        # (Add STOP, STOP_LIMIT logic if needed)

        if exec_price is not None:
            # Apply slippage
            slippage_amount = exec_price * self.slippage
            if sim_order.side == "BUY": exec_price += slippage_amount
            else: exec_price -= slippage_amount
            exec_price = round(exec_price, 2) # Round to typical precision

            self._simulate_fill(sim_order, exec_price, sim_order.quantity, system_event_order_id)


    def _simulate_fill(self, sim_order: ModelOrder, fill_price: float, fill_quantity: float, system_event_order_id: str):
        """Helper to generate fill and update events for a simulated order."""
        sim_order.status = OrderStatus.FILLED
        sim_order.average_fill_price = fill_price
        sim_order.filled_quantity = fill_quantity
        # sim_order.remaining_quantity = 0 # ModelOrder might handle this
        sim_order.last_updated = datetime.now() # Or use a simulated fill time

        # Remove from active orders
        # if sim_order.order_id in self.internal_orders: del self.internal_orders[sim_order.order_id]

        self.logger.info(f"Simulated order {sim_order.order_id} (SystemID: {system_event_order_id}) FILLED: {fill_quantity} @ {fill_price}")

        # Publish ExecutionEvent for the fill
        self._publish_execution_event(
            order_id=system_event_order_id, # System's tracking ID
            symbol=sim_order.instrument_id, exchange_str=sim_order.exchange,
            side_enum=OrderSide(sim_order.side), quantity=sim_order.quantity, # Original quantity
            order_type_enum=sim_order.order_type, status_enum=OrderStatus.FILLED,
            price=sim_order.price, # Original limit price
            strategy_id=sim_order.strategy_id, broker_order_id=sim_order.broker_order_id,
            exec_type="TRADE", # Or "FILL"
            text=f"Simulated fill: {fill_quantity} @ {fill_price}",
            filled_quantity=sim_order.filled_quantity, # Cumulative filled
            average_price=sim_order.average_fill_price,
            last_filled_quantity=fill_quantity, # This specific fill's quantity
            last_filled_price=fill_price, # This specific fill's price
            commission=0.0, # Simulated commission
            execution_id_override=f"sim-exec-{uuid.uuid4()}",
            execution_time_override=time.time()
        )

