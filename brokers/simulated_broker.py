"""
High-fidelity simulated broker that mimics the FinvasiaBroker interface.
It is designed for robust backtesting and paper trading, providing realistic
order lifecycle simulation, including latency, fills, and rejections,
driven by a configuration file. It communicates asynchronously via the
EventManager.
"""

import time
import uuid
import threading
import random
from typing import Dict, List, Any, Optional

from brokers.broker_interface import BrokerInterface
from models.order import OrderStatus
from models.instrument import Instrument
from utils.exceptions import BrokerError, OrderError
from utils.constants import OrderType as BrokerOrderType
from core.logging_manager import get_logger
from core.event_manager import EventManager
from models.events import SimulatedOrderUpdateEvent # Import the new event

class SimulatedBroker(BrokerInterface):
    """
    A simulated broker that mirrors the FinvasiaBroker's interface and behavior.
    It simulates order processing asynchronously and publishes 
    SimulatedOrderUpdateEvents to the EventManager.
    """

    # Mappings copied from FinvasiaBroker for interface consistency
    ORDER_TYPE_MAPPING = {"MARKET": "MKT", "LIMIT": "LMT", "SL_M": "SL-MKT", "SL": "SL-LMT"}
    STATUS_MAPPING = {
        'COMPLETE': OrderStatus.FILLED, 'REJECTED': OrderStatus.REJECTED,
        'CANCELED': OrderStatus.CANCELLED, 'CANCELLED': OrderStatus.CANCELLED,
        'OPEN': OrderStatus.OPEN, 'PENDING': OrderStatus.PENDING,
        'PARTIALLY FILLED': OrderStatus.PARTIALLY_FILLED,
    }
    PRODUCT_TYPE_MAPPING = {"CNC": "C", "NRML": "M", "MIS": "I"}
    BUY_SELL_MAPPING = {"BUY": "B", "SELL": "S"}
    RETENTION_MAPPING = {"DAY": "DAY", "IOC": "IOC"}

    def __init__(self, config: Dict[str, Any], **kwargs):
        """
        Initializes the SimulatedBroker.

        Args:
            config (Dict[str, Any]): The main application configuration.
            event_manager (EventManager): The system's event manager.
        """
        self.logger = get_logger("brokers.simulated_broker")
        self.event_manager = None
        
        # 1. ✅ Config Parsing
        sim_config = config.get('simulation', {})
        self.broker_config = sim_config.get('broker', {})
        self.order_flow = self.broker_config.get('order_flow', {})
        self.latency_range_ms = self.broker_config.get('latency_range_ms', (10, 50))
        self.slippage_pct = self.broker_config.get('fill_realism', {}).get('slippage_pct', 0.0005)

        self._orders: Dict[str, Dict] = {}
        self._order_lock = threading.Lock()
        self._connected = False
        
        self.logger.info("SimulatedBroker initialized.")

    def set_event_manager(self, event_manager: EventManager):
        self.event_manager = event_manager

    def connect(self) -> bool:
        self.logger.info("Connecting to SimulatedBroker.")
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self.logger.info("Disconnecting from SimulatedBroker.")
        self._connected = False
        return True

    def is_connected(self) -> bool:
        return self._connected

    def place_order(self, instrument: Instrument, order_type: BrokerOrderType, quantity: int, 
                    side: str, price: Optional[float] = None, **kwargs) -> Dict[str, Any]:
        
        if not self.is_connected(): raise OrderError("Not connected to broker")
        broker_order_id = f"sim-{uuid.uuid4()}"
        internal_order = self._create_internal_order_record(broker_order_id, instrument, order_type, quantity, side, price, kwargs)
        
        with self._order_lock:
            self._orders[broker_order_id] = internal_order
        
        threading.Thread(target=self._process_order_flow, args=(broker_order_id, 'place_order')).start()
        self.logger.info(f"Order placed with SimulatedBroker: {broker_order_id}")
        return {'order_id': broker_order_id, 'status': 'PENDING'}

    def modify_order(self, order_id: str, instrument: Instrument, **kwargs) -> Dict[str, Any]:
        with self._order_lock:
            if order_id not in self._orders: raise OrderError(f"Simulated order {order_id} not found.")
            order = self._orders[order_id]
            if order['status'] not in ['PENDING', 'OPEN']: raise OrderError(f"Cannot modify order {order_id} in state {order['status']}")
            order['prc'] = str(kwargs.get('price', order['prc']))
            order['qty'] = str(kwargs.get('quantity', order['qty']))
        
        threading.Thread(target=self._process_order_flow, args=(order_id, 'modify_order')).start()
        self.logger.info(f"Order modification accepted for {order_id}")
        return {'order_id': order_id, 'status': 'PENDING'}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        with self._order_lock:
            if order_id not in self._orders: raise OrderError(f"Simulated order {order_id} not found.")
            order = self._orders[order_id]
            if order['status'] not in ['PENDING', 'OPEN']:
                return {'order_id': order_id, 'status': order['status']}
        
        threading.Thread(target=self._process_order_flow, args=(order_id, 'cancel_order')).start()
        self.logger.info(f"Order cancellation accepted for {order_id}")
        return {'order_id': order_id, 'status': 'PENDING'}

    def _process_order_flow(self, broker_order_id: str, action: str):
        """ ✅ 4. Simulates the lifecycle of an order based on the config. """
        time.sleep(random.uniform(*self.latency_range_ms) / 1000.0)

        with self._order_lock:
            order = self._orders.get(broker_order_id)
            if not order: return

        # Get the flow from config, e.g., ["OPEN", "COMPLETE"]
        flow = self.order_flow.get(action, ["OPEN", "COMPLETE"])
        
        # Initial PENDING state update
        self._publish_simulated_update(order.copy())

        for next_state in flow:
            time.sleep(random.uniform(0.1, 0.5)) # Simulate time between state changes
            
            with self._order_lock:
                # Re-fetch order in case it was cancelled mid-flow
                order = self._orders.get(broker_order_id)
                if not order: break

                order = self._transition_order_state(order, next_state)
                payload_to_publish = order.copy()
            
            self._publish_simulated_update(payload_to_publish)

            if order['status'] in ['REJECTED', 'COMPLETE', 'CANCELED']:
                with self._order_lock:
                    if broker_order_id in self._orders:
                        del self._orders[broker_order_id]
                break
                
    def _transition_order_state(self, order: Dict, next_state: str) -> Dict:
        """Applies the logic for each state transition."""
        order['status'] = next_state
        if next_state == "REJECT":
            order['status'] = 'REJECTED' # Finvasia uses REJECTED
            order['rejreason'] = 'Simulated rejection from config'
        elif next_state == "PARTIAL_FILL":
            order['status'] = 'PARTIALLY FILLED'
            filled_qty = int(int(order['qty']) / 2)
            order['fillshares'] = str(filled_qty)
            # In a real scenario, avgprc would be calculated properly
            order['avgprc'] = order['prc'] 
        elif next_state == "FILL" or next_state == "COMPLETE":
            order['status'] = 'COMPLETE'
            order['fillshares'] = order['qty']
            # Simplified slippage logic
            base_price = float(order['prc'])
            slippage = base_price * self.slippage_pct * (-1 if order['trantype'] == 'B' else 1)
            fill_price = base_price + slippage
            order['avgprc'] = f"{fill_price:.2f}"
            order['flqty'] = order['qty'] # Finvasia payload field for last fill quantity
            order['flprc'] = f"{fill_price:.2f}" # Finvasia payload field for last fill price
        elif next_state == "CANCELED":
            order['status'] = 'CANCELED'
            
        return order
        
    def _publish_simulated_update(self, payload: Dict[str, Any]):
        """ ✅ 2 & 3. Creates and publishes the new event type via the event manager. """
        event = SimulatedOrderUpdateEvent(order_payload=payload)
        self.event_manager.publish(event)
        self.logger.debug(f"SimBroker published update for {payload['norenordno']}: {payload['status']}")

    def _create_internal_order_record(self, broker_id, inst, o_type, qty, side, price, kwargs):
        """Helper to create the initial order dictionary."""
        return {
            "norenordno": broker_id, "status": "PENDING", "tsym": inst.symbol,
            "exch": inst.exchange.value, "trantype": self.BUY_SELL_MAPPING[side.upper()],
            "qty": str(qty), "prc": str(price or 0), "prctyp": o_type.name,
            "fillshares": "0", "avgprc": "0", "flqty": "0", "flprc": "0",
            "remarks": kwargs.get('remarks', f'order_{inst.symbol[:15]}')
        }

    # --- ✅ 5. Complete Interface Compatibility ---
    def get_order_book(self) -> List[Dict[str, Any]]:
        with self._order_lock:
            return [self._process_order_status_response([o]) for o in self._orders.values()]

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        with self._order_lock:
            order = self._orders.get(order_id)
            if order: return self._process_order_status_response([order], order_id)
        raise OrderError(f"Order {order_id} not found in active simulated orders.")

    def get_positions(self) -> List[Dict[str, Any]]:
        self.logger.info("get_positions called on SimulatedBroker. Returning empty list.")
        return []

    def get_account_balance(self) -> Dict[str, Any]:
        self.logger.info("get_account_balance called on SimulatedBroker. Returning dummy data.")
        return {'total_balance': 1e7, 'available_balance': 1e7, 'margin_used': 0.0}

    def _process_order_status_response(self, response: List[Dict], order_id: Optional[str] = None) -> Dict[str, Any]:
        if not response: return {'order_id': order_id, 'status': OrderStatus.UNKNOWN.value}
        order_data = response[0]
        api_status = order_data.get('status', 'UNKNOWN').upper()
        mapped_status = self.STATUS_MAPPING.get(api_status, OrderStatus.UNKNOWN)
        return {'order_id': order_data.get('norenordno'), 'status': mapped_status.value, 'raw_response': order_data}
    
    def get_broker_info(self) -> Dict[str, Any]:
        return {'name': 'SimulatedBroker', 'version': '1.0.0', 'capabilities': ['all']}

    def __str__(self) -> str:
        status = "connected" if self.is_connected() else "disconnected"
        return f"SimulatedBroker(status={status}, active_orders={len(self._orders)})"

    def __repr__(self) -> str:
        return f"SimulatedBroker(connected={self._connected}, config={self.broker_config})"
