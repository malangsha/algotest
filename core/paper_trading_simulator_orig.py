import logging
import time
from typing import Dict, Any

from core.event_manager import EventManager
from models.events import Event, EventType, MarketDataEvent, FillEvent
from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument
from brokers.broker_interface import BrokerInterface # Assuming FinvasiaBroker implements this
from utils.constants import MarketDataType, OrderSide
from datetime import datetime

class PaperTradingSimulator:
    """
    Simulates order fills based on market data for paper trading mode.
    Listens for market data and checks against pending paper orders.
    """
    def __init__(self, event_manager: EventManager, broker_interface: BrokerInterface):
        """
        Initialize the PaperTradingSimulator.

        Args:
            event_manager: The system's EventManager.
            broker_interface: The broker interface holding simulated paper orders.
        """
        self.logger = logging.getLogger(__name__)
        self.event_manager = event_manager
        # Store the broker instance to access paper orders
        # WARNING: This creates a tight coupling. Consider alternative designs
        # if this becomes problematic (e.g., broker publishing paper order updates).
        self.broker_interface = broker_interface
        self._register_event_handlers()
        self.logger.info("Paper Trading Simulator initialized")

    def _register_event_handlers(self):
        """Register to receive MarketData events."""
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._on_market_data,
            component_name="PaperTradingSimulator"
        )
        self.logger.info("PaperTradingSimulator subscribed to MARKET_DATA events")

    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle incoming market data and check for potential paper order fills.

        Args:
            event: The MarketDataEvent.
        """
        if not self.broker_interface or not getattr(self.broker_interface, 'paper_trading', False):
            # If not in paper trading mode or no broker interface, do nothing
            return

        if not hasattr(self.broker_interface, 'simulated_orders') or not hasattr(self.broker_interface, 'get_pending_paper_orders'):
             self.logger.error("Broker interface is missing 'simulated_orders' dict or 'get_pending_paper_orders' method for paper trading.")
             return
       
        # self.logger.info(f"[PAPER] received market data for {event}")

        # Extract market data details
        last_price = event.data.get(MarketDataType.LAST_PRICE)
        bid_price = event.data.get(MarketDataType.BID, last_price) # Use last price if bid missing
        ask_price = event.data.get(MarketDataType.ASK, last_price) # Use last price if ask missing
        instrument_symbol = event.instrument.symbol if event.instrument else None

        if last_price is None or instrument_symbol is None:
            return # Cannot process without price and symbol

        # Check against pending paper orders
        # Use a method on the broker interface to get pending orders
        pending_orders = self.broker_interface.get_pending_paper_orders()

        for order_id, order in list(pending_orders.items()): # Iterate copy for safe modification
            # Ensure order object has necessary attributes
            if not all(hasattr(order, attr) for attr in ['instrument_id', 'order_type', 'side', 'quantity', 'price']):
                self.logger.warning(f"Skipping paper order {order_id}: missing attributes.")
                continue

            if order.instrument_id != instrument_symbol:
                continue # Market data is not for this order's instrument

            fill_price = None
            should_fill = False

            # Determine fill logic based on order type
            if order.order_type == OrderType.MARKET:
                fill_price = ask_price if order.side == OrderSide.BUY.value else bid_price
                should_fill = True
            elif order.order_type == OrderType.LIMIT:
                if order.side == OrderSide.BUY.value and last_price <= order.price:
                    # Buy limit triggered - fill at limit price or better (market ask)
                    fill_price = min(ask_price, order.price)
                    should_fill = True
                elif order.side == OrderSide.SELL.value and last_price >= order.price:
                    # Sell limit triggered - fill at limit price or better (market bid)
                    fill_price = max(bid_price, order.price)
                    should_fill = True
            elif order.order_type == OrderType.STOP:
                # For buy stop orders, trigger when price rises above stop price
                # For sell stop orders, trigger when price falls below stop price
                if order.side == OrderSide.BUY.value and last_price >= order.stop_price:
                    fill_price = ask_price  # Fill at ask for buy orders
                    should_fill = True
                elif order.side == OrderSide.SELL.value and last_price <= order.stop_price:
                    fill_price = bid_price  # Fill at bid for sell orders
                    should_fill = True
            elif order.order_type == OrderType.STOP_LIMIT:
                # For buy stop-limit orders:
                # 1. First check if stop price is hit
                # 2. Then check if limit price is met
                if order.side == OrderSide.BUY.value:
                    if last_price >= order.stop_price and ask_price <= order.price:
                        fill_price = order.price  # Use limit price for stop-limit orders
                        should_fill = True
                # For sell stop-limit orders:
                # 1. First check if stop price is hit
                # 2. Then check if limit price is met
                elif order.side == OrderSide.SELL.value:
                    if last_price <= order.stop_price and bid_price >= order.price:
                        fill_price = order.price  # Use limit price for stop-limit orders
                        should_fill = True

            if should_fill and fill_price is not None:
                self.logger.info(f"[PAPER] Simulating fill for order {order_id} at price {fill_price:.2f}")
                
                # Get available volume from market data
                available_volume = event.data.get(MarketDataType.VOLUME, order.quantity)
                # Calculate fill quantity based on available volume and remaining order quantity
                fill_quantity = min(available_volume, order.quantity - order.filled_quantity)
                
                if fill_quantity <= 0:
                    return  # No fill possible

                # Create FillEvent
                fill_event = FillEvent(
                    event_type=EventType.FILL,
                    timestamp=int(time.time() * 1000),
                    order_id=order_id, # Use the paper order ID
                    symbol=order.instrument_id,
                    exchange=getattr(order, 'exchange', 'PAPER_EX'), # Add exchange if available
                    side=OrderSide(order.side), # Convert string back to Enum if needed
                    quantity=fill_quantity,
                    price=fill_price,
                    commission=0.0, # Simulate zero commission for paper trades
                    strategy_id=order.strategy_id,
                    fill_id=f"PAPER_FILL_{self.broker_interface.next_sim_fill_id}"
                )
                self.broker_interface.next_sim_fill_id += 1

                # Publish the fill event
                self.event_manager.publish(fill_event)

                # Update the order status in the broker's simulated list
                order.filled_quantity = (order.filled_quantity or 0) + fill_quantity
                order.average_fill_price = ((order.average_fill_price or 0) * (order.filled_quantity - fill_quantity) + fill_price * fill_quantity) / order.filled_quantity
                order.last_updated = datetime.now()
                
                # Update order status based on fill quantity
                if order.filled_quantity >= order.quantity:
                    order.status = OrderStatus.FILLED
                else:
                    order.status = OrderStatus.PARTIALLY_FILLED

    def start(self):
        self.logger.info("Paper Trading Simulator started (listening for market data)")
        
        return True

    def stop(self):
        self.logger.info("Paper Trading Simulator stopped") 

        return True