#!/usr/bin/env python
"""
Simple test script to test the signal generation and order flow
"""

import logging
import time
import uuid
from datetime import datetime, timedelta

from utils.constants import InstrumentType, SignalType, OrderType, OrderSide, EventType
from models.instrument import Instrument
from models.events import SignalEvent
from core.event_manager import EventManager
from core.portfolio import Portfolio
from core.position_manager import PositionManager
from core.order_manager import OrderManager
from core.execution_handler import SimulatedExecutionHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SignalTest")

def setup_test_environment():
    """Set up a minimal test environment for signal testing"""
    # Create event manager
    event_manager = EventManager()
    
    # Create test instrument - Use TCS from NSE
    test_stock = Instrument(
        symbol="TCS",
        instrument_id="TCS",
        instrument_type=InstrumentType.EQUITY,
        exchange="NSE",
        currency="INR",
        lot_size=1,
        tick_size=0.05,
    )
    
    # Create position manager
    position_manager = PositionManager()
    
    # Create portfolio with configuration
    portfolio_config = {
        "portfolio": {
            "initial_capital": 1000000.0,  # Higher capital for Indian stocks
            "max_leverage": 1.0,
            "max_position_size": 0.1,
            "max_position_value": 100000.0
        }
    }
    portfolio = Portfolio(
        config=portfolio_config,
        position_manager=position_manager,
        event_manager=event_manager
    )
    
    # Initialize OrderManager
    order_manager = OrderManager(event_manager=event_manager)
    
    # Simple execution handler that just routes orders
    execution_handler = SimulatedExecutionHandler(None, event_manager)
    
    return {
        "event_manager": event_manager,
        "instrument": test_stock,
        "portfolio": portfolio,
        "position_manager": position_manager,
        "order_manager": order_manager,
        "execution_handler": execution_handler,
    }

def test_signal_to_order_flow():
    """Test the flow from signal to order to fill"""
    logger.info("Starting signal to order flow test")
    
    # Set up test environment
    env = setup_test_environment()
    
    # Extract components
    event_manager = env["event_manager"]
    instrument = env["instrument"]
    order_manager = env["order_manager"]
    
    # Create and send a test signal
    signal_event = SignalEvent(
        event_type=EventType.SIGNAL,
        timestamp=int(datetime.now().timestamp() * 1000),
        strategy_id="test_strategy",
        symbol=instrument.symbol,
        signal_type=SignalType.ENTRY,
        side=OrderSide.BUY,
        price=3500.0,  # Example price for TCS
        quantity=5  # Number of shares
    )
    
    logger.info(f"Publishing test signal: {signal_event}")
    event_manager.publish(signal_event)
    
    # Give the system time to process the signal and create orders
    logger.info("Waiting for signal processing...")
    time.sleep(2)
    
    # Check if orders were created
    if hasattr(order_manager, 'orders') and order_manager.orders:
        logger.info("--- Orders Generated ---")
        for order_id, order in order_manager.orders.items():
            logger.info(f"Order {order_id}: {getattr(order, 'side', 'N/A')} {getattr(order, 'quantity', 0)} {getattr(order, 'symbol', 'N/A')} at {getattr(order, 'price', 0)}, Status: {getattr(order, 'status', 'UNKNOWN')}")
    else:
        logger.info("No orders generated during the test")
    
    # Manually simulate fills
    if hasattr(order_manager, 'orders') and order_manager.orders:
        logger.info("Simulating fill for the first order...")
        first_order_id = next(iter(order_manager.orders))
        first_order = order_manager.orders[first_order_id]
        
        from models.events import FillEvent
        
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=first_order_id,
            symbol=instrument.symbol,
            quantity=getattr(first_order, 'quantity', 5),
            price=3500.0,
            commission=35.0  # Example commission
        )
        
        logger.info(f"Publishing fill event: {fill_event}")
        event_manager.publish(fill_event)
        
        # Wait for the fill to be processed
        time.sleep(2)
    
    # Check final positions
    position_manager = env["position_manager"]
    if hasattr(position_manager, 'positions') and position_manager.positions:
        logger.info("--- Final Positions ---")
        for symbol, position in position_manager.positions.items():
            logger.info(f"Position {symbol}: {position.quantity} shares at avg price ₹{position.average_price:.2f}")
    else:
        logger.info("No positions taken during the test")
    
    return env

if __name__ == "__main__":
    # Run the signal test
    test_signal_to_order_flow() 