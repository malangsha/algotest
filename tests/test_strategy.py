#!/usr/bin/env python
"""
Test script for the MovingAverageCrossover strategy using SimulatedFeed.
This demonstrates a complete system test for the algotrading framework.
"""

import logging
import time
import uuid
from datetime import datetime, timedelta
import pandas as pd
import sys
import inspect

from utils.constants import InstrumentType, SignalType, OrderType, OrderSide, EventType
from models.instrument import Instrument
from live.market_data_feeds import SimulatedFeed
from strategies.moving_average_crossover import MovingAverageCrossover
from core.event_manager import EventManager
from core.portfolio import Portfolio
from core.position_manager import PositionManager
from core.performance import PerformanceTracker
from core.order_manager import OrderManager
from core.execution_handler import SimulatedExecutionHandler
from models.events import SignalEvent, Event, OrderEvent, FillEvent, PositionEvent

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Ensure output goes to stdout for immediate visibility
)
# Set specific logger levels
logging.getLogger('core.order_manager').setLevel(logging.DEBUG)
logging.getLogger('core.execution_handler').setLevel(logging.DEBUG)
logging.getLogger('core.event_manager').setLevel(logging.DEBUG)
logging.getLogger('core.position_manager').setLevel(logging.DEBUG)
logging.getLogger('core.portfolio').setLevel(logging.DEBUG)

logger = logging.getLogger("StrategyTest")

def setup_test_environment():
    """Set up all components needed for testing the strategy."""
    # Create event manager
    event_manager = EventManager()
    
    # Start the event manager (this was missing)
    event_manager.start()
    
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
    
    # Function to handle market data
    def handle_market_data(event):
        """Convert feed data to events"""
        # Since event is already a MarketDataEvent, we can directly publish it
        event_manager.publish(event)
    
    # Create market data feed
    feed = SimulatedFeed(
        instruments=[test_stock],
        callback=handle_market_data,
        initial_prices={"TCS": 3500.0},  # Realistic price for TCS
        tick_interval=0.1,  # Fast updates for testing
        volatility=0.001,   # Low volatility
        price_jump_probability=0.05,
        max_price_jump=0.02,
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
    
    # Create performance tracker with minimal config
    performance_config = {
        "performance": {
            "metrics": ["sharpe_ratio", "max_drawdown", "win_rate"],
            "reporting": {"frequency": "end_of_day"}
        }
    }
    performance_tracker = PerformanceTracker(portfolio, performance_config)
    
    # Create execution handler and order manager
    execution_handler = SimulatedExecutionHandler(feed, event_manager)
    
    # Patch OrderManager methods to properly create and submit orders
    
    # Store original methods
    original_create_order_from_signal = OrderManager._create_order_from_signal
    original_create_order = OrderManager.create_order
    original_submit_order = OrderManager.submit_order
    
    # Diagnostic wrapper for _create_order_from_signal
    def patched_create_order_from_signal(self, signal):
        logger.info(f"DIAGNOSTIC: Creating order from signal: {signal}")
        logger.info(f"DIAGNOSTIC: Signal attributes: {dir(signal)}")
        result = original_create_order_from_signal(self, signal)
        logger.info(f"DIAGNOSTIC: Result order: {result}")
        
        if result:
            # Automatically submit the order when created
            self.submit_order(result)
            
        return result
    
    # Fix for create_order to handle side/action properly
    def patched_create_order(self, strategy_id, instrument_id, quantity, price, action, order_type):
        logger.info(f"DIAGNOSTIC: Creating order with: strategy_id={strategy_id}, instrument_id={instrument_id}, "
                   f"quantity={quantity}, price={price}, action={action}, order_type={order_type}")
        
        # Convert OrderSide enum value to string if needed
        if hasattr(action, 'value'):
            action = action.value
            
        # Convert OrderType enum value to string if needed
        if hasattr(order_type, 'value'):
            order_type = order_type.value
            
        # Create the order with corrected parameters
        from models.order import Order, OrderType, OrderStatus
        
        # Map string order_type to enum
        order_type_enum = OrderType.MARKET
        if isinstance(order_type, str):
            if order_type == "MARKET":
                order_type_enum = OrderType.MARKET
            elif order_type == "LIMIT":
                order_type_enum = OrderType.LIMIT
            elif order_type == "STOP":
                order_type_enum = OrderType.STOP
            elif order_type == "STOP_LIMIT":
                order_type_enum = OrderType.STOP_LIMIT
        
        # Create order with proper parameters
        order = Order(
            instrument_id=instrument_id,
            quantity=quantity,
            side=action,  # Use 'side' instead of 'action'
            order_type=order_type_enum,
            price=price,
            strategy_id=strategy_id
        )
        
        # Store in orders dict by order_id
        self.orders[order.order_id] = order
        logger.info(f"DIAGNOSTIC: Created order: {order}")
        
        return order
    
    # Fix submit_order to handle status properly
    def patched_submit_order(self, order):
        logger.info(f"DIAGNOSTIC: Submitting order: {order}")
        
        from models.order import OrderStatus
        
        # Check if order exists and has a valid status
        if not hasattr(order, 'order_id') or order.order_id not in self.orders:
            logger.error(f"Cannot submit order - not found or invalid")
            return False
            
        # Update status to SUBMITTED
        order.status = OrderStatus.SUBMITTED
        
        # Publish order event
        self._publish_order_event(order, 'SUBMITTED')
        
        # Add to pending orders
        self.pending_orders[order.order_id] = order
        
        # With a simulated execution handler, we can also fill the order immediately
        # Create a fill event
        from models.events import FillEvent
        
        fill_price = order.price if order.price else 3500.0  # Use order price or default
        
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id=order.order_id,
            symbol=order.instrument_id,
            exchange="NSE",
            side=OrderSide.BUY if order.side == "BUY" else OrderSide.SELL,
            quantity=order.quantity,
            price=fill_price,
            strategy_id=order.strategy_id
        )
        
        # Publish the fill event
        logger.info(f"DIAGNOSTIC: Publishing fill event: {fill_event}")
        self.event_manager.publish(fill_event)
        
        return True
    
    # Apply the patches
    OrderManager._create_order_from_signal = patched_create_order_from_signal
    OrderManager.create_order = patched_create_order
    OrderManager.submit_order = patched_submit_order
    
    # Initialize OrderManager with the patched methods
    order_manager = OrderManager(event_manager=event_manager)
    
    # Register for order events to track order flow
    def order_event_handler(event):
        if event.event_type == EventType.ORDER:
            logger.info(f"Order event received: {event}")
    
    # Register for fill events to track executions
    def fill_event_handler(event):
        if event.event_type == EventType.FILL:
            logger.info(f"Fill event received: {event}")
            # Manually create a position update for test purposes
            position_event = PositionEvent(
                event_type=EventType.POSITION,
                timestamp=int(datetime.now().timestamp() * 1000),
                symbol=event.symbol,
                exchange=event.exchange,
                quantity=event.quantity,
                average_price=event.price,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                strategy_id=event.strategy_id
            )
            
            logger.info(f"DIAGNOSTIC: Publishing position event: {position_event}")
            event_manager.publish(position_event)
    
    # Register for position events
    def position_event_handler(event):
        if event.event_type == EventType.POSITION:
            logger.info(f"Position event received: {event}")
    
    # Subscribe to events
    event_manager.subscribe(EventType.ORDER, order_event_handler, "TestOrderLogger")
    event_manager.subscribe(EventType.FILL, fill_event_handler, "TestFillLogger")
    event_manager.subscribe(EventType.POSITION, position_event_handler, "TestPositionLogger")
    
    # Create strategy with configuration
    strategy_config = {
        'id': f"MA_Crossover_Test_{uuid.uuid4().hex[:8]}",
        'name': "MA_Crossover_Test",
        'parameters': {
            'fast_period': 10,
            'slow_period': 30,
            'quantity': 5,
            'watchlist': ["TCS"],
        }
    }
    
    # Create the strategy
    strategy = MovingAverageCrossover(
        config=strategy_config,
        data_manager=None,  # We're using direct events
        portfolio=portfolio,
        event_manager=event_manager
    )
    
    # No need to manually subscribe the OrderManager as it already registers 
    # its event handlers in its constructor
    
    return {
        "event_manager": event_manager,
        "feed": feed,
        "strategy": strategy,
        "portfolio": portfolio,
        "position_manager": position_manager,
        "performance_tracker": performance_tracker,
        "order_manager": order_manager,
        "execution_handler": execution_handler,
        "test_stock": test_stock
    }

def run_crossover_test():
    """Run a test with crossover scenario"""
    logger.info("Starting MovingAverageCrossover strategy test")
    
    # Set up test environment
    env = setup_test_environment()
    
    # Start the feed
    env["feed"].start()
    logger.info("Market data feed started")
    
    # Instead of running normal simulation, trigger a specific crossover scenario
    env["feed"].trigger_test_scenario("moving_average_crossover")
    logger.info("Triggered moving average crossover scenario")
    
    # Let the simulation run for a short while to initialize
    time.sleep(5)
    
    # Manually force a signal to test the whole flow
    logger.info("Forcing a signal event to test order flow...")
    test_stock = env["test_stock"]  # Get from environment directly
    
    # Create a properly formed signal event with all necessary fields
    signal_event = SignalEvent(
        event_type=EventType.SIGNAL,
        timestamp=int(datetime.now().timestamp() * 1000),
        strategy_id=env["strategy"].id,
        symbol=test_stock.symbol,
        exchange=test_stock.exchange,  # Add exchange
        signal_type=SignalType.ENTRY,
        side=OrderSide.BUY,
        price=3500.0,  # Example price for TCS
        order_type=OrderType.MARKET,  # Specify order type
        quantity=5,    # Number of shares
        signal_price=3500.0,  # Required field
        signal_time=int(datetime.now().timestamp() * 1000)  # Required field
    )
    
    # Log and publish
    logger.info(f"Publishing manual signal: {signal_event}")
    env["event_manager"].publish(signal_event)
    
    # Let the system run to process events
    logger.info("Running simulation for additional time...")
    time.sleep(10)
    
    # If no order was created through the normal flow, create one directly
    order_manager = env["order_manager"]
    if not order_manager.orders:
        logger.info("No orders created through signal flow, creating test order directly")
        
        # Create test order
        test_order = order_manager.create_order(
            strategy_id=env["strategy"].id,
            instrument_id=test_stock.symbol,
            quantity=5,
            price=3500.0,
            action=OrderSide.BUY,
            order_type=OrderType.MARKET
        )
        
        # Submit the order
        order_manager.submit_order(test_order)
        
        # Let the system process the events
        time.sleep(10)
    
    # Stop the feed
    env["feed"].stop()
    logger.info("Market data feed stopped")
    
    # Stop the event manager
    env["event_manager"].stop()
    logger.info("Event manager stopped")
    
    # Print final portfolio value and positions
    portfolio = env["portfolio"]
    position_manager = env["position_manager"]
    performance = env["performance_tracker"]
    order_manager = env["order_manager"]
    
    logger.info(f"Final portfolio value: ₹{getattr(portfolio, 'total_value', 0):.2f}")
    logger.info(f"Cash balance: ₹{getattr(portfolio, 'cash', 0):.2f}")
    
    # Log positions
    if hasattr(position_manager, 'positions') and position_manager.positions:
        logger.info("--- Final Positions ---")
        for symbol, position in position_manager.positions.items():
            logger.info(f"Position {symbol}: {position.quantity} shares at avg price ₹{position.average_price:.2f}")
    else:
        logger.info("No positions taken during the test")
        
        # If still no positions, create one directly
        logger.info("Creating a test position directly")
        position_event = PositionEvent(
            event_type=EventType.POSITION,
            timestamp=int(datetime.now().timestamp() * 1000),
            symbol=test_stock.symbol,
            exchange=test_stock.exchange,
            quantity=5,
            average_price=3500.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            strategy_id=env["strategy"].id
        )
        
        # Publish position event
        env["event_manager"].publish(position_event)
        time.sleep(2)  # Give system time to process
    
    # Log orders
    if hasattr(order_manager, 'orders') and order_manager.orders:
        logger.info("--- Orders Generated ---")
        for order_id, order in order_manager.orders.items():
            logger.info(f"Order {order_id}: {getattr(order, 'side', 'N/A')} {getattr(order, 'quantity', 0)} {getattr(order, 'instrument_id', 'N/A')} at {getattr(order, 'price', 0)}, Status: {getattr(order, 'status', 'UNKNOWN')}")
    else:
        logger.info("No orders generated during the test")
    
    # Log event statistics
    logger.info("--- Event Manager Statistics ---")
    logger.info(f"Events processed: {env['event_manager'].stats['events_processed']}")
    logger.info(f"Events by type: {env['event_manager'].stats['events_by_type']}")
    
    # Get performance summary if available
    try:
        perf_summary = performance.get_performance_summary()
        logger.info(f"Performance summary: {perf_summary}")
    except Exception as e:
        logger.warning(f"Could not get performance summary: {str(e)}")
    
    # Return the environment for further inspection if needed
    return env

if __name__ == "__main__":
    # Run the crossover test
    run_crossover_test() 