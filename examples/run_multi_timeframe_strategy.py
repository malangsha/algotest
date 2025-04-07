#!/usr/bin/env python
"""
Example of a multi-timeframe strategy.

This example demonstrates how to use multiple timeframes in a strategy.
"""
import sys
import os
import logging
import time
import random
import math
import traceback
import uuid
from datetime import datetime, timedelta
from threading import Thread
import numpy as np
import pandas as pd

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import trading components
from core.event_manager import EventManager
from core.portfolio import Portfolio
from core.position_manager import PositionManager
from models.instrument import Instrument
from core.data_manager import DataManager
from strategies.multi_timeframe_strategy import MultiTimeframeStrategy
from utils.constants import EventType, OrderSide, OrderType, SignalType, OrderStatus
from models.events import Event, MarketDataEvent, BarEvent, TradeEvent, OrderEvent, FillEvent, SignalEvent, ExecutionEvent
from core.logging_manager import setup_logging
from utils.timeframe_manager import TimeframeManager
from core.event_logger import EventLogger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleOrderManager:
    """
    Component that listens for signal events and converts them to order events.
    This is the missing link between strategy signals and trade execution.
    """
    def __init__(self, event_manager, broker=None):
        """
        Initialize the order manager.
        
        Args:
            event_manager: Event manager to subscribe to and publish events
            broker: Optional broker for direct order execution
        """
        self.logger = logging.getLogger("SimpleOrderManager")
        self.event_manager = event_manager
        self.broker = broker
        
        # Subscribe to signal events
        self.event_manager.subscribe(
            EventType.SIGNAL, 
            self._on_signal_event,
            component_name="SimpleOrderManager"
        )
        
        # Track orders we've created
        self.orders = {}
        
        self.logger.info("SimpleOrderManager initialized and subscribed to signals")
        
    def _on_signal_event(self, event):
        """
        Handle signal events and convert them to orders.
        
        Args:
            event: Signal event
        """
        try:
            if not hasattr(event, 'signal_type') or not hasattr(event, 'symbol'):
                self.logger.warning(f"Received invalid signal event: {event}")
                return
                
            self.logger.info(f"Processing signal for {event.symbol}: {event.signal_type}")
            
            # Extract data directly from SignalEvent fields
            symbol = event.symbol
            signal_type = event.signal_type
            
            # Extract order parameters directly from SignalEvent attributes
            side = getattr(event, 'side', None)
            quantity = getattr(event, 'quantity', None)
            price = getattr(event, 'price', None)
            order_type = getattr(event, 'order_type', OrderType.MARKET)
            
            if not side or not quantity or not price:
                self.logger.error(f"Signal event missing required fields: {event}")
                return
                
            # Create order ID
            order_id = str(uuid.uuid4())
            
            # Create order data for tracking
            order_data = {
                'order_id': order_id,
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'price': price,
                'order_type': order_type,
                'timestamp': int(time.time() * 1000),
                'strategy_id': getattr(event, 'strategy_id', None),
                'status': 'PENDING'
            }
            
            # Create OrderEvent with direct parameters (not with data dictionary)
            order_event = OrderEvent(
                event_type=EventType.ORDER,
                timestamp=int(time.time() * 1000),
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                order_type=order_type,
                status=OrderStatus.OPEN,
                strategy_id=getattr(event, 'strategy_id', None)
            )
            
            self.logger.info(f"Created order: {order_id} for {symbol} - {side} {quantity} @ {price}")
            
            # Track this order
            self.orders[order_id] = order_data
            
            # Publish the order event
            self.event_manager.publish(order_event)
            
            # If we have a broker, submit the order directly
            if self.broker and hasattr(self.broker, 'submit_order'):
                try:
                    self.logger.info(f"Submitting order to broker: {order_id}")
                    self.broker.submit_order(order_event)
                except Exception as e:
                    self.logger.error(f"Error submitting order to broker: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
        except Exception as e:
            self.logger.error(f"Error processing signal event: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

class MarketDataSimulator:
    """
    Simulates market data for testing strategies.
    Generates ticks for multiple instruments with optional volatility and trends.
    """
    def __init__(self, 
                 event_manager, 
                 instruments,
                 start_date=None,
                 end_date=None,
                 price_volatility=0.002,
                 tick_interval=0.05,
                 market_data_handlers=None,
                 queue_threshold=0.7):
        """
        Initialize the market data simulator.
        
        Args:
            event_manager: EventManager instance for publishing events
            instruments: List of Instrument objects to simulate
            start_date: Optional start date for simulation (datetime)
            end_date: Optional end date for simulation (datetime)
            price_volatility: Volatility factor for price changes (0.002 = 0.2%)
            tick_interval: Time between ticks in seconds
            market_data_handlers: Optional list of handlers for market data events
            queue_threshold: Threshold for queue fullness (0-1) before slowing down
        """
        self.logger = logging.getLogger(__name__)
        self.event_manager = event_manager
        self.instruments = instruments
        self.current_prices = {}
        self.last_tick_time = {}
        self.start_date = start_date or datetime.now()
        self.end_date = end_date
        self.current_datetime = self.start_date
        self.reached_end_date = False
        self.price_volatility = price_volatility
        self.tick_interval = tick_interval
        self.last_generate_time = 0
        self.market_data_handlers = market_data_handlers or []
        self.queue_threshold = queue_threshold
        
        # Initialize ticks counter to help with rate control
        self.ticks_generated = 0
        self.last_rate_check = time.time()
        self.tick_rate = 0
        
        # Initialize price trends and shock schedule
        self.price_trends = {}
        self.price_shock_schedule = {}
        
        # Initialize prices based on symbol
        for instrument in self.instruments:
            symbol = instrument.symbol
            # Set initial prices based on the symbol
            # For Indian stocks, we'll use realistic starting prices
            if symbol == "RELIANCE":
                self.current_prices[symbol] = 2500.0  # Example starting price for Reliance
            elif symbol == "TCS":
                self.current_prices[symbol] = 3500.0  # Example starting price for TCS
            elif symbol == "HDFCBANK":
                self.current_prices[symbol] = 1600.0  # Example starting price for HDFC Bank
            elif symbol == "INFY":
                self.current_prices[symbol] = 1800.0  # Example starting price for Infosys
            else:
                # For any other symbol, use a random price between 500 and 5000
                self.current_prices[symbol] = random.uniform(500, 5000)
            
            # Initialize last tick time to now
            self.last_tick_time[symbol] = time.time()
            
            # Assign random trend bias for each symbol
            self.price_trends[symbol] = random.uniform(-0.0003, 0.0003)  # Small directional bias
            
            # Schedule 1-3 price shocks for each instrument
            self.price_shock_schedule[symbol] = []
            num_shocks = random.randint(1, 3)
            for _ in range(num_shocks):
                # Schedule shock at random point in the simulation
                shock_time = random.uniform(0, 1)  # Time point in the simulation (0-1)
                shock_magnitude = random.uniform(-0.05, 0.05)  # -5% to +5% shock
                self.price_shock_schedule[symbol].append((shock_time, shock_magnitude))
        
        self.logger.info(f"Initialized market data simulator with {len(instruments)} instruments")
        self.logger.info(f"Initial prices: {self.current_prices}")
        
    def start(self):
        """Start the market data simulation."""
        self.logger.info("Starting market data simulation")
        self.last_generate_time = time.time()
        
    def should_generate_tick(self):
        """
        Determine if we should generate a tick based on rate limiting and queue size.
        
        Returns:
            bool: True if we should generate a tick, False otherwise
        """
        current_time = time.time()
        
        # Check if enough time has passed since last tick
        if current_time - self.last_generate_time < self.tick_interval:
            return False
            
        # Check event queue size and back off if too full
        if hasattr(self.event_manager, 'event_queue'):
            queue_size = self.event_manager.event_queue.qsize()
            queue_capacity = self.event_manager.event_queue.maxsize
            queue_usage = queue_size / queue_capacity
            
            if queue_usage > self.queue_threshold:
                # Logarithmically increase back-off as queue fills up
                fullness_factor = (queue_usage - self.queue_threshold) / (1 - self.queue_threshold)
                backoff_time = self.tick_interval * (1 + 5 * fullness_factor)
                
                if queue_usage > 0.9:
                    self.logger.warning(f"Queue nearly full ({queue_usage:.1%}), backing off for {backoff_time:.3f}s")
                    
                time.sleep(backoff_time)
                return False
                
        return True

    def generate_ticks_for_all_symbols(self):
        """
        Generate tick data for all instruments if rate limiting allows.
        """
        # FIRST LINE: Add forced delay to prevent runaway tick generation
        time.sleep(self.tick_interval)
        
        # Then check if we should generate a tick
        if not self.should_generate_tick():
            return

        # Update the simulation time
        self.current_datetime += timedelta(seconds=self.tick_interval)
        self.last_generate_time = time.time()
        self.ticks_generated += 1
        
        # Check if we've reached the end date
        if self.end_date and self.current_datetime > self.end_date:
            self.logger.info("Reached end date for simulation")
            self.reached_end_date = True
            return
            
        # Update tick rate statistics
        if self.ticks_generated % 100 == 0:
            current_time = time.time()
            time_diff = current_time - self.last_rate_check
            if time_diff > 0:
                self.tick_rate = 100 / time_diff
                self.last_rate_check = current_time
                self.logger.debug(f"Current tick generation rate: {self.tick_rate:.1f} ticks/second")
        
        # Generate ticks for each instrument
        for instrument in self.instruments:
            symbol = instrument.symbol
            
            # Calculate price movement
            # Base random component
            random_change = random.normalvariate(0, 1) * self.price_volatility
            
            # Trend component
            trend_component = self.price_trends[symbol]
            
            # Sine wave component for cyclical behavior (period of about 500 ticks)
            cycle_component = math.sin(self.ticks_generated / 100.0) * 0.0005
            
            # Check for scheduled price shocks
            shock_component = 0
            if self.price_shock_schedule[symbol]:
                simulation_progress = 0
                if self.end_date:
                    # Calculate progress based on dates
                    total_duration = (self.end_date - self.start_date).total_seconds()
                    elapsed_duration = (self.current_datetime - self.start_date).total_seconds()
                    simulation_progress = elapsed_duration / total_duration if total_duration > 0 else 0
                else:
                    # Use arbitrary measure based on ticks
                    simulation_progress = min(1.0, self.ticks_generated / 10000)
                
                # Check each scheduled shock
                for shock_time, shock_magnitude in self.price_shock_schedule[symbol]:
                    # If we're close to a scheduled shock time
                    if abs(simulation_progress - shock_time) < 0.01:
                        shock_component = shock_magnitude
                        self.logger.info(f"Price shock for {symbol}: {shock_magnitude*100:.1f}%")
                        # Remove this shock so it doesn't trigger again
                        self.price_shock_schedule[symbol].remove((shock_time, shock_magnitude))
                        break
            
            # Combine all components for final price change
            price_change = random_change + trend_component + cycle_component + shock_component
            
            # Update price
            old_price = self.current_prices[symbol]
            self.current_prices[symbol] *= (1.0 + price_change)
            
            # Log significant price movements
            if abs(price_change) > 0.01:  # 1% change
                self.logger.info(f"Significant price movement for {symbol}: {price_change*100:.2f}% to {self.current_prices[symbol]:.2f}")
            
            # Create and publish market data event
            tick_data = {
                'symbol': symbol,
                'price': self.current_prices[symbol],
                'volume': random.randint(10, 1000),
                'timestamp': int(self.current_datetime.timestamp() * 1000),  # milliseconds
                # Add fields that PositionManager expects
                'LAST_PRICE': self.current_prices[symbol],
                'OHLC': {
                    'open': self.current_prices[symbol],
                    'high': self.current_prices[symbol],
                    'low': self.current_prices[symbol],
                    'close': self.current_prices[symbol]
                }
            }
            
            # Create MarketDataEvent without the direct price parameter
            market_data_event = MarketDataEvent(
                event_type=EventType.MARKET_DATA,
                timestamp=int(self.current_datetime.timestamp() * 1000),
                instrument=instrument,
                data=tick_data,
                data_type="TRADE"
            )
            
            # Publish the event if queue isn't too full
            if hasattr(self.event_manager, 'event_queue'):
                queue_size = self.event_manager.event_queue.qsize()
                queue_capacity = self.event_manager.event_queue.maxsize
                
                if queue_size < 0.9 * queue_capacity:
                    self.event_manager.publish(market_data_event)
                    
                    # Directly call any registered market data handlers
                    for handler in self.market_data_handlers:
                        try:
                            handler(market_data_event)
                        except Exception as e:
                            self.logger.error(f"Error in market data handler: {e}")
                else:
                    self.logger.warning(f"Skipped publishing tick for {symbol} - event queue at {queue_size}/{queue_capacity}")
            else:
                self.event_manager.publish(market_data_event)
        
        # Log prices periodically
        if self.ticks_generated % 500 == 0:
            self.logger.info(f"Current prices after {self.ticks_generated} ticks:")
            for symbol, price in self.current_prices.items():
                self.logger.info(f"  {symbol}: {price:.2f}")
        
        # Return success
        return True

    def stop(self):
        """Stop the market data simulation."""
        self.logger.info("Stopping market data simulation")
        self.reached_end_date = True  # Use this flag to stop the simulation

def main():
    """Main entry point for the example."""
    # Create EventManager with large queue to avoid overflow
    event_manager = EventManager(queue_size=10000)
    
    # Start the event manager
    event_manager.start()
    logger.info("Event manager started")
    
    # Setup the enhanced EventLogger
    event_logger = EventLogger(
        event_manager=event_manager,
        log_interval=10,  # Log every 10 seconds
        log_level=logging.INFO,
        # Track only important event types to reduce overhead
        track_event_types=[
            EventType.MARKET_DATA,
            EventType.BAR,
            EventType.SIGNAL,
            EventType.ORDER,
            EventType.EXECUTION,
            EventType.FILL,
            EventType.TRADE,
            EventType.STRATEGY
        ]
    )
    
    # Register the event logger with the event manager
    event_manager.register_event_logger(event_logger)   
    
        # For SIGNAL events, add extra detailed logging
    def log_signal_events():
        # Create an event processing function that logs signal events
        def on_signal_event(event):
            """Debug log signal events"""
            symbol = event.symbol if hasattr(event, 'symbol') else "unknown"
            signal_type = event.signal_type if hasattr(event, 'signal_type') else "unknown"
            logger.info(f"SIGNAL EVENT RECEIVED: {symbol} - {signal_type}")
            
            # Log details if available
            if hasattr(event, 'data') and isinstance(event.data, dict):
                side = event.data.get('side')
                quantity = event.data.get('quantity')
                price = event.data.get('price')
                
                if side and quantity and price:
                    logger.info(f"  Details: {side} {quantity} @ {price:.2f}")
        
        # Subscribe to signal events
        event_manager.subscribe(EventType.SIGNAL, on_signal_event, component_name="SignalLogger")
    
    # Add detailed signal logging
    log_signal_events()
    
    # For ORDER and TRADE events, add detailed logging
    def log_order_trade_events():
        # Create an event processor that logs order events
        def on_order_event(event):
            logger.info(f"===== ORDER GENERATED =====")
            logger.info(f"Symbol: {event.symbol}")
            logger.info(f"Type: {event.order_type}")
            logger.info(f"Side: {event.side}")
            logger.info(f"Quantity: {event.quantity}")
            logger.info(f"Price: {event.price if event.price else 'MARKET'}")
            logger.info(f"===========================")
        
        # Create an event processor that logs trade events
        def on_trade_event(event):
            logger.info(f"!!! TRADE EXECUTED !!!")
            logger.info(f"Symbol: {event.symbol}")
            logger.info(f"Quantity: {event.quantity}")
            logger.info(f"Price: ₹{event.price:.2f}")
            logger.info(f"Total Value: ₹{event.quantity * event.price:.2f}")
            logger.info(f"!!!!!!!!!!!!!!!!!!!!!!")
        
        # Subscribe to event types
        event_manager.subscribe(EventType.ORDER, on_order_event, component_name="OrderLogger")
        event_manager.subscribe(EventType.TRADE, on_trade_event, component_name="TradeLogger")
    
    # Add order and trade logging
    log_order_trade_events()
    
    # Create a simple MockBroker to execute orders
    class MockBroker:
        def __init__(self, event_manager):
            self.logger = logging.getLogger("MockBroker")
            self.event_manager = event_manager
            self.orders_executed = 0
            
        def submit_order(self, order_event):
            """Execute a mock order immediately"""
            self.orders_executed += 1
            
            # Extract order details directly from order_event properties
            symbol = order_event.symbol
            quantity = order_event.quantity
            price = order_event.price
            side = order_event.side
            order_id = order_event.order_id
            strategy_id = order_event.strategy_id
            
            self.logger.info(f"EXECUTING ORDER #{self.orders_executed} (ID: {order_id})")
            self.logger.info(f"  Symbol: {symbol}")
            self.logger.info(f"  Side: {side}")
            self.logger.info(f"  Quantity: {quantity}")
            self.logger.info(f"  Price: {price}")
            
            # First, create an ExecutionEvent to link OrderEvent and FillEvent
            execution_event = ExecutionEvent(
                event_type=EventType.EXECUTION,
                timestamp=int(time.time() * 1000),
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                status=OrderStatus.FILLED,
                strategy_id=strategy_id,
                execution_id=str(uuid.uuid4()),
                last_filled_quantity=quantity,
                last_filled_price=price
            )
            
            # Publish the execution event first
            self.event_manager.publish(execution_event)
            
            # Create FillEvent with direct parameters
            fill_event = FillEvent(
                event_type=EventType.FILL,
                timestamp=int(time.time() * 1000),
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                commission=price * quantity * 0.001,  # 0.1% commission
                strategy_id=strategy_id
            )
            
            # Publish the fill event
            self.event_manager.publish(fill_event)
            
            # Create TradeEvent with direct parameters
            trade_event = TradeEvent(
                event_type=EventType.TRADE,
                timestamp=int(time.time() * 1000),
                trade_id=str(uuid.uuid4()),
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                commission=price * quantity * 0.001,
                strategy_id=strategy_id
            )
            
            # Publish the trade event
            self.event_manager.publish(trade_event)
            
            return order_id
    
    # Setup broker
    mock_broker = MockBroker(event_manager)
    
    # Create the order manager to handle signals and generate orders
    order_manager = SimpleOrderManager(event_manager, mock_broker)
    
    # Diagnostic tracking
    last_event_stats_time = time.time()
    event_stats_interval = 5  # Log event stats every 5 seconds
    prev_published = 0
    prev_processed = 0
    
    # Create a diagnostic logging function
    def log_event_stats():
        nonlocal last_event_stats_time, prev_published, prev_processed
        current_time = time.time()
        if current_time - last_event_stats_time > event_stats_interval:
            # Calculate rates
            time_diff = current_time - last_event_stats_time
            current_published = event_manager.stats["events_published"]
            current_processed = event_manager.stats["events_processed"]
            
            publish_rate = (current_published - prev_published) / time_diff
            process_rate = (current_processed - prev_processed) / time_diff
            
            # Get current queue size
            queue_size = event_manager.event_queue.qsize()
            queue_capacity = event_manager.event_queue.maxsize
            
            # Log stats
            logger.info(f"EVENT STATS: Queue={queue_size}/{queue_capacity} ({(queue_size/queue_capacity)*100:.1f}%), " +
                       f"Published={publish_rate:.1f}/s, Processed={process_rate:.1f}/s, " +
                       f"Diff={(publish_rate-process_rate):.1f}/s")
            
            # Update tracking variables
            last_event_stats_time = current_time
            prev_published = current_published
            prev_processed = current_processed
    
    # Fix: Use config dictionary instead of initial_cash parameter
    portfolio_config = {
        "portfolio": {
            "initial_capital": 500000.0  # 5 lakhs initial capital for Indian market trading
        }
    }
    
    # Create a position manager
    position_manager = PositionManager(event_manager)
    
    # Create a portfolio with correct constructor parameters
    portfolio = Portfolio(
        config=portfolio_config,
        position_manager=position_manager,
        event_manager=event_manager
    )
    
    # Add a fill event handler to update portfolio cash correctly
    def on_fill_event(event):
        """Update portfolio cash when fills happen"""
        try:
            symbol = event.symbol
            quantity = event.quantity
            price = event.price
            side = event.side
            commission = getattr(event, 'commission', 0)
            
            # Calculate cash impact
            if side == OrderSide.BUY:
                cash_impact = -(quantity * price + commission)
            else:  # SELL
                cash_impact = quantity * price - commission
                
            # Update portfolio cash explicitly
            logger.info(f"Updating portfolio cash: {cash_impact:.2f} from {symbol} {side} {quantity} @ {price}")
            
            # Ensure cash doesn't go negative
            if hasattr(portfolio, 'update_cash'):
                # If there's an update_cash method, use it
                portfolio.update_cash(cash_impact)
                
                # Check if cash is negative and log warning
                if hasattr(portfolio, 'cash') and portfolio.cash < 0:
                    logger.warning(f"Portfolio cash went negative: {portfolio.cash:.2f}. This might not be realistic.")
                    # Optionally reset to zero or add margin
                    # portfolio.cash = 0  # Uncomment to prevent negative cash
            elif hasattr(portfolio, 'cash'):
                # Direct attribute update
                new_cash = portfolio.cash + cash_impact
                
                # Check if cash would go negative
                if new_cash < 0:
                    logger.warning(f"Portfolio cash would go negative: {new_cash:.2f}. Using margin instead.")
                    # Apply the update but mark it as using margin
                    portfolio.cash = new_cash
                    # Consider adding a margin attribute to track margin usage
                else:
                    portfolio.cash = new_cash
                    
                logger.info(f"New portfolio cash balance: {portfolio.cash:.2f}")
        except Exception as e:
            logger.error(f"Error updating cash on fill: {e}")
            logger.error(traceback.format_exc())
            
    # Subscribe to fill events to track cash
    event_manager.subscribe(EventType.FILL, on_fill_event, component_name="PortfolioCashUpdater")
    
    # Setup instruments for Indian stocks
    instruments = [
        Instrument(symbol="RELIANCE", security_type="STOCK", exchange="NSE"),
        Instrument(symbol="HDFCBANK", security_type="STOCK", exchange="NSE"),
        Instrument(symbol="TCS", security_type="STOCK", exchange="NSE"),
        Instrument(symbol="INFY", security_type="STOCK", exchange="NSE"),
    ]
    
    # Create a data manager for multi-timeframe data
    data_config = {
        'market_data': {
            'max_bars': 1000,
            'timeframes': ['1m', '5m', '15m'],  # Multiple timeframes
        }
    }
    
    # Create a mock broker for the DataManager
    class MockBroker:
        def __init__(self):
            self.name = "MockBroker"
            
        def get_market_data(self, symbol, data_type=None):
            # This method would normally fetch data from a broker
            return None
    
    # Initialize the DataManager with the correct parameters
    data_manager = DataManager(
        config=data_config,
        event_manager=event_manager,
        broker=MockBroker()
    )
    
    # Create a strategy that uses multiple timeframes
    strategy_config = {
        'name': 'Multi-Timeframe Strategy',
        'description': 'Strategy using multiple timeframes for Indian markets',
        'timeframe': '1s',  # Use 1-second as primary timeframe for faster testing
        'additional_timeframes': ['5s'],  # Use 5-second as confirmation timeframe
        'fast_ma_period': 5,  # Much shorter period for testing
        'slow_ma_period': 10,  # Shorter period for testing
        'confirmation_ma_period': 7,  # Shorter period for testing
        'trade_size': 100,  # Increase trade size
        'instruments': instruments
    }
    
    # Create the strategy with correct constructor parameters
    strategy = MultiTimeframeStrategy(
        config=strategy_config,
        data_manager=data_manager,
        portfolio=portfolio,
        event_manager=event_manager
    )
    
    # Setup direct connection from timeframe manager to the strategy
    if hasattr(data_manager, 'timeframe_manager'):
        # Create reference to the timeframe manager for direct processing
        timeframe_manager = data_manager.timeframe_manager
        logger.info("Connected TimeframeManager to strategy for direct bar updates")
    else:
        logger.warning("TimeframeManager not found in DataManager - bar updates will rely on events only")
        timeframe_manager = None
    
    # Set simulator start and end dates (use today and tomorrow for short test)
    simulator_start_date = datetime.now() 
    simulator_end_date = simulator_start_date + timedelta(days=1)
    run_duration = 120  # Run for 2 minutes to allow time for trades
    
    # Define the tick processing callback with proper error handling
    def process_tick_with_logging(event):
        """Process a market data event tick with the TimeframeManager and log the action."""
        try:
            symbol = event.instrument.symbol
            
            # Extract tick data in the format TimeframeManager expects
            tick_data = {
                'timestamp': event.data.get('timestamp', int(time.time() * 1000)),
                'price': event.data.get('price', 0),
                'volume': event.data.get('volume', 0)
            }
            
            logger.info(f"Processing tick for {symbol} at {tick_data['price']}")
            
            # Call the TimeframeManager with the correct parameters (symbol and tick_data)
            result = timeframe_manager.process_tick(symbol, tick_data)
            
            # Log if any bars were completed
            if result and any(result.values()):
                completed_bars = []
                for tf, completed in result.items():
                    if completed:
                        completed_bars.append(tf)
                        
                if completed_bars:
                    logger.info(f"Completed bars for {symbol}: {', '.join(completed_bars)}")
                    
                    # Get the bars from the TimeframeManager
                    for tf in completed_bars:
                        bars = timeframe_manager.get_bars(symbol, tf)
                        if bars is not None and not bars.empty:
                            latest_bar = bars.iloc[-1]
                            logger.info(f"New {tf} bar for {symbol}: O:{latest_bar['open']:.2f} H:{latest_bar['high']:.2f} L:{latest_bar['low']:.2f} C:{latest_bar['close']:.2f} V:{latest_bar['volume']}")
                            
                            # Create BarEvent with individual parameters instead of a data dictionary
                            bar_event = BarEvent(
                                event_type=EventType.BAR,
                                instrument=event.instrument,
                                timeframe=tf,
                                timestamp=int(latest_bar.name * 1000),  # Index is timestamp
                                open_price=float(latest_bar['open']),
                                high_price=float(latest_bar['high']),
                                low_price=float(latest_bar['low']),
                                close_price=float(latest_bar['close']),
                                volume=float(latest_bar['volume'])
                            )
                            
                            # Publish the bar event
                            event_manager.publish(bar_event)
            
            return result
        except Exception as e:
            logger.error(f"Error in process_tick_with_logging: {e}")
            # Return None or empty dict to indicate failure
            return {}
    
    # Create market data simulator with increased volatility and realistic parameters
    simulator = MarketDataSimulator(
        event_manager=event_manager,
        instruments=instruments,
        start_date=simulator_start_date,
        end_date=simulator_end_date,
        price_volatility=0.01,    # Higher volatility (1% per tick) for more dramatic price movements
        tick_interval=0.5,       # Generate ticks every 500ms (2 ticks/sec) - more realistic timing
        queue_threshold=0.7,     # Start backing off when queue is 70% full
        market_data_handlers=[process_tick_with_logging] if timeframe_manager else []
    )
    
    # Start the strategy
    strategy.start()
    logger.info("Strategy started")
    
    # Start the simulator
    simulator.start()
    logger.info("Simulator started")

    # Main simulation loop
    logger.info("Starting simulation...")
    start_time = time.time()
    tick_count = 0
    progress_interval = 500  # Log progress every 500 ticks
    
    try:
        while time.time() - start_time < run_duration and not simulator.reached_end_date:
            # Generate a tick for each instrument
            simulator.generate_ticks_for_all_symbols()
            tick_count += 1
            
            # Log event stats periodically
            log_event_stats()
            
            # Check if event queue is healthy
            queue_size = event_manager.event_queue.qsize()
            queue_capacity = event_manager.event_queue.maxsize
            queue_usage = queue_size / queue_capacity
            
            if queue_usage > 0.9:
                logger.warning(f"Event queue approaching capacity: {queue_size}/{queue_capacity} ({queue_usage*100:.1f}%)")
                
                # Diagnostics when queue is near full
                market_data_subscribers = len(event_manager._subscribers.get(EventType.MARKET_DATA, []))
                bar_data_subscribers = len(event_manager._subscribers.get(EventType.BAR_DATA, []))
                
                logger.warning(f"Subscribers: MARKET_DATA={market_data_subscribers}, BAR_DATA={bar_data_subscribers}")
                logger.warning(f"Events published: {event_manager.stats['events_published']}, processed: {event_manager.stats['events_processed']}")
                
                # Slow down if queue is getting full
                time.sleep(0.1)
                
            # Log progress periodically 
            if tick_count % progress_interval == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {tick_count} ticks generated in {elapsed:.2f}s ({tick_count/elapsed:.2f} ticks/sec)")
                
                # Check for portfolio.calculate_portfolio_value or portfolio.get_total_value
                try:
                    portfolio_value = portfolio.get_total_value() if hasattr(portfolio, 'get_total_value') else portfolio.calculate_portfolio_value()
                    logger.info(f"Portfolio value: {portfolio_value:.2f}")
                    
                    # Display positions if any
                    positions = None
                    if hasattr(portfolio.position_manager, 'get_positions'):
                        positions = portfolio.position_manager.get_positions()
                    elif hasattr(portfolio.position_manager, 'get_all_positions'):
                        positions = portfolio.position_manager.get_all_positions()
                        
                    if positions:
                        for symbol, position in positions.items():
                            # Adapt to different position object structures
                            qty = getattr(position, 'quantity', None)
                            if qty is None and hasattr(position, 'get_quantity'):
                                qty = position.get_quantity()
                                
                            avg_price = getattr(position, 'average_price', None)
                            if avg_price is None and hasattr(position, 'get_average_price'):
                                avg_price = position.get_average_price()
                                
                            if qty and avg_price:
                                logger.info(f"Position: {symbol} - {qty} @ {avg_price:.2f}")
                except Exception as e:
                    logger.error(f"Error displaying portfolio: {e}")
                    
                # Display strategy state if the method exists
                if hasattr(strategy, 'get_state'):
                    try:
                        logger.info(f"Strategy state: {strategy.get_state()}")
                    except Exception as e:
                        logger.error(f"Error getting strategy state: {e}")
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    except Exception as e:
        logger.error(f"Error during simulation: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Create a strategy summary message before stopping
        strategy_summary = "Multi-Timeframe Strategy completed successfully"
        
        # Get more detailed strategy state if available
        if hasattr(strategy, 'get_state'):
            try:
                state = strategy.get_state()
                strategy_summary += f" - {state}"
            except Exception:
                pass
        
        # Count signals generated
        signals_count = getattr(strategy, 'signals_generated', 0)
        if signals_count:
            strategy_summary += f" | Generated {signals_count} signals"
            
        # Get some statistics from the strategy if available
        if hasattr(strategy, 'fast_ma') and hasattr(strategy, 'slow_ma'):
            for symbol in [instr.symbol for instr in instruments]:
                fast_ma_values = getattr(strategy.fast_ma, symbol, None)
                slow_ma_values = getattr(strategy.slow_ma, symbol, None)
                if fast_ma_values and slow_ma_values and len(fast_ma_values) > 0 and len(slow_ma_values) > 0:
                    last_fast = fast_ma_values[-1] if len(fast_ma_values) > 0 else None
                    last_slow = slow_ma_values[-1] if len(slow_ma_values) > 0 else None
                    if last_fast and last_slow:
                        position = "BULLISH" if last_fast > last_slow else "BEARISH"
                        strategy_summary += f" | {symbol}: {position}"
        
        # Log the strategy summary
        logger.info(f"Strategy Summary: {strategy_summary}")
        
        # Publish the strategy summary as an event - create a proper event without using 'data'
        strategy_event = Event(
            event_type=EventType.STRATEGY,
            timestamp=int(time.time() * 1000)
        )
        # Add message attribute manually
        strategy_event.message = strategy_summary
        
        # Publish the event
        event_manager.publish(strategy_event)
        
        # Stop everything
        logger.info("Stopping all components...")
        
        # Stop components in a specific order
        simulator.stop()
        strategy.stop()
        data_manager.stop()
        
        # Stop event logger
        if event_logger:
            event_logger.stop()
            logger.info("EventLogger stopped")
            
        # Stop event manager last
        event_manager.stop()
        logger.info("EventManager stopped")

if __name__ == "__main__":
    main()
