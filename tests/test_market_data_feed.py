import unittest
import logging
import time
from datetime import datetime
from unittest.mock import Mock, patch

from core.event_manager import EventManager
from live.market_data_feeds import MarketDataFeed
from models.events import MarketDataEvent, EventType
from models.market_data import Quote, Trade, Bar, OHLCV
from models.instrument import Instrument
from utils.constants import Exchange, Timeframe

class MockDataProvider:
    """Mock data provider for testing the market data feed."""
    
    def __init__(self):
        self.subscriptions = {}
        self.historical_data = {}
        self.connected = False
        
        # Set up some test data
        self.quotes = {
            "AAPL": {
                "symbol": "RELIANCE",
                "bid": 150.0,
                "ask": 150.1,
                "bid_size": 100,
                "ask_size": 150,
                "timestamp": int(datetime.now().timestamp() * 1000)
            },
            "MSFT": {
                "symbol": "MSFT",
                "bid": 250.0,
                "ask": 250.2,
                "bid_size": 200,
                "ask_size": 250,
                "timestamp": int(datetime.now().timestamp() * 1000)
            }
        }
        
        self.trades = {
            "AAPL": {
                "symbol": "RELIANCE",
                "price": 150.05,
                "size": 100,
                "timestamp": int(datetime.now().timestamp() * 1000)
            },
            "MSFT": {
                "symbol": "MSFT",
                "price": 250.1,
                "size": 150,
                "timestamp": int(datetime.now().timestamp() * 1000)
            }
        }
        
        self.bars = {
            "AAPL": {
                "symbol": "RELIANCE",
                "open": 149.0,
                "high": 151.0,
                "low": 148.5,
                "close": 150.05,
                "volume": 5000,
                "timestamp": int(datetime.now().timestamp() * 1000),
                "timeframe": Timeframe.MINUTE_1.value
            },
            "MSFT": {
                "symbol": "MSFT",
                "open": 249.0,
                "high": 251.0,
                "low": 248.5,
                "close": 250.1,
                "volume": 6000,
                "timestamp": int(datetime.now().timestamp() * 1000),
                "timeframe": Timeframe.MINUTE_1.value
            }
        }
        
    def connect(self):
        """Connect to the mock data provider."""
        self.connected = True
        return {"status": "success", "message": "Connected successfully"}
        
    def disconnect(self):
        """Disconnect from the mock data provider."""
        self.connected = False
        return {"status": "success", "message": "Disconnected successfully"}
        
    def subscribe(self, symbol, data_type, timeframe=None):
        """Subscribe to a data feed."""
        key = (symbol, data_type)
        if timeframe:
            key = (*key, timeframe)
            
        self.subscriptions[key] = True
        return {"status": "success", "message": f"Subscribed to {symbol} {data_type}"}
        
    def unsubscribe(self, symbol, data_type, timeframe=None):
        """Unsubscribe from a data feed."""
        key = (symbol, data_type)
        if timeframe:
            key = (*key, timeframe)
            
        if key in self.subscriptions:
            del self.subscriptions[key]
            
        return {"status": "success", "message": f"Unsubscribed from {symbol} {data_type}"}
        
    def get_last_quote(self, symbol):
        """Get the last quote for a symbol."""
        if symbol in self.quotes:
            return {"status": "success", "data": self.quotes[symbol]}
        else:
            return {"status": "error", "message": f"No quote data for {symbol}"}
            
    def get_last_trade(self, symbol):
        """Get the last trade for a symbol."""
        if symbol in self.trades:
            return {"status": "success", "data": self.trades[symbol]}
        else:
            return {"status": "error", "message": f"No trade data for {symbol}"}
            
    def get_last_bar(self, symbol, timeframe):
        """Get the last bar for a symbol and timeframe."""
        if symbol in self.bars:
            bar = self.bars[symbol].copy()
            bar["timeframe"] = timeframe.value if hasattr(timeframe, "value") else timeframe
            return {"status": "success", "data": bar}
        else:
            return {"status": "error", "message": f"No bar data for {symbol}"}
            
    def get_historical_data(self, symbol, data_type, timeframe=None, start_time=None, end_time=None, limit=None):
        """Get historical data for a symbol."""
        key = (symbol, data_type)
        if timeframe:
            key = (*key, timeframe)
            
        if key in self.historical_data:
            return {"status": "success", "data": self.historical_data[key]}
        elif data_type == "BAR" and symbol in self.bars:
            # Generate some fake historical bars
            current_time = int(datetime.now().timestamp() * 1000)
            interval = 60 * 1000  # 1 minute in ms
            
            # Create 10 bars
            bars = []
            for i in range(10):
                bar = self.bars[symbol].copy()
                bar["timestamp"] = current_time - (i * interval)
                bars.append(bar)
                
            return {"status": "success", "data": bars}
        else:
            return {"status": "error", "message": f"No historical data for {symbol} {data_type}"}
            
    # Methods to simulate streaming data
    def simulate_quote_update(self, symbol, bid, ask, bid_size=None, ask_size=None):
        """Simulate a quote update."""
        if symbol not in self.quotes:
            self.quotes[symbol] = {
                "symbol": symbol,
                "bid": 0,
                "ask": 0,
                "bid_size": 0,
                "ask_size": 0,
                "timestamp": 0
            }
            
        self.quotes[symbol]["bid"] = bid
        self.quotes[symbol]["ask"] = ask
        
        if bid_size is not None:
            self.quotes[symbol]["bid_size"] = bid_size
            
        if ask_size is not None:
            self.quotes[symbol]["ask_size"] = ask_size
            
        self.quotes[symbol]["timestamp"] = int(datetime.now().timestamp() * 1000)
        
        return self.quotes[symbol]
        
    def simulate_trade_update(self, symbol, price, size):
        """Simulate a trade update."""
        if symbol not in self.trades:
            self.trades[symbol] = {
                "symbol": symbol,
                "price": 0,
                "size": 0,
                "timestamp": 0
            }
            
        self.trades[symbol]["price"] = price
        self.trades[symbol]["size"] = size
        self.trades[symbol]["timestamp"] = int(datetime.now().timestamp() * 1000)
        
        return self.trades[symbol]
        
    def simulate_bar_update(self, symbol, open_price, high, low, close, volume, timeframe=Timeframe.MINUTE_1):
        """Simulate a bar update."""
        if symbol not in self.bars:
            self.bars[symbol] = {
                "symbol": symbol,
                "open": 0,
                "high": 0,
                "low": 0,
                "close": 0,
                "volume": 0,
                "timestamp": 0,
                "timeframe": Timeframe.MINUTE_1.value
            }
            
        self.bars[symbol]["open"] = open_price
        self.bars[symbol]["high"] = high
        self.bars[symbol]["low"] = low
        self.bars[symbol]["close"] = close
        self.bars[symbol]["volume"] = volume
        self.bars[symbol]["timestamp"] = int(datetime.now().timestamp() * 1000)
        self.bars[symbol]["timeframe"] = timeframe.value if hasattr(timeframe, "value") else timeframe
        
        return self.bars[symbol]

class MarketDataFeedTest(unittest.TestCase):
    """Test the MarketDataFeed component."""
    
    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.market_data_feed")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()
        
        # Create instruments for testing
        self.aapl = Instrument(
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )
        
        self.msft = Instrument(
            symbol="TCS",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )
        
        # Create market data feed with instruments and settings
        self.market_data_feed = MarketDataFeed(
            feed_type="SIMULATED",
            broker=None,  # No broker needed for simulated feed
            instruments=[self.aapl, self.msft],
            event_manager=self.event_manager,
            settings={
                'initial_prices': {
                    'RELIANCE': 2500.0,
                    'TCS': 3500.0
                },
                'volatility': 0.001,
                'tick_interval': 0.1  # Reduced tick interval for faster test execution
            }
        )
        
        # Track market data events
        self.market_data_events = []
        
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            lambda event: self.market_data_events.append(event),
            component_name="TestMarketDataTracker"
        )
        
        self.logger.info("MarketDataFeed test setup complete")
        
    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("MarketDataFeed test teardown complete")
        
    def test_start_and_stop(self):
        """Test starting and stopping the market data feed."""
        # Start the feed
        self.market_data_feed.start()
        # Wait for events to start
        time.sleep(0.5)  # Reduced wait time since we reduced tick_interval
        self.assertTrue(len(self.market_data_events) > 0, "No market data events received after starting feed")

        # Stop the feed
        self.market_data_feed.stop()
        # Check if feed is stopped by verifying no new events
        event_count = len(self.market_data_events)
        time.sleep(0.5)  # Reduced wait time since we reduced tick_interval
        self.assertEqual(len(self.market_data_events), event_count, "New events received after stopping feed")

    def test_subscribe_and_unsubscribe(self):
        """Test subscribing and unsubscribing to instruments."""
        # Create a new instrument
        infy = Instrument(
            symbol="INFY",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )
        
        # Start the feed first
        self.market_data_feed.start()
        time.sleep(0.2)  # Reduced wait time since we reduced tick_interval

        # Subscribe to the instrument
        self.market_data_feed.subscribe(infy)
        # Wait for subscription to take effect and events to be generated
        time.sleep(0.5)  # Reduced wait time since we reduced tick_interval
        # Check if we receive market data events for the new instrument
        infy_events = [e for e in self.market_data_events if e.symbol == infy.symbol and e.event_type == EventType.MARKET_DATA and not hasattr(e, 'is_teardown')]
        self.assertTrue(len(infy_events) > 0, "No market data events received for subscribed instrument")

        # Clear the events list to only check for new events after unsubscribe
        self.market_data_events.clear()

        # Unsubscribe from the instrument
        self.market_data_feed.unsubscribe(infy)
        # Wait for unsubscription to take effect
        time.sleep(0.5)  # Reduced wait time since we reduced tick_interval
        # Check if we still receive market data events for the unsubscribed instrument
        infy_events = [e for e in self.market_data_events if e.symbol == infy.symbol and e.event_type == EventType.MARKET_DATA and not hasattr(e, 'is_teardown')]
        self.assertEqual(len(infy_events), 0, "Still receiving market data events for unsubscribed instrument")

        # Stop the feed
        self.market_data_feed.stop()

    def test_market_data_events(self):
        """Test that market data events are generated."""
        # Start the feed
        self.market_data_feed.start()

        # Wait for some events to be generated
        time.sleep(2)

        # Stop the feed
        self.market_data_feed.stop()

        # Check that events were generated
        self.assertTrue(len(self.market_data_events) > 0)

        # Check event properties
        for event in self.market_data_events:
            self.assertEqual(event.event_type, EventType.MARKET_DATA)
            self.assertIsNotNone(event.data)
            self.assertIsNotNone(event.symbol)
        
    def test_error_handling(self):
        """Test error handling in the market data feed."""
        # Mock the callback to raise an exception
        def error_callback(event):
            raise Exception("Test error")

        # Create a new feed with error callback
        error_feed = MarketDataFeed(
            feed_type="SIMULATED",
            broker=None,  # No broker needed for simulated feed
            instruments=[self.aapl],
            event_manager=self.event_manager
        )
        error_feed.callback = error_callback

        # Start the feed - should not raise exception
        error_feed.start()
        time.sleep(1)
        error_feed.stop()

if __name__ == "__main__":
    unittest.main() 
