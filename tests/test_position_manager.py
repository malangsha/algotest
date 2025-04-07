import unittest
import logging
import time
from datetime import datetime

from core.position_manager import PositionManager
from core.event_manager import EventManager
from models.events import FillEvent, OrderSide, EventType, PositionEvent, MarketDataEvent, TradeEvent
from models.instrument import Instrument
from models.position import Position
from utils.constants import MarketDataType, Exchange

class PositionManagerTest(unittest.TestCase):
    """Test the PositionManager component's functionality."""
    
    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.position_manager")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()
        
        # Create position manager
        self.position_manager = PositionManager(self.event_manager)
        
        # Create test instruments
        self.instrument1 = Instrument(symbol="RELIANCE", exchange=Exchange.NSE)
        self.instrument2 = Instrument(symbol="TCS", exchange=Exchange.NSE)
        
        # Add instruments to position manager
        self.position_manager.add_instrument(self.instrument1)
        self.position_manager.add_instrument(self.instrument2)
        
        # Track position events
        self.position_events = []
        self.event_manager.subscribe(
            EventType.POSITION,
            lambda event: self.position_events.append(event),
            component_name="TestPositionTracker"
        )
        
        self.logger.info("PositionManager test setup complete")
        
    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("PositionManager test teardown complete")
        
    def test_add_instrument(self):
        """Test adding instruments to the position manager."""
        # Check existing instruments are tracked
        self.assertIn("RELIANCE", self.position_manager.instruments_by_symbol)
        self.assertIn("TCS", self.position_manager.instruments_by_symbol)
        
        # Add a new instrument
        instrument3 = Instrument(symbol="INFY", exchange=Exchange.NSE)
        self.position_manager.add_instrument(instrument3)
        
        # Check new instrument is tracked
        self.assertIn("INFY", self.position_manager.instruments_by_symbol)
        
        # Test add_instruments method
        instrument4 = Instrument(symbol="SBIN", exchange=Exchange.NSE)
        instrument5 = Instrument(symbol="HDFCBANK", exchange=Exchange.NSE)
        self.position_manager.add_instruments([instrument4, instrument5])
        
        # Check both instruments are tracked
        self.assertIn("SBIN", self.position_manager.instruments_by_symbol)
        self.assertIn("HDFCBANK", self.position_manager.instruments_by_symbol)
        
    def test_create_position_from_fill(self):
        """Test creating a position from a fill event."""
        # Create a buy fill
        buy_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the buy fill
        self.event_manager.publish(buy_fill)
        
        # Wait for processing
        time.sleep(0.1)
        
        # Check position was created
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 10)
        self.assertEqual(position.average_price, 2500.0)
        
        # Check position event was published
        self.assertEqual(len(self.position_events), 1)
        position_event = self.position_events[0]
        self.assertEqual(position_event.symbol, "RELIANCE")
        self.assertEqual(position_event.quantity, 10)
        self.assertEqual(position_event.average_price, 2500.0)
        
    def test_update_position_from_fill(self):
        """Test updating an existing position from a fill event."""
        # First create a position
        buy_fill1 = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy1",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the first buy fill
        self.event_manager.publish(buy_fill1)
        time.sleep(0.1)
        
        # Clear position events
        self.position_events.clear()
        
        # Create a second buy fill
        buy_fill2 = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy2",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=5,
            price=2600.0,
            strategy_id="test_strategy"
        )
        
        # Publish the second buy fill
        self.event_manager.publish(buy_fill2)
        time.sleep(0.1)
        
        # Check position was updated
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 15)  # 10 + 5
        
        # Check average price was updated correctly
        # (10 * 2500 + 5 * 2600) / 15 = (25000 + 13000) / 15 = 38000 / 15 = 2533.33
        expected_avg_price = (10 * 2500.0 + 5 * 2600.0) / 15
        self.assertAlmostEqual(position.average_price, expected_avg_price, delta=0.01)
        
        # Check position event was published
        self.assertEqual(len(self.position_events), 1)
        position_event = self.position_events[0]
        self.assertEqual(position_event.quantity, 15)
        self.assertAlmostEqual(position_event.average_price, expected_avg_price, delta=0.01)
        
    def test_reduce_position_from_fill(self):
        """Test reducing a position from a sell fill event."""
        # First create a position
        buy_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the buy fill
        self.event_manager.publish(buy_fill)
        time.sleep(0.1)
        
        # Clear position events
        self.position_events.clear()
        
        # Create a sell fill
        sell_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_sell",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=5,
            price=2600.0,
            strategy_id="test_strategy"
        )
        
        # Publish the sell fill
        self.event_manager.publish(sell_fill)
        time.sleep(0.1)
        
        # Check position was reduced
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 5)  # 10 - 5
        
        # Check realized PnL was calculated correctly
        expected_pnl = 5 * (2600.0 - 2500.0)  # 5 shares with 100 profit each
        self.assertAlmostEqual(position.realized_pnl, expected_pnl, delta=0.01)
        
        # Check position event was published
        self.assertEqual(len(self.position_events), 1)
        position_event = self.position_events[0]
        self.assertEqual(position_event.quantity, 5)
        
    def test_close_position_from_fill(self):
        """Test closing a position completely from a sell fill event."""
        # First create a position
        buy_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the buy fill
        self.event_manager.publish(buy_fill)
        time.sleep(0.1)
        
        # Clear position events
        self.position_events.clear()
        
        # Create a sell fill for the full position
        sell_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_sell",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.SELL,
            quantity=10,
            price=2600.0,
            strategy_id="test_strategy"
        )
        
        # Publish the sell fill
        self.event_manager.publish(sell_fill)
        time.sleep(0.1)
        
        # Check position was closed but still exists
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 0)
        
        # Check realized PnL was calculated correctly
        expected_pnl = 10 * (2600.0 - 2500.0)  # 10 shares with 100 profit each
        self.assertAlmostEqual(position.realized_pnl, expected_pnl, delta=0.01)
        
        # Check close_timestamp was set
        self.assertIsNotNone(position.close_timestamp)
        
        # Check position event was published
        self.assertEqual(len(self.position_events), 1)
        position_event = self.position_events[0]
        self.assertEqual(position_event.quantity, 0)
        
    def test_position_market_price_update(self):
        """Test updating a position with market price data."""
        # First create a position
        buy_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the buy fill
        self.event_manager.publish(buy_fill)
        time.sleep(0.1)
        
        # Create a market data event
        market_data = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(datetime.now().timestamp() * 1000),
            instrument=self.instrument1,
            exchange=Exchange.NSE,
            data_type=MarketDataType.LAST_PRICE,
            data={
                MarketDataType.LAST_PRICE: 2600.0,
                MarketDataType.TIMESTAMP: int(datetime.now().timestamp() * 1000)
            }
        )
        
        # Publish the market data
        self.event_manager.publish(market_data)
        time.sleep(0.1)
        
        # Check position last price was updated
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.last_price, 2600.0)
        
        # Check unrealized PnL was calculated correctly
        expected_unrealized_pnl = 10 * (2600.0 - 2500.0)  # 10 shares with 100 unrealized profit each
        self.assertAlmostEqual(position.unrealized_pnl, expected_unrealized_pnl, delta=0.01)
        
    def test_apply_trade(self):
        """Test applying a trade event."""
        # Create a trade event
        trade = TradeEvent(
            event_type=EventType.TRADE,
            timestamp=int(datetime.now().timestamp() * 1000),
            trade_id="test_trade",
            order_id="test_order",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the trade
        self.event_manager.publish(trade)
        time.sleep(0.1)
        
        # Check position was created
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 10)
        self.assertEqual(position.average_price, 2500.0)
        
        # Check position event was published
        self.assertEqual(len(self.position_events), 1)
        position_event = self.position_events[0]
        self.assertEqual(position_event.symbol, "RELIANCE")
        self.assertEqual(position_event.quantity, 10)
        
    def test_get_all_positions(self):
        """Test getting all positions."""
        # Create positions for different symbols
        symbols_prices = [("RELIANCE", 2500.0), ("TCS", 3500.0)]
        for symbol, price in symbols_prices:
            fill = FillEvent(
                event_type=EventType.FILL,
                timestamp=int(datetime.now().timestamp() * 1000),
                order_id=f"test_order_{symbol}",
                symbol=symbol,
                exchange=Exchange.NSE,
                side=OrderSide.BUY,
                quantity=10,
                price=price,
                strategy_id="test_strategy"
            )
            self.event_manager.publish(fill)
            time.sleep(0.1)
        
        # Get all positions
        positions = self.position_manager.get_all_positions()
        
        # Check all positions are returned
        self.assertEqual(len(positions), 2)
        self.assertIn("RELIANCE", positions)
        self.assertIn("TCS", positions)
        
        # Check position details
        for symbol, position in positions.items():
            self.assertEqual(position.quantity, 10)
            # Price varies now, check based on symbol
            expected_price = 2500.0 if symbol == "RELIANCE" else 3500.0
            self.assertEqual(position.average_price, expected_price)
            
    def test_handling_unknown_symbol(self):
        """Test handling of fills for unknown symbols."""
        # Create a fill for an unknown symbol
        fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order",
            symbol="UNKNOWN",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=150.0,
            strategy_id="test_strategy"
        )
        
        # Publish the fill
        self.event_manager.publish(fill)
        time.sleep(0.1)
        
        # Check an instrument was automatically created
        self.assertIn("UNKNOWN", self.position_manager.instruments_by_symbol)
        
        # Check position was created
        position = self.position_manager.get_position("UNKNOWN")
        self.assertIsNotNone(position)
        self.assertEqual(position.quantity, 10)
        
    def test_market_data_handling(self):
        """Test more complex market data handling."""
        # First create a position
        buy_fill = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(datetime.now().timestamp() * 1000),
            order_id="test_order_buy",
            symbol="RELIANCE",
            exchange=Exchange.NSE,
            side=OrderSide.BUY,
            quantity=10,
            price=2500.0,
            strategy_id="test_strategy"
        )
        
        # Publish the buy fill
        self.event_manager.publish(buy_fill)
        time.sleep(0.1)
        
        # Create a market data event with OHLC data
        market_data = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(datetime.now().timestamp() * 1000),
            instrument=self.instrument1,
            exchange=Exchange.NSE,
            data_type=MarketDataType.OHLC,
            data={
                MarketDataType.OHLC: {
                    "open": 2550.0,
                    "high": 2650.0,
                    "low": 2550.0,
                    "close": 2600.0,
                    "volume": 1000000
                },
                MarketDataType.TIMESTAMP: int(datetime.now().timestamp() * 1000)
            }
        )
        
        # Publish the market data
        self.event_manager.publish(market_data)
        time.sleep(0.1)
        
        # Check position last price was updated using close price
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.last_price, 2600.0)  # Should use close price from OHLC
        
        # Create another market data event with bid/ask
        market_data2 = MarketDataEvent(
            event_type=EventType.MARKET_DATA,
            timestamp=int(datetime.now().timestamp() * 1000),
            instrument=self.instrument1,
            exchange=Exchange.NSE,
            data_type=MarketDataType.QUOTE,
            data={
                MarketDataType.BID: 2590.0,
                MarketDataType.ASK: 2610.0,
                MarketDataType.TIMESTAMP: int(datetime.now().timestamp() * 1000)
            }
        )
        
        # Publish the market data
        self.event_manager.publish(market_data2)
        time.sleep(0.1)
        
        # Check position last price was updated using mid price
        position = self.position_manager.get_position("RELIANCE")
        self.assertIsNotNone(position)
        self.assertEqual(position.last_price, 2600.0)  # Should use mid price of bid/ask
        
if __name__ == "__main__":
    unittest.main() 