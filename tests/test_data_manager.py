import unittest
import logging
import time
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from core.event_manager import EventManager
from core.data_manager import DataManager
from models.events import EventType, MarketDataEvent
from models.instrument import Instrument
from utils.constants import Exchange, Timeframe

class MockDataProvider:
    """Mock data provider for testing the data manager."""

    def __init__(self):
        """Initialize with test data."""
        self.historical_data = {}

        # Generate some test data for AAPL
        self.setup_test_data()

    def setup_test_data(self):
        """Set up test data for various instruments and timeframes."""
        # AAPL minute bars
        aapl_minute_bars = []
        base_time = int(datetime(2023, 1, 1, 9, 30).timestamp())

        for i in range(390):  # One trading day (6.5 hours)
            bar = {
                "timestamp": (base_time + i * 60) * 1000,  # Convert to milliseconds
                "symbol": "RELIANCE",
                "open": 2500.0 + (i * 0.1),
                "high": 2500.0 + (i * 0.1) + 0.5,
                "low": 2500.0 + (i * 0.1) - 0.5,
                "close": 2500.0 + (i * 0.1) + 0.2,
                "volume": 1000 + i * 10,
                "timeframe": Timeframe.MINUTE_1.value
            }
            aapl_minute_bars.append(bar)

        self.historical_data[("RELIANCE", "BAR", Timeframe.MINUTE_1.value)] = aapl_minute_bars

        # AAPL hourly bars (derived from minute bars)
        aapl_hourly_bars = []
        for i in range(7):  # 7 hours in trading day
            hour_start = i * 60
            if hour_start + 60 > len(aapl_minute_bars):
                break

            minute_bars_in_hour = aapl_minute_bars[hour_start:hour_start + 60]

            bar = {
                "timestamp": minute_bars_in_hour[0]["timestamp"],
                "symbol": "RELIANCE",
                "open": minute_bars_in_hour[0]["open"],
                "high": max(b["high"] for b in minute_bars_in_hour),
                "low": min(b["low"] for b in minute_bars_in_hour),
                "close": minute_bars_in_hour[-1]["close"],
                "volume": sum(b["volume"] for b in minute_bars_in_hour),
                "timeframe": Timeframe.HOUR_1.value
            }
            aapl_hourly_bars.append(bar)

        self.historical_data[("RELIANCE", "BAR", Timeframe.HOUR_1.value)] = aapl_hourly_bars

        # AAPL daily bars
        aapl_daily_bars = []
        daily_base_time = int(datetime(2023, 1, 1).timestamp())

        for i in range(30):  # 30 days of data
            bar = {
                "timestamp": (daily_base_time + i * 86400) * 1000,  # Convert to milliseconds
                "symbol": "RELIANCE",
                "open": 2500.0 + (i * 1.0),
                "high": 2500.0 + (i * 1.0) + 5.0,
                "low": 2500.0 + (i * 1.0) - 5.0,
                "close": 2500.0 + (i * 1.0) + 2.0,
                "volume": 10000 + i * 1000,
                "timeframe": Timeframe.DAY_1.value
            }
            aapl_daily_bars.append(bar)

        self.historical_data[("RELIANCE", "BAR", Timeframe.DAY_1.value)] = aapl_daily_bars

        # MSFT minute bars
        msft_minute_bars = []
        msft_base_time = int(datetime(2023, 1, 1, 9, 30).timestamp())

        for i in range(390):  # One trading day
            bar = {
                "timestamp": (msft_base_time + i * 60) * 1000,
                "symbol": "TCS",
                "open": 3500.0 + (i * 0.1),
                "high": 3500.0 + (i * 0.1) + 0.5,
                "low": 3500.0 + (i * 0.1) - 0.5,
                "close": 3500.0 + (i * 0.1) + 0.2,
                "volume": 2000 + i * 15,
                "timeframe": Timeframe.MINUTE_1.value
            }
            msft_minute_bars.append(bar)

        self.historical_data[("TCS", "BAR", Timeframe.MINUTE_1.value)] = msft_minute_bars

        # Generate quotes data
        aapl_quotes = []
        quote_base_time = int(datetime(2023, 1, 1, 9, 30).timestamp())

        for i in range(1000):  # 1000 quotes
            quote = {
                "timestamp": (quote_base_time + i * 10) * 1000,  # Every 10 seconds
                "symbol": "RELIANCE",
                "bid": 2500.0 + (i * 0.05),
                "ask": 2500.0 + (i * 0.05) + 0.1,
                "bid_size": 100 + (i % 10) * 10,
                "ask_size": 100 + ((i + 5) % 10) * 10
            }
            aapl_quotes.append(quote)

        self.historical_data[("RELIANCE", "QUOTE")] = aapl_quotes

        # Generate trades data
        aapl_trades = []
        trade_base_time = int(datetime(2023, 1, 1, 9, 30).timestamp())

        for i in range(500):  # 500 trades
            trade = {
                "timestamp": (trade_base_time + i * 20) * 1000,  # Every 20 seconds
                "symbol": "RELIANCE",
                "price": 2500.0 + (i * 0.08),
                "size": 50 + (i % 20) * 5
            }
            aapl_trades.append(trade)

        self.historical_data[("RELIANCE", "TRADE")] = aapl_trades

    def get_historical_data(self, symbol, data_type, timeframe=None, start_time=None, end_time=None, limit=None):
        """Get historical data for a symbol."""
        key = (symbol, data_type)
        if timeframe:
            key = (*key, timeframe)

        if key not in self.historical_data:
            return {"status": "error", "message": f"No data available for {symbol} {data_type}"}

        data = self.historical_data[key]

        # Apply filters
        if start_time:
            data = [d for d in data if d["timestamp"] >= start_time]

        if end_time:
            data = [d for d in data if d["timestamp"] <= end_time]

        if limit and limit < len(data):
            data = data[-limit:]

        return {"status": "success", "data": data}

class DataManagerTest(unittest.TestCase):
    """Test the DataManager component."""

    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.data_manager")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Create a temporary directory for cache
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.temp_dir, "data_cache")

        # Create mock data provider
        self.data_provider = MockDataProvider()

        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()

        # Create data manager with correct initialization
        self.data_manager = DataManager(
            config={},  # Empty config for testing
            event_manager=self.event_manager
        )
        self.data_manager.data_provider = self.data_provider  # Set provider after initialization

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

        # Add instruments to data manager
        self.data_manager.add_instrument(self.aapl)
        self.data_manager.add_instrument(self.msft)

        self.logger.info("DataManager test setup complete")

    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()

        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        self.logger.info("DataManager test teardown complete")

    def test_get_bars(self):
        """Test retrieving bar data."""
        # Get RELIANCE 1-minute bars
        bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=100
        )

        # Check bars were retrieved
        self.assertEqual(len(bars), 100)

        # Check bar data format
        first_bar = bars[0]
        self.assertEqual(first_bar.symbol, "RELIANCE")
        self.assertTrue(hasattr(first_bar, "open"))
        self.assertTrue(hasattr(first_bar, "high"))
        self.assertTrue(hasattr(first_bar, "low"))
        self.assertTrue(hasattr(first_bar, "close"))
        self.assertTrue(hasattr(first_bar, "volume"))
        self.assertTrue(hasattr(first_bar, "timestamp"))
        self.assertEqual(first_bar.timeframe, Timeframe.MINUTE_1)

    def test_get_bars_with_time_range(self):
        """Test retrieving bar data with time range."""
        # Define time range
        end_time = int(datetime(2023, 1, 1, 10, 30).timestamp() * 1000)  # 10:30 AM
        start_time = int(datetime(2023, 1, 1, 10, 0).timestamp() * 1000)  # 10:00 AM

        # Get RELIANCE 1-minute bars for the specified time range
        bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            start_time=start_time,
            end_time=end_time
        )

        # Check bars count (should be about 31 bars for 30-minute range)
        self.assertGreaterEqual(len(bars), 30)
        self.assertLessEqual(len(bars), 31)

        # Check bars are within time range
        for bar in bars:
            self.assertGreaterEqual(bar.timestamp, start_time)
            self.assertLessEqual(bar.timestamp, end_time)

    def test_get_quotes(self):
        """Test retrieving quote data."""
        # Get RELIANCE quotes
        quotes = self.data_manager.get_quotes(
            symbol="RELIANCE",
            limit=50
        )

        # Check quotes were retrieved
        self.assertEqual(len(quotes), 50)

        # Check quote data format
        first_quote = quotes[0]
        self.assertEqual(first_quote.symbol, "RELIANCE")
        self.assertTrue(hasattr(first_quote, "bid"))
        self.assertTrue(hasattr(first_quote, "ask"))
        self.assertTrue(hasattr(first_quote, "bid_size"))
        self.assertTrue(hasattr(first_quote, "ask_size"))
        self.assertTrue(hasattr(first_quote, "timestamp"))

    def test_get_trades(self):
        """Test retrieving trade data."""
        # Get RELIANCE trades
        trades = self.data_manager.get_trades(
            symbol="RELIANCE",
            limit=25
        )

        # Check trades were retrieved
        self.assertEqual(len(trades), 25)

        # Check trade data format
        first_trade = trades[0]
        self.assertEqual(first_trade.symbol, "RELIANCE")
        self.assertTrue(hasattr(first_trade, "price"))
        self.assertTrue(hasattr(first_trade, "size"))
        self.assertTrue(hasattr(first_trade, "timestamp"))

    def test_data_caching(self):
        """Test data caching functionality."""
        # Get data first time (should fetch from provider)
        first_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=200
        )

        # Check cache directory was created
        cache_path = os.path.join(
            self.cache_dir,
            "bars",
            "RELIANCE",
            Timeframe.MINUTE_1.value
        )
        self.assertTrue(os.path.exists(cache_path))

        # Modify the mock provider's data to confirm we get cached data
        original_data = self.data_provider.historical_data[("RELIANCE", "BAR", Timeframe.MINUTE_1.value)][0]["open"]
        self.data_provider.historical_data[("RELIANCE", "BAR", Timeframe.MINUTE_1.value)][0]["open"] += 10.0

        # Get data second time (should use cache)
        second_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=200
        )

        # Check both fetches returned same data
        self.assertEqual(first_bars[0].open, second_bars[0].open)

        # Now with cache disabled, should get updated data
        self.data_manager.enable_cache = False

        third_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=200
        )

        # Check we got updated data
        self.assertNotEqual(first_bars[0].open, third_bars[0].open)
        self.assertEqual(third_bars[0].open, original_data + 10.0)

    def test_bar_resampling(self):
        """Test resampling bars to a higher timeframe."""
        # Get RELIANCE 1-minute bars
        minute_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=60
        )

        # Resample to 5-minute bars
        resampled_bars = self.data_manager.resample_bars(
            bars=minute_bars,
            target_timeframe=Timeframe.MINUTE_5
        )

        # Check number of resampled bars
        self.assertEqual(len(resampled_bars), 12)  # 60 minutes -> 12 5-minute bars

        # Check resampled bar properties
        first_5min_bar = resampled_bars[0]
        source_bars = minute_bars[:5]

        self.assertEqual(first_5min_bar.open, source_bars[0].open)
        self.assertEqual(first_5min_bar.high, max(bar.high for bar in source_bars))
        self.assertEqual(first_5min_bar.low, min(bar.low for bar in source_bars))
        self.assertEqual(first_5min_bar.close, source_bars[-1].close)
        self.assertEqual(first_5min_bar.volume, sum(bar.volume for bar in source_bars))
        self.assertEqual(first_5min_bar.timestamp, source_bars[0].timestamp)
        self.assertEqual(first_5min_bar.timeframe, Timeframe.MINUTE_5)

    def test_time_series_transformations(self):
        """Test time series transformation functions."""
        # Get RELIANCE bar data
        bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=100
        )

        # Extract price series
        close_prices = self.data_manager.extract_field(bars, "close")

        # Check prices array
        self.assertEqual(len(close_prices), 100)
        self.assertTrue(all(isinstance(price, float) for price in close_prices))

        # Calculate returns
        returns = self.data_manager.calculate_returns(close_prices)

        # Check returns array
        self.assertEqual(len(returns), 99)  # One less than input length

        # Calculate moving average
        ma10 = self.data_manager.calculate_moving_average(close_prices, window=10)

        # Check MA array
        self.assertEqual(len(ma10), 91)  # 100 - 10 + 1

        # First MA should be average of first 10 prices
        expected_first_ma = sum(close_prices[:10]) / 10
        self.assertAlmostEqual(ma10[0], expected_first_ma, places=6)

    def test_instrument_management(self):
        """Test adding and removing instruments."""
        # Add new instrument
        googl = Instrument(
            symbol="INFY",
            exchange=Exchange.NSE,
            instrument_type="STOCK",
            tick_size=0.01,
            lot_size=1
        )

        self.data_manager.add_instrument(googl)

        # Check instrument was added
        self.assertIn("INFY", self.data_manager.instruments)

        # Remove instrument
        self.data_manager.remove_instrument("INFY")

        # Check instrument was removed
        self.assertNotIn("INFY", self.data_manager.instruments)

    def test_handle_realtime_market_data(self):
        """Test handling real-time market data updates."""
        # Set up to track the last bar
        last_bar = {"timestamp": 0}

        def update_last_bar(bar):
            nonlocal last_bar
            last_bar = bar

        # Register callback
        self.data_manager.register_bar_callback("RELIANCE", Timeframe.MINUTE_1, update_last_bar)

        # Create a market data event
        now = int(datetime.now().timestamp() * 1000)

        bar_data = {
            "timestamp": now,
            "symbol": "RELIANCE",
            "open": 2550.0,
            "high": 2555.0,
            "low": 2548.0,
            "close": 2552.0,
            "volume": 10000,
            "timeframe": Timeframe.MINUTE_1.value
        }

        # Process the market data
        self.data_manager.process_bar_update(bar_data)

        # Check callback was called with correct data
        self.assertEqual(last_bar["timestamp"], now)
        self.assertEqual(last_bar["close"], 2552.0)

        # Check bar was stored in cache
        cached_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=1
        )

        self.assertEqual(cached_bars[0].close, 2552.0)

    def test_data_subscription(self):
        """Test data subscription management."""
        # Register for data updates
        callback_count = 0
        received_data = None

        def test_callback(data):
            nonlocal callback_count, received_data
            callback_count += 1
            received_data = data

        # Subscribe to RELIANCE bars
        self.data_manager.register_bar_callback("RELIANCE", Timeframe.MINUTE_1, test_callback)

        # Process a bar update
        bar_data = {
            "timestamp": int(datetime.now().timestamp() * 1000),
            "symbol": "RELIANCE",
            "open": 2560.0,
            "high": 2565.0,
            "low": 2558.0,
            "close": 2562.0,
            "volume": 10500,
            "timeframe": Timeframe.MINUTE_1.value
        }

        self.data_manager.process_bar_update(bar_data)

        # Check callback was called
        self.assertEqual(callback_count, 1)
        self.assertEqual(received_data["close"], 2562.0)

        # Unsubscribe
        self.data_manager.unregister_bar_callback("RELIANCE", Timeframe.MINUTE_1, test_callback)

        # Process another update
        bar_data["close"] = 2570.0
        self.data_manager.process_bar_update(bar_data)

        # Check callback wasn't called again
        self.assertEqual(callback_count, 1)
        self.assertEqual(received_data["close"], 2562.0)  # Unchanged

    def test_data_integrity(self):
        """Test data integrity methods."""
        # Get bar data with missing values
        bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=100
        )

        # Introduce some NaN/None values
        bars[10].close = None
        bars[20].volume = None

        # Fill missing values
        filled_bars = self.data_manager.fill_missing_values(bars)

        # Check missing values were filled
        self.assertIsNotNone(filled_bars[10].close)
        self.assertIsNotNone(filled_bars[20].volume)

        # Check interpolation was done correctly (linear)
        expected_close = (bars[9].close + bars[11].close) / 2
        self.assertAlmostEqual(filled_bars[10].close, expected_close, places=4)

    def test_indicator_calculation(self):
        """Test technical indicator calculations."""
        # Get RELIANCE bar data
        bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=100
        )

        # Extract close prices
        close_prices = [bar.close for bar in bars]

        # Calculate SMA
        sma_periods = 20
        sma = self.data_manager.calculate_moving_average(close_prices, window=sma_periods)

        # Check SMA length
        self.assertEqual(len(sma), 100 - sma_periods + 1)

        # First SMA should be average of first N prices
        expected_first_sma = sum(close_prices[:sma_periods]) / sma_periods
        self.assertAlmostEqual(sma[0], expected_first_sma, places=4)

        # Calculate RSI
        rsi_periods = 14
        rsi = self.data_manager.calculate_rsi(close_prices, periods=rsi_periods)

        # Check RSI length and values
        self.assertEqual(len(rsi), 100 - rsi_periods)
        self.assertTrue(all(0 <= r <= 100 for r in rsi))

    def test_price_sampling(self):
        """Test price sampling at specific times."""
        # Get all RELIANCE minute bars for reference
        all_bars = self.data_manager.get_bars(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            limit=390  # Full trading day
        )

        # Define sampling times
        base_time = datetime(2023, 1, 1, 9, 30)
        sample_times = [
            int((base_time + timedelta(minutes=30)).timestamp() * 1000),  # 10:00 AM
            int((base_time + timedelta(minutes=60)).timestamp() * 1000),  # 10:30 AM
            int((base_time + timedelta(minutes=90)).timestamp() * 1000)   # 11:00 AM
        ]

        # Sample prices at specific times
        sampled_prices = self.data_manager.sample_prices_at_times(
            symbol="RELIANCE",
            timeframe=Timeframe.MINUTE_1,
            times=sample_times
        )

        # Check we got samples for each time
        self.assertEqual(len(sampled_prices), 3)

        # Check each sampled price against reference data
        for i, time in enumerate(sample_times):
            # Find the bar in reference data that includes this time
            matching_bars = [bar for bar in all_bars if bar.timestamp <= time]
            if matching_bars:
                expected_price = matching_bars[-1].close  # Use latest bar before the time
                self.assertEqual(sampled_prices[i], expected_price)

if __name__ == "__main__":
    unittest.main()
