import unittest
import logging
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from core.event_manager import EventManager
from core.performance import PerformanceTracker
from models.events import EventType
from models.portfolio import PortfolioEvent
from models.position import PositionEvent
from utils.metrics import PerformanceMetrics, TradeStats
from models.trade import Trade
from core.position_manager import PositionManager

class PerformanceManagerTest(unittest.TestCase):
    """Test the PerformanceManager component."""
    
    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.performance_manager")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Create event manager
        self.event_manager = EventManager(enable_monitoring=True)
        self.event_manager.start()
        
        # Mock Portfolio and PositionManager for PerformanceTracker init
        self.mock_portfolio = MagicMock()
        self.mock_portfolio.initial_capital = 100000.0
        self.mock_portfolio.calculate_portfolio_value.return_value = 100000.0
        
        self.mock_position_manager = MagicMock(spec=PositionManager)
        
        # Create performance manager
        self.performance_manager = PerformanceTracker(
            portfolio=self.mock_portfolio, # Pass mock portfolio
            config={"performance": {}}, # Pass minimal config
            event_manager=self.event_manager,
            position_manager=self.mock_position_manager
        )
        
        # Track performance events
        self.performance_events = []
        
        self.event_manager.subscribe(
            EventType.PERFORMANCE,
            lambda event: self.performance_events.append(event),
            component_name="TestPerformanceTracker"
        )
        
        # Set up test data timestamps
        self.start_time = int(datetime.now().timestamp() * 1000)
        
        self.logger.info("PerformanceManager test setup complete")
        
    def tearDown(self):
        """Clean up after the test."""
        self.event_manager.stop()
        self.logger.info("PerformanceManager test teardown complete")
        
    def publish_portfolio_update(self, cash, equity, timestamp=None):
        """Helper method to publish a portfolio update event."""
        if timestamp is None:
            timestamp = int(datetime.now().timestamp() * 1000)
            
        portfolio_event = PortfolioEvent(
            event_type=EventType.PORTFOLIO,
            timestamp=timestamp,
            cash=cash,
            equity=equity,
            positions_value=(equity - cash),
            positions_count=1 if equity > cash else 0
        )
        
        self.event_manager.publish(portfolio_event)
        
        # Small delay to ensure event processing
        time.sleep(0.01)
        
        return portfolio_event
        
    def test_initial_metrics(self):
        """Test initial performance metrics."""
        # Get initial metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check initial values
        self.assertEqual(metrics.initial_capital, 100000.0)
        self.assertEqual(metrics.current_capital, 100000.0)
        self.assertEqual(metrics.total_return, 0.0)
        self.assertEqual(metrics.daily_returns, {})
        self.assertEqual(metrics.drawdowns, [])
        self.assertEqual(metrics.max_drawdown, 0.0)
        self.assertEqual(metrics.sharpe_ratio, 0.0)
        self.assertEqual(metrics.winning_trades, 0)
        self.assertEqual(metrics.losing_trades, 0)
        
    def test_track_portfolio_updates(self):
        """Test tracking portfolio updates."""
        # Publish portfolio updates
        self.publish_portfolio_update(100000.0, 100000.0, self.start_time)
        self.publish_portfolio_update(90000.0, 102000.0, self.start_time + 3600000)  # +1 hour
        self.publish_portfolio_update(90000.0, 104000.0, self.start_time + 7200000)  # +2 hours
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check metrics updated correctly
        self.assertEqual(metrics.initial_capital, 100000.0)
        self.assertEqual(metrics.current_capital, 104000.0)
        self.assertAlmostEqual(metrics.total_return, 0.04, places=5)  # 4% return
        self.assertEqual(metrics.peak_capital, 104000.0)
        
        # Check performance events were published
        self.assertTrue(len(self.performance_events) > 0)
        
        # Check last performance event
        last_event = self.performance_events[-1]
        self.assertEqual(last_event.event_type, EventType.PERFORMANCE)
        self.assertAlmostEqual(last_event.metrics.total_return, 0.04, places=5)
        
    def test_calculate_drawdown(self):
        """Test drawdown calculations."""
        # Simulate equity curve with drawdown
        self.publish_portfolio_update(100000.0, 100000.0, self.start_time)
        self.publish_portfolio_update(90000.0, 105000.0, self.start_time + 3600000)  # +1 hour
        self.publish_portfolio_update(90000.0, 102000.0, self.start_time + 7200000)  # +2 hours, drawdown starts
        self.publish_portfolio_update(90000.0, 98000.0, self.start_time + 10800000)  # +3 hours, drawdown continues
        self.publish_portfolio_update(90000.0, 101000.0, self.start_time + 14400000)  # +4 hours, partial recovery
        self.publish_portfolio_update(90000.0, 107000.0, self.start_time + 18000000)  # +5 hours, new peak
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check drawdown metrics
        self.assertEqual(len(metrics.drawdowns), 1)  # One drawdown period
        self.assertAlmostEqual(metrics.max_drawdown, 0.06666, places=4)  # 6.7% max drawdown
        
        # Check drawdown details
        drawdown = metrics.drawdowns[0]
        self.assertEqual(drawdown["start_time"], self.start_time + 7200000)
        self.assertEqual(drawdown["end_time"], self.start_time + 14400000)
        self.assertEqual(drawdown["peak_equity"], 105000.0)
        self.assertEqual(drawdown["trough_equity"], 98000.0)
        self.assertAlmostEqual(drawdown["drawdown_percentage"], 0.06666, places=4)
        
    def test_calculate_daily_returns(self):
        """Test calculation of daily returns."""
        # Set fixed dates for testing
        day1 = datetime(2023, 1, 1, 12, 0, 0)
        day2 = datetime(2023, 1, 2, 12, 0, 0)
        day3 = datetime(2023, 1, 3, 12, 0, 0)
        
        # Publish portfolio updates across multiple days
        self.publish_portfolio_update(100000.0, 100000.0, int(day1.timestamp() * 1000))
        self.publish_portfolio_update(90000.0, 103000.0, int(day1.timestamp() * 1000) + 3600000)  # Same day
        self.publish_portfolio_update(90000.0, 105000.0, int(day2.timestamp() * 1000))  # Next day
        self.publish_portfolio_update(85000.0, 101000.0, int(day3.timestamp() * 1000))  # Day after
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check daily returns
        self.assertEqual(len(metrics.daily_returns), 3)  # Three days of data
        
        # Extract dates as strings for comparison (YYYY-MM-DD)
        date_returns = {k.strftime("%Y-%m-%d"): v for k, v in metrics.daily_returns.items()}
        
        # Check return values for each day
        self.assertAlmostEqual(date_returns["2023-01-01"], 0.03, places=5)  # 3% return on day 1
        self.assertAlmostEqual(date_returns["2023-01-02"], 0.01942, places=5)  # 1.94% return on day 2
        self.assertAlmostEqual(date_returns["2023-01-03"], -0.03810, places=5)  # -3.81% return on day 3
        
    def test_calculate_sharpe_ratio(self):
        """Test calculation of Sharpe ratio."""
        # Set fixed dates for testing
        day1 = datetime(2023, 1, 1, 12, 0, 0)
        day2 = datetime(2023, 1, 2, 12, 0, 0)
        day3 = datetime(2023, 1, 3, 12, 0, 0)
        day4 = datetime(2023, 1, 4, 12, 0, 0)
        day5 = datetime(2023, 1, 5, 12, 0, 0)
        
        # Publish portfolio updates across multiple days with consistent returns
        self.publish_portfolio_update(100000.0, 100000.0, int(day1.timestamp() * 1000))
        self.publish_portfolio_update(90000.0, 101000.0, int(day2.timestamp() * 1000))  # +1%
        self.publish_portfolio_update(90000.0, 102010.0, int(day3.timestamp() * 1000))  # +1%
        self.publish_portfolio_update(90000.0, 103030.1, int(day4.timestamp() * 1000))  # +1%
        self.publish_portfolio_update(90000.0, 104060.4, int(day5.timestamp() * 1000))  # +1%
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check Sharpe ratio
        # For consistent 1% daily returns with very low volatility, Sharpe should be high
        self.assertTrue(metrics.sharpe_ratio > 3.0)
        
    def test_track_trades(self):
        """Test tracking trade statistics."""
        # Simulate completed trades
        # The PerformanceManager tracks trades based on fill events
        
        # Create and register trade statistics manually for testing
        trade_stats = TradeStats()
        trade_stats.add_trade("AAPL", True, 150.0, 160.0, 10, 100.0)  # Winning trade: +$100
        trade_stats.add_trade("MSFT", True, 250.0, 240.0, 5, -50.0)   # Losing trade: -$50
        trade_stats.add_trade("GOOGL", True, 2000.0, 2100.0, 2, 200.0) # Winning trade: +$200
        
        # Set trade stats in performance manager
        self.performance_manager.trade_stats = trade_stats
        
        # Mock the record_trade method for verification in the next test
        self.performance_manager.record_trade = MagicMock()
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check trade statistics
        self.assertEqual(metrics.total_trades, 3)
        self.assertEqual(metrics.winning_trades, 2)
        self.assertEqual(metrics.losing_trades, 1)
        self.assertAlmostEqual(metrics.win_rate, 0.6666, places=4)  # 66.67% win rate
        self.assertAlmostEqual(metrics.average_win, 150.0, places=2)  # $150 average win
        self.assertAlmostEqual(metrics.average_loss, -50.0, places=2)  # $50 average loss
        self.assertAlmostEqual(metrics.profit_factor, 6.0, places=2)  # +$300 / $50 = 6.0
        
    def test_calculate_cagr(self):
        """Test calculation of CAGR (Compound Annual Growth Rate)."""
        # Simulate one year of trading with 10% return
        start_date = datetime(2022, 1, 1, 12, 0, 0)
        end_date = datetime(2023, 1, 1, 12, 0, 0)  # Exactly one year later
        
        self.publish_portfolio_update(100000.0, 100000.0, int(start_date.timestamp() * 1000))
        self.publish_portfolio_update(90000.0, 110000.0, int(end_date.timestamp() * 1000))
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check CAGR
        self.assertAlmostEqual(metrics.cagr, 0.10, places=2)  # 10% CAGR
        
        # Test multi-year CAGR
        # Simulate two years of trading with 21% return (10% per year compounded)
        end_date_2y = datetime(2024, 1, 1, 12, 0, 0)  # Two years from start
        
        self.publish_portfolio_update(90000.0, 121000.0, int(end_date_2y.timestamp() * 1000))
        
        # Get updated metrics
        metrics = self.performance_manager.get_metrics()
        
        # Check CAGR
        self.assertAlmostEqual(metrics.cagr, 0.10, places=2)  # 10% CAGR
        
    def test_performance_report(self):
        """Test generating performance reports."""
        # Add some portfolio data
        self.publish_portfolio_update(100000.0, 100000.0, self.start_time)
        self.publish_portfolio_update(90000.0, 105000.0, self.start_time + 3600000)
        self.publish_portfolio_update(90000.0, 102000.0, self.start_time + 7200000)
        
        # Create trade statistics
        trade_stats = TradeStats()
        trade_stats.add_trade("AAPL", True, 150.0, 160.0, 10, 100.0)
        trade_stats.add_trade("MSFT", True, 250.0, 240.0, 5, -50.0)
        
        # Set trade stats in performance manager
        self.performance_manager.trade_stats = trade_stats
        
        # Generate performance report
        report = self.performance_manager.generate_report()
        
        # Check report contains expected sections
        self.assertTrue("Overall Performance" in report)
        self.assertTrue("Trade Statistics" in report)
        self.assertTrue("Drawdown Analysis" in report)
        
        # Check report contains key metrics
        self.assertTrue("Initial Capital: $100,000.00" in report)
        self.assertTrue("Current Equity: $102,000.00" in report)
        self.assertTrue("Total Return: 2.00%" in report)
        self.assertTrue("Win Rate: 50.00%" in report)
        
    def test_reset_metrics(self):
        """Test resetting performance metrics."""
        # Add some portfolio data
        self.publish_portfolio_update(100000.0, 100000.0, self.start_time)
        self.publish_portfolio_update(90000.0, 105000.0, self.start_time + 3600000)
        
        # Verify metrics were updated
        metrics_before = self.performance_manager.get_metrics()
        self.assertEqual(metrics_before.current_capital, 105000.0)
        
        # Reset metrics
        self.performance_manager.reset(initial_capital=200000.0)
        
        # Verify metrics were reset
        metrics_after = self.performance_manager.get_metrics()
        self.assertEqual(metrics_after.initial_capital, 200000.0)
        self.assertEqual(metrics_after.current_capital, 200000.0)
        self.assertEqual(metrics_after.total_return, 0.0)
        self.assertEqual(metrics_after.daily_returns, {})
        
    def test_custom_risk_free_rate(self):
        """Test setting custom risk-free rate for Sharpe ratio calculation."""
        # Create performance manager with custom risk-free rate
        custom_performance_manager = PerformanceTracker(
            event_manager=self.event_manager,
            initial_capital=100000.0,
            risk_free_rate=0.05  # 5% risk-free rate
        )
        
        # Add some portfolio data
        day1 = datetime(2023, 1, 1, 12, 0, 0)
        day2 = datetime(2023, 1, 2, 12, 0, 0)
        day3 = datetime(2023, 1, 3, 12, 0, 0)
        
        custom_performance_manager.update_metrics(100000.0, 100000.0, int(day1.timestamp() * 1000))
        custom_performance_manager.update_metrics(90000.0, 101000.0, int(day2.timestamp() * 1000))
        custom_performance_manager.update_metrics(90000.0, 102010.0, int(day3.timestamp() * 1000))
        
        # Get metrics
        metrics = custom_performance_manager.get_metrics()
        
        # Verify risk-free rate is applied (Sharpe should be lower with higher risk-free rate)
        self.assertEqual(custom_performance_manager.risk_free_rate, 0.05)
        
        # Create another performance manager with standard risk-free rate for comparison
        standard_performance_manager = PerformanceTracker(
            event_manager=self.event_manager,
            initial_capital=100000.0,
            risk_free_rate=0.0  # 0% risk-free rate
        )
        
        standard_performance_manager.update_metrics(100000.0, 100000.0, int(day1.timestamp() * 1000))
        standard_performance_manager.update_metrics(90000.0, 101000.0, int(day2.timestamp() * 1000))
        standard_performance_manager.update_metrics(90000.0, 102010.0, int(day3.timestamp() * 1000))
        
        standard_metrics = standard_performance_manager.get_metrics()
        
        # Sharpe with higher risk-free rate should be lower
        self.assertTrue(metrics.sharpe_ratio < standard_metrics.sharpe_ratio)

    def test_record_trade_on_position_close(self):
        """Test that a trade is recorded when a PositionEvent indicates closure."""
        # Simulate a PositionEvent for a closed position with closing details
        closing_details = {
            'type': 'CLOSE_LONG',
            'quantity': 10,
            'entry_price': 150.0,
            'exit_price': 160.0,
            'pnl': 100.0, # (160 - 150) * 10
            'timestamp': datetime.now()
        }
        position_close_event = PositionEvent(
            event_type=EventType.POSITION,
            timestamp=int(time.time() * 1000),
            symbol="AAPL",
            quantity=0.0, # Position is closed
            average_price=0.0, # Avg price becomes 0 when closed
            realized_pnl=100.0, # Total realized PNL for the position
            unrealized_pnl=0.0,
            strategy_id="test_strategy",
            closing_trade_details=closing_details
        )

        # Simulate PositionManager returning the closed position (needed by the handler)
        mock_closed_position = MagicMock()
        mock_closed_position.is_closed.return_value = True
        self.mock_position_manager.get_position.return_value = mock_closed_position

        # Simulate the event being processed by the handler
        self.performance_manager._on_position_event(position_close_event)

        # Verify record_trade was called
        self.performance_manager.record_trade.assert_called_once()

        # Verify the details of the recorded trade
        call_args = self.performance_manager.record_trade.call_args[0]
        recorded_trade = call_args[0]
        self.assertIsInstance(recorded_trade, Trade)
        self.assertEqual(recorded_trade.instrument_id, "AAPL")
        self.assertEqual(recorded_trade.quantity, 10) # Should use closing quantity
        self.assertEqual(recorded_trade.price, 160.0) # Should use exit price
        self.assertEqual(recorded_trade.profit, 100.0) # Should use PnL from closing details
        self.assertEqual(recorded_trade.strategy_id, "test_strategy")

if __name__ == "__main__":
    unittest.main() 