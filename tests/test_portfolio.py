import unittest
import logging
import time
from datetime import datetime
from unittest.mock import MagicMock, patch

from core.portfolio import Portfolio
from core.position_manager import PositionManager
from core.event_manager import EventManager
from models.events import FillEvent, EventType, OrderSide, PositionEvent, AccountEvent
from models.position import Position
from models.instrument import Instrument
from utils.constants import Exchange

class PortfolioTest(unittest.TestCase):
    """Test the Portfolio component's functionality."""

    def setUp(self):
        """Set up the test environment."""
        # Configure logging
        self.logger = logging.getLogger("tests.portfolio")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # Avoid adding multiple handlers if tests are run multiple times
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

        # Mock Event Manager
        self.event_manager = MagicMock(spec=EventManager)

        # Mock Position Manager
        self.position_manager = MagicMock(spec=PositionManager)
        self.position_manager.get_all_positions.return_value = {} # Start with no positions

        # Test Instruments
        self.reliance = Instrument(symbol="RELIANCE", exchange=Exchange.NSE)
        self.tcs = Instrument(symbol="TCS", exchange=Exchange.NSE)

        # Basic Config
        self.config = {
            "portfolio": {
                "initial_capital": 100000.0,
            }
            # Add other necessary config sections if Portfolio depends on them
        }

        # Create Portfolio instance
        self.portfolio = Portfolio(
            config=self.config,
            position_manager=self.position_manager,
            event_manager=self.event_manager
        )
        self.portfolio.equity_history = [(int(time.time() * 1000), 100000.0)]  # Initialize with initial value

        self.logger.info("Portfolio test setup complete")

    def test_initial_state(self):
        """Test the initial state of the portfolio."""
        self.assertEqual(self.portfolio.initial_capital, 100000.0)
        self.assertEqual(self.portfolio.cash, 100000.0)
        self.assertEqual(self.portfolio.equity, 100000.0)
        self.assertEqual(self.portfolio.positions_value, 0.0)
        self.assertEqual(self.portfolio.highest_equity, 100000.0)
        self.assertEqual(len(self.portfolio.equity_history), 1) # Initial value is recorded

    def test_cash_update_on_buy_fill(self):
        """Test cash decreases correctly on a BUY fill."""
        initial_cash = self.portfolio.cash
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(time.time() * 1000),
            symbol="RELIANCE",
            quantity=10,
            price=2500.0,
            side=OrderSide.BUY,
            commission=5.0
        )

        # Simulate event manager calling the handler
        self.portfolio._on_fill_event(fill_event)

        expected_cash = initial_cash - (10 * 2500.0) - 5.0
        self.assertAlmostEqual(self.portfolio.cash, expected_cash)
        # Check that an account event was published
        self.event_manager.publish.assert_called()
        # Get the last call arguments
        last_call_args = self.event_manager.publish.call_args[0]
        published_event = last_call_args[0]
        self.assertIsInstance(published_event, AccountEvent)
        self.assertAlmostEqual(published_event.balance, expected_cash)


    def test_cash_update_on_sell_fill(self):
        """Test cash increases correctly on a SELL fill."""
        initial_cash = self.portfolio.cash
        fill_event = FillEvent(
            event_type=EventType.FILL,
            timestamp=int(time.time() * 1000),
            symbol="TCS",
            quantity=5, # Sell 5 shares
            price=3500.0,
            side=OrderSide.SELL,
            commission=2.0
        )

        # Simulate event manager calling the handler
        self.portfolio._on_fill_event(fill_event)

        expected_cash = initial_cash + (5 * 3500.0) - 2.0
        self.assertAlmostEqual(self.portfolio.cash, expected_cash)
        # Check that an account event was published
        self.event_manager.publish.assert_called()
        last_call_args = self.event_manager.publish.call_args[0]
        published_event = last_call_args[0]
        self.assertIsInstance(published_event, AccountEvent)
        self.assertAlmostEqual(published_event.balance, expected_cash)

    def test_recalculate_portfolio_value(self):
        """Test equity and positions_value recalculation."""
        # Simulate having cash reduced by a buy
        self.portfolio.cash = 97495.0 # 100000 - (10*2500) - 5

        # Mock position manager to return an open position
        aapl_position = MagicMock(spec=Position)
        aapl_position.quantity = 10
        aapl_position.last_price = 2550.0 # Price increased
        self.position_manager.get_all_positions.return_value = {"RELIANCE": aapl_position}

        # Trigger recalculation (normally done via _on_position_event or _on_fill_event)
        self.portfolio._recalculate_portfolio_value()

        expected_positions_value = 10 * 2550.0
        expected_equity = 97495.0 + expected_positions_value

        self.assertAlmostEqual(self.portfolio.positions_value, expected_positions_value)
        self.assertAlmostEqual(self.portfolio.equity, expected_equity)
        self.assertGreater(self.portfolio.highest_equity, 100000.0) # Equity increased
        self.assertGreater(len(self.portfolio.equity_history), 1)

    def test_recalculate_portfolio_value_no_positions(self):
        """Test recalculation when there are no open positions."""
        self.portfolio.cash = 99000.0 # Some cash remaining
        self.position_manager.get_all_positions.return_value = {} # Ensure no positions

        self.portfolio._recalculate_portfolio_value()

        self.assertEqual(self.portfolio.positions_value, 0.0)
        self.assertEqual(self.portfolio.equity, 99000.0)

    def test_publish_account_event(self):
         """Test that account events are published correctly."""
         self.portfolio.cash = 98000.0
         self.portfolio.positions_value = 3000.0
         self.portfolio.equity = 101000.0
         self.portfolio.highest_equity = 102000.0 # Simulate a previous peak
         self.portfolio.starting_equity = 100000.0

         # Manually trigger publish
         self.portfolio._publish_account_event()

         # Verify publish was called
         self.event_manager.publish.assert_called_once()
         call_args = self.event_manager.publish.call_args[0]
         published_event = call_args[0]

         self.assertIsInstance(published_event, AccountEvent)
         self.assertEqual(published_event.event_type, EventType.ACCOUNT)
         self.assertAlmostEqual(published_event.balance, 98000.0)
         self.assertAlmostEqual(published_event.equity, 101000.0)
         self.assertAlmostEqual(published_event.positions_value, 3000.0)
         self.assertAlmostEqual(published_event.highest_equity, 102000.0)
         expected_drawdown = (102000.0 - 101000.0) / 102000.0
         self.assertAlmostEqual(published_event.drawdown, expected_drawdown)
         expected_return = (101000.0 - 100000.0) / 100000.0
         self.assertAlmostEqual(published_event.return_pct, expected_return)

    # TODO: Add tests for get_drawdown(), get_leverage(), get_total_exposure() etc.
    # TODO: Add test for _on_position_event handler

if __name__ == '__main__':
    unittest.main() 