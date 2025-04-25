from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
import datetime
import logging
from pathlib import Path
import time

from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument
from models.trade import Trade
from models.position import Position
from models.events import Event, EventType
from models.market_data import OHLCV

from core.position_manager import PositionManager
from core.order_manager import OrderManager
from core.portfolio import Portfolio
from core.performance import PerformanceTracker
from core.risk_manager import RiskManager
from core.event_manager import EventManager
from core.logging_manager import LoggingManager

from strategies.base_strategy import OptionStrategy
from strategies.strategy_registry import StrategyRegistry

from .data_loader import DataLoader
from .data_processor import DataProcessor

from utils.config_loader import ConfigLoader
from utils.metrics import PerformanceMetrics
from utils.helpers import create_output_directory


class BacktestingEngine:
    """
    The main backtesting engine that runs historical data through strategies
    and simulates trading.
    """

    def __init__(self, config_path: str):
        """
        Initialize the backtesting engine with configuration.

        Args:
            config_path: Path to the configuration file
        """
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load_config()

        # Setup logging
        self.logging_manager = LoggingManager(
            log_level=self.config.get('log_level', 'INFO'),
            log_file=self.config.get('log_file', 'backtest.log')
        )
        self.logger = self.logging_manager.get_logger('backtest')

        # Initialize components
        self.data_loader = DataLoader(self.config.get('data', {}))
        self.data_processor = DataProcessor()

        # Core components
        self.event_manager = EventManager()
        self.portfolio = Portfolio(initial_capital=self.config.get('initial_capital', 100000))
        self.position_manager = PositionManager(self.portfolio)
        self.order_manager = OrderManager(self.portfolio, self.position_manager)
        self.risk_manager = RiskManager(
            self.portfolio,
            self.position_manager,
            max_position_size=self.config.get('risk', {}).get('max_position_size', 0.1),
            max_drawdown=self.config.get('risk', {}).get('max_drawdown', 0.05)
        )
        self.performance_tracker = PerformanceTracker()

        # Load strategies
        self.strategy_factory = StrategyRegistry()
        self.strategies = self._load_strategies()

        # Data storage
        self.historical_data = {}
        self.current_date = None
        self.equity_curve = []
        self.trades_history = []
        self.orders_history = []
        self.performance_metrics = {}

        # Output directory for results
        self.output_dir = create_output_directory(
            self.config.get('output_directory', 'backtest_results')
        )

        self.logger.info(f"Backtesting engine initialized with config from {config_path}")

    def _load_strategies(self) -> List[OptionStrategy]:
        """Load strategies specified in the configuration"""
        strategies = []
        strategy_configs = self.config.get('strategies', [])

        for strategy_config in strategy_configs:
            strategy_name = strategy_config.get('name')
            strategy_params = strategy_config.get('params', {})

            strategy = self.strategy_factory.create_strategy(
                strategy_name,
                strategy_params,
                self.order_manager,
                self.position_manager,
                self.portfolio,
                self.event_manager
            )

            strategies.append(strategy)
            self.logger.info(f"Loaded strategy: {strategy_name}")

        return strategies

    def load_data(self) -> None:
        """Load and preprocess historical data for backtesting"""
        instruments = self.config.get('instruments', [])
        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date')

        self.logger.info(f"Loading data for {len(instruments)} instruments from {start_date} to {end_date}")

        for instrument_config in instruments:
            symbol = instrument_config.get('symbol')
            instrument_type = instrument_config.get('type', 'stock')
            exchange = instrument_config.get('exchange', 'NSE')

            instrument = Instrument(symbol=symbol, instrument_type=instrument_type, exchange=exchange)

            # Load historical data
            data = self.data_loader.load_data(
                instrument=instrument,
                start_date=start_date,
                end_date=end_date
            )

            # Preprocess data
            data = self.data_processor.process_data(data)

            self.historical_data[symbol] = data
            self.logger.info(f"Loaded {len(data)} data points for {symbol}")

        # Align all data to the same date range
        self._align_data()

    def _align_data(self) -> None:
        """Ensure all data series have the same date range"""
        # Find common dates across all instruments
        all_dates = set()
        for symbol, data in self.historical_data.items():
            dates = set(data.index)
            if not all_dates:
                all_dates = dates
            else:
                all_dates = all_dates.intersection(dates)

        # Filter data to include only common dates
        for symbol in self.historical_data:
            self.historical_data[symbol] = self.historical_data[symbol].loc[sorted(all_dates)]

        self.logger.info(f"Aligned data to {len(all_dates)} common trading days")

    def run(self, verbose: bool = True) -> Dict[str, Any]:
        """
        Run the backtest simulation.

        Args:
            verbose: Whether to print progress information

        Returns:
            Dictionary containing backtest results
        """
        if not self.historical_data:
            self.logger.error("No historical data loaded. Call load_data() first.")
            raise ValueError("No historical data loaded")

        # Get sorted dates from the first instrument (all are aligned)
        symbol = next(iter(self.historical_data))
        dates = self.historical_data[symbol].index

        total_days = len(dates)
        start_time = time.time()

        self.logger.info(f"Starting backtest over {total_days} trading days")

        # Initialize equity curve with starting capital
        self.equity_curve = [{
            'date': dates[0],
            'equity': self.portfolio.initial_capital,
            'cash': self.portfolio.initial_capital,
            'holdings': 0
        }]

        # Simulate trading day by day
        for i, date in enumerate(dates):
            self.current_date = date

            if verbose and i % 20 == 0:
                progress = (i / total_days) * 100
                self.logger.info(f"Progress: {progress:.2f}% - Processing {date}")

            # Get current market data for all instruments
            current_market_data = {}
            for symbol, data in self.historical_data.items():
                if date in data.index:
                    bar = data.loc[date]
                    current_market_data[symbol] = OHLCV(
                        symbol=symbol,
                        timestamp=date,
                        open=bar['open'],
                        high=bar['high'],
                        low=bar['low'],
                        close=bar['close'],
                        volume=bar['volume'] if 'volume' in bar else 0
                    )

            # Create market data event
            market_data_event = Event(
                event_type=EventType.MARKET_DATA,
                timestamp=date,
                data=current_market_data
            )

            # Process the event
            self._process_market_data(market_data_event)

            # Process limit and stop orders that may be triggered by new prices
            self._process_pending_orders(current_market_data)

            # Update portfolio value and equity curve
            self._update_portfolio_value(date, current_market_data)

            # Check risk management rules
            self._check_risk_management()

        # Calculate final performance metrics
        self.performance_metrics = self._calculate_performance()

        # Save results
        self._save_results()

        elapsed_time = time.time() - start_time
        self.logger.info(f"Backtest completed in {elapsed_time:.2f} seconds")

        return {
            'equity_curve': pd.DataFrame(self.equity_curve),
            'trades': self.trades_history,
            'orders': self.orders_history,
            'metrics': self.performance_metrics
        }

    def _process_market_data(self, event: Event) -> None:
        """Process market data event by updating strategies"""
        market_data = event.data

        # First update all strategies with new market data
        for strategy in self.strategies:
            strategy.on_market_data(event)

        # Process any events generated by strategies
        while not self.event_manager.is_empty():
            event = self.event_manager.get_next_event()

            if event.event_type == EventType.ORDER:
                self._process_order_event(event)
            elif event.event_type == EventType.SIGNAL:
                self._process_signal_event(event)

    def _process_order_event(self, event: Event) -> None:
        """Process order events generated by strategies"""
        order = event.data

        # Validate order through risk manager
        if not self.risk_manager.validate_order(order):
            self.logger.warning(f"Order rejected by risk manager: {order}")
            order.status = OrderStatus.REJECTED
            self.orders_history.append(order)
            return

        # For market orders in backtest, execute immediately
        if order.order_type == OrderType.MARKET:
            self._execute_market_order(order)
        else:
            # Add limit/stop orders to pending orders
            self.order_manager.add_order(order)
            self.orders_history.append(order)

    def _process_signal_event(self, event: Event) -> None:
        """Process signal events and convert them to orders"""
        signal = event.data

        # Let the strategy factory convert signals to orders
        for strategy in self.strategies:
            if strategy.name == signal.strategy_name:
                strategy.on_signal(signal)

    def _execute_market_order(self, order: Order) -> None:
        """Execute a market order at current price"""
        # Get current market data for the symbol
        market_data = self.historical_data.get(order.symbol)
        if market_data is None or self.current_date not in market_data.index:
            self.logger.warning(f"No market data for {order.symbol} on {self.current_date}")
            order.status = OrderStatus.REJECTED
            return

        # Get execution price (using close price in backtest)
        price = market_data.loc[self.current_date]['close']

        # Calculate transaction costs
        commission = self.config.get('commission_model', {}).get('fixed', 0.0)
        commission += price * order.quantity * self.config.get('commission_model', {}).get('percentage', 0.0)

        # Create trade
        trade = Trade(
            order_id=order.order_id,
            symbol=order.symbol,
            timestamp=self.current_date,
            quantity=order.quantity,
            price=price,
            commission=commission,
            side=order.side
        )

        # Update position
        self.position_manager.update_position(trade)

        # Update portfolio
        self.portfolio.update_from_trade(trade)

        # Update order status
        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_quantity = order.quantity
        order.filled_timestamp = self.current_date

        # Add to history
        self.trades_history.append(trade)
        self.orders_history.append(order)

        self.logger.info(f"Executed market order: {order.order_id} at {price} for {order.quantity} shares")

    def _process_pending_orders(self, current_market_data: Dict[str, OHLCV]) -> None:
        """Process pending limit and stop orders based on current prices"""
        pending_orders = self.order_manager.get_pending_orders()

        for order in pending_orders:
            if order.symbol not in current_market_data:
                continue

            market_data = current_market_data[order.symbol]

            # Check if limit or stop order should be triggered
            if self._check_order_trigger(order, market_data):
                # Execute the order
                self._execute_triggered_order(order, market_data)

    def _check_order_trigger(self, order: Order, market_data: OHLCV) -> bool:
        """Check if a limit or stop order should be triggered"""
        if order.order_type == OrderType.LIMIT_BUY:
            return market_data.low <= order.price
        elif order.order_type == OrderType.LIMIT_SELL:
            return market_data.high >= order.price
        elif order.order_type == OrderType.STOP_BUY:
            return market_data.high >= order.price
        elif order.order_type == OrderType.STOP_SELL:
            return market_data.low <= order.price
        return False

    def _execute_triggered_order(self, order: Order, market_data: OHLCV) -> None:
        """Execute a triggered limit or stop order"""
        # In a real system, limit orders might get different prices
        # For simplicity in backtesting, we use the specified price
        price = order.price

        # Calculate transaction costs
        commission = self.config.get('commission_model', {}).get('fixed', 0.0)
        commission += price * order.quantity * self.config.get('commission_model', {}).get('percentage', 0.0)

        # Create trade
        trade = Trade(
            order_id=order.order_id,
            symbol=order.symbol,
            timestamp=self.current_date,
            quantity=order.quantity,
            price=price,
            commission=commission,
            side=order.side
        )

        # Update position
        self.position_manager.update_position(trade)

        # Update portfolio
        self.portfolio.update_from_trade(trade)

        # Update order status
        order.status = OrderStatus.FILLED
        order.filled_price = price
        order.filled_quantity = order.quantity
        order.filled_timestamp = self.current_date

        # Remove from pending orders
        self.order_manager.remove_order(order.order_id)

        # Add to history
        self.trades_history.append(trade)
        self.orders_history.append(order)

        self.logger.info(f"Executed triggered order: {order.order_id} at {price} for {order.quantity} shares")

    def _update_portfolio_value(self, date: datetime.datetime, market_data: Dict[str, OHLCV]) -> None:
        """Update portfolio value based on current market prices"""
        # Calculate value of positions
        positions_value = 0
        for position in self.position_manager.get_positions().values():
            if position.symbol in market_data:
                current_price = market_data[position.symbol].close
                position_value = position.quantity * current_price
                positions_value += position_value

        # Record equity curve
        equity = self.portfolio.cash + positions_value
        self.equity_curve.append({
            'date': date,
            'equity': equity,
            'cash': self.portfolio.cash,
            'holdings': positions_value
        })

    def _check_risk_management(self) -> None:
        """Check risk management rules and take actions if needed"""
        # Maximum drawdown check
        if len(self.equity_curve) >= 2:
            current_equity = self.equity_curve[-1]['equity']
            peak_equity = max(point['equity'] for point in self.equity_curve)
            drawdown = (peak_equity - current_equity) / peak_equity

            max_drawdown = self.config.get('risk', {}).get('max_drawdown', 0.20)

            if drawdown > max_drawdown:
                self.logger.warning(f"Maximum drawdown exceeded: {drawdown:.2%}")
                # Liquidate positions if configured to do so
                if self.config.get('risk', {}).get('auto_liquidate_on_max_drawdown', False):
                    self._liquidate_all_positions()

    def _liquidate_all_positions(self) -> None:
        """Liquidate all positions at current market prices"""
        for position in self.position_manager.get_positions().values():
            if position.quantity != 0:
                # Create liquidation order
                order = Order(
                    instrument_id=position.symbol,
                    quantity=abs(position.quantity),  # Use absolute value
                    side='SELL' if position.quantity > 0 else 'BUY',  # Standardize side to uppercase
                    order_type=OrderType.MARKET
                )

                # Execute immediately
                self._execute_market_order(order)

    def _calculate_performance(self) -> Dict[str, Any]:
        """Calculate and return performance metrics"""
        if not self.equity_curve:
            return {}

        # Convert to DataFrame for easier calculations
        equity_df = pd.DataFrame(self.equity_curve)
        equity_df.set_index('date', inplace=True)

        # Calculate returns
        equity_df['returns'] = equity_df['equity'].pct_change()

        # Calculate performance metrics
        metrics = PerformanceMetrics.calculate_metrics(
            equity_series=equity_df['equity'],
            returns_series=equity_df['returns'].fillna(0)
        )

        # Add trade statistics
        if self.trades_history:
            trades_df = pd.DataFrame([
                {
                    'symbol': t.symbol,
                    'side': t.side,
                    'quantity': t.quantity,
                    'price': t.price,
                    'timestamp': t.timestamp,
                    'commission': t.commission,
                    'pnl': t.realized_pnl if hasattr(t, 'realized_pnl') else 0
                }
                for t in self.trades_history
            ])

            metrics.update({
                'total_trades': len(self.trades_history),
                'winning_trades': len(trades_df[trades_df['pnl'] > 0]),
                'losing_trades': len(trades_df[trades_df['pnl'] < 0]),
                'total_commission': trades_df['commission'].sum(),
            })

            if metrics['total_trades'] > 0:
                metrics['win_rate'] = metrics['winning_trades'] / metrics['total_trades']

        return metrics

    def _save_results(self) -> None:
        """Save backtest results to output directory"""
        # Create directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Save equity curve
        pd.DataFrame(self.equity_curve).to_csv(
            f"{self.output_dir}/equity_curve.csv", index=False
        )

        # Save trades
        if self.trades_history:
            trades_df = pd.DataFrame([
                {
                    'order_id': t.order_id,
                    'symbol': t.symbol,
                    'side': t.side,
                    'quantity': t.quantity,
                    'price': t.price,
                    'timestamp': t.timestamp,
                    'commission': t.commission,
                    'pnl': t.realized_pnl if hasattr(t, 'realized_pnl') else 0
                }
                for t in self.trades_history
            ])
            trades_df.to_csv(f"{self.output_dir}/trades.csv", index=False)

        # Save orders
        if self.orders_history:
            orders_df = pd.DataFrame([
                {
                    'order_id': o.order_id,
                    'symbol': o.symbol,
                    'side': o.side,
                    'quantity': o.quantity,
                    'order_type': o.order_type.name,
                    'price': o.price,
                    'status': o.status.name,
                    'timestamp': o.timestamp,
                    'filled_price': o.filled_price,
                    'filled_quantity': o.filled_quantity,
                    'filled_timestamp': o.filled_timestamp
                }
                for o in self.orders_history
            ])
            orders_df.to_csv(f"{self.output_dir}/orders.csv", index=False)

        # Save performance metrics
        with open(f"{self.output_dir}/performance_metrics.txt", 'w') as f:
            for key, value in self.performance_metrics.items():
                f.write(f"{key}: {value}\n")

        self.logger.info(f"Results saved to {self.output_dir}")

    def plot_results(self) -> None:
        """Plot backtest results - requires matplotlib to be installed"""
        try:
            import matplotlib.pyplot as plt

            # Convert equity curve to DataFrame
            equity_df = pd.DataFrame(self.equity_curve)
            equity_df.set_index('date', inplace=True)

            # Create figure and axes
            fig, axes = plt.subplots(3, 1, figsize=(12, 18), sharex=True)

            # Plot equity curve
            axes[0].plot(equity_df.index, equity_df['equity'], label='Portfolio Value')
            axes[0].set_title('Equity Curve')
            axes[0].set_ylabel('Value')
            axes[0].legend()
            axes[0].grid(True)

            # Plot cash and holdings
            axes[1].plot(equity_df.index, equity_df['cash'], label='Cash')
            axes[1].plot(equity_df.index, equity_df['holdings'], label='Holdings')
            axes[1].set_title('Cash vs Holdings')
            axes[1].set_ylabel('Value')
            axes[1].legend()
            axes[1].grid(True)

            # Calculate drawdown
            equity_df['peak'] = equity_df['equity'].cummax()
            equity_df['drawdown'] = (equity_df['peak'] - equity_df['equity']) / equity_df['peak']

            # Plot drawdown
            axes[2].fill_between(equity_df.index, 0, equity_df['drawdown'], color='red', alpha=0.3)
            axes[2].set_title('Drawdown')
            axes[2].set_ylabel('Drawdown %')
            axes[2].set_xlabel('Date')
            axes[2].grid(True)

            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(f"{self.output_dir}/equity_curve.png")

            # Plot trade analysis if we have trades
            if self.trades_history:
                trades_df = pd.DataFrame([
                    {
                        'symbol': t.symbol,
                        'side': t.side,
                        'quantity': t.quantity,
                        'price': t.price,
                        'timestamp': t.timestamp,
                        'pnl': t.realized_pnl if hasattr(t, 'realized_pnl') else 0
                    }
                    for t in self.trades_history
                ])

                # Group trades by symbol
                symbols = trades_df['symbol'].unique()

                # Plot PnL by symbol
                fig, ax = plt.subplots(figsize=(12, 8))

                for symbol in symbols:
                    symbol_trades = trades_df[trades_df['symbol'] == symbol]
                    symbol_trades = symbol_trades.sort_values('timestamp')

                    cumulative_pnl = symbol_trades['pnl'].cumsum()

                    ax.plot(symbol_trades['timestamp'], cumulative_pnl, label=symbol)

                ax.set_title('Cumulative PnL by Symbol')
                ax.set_xlabel('Date')
                ax.set_ylabel('Cumulative PnL')
                ax.legend()
                ax.grid(True)

                plt.tight_layout()
                plt.savefig(f"{self.output_dir}/pnl_by_symbol.png")

            plt.close('all')
            self.logger.info("Plots saved to output directory")

        except ImportError:
            self.logger.warning("Matplotlib not installed. Skipping plot generation.")
