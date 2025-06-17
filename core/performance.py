import numpy as np
import pandas as pd
import logging
import os
import json

from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

from models.trade import Trade
from models.position import Position
from utils.metrics import PerformanceMetrics
from models.events import Event, EventType, AccountEvent, FillEvent, PositionEvent
from core.event_manager import EventManager
from core.position_manager import PositionManager
from utils.constants import OrderSide
from core.portfolio import Portfolio 
from core.logging_manager import get_logger

class PerformanceTracker:
    """Tracks and calculates performance metrics for strategies and portfolio"""

    def __init__(self, portfolio: Portfolio, config, event_manager: EventManager = None, position_manager: PositionManager = None):
        """
        Initialize the performance tracker

        Args:
            portfolio: Portfolio instance to track
            config: Configuration dictionary
            event_manager: EventManager instance
            position_manager: PositionManager instance
        """
        self.logger = get_logger(__name__)
        self.portfolio = portfolio
        self.config = config
        self.event_manager = event_manager
        self.position_manager = position_manager

        self.portfolio_values = []  # List of (timestamp, value) tuples
        self.benchmark_values = []  # List of (timestamp, value) tuples for comparison benchmark
        self.trade_history = []  # List of all trades
        self.daily_returns = {}  # Dictionary of date: return pairs
        self.strategy_returns = {}  # Dictionary of strategy_id: {date: return} pairs

        # Get benchmark from config
        self.benchmark_symbol = self.config.get("performance", {}).get("benchmark", "NIFTY 50")

        # Define metrics to track based on config
        self.metrics_to_track = self.config.get("performance", {}).get("metrics", [
            "sharpe_ratio", "sortino_ratio", "max_drawdown", "win_rate",
            "profit_factor", "expectancy", "cagr"
        ])

        # Reporting settings
        self.reporting_config = self.config.get("performance", {}).get("reporting", {})
        self.reporting_frequency = self.reporting_config.get("frequency", "end_of_day")

        # Initialize with current portfolio value
        try:
            initial_value = self.portfolio.calculate_portfolio_value()
            self.record_portfolio_value(datetime.now(), initial_value)
        except Exception as e:
            self.logger.error(f"Failed to get initial portfolio value: {e}")
            initial_value = self.portfolio.initial_capital
            self.record_portfolio_value(datetime.now(), initial_value)

        if self.event_manager:
            self._register_event_handlers()

        self.logger.info(f"Performance tracker initialized with benchmark: {self.benchmark_symbol}")

    def _register_event_handlers(self):
        """Register for relevant events."""
        self.event_manager.subscribe(
            EventType.ACCOUNT,
            self._on_account_event,
            component_name="PerformanceTracker"
        )
        self.event_manager.subscribe(
            EventType.POSITION,
            self._on_position_event,
            component_name="PerformanceTracker"
        )
        self.logger.info("PerformanceTracker registered with Event Manager")

    def _on_account_event(self, event: Event):
        """Handle Account events to track portfolio value changes."""
        if not isinstance(event, AccountEvent):
            self.logger.warning(f"Invalid account event type received: {type(event)}")
            return
        
        if hasattr(event, 'equity') and hasattr(event, 'timestamp'):
            ts = datetime.fromtimestamp(event.timestamp / 1000)
            self.record_portfolio_value(ts, event.equity)
            self.update()
        else:
            self.logger.warning("Received AccountEvent missing equity or timestamp")

    def _on_position_event(self, event: Event):
        """Handle Position events to record completed trades."""
        if not isinstance(event, PositionEvent):
            self.logger.warning(f"Invalid position event type received: {type(event)}")
            return
        
        if not self.position_manager:
            self.logger.warning("PositionManager not available, cannot record trades from positions.")
            return

        if not hasattr(event, 'symbol'):
            self.logger.warning("PositionEvent missing symbol for trade recording.")
            return

        position = self.position_manager.get_position(event.symbol)

        if position and position.is_closed():
            latest_realized_pnl = 0

            closing_details = getattr(event, 'closing_trade_details', None)
            if closing_details:
                latest_realized_pnl = closing_details.get('pnl', 0.0)
            else:
                self.logger.warning(f"PositionEvent for closed position {event.symbol} missing closing_trade_details.")

            trade_quantity = closing_details.get('quantity', 0) if closing_details else 0
            trade_price = closing_details.get('exit_price', event.avg_price) if closing_details else event.avg_price
            trade_side = OrderSide.BUY.value if closing_details and closing_details.get('type') == 'CLOSE_SHORT' else OrderSide.SELL.value if closing_details else 'UNKNOWN'
            trade_timestamp = closing_details.get('timestamp', datetime.fromtimestamp(event.timestamp / 1000)) if closing_details else datetime.fromtimestamp(event.timestamp / 1000)

            trade = Trade(
                order_id=getattr(event, 'order_id', f'pos_close_{event.symbol}_{event.timestamp}'),
                instrument_id=event.symbol,
                quantity=trade_quantity,
                price=trade_price,
                side=trade_side,
                timestamp=trade_timestamp,
                commission=0.0,
                strategy_id=getattr(event, 'strategy_id', None),
                profit=latest_realized_pnl
            )
            self.record_trade(trade)
            self.logger.info(f"Recorded closed position {event.symbol} as trade with PnL: {latest_realized_pnl:.2f}")

    def update(self):
        """Update performance metrics based on current portfolio state"""
        current_time = datetime.now()
        current_value = self.portfolio.calculate_portfolio_value()

        record_value = True
        if self.portfolio_values:
            last_ts, last_val = self.portfolio_values[-1]
            if (current_time - last_ts).total_seconds() < 60 and abs(current_value - last_val) / last_val < 0.001:
                 record_value = False
        
        if record_value:
            self.record_portfolio_value(current_time, current_value)

        current_date = current_time.date()

        if self.portfolio_values and current_date not in self.daily_returns:
            previous_values = [(t, v) for t, v in self.portfolio_values[:-1]
                               if t.date() < current_date]

            if previous_values:
                last_timestamp, last_value = previous_values[-1]
                if last_value != 0:
                     daily_return = (current_value / last_value) - 1
                     self.record_daily_return(current_date, daily_return)
                else:
                     self.logger.warning("Cannot calculate daily return: previous day's value was zero.")

        self._check_for_scheduled_reporting(current_time)

    def _check_for_scheduled_reporting(self, current_time):
        """Check if it's time to generate a scheduled report"""
        if self.reporting_frequency == "real_time":
            self._generate_report()
        elif self.reporting_frequency == "hourly" and current_time.minute == 0:
            self._generate_report()
        elif self.reporting_frequency == "end_of_day":
            market_close = self.config.get("market", {}).get("market_hours", {}).get("end", "15:30:00")
            if market_close:
                try:
                    close_hour, close_minute, _ = map(int, market_close.split(":"))
                    if (current_time.hour == close_hour and
                        current_time.minute >= close_minute and
                        current_time.minute < close_minute + 5):
                        self._generate_report()
                except ValueError:
                    pass

    def _generate_report(self):
        """Generate performance report based on configuration"""
        save_format = self.reporting_config.get("format", "csv")

        if self.reporting_config.get("save_equity_curve", False):
            self._save_equity_curve(save_format)

        if self.reporting_config.get("save_trades", False):
            self._save_trades(save_format)

        if self.reporting_config.get("save_positions", False):
            self._save_positions(save_format)

    def _save_equity_curve(self, format_type):
        """Save equity curve to file"""
        output_dir = self.config.get("system", {}).get("output_dir", "output")
        filename = f"{output_dir}/equity_curve_{datetime.now().strftime('%Y%m%d')}"

        df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])

        if format_type == "csv":
            df.to_csv(f"{filename}.csv", index=False)
        elif format_type == "json":
            df.to_json(f"{filename}.json", orient='records')

        self.logger.info(f"Saved equity curve to {filename}.{format_type}")

    def _save_trades(self, format_type):
        """Save trade history to file"""
        if not self.trade_history:
            return

        output_dir = self.config.get("system", {}).get("output_dir", "output")
        filename = f"{output_dir}/trades_{datetime.now().strftime('%Y%m%d')}"

        trade_dicts = [t.__dict__ for t in self.trade_history]
        df = pd.DataFrame(trade_dicts)

        if format_type == "csv":
            df.to_csv(f"{filename}.csv", index=False)
        elif format_type == "json":
            df.to_json(f"{filename}.json", orient='records')

        self.logger.info(f"Saved trade history to {filename}.{format_type}")

    def _save_positions(self, format_type):
        """Save current positions to file"""
        output_dir = self.config.get("system", {}).get("output_dir", "output")
        filename = f"{output_dir}/positions_{datetime.now().strftime('%Y%m%d')}"

        positions = self.portfolio.get_open_positions()
        if not positions:
            return

        position_dicts = [p.__dict__ for p in positions]
        df = pd.DataFrame(position_dicts)

        if format_type == "csv":
            df.to_csv(f"{filename}.csv", index=False)
        elif format_type == "json":
            df.to_json(f"{filename}.json", orient='records')

        self.logger.info(f"Saved positions to {filename}.{format_type}")

    def record_portfolio_value(self, timestamp: datetime, value: float) -> None:
        """Record a portfolio value snapshot"""
        self.portfolio_values.append((timestamp, value))

    def record_benchmark_value(self, timestamp: datetime, value: float) -> None:
        """Record a benchmark value snapshot"""
        self.benchmark_values.append((timestamp, value))

    def record_trade(self, trade: Trade) -> None:
        """Record a trade for performance analysis"""
        self.trade_history.append(trade)

    def record_daily_return(self, date: datetime.date, return_value: float, strategy_id: Optional[str] = None) -> None:
        """Record daily return for the portfolio or a specific strategy"""
        self.daily_returns[date] = return_value

        if strategy_id:
            if strategy_id not in self.strategy_returns:
                self.strategy_returns[strategy_id] = {}
            self.strategy_returns[strategy_id][date] = return_value

    def calculate_returns(self, lookback_days: Optional[int] = None) -> Dict:
        """Calculate various return metrics"""
        if not self.portfolio_values:
            return {"error": "No portfolio values recorded"}

        df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])
        df['date'] = df['timestamp'].dt.date

        if lookback_days:
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).date()
            df = df[df['date'] >= cutoff_date]

        if len(df) < 2:
            return {"error": "Insufficient data points for return calculation"}

        df = df.sort_values('timestamp')
        df_daily = df.groupby('date')['value'].last().reset_index()
        df_daily['return'] = df_daily['value'].pct_change()

        total_return = (df_daily['value'].iloc[-1] / df_daily['value'].iloc[0]) - 1
        daily_returns = df_daily['return'].dropna().values

        if len(daily_returns) <= 1:
            return

        annualized_return = (1 + total_return) ** (252 / len(df_daily)) - 1
        volatility = np.std(daily_returns) * np.sqrt(252)

        risk_free_rate = 0.02
        daily_rf = (1 + risk_free_rate) ** (1/252) - 1

        sharpe = PerformanceMetrics.calculate_sharpe_ratio(daily_returns, daily_rf)
        sortino = PerformanceMetrics.calculate_sortino_ratio(daily_returns, daily_rf)

        max_dd = PerformanceMetrics.calculate_max_drawdown(df_daily['value'].values)

        return {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "max_drawdown": max_dd,
            "start_date": df_daily['date'].iloc[0],
            "end_date": df_daily['date'].iloc[-1],
            "days": len(df_daily)
        }

    def calculate_strategy_performance(self, strategy_id: str, lookback_days: Optional[int] = None) -> Dict:
        """Calculate performance metrics for a specific strategy"""
        if strategy_id not in self.strategy_returns:
            return {"error": f"No data recorded for strategy {strategy_id}"}

        strategy_returns = self.strategy_returns[strategy_id]

        df = pd.DataFrame([(date, ret) for date, ret in strategy_returns.items()],
                          columns=['date', 'return'])

        if lookback_days:
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).date()
            df = df[df['date'] >= cutoff_date]

        if len(df) < 2:
            return {"error": "Insufficient data points for performance calculation"}

        total_return = (1 + df['return']).prod() - 1
        daily_returns = df['return'].values

        annualized_return = (1 + total_return) ** (252 / len(df)) - 1
        volatility = np.std(daily_returns) * np.sqrt(252)

        risk_free_rate = 0.02
        daily_rf = (1 + risk_free_rate) ** (1/252) - 1

        sharpe = PerformanceMetrics.calculate_sharpe_ratio(daily_returns, daily_rf)
        sortino = PerformanceMetrics.calculate_sortino_ratio(daily_returns, daily_rf)

        cumulative_returns = (1 + df['return']).cumprod().values
        max_dd = PerformanceMetrics.calculate_max_drawdown(cumulative_returns)

        return {
            "strategy_id": strategy_id,
            "total_return": total_return,
            "annualized_return": annualized_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "max_drawdown": max_dd,
            "win_rate": self.calculate_win_rate(strategy_id),
            "profit_factor": self.calculate_profit_factor(strategy_id),
            "avg_profit_per_trade": self.calculate_avg_profit_per_trade(strategy_id),
            "start_date": df['date'].iloc[0],
            "end_date": df['date'].iloc[-1],
            "days": len(df)
        }

    def calculate_win_rate(self, strategy_id: str) -> float:
        """Calculate win rate for a strategy"""
        strategy_trades = [t for t in self.trade_history if t.strategy_id == strategy_id]

        if not strategy_trades:
            return 0.0

        winning_trades = [t for t in strategy_trades if t.profit > 0]
        return len(winning_trades) / len(strategy_trades)

    def calculate_profit_factor(self, strategy_id: str) -> float:
        """Calculate profit factor (gross profits / gross losses)"""
        strategy_trades = [t for t in self.trade_history if t.strategy_id == strategy_id]

        if not strategy_trades:
            return 0.0

        gross_profit = sum([t.profit for t in strategy_trades if t.profit > 0])
        gross_loss = abs(sum([t.profit for t in strategy_trades if t.profit < 0]))

        if gross_loss == 0:
            return float('inf')

        return gross_profit / gross_loss

    def calculate_avg_profit_per_trade(self, strategy_id: str) -> float:
        """Calculate average profit per trade"""
        strategy_trades = [t for t in self.trade_history if t.strategy_id == strategy_id]

        if not strategy_trades:
            return 0.0

        total_profit = sum([t.profit for t in strategy_trades])
        return total_profit / len(strategy_trades)

    def calculate_drawdown_series(self) -> List[Tuple[datetime, float]]:
        """Calculate a time series of drawdowns"""
        if not self.portfolio_values:
            return []

        df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])

        df = df.sort_values('timestamp')
        df['peak'] = df['value'].cummax()
        df['drawdown'] = (df['value'] - df['peak']) / df['peak']

        return list(zip(df['timestamp'], df['drawdown']))

    def compare_to_benchmark(self, lookback_days: Optional[int] = None) -> Dict:
        """Compare strategy performance to a benchmark"""
        if not self.portfolio_values or not self.benchmark_values:
            return {"error": "Missing portfolio or benchmark values"}

        portfolio_df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])
        portfolio_df['date'] = portfolio_df['timestamp'].dt.date

        benchmark_df = pd.DataFrame(self.benchmark_values, columns=['timestamp', 'value'])
        benchmark_df['date'] = benchmark_df['timestamp'].dt.date

        if lookback_days:
            cutoff_date = (datetime.now() - timedelta(days=lookback_days)).date()
            portfolio_df = portfolio_df[portfolio_df['date'] >= cutoff_date]
            benchmark_df = benchmark_df[benchmark_df['date'] >= cutoff_date]

        portfolio_daily = portfolio_df.groupby('date')['value'].last().reset_index()
        benchmark_daily = benchmark_df.groupby('date')['value'].last().reset_index()

        merged_df = pd.merge(portfolio_daily, benchmark_daily, on='date',
                             suffixes=('_portfolio', '_benchmark'))

        merged_df['return_portfolio'] = merged_df['value_portfolio'].pct_change()
        merged_df['return_benchmark'] = merged_df['value_benchmark'].pct_change()

        merged_df = merged_df.dropna()

        if len(merged_df) < 2:
            return {"error": "Insufficient data points for comparison"}

        portfolio_total_return = (merged_df['value_portfolio'].iloc[-1] /
                                merged_df['value_portfolio'].iloc[0]) - 1
        benchmark_total_return = (merged_df['value_benchmark'].iloc[-1] /
                                merged_df['value_benchmark'].iloc[0]) - 1

        covariance = np.cov(merged_df['return_portfolio'], merged_df['return_benchmark'])[0][1]
        benchmark_variance = np.var(merged_df['return_benchmark'])
        beta = covariance / benchmark_variance if benchmark_variance != 0 else 1.0

        risk_free_rate = 0.02
        daily_rf = (1 + risk_free_rate) ** (1/252) - 1

        portfolio_mean_return = merged_df['return_portfolio'].mean()
        benchmark_mean_return = merged_df['return_benchmark'].mean()

        alpha_daily = portfolio_mean_return - (daily_rf + beta * (benchmark_mean_return - daily_rf))
        alpha_annualized = alpha_daily * 252

        tracking_diff = merged_df['return_portfolio'] - merged_df['return_benchmark']
        tracking_error = np.std(tracking_diff) * np.sqrt(252)

        information_ratio = (portfolio_mean_return - benchmark_mean_return) * 252 / tracking_error if tracking_error != 0 else 0

        return {
            "portfolio_return": portfolio_total_return,
            "benchmark_return": benchmark_total_return,
            "excess_return": portfolio_total_return - benchmark_total_return,
            "beta": beta,
            "alpha_annualized": alpha_annualized,
            "tracking_error": tracking_error,
            "information_ratio": information_ratio,
            "correlation": np.corrcoef(merged_df['return_portfolio'], merged_df['return_benchmark'])[0][1],
            "start_date": merged_df['date'].iloc[0],
            "end_date": merged_df['date'].iloc[-1],
            "days": len(merged_df)
        }

    def get_performance_summary(self) -> Dict:
        """Get a comprehensive performance summary"""
        summary = {
            "portfolio": self.calculate_returns(),
            "drawdown": {
                "current": self.get_current_drawdown(),
                "max": self.get_max_drawdown()
            },
            "trade_metrics": self.get_trade_metrics(),
        }

        if self.benchmark_values:
            summary["benchmark_comparison"] = self.compare_to_benchmark()

        summary["strategies"] = {}
        for strategy_id in self.strategy_returns:
            summary["strategies"][strategy_id] = self.calculate_strategy_performance(strategy_id)

        return summary

    def get_current_drawdown(self) -> float:
        """Get the current drawdown value"""
        if not self.portfolio_values:
            return 0.0

        df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])
        df = df.sort_values('timestamp')

        latest_value = df['value'].iloc[-1]
        peak_value = df['value'].max()

        if peak_value == 0:
            return 0.0

        return (latest_value - peak_value) / peak_value

    def get_max_drawdown(self) -> float:
        """Get the maximum drawdown value"""
        if not self.portfolio_values:
            return 0.0

        return PerformanceMetrics.calculate_max_drawdown([v for _, v in self.portfolio_values])

    def get_trade_metrics(self) -> Dict:
        """Calculate trade-related metrics across all strategies"""
        if not self.trade_history:
            return {"error": "No trades recorded"}

        total_trades = len(self.trade_history)
        winning_trades = len([t for t in self.trade_history if t.profit > 0])
        losing_trades = len([t for t in self.trade_history if t.profit < 0])

        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        gross_profit = sum([t.profit for t in self.trade_history if t.profit > 0])
        gross_loss = abs(sum([t.profit for t in self.trade_history if t.profit < 0]))

        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        avg_profit = sum([t.profit for t in self.trade_history]) / total_trades if total_trades > 0 else 0
        avg_win = gross_profit / winning_trades if winning_trades > 0 else 0
        avg_loss = gross_loss / losing_trades if losing_trades > 0 else 0

        expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

        holding_times = []
        for trade in self.trade_history:
            if trade.entry_time and trade.exit_time:
                holding_time = (trade.exit_time - trade.entry_time).total_seconds() / 3600
                holding_times.append(holding_time)

        avg_holding_time = sum(holding_times) / len(holding_times) if holding_times else 0

        return {
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
            "avg_profit": avg_profit,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "avg_holding_time_hours": avg_holding_time,
            "total_profit": gross_profit - gross_loss
        }

    def export_performance_report(self, filename: str = None) -> str:
        """Export a comprehensive performance report to a file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = self.config.get("system", {}).get("output_dir", "output")
            filename = f"{output_dir}/performance_report_{timestamp}.json"

        summary = self.get_performance_summary()

        df = pd.DataFrame([summary])
        df.to_json(filename, orient='records', indent=4)

        self.logger.info(f"Performance report exported to {filename}")
        return filename

    def calculate_intraday_metrics(self) -> Dict:
        """Calculate intraday performance metrics"""
        if not self.portfolio_values:
            return {"error": "No portfolio values recorded"}

        df = pd.DataFrame(self.portfolio_values, columns=['timestamp', 'value'])

        today = datetime.now().date()
        df = df[df['timestamp'].dt.date == today]

        if len(df) < 2:
            return {"error": "Insufficient intraday data points"}

        start_value = df['value'].iloc[0]
        current_value = df['value'].iloc[-1]

        intraday_return = (current_value / start_value) - 1

        intraday_high = df['value'].max()
        intraday_low = df['value'].min()

        df['peak'] = df['value'].cummax()
        df['drawdown'] = (df['value'] - df['peak']) / df['peak']
        max_drawdown = df['drawdown'].min()

        return {
            "date": today,
            "start_value": start_value,
            "current_value": current_value,
            "intraday_return": intraday_return,
            "intraday_high": intraday_high,
            "intraday_low": intraday_low,
            "intraday_max_drawdown": max_drawdown
        }

    def reset(self):
        """Reset all performance data"""
        self.portfolio_values = []
        self.benchmark_values = []
        self.trade_history = []
        self.daily_returns = {}
        self.strategy_returns = {}

        try:
            initial_value = self.portfolio.calculate_portfolio_value()
            self.record_portfolio_value(datetime.now(), initial_value)
        except Exception as e:
            self.logger.error(f"Failed to get initial portfolio value: {e}")
            initial_value = self.portfolio.initial_capital
            self.record_portfolio_value(datetime.now(), initial_value)

        if self.event_manager:
            self._register_event_handlers()

        self.logger.info("Performance tracker reset")

    def save(self, filepath: str) -> None:
        """
        Save performance metrics to a file.

        Args:
            filepath: Path to save the performance data
        """
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            try:
                metrics = self.get_performance_summary()
            except Exception as e:
                metrics = {
                    "basic_info": {
                        "portfolio_values": len(self.portfolio_values),
                        "benchmark_values": len(self.benchmark_values),
                        "trade_history": len(self.trade_history),
                        "daily_returns": len(self.daily_returns)
                    }
                }

            metrics['timestamp'] = datetime.now().isoformat()

            with open(filepath, 'w') as f:
                json.dump(metrics, f, indent=4)

            self.logger.info(f"Performance data saved to {filepath}")

        except Exception as e:
            self.logger.error(f"Error saving performance data: {str(e)}")


class PerformanceMetrics:
    """Utility class for calculating performance metrics"""

    @staticmethod
    def calculate_sharpe_ratio(returns, risk_free_rate=0):
        """Calculate Sharpe ratio from a series of returns"""
        import numpy as np

        excess_returns = returns - risk_free_rate
        if len(excess_returns) < 2:
            return 0.0

        return np.mean(excess_returns) / np.std(excess_returns, ddof=1) * np.sqrt(252) if np.std(excess_returns, ddof=1) > 0 else 0.0

    @staticmethod
    def calculate_sortino_ratio(returns, risk_free_rate=0):
        """Calculate Sortino ratio from a series of returns"""
        import numpy as np

        excess_returns = returns - risk_free_rate
        if len(excess_returns) < 2:
            return 0.0

        negative_returns = np.where(excess_returns < 0, excess_returns, 0)
        downside_deviation = np.sqrt(np.mean(np.square(negative_returns))) if len(negative_returns) > 0 else 0.0

        return np.mean(excess_returns) / downside_deviation * np.sqrt(252) if downside_deviation > 0 else 0.0

    @staticmethod
    def calculate_max_drawdown(equity_curve):
        """Calculate maximum drawdown from an equity curve"""
        import numpy as np

        if not isinstance(equity_curve, np.ndarray):
            equity_curve = np.array(equity_curve)

        if len(equity_curve) < 2:
            return 0.0

        running_max = np.maximum.accumulate(equity_curve)

        drawdowns = (equity_curve - running_max) / running_max

        return abs(np.min(drawdowns)) if len(drawdowns) > 0 else 0.0
