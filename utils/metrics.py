import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Union, Tuple
import logging

logger = logging.getLogger(__name__)

class PerformanceMetrics:
    """Calculate various performance metrics for trading strategies."""

    @staticmethod
    def calculate_returns(equity_curve: np.ndarray) -> np.ndarray:
        """
        Calculate period returns from an equity curve.

        Args:
            equity_curve: Array of equity values

        Returns:
            Array of returns
        """
        if len(equity_curve) <= 1:
            return np.array([])

        return np.diff(equity_curve) / equity_curve[:-1]

    @staticmethod
    def calculate_cumulative_returns(returns: np.ndarray) -> np.ndarray:
        """
        Calculate cumulative returns from periodic returns.

        Args:
            returns: Array of period returns

        Returns:
            Array of cumulative returns
        """
        return np.cumprod(1 + returns) - 1

    @staticmethod
    def calculate_drawdown(equity_curve: np.ndarray) -> Tuple[np.ndarray, float, int]:
        """
        Calculate drawdown series and maximum drawdown.

        Args:
            equity_curve: Array of equity values

        Returns:
            Tuple of (drawdown_series, max_drawdown, max_drawdown_duration)
        """
        if len(equity_curve) <= 1:
            return np.array([]), 0.0, 0

        # Calculate running maximum
        running_max = np.maximum.accumulate(equity_curve)

        # Calculate drawdown series
        drawdown = (equity_curve - running_max) / running_max

        # Calculate maximum drawdown
        max_drawdown = np.min(drawdown)

        # Calculate maximum drawdown duration
        max_idx = np.argmin(drawdown)
        if max_idx == 0:
            max_drawdown_duration = 0
        else:
            # Find the last peak before the maximum drawdown
            prev_peak = np.where(equity_curve[:max_idx] == running_max[max_idx])[0][-1]
            max_drawdown_duration = max_idx - prev_peak

        return drawdown, max_drawdown, max_drawdown_duration

    @staticmethod
    def calculate_sharpe_ratio(returns: np.ndarray, risk_free_rate: float = 0.05, annualization_factor: int = 252) -> float:
        """
        Calculate the Sharpe ratio.

        Args:
            returns: Array of period returns
            risk_free_rate: Annual risk-free rate
            annualization_factor: Number of periods in a year

        Returns:
            Sharpe ratio
        """
        if len(returns) <= 1:
            return 0.0

        # Convert annual risk-free rate to period rate
        period_risk_free = (1 + risk_free_rate) ** (1 / annualization_factor) - 1

        excess_returns = returns - period_risk_free
        mean_excess = np.mean(excess_returns)
        std_excess = np.std(excess_returns, ddof=1)

        if std_excess == 0:
            return 0.0

        return (mean_excess / std_excess) * np.sqrt(annualization_factor)

    @staticmethod
    def calculate_sortino_ratio(returns: np.ndarray, risk_free_rate: float = 0.05, annualization_factor: int = 252) -> float:
        """
        Calculate the Sortino ratio.

        Args:
            returns: Array of period returns
            risk_free_rate: Annual risk-free rate
            annualization_factor: Number of periods in a year

        Returns:
            Sortino ratio
        """
        if len(returns) <= 1:
            return 0.0

        # Convert annual risk-free rate to period rate
        period_risk_free = (1 + risk_free_rate) ** (1 / annualization_factor) - 1

        excess_returns = returns - period_risk_free
        mean_excess = np.mean(excess_returns)

        # Calculate downside deviation (only negative returns)
        negative_returns = excess_returns[excess_returns < 0]

        if len(negative_returns) == 0:
            return np.inf  # No negative returns

        downside_deviation = np.sqrt(np.mean(negative_returns ** 2))

        if downside_deviation == 0:
            return 0.0

        return (mean_excess / downside_deviation) * np.sqrt(annualization_factor)

    @staticmethod
    def calculate_cagr(equity_curve: np.ndarray, num_years: float) -> float:
        """
        Calculate Compound Annual Growth Rate.

        Args:
            equity_curve: Array of equity values
            num_years: Number of years

        Returns:
            CAGR value
        """
        if len(equity_curve) <= 1 or num_years <= 0:
            return 0.0

        start_value = equity_curve[0]
        end_value = equity_curve[-1]

        if start_value <= 0:
            return 0.0

        return (end_value / start_value) ** (1 / num_years) - 1

    @staticmethod
    def calculate_win_rate(trades: List[Dict[str, Any]]) -> float:
        """
        Calculate win rate from list of trades.

        Args:
            trades: List of trade dictionaries

        Returns:
            Win rate as a percentage
        """
        if not trades:
            return 0.0

        winning_trades = sum(1 for trade in trades if trade.get('pnl', 0) > 0)
        return (winning_trades / len(trades)) * 100

    @staticmethod
    def calculate_profit_factor(trades: List[Dict[str, Any]]) -> float:
        """
        Calculate profit factor (gross profit / gross loss).

        Args:
            trades: List of trade dictionaries

        Returns:
            Profit factor
        """
        if not trades:
            return 0.0

        gross_profit = sum(trade.get('pnl', 0) for trade in trades if trade.get('pnl', 0) > 0)
        gross_loss = sum(abs(trade.get('pnl', 0)) for trade in trades if trade.get('pnl', 0) < 0)

        if gross_loss == 0:
            return np.inf if gross_profit > 0 else 0.0

        return gross_profit / gross_loss

    @staticmethod
    def calculate_expectancy(trades: List[Dict[str, Any]]) -> float:
        """
        Calculate expectancy (expected return per trade).

        Args:
            trades: List of trade dictionaries

        Returns:
            Expectancy value
        """
        if not trades:
            return 0.0

        total_pnl = sum(trade.get('pnl', 0) for trade in trades)
        return total_pnl / len(trades)

    @staticmethod
    def calculate_average_trade_duration(trades: List[Dict[str, Any]]) -> float:
        """
        Calculate average trade duration in days.

        Args:
            trades: List of trade dictionaries

        Returns:
            Average trade duration in days
        """
        if not trades:
            return 0.0

        durations = []
        for trade in trades:
            if 'entry_time' in trade and 'exit_time' in trade:
                duration = (trade['exit_time'] - trade['entry_time']) / (1000 * 60 * 60 * 24)  # Convert ms to days
                durations.append(duration)

        if not durations:
            return 0.0

        return sum(durations) / len(durations)

    @staticmethod
    def calculate_all_metrics(equity_curve: np.ndarray, trades: List[Dict[str, Any]], num_years: float = 1.0, risk_free_rate: float = 0.05) -> Dict[str, Any]:
        """
        Calculate all performance metrics.

        Args:
            equity_curve: Array of equity values
            trades: List of trade dictionaries
            num_years: Number of years
            risk_free_rate: Annual risk-free rate

        Returns:
            Dictionary of calculated metrics
        """
        if len(equity_curve) <= 1:
            return {}

        # Calculate returns
        returns = PerformanceMetrics.calculate_returns(equity_curve)

        # Calculate drawdown
        drawdown, max_drawdown, max_drawdown_duration = PerformanceMetrics.calculate_drawdown(equity_curve)

        # Calculate other metrics
        metrics = {
            "initial_capital": equity_curve[0],
            "final_capital": equity_curve[-1],
            "absolute_return": (equity_curve[-1] / equity_curve[0] - 1) * 100,
            "cagr": PerformanceMetrics.calculate_cagr(equity_curve, num_years) * 100,
            "sharpe_ratio": PerformanceMetrics.calculate_sharpe_ratio(returns, risk_free_rate),
            "sortino_ratio": PerformanceMetrics.calculate_sortino_ratio(returns, risk_free_rate),
            "max_drawdown": max_drawdown * 100,
            "max_drawdown_duration": max_drawdown_duration,
            "win_rate": PerformanceMetrics.calculate_win_rate(trades),
            "profit_factor": PerformanceMetrics.calculate_profit_factor(trades),
            "expectancy": PerformanceMetrics.calculate_expectancy(trades),
            "avg_trade_duration": PerformanceMetrics.calculate_average_trade_duration(trades),
            "total_trades": len(trades),
            "winning_trades": sum(1 for trade in trades if trade.get('pnl', 0) > 0),
            "losing_trades": sum(1 for trade in trades if trade.get('pnl', 0) < 0),
            "avg_profit": np.mean([trade.get('pnl', 0) for trade in trades if trade.get('pnl', 0) > 0]) if any(trade.get('pnl', 0) > 0 for trade in trades) else 0.0,
            "avg_loss": np.mean([trade.get('pnl', 0) for trade in trades if trade.get('pnl', 0) < 0]) if any(trade.get('pnl', 0) < 0 for trade in trades) else 0.0,
            "max_profit": max([trade.get('pnl', 0) for trade in trades]) if trades else 0.0,
            "max_loss": min([trade.get('pnl', 0) for trade in trades]) if trades else 0.0,
            "annual_volatility": np.std(returns, ddof=1) * np.sqrt(252) * 100
        }

        return metrics
