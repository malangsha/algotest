"""
Moving Average Crossover Strategy

This strategy generates trading signals based on the crossover of two moving averages.
"""

import logging
from typing import Dict, List, Optional, Any
import numpy as np
import pandas as pd

from strategies.base_strategy import OptionStrategy
from strategies.strategy_registry import StrategyRegistry
from core.logging_manager import get_logger

@StrategyRegistry.register('moving_average_crossover')
class MovingAverageCrossover(OptionStrategy):
    """
    Strategy that generates signals based on moving average crossovers.
    
    Parameters:
        fast_period: Period for fast moving average
        slow_period: Period for slow moving average
        stop_loss: Stop loss percentage
        take_profit: Take profit percentage
    """
    
    def __init__(self, strategy_id: str, config: Dict, data_manager, option_manager, portfolio_manager, event_manager):
        """Initialize the strategy."""
        super().__init__(strategy_id, config, data_manager, option_manager, portfolio_manager, event_manager)
        
        # Strategy parameters
        self.fast_period = config.get('parameters', {}).get('fast_period', 10)
        self.slow_period = config.get('parameters', {}).get('slow_period', 20)
        self.stop_loss = config.get('parameters', {}).get('stop_loss', 0.02)
        self.take_profit = config.get('parameters', {}).get('take_profit', 0.03)
        
        # Initialize state
        self.positions: Dict[str, Dict] = {}
        self.price_history: Dict[str, pd.DataFrame] = {}
        self.indicators: Dict[str, Dict] = {}
        
        # Initialize indices from config
        self.indices = config.get('indices', [])
        if not self.indices:
            self.logger.warning("No indices configured for MovingAverageCrossover strategy")
        
    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get the required options for this strategy.
        
        Returns:
            Dictionary mapping index symbols to lists of option requirements.
            Each option requirement is a dict with keys: strike, option_type, expiry_offset
        """
        required_options = {}
        for index in self.indices:
            required_options[index] = []  # This strategy doesn't use options
        return required_options
        
    def on_market_data(self, event: Dict) -> None:
        """
        Process market data event.
        
        Args:
            event: Market data event containing price information
        """
        symbol = event.get('symbol')
        if not symbol:
            return
            
        # Update price history
        if symbol not in self.price_history:
            self.price_history[symbol] = pd.DataFrame()
            
        price_data = pd.DataFrame([{
            'timestamp': event['timestamp'],
            'price': event['price']
        }])
        self.price_history[symbol] = pd.concat([self.price_history[symbol], price_data])
        
        # Calculate indicators
        if len(self.price_history[symbol]) >= self.slow_period:
            self.indicators[symbol] = {
                'fast_ma': self.price_history[symbol]['price'].rolling(self.fast_period).mean().iloc[-1],
                'slow_ma': self.price_history[symbol]['price'].rolling(self.slow_period).mean().iloc[-1]
            }
            
            # Check for crossover signals
            if symbol not in self.positions:
                self._try_enter_position(symbol)
            else:
                self._check_exit_conditions(symbol)
                
    def on_bar(self, event: Dict) -> None:
        """
        Process bar event.
        
        Args:
            event: Bar event containing OHLCV data
        """
        symbol = event.get('symbol')
        if not symbol or symbol not in self.positions:
            return
            
        self._check_exit_conditions(symbol)
        
    def _try_enter_position(self, symbol: str) -> None:
        """
        Try to enter a position based on crossover signals.
        
        Args:
            symbol: Symbol to enter position for
        """
        if symbol not in self.indicators:
            return
            
        indicators = self.indicators[symbol]
        if indicators['fast_ma'] > indicators['slow_ma']:
            # Bullish crossover - enter long position
            self._enter_position(symbol, 'LONG')
        elif indicators['fast_ma'] < indicators['slow_ma']:
            # Bearish crossover - enter short position
            self._enter_position(symbol, 'SHORT')
            
    def _check_exit_conditions(self, symbol: str) -> None:
        """
        Check if position should be exited.
        
        Args:
            symbol: Symbol to check exit conditions for
        """
        if symbol not in self.positions or symbol not in self.indicators:
            return
            
        position = self.positions[symbol]
        current_price = self.price_history[symbol]['price'].iloc[-1]
        entry_price = position['entry_price']
        
        # Check stop loss and take profit
        if position['direction'] == 'LONG':
            if current_price <= entry_price * (1 - self.stop_loss):
                self._exit_position(symbol, 'STOP_LOSS')
            elif current_price >= entry_price * (1 + self.take_profit):
                self._exit_position(symbol, 'TAKE_PROFIT')
        else:  # SHORT
            if current_price >= entry_price * (1 + self.stop_loss):
                self._exit_position(symbol, 'STOP_LOSS')
            elif current_price <= entry_price * (1 - self.take_profit):
                self._exit_position(symbol, 'TAKE_PROFIT')
                
    def _enter_position(self, symbol: str, direction: str) -> None:
        """
        Enter a new position.
        
        Args:
            symbol: Symbol to enter position for
            direction: Direction of position (LONG or SHORT)
        """
        current_price = self.price_history[symbol]['price'].iloc[-1]
        
        self.positions[symbol] = {
            'direction': direction,
            'entry_price': current_price,
            'entry_time': self.price_history[symbol]['timestamp'].iloc[-1]
        }
        
        logger.info(f"Entered {direction} position for {symbol} at {current_price}")
        
    def _exit_position(self, symbol: str, reason: str) -> None:
        """
        Exit an existing position.
        
        Args:
            symbol: Symbol to exit position for
            reason: Reason for exit (STOP_LOSS, TAKE_PROFIT, etc.)
        """
        if symbol not in self.positions:
            return
            
        position = self.positions[symbol]
        current_price = self.price_history[symbol]['price'].iloc[-1]
        
        logger.info(f"Exiting {position['direction']} position for {symbol} at {current_price} ({reason})")
        del self.positions[symbol]
        
    def on_fill(self, event: Dict) -> None:
        """
        Process fill event.
        
        Args:
            event: Fill event containing order execution details
        """
        pass  # No specific handling needed for this strategy
        
    def on_stop(self) -> None:
        """Clean up when strategy is stopped."""
        self.positions.clear()
        self.price_history.clear()
        self.indicators.clear()
