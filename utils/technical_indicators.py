"""
Technical Indicators Module

This module provides various technical analysis indicators used by trading strategies.
"""

import numpy as np
from typing import List, Union

def calculate_ema(prices: List[float], period: int) -> float:
    """
    Calculate Exponential Moving Average (EMA) for a given price series.
    
    Args:
        prices: List of price values
        period: Number of periods to use for EMA calculation
        
    Returns:
        float: The calculated EMA value
    """
    if not prices or period <= 0:
        return 0.0
        
    # Convert to numpy array for efficient calculations
    prices_array = np.array(prices)
    
    # Calculate smoothing factor
    alpha = 2 / (period + 1)
    
    # Initialize EMA with SMA of first 'period' prices
    ema = np.mean(prices_array[:period])
    
    # Calculate EMA for remaining prices
    for price in prices_array[period:]:
        ema = alpha * price + (1 - alpha) * ema
        
    return float(ema)

def calculate_sma(prices: List[float], period: int) -> float:
    """
    Calculate Simple Moving Average (SMA) for a given price series.
    
    Args:
        prices: List of price values
        period: Number of periods to use for SMA calculation
        
    Returns:
        float: The calculated SMA value
    """
    if not prices or period <= 0 or len(prices) < period:
        return 0.0
        
    return float(sum(prices[-period:]) / period)

def calculate_rsi(prices: List[float], period: int = 14) -> float:
    """
    Calculate Relative Strength Index (RSI) for a given price series.
    
    Args:
        prices: List of price values
        period: Number of periods to use for RSI calculation (default: 14)
        
    Returns:
        float: The calculated RSI value (0-100)
    """
    if not prices or period <= 0 or len(prices) < period + 1:
        return 50.0  # Return neutral value if insufficient data
        
    # Calculate price changes
    deltas = np.diff(prices)
    
    # Separate gains and losses
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Calculate average gain and loss
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    # Calculate RS and RSI
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return float(rsi)

def calculate_macd(prices: List[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> tuple:
    """
    Calculate Moving Average Convergence Divergence (MACD).
    
    Args:
        prices: List of price values
        fast_period: Period for fast EMA (default: 12)
        slow_period: Period for slow EMA (default: 26)
        signal_period: Period for signal line (default: 9)
        
    Returns:
        tuple: (MACD line, Signal line, MACD histogram)
    """
    if not prices or len(prices) < slow_period + signal_period:
        return 0.0, 0.0, 0.0
        
    # Calculate EMAs
    fast_ema = calculate_ema(prices, fast_period)
    slow_ema = calculate_ema(prices, slow_period)
    
    # Calculate MACD line
    macd_line = fast_ema - slow_ema
    
    # Calculate signal line
    signal_line = calculate_ema([macd_line], signal_period)
    
    # Calculate MACD histogram
    histogram = macd_line - signal_line
    
    return float(macd_line), float(signal_line), float(histogram)

def calculate_bollinger_bands(prices: List[float], period: int = 20, num_std: float = 2.0) -> tuple:
    """
    Calculate Bollinger Bands for a given price series.
    
    Args:
        prices: List of price values
        period: Number of periods to use for calculation (default: 20)
        num_std: Number of standard deviations for bands (default: 2.0)
        
    Returns:
        tuple: (Middle band, Upper band, Lower band)
    """
    if not prices or period <= 0 or len(prices) < period:
        return 0.0, 0.0, 0.0
        
    # Calculate SMA
    sma = calculate_sma(prices, period)
    
    # Calculate standard deviation
    std = np.std(prices[-period:])
    
    # Calculate bands
    upper_band = sma + (num_std * std)
    lower_band = sma - (num_std * std)
    
    return float(sma), float(upper_band), float(lower_band) 