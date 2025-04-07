import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple, Union, Optional
import datetime
import time
import uuid
import logging
import pytz

logger = logging.getLogger(__name__)

def generate_order_id() -> str:
    """Generate a unique order ID."""
    return str(uuid.uuid4())

def timestamp_now() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)

def date_to_timestamp(date_str: str) -> int:
    """Convert date string to timestamp in milliseconds."""
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)

def timestamp_to_date(timestamp: int) -> str:
    """Convert millisecond timestamp to date string."""
    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
    return dt.strftime("%Y-%m-%d")

def get_ist_datetime() -> datetime.datetime:
    """Get current datetime in Indian Standard Time."""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.datetime.now(ist)

def is_market_open() -> bool:
    """Check if the Indian market is currently open."""
    now = get_ist_datetime()

    # Check if it's a weekend
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        return False

    # Check market hours (9:15 AM to 3:30 PM)
    market_open = datetime.time(9, 15)
    market_close = datetime.time(15, 30)
    current_time = now.time()

    return market_open <= current_time <= market_close

def round_to_tick_size(price: float, tick_size: float = 0.05) -> float:
    """Round price to the nearest tick size."""
    return round(price / tick_size) * tick_size

def calculate_lot_size(symbol: str, default: int = 1) -> int:
    """Get the lot size for a given symbol."""
    from utils.constants import DEFAULT_LOT_SIZES
    return DEFAULT_LOT_SIZES.get(symbol, default)

def calculate_brokerage(order_value: float, is_delivery: bool = False) -> float:
    """
    Calculate approximate brokerage for Indian brokers.

    Args:
        order_value: Total value of the order
        is_delivery: Whether it's a delivery order (CNC) or intraday (MIS)

    Returns:
        Estimated brokerage amount
    """
    # This is a simplistic model - actual calculations vary by broker
    if is_delivery:
        return min(order_value * 0.0005, 20)  # 0.05% or ₹20, whichever is lower
    else:
        return min(order_value * 0.0003, 20)  # 0.03% or ₹20, whichever is lower

def calculate_transaction_charges(order_value: float, exchange: str = "NSE") -> Dict[str, float]:
    """
    Calculate transaction charges for Indian exchanges.

    Args:
        order_value: Total value of the order
        exchange: Exchange name (NSE or BSE)

    Returns:
        Dictionary with breakdown of charges
    """
    # Approximate calculations
    stt = order_value * 0.001 if exchange == "NSE" else order_value * 0.0001
    exchange_charge = order_value * 0.0000325
    sebi_charges = order_value * 0.000001
    stamp_duty = order_value * 0.00002
    gst = (exchange_charge + sebi_charges) * 0.18

    return {
        "stt": stt,
        "exchange_charge": exchange_charge,
        "sebi_charges": sebi_charges,
        "stamp_duty": stamp_duty,
        "gst": gst,
        "total": stt + exchange_charge + sebi_charges + stamp_duty + gst
    }

def moving_average(data: np.ndarray, window: int) -> np.ndarray:
    """Calculate simple moving average."""
    return np.convolve(data, np.ones(window) / window, mode='valid')

def exponential_moving_average(data: np.ndarray, window: int) -> np.ndarray:
    """Calculate exponential moving average."""
    return pd.Series(data).ewm(span=window, adjust=False).mean().values

def bollinger_bands(data: np.ndarray, window: int, num_std: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Calculate Bollinger Bands.

    Args:
        data: Price data array
        window: Window size for moving average
        num_std: Number of standard deviations for the bands

    Returns:
        Tuple of (upper_band, middle_band, lower_band)
    """
    if len(data) < window:
        return np.array([]), np.array([]), np.array([])

    middle_band = moving_average(data, window)

    # Calculate standard deviation
    rolling_std = np.array([np.std(data[i:i+window]) for i in range(len(data)-window+1)])

    upper_band = middle_band + (rolling_std * num_std)
    lower_band = middle_band - (rolling_std * num_std)

    return upper_band, middle_band, lower_band

def rsi(data: np.ndarray, window: int = 14) -> np.ndarray:
    """
    Calculate Relative Strength Index.

    Args:
        data: Price data array
        window: RSI period

    Returns:
        RSI values
    """
    if len(data) <= window:
        return np.array([])

    # Calculate price changes
    delta = np.diff(data)

    # Separate gains and losses
    gains = np.maximum(delta, 0)
    losses = np.abs(np.minimum(delta, 0))

    # Calculate average gains and losses
    avg_gain = np.array([np.mean(gains[i:i+window]) for i in range(len(gains)-window+1)])
    avg_loss = np.array([np.mean(losses[i:i+window]) for i in range(len(losses)-window+1)])

    # Calculate RS and RSI
    rs = avg_gain / np.where(avg_loss == 0, 0.001, avg_loss)  # Avoid division by zero
    rsi_values = 100 - (100 / (1 + rs))

    # Pad with NaN to match original data length
    return np.concatenate([np.array([np.nan] * (window)), rsi_values])

def macd(data: np.ndarray, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Calculate MACD (Moving Average Convergence Divergence).

    Args:
        data: Price data array
        fast_period: Fast EMA period
        slow_period: Slow EMA period
        signal_period: Signal EMA period

    Returns:
        Tuple of (macd_line, signal_line, histogram)
    """
    if len(data) <= slow_period:
        return np.array([]), np.array([]), np.array([])

    # Calculate EMAs
    fast_ema = pd.Series(data).ewm(span=fast_period, adjust=False).mean()
    slow_ema = pd.Series(data).ewm(span=slow_period, adjust=False).mean()

    # Calculate MACD line
    macd_line = fast_ema - slow_ema

    # Calculate signal line
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()

    # Calculate histogram
    histogram = macd_line - signal_line

    return macd_line.values, signal_line.values, histogram.values

def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """
    Calculate Average True Range.

    Args:
        high: High prices array
        low: Low prices array
        close: Close prices array
        period: ATR period

    Returns:
        ATR values
    """
    if len(high) <= period:
        return np.array([])

    # Calculate true range
    tr1 = high[1:] - low[1:]
    tr2 = np.abs(high[1:] - close[:-1])
    tr3 = np.abs(low[1:] - close[:-1])

    tr = np.maximum(np.maximum(tr1, tr2), tr3)

    # Calculate ATR using simple moving average
    atr_values = moving_average(tr, period)

    # Pad with NaN to match original data length
    return np.concatenate([np.array([np.nan] * (period+1)), atr_values])

def create_output_directory(name: str):
    pass
