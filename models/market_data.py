from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

class BarInterval(Enum):
    TICK = "TICK"
    SECOND_1 = "1S"
    SECOND_5 = "5S"
    SECOND_15 = "15S"
    SECOND_30 = "30S"
    MINUTE_1 = "1M"
    MINUTE_5 = "5M"
    MINUTE_15 = "15M"
    MINUTE_30 = "30M"
    HOUR_1 = "1H"
    HOUR_4 = "4H"
    DAY_1 = "1D"
    WEEK_1 = "1W"
    MONTH_1 = "1M"

@dataclass
class OHLCV:
    """Represents Open-High-Low-Close-Volume data"""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    interval: BarInterval
    instrument_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.timestamp.isoformat(),
            "interval": self.interval.value,
            "instrument_id": self.instrument_id
        }

@dataclass
class Quote:
    """Represents a bid-ask quote"""
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    timestamp: datetime
    instrument_id: str

    @property
    def mid(self) -> float:
        """Calculate the mid price"""
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float:
        """Calculate the bid-ask spread"""
        return self.ask - self.bid

    @property
    def spread_pct(self) -> float:
        """Calculate the bid-ask spread as a percentage of the mid price"""
        return (self.spread / self.mid) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bid": self.bid,
            "ask": self.ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "timestamp": self.timestamp.isoformat(),
            "instrument_id": self.instrument_id,
            "mid": self.mid,
            "spread": self.spread,
            "spread_pct": self.spread_pct
        }

@dataclass
class Trade:
    """Represents a market trade"""
    price: float
    quantity: float
    timestamp: datetime
    instrument_id: str
    trade_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "price": self.price,
            "quantity": self.quantity,
            "timestamp": self.timestamp.isoformat(),
            "instrument_id": self.instrument_id,
            "trade_id": self.trade_id
        }

class MarketData:
    """Container for various types of market data"""

    def __init__(self, instrument_id: str):
        self.instrument_id = instrument_id
        self.bars: Dict[BarInterval, List[OHLCV]] = {interval: [] for interval in BarInterval}
        self.quotes: List[Quote] = []
        self.trades: List[Trade] = []
        self.last_update_time: Optional[datetime] = None

    def add_bar(self, bar: OHLCV) -> None:
        """Add a new OHLCV bar"""
        if bar.instrument_id != self.instrument_id:
            raise ValueError(f"Bar instrument ID {bar.instrument_id} does not match {self.instrument_id}")

        self.bars[bar.interval].append(bar)
        self.last_update_time = max(self.last_update_time or bar.timestamp, bar.timestamp)

    def add_quote(self, quote: Quote) -> None:
        """Add a new quote"""
        if quote.instrument_id != self.instrument_id:
            raise ValueError(f"Quote instrument ID {quote.instrument_id} does not match {self.instrument_id}")

        self.quotes.append(quote)
        self.last_update_time = max(self.last_update_time or quote.timestamp, quote.timestamp)

    def add_trade(self, trade: Trade) -> None:
        """Add a new trade"""
        if trade.instrument_id != self.instrument_id:
            raise ValueError(f"Trade instrument ID {trade.instrument_id} does not match {self.instrument_id}")

        self.trades.append(trade)
        self.last_update_time = max(self.last_update_time or trade.timestamp, trade.timestamp)

    def get_latest_bar(self, interval: BarInterval) -> Optional[OHLCV]:
        """Get the most recent bar for the specified interval"""
        if not self.bars[interval]:
            return None
        return self.bars[interval][-1]

    def get_latest_quote(self) -> Optional[Quote]:
        """Get the most recent quote"""
        if not self.quotes:
            return None
        return self.quotes[-1]

    def get_latest_trade(self) -> Optional[Trade]:
        """Get the most recent trade"""
        if not self.trades:
            return None
        return self.trades[-1]
              
    def get_latest_price(self) -> Optional[float]:
        """Get the most recent price from any available source"""
        # Try to get from latest trade
        latest_trade = self.get_latest_trade()
        if latest_trade:
            return latest_trade.price

        # Try to get from latest quote (mid price)
        latest_quote = self.get_latest_quote()
        if latest_quote:
            return latest_quote.mid

        # Try to get from latest 1-minute bar
        latest_bar = self.get_latest_bar(BarInterval.MINUTE_1)
        if latest_bar:
            return latest_bar.close

        # If no data is available
        return None

    def get_bars(self, interval: BarInterval, count: int = -1) -> List[OHLCV]:
        """Get bars for the specified interval and count"""
        if count < 0:
            return self.bars[interval].copy()
        return self.bars[interval][-count:].copy()

    def clear_historical_data(self, before_timestamp: datetime) -> None:
        """Clear historical data older than the specified timestamp"""
        # Clear old bars
        for interval in self.bars:
            self.bars[interval] = [
                bar for bar in self.bars[interval]
                if bar.timestamp >= before_timestamp
            ]

        # Clear old quotes
        self.quotes = [
            quote for quote in self.quotes
            if quote.timestamp >= before_timestamp
        ]

        # Clear old trades
        self.trades = [
            trade for trade in self.trades
            if trade.timestamp >= before_timestamp
        ]

@dataclass
class Bar:
    """Represents a price bar with OHLCV data"""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    interval: BarInterval
    instrument_id: str

    def __post_init__(self):
        """Validate the bar data upon initialization"""
        if self.low > self.high:
            raise ValueError(f"Bar low price ({self.low}) cannot be greater than high price ({self.high})")
        if self.open < self.low or self.open > self.high:
            raise ValueError(f"Bar open price ({self.open}) must be between low ({self.low}) and high ({self.high})")
        if self.close < self.low or self.close > self.high:
            raise ValueError(f"Bar close price ({self.close}) must be between low ({self.low}) and high ({self.high})")

    @property
    def range(self) -> float:
        """Calculate the price range of the bar"""
        return self.high - self.low

    @property
    def body(self) -> float:
        """Calculate the body size of the bar (difference between open and close)"""
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        """Check if the bar is bullish (close > open)"""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """Check if the bar is bearish (close < open)"""
        return self.close < self.open

    @property
    def is_doji(self) -> bool:
        """Check if the bar is a doji (open == close)"""
        return self.close == self.open

    @property
    def upper_shadow(self) -> float:
        """Calculate the upper shadow length"""
        return self.high - max(self.open, self.close)

    @property
    def lower_shadow(self) -> float:
        """Calculate the lower shadow length"""
        return min(self.open, self.close) - self.low

    @property
    def typical_price(self) -> float:
        """Calculate the typical price (high + low + close) / 3"""
        return (self.high + self.low + self.close) / 3

    @property
    def vwap(self) -> float:
        """Calculate the volume-weighted average price"""
        return self.typical_price  # Simple approximation

    def to_dict(self) -> Dict[str, Any]:
        """Convert bar to dictionary representation"""
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.timestamp.isoformat(),
            "interval": self.interval.value,
            "instrument_id": self.instrument_id,
            "range": self.range,
            "body": self.body,
            "is_bullish": self.is_bullish,
            "is_bearish": self.is_bearish,
            "is_doji": self.is_doji
        }

    def to_ohlcv(self) -> OHLCV:
        """Convert Bar to OHLCV object"""
        return OHLCV(
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            timestamp=self.timestamp,
            interval=self.interval,
            instrument_id=self.instrument_id
        )
