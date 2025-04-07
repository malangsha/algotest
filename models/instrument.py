from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime, time
from utils.constants import InstrumentType

class AssetClass(Enum):
    EQUITY = "EQUITY"
    FOREX = "FOREX"
    CRYPTO = "CRYPTO"
    FUTURES = "FUTURES"
    OPTIONS = "OPTIONS"
    BOND = "BOND"
    ETF = "ETF"
    INDEX = "INDEX"
    COMMODITY = "COMMODITY"

class Instrument:
    """Represents a tradable financial instrument"""

    def __init__(
        self,
        instrument_id: str = None,
        symbol: str = None,
        instrument_type: InstrumentType = InstrumentType.EQUITY,
        asset_class: AssetClass = None,
        exchange: str = None,
        description: Optional[str] = None,
        currency: str = "INR",
        timezone: str = "Asia/Kolkata",
        lot_size: float = 1.0,
        tick_size: float = 0.01,
        min_quantity: float = 1.0,
        max_quantity: Optional[float] = None,
        margin_requirement: float = 1.0,  # 1.0 means no margin (cash required)
        trading_hours: Optional[Dict[str, List[Dict[str, time]]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        # Handle the case where 'id' is passed instead of 'instrument_id'
        if 'id' in kwargs and instrument_id is None:
            instrument_id = kwargs.pop('id')

        self.instrument_id = instrument_id
        self.symbol = symbol
        self.instrument_type = instrument_type
        self.asset_class = asset_class
        self.exchange = exchange
        self.description = description
        self.currency = currency
        self.timezone = timezone
        self.lot_size = lot_size
        self.tick_size = tick_size
        self.min_quantity = min_quantity
        self.max_quantity = max_quantity
        self.margin_requirement = margin_requirement
        self.trading_hours = trading_hours or {}
        self.metadata = metadata or {}

        # Store any additional attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return (f"Instrument(id={self.instrument_id}, symbol={self.symbol}, "
                f"class={self.asset_class.value if self.asset_class else None}, exchange={self.exchange})")

    # Rest of the methods remain unchanged...
    def is_trading_now(self, current_datetime: Optional[datetime] = None) -> bool:
        """Check if the instrument is tradable at the given datetime"""
        if not self.trading_hours:
            return True  # Default if no trading hours defined

        current_datetime = current_datetime or datetime.now()
        weekday = current_datetime.strftime("%A")
        current_time = current_datetime.time()

        # Check if today has trading hours
        if weekday not in self.trading_hours:
            return False

        # Check if current time is within any of the trading sessions
        for session in self.trading_hours[weekday]:
            if session["start"] <= current_time <= session["end"]:
                return True

        return False

    def round_quantity(self, quantity: float) -> float:
        """Round the quantity to the nearest lot size"""
        return round(quantity / self.lot_size) * self.lot_size

    def round_price(self, price: float) -> float:
        """Round the price to the nearest tick size"""
        return round(price / self.tick_size) * self.tick_size

    def validate_quantity(self, quantity: float) -> bool:
        """Validate if the quantity is valid for this instrument"""
        if quantity < self.min_quantity:
            return False
        if self.max_quantity is not None and quantity > self.max_quantity:
            return False
        # Check if quantity is a multiple of lot size
        remainder = quantity % self.lot_size
        return abs(remainder) < 1e-10  # Using epsilon for float comparison

    def to_dict(self) -> Dict[str, Any]:
        """Convert instrument to dictionary for serialization"""
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "instrument_type": self.instrument_type.value if self.instrument_type else InstrumentType.EQUITY,
            "asset_class": self.asset_class.value if self.asset_class else None,
            "exchange": self.exchange,
            "description": self.description,
            "currency": self.currency,
            "timezone": self.timezone,
            "lot_size": self.lot_size,
            "tick_size": self.tick_size,
            "min_quantity": self.min_quantity,
            "max_quantity": self.max_quantity,
            "margin_requirement": self.margin_requirement,
            "trading_hours": self._serialize_trading_hours(),
            "metadata": self.metadata
        }

    def _serialize_trading_hours(self) -> Dict[str, List[Dict[str, str]]]:
        """Serialize trading hours for JSON serialization"""
        result = {}
        for day, sessions in self.trading_hours.items():
            result[day] = []
            for session in sessions:
                result[day].append({
                    "start": session["start"].strftime("%H:%M:%S"),
                    "end": session["end"].strftime("%H:%M:%S")
                })
        return result
