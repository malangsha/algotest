from datetime import datetime
from typing import Optional, Dict, Any
import uuid

class Trade:
    """Represents an executed trade in the system"""
    
    def __init__(
        self,
        order_id: str,
        instrument_id: str,
        quantity: float,
        price: float,
        side: str,
        timestamp: Optional[datetime] = None,
        commission: float = 0.0,
        fees: float = 0.0,
        strategy_id: Optional[str] = None,
        profit: float = 0.0
    ):
        self.trade_id = str(uuid.uuid4())
        self.order_id = order_id
        self.instrument_id = instrument_id
        self.quantity = quantity
        self.price = price
        self.side = side.upper()  # BUY or SELL
        self.timestamp = timestamp or datetime.now()
        self.commission = commission
        self.fees = fees
        self.strategy_id = strategy_id
        self.profit = profit
        
    def __str__(self):
        return (f"Trade(id={self.trade_id}, instrument={self.instrument_id}, "
                f"side={self.side}, qty={self.quantity}, price={self.price})")
    
    @property
    def value(self) -> float:
        """Calculate the monetary value of the trade"""
        return self.quantity * self.price
    
    @property
    def total_cost(self) -> float:
        """Calculate the total cost including commission and fees"""
        return self.value + self.commission + self.fees
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trade to dictionary for serialization"""
        return {
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "instrument_id": self.instrument_id,
            "quantity": self.quantity,
            "price": self.price,
            "side": self.side,
            "timestamp": self.timestamp.isoformat(),
            "commission": self.commission,
            "fees": self.fees,
            "strategy_id": self.strategy_id,
            "value": self.value,
            "total_cost": self.total_cost,
            "profile": self.profit
        }