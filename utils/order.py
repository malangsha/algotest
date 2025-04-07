from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from utils.constants import OrderType, OrderSide, OrderStatus, TimeInForce, ProductType

@dataclass
class Order:
    """Represents an order in the trading system."""
    
    symbol: str
    exchange: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    order_id: str = None
    client_order_id: str = None
    broker_order_id: Optional[str] = None
    strategy_id: Optional[str] = None
    product_type: ProductType = ProductType.CNC
    time_in_force: TimeInForce = TimeInForce.DAY
    filled_quantity: float = 0.0
    remaining_quantity: Optional[float] = None
    average_price: Optional[float] = None
    order_time: Optional[int] = None
    last_update_time: Optional[int] = None
    rejection_reason: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    parent_order_id: Optional[str] = None
    child_order_ids: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize default values and validate."""
        if self.order_id is None:
            self.order_id = str(uuid.uuid4())
        
        if self.client_order_id is None:
            self.client_order_id = f"CL-{str(uuid.uuid4())[:8].upper()}"
        
        if self.order_time is None:
            self.order_time = int(datetime.now().timestamp() * 1000)
        
        if self.last_update_time is None:
            self.last_update_time = self.order_time
        
        if self.remaining_quantity is None:
            self.remaining_quantity = self.quantity