from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
import uuid
import logging

logger = logging.getLogger("models.order")

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"

class OrderStatus(Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PENDING = "PENDING"
    CANCELED = "CANCELED"
    CANCELLED = "CANCELLED"  # Alternative spelling for backward compatibility
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"

class OrderActionType(Enum):
    CREATE = "CREATE"
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    MODIFY = "MODIFY"
    UPDATE_STATUS = "UPDATE_STATUS"
    UPDATE_FILL = "UPDATE_FILL"
    
class OrderValidationError(Exception):
    """Exception raised when order validation fails"""
    pass
    
def validate_order(order, required_fields: List[str]):
    """
    Validate that an order has all required fields.
    
    Args:
        order: The order to validate
        required_fields: List of required field names
        
    Raises:
        OrderValidationError: If any required field is missing
    """
    missing_fields = []
    for field in required_fields:
        if not hasattr(order, field) or getattr(order, field) is None:
            missing_fields.append(field)
    
    if missing_fields:
        error_msg = f"Missing required fields in Order: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise OrderValidationError(error_msg)
    
    return True

class Order:
    """Represents a trading order in the system"""

    def __init__(
        self,
        instrument_id: str,
        quantity: float,
        side: str,
        order_type: OrderType = OrderType.MARKET,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        time_in_force: str = "DAY",
        strategy_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        action: Optional[str] = None  # For backward compatibility
    ):
        self.order_id = str(uuid.uuid4())
        self.instrument_id = instrument_id
        self.quantity = quantity
        
        # Standardize side and action - side is the canonical parameter
        if side:
            self.side = side.upper()  # BUY or SELL
            self.action = side.upper()  # Set action for backward compatibility
        elif action:
            self.side = action.upper()
            self.action = action.upper()
        else:
            raise OrderValidationError("Either side or action must be provided")
            
        self.order_type = order_type
        self.price = price
        self.stop_price = stop_price
        self.time_in_force = time_in_force
        self.strategy_id = strategy_id
        self.params = params or {}

        self.status = OrderStatus.CREATED
        self.filled_quantity = 0.0
        self.average_fill_price = 0.0
        self.created_at = datetime.now()
        self.submitted_at = None
        self.filled_at = None
        self.canceled_at = None
        self.broker_order_id = None
        self.last_updated = self.created_at
        
        # Validate the order
        self.validate()

    def __str__(self):
        # Handle both string and enum values for order_type and status
        order_type_str = self.order_type.value if hasattr(self.order_type, 'value') else str(self.order_type)
        status_str = self.status.value if hasattr(self.status, 'value') else str(self.status)
        
        return (f"Order(id={self.order_id}, instrument={self.instrument_id}, "
                f"side={self.side}, qty={self.quantity}, type={order_type_str}, "
                f"status={status_str})")
                
    def validate(self) -> bool:
        """
        Validate that the order has all required fields.
        
        Returns:
            bool: True if valid, raises exception otherwise
        """
        required_fields = ["order_id", "instrument_id", "quantity", "side", "status"]
        
        # Additional validation for specific order types
        if self.order_type == OrderType.LIMIT and self.price is None:
            error_msg = "Limit orders must have a price"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)
            
        if self.order_type == OrderType.STOP and self.stop_price is None:
            error_msg = "Stop orders must have a stop price"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)
            
        if self.order_type == OrderType.STOP_LIMIT and (self.stop_price is None or self.price is None):
            error_msg = "Stop-limit orders must have both a price and a stop price"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)
            
        return validate_order(self, required_fields)

    def remaining_quantity(self) -> float:
        """Return the unfilled quantity of this order"""
        return self.quantity - self.filled_quantity

    def is_filled(self) -> bool:
        """Check if the order is completely filled"""
        return self.status == OrderStatus.FILLED

    def is_active(self) -> bool:
        """Check if the order is still active in the market"""
        active_statuses = [
            OrderStatus.CREATED,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIALLY_FILLED
        ]
        return self.status in active_statuses

    def update_status(self, new_status: OrderStatus, timestamp: Optional[datetime] = None):
        """Update the order status and relevant timestamps"""
        timestamp = timestamp or datetime.now()
        
        # Log status change
        logger.info(f"Order {self.order_id} status changed: {self.status.value if hasattr(self.status, 'value') else self.status} -> {new_status.value if hasattr(new_status, 'value') else new_status}")
        
        self.status = new_status
        self.last_updated = timestamp

        if new_status == OrderStatus.SUBMITTED:
            self.submitted_at = timestamp
        elif new_status == OrderStatus.FILLED:
            self.filled_at = timestamp
        elif new_status == OrderStatus.CANCELED:
            self.canceled_at = timestamp

    def update_fill(self, fill_quantity: float, fill_price: float, timestamp: Optional[datetime] = None):
        """Update the order with a new fill"""
        timestamp = timestamp or datetime.now()

        # Calculate the new average fill price
        total_filled_value = (self.average_fill_price * self.filled_quantity) + (fill_price * fill_quantity)
        self.filled_quantity += fill_quantity

        if self.filled_quantity > 0:
            self.average_fill_price = total_filled_value / self.filled_quantity

        # Update status based on fill
        if abs(self.filled_quantity - self.quantity) < 1e-10:  # Account for floating point errors
            self.update_status(OrderStatus.FILLED, timestamp)
        elif self.filled_quantity > 0:
            self.update_status(OrderStatus.PARTIALLY_FILLED, timestamp)

        self.last_updated = timestamp
        
        logger.info(f"Order {self.order_id} filled: {fill_quantity} @ {fill_price}, total filled: {self.filled_quantity}/{self.quantity}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary for serialization"""
        return {
            "order_id": self.order_id,
            "instrument_id": self.instrument_id,
            "quantity": self.quantity,
            "side": self.side,
            "action": self.side,  # Include action for backward compatibility
            "order_type": self.order_type.value,
            "price": self.price,
            "stop_price": self.stop_price,
            "time_in_force": self.time_in_force,
            "strategy_id": self.strategy_id,
            "status": self.status.value,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "canceled_at": self.canceled_at.isoformat() if self.canceled_at else None,
            "broker_order_id": self.broker_order_id,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "params": self.params
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """Create an order object from a dictionary"""
        # Parse enum values
        order_type_str = data.get("order_type", OrderType.MARKET.value)
        status_str = data.get("status", OrderStatus.CREATED.value)
        
        # Handle order_type
        if isinstance(order_type_str, str):
            order_type = OrderType(order_type_str)
        else:
            order_type = order_type_str
            
        # Handle status
        if isinstance(status_str, str):
            status = OrderStatus(status_str)
        else:
            status = status_str
        
        # Create the order object
        order = cls(
            instrument_id=data["instrument_id"],
            quantity=data["quantity"],
            side=data.get("side") or data.get("action"),
            order_type=order_type,
            price=data.get("price"),
            stop_price=data.get("stop_price"),
            time_in_force=data.get("time_in_force", "DAY"),
            strategy_id=data.get("strategy_id"),
            params=data.get("params", {})
        )
        
        # Set other fields
        order.order_id = data.get("order_id", order.order_id)
        order.status = status
        order.filled_quantity = data.get("filled_quantity", 0.0)
        order.average_fill_price = data.get("average_fill_price", 0.0)
        order.broker_order_id = data.get("broker_order_id")
        
        # Parse timestamps
        for field, value in [
            ("created_at", data.get("created_at")),
            ("submitted_at", data.get("submitted_at")),
            ("filled_at", data.get("filled_at")),
            ("canceled_at", data.get("canceled_at")),
            ("last_updated", data.get("last_updated"))
        ]:
            if value:
                if isinstance(value, str):
                    try:
                        setattr(order, field, datetime.fromisoformat(value))
                    except Exception as e:
                        logger.warning(f"Failed to parse timestamp for {field}: {e}")
                else:
                    setattr(order, field, value)
        
        return order

class OrderAction:
    """Represents an action performed on an order in the system"""

    def __init__(
        self,
        order_id: str,
        action_type: OrderActionType,
        timestamp: Optional[datetime] = None,
        data: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
        source: Optional[str] = None
    ):
        self.action_id = str(uuid.uuid4())
        self.order_id = order_id
        self.action_type = action_type
        self.timestamp = timestamp or datetime.now()
        self.data = data or {}
        self.user_id = user_id
        self.source = source  # System, API, User Interface, etc.
        
        # Validate
        self.validate()
        
    def validate(self) -> bool:
        """
        Validate that the order action has all required fields.
        
        Returns:
            bool: True if valid, raises exception otherwise
        """
        required_fields = ["action_id", "order_id", "action_type", "timestamp"]
        
        missing_fields = []
        for field in required_fields:
            if not hasattr(self, field) or getattr(self, field) is None:
                missing_fields.append(field)
        
        if missing_fields:
            error_msg = f"Missing required fields in OrderAction: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)
        
        return True

    def __str__(self):
        return (f"OrderAction(id={self.action_id}, order_id={self.order_id}, "
                f"action={self.action_type.value}, timestamp={self.timestamp})")

    def to_dict(self) -> Dict[str, Any]:
        """Convert order action to dictionary for serialization"""
        return {
            "action_id": self.action_id,
            "order_id": self.order_id,
            "action_type": self.action_type.value,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "user_id": self.user_id,
            "source": self.source
        }

    @classmethod
    def create_status_update_action(
        cls,
        order_id: str,
        new_status: str,
        user_id: Optional[str] = None,
        source: Optional[str] = None
    ):
        """Factory method to create a status update action"""
        data = {"new_status": new_status}
        return cls(
            order_id=order_id,
            action_type=OrderActionType.UPDATE_STATUS,
            data=data,
            user_id=user_id,
            source=source
        )

    @classmethod
    def create_fill_update_action(
        cls,
        order_id: str,
        fill_quantity: float,
        fill_price: float,
        execution_id: Optional[str] = None,
        user_id: Optional[str] = None,
        source: Optional[str] = None
    ):
        """Factory method to create a fill update action"""
        data = {
            "fill_quantity": fill_quantity,
            "fill_price": fill_price,
            "execution_id": execution_id
        }
        return cls(
            order_id=order_id,
            action_type=OrderActionType.UPDATE_FILL,
            data=data,
            user_id=user_id,
            source=source
        )

    @classmethod
    def create_modify_action(
        cls,
        order_id: str,
        modifications: Dict[str, Any],
        user_id: Optional[str] = None,
        source: Optional[str] = None
    ):
        """Factory method to create a modification action"""
        return cls(
            order_id=order_id,
            action_type=OrderActionType.MODIFY,
            data=modifications,
            user_id=user_id,
            source=source
        )

