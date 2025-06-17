from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from utils.constants import OrderStatus, OrderType

import uuid
import logging

logger = logging.getLogger("models.order")

class OrderActionType(Enum):
    CREATE = "CREATE"
    SUBMIT = "SUBMIT"
    CANCEL = "CANCEL"
    MODIFY = "MODIFY"
    PENDING = "PENDING"
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
        side: str, # BUY or SELL
        order_type: OrderType = OrderType.MARKET,
        product_type: Optional[str] = None, # e.g., MIS, CNC, NRML
        exchange: Optional[str] = None, # e.g., NSE, BSE, NFO
        price: Optional[float] = None,
        stop_price: Optional[float] = None, # Also known as trigger_price for stop orders
        time_in_force: str = "DAY", # e.g., DAY, IOC, GTC
        strategy_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None, # For additional broker-specific or custom parameters
        action: Optional[str] = None,  # For backward compatibility, 'side' is preferred
        # client_order_id: Optional[str] = None # Optional: if you want to track a client-provided ID
        rejection_reason: Optional[str] = None # Added rejection_reason
    ):
        self.order_id = str(uuid.uuid4())
        self.instrument_id = instrument_id
        self.quantity = quantity
        self.product_type = product_type
        self.exchange = exchange

        # Standardize side and action - side is the canonical parameter
        if side:
            self.side = side.upper() # Ensure BUY or SELL
            self.action = self.side  # Set action for backward compatibility
        elif action:
            self.side = action.upper()
            self.action = self.side
        else:
            raise OrderValidationError("Either 'side' or 'action' (for backward compatibility) must be provided and be 'BUY' or 'SELL'")

        if self.side not in ["BUY", "SELL"]:
             raise OrderValidationError(f"Invalid side/action: '{self.side}'. Must be 'BUY' or 'SELL'.")


        self.order_type = order_type # Should be an instance of OrderType enum
        self.price = price
        self.stop_price = stop_price
        self.time_in_force = time_in_force
        self.strategy_id = strategy_id
        self.params = params or {}
        # self.client_order_id = client_order_id

        self.status: OrderStatus = OrderStatus.CREATED # Should be an instance of OrderStatus enum
        self.filled_quantity = 0.0
        self.average_fill_price = 0.0
        self.created_at = datetime.now()
        self.submitted_at: Optional[datetime] = None
        self.filled_at: Optional[datetime] = None
        self.canceled_at: Optional[datetime] = None
        self.broker_order_id: Optional[str] = None
        self.last_updated = self.created_at
        self.rejection_reason = rejection_reason # Initialize rejection_reason

        # Validate the order
        self.validate()

    def __str__(self):
        # Handle both string and enum values for order_type and status
        order_type_str = self.order_type.value if hasattr(self.order_type, 'value') else str(self.order_type)
        status_str = self.status.value if hasattr(self.status, 'value') else str(self.status)

        return (f"Order(id={self.order_id}, instrument={self.instrument_id}, "
                f"side={self.side}, qty={self.quantity}, type={order_type_str}, "
                f"status={status_str}{f', reason={self.rejection_reason}' if self.rejection_reason else ''})")

    def validate(self) -> bool:
        """
        Validate that the order has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        required_fields = ["order_id", "instrument_id", "quantity", "side", "status", "exchange"]

        # Additional validation for specific order types
        # Assuming OrderType has .value for comparison if it's an enum, or it's directly comparable
        order_type_value = self.order_type.value if isinstance(self.order_type, Enum) else self.order_type

        if order_type_value == OrderType.LIMIT.value and self.price is None:
            error_msg = "Limit orders must have a price"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)

        if order_type_value == OrderType.SL.value and self.stop_price is None: # Assuming SL is a string or enum value
            error_msg = "Stop orders (SL) must have a stop price (trigger_price)"
            logger.error(error_msg)
            raise OrderValidationError(error_msg)

        if order_type_value == OrderType.SL_M.value and self.stop_price is None: # Assuming SL_M for Stop-Market
             error_msg = "Stop-Market orders (SL-M) must have a stop price (trigger_price)"
             logger.error(error_msg)
             raise OrderValidationError(error_msg)
        # Note: Original code had SL_M check `self.price is None`.
        # For SL-M, price is not used to limit execution after trigger.
        # If SL-M means Stop-Limit, then both price and stop_price are needed.
        # Clarify if SL_M is Stop-Market or Stop-Limit. If Stop-Limit, original check was:
        # if order_type_value == OrderType.SL_M.value and (self.stop_price is None or self.price is None):
        #     error_msg = "Stop-limit orders must have both a price and a stop price"

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
            OrderStatus.PENDING, # Added PENDING
            OrderStatus.SUBMITTED,
            OrderStatus.OPEN, # Added OPEN
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
        elif new_status == OrderStatus.CANCELLED: # Corrected from CANCELED
            self.canceled_at = timestamp
        elif new_status == OrderStatus.REJECTED and not self.rejection_reason: # Set default if not already set
            self.rejection_reason = "Rejected without specific reason"


    def update_fill(self, fill_quantity: float, fill_price: float, timestamp: Optional[datetime] = None):
        """Update the order with a new fill"""
        timestamp = timestamp or datetime.now()

        if fill_quantity <= 0:
            logger.warning(f"Attempted to update fill for order {self.order_id} with non-positive quantity: {fill_quantity}")
            return

        # Calculate the new average fill price
        total_filled_value = (self.average_fill_price * self.filled_quantity) + (fill_price * fill_quantity)
        self.filled_quantity += fill_quantity

        if self.filled_quantity > 0:
            self.average_fill_price = total_filled_value / self.filled_quantity

        # Update status based on fill
        # Using a small tolerance for float comparison
        if abs(self.quantity - self.filled_quantity) < 1e-9: # Check if fully filled
            self.update_status(OrderStatus.FILLED, timestamp)
        elif self.filled_quantity > 0: # Partially filled
            self.update_status(OrderStatus.PARTIALLY_FILLED, timestamp)
        # If filled_quantity is 0 after this (should not happen if fill_quantity > 0), status remains as is or PENDING/SUBMITTED

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
            "order_type": self.order_type.value if isinstance(self.order_type, Enum) else self.order_type,
            "product_type": self.product_type,
            "exchange": self.exchange,
            "price": self.price,
            "stop_price": self.stop_price,
            "time_in_force": self.time_in_force,
            "strategy_id": self.strategy_id,
            "status": self.status.value if isinstance(self.status, Enum) else self.status,
            "filled_quantity": self.filled_quantity,
            "average_fill_price": self.average_fill_price,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "canceled_at": self.canceled_at.isoformat() if self.canceled_at else None,
            "broker_order_id": self.broker_order_id,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "rejection_reason": self.rejection_reason, # Added rejection_reason
            "params": self.params
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Order':
        """Create an order object from a dictionary"""
        # Parse enum values safely
        order_type_val = data.get("order_type")
        if isinstance(order_type_val, str):
            order_type_to_use = OrderType(order_type_val)
        elif isinstance(order_type_val, OrderType):
            order_type_to_use = order_type_val
        else: # Default or raise error
            order_type_to_use = OrderType.MARKET
            logger.warning(f"Order type '{order_type_val}' not recognized or missing, defaulting to MARKET for order data: {data.get('order_id')}")


        status_val = data.get("status")
        if isinstance(status_val, str):
            status_to_use = OrderStatus(status_val)
        elif isinstance(status_val, OrderStatus):
            status_to_use = status_val
        else: # Default or raise error
            status_to_use = OrderStatus.CREATED
            logger.warning(f"Status '{status_val}' not recognized or missing, defaulting to CREATED for order data: {data.get('order_id')}")


        # Create the order object
        order = cls(
            instrument_id=data["instrument_id"],
            quantity=float(data["quantity"]),
            side=data.get("side") or data.get("action"), # Handle potential missing side
            order_type=order_type_to_use,
            product_type=data.get("product_type"),
            exchange=data.get("exchange"),
            price=data.get("price"),
            stop_price=data.get("stop_price"),
            time_in_force=data.get("time_in_force", "DAY"),
            strategy_id=data.get("strategy_id"),
            params=data.get("params", {}),
            rejection_reason=data.get("rejection_reason") # Added rejection_reason
        )

        # Set other fields that are not part of __init__ or need override
        order.order_id = data.get("order_id", order.order_id) # Keep original if re-hydrating
        order.status = status_to_use
        order.filled_quantity = float(data.get("filled_quantity", 0.0))
        order.average_fill_price = float(data.get("average_fill_price", 0.0))
        order.broker_order_id = data.get("broker_order_id")
        # order.client_order_id = data.get("client_order_id")


        # Parse timestamps
        for field_name in ["created_at", "submitted_at", "filled_at", "canceled_at", "last_updated"]:
            ts_val = data.get(field_name)
            if ts_val:
                if isinstance(ts_val, str):
                    try:
                        setattr(order, field_name, datetime.fromisoformat(ts_val.replace("Z", "+00:00")))
                    except ValueError:
                         try: # Attempt another common format if ISO fails
                            setattr(order, field_name, datetime.strptime(ts_val, "%Y-%m-%d %H:%M:%S.%f%z"))
                         except ValueError:
                            logger.warning(f"Failed to parse timestamp string '{ts_val}' for {field_name} in order {order.order_id}")
                elif isinstance(ts_val, datetime):
                     setattr(order, field_name, ts_val)
                else:
                    logger.warning(f"Unsupported timestamp type for {field_name} in order {order.order_id}: {type(ts_val)}")


        # Ensure last_updated is set if created_at is
        if order.created_at and not data.get("last_updated"):
            order.last_updated = order.created_at

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

