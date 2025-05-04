from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Union, List, Callable
from datetime import datetime
import json
import uuid
import logging
import time
from enum import Enum

from utils.constants import Exchange, OrderType, OrderSide, OrderStatus, SignalType, EventType, MarketDataType, EventPriority, TimeframeEventType
from models.instrument import Instrument

logger = logging.getLogger("models.events")

class EventValidationError(Exception):
    """Exception raised when event validation fails"""
    pass

def validate_event(event, required_fields: List[str]):
    """
    Validate that an event has all required fields.

    Args:
        event: The event to validate
        required_fields: List of required field names

    Raises:
        EventValidationError: If any required field is missing
    """
    missing_fields = []
    for field in required_fields:
        if not hasattr(event, field) or getattr(event, field) is None:
            missing_fields.append(field)

    if missing_fields:
        error_msg = f"Missing required fields in {event.__class__.__name__}: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise EventValidationError(error_msg)

    return True

@dataclass
class Event:
    """Base class for all events in the system."""

    event_type: EventType
    timestamp: int
    event_id: str = None
    priority: EventPriority = EventPriority.NORMAL

    def __post_init__(self):
        if self.event_id is None:
            self.event_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = int(datetime.now().timestamp() * 1000)

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "priority": self.priority.value
        }

    def to_json(self) -> str:
        """Convert event to JSON string."""
        return json.dumps(self.to_dict())

    def validate(self) -> bool:
        """
        Validate that the event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        required_fields = ["event_type", "timestamp", "event_id"]
        return validate_event(self, required_fields)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]) if isinstance(data["event_type"], str) else data["event_type"],
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            priority=EventPriority(data["priority"]) if "priority" in data else EventPriority.NORMAL
        )

    def __lt__(self, other):
        """Compare events based on priority and timestamp"""
        if self.priority != other.priority:
            return self.priority.value < other.priority.value
        return self.timestamp < other.timestamp

@dataclass
class MarketDataEvent(Event):
    """Event for market data updates."""
    # Required arguments must come first
    instrument: Instrument = None
    exchange: str = Exchange.NSE
    data_type: MarketDataType = MarketDataType.QUOTE
    data: Dict[str, Any] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.MARKET_DATA
        if self.data is None:
            self.data = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert market data event to dictionary."""
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.instrument.symbol if self.instrument else None,  # Include symbol in serialized form for convenience
            "exchange": self.exchange,
            "data_type": self.data_type.value,
            "data": self.data
        })
        return result

    def validate(self) -> bool:
        """
        Validate that the market data event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["instrument", "data_type", "data"]
        return validate_event(self, required_fields)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'MarketDataEvent':
        """
        Create market data event from dictionary.

        Args:
            data: Dictionary containing event data
            instrument_registry: Optional registry to look up instrument
        """
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")

        # Attempt to get instrument from registry
        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol:
                instrument = instrument_registry.get_by_symbol(symbol)

        if not instrument:
            # Create a basic instrument if we can't find one
            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=data.get("exchange")
            )

        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            instrument=instrument,
            exchange=data["exchange"],
            data_type=MarketDataType(data["data_type"]),
            data=data["data"]
        )

    @property
    def symbol(self) -> str:
        """Get the symbol from the instrument for backward compatibility."""
        return self.instrument.symbol if self.instrument else None

@dataclass
class BarEvent(Event):
    """Event for timeframe-specific OHLCV bar updates."""

    instrument: Instrument = None
    timeframe: str = '1m'  # Default timeframe is 1 minute
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    volume: float = 0.0
    bar_start_time: Optional[int] = None  # Start time of the bar in milliseconds

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.BAR
        # If bar_start_time is not set, calculate it from the timestamp
        if self.bar_start_time is None:
            self.bar_start_time = self.timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert bar event to dictionary."""
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.instrument.symbol if self.instrument else None,
            "exchange": self.instrument.exchange if self.instrument else None,
            "timeframe": self.timeframe,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "volume": self.volume,
            "bar_start_time": self.bar_start_time
        })
        return result

    def validate(self) -> bool:
        """
        Validate that the bar event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["instrument", "timeframe", "open_price", "high_price", "low_price", "close_price"]
        return validate_event(self, required_fields)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'BarEvent':
        """
        Create bar event from dictionary.

        Args:
            data: Dictionary containing event data
            instrument_registry: Optional registry to look up instrument
        """
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange = data.get("exchange")

        # Attempt to get instrument from registry
        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol:
                instrument = instrument_registry.get_by_symbol(symbol, exchange)

        if not instrument:
            # Create a basic instrument if we can't find one
            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange
            )

        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            instrument=instrument,
            timeframe=data["timeframe"],
            open_price=data["open_price"],
            high_price=data["high_price"],
            low_price=data["low_price"],
            close_price=data["close_price"],
            volume=data.get("volume", 0.0),
            bar_start_time=data.get("bar_start_time")
        )

    @property
    def symbol(self) -> str:
        """Get the symbol from the instrument for backward compatibility."""
        return self.instrument.symbol if self.instrument else None

@dataclass
class TimeframeBarEvent(Event):
    """Event for completed timeframe bars"""
    instrument: Instrument = None
    timeframe: str = TimeframeEventType.BAR_1M
    bar_data: Dict[str, Any] = None
    greeks: Optional[Dict[str, float]] = None
    open_interest: Optional[float] = None
    _event_type: EventType = None  # Use a private field for setting the value
    
    def __post_init__(self):
        # Set the _event_id and _timestamp in parent class
        if self.event_id is None:
            self.event_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = int(datetime.now().timestamp() * 1000)
        
        # Always derive the event_type from timeframe
        self._event_type = self._derive_event_type_from_timeframe()
    
    def _derive_event_type_from_timeframe(self) -> EventType:
        """Derive the event type from the timeframe"""
        timeframe_map = {
            '1m': TimeframeEventType.BAR_1M,
            '5m': TimeframeEventType.BAR_5M,
            '15m': TimeframeEventType.BAR_15M,
            '30m': TimeframeEventType.BAR_30M,
            '1h': TimeframeEventType.BAR_1H,
            '4h': TimeframeEventType.BAR_4H,
            '1d': TimeframeEventType.BAR_1D
        }
        return timeframe_map.get(self.timeframe, TimeframeEventType.BAR_1M)
    
    @property
    def event_type(self) -> EventType:
        """Get the event type based on timeframe"""
        return self._event_type
    
    @event_type.setter
    def event_type(self, value):
        """Allow setting the event_type, but silently ignore it since we derive from timeframe"""
        pass  # Silently ignore attempts to set event_type directly
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert timeframe bar event to dictionary."""
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.instrument.symbol if self.instrument else None,
            "exchange": self.instrument.exchange if self.instrument else None,
            "timeframe": self.timeframe,
            "bar_data": self.bar_data,
            "greeks": self.greeks,
            "open_interest": self.open_interest,
            "bar_start_time": self.bar_start_time
        })
        return result
    
    def validate(self) -> bool:
        """
        Validate that the bar event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["instrument", "timeframe", "bar_data"]
        return validate_event(self, required_fields)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'TimeframeBarEvent':
        """
        Create timeframe bar event from dictionary.

        Args:
            data: Dictionary containing event data
            instrument_registry: Optional registry to look up instrument
        """
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange = data.get("exchange")

        # Attempt to get instrument from registry
        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol:
                instrument = instrument_registry.get_by_symbol(symbol, exchange)

        if not instrument:
            # Create a basic instrument if we can't find one
            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange
            )
            
        # Create bar_data dictionary from OHLCV fields if they exist
        bar_data = data.get("bar_data", {})
        if not bar_data and all(key in data for key in ["open_price", "high_price", "low_price", "close_price"]):
            bar_data = {
                "open": data["open_price"],
                "high": data["high_price"],
                "low": data["low_price"],
                "close": data["close_price"],
                "volume": data.get("volume", 0.0),
                "bar_start_time": data.get("bar_start_time")
            }

        return cls(
            event_type=EventType(data["event_type"]) if "event_type" in data else None,
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            instrument=instrument,
            timeframe=data["timeframe"],
            bar_data=bar_data,
            greeks=data.get("greeks"),
            open_interest=data.get("open_interest")
        )

    @property
    def symbol(self) -> str:
        """Get the symbol from the instrument for backward compatibility."""
        return self.instrument.symbol if self.instrument else None
        
    @property
    def open_price(self) -> float:
        """Get open price from bar_data."""
        return self.bar_data.get("open", 0.0)
    
    @property
    def high_price(self) -> float:
        """Get high price from bar_data."""
        return self.bar_data.get("high", 0.0)
    
    @property
    def low_price(self) -> float:
        """Get low price from bar_data."""
        return self.bar_data.get("low", 0.0)
    
    @property
    def close_price(self) -> float:
        """Get close price from bar_data."""
        return self.bar_data.get("close", 0.0)
    
    @property
    def volume(self) -> float:
        """Get volume from bar_data."""
        return self.bar_data.get("volume", 0.0)
    
    @property
    def bar_start_time(self) -> Optional[int]:
        """Get bar start time from bar_data."""
        return self.bar_data.get("bar_start_time", self.timestamp)

@dataclass
class OrderEvent(Event):
    """Event for order updates."""

    order_id: str = None
    symbol: str = None
    exchange: str = Exchange.NSE
    side: OrderSide = OrderSide.BUY
    quantity: float = 0
    order_type: OrderType = OrderType.LIMIT
    status: OrderStatus = OrderStatus.OPEN
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    filled_quantity: float = 0.0
    remaining_quantity: Optional[float] = None
    average_price: Optional[float] = None
    order_time: Optional[int] = None
    last_update_time: Optional[int] = None
    strategy_id: Optional[str] = None
    client_order_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None
    # For backward compatibility with systems that use 'action' instead of 'side'
    action: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.ORDER
        if self.remaining_quantity is None:
            self.remaining_quantity = self.quantity - self.filled_quantity

        # Standardize between side and action (action is deprecated)
        self._standardize_side_and_action()

        # Ensure order_id is not None
        if self.order_id is None:
            self.order_id = str(uuid.uuid4())

        # Convert order_type if it's a string
        if isinstance(self.order_type, str):
            try:
                self.order_type = OrderType(self.order_type)
            except (ValueError, TypeError):
                pass

        # Convert status if it's a string
        if isinstance(self.status, str):
            try:
                self.status = OrderStatus(self.status)
            except (ValueError, TypeError):
                pass

        # Set order_time if not provided
        if self.order_time is None:
            self.order_time = self.timestamp

        # Set last_update_time if not provided
        if self.last_update_time is None:
            self.last_update_time = self.timestamp

    def _standardize_side_and_action(self):
        """Standardize between side and action for backward compatibility"""
        if self.action is None and self.side is not None:
            # Convert side to string action
            self.action = self.side.value if isinstance(self.side, OrderSide) else self.side
        elif self.side is None and self.action is not None:
            # Convert action to OrderSide enum
            if isinstance(self.action, str):
                try:
                    self.side = OrderSide(self.action)
                except (ValueError, TypeError):
                    self.side = self.action
            else:
                self.side = self.action

        # Ensure side is an OrderSide enum when possible
        if isinstance(self.side, str):
            try:
                self.side = OrderSide(self.side)
            except (ValueError, TypeError):
                pass

    def validate(self) -> bool:
        """
        Validate that the order event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["order_id", "symbol", "side", "quantity", "order_type", "status"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert order event to dictionary."""
        result = super().to_dict()
        result.update({
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "action": self.action if self.action else (self.side.value if isinstance(self.side, OrderSide) else self.side),
            "quantity": self.quantity,
            "order_type": self.order_type.value if isinstance(self.order_type, OrderType) else self.order_type,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "average_price": self.average_price,
            "order_time": self.order_time,
            "last_update_time": self.last_update_time,
            "strategy_id": self.strategy_id,
            "client_order_id": self.client_order_id,
            "broker_order_id": self.broker_order_id,
            "rejection_reason": self.rejection_reason
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrderEvent':
        """Create order event from dictionary."""
        # Standardize between side and action
        side = data.get("side")
        action = data.get("action")

        if side is None and action is not None:
            side = action

        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            order_id=data["order_id"],
            symbol=data["symbol"],
            exchange=data["exchange"],
            side=OrderSide(side) if side else None,
            action=action,
            quantity=data["quantity"],
            order_type=OrderType(data["order_type"]) if "order_type" in data else None,
            status=OrderStatus(data["status"]) if "status" in data else None,
            price=data.get("price"),
            trigger_price=data.get("trigger_price"),
            filled_quantity=data.get("filled_quantity", 0.0),
            remaining_quantity=data.get("remaining_quantity"),
            average_price=data.get("average_price"),
            order_time=data.get("order_time"),
            last_update_time=data.get("last_update_time"),
            strategy_id=data.get("strategy_id"),
            client_order_id=data.get("client_order_id"),
            broker_order_id=data.get("broker_order_id"),
            rejection_reason=data.get("rejection_reason")
        )

@dataclass
class ExecutionEvent(OrderEvent):
    """
    Event representing an execution update for an order.

    ExecutionEvent is an intermediate step between OrderEvent and FillEvent.
    It represents an execution report from the broker or exchange about the
    status of an order, which may be partially filled, filled, rejected, or
    in some other state.
    """
    # Additional fields specific to executions
    execution_id: Optional[str] = None
    execution_time: Optional[int] = None
    last_filled_quantity: Optional[float] = None
    last_filled_price: Optional[float] = None
    average_filled_price: Optional[float] = None
    leaves_quantity: Optional[float] = None  # Quantity remaining to be filled
    cumulative_filled_quantity: Optional[float] = None
    commission: Optional[float] = None
    execution_type: Optional[str] = None  # NEW, PARTIAL, CANCELED, REJECTED, etc.
    text: Optional[str] = None  # Additional execution info or error message

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.EXECUTION

        # Default execution_id if not provided
        if self.execution_id is None:
            self.execution_id = str(uuid.uuid4())

        # Default execution_time if not provided
        if self.execution_time is None:
            self.execution_time = self.timestamp

        # Calculate leaves_quantity if not provided
        if self.leaves_quantity is None and self.quantity is not None and self.cumulative_filled_quantity is not None:
            self.leaves_quantity = self.quantity - self.cumulative_filled_quantity

        # Set cumulative_filled_quantity if not provided
        if self.cumulative_filled_quantity is None:
            self.cumulative_filled_quantity = self.filled_quantity

    def validate(self) -> bool:
        """
        Validate that the execution event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["order_id", "symbol", "status", "execution_id"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution event to dictionary."""
        result = super().to_dict()
        result.update({
            "execution_id": self.execution_id,
            "execution_time": self.execution_time,
            "last_filled_quantity": self.last_filled_quantity,
            "last_filled_price": self.last_filled_price,
            "average_filled_price": self.average_filled_price,
            "leaves_quantity": self.leaves_quantity,
            "cumulative_filled_quantity": self.cumulative_filled_quantity,
            "commission": self.commission,
            "execution_type": self.execution_type,
            "text": self.text
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionEvent':
        """Create execution event from dictionary."""
        # Create base OrderEvent
        order_event_data = data.copy()

        # Extract execution-specific fields
        execution_specific = {
            "execution_id": data.get("execution_id"),
            "execution_time": data.get("execution_time"),
            "last_filled_quantity": data.get("last_filled_quantity"),
            "last_filled_price": data.get("last_filled_price"),
            "average_filled_price": data.get("average_filled_price"),
            "leaves_quantity": data.get("leaves_quantity"),
            "cumulative_filled_quantity": data.get("cumulative_filled_quantity"),
            "commission": data.get("commission"),
            "execution_type": data.get("execution_type"),
            "text": data.get("text")
        }

        # Create OrderEvent object first
        order_event = OrderEvent.from_dict(order_event_data)

        # Convert to dict and update with execution fields
        order_dict = asdict(order_event)
        order_dict.update(execution_specific)

        # Set the event type to EXECUTION
        order_dict["event_type"] = EventType.EXECUTION

        # Create ExecutionEvent using the combined dictionary
        return cls(**order_dict)

@dataclass
class FillEvent(Event):
    """Event for order fill updates."""

    order_id: str = None
    symbol: str = None
    exchange: str = None
    side: OrderSide = OrderSide.BUY
    quantity: float = 0.0
    price: float = 0.0
    commission: float = 0.0
    fill_time: Optional[int] = None
    strategy_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    fill_id: Optional[str] = None
    # For backward compatibility
    action: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.FILL
        if self.fill_id is None:
            self.fill_id = str(uuid.uuid4())
        if self.fill_time is None:
            self.fill_time = self.timestamp

        # Standardize between side and action
        self._standardize_side_and_action()

    def _standardize_side_and_action(self):
        """Standardize between side and action for backward compatibility"""
        if self.action is None and self.side is not None:
            # Convert side to string action
            self.action = self.side.value if isinstance(self.side, OrderSide) else self.side
        elif self.side is None and self.action is not None:
            # Convert action to OrderSide enum
            if isinstance(self.action, str):
                try:
                    self.side = OrderSide(self.action)
                except (ValueError, TypeError):
                    self.side = self.action
            else:
                self.side = self.action

        # Ensure side is an OrderSide enum when possible
        if isinstance(self.side, str):
            try:
                self.side = OrderSide(self.side)
            except (ValueError, TypeError):
                pass

    def validate(self) -> bool:
        """
        Validate that the fill event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["order_id", "symbol", "side", "quantity", "price", "fill_id"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert fill event to dictionary."""
        result = super().to_dict()
        result.update({
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "action": self.action if self.action else (self.side.value if isinstance(self.side, OrderSide) else self.side),
            "quantity": self.quantity,
            "price": self.price,
            "commission": self.commission,
            "fill_time": self.fill_time,
            "strategy_id": self.strategy_id,
            "broker_order_id": self.broker_order_id
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FillEvent':
        """Create fill event from dictionary."""
        # Standardize between side and action
        side = data.get("side")
        action = data.get("action")

        if side is None and action is not None:
            side = action

        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            fill_id=data.get("fill_id"),
            order_id=data["order_id"],
            symbol=data["symbol"],
            exchange=data["exchange"],
            side=OrderSide(side) if side else None,
            action=action,
            quantity=data["quantity"],
            price=data["price"],
            commission=data.get("commission", 0.0),
            fill_time=data.get("fill_time"),
            strategy_id=data.get("strategy_id"),
            broker_order_id=data.get("broker_order_id")
        )

@dataclass
class SignalEvent(Event):
    """Event for trading signals generated by strategies."""

    symbol: str = None
    exchange: str = None
    signal_type: SignalType = SignalType.ENTRY
    signal_price: float = 0.0
    signal_time: int = None
    strategy_id: str = None
    side: Optional[OrderSide] = None
    quantity: Optional[float] = None
    order_type: Optional[OrderType] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    expiry: Optional[int] = None
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.SIGNAL
        if self.metadata is None:
            self.metadata = {}
        if self.signal_time is None:
            self.signal_time = self.timestamp
        # Standardize side
        if isinstance(self.side, str):
            try:
                self.side = OrderSide(self.side)
            except (ValueError, TypeError):
                pass

        # If price is not set but signal_price is, use signal_price
        if self.price is None and self.signal_price is not None:
            self.price = self.signal_price

    def validate(self) -> bool:
        """
        Validate that the signal event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["symbol", "signal_type", "signal_price", "signal_time", "strategy_id", "side"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert signal event to dictionary."""
        result = super().to_dict()
        result.update({
            "symbol": self.symbol,
            "exchange": self.exchange,
            "signal_type": self.signal_type.value if hasattr(self.signal_type, 'value') else self.signal_type,
            "signal_price": self.signal_price,
            "signal_time": self.signal_time,
            "strategy_id": self.strategy_id,
            "side": self.side.value if hasattr(self.side, 'value') else self.side,
            "quantity": self.quantity,
            "order_type": self.order_type.value if hasattr(self.order_type, 'value') else self.order_type,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "expiry": self.expiry,
            "confidence": self.confidence,
            "metadata": self.metadata
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SignalEvent':
        """Create signal event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            symbol=data["symbol"],
            exchange=data["exchange"],
            signal_type=SignalType(data["signal_type"]),
            signal_price=data["signal_price"],
            signal_time=data["signal_time"],
            strategy_id=data["strategy_id"],
            side=OrderSide(data["side"]) if data.get("side") else None,
            quantity=data.get("quantity"),
            order_type=OrderType(data["order_type"]) if data.get("order_type") else None,
            price=data.get("price"),
            trigger_price=data.get("trigger_price"),
            expiry=data.get("expiry"),
            confidence=data.get("confidence"),
            metadata=data.get("metadata", {})
        )

@dataclass
class TradeEvent(Event):
    """Event for trade executions."""

    trade_id: str = None
    order_id: str = None
    symbol: str = None
    exchange: str = None
    side: OrderSide = OrderSide.BUY
    quantity: float = 0.0
    price: float = 0.0
    timestamp: int = 0
    commission: float = 0.0
    strategy_id: Optional[str] = None
    broker_trade_id: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.TRADE
        if self.trade_id is None:
            self.trade_id = str(uuid.uuid4())

    def validate(self) -> bool:
        """
        Validate that the trade event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["trade_id", "order_id", "symbol", "side", "quantity", "price"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert trade event to dictionary."""
        result = super().to_dict()
        result.update({
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "quantity": self.quantity,
            "price": self.price,
            "commission": self.commission,
            "strategy_id": self.strategy_id,
            "broker_trade_id": self.broker_trade_id
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TradeEvent':
        """Create trade event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            trade_id=data["trade_id"],
            order_id=data["order_id"],
            symbol=data["symbol"],
            exchange=data["exchange"],
            side=OrderSide(data["side"]) if isinstance(data["side"], str) else data["side"],
            quantity=data["quantity"],
            price=data["price"],
            commission=data.get("commission", 0.0),
            strategy_id=data.get("strategy_id"),
            broker_trade_id=data.get("broker_trade_id")
        )

@dataclass
class PositionEvent(Event):
    """Event for position updates."""

    symbol: str = None
    exchange: str = None
    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    strategy_id: Optional[str] = None
    # Optional field to carry details of the trade that closed/reduced the position
    closing_trade_details: Optional[Dict] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.POSITION
        if self.closing_trade_details is None:
            self.closing_trade_details = {}

    def validate(self) -> bool:
        """
        Validate that the position event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["symbol", "quantity", "average_price"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert position event to dictionary."""
        result = super().to_dict()
        result.update({
            "symbol": self.symbol,
            "exchange": self.exchange,
            "quantity": self.quantity,
            "average_price": self.average_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "strategy_id": self.strategy_id,
            "closing_trade_details": self.closing_trade_details
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PositionEvent':
        """Create position event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            symbol=data["symbol"],
            exchange=data["exchange"],
            quantity=data["quantity"],
            average_price=data["average_price"],
            realized_pnl=data.get("realized_pnl", 0.0),
            unrealized_pnl=data.get("unrealized_pnl", 0.0),
            strategy_id=data.get("strategy_id"),
            closing_trade_details=data.get("closing_trade_details")
        )

@dataclass
class AccountEvent(Event):
    """Event for account updates with portfolio state information."""

    balance: float = 0.0  # Cash balance
    equity: float = 0.0  # Total portfolio value (cash + positions)
    positions_value: float = 0.0  # Value of all positions
    margin_available: float = 0.0  # Available margin for trading
    margin_used: float = 0.0  # Margin currently in use
    highest_equity: float = 0.0  # Highest equity value reached
    drawdown: float = 0.0  # Current drawdown from peak
    return_pct: float = 0.0  # Return percentage from starting equity
    realized_pnl: float = 0.0  # Realized profit and loss
    unrealized_pnl: float = 0.0  # Unrealized profit and loss
    account_id: Optional[str] = None  # Account identifier

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.ACCOUNT

    def validate(self) -> bool:
        """
        Validate that the account event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["balance", "equity"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert account event to dictionary."""
        result = super().to_dict()
        result.update({
            "balance": self.balance,
            "equity": self.equity,
            "positions_value": self.positions_value,
            "margin_available": self.margin_available,
            "margin_used": self.margin_used,
            "highest_equity": self.highest_equity,
            "drawdown": self.drawdown,
            "return_pct": self.return_pct,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "account_id": self.account_id
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountEvent':
        """Create account event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            balance=data.get("balance", 0.0),
            equity=data.get("equity", 0.0),
            positions_value=data.get("positions_value", 0.0),
            margin_available=data.get("margin_available", 0.0),
            margin_used=data.get("margin_used", 0.0),
            highest_equity=data.get("highest_equity", 0.0),
            drawdown=data.get("drawdown", 0.0),
            return_pct=data.get("return_pct", 0.0),
            realized_pnl=data.get("realized_pnl", 0.0),
            unrealized_pnl=data.get("unrealized_pnl", 0.0),
            account_id=data.get("account_id")
        )

@dataclass
class StrategyEvent(Event):
    """Event for strategy status updates."""

    strategy_id: str = None
    strategy_name: str = None
    status: str = None
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.STRATEGY
        if self.data is None:
            self.data = {}

    def validate(self) -> bool:
        """
        Validate that the strategy event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["strategy_id", "status"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy event to dictionary."""
        result = super().to_dict()
        result.update({
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "status": self.status,
            "message": self.message,
            "data": self.data
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StrategyEvent':
        """Create strategy event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            strategy_id=data["strategy_id"],
            strategy_name=data["strategy_name"],
            status=data["status"],
            message=data.get("message"),
            data=data.get("data", {})
        )

@dataclass
class SystemEvent(Event):
    """Event for system-level updates."""

    system_event_type: str = None
    message: str = None
    severity: str = "INFO"
    data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.SYSTEM
        if self.data is None:
            self.data = {}

    def validate(self) -> bool:
        """
        Validate that the system event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["system_event_type", "message"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert system event to dictionary."""
        result = super().to_dict()
        result.update({
            "system_event_type": self.system_event_type,
            "message": self.message,
            "severity": self.severity,
            "data": self.data
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemEvent':
        """Create system event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            system_event_type=data["system_event_type"],
            message=data["message"],
            severity=data.get("severity", "INFO"),
            data=data.get("data", {})
        )

@dataclass
class TimerEvent(Event):
    """Event for timer-based scheduling and timing operations."""

    timer_id: str = None
    timer_type: str = None
    interval: float = 0.0  # Interval in seconds
    callback: Optional[Callable] = None
    data: Optional[Dict[str, Any]] = None
    repeat: bool = False
    next_execution: Optional[int] = None  # Timestamp for next execution

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.TIMER
        if self.timer_id is None:
            self.timer_id = str(uuid.uuid4())
        if self.data is None:
            self.data = {}
        if self.next_execution is None:
            self.next_execution = self.timestamp

    def validate(self) -> bool:
        """
        Validate that the timer event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["timer_id", "timer_type", "interval"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert timer event to dictionary."""
        result = super().to_dict()
        result.update({
            "timer_id": self.timer_id,
            "timer_type": self.timer_type,
            "interval": self.interval,
            "repeat": self.repeat,
            "next_execution": self.next_execution,
            "data": self.data
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimerEvent':
        """Create timer event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            timer_id=data["timer_id"],
            timer_type=data["timer_type"],
            interval=data["interval"],
            repeat=data.get("repeat", False),
            next_execution=data.get("next_execution"),
            data=data.get("data", {})
        )

@dataclass
class RiskBreachEvent(Event):
    """Event for risk management breaches."""

    risk_type: str = None  # Type of risk breach (e.g., "POSITION_LIMIT", "DRAWDOWN_LIMIT")
    symbol: Optional[str] = None  # Symbol related to the breach, if applicable
    strategy_id: Optional[str] = None  # Strategy related to the breach, if applicable
    action: Optional[str] = None  # Suggested action (e.g., "exit_position", "reduce_position")
    position: Optional[float] = None  # Current position size, if applicable
    threshold: Optional[float] = None  # Threshold that was breached
    current_value: Optional[float] = None  # Current value that triggered the breach
    message: Optional[str] = None  # Human-readable description of the breach
    data: Optional[Dict[str, Any]] = None  # Additional data related to the breach

    def __post_init__(self):
        super().__post_init__()
        if self.event_type is None:
            self.event_type = EventType.RISK_BREACH
        if self.data is None:
            self.data = {}

    def validate(self) -> bool:
        """
        Validate that the risk breach event has all required fields.

        Returns:
            bool: True if valid, raises exception otherwise
        """
        super().validate()
        required_fields = ["risk_type"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert risk breach event to dictionary."""
        result = super().to_dict()
        result.update({
            "risk_type": self.risk_type,
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
            "action": self.action,
            "position": self.position,
            "threshold": self.threshold,
            "current_value": self.current_value,
            "message": self.message,
            "data": self.data
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RiskBreachEvent':
        """Create risk breach event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            risk_type=data["risk_type"],
            symbol=data.get("symbol"),
            strategy_id=data.get("strategy_id"),
            action=data.get("action"),
            position=data.get("position"),
            threshold=data.get("threshold"),
            current_value=data.get("current_value"),
            message=data.get("message"),
            data=data.get("data", {})
        )

# --- Define OptionsSubscribedEvent if not available ---
# Create a placeholder if the actual event class is not accessible
# In a real scenario, this should be in models/events.py
class OptionsSubscribedEvent(Event):
    """Event published when new option symbols have been subscribed."""
    def __init__(self, underlying_symbol: str, instruments: List[Instrument]):
        super().__init__(event_type=EventType.CUSTOM, # Or a dedicated EventType.OPTIONS_SUBSCRIBED
                         timestamp=time.time())
        self.underlying_symbol = underlying_symbol
        self.instruments = instruments # List of Instrument objects subscribed
        # Add a specific sub-type if using EventType.CUSTOM
        self.custom_event_type = "OPTIONS_SUBSCRIBED"

# ------------------------------------------------------
