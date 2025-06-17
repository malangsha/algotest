import json
import uuid
import logging
import time
from collections import deque
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Set, List, Callable
from enum import Enum

from utils.constants import (Exchange, OrderType, OrderSide, 
                             OrderStatus, SignalType, EventType, 
                             MarketDataType, EventPriority, EventSource,
                             BufferWindowState)
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
    for field_name in required_fields:
        if not hasattr(event, field_name) or getattr(event, field_name) is None:
            missing_fields.append(field_name)

    if missing_fields:
        error_msg = f"Missing required fields in {event.__class__.__name__}: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise EventValidationError(error_msg)

    return True

@dataclass
class Event:
    """Base class for all events in the system."""

    event_type: EventType
    timestamp: float # Changed to float for consistency with time.time()
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: EventPriority = EventPriority.NORMAL

    def __post_init__(self):
        if self.timestamp is None: # Should not happen if default_factory is used for timestamp too
            self.timestamp = time.time()

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
            priority=EventPriority(data["priority"]) if "priority" in data and data["priority"] is not None else EventPriority.NORMAL
        )

    def __lt__(self, other):
        """Compare events based on priority and timestamp"""
        if not isinstance(other, Event):
            return NotImplemented
        if self.priority != other.priority:
            return self.priority.value < other.priority.value
        return self.timestamp < other.timestamp

@dataclass
class MarketDataEvent(Event):
    """Event for market data updates."""
    instrument: Optional[Instrument] = None
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    data_type: MarketDataType = MarketDataType.QUOTE
    data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        # Ensure event_type is set, super().__post_init__() might not be called if event_type is passed in constructor
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.MARKET_DATA
        if self.instrument and self.exchange is None:
            self.exchange = self.instrument.exchange


    def to_dict(self) -> Dict[str, Any]:
        """Convert market data event to dictionary."""
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.instrument.symbol if self.instrument else None,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "data_type": self.data_type.value,
            "data": self.data
        })
        return result

    def validate(self) -> bool:
        super().validate()
        required_fields = ["instrument", "data_type", "data"]
        # Instrument implies exchange, so check instrument or (symbol and exchange)
        if not self.instrument and not (hasattr(self, 'symbol') and self.symbol and self.exchange):
             raise EventValidationError("MarketDataEvent requires either 'instrument' or both 'symbol' and 'exchange'.")
        return validate_event(self, required_fields)


    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'MarketDataEvent':
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange_val = data.get("exchange")
        exchange_enum = Exchange(exchange_val) if exchange_val else None


        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol and exchange_enum:
                instrument = instrument_registry.get_by_symbol(symbol, exchange_enum)

        if not instrument and (instrument_id or symbol):
            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange_enum
            )
        
        event_type_val = data.get("event_type", EventType.MARKET_DATA.value)


        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            instrument=instrument,
            exchange=exchange_enum,
            data_type=MarketDataType(data["data_type"]),
            data=data.get("data", {})
        )

    @property
    def symbol(self) -> Optional[str]:
        return self.instrument.symbol if self.instrument else None

@dataclass
class BarEvent(Event):
    """Event for timeframe-specific OHLCV bar updates."""
    instrument: Optional[Instrument] = None
    timeframe: str = '1m'
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    volume: float = 0.0
    bar_start_time: Optional[float] = None # Changed to float

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.BAR
        if self.bar_start_time is None:
            self.bar_start_time = self.timestamp

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.instrument.symbol if self.instrument else None,
            "exchange": self.instrument.exchange.value if self.instrument and isinstance(self.instrument.exchange, Exchange) else (self.instrument.exchange if self.instrument else None),
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
        super().validate()
        required_fields = ["instrument", "timeframe", "open_price", "high_price", "low_price", "close_price"]
        if not self.instrument:
             raise EventValidationError("BarEvent requires 'instrument'.")
        return validate_event(self, required_fields)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'BarEvent':
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange_val = data.get("exchange")
        exchange_enum = Exchange(exchange_val) if exchange_val else None

        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol and exchange_enum:
                instrument = instrument_registry.get_by_symbol(symbol, exchange_enum)

        if not instrument and (instrument_id or symbol):
            instrument = Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange_enum
            )
        
        event_type_val = data.get("event_type", EventType.BAR.value)

        return cls(
            event_type=EventType(event_type_val),
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
    def symbol(self) -> Optional[str]:
        return self.instrument.symbol if self.instrument else None

@dataclass
class OrderEvent(Event):
    """Event for order updates."""
    order_id: Optional[str] = None # Made optional, will be set in post_init if None
    symbol: Optional[str] = None
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    side: Optional[OrderSide] = None
    quantity: float = 0.0
    order_type: Optional[OrderType] = None
    status: Optional[OrderStatus] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    filled_quantity: float = 0.0
    remaining_quantity: Optional[float] = None
    average_price: Optional[float] = None
    order_time: Optional[float] = None # Changed to float
    last_update_time: Optional[float] = None # Changed to float
    strategy_id: Optional[str] = None
    client_order_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None
    action: Optional[str] = None
    instrument: Optional[Instrument] = None
    time_in_force: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict) # Added metadata field
    source: EventSource = EventSource.ORDER_MANAGER
    remarks: Optional[str] = None 

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None: # Ensure event_type is set
            self.event_type = EventType.ORDER
        if self.order_id is None:
            self.order_id = str(uuid.uuid4())
        if self.remaining_quantity is None:
            self.remaining_quantity = self.quantity - self.filled_quantity
        if self.order_time is None:
            self.order_time = self.timestamp
        if self.last_update_time is None:
            self.last_update_time = self.timestamp
        if self.remarks is None:
            self.remarks = self.order_id    
        
        self._standardize_enums()
        self._standardize_side_and_action()


    def _standardize_enums(self):
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)
        if isinstance(self.side, str): self.side = OrderSide(self.side.upper())
        if isinstance(self.order_type, str): self.order_type = OrderType(self.order_type.upper())
        if isinstance(self.status, str): self.status = OrderStatus(self.status.upper())


    def _standardize_side_and_action(self):
        if self.action is None and self.side is not None:
            self.action = self.side.value
        elif self.side is None and self.action is not None:
            try:
                self.side = OrderSide(self.action.upper())
            except ValueError:
                logger.warning(f"Could not convert action '{self.action}' to OrderSide for order {self.order_id}")


    def validate(self) -> bool:
        super().validate()
        required_fields = ["order_id", "symbol", "side", "quantity", "order_type", "status", "exchange"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "action": self.action,
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
            "rejection_reason": self.rejection_reason,
            "time_in_force": self.time_in_force,
            "metadata": self.metadata, # Added metadata
            "source": self.source,
            "remarks": self.remarks
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrderEvent':
        side_val = data.get("side") or data.get("action")
        
        event_type_val = data.get("event_type", EventType.ORDER.value)

        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            order_id=data.get("order_id"),
            symbol=data.get("symbol"),
            exchange=Exchange(data["exchange"]) if data.get("exchange") else None,
            side=OrderSide(side_val.upper()) if side_val else None,
            action=data.get("action"), # Keep original action if present
            quantity=data.get("quantity", 0.0),
            order_type=OrderType(data["order_type"].upper()) if data.get("order_type") else None,
            status=OrderStatus(data["status"].upper()) if data.get("status") else None,
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
            rejection_reason=data.get("rejection_reason"),
            time_in_force=data.get("time_in_force"),
            metadata=data.get("metadata", {}), # Added metadata
            source=data.get("source"),
            remarks=data.get("remarks")
        )

@dataclass
class ExecutionEvent(OrderEvent):
    """ Event representing an execution update for an order. """
    execution_id: Optional[str] = None
    execution_time: Optional[float] = None # Changed to float
    last_filled_quantity: Optional[float] = None
    last_filled_price: Optional[float] = None
    # average_filled_price is inherited from OrderEvent
    leaves_quantity: Optional[float] = None
    cumulative_filled_quantity: Optional[float] = None
    commission: Optional[float] = None
    execution_type: Optional[str] = None
    text: Optional[str] = None
    source: EventSource = EventSource.EXECUTION_HANDLER

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or not self.event_type:
            self.event_type = EventType.EXECUTION
        if self.execution_id is None:
            self.execution_id = f"exec-{uuid.uuid4()}"
        if self.execution_time is None:
            self.execution_time = self.timestamp
        if self.cumulative_filled_quantity is None:
             self.cumulative_filled_quantity = self.filled_quantity # from OrderEvent part
        if self.leaves_quantity is None and self.quantity is not None and self.cumulative_filled_quantity is not None:
            self.leaves_quantity = self.quantity - self.cumulative_filled_quantity


    def validate(self) -> bool:
        super().validate() # Validates OrderEvent fields
        required_fields = ["execution_id"] # execution_time is defaulted
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "execution_id": self.execution_id,
            "execution_time": self.execution_time,
            "last_filled_quantity": self.last_filled_quantity,
            "last_filled_price": self.last_filled_price,
            "average_filled_price": self.average_price, # from OrderEvent part
            "leaves_quantity": self.leaves_quantity,
            "cumulative_filled_quantity": self.cumulative_filled_quantity,
            "commission": self.commission,
            "execution_type": self.execution_type,
            "text": self.text,
            "source": self.source            
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionEvent':
        # Create base OrderEvent from the dict
        # Ensure all necessary fields for OrderEvent are present or handled by OrderEvent.from_dict
        base_event_data = {k: v for k, v in data.items() if k not in [
            "execution_id", "execution_time", "last_filled_quantity", 
            "last_filled_price", "leaves_quantity", "cumulative_filled_quantity", 
            "commission", "execution_type", "text"]}
        
        # Explicitly set event_type for base class if it's coming as EXECUTION
        base_event_data['event_type'] = EventType.ORDER.value # Temporarily set for OrderEvent constructor

        order_event_part = OrderEvent.from_dict(base_event_data)

        # Now create ExecutionEvent specific parts
        exec_event = cls(
            **asdict(order_event_part), # Spread OrderEvent fields
            execution_id=data.get("execution_id"),
            execution_time=data.get("execution_time"),
            last_filled_quantity=data.get("last_filled_quantity"),
            last_filled_price=data.get("last_filled_price"),
            average_price=data.get("average_price", order_event_part.average_price), # Use already processed one
            leaves_quantity=data.get("leaves_quantity"),
            cumulative_filled_quantity=data.get("cumulative_filled_quantity", order_event_part.filled_quantity),
            commission=data.get("commission"),
            execution_type=data.get("execution_type"),
            text=data.get("text")
        )
        exec_event.event_type = EventType.EXECUTION # Correct the event type
        return exec_event


@dataclass
class FillEvent(Event):
    """Event for order fill updates."""
    order_id: Optional[str] = None
    symbol: Optional[str] = None # Kept for direct access, though instrument is preferred
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    side: Optional[OrderSide] = None
    quantity: float = 0.0 # This is fill_quantity for this specific fill
    price: float = 0.0 # This is fill_price for this specific fill
    commission: float = 0.0
    fill_time: Optional[float] = None # Changed to float
    strategy_id: Optional[str] = None
    broker_order_id: Optional[str] = None
    fill_id: Optional[str] = None # Unique ID for this specific fill
    action: Optional[str] = None # For backward compatibility
    instrument: Optional[Instrument] = None
    execution_id: Optional[str] = None # Link to the execution report if applicable

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.FILL
        if self.fill_id is None:
            self.fill_id = f"fill-{uuid.uuid4()}"
        if self.fill_time is None:
            self.fill_time = self.timestamp
        if self.instrument and not self.symbol: self.symbol = self.instrument.symbol
        if self.instrument and not self.exchange: self.exchange = self.instrument.exchange

        self._standardize_enums()
        self._standardize_side_and_action()

    def _standardize_enums(self):
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)
        if isinstance(self.side, str): self.side = OrderSide(self.side.upper())

    def _standardize_side_and_action(self):
        if self.action is None and self.side is not None:
            self.action = self.side.value
        elif self.side is None and self.action is not None:
            try:
                self.side = OrderSide(self.action.upper())
            except ValueError:
                 logger.warning(f"Could not convert action '{self.action}' to OrderSide for fill {self.fill_id}")


    def validate(self) -> bool:
        super().validate()
        required_fields = ["order_id", "symbol", "side", "quantity", "price", "fill_id"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "action": self.action,
            "quantity": self.quantity,
            "price": self.price,
            "commission": self.commission,
            "fill_time": self.fill_time,
            "strategy_id": self.strategy_id,
            "broker_order_id": self.broker_order_id,
            "execution_id": self.execution_id
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FillEvent':
        side_val = data.get("side") or data.get("action")
        event_type_val = data.get("event_type", EventType.FILL.value)

        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            fill_id=data.get("fill_id"),
            order_id=data.get("order_id"),
            symbol=data.get("symbol"),
            exchange=Exchange(data["exchange"]) if data.get("exchange") else None,
            side=OrderSide(side_val.upper()) if side_val else None,
            action=data.get("action"),
            quantity=data.get("quantity", 0.0),
            price=data.get("price", 0.0),
            commission=data.get("commission", 0.0),
            fill_time=data.get("fill_time"),
            strategy_id=data.get("strategy_id"),
            broker_order_id=data.get("broker_order_id"),
            execution_id=data.get("execution_id")
        )

@dataclass
class PartialFillEvent(FillEvent):
    """ Event for partial order fill updates. """
    total_order_quantity: float = 0.0  # Total original order quantity
    cumulative_filled_quantity: float = 0.0  # Total filled quantity for the order so far
    remaining_order_quantity: float = 0.0  # Quantity remaining to be filled for the order
    
    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.PARTIAL_FILL # Ensure correct type
            
        if self.total_order_quantity == 0.0 and self.remaining_order_quantity > 0.0:
            self.total_order_quantity = self.quantity + self.remaining_order_quantity # quantity is this fill's qty
            
        if self.cumulative_filled_quantity == 0.0: # If not provided, assume this fill is the first part or only part known
            self.cumulative_filled_quantity = self.quantity
            
        if self.remaining_order_quantity == 0.0 and self.total_order_quantity > 0.0:
            self.remaining_order_quantity = self.total_order_quantity - self.cumulative_filled_quantity
    
    def validate(self) -> bool:
        super().validate()
        required_fields = ["total_order_quantity", "cumulative_filled_quantity", "remaining_order_quantity"]
        return validate_event(self, required_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "total_order_quantity": self.total_order_quantity,
            "cumulative_filled_quantity": self.cumulative_filled_quantity,
            "remaining_order_quantity": self.remaining_order_quantity
        })
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PartialFillEvent':
        base_event_data = {k:v for k,v in data.items() if k not in [
            "total_order_quantity", "cumulative_filled_quantity", "remaining_order_quantity"]}
        base_event_data['event_type'] = EventType.FILL.value # Temp for FillEvent constructor

        fill_event_part = FillEvent.from_dict(base_event_data)
        
        partial_fill_event = cls(
            **asdict(fill_event_part),
            total_order_quantity=data.get("total_order_quantity", 0.0),
            cumulative_filled_quantity=data.get("cumulative_filled_quantity", data.get("quantity",0.0)), # Default to this fill's qty
            remaining_order_quantity=data.get("remaining_order_quantity", 0.0)
        )
        partial_fill_event.event_type = EventType.PARTIAL_FILL # Correct type
        return partial_fill_event


@dataclass
class SignalEvent(Event):
    """Event for trading signals generated by strategies."""
    symbol: Optional[str] = None
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    signal_type: Optional[SignalType] = None
    signal_price: float = 0.0 # Price of underlying at signal time
    signal_time: Optional[float] = None # Changed to float
    strategy_id: Optional[str] = None
    side: Optional[OrderSide] = None
    quantity: Optional[float] = None
    order_type: Optional[OrderType] = None # This should be utils.constants.OrderType
    price: Optional[float] = None # Target price for the order (e.g. limit price for option)
    trigger_price: Optional[float] = None
    expiry: Optional[int] = None # Typically for options, could be date string or offset
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    instrument: Optional[Instrument] = None
    timeframe: Optional[str] = None
    time_in_force: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.SIGNAL
        if self.signal_time is None:
            self.signal_time = self.timestamp
        
        self._standardize_enums()

    def _standardize_enums(self):
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)
        if isinstance(self.signal_type, str): self.signal_type = SignalType(self.signal_type)
        if isinstance(self.side, str): self.side = OrderSide(self.side.upper())
        if isinstance(self.order_type, str): self.order_type = OrderType(self.order_type.upper())


    def validate(self) -> bool:
        super().validate()
        required_fields = ["symbol", "signal_type", "signal_price", "signal_time", "strategy_id", "side", "quantity", "order_type"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "signal_type": self.signal_type.value if isinstance(self.signal_type, SignalType) else self.signal_type,
            "signal_price": self.signal_price,
            "signal_time": self.signal_time,
            "strategy_id": self.strategy_id,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "quantity": self.quantity,
            "order_type": self.order_type.value if isinstance(self.order_type, OrderType) else self.order_type,
            "price": self.price,
            "trigger_price": self.trigger_price,
            "expiry": self.expiry,
            "confidence": self.confidence,
            "metadata": self.metadata,
            "timeframe": self.timeframe,
            "time_in_force": self.time_in_force
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SignalEvent':
        event_type_val = data.get("event_type", EventType.SIGNAL.value)
        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            symbol=data.get("symbol"),
            exchange=Exchange(data["exchange"]) if data.get("exchange") else None,
            signal_type=SignalType(data["signal_type"]) if data.get("signal_type") else None,
            signal_price=data.get("signal_price", 0.0),
            signal_time=data.get("signal_time"),
            strategy_id=data.get("strategy_id"),
            side=OrderSide(data["side"].upper()) if data.get("side") else None,
            quantity=data.get("quantity"),
            order_type=OrderType(data["order_type"].upper()) if data.get("order_type") else None,
            price=data.get("price"),
            trigger_price=data.get("trigger_price"),
            expiry=data.get("expiry"),
            confidence=data.get("confidence"),
            metadata=data.get("metadata", {}),
            timeframe=data.get("timeframe"),
            time_in_force=data.get("time_in_force")
        )

@dataclass
class PositionEvent(Event):
    """Event for position updates."""
    instrument: Optional[Instrument] = None
    quantity: float = 0.0
    avg_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    position_value: Optional[float] = None
    margin_used: Optional[float] = None
    last_update_time: Optional[float] = None # Changed to float
    strategy_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    symbol: Optional[str] = None # For backward compatibility / ease of access
    exchange: Optional[Exchange] = None # For backward compatibility / ease of access

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.POSITION
        if self.last_update_time is None:
            self.last_update_time = self.timestamp
        if self.instrument:
            if self.symbol is None: self.symbol = self.instrument.symbol
            if self.exchange is None: self.exchange = self.instrument.exchange
        if self.position_value is None and self.avg_price is not None: # quantity can be 0
            self.position_value = abs(self.quantity) * self.avg_price
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)


    def validate(self) -> bool:
        super().validate()
        if not self.instrument and not self.symbol:
            raise EventValidationError("PositionEvent requires either 'instrument' or 'symbol'.")
        required_fields = ["quantity", "avg_price"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "quantity": self.quantity,
            "avg_price": self.avg_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "position_value": self.position_value,
            "margin_used": self.margin_used,
            "last_update_time": self.last_update_time,
            "strategy_id": self.strategy_id,
            "metadata": self.metadata
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'PositionEvent':
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange_val = data.get("exchange")
        exchange_enum = Exchange(exchange_val) if exchange_val else None

        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol and exchange_enum:
                instrument = instrument_registry.get_by_symbol(symbol, exchange_enum)

        if not instrument and (instrument_id or symbol):
            instrument = Instrument(instrument_id=instrument_id, symbol=symbol, exchange=exchange_enum)
        
        event_type_val = data.get("event_type", EventType.POSITION.value)

        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            instrument=instrument,
            symbol=symbol, # Keep passed symbol if instrument not found
            exchange=exchange_enum, # Keep passed exchange
            quantity=data.get("quantity",0.0),
            avg_price=data.get("avg_price",0.0),
            unrealized_pnl=data.get("unrealized_pnl", 0.0),
            realized_pnl=data.get("realized_pnl", 0.0),
            position_value=data.get("position_value"),
            margin_used=data.get("margin_used"),
            last_update_time=data.get("last_update_time"),
            strategy_id=data.get("strategy_id"),
            metadata=data.get("metadata", {})
        )

@dataclass
class AccountEvent(Event):
    """Event for account updates."""
    balance: float = 0.0
    equity: float = 0.0
    margin: float = 0.0 # Total margin used
    free_margin: float = 0.0 # Margin available
    margin_level: Optional[float] = None # Margin level percentage
    account_id: Optional[str] = None
    currency: Optional[str] = None
    last_update_time: Optional[float] = None # Changed to float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.ACCOUNT
        if self.last_update_time is None:
            self.last_update_time = self.timestamp
        if self.margin_level is None and self.margin != 0:
            self.margin_level = (self.equity / self.margin) * 100 if self.margin else float('inf')


    def validate(self) -> bool:
        super().validate()
        required_fields = ["balance", "equity", "margin", "free_margin"]
        return validate_event(self, required_fields)

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "balance": self.balance,
            "equity": self.equity,
            "margin": self.margin,
            "free_margin": self.free_margin,
            "margin_level": self.margin_level,
            "account_id": self.account_id,
            "currency": self.currency,
            "last_update_time": self.last_update_time,
            "metadata": self.metadata
        })
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountEvent':
        event_type_val = data.get("event_type", EventType.ACCOUNT.value)
        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            balance=data.get("balance",0.0),
            equity=data.get("equity",0.0),
            margin=data.get("margin",0.0),
            free_margin=data.get("free_margin",0.0),
            margin_level=data.get("margin_level"),
            account_id=data.get("account_id"),
            currency=data.get("currency"),
            last_update_time=data.get("last_update_time"),
            metadata=data.get("metadata", {})
        )

@dataclass
class TimerEvent(Event):
    """Event for timer-based triggers."""
    interval: int = 1000  # Interval in milliseconds
    repeat: bool = False
    timer_id: Optional[str] = None
    callback_name: Optional[str] = None # Store name of callback for identification
    next_trigger_time: Optional[float] = None # Changed to float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.TIMER
        if self.timer_id is None:
            self.timer_id = str(uuid.uuid4())
        if self.next_trigger_time is None:
            self.next_trigger_time = self.timestamp + (self.interval / 1000.0)

    
    def validate(self) -> bool:
        super().validate()
        required_fields = ["interval", "timer_id", "next_trigger_time"]
        return validate_event(self, required_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "interval": self.interval,
            "repeat": self.repeat,
            "timer_id": self.timer_id,
            "callback_name": self.callback_name,
            "next_trigger_time": self.next_trigger_time,
            "metadata": self.metadata
        })
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimerEvent':
        event_type_val = data.get("event_type", EventType.TIMER.value)
        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            interval=data.get("interval", 1000),
            repeat=data.get("repeat", False),
            timer_id=data.get("timer_id"),
            callback_name=data.get("callback_name"),
            next_trigger_time=data.get("next_trigger_time"),
            metadata=data.get("metadata", {})
        )

@dataclass
class SystemEvent(Event):
    """Event for system-level notifications and control."""
    system_action: Optional[str] = None
    message: Optional[str] = None
    severity: str = "INFO"
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.SYSTEM

    def validate(self) -> bool:
        super().validate()
        required_fields = ["system_action"]
        return validate_event(self, required_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "system_action": self.system_action,
            "message": self.message,
            "severity": self.severity,
            "source": self.source,
            "metadata": self.metadata
        })
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemEvent':
        event_type_val = data.get("event_type", EventType.SYSTEM.value)
        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            system_action=data.get("system_action"),
            message=data.get("message"),
            severity=data.get("severity", "INFO"),
            source=data.get("source"),
            metadata=data.get("metadata", {})
        )

@dataclass
class RiskBreachEvent(Event):
    """Event for risk limit breaches."""
    risk_type: Optional[str] = None
    symbol: Optional[str] = None
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    instrument: Optional[Instrument] = None
    strategy_id: Optional[str] = None
    limit_value: Optional[float] = None
    current_value: Optional[float] = None
    breach_percentage: Optional[float] = None
    message: Optional[str] = None
    severity: str = "WARNING"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        if not hasattr(self, 'event_type') or self.event_type is None:
            self.event_type = EventType.RISK_BREACH
        if self.instrument:
            if self.symbol is None: self.symbol = self.instrument.symbol
            if self.exchange is None: self.exchange = self.instrument.exchange
        if self.breach_percentage is None and self.limit_value is not None and self.current_value is not None and self.limit_value != 0:
            self.breach_percentage = (self.current_value / self.limit_value) * 100
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)

    
    def validate(self) -> bool:
        super().validate()
        required_fields = ["risk_type"]
        return validate_event(self, required_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "risk_type": self.risk_type,
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "strategy_id": self.strategy_id,
            "limit_value": self.limit_value,
            "current_value": self.current_value,
            "breach_percentage": self.breach_percentage,
            "message": self.message,
            "severity": self.severity,
            "metadata": self.metadata
        })
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'RiskBreachEvent':
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange_val = data.get("exchange")
        exchange_enum = Exchange(exchange_val) if exchange_val else None

        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol and exchange_enum:
                instrument = instrument_registry.get_by_symbol(symbol, exchange_enum)

        if not instrument and (instrument_id or symbol):
            instrument = Instrument(instrument_id=instrument_id, symbol=symbol, exchange=exchange_enum)
        
        event_type_val = data.get("event_type", EventType.RISK_BREACH.value)
            
        return cls(
            event_type=EventType(event_type_val),
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            risk_type=data.get("risk_type"),
            symbol=symbol,
            exchange=exchange_enum,
            instrument=instrument,
            strategy_id=data.get("strategy_id"),
            limit_value=data.get("limit_value"),
            current_value=data.get("current_value"),
            breach_percentage=data.get("breach_percentage"),
            message=data.get("message"),
            severity=data.get("severity", "WARNING"),
            metadata=data.get("metadata", {})
        )

@dataclass
class TradeEvent(Event): # Note: EventType does not have TRADE. This might be a custom event.
    """Event for trade updates and trade-related information."""
    trade_id: Optional[str] = None
    symbol: Optional[str] = None
    exchange: Optional[Exchange] = None # Changed to Optional[Exchange]
    instrument: Optional[Instrument] = None
    side: Optional[OrderSide] = None
    quantity: float = 0.0
    price: float = 0.0
    trade_time: Optional[float] = None # Changed to float
    order_id: Optional[str] = None
    strategy_id: Optional[str] = None
    pnl: Optional[float] = None
    commission: Optional[float] = None
    slippage: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        # Assuming EventType.TRADE exists or this is handled as EventType.CUSTOM
        if not hasattr(self, 'event_type') or self.event_type is None:
             # Check if EventType.TRADE exists, otherwise use CUSTOM
            if hasattr(EventType, 'TRADE'):
                self.event_type = EventType.TRADE
            else:
                self.event_type = EventType.CUSTOM 
                self.custom_event_type = "TRADE" # Add a sub-identifier for custom events

        if self.trade_id is None:
            self.trade_id = f"trade-{uuid.uuid4()}"
        if self.trade_time is None:
            self.trade_time = self.timestamp
        if self.instrument:
            if self.symbol is None: self.symbol = self.instrument.symbol
            if self.exchange is None: self.exchange = self.instrument.exchange
        if isinstance(self.exchange, str): self.exchange = Exchange(self.exchange)
        if isinstance(self.side, str): self.side = OrderSide(self.side.upper())

    def validate(self) -> bool:
        super().validate()
        if not self.instrument and not self.symbol:
            raise EventValidationError("TradeEvent requires either 'instrument' or 'symbol'.")
        required_fields = ["trade_id", "side", "quantity", "price"]
        return validate_event(self, required_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result.update({
            "trade_id": self.trade_id,
            "instrument_id": self.instrument.instrument_id if self.instrument else None,
            "symbol": self.symbol,
            "exchange": self.exchange.value if isinstance(self.exchange, Exchange) else self.exchange,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "quantity": self.quantity,
            "price": self.price,
            "trade_time": self.trade_time,
            "order_id": self.order_id,
            "strategy_id": self.strategy_id,
            "pnl": self.pnl,
            "commission": self.commission,
            "slippage": self.slippage,
            "metadata": self.metadata
        })
        if self.event_type == EventType.CUSTOM and hasattr(self, 'custom_event_type'):
            result['custom_event_type'] = self.custom_event_type
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], instrument_registry=None) -> 'TradeEvent':
        instrument_id = data.get("instrument_id")
        symbol = data.get("symbol")
        exchange_val = data.get("exchange")
        exchange_enum = Exchange(exchange_val) if exchange_val else None

        instrument = None
        if instrument_registry:
            if instrument_id:
                instrument = instrument_registry.get_by_id(instrument_id)
            elif symbol and exchange_enum:
                instrument = instrument_registry.get_by_symbol(symbol, exchange_enum)

        if not instrument and (instrument_id or symbol):
            instrument = Instrument(instrument_id=instrument_id, symbol=symbol, exchange=exchange_enum)
            
        side_val = data.get("side")
        side_enum = OrderSide(side_val.upper()) if side_val and isinstance(side_val, str) else side_val
        
        event_type_val = data.get("event_type")
        if event_type_val:
            event_type_enum = EventType(event_type_val)
        elif hasattr(EventType, 'TRADE'):
            event_type_enum = EventType.TRADE
        else:
            event_type_enum = EventType.CUSTOM


        trade_event = cls(
            event_type=event_type_enum,
            timestamp=data["timestamp"],
            event_id=data.get("event_id"),
            trade_id=data.get("trade_id"),
            symbol=symbol,
            exchange=exchange_enum,
            instrument=instrument,
            side=side_enum,
            quantity=data.get("quantity",0.0),
            price=data.get("price",0.0),
            trade_time=data.get("trade_time"),
            order_id=data.get("order_id"),
            strategy_id=data.get("strategy_id"),
            pnl=data.get("pnl"),
            commission=data.get("commission"),
            slippage=data.get("slippage"),
            metadata=data.get("metadata", {})
        )
        if event_type_enum == EventType.CUSTOM and 'custom_event_type' in data:
            trade_event.custom_event_type = data['custom_event_type']
        return trade_event

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

@dataclass
class BufferWindow:
    """Manages buffer window for a specific order"""
    internal_id: str
    state: BufferWindowState = BufferWindowState.NOT_STARTED
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    timeout_duration: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    expected_eh_event: Optional[ExecutionEvent] = None
    buffered_events: deque = field(default_factory=deque)
    processed_broker_ids: Set[str] = field(default_factory=set)
    
    def is_active(self) -> bool:
        return self.state == BufferWindowState.ACTIVE
    
    def is_expired(self) -> bool:
        if self.state != BufferWindowState.ACTIVE or not self.start_time:
            return False
        return datetime.now() > (self.start_time + self.timeout_duration)
    
    def should_buffer_event(self, event: ExecutionEvent) -> bool:
        """Determine if this event should be buffered"""
        if self.state != BufferWindowState.ACTIVE:
            return False
        
        # Only buffer MDF events
        if event.source != EventSource.MARKET_DATA_FEED:
            return False
            
        # Only buffer events with broker_order_id
        if not event.broker_order_id:
            return False
            
        return True
    
    def add_buffered_event(self, event: ExecutionEvent) -> None:
        """Add event to buffer"""
        if self.should_buffer_event(event):
            self.buffered_events.append(event)
    
    def get_buffered_events(self) -> List[ExecutionEvent]:
        """Get all buffered events in order"""
        return list(self.buffered_events)
    
    def clear_buffer(self) -> None:
        """Clear all buffered events"""
        self.buffered_events.clear()
        
@dataclass
class SimulatedOrderUpdateEvent(Event):
    """
    Carries a simulated order update payload from the SimulatedBroker 
    to the SimulatedFeed. This mimics a WebSocket message from a real broker.
    """
    # The payload is a dictionary that looks exactly like a Finvasia order update
    order_payload: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.SIMULATED_ORDER_UPDATE

    def validate(self) -> bool:
        super().validate()
        if not self.order_payload or 'norenordno' not in self.order_payload:
            raise EventValidationError("SimulatedOrderUpdateEvent requires a non-empty 'order_payload' with a 'norenordno'.")
        return True

    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result['order_payload'] = self.order_payload
        return result
