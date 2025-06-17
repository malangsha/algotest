"""
Base class for option trading strategies.

This module provides the foundational architecture for implementing option trading strategies
with standardized interfaces, event handling, and lifecycle management.
"""
import time
import uuid
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Set, Callable
from datetime import datetime
import pandas as pd

from models.events import (
    Event, EventType, MarketDataEvent, SignalEvent,
    BarEvent, PositionEvent, FillEvent,
    OrderEvent, TimerEvent, ExecutionEvent
)

from enum import Enum
from models.order import Order, OrderType
from models.position import Position
from utils.constants import (
    SignalType, OrderSide,
    Timeframe, EventPriority, Exchange
)

from models.instrument import (
    Instrument, InstrumentType, AssetClass
)
from models.events import EventValidationError
from core.logging_manager import get_logger


class StrategyError(Exception):
    """Base exception for strategy-related errors."""
    pass


class StrategyInitializationError(StrategyError):
    """Raised when strategy initialization fails."""
    pass


class StrategyExecutionError(StrategyError):
    """Raised when strategy execution encounters critical errors."""
    pass


class InvalidSignalError(StrategyError):
    """Raised when attempting to generate invalid signals."""
    pass


class StrategyState(Enum):
    """Strategy lifecycle states."""
    INITIALIZED = "initialized"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

class EventHandler:
    """Encapsulates event handling logic to improve separation of concerns."""
    
    def __init__(self, strategy: 'OptionStrategy'):
        self._strategy = strategy
        self._logger = strategy.logger

    def handle_market_data(self, event: Event) -> None:
        """Handle MarketDataEvent with validation and error handling."""
        if not self._validate_market_data_event(event):
            return
            
        self._strategy.last_heartbeat = time.time()
        try:
            self._strategy.on_market_data(event)
        except Exception as e:
            self._logger.error(f"Error in on_market_data: {e}", exc_info=True)
            self._strategy._handle_execution_error(e)

    def handle_position(self, event: Event) -> None:
        """Handle PositionEvent with comprehensive validation."""
        self._logger.debug(f"Received position event: {event}")

        if not self._validate_position_event(event):
            return            
        
        try:
            self._strategy.on_position(event)
        except Exception as e:
            self._logger.error(f"Error handling position event: {e}", exc_info=True)
            self._strategy._handle_execution_error(e)

    def handle_fill(self, event: Event) -> None:
        """Handle FillEvent with validation."""
        self._logger.debug(f"Received fill event: {event}")

        if not self._validate_fill_event(event):
            return
            
        try:
            self._strategy.on_fill(event)
        except Exception as e:
            self._logger.error(f"Error in on_fill: {e}", exc_info=True)
            self._strategy._handle_execution_error(e)

    def handle_order_or_execution(self, event: Union[OrderEvent, ExecutionEvent]) -> None:
        """Handle OrderEvent or ExecutionEvent with validation."""
        # self._logger.debug(f"Received handle_order_or_execution event: {event}")

        if not self._validate_order_event(event):
            return        

        try:
            self._strategy.on_order(event)
        except Exception as e:
            self._logger.error(f"Error in on_order: {e}", exc_info=True)
            self._strategy._handle_execution_error(e)

    def handle_timer(self, event: Event) -> None:
        """Handle TimerEvent with validation."""
        if not isinstance(event, TimerEvent):
            return
            
        try:
            self._strategy.on_timer(event)
        except Exception as e:
            self._logger.error(f"Error in on_timer: {e}", exc_info=True)
            self._strategy._handle_execution_error(e)

    def _validate_market_data_event(self, event: Event) -> bool:
        """Validate MarketDataEvent structure."""
        return (isinstance(event, MarketDataEvent) and 
                event.instrument is not None)

    def _validate_position_event(self, event: Event) -> bool:
        """Validate PositionEvent structure."""
        if not isinstance(event, PositionEvent):
            self._logger.debug(f"Received non-PositionEvent: {type(event)}")
            return False
            
         # Now check the attributes directly:
        if not getattr(event, "instrument", None):
            self._logger.warning("PositionEvent missing instrument")
            return False

        if getattr(event, "quantity", None) is None:
            self._logger.warning("PositionEvent missing quantity")
            return False        
     
        return True

    def _validate_fill_event(self, event: Event) -> bool:
        """Validate FillEvent structure."""
        return (isinstance(event, FillEvent) and 
                hasattr(event, "symbol"))

    def _validate_order_event(self, event: Union[OrderEvent, ExecutionEvent]) -> bool:
        """Validate OrderEvent structure."""
        if not isinstance(event, (OrderEvent, ExecutionEvent)):
            return False

        return True

        # return (isinstance(event, OrderEvent) and 
        #         hasattr(event, "order") and 
        #         event.order is not None and
        #         hasattr(event.order, "instrument_id"))
    
class SignalGenerator:
    """Encapsulates signal generation logic for better separation of concerns."""
    
    def __init__(self, strategy: 'OptionStrategy'):
        self._strategy = strategy
        self._logger = strategy.logger

    def generate(self, 
                instrument_id: str, 
                signal_type: SignalType, 
                data: Optional[Dict[str, Any]] = None, 
                priority: EventPriority = EventPriority.NORMAL) -> bool:
        """
        Generate a trading signal with comprehensive validation.
        
        Args:
            instrument_id: The instrument identifier
            signal_type: Type of signal to generate
            data: Additional signal data
            priority: Signal priority level
            
        Returns:
            bool: True if signal was successfully generated, False otherwise
            
        Raises:
            InvalidSignalError: If signal parameters are invalid
        """
        try:
            self._validate_signal_inputs(instrument_id, signal_type)
            instrument_obj = self._get_validated_instrument(instrument_id)
            data = data or {}
            
            order_side = self._determine_order_side(signal_type)
            order_type_enum = self._parse_order_type(data)
            price = self._calculate_signal_price(data, order_type_enum, instrument_obj)
            quantity = self._calculate_signal_quantity(data, instrument_obj)
            
            signal_event = self._create_signal_event(
                instrument_obj, signal_type, order_side, 
                order_type_enum, quantity, price, data
            )
            
            self._publish_signal(signal_event, priority, data)
            return True
            
        except (InvalidSignalError, EventValidationError) as e:
            self._logger.error(f"Signal generation failed: {e}")
            return False
        except Exception as e:
            self._logger.error(f"Unexpected error in signal generation: {e}", exc_info=True)
            return False

    def _validate_signal_inputs(self, instrument_id: str, signal_type: SignalType) -> None:
        """Validate basic signal inputs."""
        if not instrument_id:
            raise InvalidSignalError("instrument_id is required")
        if not isinstance(signal_type, SignalType):
            raise InvalidSignalError(f"signal_type must be SignalType enum, got: {type(signal_type)}")

    def _get_validated_instrument(self, instrument_id: str) -> Instrument:
        """Get and validate instrument object."""
        instrument_obj = self._strategy.data_manager.get_instrument(instrument_id)
        if not instrument_obj:
            raise InvalidSignalError(f"Instrument not found for id '{instrument_id}'")
        if instrument_obj.exchange is None:
            raise InvalidSignalError(f"Instrument '{instrument_id}' has no exchange information")
        return instrument_obj

    def _determine_order_side(self, signal_type: SignalType) -> OrderSide:
        """Determine order side from signal type."""
        buy_signals = {SignalType.BUY_CALL, SignalType.BUY_PUT, SignalType.BUY}
        sell_signals = {SignalType.SELL_CALL, SignalType.SELL_PUT, SignalType.SELL}
        
        if signal_type in buy_signals:
            return OrderSide.BUY
        elif signal_type in sell_signals:
            return OrderSide.SELL
        else:
            raise InvalidSignalError(f"Cannot determine OrderSide for SignalType {signal_type.value}")

    def _parse_order_type(self, data: Dict[str, Any]) -> OrderType:
        """Parse and validate order type from data."""
        order_type_val = data.get('order_type', OrderType.MARKET.value)
        
        try:
            if isinstance(order_type_val, str):
                return OrderType(order_type_val.upper())
            elif isinstance(order_type_val, OrderType):
                return order_type_val
            else:
                self._logger.warning(f"Invalid order_type '{order_type_val}', defaulting to MARKET")
                return OrderType.MARKET
        except ValueError:
            self._logger.warning(f"Unknown order_type '{order_type_val}', defaulting to MARKET")
            return OrderType.MARKET

    def _calculate_signal_price(self, data: Dict[str, Any], 
                              order_type: OrderType, 
                              instrument: Instrument) -> Optional[float]:
        """Calculate and validate signal price."""
        raw_price = data.get('price', data.get('entry_price_ref'))
        
        if raw_price is not None and raw_price > 0:
            return self._strategy.calculate_valid_price(float(raw_price), instrument.tick_size)
        
        return None if order_type != OrderType.MARKET else 0.0

    def _calculate_signal_quantity(self, data: Dict[str, Any], instrument: Instrument) -> int:
        """Calculate and validate signal quantity."""
        raw_quantity = data.get('quantity', 1)
        quantity = int(raw_quantity * instrument.lot_size)
        
        if quantity <= 0:
            raise InvalidSignalError(f"Invalid quantity calculated: {quantity}")
            
        return quantity

    def _create_signal_event(self, instrument: Instrument, signal_type: SignalType,
                           order_side: OrderSide, order_type: OrderType,
                           quantity: int, price: Optional[float],
                           data: Dict[str, Any]) -> SignalEvent:
        """Create and validate signal event."""
        exchange_val = (instrument.exchange.value 
                       if isinstance(instrument.exchange, Enum) 
                       else str(instrument.exchange))
        
        signal_event = SignalEvent(
            symbol=instrument.instrument_id,
            exchange=exchange_val,
            signal_type=signal_type,
            side=order_side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            strategy_id=self._strategy.id,
            event_type=EventType.SIGNAL,
            timestamp=int(datetime.now().timestamp() * 1000),
            metadata=data
        )
        
        signal_event.validate()
        return signal_event

    def _publish_signal(self, signal_event: SignalEvent, priority: EventPriority,
                       data: Dict[str, Any]) -> None:
        """Publish signal event and update metrics."""
        self._strategy.event_manager.publish(signal_event)
        self._strategy.signals_generated += 1
        
        self._logger.info(
            f"Generated {signal_event.signal_type.value} signal for "
            f"{signal_event.symbol} (Exch: {signal_event.exchange}, "
            f"Side: {signal_event.side.value}, Type: {signal_event.order_type.value}, "
            f"Qty: {signal_event.quantity}, Px: {signal_event.price}). "
            f"Priority: {priority.name}. Data: {data}"
        )

class PositionManager:
    """Manages position-related operations for better encapsulation."""
    
    def __init__(self, strategy: 'OptionStrategy'):
        self._strategy = strategy
        self._positions: Dict[str, Position] = {}

    @property
    def positions(self) -> Dict[str, Position]:
        """Get read-only view of positions."""
        return self._positions.copy()

    def get_position(self, instrument_id: str) -> Optional[Position]:
        """Get position for specific instrument."""
        return self._positions.get(instrument_id)

    def has_position(self, instrument_id: str) -> bool:
        """Check if active position exists for instrument."""
        pos = self.get_position(instrument_id)
        return pos is not None and pos.quantity != 0

    def get_position_side(self, instrument_id: str) -> Optional[str]:
        """Get position side ('long', 'short') or None if flat."""
        position = self.get_position(instrument_id)
        if not position or position.quantity == 0:
            return None
        return 'long' if position.quantity > 0 else 'short'

    def update_position(self, instrument_id: str, position: Position) -> None:
        """Update position in cache."""
        self._positions[instrument_id] = position

    def remove_position(self, instrument_id: str) -> Optional[Position]:
        """Remove position from cache and return it."""
        return self._positions.pop(instrument_id, None)

    def get_positions_count(self) -> int:
        """Get count of active positions."""
        return len(self._positions)

class OptionStrategy(ABC):
    """
    Base Strategy class for option trading strategies.
    
    This class provides a standardized interface and lifecycle management for option 
    trading strategies, implementing the Template Method pattern for strategy execution
    and the Observer pattern for event handling.
    
    The class follows SOLID principles:
    - Single Responsibility: Each component handles specific concerns
    - Open/Closed: Extensible through inheritance and composition
    - Liskov Substitution: Concrete strategies can replace base without breaking behavior
    - Interface Segregation: Clean separation of event handlers and lifecycle methods
    - Dependency Inversion: Depends on abstractions (data_manager, event_manager, etc.)
    """

    def __init__(self, 
                 strategy_id: str, 
                 config: Dict[str, Any],
                 data_manager, 
                 option_manager, 
                 portfolio_manager,
                 event_manager, 
                 broker=None, 
                 strategy_manager=None):
        """
        Initialize the strategy with required dependencies.

        Args:
            strategy_id: Unique identifier for the strategy
            config: Strategy configuration dictionary
            data_manager: Market data manager instance
            option_manager: Option manager for option-specific operations
            portfolio_manager: Portfolio manager for position management
            event_manager: Event manager for event handling
            broker: Optional broker instance
            strategy_manager: Optional strategy manager instance
            
        Raises:
            StrategyInitializationError: If initialization fails
        """
        try:
            self._initialize_core_components(strategy_id, config, data_manager, 
                                           option_manager, portfolio_manager, 
                                           event_manager, broker, strategy_manager)
            self._initialize_state_management()
            self._initialize_configuration()
            self._initialize_timeframes()
            self._initialize_threading()
            self._initialize_performance_tracking()
            self._initialize_symbol_tracking()
            
            # Initialize specialized components
            self._event_handler = EventHandler(self)
            self._signal_generator = SignalGenerator(self)
            self._position_manager = PositionManager(self)  # TODO: we have separate position manager class, so _position_manager can be removed.
            
            self.logger.info(
                f"Strategy \"{self.name}\" ({self.id}) base initialized. "
                f"Primary TF: {self.timeframe}, All TFs: {self.all_timeframes}"
            )
            
        except Exception as e:
            error_msg = f"Failed to initialize strategy {strategy_id}: {e}"
            if hasattr(self, 'logger'):
                self.logger.error(error_msg, exc_info=True)
            raise StrategyInitializationError(error_msg) from e

    def _initialize_core_components(self, strategy_id: str, config: Dict[str, Any],
                                   data_manager, option_manager, portfolio_manager,
                                   event_manager, broker, strategy_manager) -> None:
        """Initialize core strategy components."""
        self.logger = get_logger(f"strategies.{self.__class__.__name__}.{strategy_id}")
        self.id = strategy_id
        self.config = config
        self.data_manager = data_manager
        self.option_manager = option_manager
        self.portfolio_manager = portfolio_manager
        self.event_manager = event_manager
        self.broker = broker
        self.strategy_manager = strategy_manager

    def _initialize_state_management(self) -> None:
        """Initialize strategy state tracking."""
        self._state = StrategyState.INITIALIZED
        self.is_running = False
        self.last_run_time = None
        self.last_heartbeat = time.time()

    def _initialize_configuration(self) -> None:
        """Initialize strategy configuration parameters."""
        self.name = self.config.get('name', self.__class__.__name__)
        self.description = self.config.get('description', '')
        self.max_bars_to_keep = self.config.get('data', {}).get(
            'max_bars_to_keep', 
            self.config.get('max_bars', 1000)
        )

    def _initialize_timeframes(self) -> None:
        """Initialize timeframe configuration with validation."""
        self.timeframe = str(self.config.get('timeframe', '1m'))
        
        additional_tfs_cfg = self.config.get('additional_timeframes', [])
        if not isinstance(additional_tfs_cfg, list):
            additional_tfs_cfg = [str(additional_tfs_cfg)] if additional_tfs_cfg else []
        else:
            additional_tfs_cfg = [str(tf) for tf in additional_tfs_cfg]

        self.additional_timeframes = set(additional_tfs_cfg)
        self.all_timeframes = {self.timeframe} | self.additional_timeframes
        
        if not self.timeframe:
            self.logger.warning(
                f"Primary timeframe is empty or missing in config. Defaulting to '1m'."
            )
            self.timeframe = '1m'
            self.all_timeframes.add('1m')

    def _initialize_threading(self) -> None:
        """Initialize threading components."""
        self.strategy_thread = None
        self.thread_stop_event = threading.Event()

    def _initialize_performance_tracking(self) -> None:
        """Initialize performance metrics."""
        self.signals_generated = 0
        self.successful_trades = 0
        self.failed_trades = 0

    def _initialize_symbol_tracking(self) -> None:
        """Initialize symbol tracking."""
        self.used_symbols: Set[str] = set()

    @property
    def state(self) -> StrategyState:
        """Get current strategy state."""
        return self._state

    @property
    def positions(self) -> Dict[str, Position]:
        """Get current positions (backward compatibility)."""
        return self._position_manager.positions

    def initialize(self) -> None:
        """
        Initialize strategy-specific components.
        
        Called by StrategyManager after __init__ and event registration.
        Override in concrete strategies for custom initialization logic.
        """
        self.logger.info(f"Strategy \"{self.name}\" ({self.id}) performing custom initialization.")

    def _register_event_handlers(self) -> None:
        """
        Register event handlers with the event manager.
        
        This method is called by StrategyManager to set up event subscriptions.
        Uses the EventHandler component for clean separation of concerns.
        """
        self.logger.info(f"Strategy {self.id}: Registering event handlers.")
        
        # Market data events
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._event_handler.handle_market_data,
            component_name=self.id
        )

        # Position events
        self.event_manager.subscribe(
            EventType.POSITION,
            self._event_handler.handle_position,
            component_name=self.id
        )

        # Fill events
        self.event_manager.subscribe(
            EventType.FILL,
            self._event_handler.handle_fill,
            component_name=self.id
        )

        # Order and execution events
        self.event_manager.subscribe(
            EventType.ORDER,
            self._event_handler.handle_order_or_execution,
            component_name=self.id
        )
        self.event_manager.subscribe(
            EventType.EXECUTION,
            self._event_handler.handle_order_or_execution,
            component_name=self.id
        )
        
        # Timer events (if configured)
        if self.config.get('uses_timer', False):
            self.event_manager.subscribe(
                EventType.TIMER,
                self._event_handler.handle_timer,
                component_name=self.id
            )

        self.logger.info(
            f"Strategy {self.id} event handlers registered "
            f"(Note: BarEvent direct subscription removed from BaseStrategy)."
        )

    def start(self) -> None:
        """
        Start the strategy execution.
        
        Called by StrategyManager to begin strategy operations.
        Implements proper state management and error handling.
        
        Raises:
            StrategyExecutionError: If strategy fails to start
        """
        if self.is_running:
            self.logger.warning(f"Strategy {self.id} is already running.")
            return

        try:
            self._state = StrategyState.STARTING
            self._start_internal_thread_if_configured()
            self._execute_startup_hook()
            
            self.is_running = True
            self._state = StrategyState.RUNNING
            
            self.logger.info(f"Strategy {self.id} started successfully.")
            
        except Exception as e:
            self._state = StrategyState.ERROR
            self.is_running = False
            error_msg = f"Strategy {self.id} failed to start: {e}"
            self.logger.error(error_msg, exc_info=True)
            raise StrategyExecutionError(error_msg) from e

    def _start_internal_thread_if_configured(self) -> None:
        """Start internal execution thread if configured."""
        iteration_interval_cfg = self.config.get("iteration_interval_seconds")
        
        if iteration_interval_cfg is None:
            self.logger.info(
                f"Strategy {self.id} is purely event-driven "
                f"(no iteration_interval_seconds configured)."
            )
            return

        try:
            iteration_interval = float(iteration_interval_cfg)
            if iteration_interval <= 0:
                self.logger.info(
                    f"Strategy {self.id} iteration_interval_seconds is <= 0, "
                    f"internal thread not started. Will be event-driven."
                )
                return

            self.thread_stop_event.clear()
            self.strategy_thread = threading.Thread(
                target=self._strategy_thread_func,
                name=f"Strategy_{self.id}_Thread"
            )
            self.strategy_thread.daemon = True
            self.strategy_thread.start()
            
            self.logger.info(
                f"Strategy {self.id} internal thread started with interval {iteration_interval}s."
            )
            
        except ValueError as e:
            self.logger.error(
                f"Strategy {self.id} has invalid iteration_interval_seconds: "
                f"{iteration_interval_cfg}. Internal thread not started."
            )

    def _execute_startup_hook(self) -> None:
        """Execute strategy-specific startup logic."""
        try:
            self.on_start()
        except Exception as e:
            self.logger.error(f"Error during on_start for strategy {self.id}: {e}", exc_info=True)
            raise

    def stop(self) -> None:
        """
        Stop the strategy execution.
        
        Called by StrategyManager to halt strategy operations.
        Implements graceful shutdown with proper cleanup.
        """
        if not self.is_running:
            self.logger.warning(f"Strategy {self.id} is not running.")
            return

        try:
            self._state = StrategyState.STOPPING
            self.is_running = False
            
            self._stop_internal_thread()
            self._execute_shutdown_hook()
            
            self._state = StrategyState.STOPPED
            self.logger.info(f"Strategy {self.id} stopped successfully.")
            
        except Exception as e:
            self._state = StrategyState.ERROR
            self.logger.error(f"Error during strategy {self.id} shutdown: {e}", exc_info=True)

    def _stop_internal_thread(self) -> None:
        """Stop internal execution thread gracefully."""
        self.thread_stop_event.set()

        if self.strategy_thread and self.strategy_thread.is_alive():
            self.logger.debug(f"Waiting for strategy {self.id} internal thread to join...")
            self.strategy_thread.join(timeout=5.0)
            
            if self.strategy_thread.is_alive():
                self.logger.warning(f"Strategy {self.id} internal thread did not join in time.")
            else:
                self.logger.debug(f"Strategy {self.id} internal thread joined.")
                
        self.strategy_thread = None

    def _execute_shutdown_hook(self) -> None:
        """Execute strategy-specific shutdown logic."""
        try:
            self.on_stop()
        except Exception as e:
            self.logger.error(f"Error during on_stop for strategy {self.id}: {e}", exc_info=True)

    def _strategy_thread_func(self) -> None:
        """
        Strategy's internal periodic execution thread function.
        
        Runs the strategy's run_iteration method periodically if configured.
        Implements proper error handling and graceful shutdown.
        """
        iteration_interval = float(self.config.get("iteration_interval_seconds", 1.0))
        self.logger.info(
            f"Strategy {self.id} internal execution thread started (interval: {iteration_interval}s)."
        )

        while not self.thread_stop_event.wait(iteration_interval):
            if not self.is_running:
                break
                
            try:
                self.run_iteration()
                self.last_run_time = time.time()
            except Exception as e:
                self.logger.error(f"Error in strategy {self.id} run_iteration: {e}", exc_info=True)
                self._handle_execution_error(e)
                
        self.logger.info(f"Strategy {self.id} internal execution thread stopped.")

    def _handle_execution_error(self, error: Exception) -> None:
        """
        Handle execution errors with configurable error handling strategy.
        
        Args:
            error: The exception that occurred during execution
        """
        self.failed_trades += 1
        
        # In production, this could implement more sophisticated error handling
        # such as circuit breakers, error rate limiting, or automatic recovery
        if self.failed_trades > self.config.get('max_consecutive_errors', 10):
            self.logger.critical(
                f"Strategy {self.id} exceeded maximum consecutive errors. "
                f"Consider manual intervention."
            )
            # Could automatically stop strategy or trigger alerts

    def run_iteration(self) -> None:
        """
        Execute a single iteration of the strategy.
        
        This method is called periodically by the strategy's internal thread if configured.
        Strategies that are purely event-driven might not need to implement this.
        Override in concrete strategies for periodic logic.
        """
        self.last_heartbeat = time.time()

    def calculate_valid_price(self, raw_price: float, tick_size: float) -> float:
        """
        Calculate price rounded to nearest valid tick boundary.
        
        Args:
            raw_price: The raw price to be rounded
            tick_size: The minimum price increment
            
        Returns:
            float: Price rounded to valid tick boundary
        """
        if raw_price <= 0 or tick_size <= 0:
            return 0.0
        return round(raw_price / tick_size) * tick_size

    def generate_signal(self, 
                       instrument_id: str, 
                       signal_type: SignalType, 
                       data: Optional[Dict[str, Any]] = None, 
                       priority: EventPriority = EventPriority.NORMAL) -> bool:
        """
        Generate a trading signal using the SignalGenerator component.
        
        Args:
            instrument_id: The instrument identifier
            signal_type: Type of signal to generate
            data: Additional signal data
            priority: Signal priority level
            
        Returns:
            bool: True if signal was successfully generated, False otherwise
        """
        return self._signal_generator.generate(instrument_id, signal_type, data, priority)

    def get_bars(self, 
                instrument_id: str, 
                timeframe: Optional[str] = None, 
                limit: Optional[int] = None,
                start_time: Optional[datetime] = None, 
                end_time: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Retrieve historical bar data for a given instrument and timeframe.
        
        Args:
            instrument_id: The instrument identifier
            timeframe: Timeframe for the bars (defaults to strategy's primary timeframe)
            limit: Maximum number of bars to retrieve
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            
        Returns:
            Optional[pd.DataFrame]: Historical bar data or None if not available
        """
        tf_to_use = timeframe or self.timeframe
        
        if not tf_to_use:
            self.logger.error(
                f"Strategy {self.id}: Cannot get bars for {instrument_id}, "
                f"no timeframe specified or defaulted."
            )
            return None

        try:            
            bars_df = self.data_manager.get_historical_data(
                symbol=instrument_id,
                timeframe=tf_to_use,
                n_bars=limit,
                start_time=start_time,
                end_time=end_time
            )
            
            if bars_df is None or bars_df.empty:
                self.logger.debug(
                    f"No bars available for {instrument_id} on timeframe {tf_to_use}"
                )
                return None
                
            return bars_df
            
        except Exception as e:
            self.logger.error(
                f"Error retrieving bars for {instrument_id} on {tf_to_use}: {e}",
                exc_info=True
            )
            return None

    def get_position(self, instrument_id: str) -> Optional[Position]:
        """
        Get the current position for a specific instrument.
        
        Args:
            instrument_id: The instrument identifier
            
        Returns:
            Optional[Position]: Current position or None if no position exists
        """
        return self._position_manager.get_position(instrument_id)

    def has_position(self, instrument_id: str) -> bool:
        """
        Check if there's an active position for the given instrument.
        
        Args:
            instrument_id: The instrument identifier
            
        Returns:
            bool: True if active position exists, False otherwise
        """
        return self._position_manager.has_position(instrument_id)

    def get_position_side(self, instrument_id: str) -> Optional[str]:
        """
        Get the side of the current position for an instrument.
        
        Args:
            instrument_id: The instrument identifier
            
        Returns:
            Optional[str]: 'long', 'short', or None if no position/flat
        """
        return self._position_manager.get_position_side(instrument_id)

    def request_symbol(self,
                       symbol_name: str,
                       exchange_value: str,
                       instrument_type_value: str = "EQUITY",
                       asset_class_value: str = "EQUITY",
                       timeframes_to_subscribe: Optional[Set[str]] = None,
                       option_details: Optional[Dict[str, Any]] = None
                       ) -> bool:
        """
        Dynamically requests data subscription for a symbol across specified timeframes.
        This method allows a strategy to add new symbols to its watchlist during runtime.
        It communicates with DataManager to ensure the feed is active and the strategy
        is registered for bar data for the specified timeframes.

        Args:
            symbol_name: The trading symbol (e.g., "RELIANCE", "NIFTY23JUL20000CE").
            exchange_value: The exchange code as a string (e.g., "NSE", "NFO").
            instrument_type_value: The type of instrument as a string (e.g., "EQUITY", "OPTION", "INDEX").
            asset_class_value: The asset class as a string (e.g., "EQUITY", "OPTIONS", "INDEX").
            timeframes_to_subscribe: A set of timeframe strings (e.g., {"1m", "5m"}).
                                     If None, uses the strategy's `all_timeframes`.
            option_details: For options, a dictionary with keys like 'option_type',
                            'strike_price', 'expiry_date' (datetime.date object),
                            'underlying_symbol_key'.

        Returns:
            bool: True if all requested subscriptions were successful (or already active), False otherwise.
        """        
        self.logger.info(f"Strategy {self.id}: Requesting dynamic symbol {exchange_value}:{symbol_name} (Type: {instrument_type_value})")

        try:
            exchange_enum = Exchange(exchange_value.upper())
            instrument_type_enum = InstrumentType(instrument_type_value.upper())
            asset_class_enum = AssetClass(asset_class_value.upper())
        except ValueError as e:
            self.logger.error(f"Strategy {self.id}: Invalid enum value in request_symbol for {symbol_name}: {e}")
            return False

        instrument_id = f"{exchange_enum.value}:{symbol_name}"

        instrument_args = {
            "symbol": symbol_name, "exchange": exchange_enum,
            "instrument_type": instrument_type_enum, "asset_class": asset_class_enum,
            "instrument_id": instrument_id
        }

        if instrument_type_enum == InstrumentType.OPTION:
            if not option_details or not all(k in option_details for k in ["option_type", "strike_price", "expiry_date"]):
                self.logger.error(f"Strategy {self.id}: Missing or incomplete option_details for option {symbol_name} in request_symbol.")
                return False
            # Ensure expiry_date is a date object if provided
            if 'expiry_date' in option_details and isinstance(option_details['expiry_date'], str):
                try:
                    option_details['expiry_date'] = datetime.strptime(option_details['expiry_date'], '%Y-%m-%d').date()
                except ValueError:
                     self.logger.error(f"Invalid expiry_date string format for {symbol_name}. Use YYYY-MM-DD.")
                     return False
            instrument_args.update(option_details)

        try:
            instrument = Instrument(**instrument_args)
        except Exception as e:
            self.logger.error(f"Strategy {self.id}: Failed to create Instrument object for {symbol_name}: {e}", exc_info=True)
            return False

        target_tfs = timeframes_to_subscribe if timeframes_to_subscribe is not None else self.all_timeframes
        if not target_tfs:
            self.logger.warning(f"Strategy {self.id}: No timeframes specified or defaulted for dynamic subscription of {instrument_id}.")
            return False

        all_requests_successful = True
        for tf_str in target_tfs:
            if not tf_str: continue
            self.logger.info(f"Strategy {self.id}: Requesting DataManager subscription for {instrument.instrument_id} on timeframe {tf_str}")
            
            success_dm = self.data_manager.subscribe_to_timeframe(instrument=instrument, timeframe=tf_str, strategy_id=self.id)
            
            if success_dm:
                self.used_symbols.add(instrument.instrument_id)
                self.logger.debug(f"Strategy {self.id}: DataManager confirmed subscription for {instrument.instrument_id}@{tf_str}.")
            
                if self.strategy_manager:
                    if hasattr(self.strategy_manager, 'register_dynamic_subscription'):
                        self.strategy_manager.register_dynamic_subscription(
                            strategy_id=self.id,
                            instrument_id=instrument.instrument_id,
                            timeframe=tf_str
                        )
                        self.logger.info(f"Strategy {self.id}: Notified StrategyManager of dynamic subscription for {instrument.instrument_id}@{tf_str}.")
                    else:
                        self.logger.warning(f"Strategy {self.id}: StrategyManager reference found, but it's missing 'register_dynamic_subscription' method.")
                else:
                    self.logger.warning(f"Strategy {self.id}: StrategyManager reference not available. Cannot register dynamic subscription with StrategyManager for {instrument.instrument_id}@{tf_str}.")            
            else:
                self.logger.error(f"Strategy {self.id}: DataManager failed to subscribe to {instrument.instrument_id}@{tf_str}")
                all_requests_successful = False
        
        return all_requests_successful

    # Abstract Event Handler Methods (Template Method Pattern)
    # These methods define the interface that concrete strategies must implement
    
    def on_bar(self, event: BarEvent) -> None:
        """
        Handle bar events (OHLCV data updates).
        
        This method is called when new bar data is available.
        Override in concrete strategies to implement bar-based logic.
        
        Args:
            event: The bar event containing OHLCV data
        """
        pass

    def on_market_data(self, event: MarketDataEvent) -> None:
        """
        Handle market data events (tick updates, order book changes, etc.).
        
        This method is called when market data is updated.
        Override in concrete strategies to implement tick-based logic.
        
        Args:
            event: The market data event
        """
        pass

    def on_position(self, event: PositionEvent) -> None:
        """
        Handle position update events.
        
        This method is called when position information changes.
        Override in concrete strategies to react to position changes.
        
        Args:
            event: The position event containing updated position data
        """
        pass

    def on_fill(self, event: FillEvent) -> None:
        """
        Handle order fill events.
        
        This method is called when an order is filled (fully or partially).
        Override in concrete strategies to react to trade executions.
        
        Args:
            event: The fill event containing execution details
        """
        pass

    def on_order(self, event: OrderEvent) -> None:
        """
        Handle order status events.
        
        This method is called when order status changes (submitted, rejected, etc.).
        Override in concrete strategies to track order lifecycle.
        
        Args:
            event: The order event containing order status information
        """
        pass

    def on_timer(self, event: TimerEvent) -> None:
        """
        Handle timer events for time-based strategy operations.
        
        This method is called on timer intervals if strategy is configured
        to use timer events. Override for time-based logic.
        
        Args:
            event: The timer event containing timing information
        """
        pass

    def on_start(self) -> None:
        """
        Hook called when the strategy starts.
        
        Override in concrete strategies to implement custom startup logic
        such as initializing indicators, subscribing to symbols, etc.
        """
        pass

    def on_stop(self) -> None:
        """
        Hook called when the strategy stops.
        
        Override in concrete strategies to implement custom cleanup logic
        such as closing positions, saving state, etc.
        """
        pass

    # Utility and Information Methods
    
    def _get_instrument_details(self, instrument_id: str) -> Optional[Instrument]:
        """
        Get detailed instrument information.
        
        Internal method to retrieve instrument metadata from data manager.
        
        Args:
            instrument_id: The instrument identifier
            
        Returns:
            Optional[Instrument]: Instrument details or None if not found
        """
        try:
            if hasattr(self.data_manager, 'get_instrument'):
                return self.data_manager.get_instrument(instrument_id)
            else:
                self.logger.warning(
                    f"data_manager does not support get_instrument method"
                )
                return None
                
        except Exception as e:
            self.logger.error(
                f"Error retrieving instrument details for {instrument_id}: {e}",
                exc_info=True
            )
            return None

    def get_status(self) -> Dict[str, Any]:
        """
        Get comprehensive strategy status information.
        
        Returns a dictionary containing current strategy state, performance metrics,
        and operational information useful for monitoring and debugging.
        
        Returns:
            Dict[str, Any]: Strategy status information including:
                - Basic info (id, name, state, running status)
                - Performance metrics (signals generated, trades, errors)
                - Configuration details (timeframes, thread status)
                - Position information
                - Timing information (last run, heartbeat)
        """
        position_count = self._position_manager.get_positions_count()
        
        # Calculate uptime if strategy is running
        uptime_seconds = None
        if self.is_running and self.last_run_time:
            uptime_seconds = time.time() - self.last_run_time

        # Thread status
        thread_status = "not_configured"
        if self.strategy_thread:
            thread_status = "alive" if self.strategy_thread.is_alive() else "dead"

        status = {
            # Basic Information
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "state": self.state.value,
            "is_running": self.is_running,
            
            # Performance Metrics
            "signals_generated": getattr(self, 'signals_generated', 0),
            "successful_trades": getattr(self, 'successful_trades', 0),
            "failed_trades": getattr(self, 'failed_trades', 0),
            
            # Configuration
            "timeframe": self.timeframe,
            "additional_timeframes": list(self.additional_timeframes),
            "all_timeframes": list(self.all_timeframes),
            "max_bars_to_keep": self.max_bars_to_keep,
            
            # Position Information
            "active_positions": position_count,
            "used_symbols": list(getattr(self, 'used_symbols', set())),
            
            # Timing Information
            "last_run_time": self.last_run_time,
            "last_heartbeat": self.last_heartbeat,
            "uptime_seconds": uptime_seconds,
            
            # Thread Information
            "thread_status": thread_status,
            "iteration_interval": self.config.get("iteration_interval_seconds"),
            
            # Health Indicators
            "heartbeat_age_seconds": time.time() - self.last_heartbeat if self.last_heartbeat else None,
            "error_rate": self._calculate_error_rate(),
        }
        
        return status

    def _calculate_error_rate(self) -> Optional[float]:
        """
        Calculate the current error rate for health monitoring.
        
        Returns:
            Optional[float]: Error rate as percentage (0-100) or None if no data
        """
        total_operations = getattr(self, 'signals_generated', 0) + getattr(self, 'successful_trades', 0)
        failed_operations = getattr(self, 'failed_trades', 0)
        
        if total_operations == 0:
            return None
            
        return (failed_operations / total_operations) * 100.0

    def __str__(self) -> str:
        """String representation of the strategy."""
        return f"OptionStrategy(id='{self.id}', name='{self.name}', state='{self.state.value}')"

    def __repr__(self) -> str:
        """Detailed string representation for debugging."""
        return (
            f"OptionStrategy(id='{self.id}', name='{self.name}', "
            f"state='{self.state.value}', running={self.is_running}, "
            f"timeframe='{self.timeframe}', positions={self._position_manager.get_positions_count()})"
        )