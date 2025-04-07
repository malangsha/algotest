import logging
import time
import uuid
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Set
from datetime import datetime

from models.order import Order
from models.instrument import Instrument
from models.market_data import MarketData, Quote, Trade
from models.position import Position
from models.events import Event, EventType, MarketDataEvent, SignalEvent, BarEvent
from core.event_manager import EventManager
from utils.constants import SignalType, Timeframe
from utils.timeframe_manager import TimeframeManager

class Strategy(ABC):
    """
    Base Strategy class that all strategies should inherit from.
    Provides standardized interface for strategy implementation and event handling.
    """

    def __init__(self, config: Dict[str, Any], data_manager, portfolio, event_manager):
        """
        Initialize the strategy.

        Args:
            config: Strategy configuration
            data_manager: Market data manager instance
            portfolio: Portfolio manager instance
            event_manager: Event manager for subscribing to events
        """
        self.logger = logging.getLogger(f"strategies.{self.__class__.__name__}")
        self.config = config
        self.data_manager = data_manager
        self.portfolio = portfolio
        self.event_manager = event_manager

        # Basic strategy information
        self.id = config.get('id', f"{self.__class__.__name__}_{uuid.uuid4().hex[:8]}")
        self.name = config.get('name', self.__class__.__name__)
        self.description = config.get('description', '')

        # Strategy state
        self.is_running = False
        self.last_run_time = None
        self.instruments = config.get('instruments', [])
        
        # Timeframe settings
        self.timeframe = config.get('timeframe', 'tick')  # Default to tick data if not specified
        if self.timeframe not in TimeframeManager.VALID_TIMEFRAMES:
            self.logger.warning(f"Invalid timeframe: {self.timeframe}, using 'tick' instead")
            self.timeframe = 'tick'
            
        # Additional timeframes this strategy is interested in
        self.additional_timeframes = set(config.get('additional_timeframes', []))
        for tf in list(self.additional_timeframes):
            if tf not in TimeframeManager.VALID_TIMEFRAMES:
                self.logger.warning(f"Invalid additional timeframe: {tf}, removing")
                self.additional_timeframes.discard(tf)
                
        # Add primary timeframe to the set of all timeframes
        self.all_timeframes = {self.timeframe} | self.additional_timeframes
        
        # Thread for strategy execution
        self.strategy_thread = None
        self.thread_stop_event = threading.Event()
        
        # Data caching for bars
        self.bar_data = {}  # symbol -> {timeframe -> List[BarEvent]}
        self.max_bars_to_keep = config.get('max_bars', 1000)
        
        # Performance tracking
        self.signals_generated = 0
        self.successful_trades = 0
        self.failed_trades = 0
        
        # Position tracking
        self.positions = {}  # symbol -> position info

        # Register for events
        self._register_event_handlers()        

        self.logger.info(f"Strategy '{self.name}' ({self.id}) initialized with timeframe {self.timeframe}")

    def _register_event_handlers(self):
        """Register event handlers with the event manager."""
        # Register for market data events
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._handle_market_data,
            component_name=f"Strategy_{self.id}"
        )

        # Register for tick events - Note: EventType.TICK doesn't exist, using market data instead
        # Removed TICK event subscription as it's not in EventType enum
        
        # Register for bar events
        self.event_manager.subscribe(
            EventType.BAR,
            self._handle_bar,
            component_name=f"Strategy_{self.id}"
        )

        # Register for account events
        self.event_manager.subscribe(
            EventType.ACCOUNT,
            self._handle_account,
            component_name=f"Strategy_{self.id}"
        )

        # Register for position events
        self.event_manager.subscribe(
            EventType.POSITION,
            self._handle_position,
            component_name=f"Strategy_{self.id}"
        )

        # Register for fill events
        self.event_manager.subscribe(
            EventType.FILL,
            self._handle_fill,
            component_name=f"Strategy_{self.id}"
        )

        # Register for order events
        self.event_manager.subscribe(
            EventType.ORDER,
            self._handle_order,
            component_name=f"Strategy_{self.id}"
        )
        
        # Register for timer events if needed
        if self.config.get('uses_timer', False):
            self.event_manager.subscribe(
                EventType.TIMER,
                self._handle_timer,
                component_name=f"Strategy_{self.id}"
            )

        self.logger.info(f"Strategy {self.id} registered for events")
        
    def _subscribe_to_market_data(self):
        """Subscribe to market data for each instrument and timeframe."""
        if not self.data_manager:
            self.logger.warning("No data manager available for market data subscription")
            return
            
        # Subscribe to market data for each instrument
        for instrument in self.instruments:
            symbol = instrument.symbol if hasattr(instrument, 'symbol') else str(instrument)
            
            # Subscribe to the primary timeframe
            self.data_manager.subscribe_to_timeframe(self.id, symbol, self.timeframe)
            
            # Subscribe to additional timeframes if any
            for timeframe in self.additional_timeframes:
                self.data_manager.subscribe_to_timeframe(self.id, symbol, timeframe)
                
        self.logger.info(f"Subscribed to market data for {len(self.instruments)} instruments across {len(self.all_timeframes)} timeframes")

    def _handle_market_data(self, event: Event):
        """
        Handle market data events.

        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent):
            return

        if not self.is_running:
            return

        # Check if the event is for an instrument we're monitoring
        instrument = event.instrument
        symbols = [instr.symbol for instr in self.instruments]

        if instrument.symbol in symbols:
            # Process the market data in derived strategy implementations
            self.on_market_data(event)

    def _handle_tick(self, event: Event):
        """
        Handle tick events.

        Args:
            event: Tick event
        """
        if not self.is_running:
            return

        # Extract symbol
        symbol = None
        if hasattr(event, 'instrument') and hasattr(event.instrument, 'symbol'):
            symbol = event.instrument.symbol
        elif hasattr(event, 'symbol'):
            symbol = event.symbol
            
        if not symbol:
            return
            
        # Check if we're monitoring this symbol
        symbols = [instr.symbol for instr in self.instruments]
        if symbol in symbols:
            # Call the overridable method in the derived class
            self.on_tick(event)

    def _handle_bar(self, event: Event):
        """
        Handle bar events.

        Args:
            event: Bar event
        """
        if not self.is_running:
            return

        if not isinstance(event, BarEvent):
            return
            
        # Extract symbol and timeframe
        instrument = event.instrument
        if not instrument:
            return
            
        symbol = instrument.symbol
        timeframe = event.timeframe
            
        # Check if we're subscribed to this symbol and timeframe
        symbols = [instr.symbol for instr in self.instruments]
        if symbol in symbols and timeframe in self.all_timeframes:
            # Cache the bar data
            if symbol not in self.bar_data:
                self.bar_data[symbol] = {}
                
            if timeframe not in self.bar_data[symbol]:
                self.bar_data[symbol][timeframe] = []
                
            # Add the bar to the cache
            self.bar_data[symbol][timeframe].append(event)
            
            # Trim to max size
            if len(self.bar_data[symbol][timeframe]) > self.max_bars_to_keep:
                self.bar_data[symbol][timeframe] = self.bar_data[symbol][timeframe][-self.max_bars_to_keep:]
                
            # Call the overridable method in the derived class
            self.on_bar(event)

    def _handle_position(self, event: Event):
        """
        Handle position events.

        Args:
            event: Position event
        """
        if not self.is_running:
            return
            
        # Extract basic position information for tracking
        if hasattr(event, 'symbol') and hasattr(event, 'quantity'):
            symbol = event.symbol
            quantity = event.quantity
            
            # Update our internal position tracking
            if hasattr(event, 'average_price'):
                avg_price = event.average_price
            else:
                avg_price = 0
                
            self.positions[symbol] = {
                'quantity': quantity,
                'average_price': avg_price,
                'realized_pnl': getattr(event, 'realized_pnl', 0),
                'unrealized_pnl': getattr(event, 'unrealized_pnl', 0),
                'last_update_time': getattr(event, 'timestamp', int(time.time() * 1000))
            }
        
        # Call the overridable method in the derived class
        self.on_position(event)

    def _handle_fill(self, event: Event):
        """
        Handle fill events.

        Args:
            event: Fill event
        """
        if not self.is_running:
            return
            
        # Track successful orders
        if (hasattr(event, 'order_id') and 
            hasattr(event, 'quantity') and 
            hasattr(event, 'price') and
            getattr(event, 'quantity', 0) > 0):
            
            self.successful_trades += 1
            
        # Call the overridable method in the derived class
        self.on_fill(event)

    def _handle_order(self, event: Event):
        """
        Handle order events.

        Args:
            event: Order event
        """
        if not self.is_running:
            return
            
        # Track failed orders
        if (hasattr(event, 'status') and 
            getattr(event, 'status', None) is not None and
            str(getattr(event, 'status')).upper() == 'REJECTED'):
            
            self.failed_trades += 1
            reason = getattr(event, 'rejection_reason', 'Unknown reason')
            self.logger.warning(f"Order rejected: {reason}")
            
        # Call the overridable method in the derived class
        self.on_order(event)

    def _handle_account(self, event: Event):
        """
        Handle account events.

        Args:
            event: Account event
        """
        if not self.is_running:
            return
            
        # Call the overridable method in the derived class
        self.on_account(event)

    def _handle_timer(self, event: Event):
        """
        Handle timer events.

        Args:
            event: Timer event
        """
        if not self.is_running:
            return
            
        # Call the overridable method in the derived class
        self.on_timer(event)

    def start(self):
        """Start the strategy."""
        if self.is_running:
            self.logger.warning(f"Strategy {self.id} is already running")
            return

        self.is_running = True
        self.last_run_time = datetime.now()
        
        # Subscribe to market data for all instruments and timeframes
        self._subscribe_to_market_data()
        
        # Start the strategy thread
        self.thread_stop_event.clear()
        self.strategy_thread = threading.Thread(
            target=self._strategy_thread_func, 
            name=f"Strategy_{self.id}", 
            daemon=True
        )
        self.strategy_thread.start()
        
        # Call the on_start user method
        self.on_start()
        
        self._publish_strategy_event("RUNNING", "Strategy started")
        self.logger.info(f"Strategy '{self.name}' ({self.id}) started")

    def stop(self):
        """Stop the strategy."""
        if not self.is_running:
            self.logger.warning(f"Strategy {self.id} is not running")
            return

        self.is_running = False
        
        # Signal the strategy thread to stop
        self.thread_stop_event.set()
        
        # Wait for the thread to complete
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.strategy_thread.join(timeout=5.0)
        
        # Unsubscribe from all timeframes
        if self.data_manager:
            self.data_manager.unsubscribe_from_timeframe(self.id)
        
        # Call the on_stop user method
        self.on_stop()
        
        self._publish_strategy_event("STOPPED", "Strategy stopped")
        self.logger.info(f"Strategy '{self.name}' ({self.id}) stopped")
        
    def _strategy_thread_func(self):
        """
        Main function for the strategy thread.
        This is a continuous loop that runs until the strategy is stopped.
        """
        self.logger.info(f"Strategy {self.id} thread started")
        
        try:
            while not self.thread_stop_event.is_set() and self.is_running:
                try:
                    # Run strategy-specific logic
                    self.run_iteration()
                    
                    # Sleep briefly to prevent high CPU usage
                    time.sleep(0.001)
                    
                except Exception as e:
                    self.logger.error(f"Error in strategy {self.id} iteration: {str(e)}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    
                    # Sleep longer after an error
                    time.sleep(1.0)
        finally:
            self.logger.info(f"Strategy {self.id} thread stopped")
            
    def run_iteration(self):
        """
        Run a single iteration of the strategy.
        This method is called repeatedly in the strategy thread.
        Override this in derived classes for custom logic.
        """
        # Default implementation just sleeps
        time.sleep(0.1)

    def _publish_strategy_event(self, status: str, message: str = None, data: Dict[str, Any] = None):
        """
        Publish a strategy event.

        Args:
            status: Status of the strategy
            message: Optional message
            data: Optional data dictionary
        """
        if not self.event_manager:
            return
            
        try:
            from models.events import StrategyEvent
            
            event = StrategyEvent(
                event_type=EventType.STRATEGY,
                timestamp=int(time.time() * 1000),
                strategy_id=self.id,
                strategy_name=self.name,
                status=status,
                message=message,
                data=data if data else {}
            )
            
            self.event_manager.publish(event)
            
        except Exception as e:
            self.logger.error(f"Error publishing strategy event: {str(e)}")

    def generate_signal(self, symbol: str, signal_type: SignalType, data: Dict[str, Any] = None):
        """
        Generate and publish a signal event.

        Args:
            symbol: Symbol for the signal
            signal_type: Type of signal
            data: Additional signal data

        Returns:
            The generated signal event
        """
        if not self.event_manager:
            self.logger.error("No event manager available to publish signal")
            return None
            
        try:
            # Create and validate data dictionary
            signal_data = data or {}
            
            # Add required fields if not present
            if 'side' not in signal_data:
                self.logger.error("Signal must include 'side'")
                return None
                
            # Find the instrument for this symbol
            instrument = None
            for instr in self.instruments:
                if (hasattr(instr, 'symbol') and instr.symbol == symbol) or str(instr) == symbol:
                    instrument = instr
                    break
                    
            if not instrument:
                self.logger.warning(f"No instrument found for symbol {symbol}")
                # Create a basic instrument
                from models.instrument import Instrument
                instrument = Instrument(symbol=symbol)
            
            # Create the signal event    
            from models.events import SignalEvent
            
            signal = SignalEvent(
                event_type=EventType.SIGNAL,
                timestamp=int(time.time() * 1000),
                symbol=symbol,
                exchange=getattr(instrument, 'exchange', None),
                signal_type=signal_type,
                strategy_id=self.id,
                **signal_data
            )
            
            # Publish the signal
            self.event_manager.publish(signal)
            
            # Track statistics
            self.signals_generated += 1
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error generating signal: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
            
    def get_bars(self, symbol: str, timeframe: str = None, limit: int = None) -> List[BarEvent]:
        """
        Get the cached bar data for a symbol and timeframe.
        
        Args:
            symbol: Symbol to get bars for
            timeframe: Timeframe to get bars for (uses strategy default if None)
            limit: Maximum number of bars to return (all if None)
            
        Returns:
            List of BarEvent objects
        """
        # Use the strategy's primary timeframe if none specified
        if timeframe is None:
            timeframe = self.timeframe
            
        # Return empty list if we don't have data for this symbol or timeframe
        if symbol not in self.bar_data or timeframe not in self.bar_data[symbol]:
            return []
            
        # Return all or limited bars
        bars = self.bar_data[symbol][timeframe]
        if limit:
            return bars[-limit:]
        return bars

    def get_position(self, symbol: str) -> Dict[str, Any]:
        """
        Get the current position for a symbol.

        Args:
            symbol: Symbol to get position for

        Returns:
            Dictionary with position information
        """
        return self.positions.get(symbol, {'quantity': 0, 'average_price': 0})

    def has_position(self, symbol: str) -> bool:
        """
        Check if the strategy has a position in the given symbol.

        Args:
            symbol: Symbol to check

        Returns:
            True if the strategy has a position, False otherwise
        """
        position = self.get_position(symbol)
        return abs(position['quantity']) > 0

    def get_position_side(self, symbol: str) -> Optional[str]:
        """
        Get the side of the current position for a symbol.

        Args:
            symbol: Symbol to get position side for

        Returns:
            'LONG' for positive position, 'SHORT' for negative position, None for no position
        """
        position = self.get_position(symbol)
        quantity = position['quantity']

        if quantity > 0:
            return 'LONG'
        elif quantity < 0:
            return 'SHORT'
        else:
            return None

    @abstractmethod
    def on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events.
        This method should be implemented by derived strategy classes.

        Args:
            event: Market data event
        """
        pass

    def on_tick(self, event: Event):
        """
        Handle tick events.
        Default implementation does nothing.

        Args:
            event: Tick event
        """
        pass

    def on_bar(self, event: BarEvent):
        """
        Handle bar events.
        Default implementation does nothing.

        Args:
            event: Bar event
        """
        pass

    def on_position(self, event: Event):
        """
        Handle position events.
        Default implementation does nothing.

        Args:
            event: Position event
        """
        pass

    def on_fill(self, event: Event):
        """
        Handle fill events.
        Default implementation does nothing.

        Args:
            event: Fill event
        """
        pass

    def on_order(self, event: Event):
        """
        Handle order events.
        Default implementation does nothing.

        Args:
            event: Order event
        """
        pass

    def on_account(self, event: Event):
        """
        Handle account events.
        Default implementation does nothing.

        Args:
            event: Account event
        """
        pass

    def on_timer(self, event: Event):
        """
        Handle timer events.
        Default implementation does nothing.

        Args:
            event: Timer event
        """
        pass

    def on_start(self):
        """
        Called when the strategy is started.
        Default implementation does nothing.
        """
        pass

    def on_stop(self):
        """
        Called when the strategy is stopped.
        Default implementation does nothing.
        """
        pass

    def get_state(self) -> Dict[str, Any]:
        """
        Get the current state of the strategy.

        Returns:
            Dictionary containing strategy state
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'is_running': self.is_running,
            'last_run_time': self.last_run_time.isoformat() if self.last_run_time else None,
            'timeframe': self.timeframe,
            'additional_timeframes': list(self.additional_timeframes),
            'instruments': [instr.symbol if hasattr(instr, 'symbol') else str(instr) for instr in self.instruments],
            'signals_generated': self.signals_generated,
            'successful_trades': self.successful_trades,
            'failed_trades': self.failed_trades,
            'positions': self.positions
        }

