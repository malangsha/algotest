"""
Base class for option trading strategies.
"""

import logging
import time
import uuid
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Set
from datetime import datetime

from models.events import Event, EventType, MarketDataEvent, SignalEvent, BarEvent, PositionEvent, FillEvent, OrderEvent, TimerEvent
from models.order import Order
from models.position import Position
from utils.constants import SignalType, Timeframe, EventPriority

class OptionStrategy(ABC):
    """
    Base Strategy class for option trading strategies.
    Provides standardized interface for option strategy implementation.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any], data_manager, option_manager, portfolio_manager, event_manager):
        """
        Initialize the strategy.

        Args:
            strategy_id: Unique identifier for the strategy
            config: Strategy configuration dictionary
            data_manager: Market data manager instance
            option_manager: Option manager for option-specific operations
            portfolio_manager: Portfolio manager for position management
            event_manager: Event manager for event handling
        """
        self.logger = logging.getLogger(f"strategies.{self.__class__.__name__}.{strategy_id}")
        self.id = strategy_id
        self.config = config
        self.data_manager = data_manager
        self.option_manager = option_manager
        self.portfolio_manager = portfolio_manager
        self.event_manager = event_manager

        # Strategy state
        self.is_running = False
        self.last_run_time = None
        self.last_heartbeat = time.time()
        
        # Strategy name and description
        self.name = config.get('name', self.__class__.__name__)
        self.description = config.get('description', '')
        
        # Performance tracking
        self.signals_generated = 0
        self.successful_trades = 0
        self.failed_trades = 0
        
        # Data caching
        self.bar_data = {}  # symbol -> {timeframe -> List[BarEvent]}
        self.option_data = {}  # option_symbol -> Dict
        self.max_bars_to_keep = config.get('max_bars', 1000)
        
        # Symbol tracking
        self.used_symbols = set()  # All symbols used by this strategy
        self.active_subscriptions = set()  # Currently subscribed symbols
        
        # Timeframe settings
        self.timeframe = config.get('timeframe', '1m')
        self.additional_timeframes = set(config.get('additional_timeframes', []))
        self.all_timeframes = {self.timeframe} | self.additional_timeframes
        
        # Position tracking
        self.positions = {}  # symbol -> position info
        
        # Thread for strategy execution
        self.strategy_thread = None
        self.thread_stop_event = threading.Event()
        
        # Register for events
        self._register_event_handlers()
        
        self.logger.info(f"Strategy '{self.name}' ({self.id}) initialized")

    def _register_event_handlers(self):
        """Register event handlers with the event manager."""
        # Market data events
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._handle_market_data,
            component_name=self.id
        )
        
        # Bar events
        self.event_manager.subscribe(
            EventType.BAR,
            self._handle_bar,
            component_name=self.id
        )
        
        # Position events
        self.event_manager.subscribe(
            EventType.POSITION,
            self._handle_position,
            component_name=self.id
        )
        
        # Fill events
        self.event_manager.subscribe(
            EventType.FILL,
            self._handle_fill,
            component_name=self.id
        )
        
        # Order events
        self.event_manager.subscribe(
            EventType.ORDER,
            self._handle_order,
            component_name=self.id
        )
        
        # Timer events (NORMAL priority)
        if self.config.get('uses_timer', False):
            self.event_manager.subscribe(
                EventType.TIMER,
                self._handle_timer,
                component_name=self.id

            )
        
        self.logger.info(f"Strategy {self.id} event handlers registered with appropriate priorities")

    def _handle_market_data(self, event: Event):
        """Handle market data events."""
        if not isinstance(event, MarketDataEvent):
            return
            
        self.logger.debug(f"Received MarketDataEvent: {event}")
        # Update last heartbeat
        self.last_heartbeat = time.time()
            
        # Check if the event is for a symbol we're monitoring
        symbol = event.instrument.symbol
        
        if symbol in self.used_symbols:
            # If this is an option, update option data cache
            if self._is_option_symbol(symbol):
                self._update_option_data(symbol, event.data)
                
            # Process the market data in the strategy
            self.on_market_data(event)

    def _handle_bar(self, event: Event):
        """Handle bar events."""
        if not isinstance(event, BarEvent):
            return
            
        # Update last heartbeat
        self.last_heartbeat = time.time()
            
        # Check if the event is for a symbol and timeframe we're monitoring
        symbol = event.instrument.symbol
        timeframe = event.timeframe
        
        if symbol in self.used_symbols and timeframe in self.all_timeframes:
            # Cache the bar data
            if symbol not in self.bar_data:
                self.bar_data[symbol] = {}
                
            if timeframe not in self.bar_data[symbol]:
                self.bar_data[symbol][timeframe] = []
                
            # Add the new bar and trim if needed
            self.bar_data[symbol][timeframe].append(event)
            if len(self.bar_data[symbol][timeframe]) > self.max_bars_to_keep:
                self.bar_data[symbol][timeframe] = self.bar_data[symbol][timeframe][-self.max_bars_to_keep:]
                
            # Process the bar in the strategy
            self.on_bar(event)

    def _handle_position(self, event: Event):
        """Handle position events."""
        if not isinstance(event, PositionEvent):
            return
            
        # Update positions
        if hasattr(event, 'position'):
            position = event.position
            symbol = position.symbol
            
            if symbol in self.used_symbols:
                self.positions[symbol] = position
                self.on_position(event)

    def _handle_fill(self, event: Event):
        """Handle fill events."""
        if not isinstance(event, FillEvent):
            return
            
        # Check if the fill is for one of our orders
        if hasattr(event, 'order_id') and hasattr(event, 'symbol'):
            symbol = event.symbol
            
            if symbol in self.used_symbols:
                self.on_fill(event)

    def _handle_order(self, event: Event):
        """Handle order events."""
        if not isinstance(event, OrderEvent):
            return
            
        # Check if the order is one of ours
        if hasattr(event, 'order'):
            order = event.order
            symbol = order.symbol
            
            if symbol in self.used_symbols:
                self.on_order(event)

    def _handle_timer(self, event: Event):
        """Handle timer events."""
        if not isinstance(event, TimerEvent):
            return
            
        self.on_timer(event)

    def start(self):
        """
        Start the strategy.
        """
        if self.is_running:
            self.logger.warning(f"Strategy {self.id} is already running")
            return
            
        self.is_running = True
        self.thread_stop_event.clear()
        
        # Create and start the strategy thread
        self.strategy_thread = threading.Thread(
            target=self._strategy_thread_func,
            name=f"Strategy_{self.id}"
        )
        self.strategy_thread.daemon = True
        self.strategy_thread.start()
        
        # Call the strategy's on_start method
        self.on_start()
        
        self.logger.info(f"Strategy {self.id} started")

    def stop(self):
        """
        Stop the strategy.
        """
        if not self.is_running:
            self.logger.warning(f"Strategy {self.id} is not running")
            return
            
        # Set stop flags
        self.is_running = False
        self.thread_stop_event.set()
        
        # Wait for the strategy thread to stop
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.strategy_thread.join(timeout=5.0)
            
        # Call the strategy's on_stop method
        self.on_stop()
        
        self.logger.info(f"Strategy {self.id} stopped")

    def _strategy_thread_func(self):
        """
        Strategy thread function.
        Runs the strategy's run_iteration method periodically.
        """
        self.logger.info(f"Strategy {self.id} thread started")
        
        while not self.thread_stop_event.is_set() and self.is_running:
            try:
                # Run the strategy iteration
                self.run_iteration()
                
                # Update last run time
                self.last_run_time = time.time()
                
                # Sleep for a bit
                time.sleep(1)  # Default sleep time
                
            except Exception as e:
                self.logger.error(f"Error in strategy {self.id} iteration: {e}")
                time.sleep(5)  # Sleep longer on error
                
        self.logger.info(f"Strategy {self.id} thread stopped")

    def run_iteration(self):
        """
        Run a single iteration of the strategy.
        This method is called periodically by the strategy thread.
        """
        # Update heartbeat
        self.last_heartbeat = time.time()
        
        # Default implementation does nothing
        pass

    def generate_signal(self, symbol: str, signal_type: SignalType, data: Dict[str, Any] = None, priority: EventPriority = EventPriority.NORMAL):
        """
        Generate a trading signal with specified priority.

        Args:
            symbol: Symbol to trade
            signal_type: Type of signal (BUY, SELL, etc.)
            data: Additional signal data
            priority: Priority level of the signal
        """
        # Basic signal validation
        if not symbol:
            self.logger.error("Cannot generate signal: symbol is required")
            return
            
        if not signal_type:
            self.logger.error("Cannot generate signal: signal type is required")
            return
            
        # Create signal data
        signal_data = {
            'symbol': symbol,
            'signal_type': signal_type,
            'strategy_id': self.id,
            'timestamp': datetime.now(),
            'data': data or {}
        }
        
        # Create a signal event with specified priority
        signal_event = SignalEvent(
            timestamp=datetime.now(),
            instrument=self.data_manager.get_instrument(symbol),
            signal_type=signal_type,
            strategy_id=self.id,
            data=signal_data,
            priority=priority
        )
        
        # Publish the signal event
        self.event_manager.publish(signal_event)
        
        # Increment signals generated
        self.signals_generated += 1
        
        self.logger.info(f"Generated {signal_type} signal for {symbol} with priority {priority.name}")

    def get_bars(self, symbol: str, timeframe: str = None, limit: int = None) -> List[BarEvent]:
        """
        Get historical bars for a symbol.

        Args:
            symbol: Symbol to get bars for
            timeframe: Timeframe of the bars (default: strategy's primary timeframe)
            limit: Maximum number of bars to return (default: all available)

        Returns:
            List[BarEvent]: List of bar events
        """
        # Use the strategy's primary timeframe if none specified
        if not timeframe:
            timeframe = self.timeframe
            
        # Check if we have cached bars for this symbol and timeframe
        if symbol in self.bar_data and timeframe in self.bar_data[symbol]:
            bars = self.bar_data[symbol][timeframe]
            
            # Apply limit if specified
            if limit and limit > 0:
                return bars[-limit:]
            return bars
            
        # No cached bars available
        return []

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get the current position for a symbol.

        Args:
            symbol: Symbol to get position for

        Returns:
            Optional[Position]: Current position or None if no position
        """
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        """
        Check if there is an open position for a symbol.

        Args:
            symbol: Symbol to check

        Returns:
            bool: True if there is an open position, False otherwise
        """
        position = self.get_position(symbol)
        return position is not None and position.quantity != 0

    def get_position_side(self, symbol: str) -> Optional[str]:
        """
        Get the side of the current position for a symbol.

        Args:
            symbol: Symbol to check

        Returns:
            Optional[str]: 'long', 'short', or None if no position
        """
        position = self.get_position(symbol)
        
        if not position or position.quantity == 0:
            return None
            
        return 'long' if position.quantity > 0 else 'short'

    def request_symbol(self, symbol: str) -> bool:
        """
        Request subscription to a symbol.

        Args:
            symbol: Symbol to subscribe to

        Returns:
            bool: True if subscription was successful, False otherwise
        """
        # Add to used symbols
        self.used_symbols.add(symbol)
        
        # Request subscription through the strategy manager
        # This would typically be done through a strategy manager instance
        # For now, we'll just use the data manager directly
        success = self.data_manager.subscribe(symbol)
        
        if success:
            self.active_subscriptions.add(symbol)
            self.logger.info(f"Subscribed to {symbol}")
        else:
            self.logger.error(f"Failed to subscribe to {symbol}")
            
        return success

    def request_option(self, index_symbol: str, strike: float, option_type: str) -> bool:
        """
        Request subscription to a specific option.

        Args:
            index_symbol: Index symbol
            strike: Strike price
            option_type: Option type ('CE' or 'PE')

        Returns:
            bool: True if subscription was successful, False otherwise
        """
        # This would typically be implemented to use the option manager
        # For now, we'll just return a placeholder
        return False

    def _is_option_symbol(self, symbol: str) -> bool:
        """
        Check if a symbol is an option symbol.

        Args:
            symbol: Symbol to check

        Returns:
            bool: True if symbol is an option, False otherwise
        """
        # Simplified check - in a real implementation, this would be more robust
        return 'CE' in symbol or 'PE' in symbol
    
    def _update_option_data(self, symbol: str, data: Dict[str, Any]):
        """
        Update the cached option data.
        
        Args:
            symbol: Option symbol
            data: Option data
        """
        self.option_data[symbol] = data

    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the strategy.

        Returns:
            Dict[str, Any]: Strategy status information
        """
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'is_running': self.is_running,
            'last_run_time': self.last_run_time,
            'signals_generated': self.signals_generated,
            'successful_trades': self.successful_trades,
            'failed_trades': self.failed_trades,
            'positions': len(self.positions),
            'active_subscriptions': len(self.active_subscriptions)
        }

    def get_required_symbols(self) -> Dict[str, List[str]]:
        """
        Get the list of symbols required by this strategy.
        This is used by the strategy manager to subscribe to the required symbols.

        Returns:
            Dict[str, List[str]]: Dictionary mapping timeframes to lists of required symbols
        """
        # Convert used_symbols to the required format
        symbols_by_timeframe = {}
        
        # Add primary timeframe symbols
        symbols_by_timeframe[self.timeframe] = list(self.used_symbols)
        
        # Add additional timeframe symbols
        for timeframe in self.additional_timeframes:
            symbols_by_timeframe[timeframe] = list(self.used_symbols)
            
        return symbols_by_timeframe

    def get_required_options(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all options required by this strategy.
        This method should be overridden by derived strategies that need options.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping index symbols to lists of option requirements
            Each option requirement is a dict with keys: strike, option_type, expiry_offset
        """
        # Default implementation returns empty dict
        # Should be overridden by concrete strategy implementations
        return {}

    @abstractmethod
    def on_market_data(self, event: MarketDataEvent):
        """
        Process market data events.
        Must be implemented by concrete strategy classes.

        Args:
            event: Market data event
        """
        pass

    def on_bar(self, event: BarEvent):
        """
        Process bar events.
        Override in concrete strategy implementations if needed.

        Args:
            event: Bar event
        """
        pass

    def on_position(self, event: Event):
        """
        Process position events.
        Override in concrete strategy implementations if needed.

        Args:
            event: Position event
        """
        pass

    def on_fill(self, event: Event):
        """
        Process fill events.
        Override in concrete strategy implementations if needed.

        Args:
            event: Fill event
        """
        pass

    def on_order(self, event: Event):
        """
        Process order events.
        Override in concrete strategy implementations if needed.

        Args:
            event: Order event
        """
        pass

    def on_timer(self, event: Event):
        """
        Process timer events.
        Override in concrete strategy implementations if needed.

        Args:
            event: Timer event
        """
        pass

    def on_start(self):
        """
        Called when the strategy is started.
        Override in concrete strategy implementations if needed.
        """
        pass

    def on_stop(self):
        """
        Called when the strategy is stopped.
        Override in concrete strategy implementations if needed.
        """
        pass

    def handle_missing_data(self, symbol: str, required_data_points: int) -> bool:
        """
        Handle cases where we don't have enough historical data.
        
        Args:
            symbol: Symbol with missing data
            required_data_points: Number of data points required
            
        Returns:
            bool: True if we can proceed with the strategy, False if we should wait
        """
        # Check if this is an option symbol
        if self._is_option_symbol(symbol):
            # Use option manager to check if the option has enough data
            return self.option_manager.handle_missing_data(symbol, required_data_points)
            
        # For regular symbols, check bar data
        if symbol in self.bar_data and self.timeframe in self.bar_data[symbol]:
            return len(self.bar_data[symbol][self.timeframe]) >= required_data_points
            
        return False