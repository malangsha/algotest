"""
Strategy Manager module for managing trading strategies.

This module provides the central component for strategy management including:
- Strategy configuration and loading
- Symbol subscription management
- Strategy lifecycle management
- Data distribution to strategies
"""

import logging
import threading
import time
import yaml
from typing import Dict, List, Set, Any, Optional, Type
from functools import lru_cache
import importlib
import inspect
from pathlib import Path
from datetime import datetime

from core.option_manager import OptionManager
from core.event_manager import EventManager
from models.events import Event, EventType, MarketDataEvent, SignalEvent, BarEvent, SignalType
from strategies.base_strategy import OptionStrategy
from utils.config_loader import load_config
from core.logging_manager import get_logger
from strategies.strategy_factory import StrategyFactory
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument
from models.position import Position
from utils.constants import EventPriority

class StrategyManager:
    """
    Manages strategies and their lifecycle.
    Handles strategy initialization, starting, stopping, and symbol subscriptions.
    """

    def __init__(self, data_manager, event_manager, portfolio_manager, risk_manager, broker, config=None):
        """
        Initialize the StrategyManager.

        Args:
            data_manager: DataManager instance for market data
            event_manager: EventManager for event handling
            portfolio_manager: PortfolioManager for position tracking
            risk_manager: RiskManager for risk checks
            broker: Broker for trading operations
            config: Configuration dictionary
        """
        self.logger = get_logger("core.strategy_manager")
        self.data_manager = data_manager
        self.event_manager = event_manager
        self.portfolio_manager = portfolio_manager
        self.risk_manager = risk_manager
        self.broker = broker
        self.config = config

        # Strategy storage
        self.strategies = {}  # strategy_id -> Strategy instance
        self.strategy_configs = {}  # strategy_id -> config

        # Symbol tracking
        self.strategy_symbols = {}  # strategy_id -> {timeframe -> set(symbols)}
        self.symbol_strategies = {}  # symbol -> {timeframe -> set(strategy_ids)}
        self.strategy_options = {}  # strategy_id -> {index -> list(option_reqs)}

        # Initialize strategy factory
        self.strategy_factory = StrategyFactory()

        # Use the OptionManager instance passed from TradingEngine
        self.option_manager = None  # Will be set by TradingEngine

        # Configuration
        self.underlyings_config = None
        self.main_config = None

        # Underlying price updates
        self._register_event_handlers()

        # Background threads
        self.monitoring_thread = None
        self.stop_event = threading.Event()

        # Initialize strategy registry
        self.strategy_registry = StrategyRegistry()

        self.logger.info("Strategy Manager initialized")

    def _register_event_handlers(self):
        """Register event handlers with the event manager."""
        # Market data events
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._handle_market_data,
            component_name="StrategyManager"
        )
        
        # Signal events
        self.event_manager.subscribe(
            EventType.SIGNAL,
            self._handle_strategy_signal,
            component_name="StrategyManager"
        )
        
        # Bar events
        self.event_manager.subscribe(
            EventType.BAR,
            self._on_bar,
            component_name="StrategyManager"
        )
        
        self.logger.info("StrategyManager event handlers registered")

    def _handle_market_data(self, event: Event):
        """
        Handle market data events.

        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent):
            return

        self.logger.debug(f"Received MarketDataEvent: {event}")
        
        # Check if this is a configured underlying
        instrument = event.instrument
        symbol = instrument.symbol

        if symbol in self.option_manager.underlying_subscriptions:
            price = event.data.get('close') or event.data.get('last') or event.data.get('price')
            if price:
                self.option_manager.update_underlying_price(symbol, price)

    def _handle_strategy_signal(self, event: Event):
        """
        Handle strategy signal events.

        Args:
            event: Signal event
        """
        if not isinstance(event, SignalEvent):
            return

        # Validate signal through risk manager
        if self.risk_manager and not self.risk_manager.validate_signal(event):
            self.logger.warning(f"Signal rejected by risk manager: {event}")
            return

        # Process the signal
        self._process_signal(event)

    def _on_bar(self, event: BarEvent):
        """
        Handle bar events.

        Args:
            event: Bar event
        """
        if not isinstance(event, BarEvent):
            return

        # Process the bar event
        self._process_bar(event)

    def load_config(self, strategy_config_path='config/strategies.yml', main_config_path='config/config.yaml'):
        """
        Load strategy and main configuration.

        Args:
            strategy_config_path: Path to the strategy configuration file
            main_config_path: Path to the main configuration file
        """
        try:
            # Load strategy configuration
            if isinstance(strategy_config_path, dict):
                self.config = strategy_config_path
            else:
                with open(strategy_config_path, 'r') as f:
                    self.config = yaml.safe_load(f)

            # Load main configuration to get indices
            if isinstance(main_config_path, dict):
                self.main_config = main_config_path
            else:
                try:
                    with open(main_config_path, 'r') as f:
                        self.main_config = yaml.safe_load(f)
                except Exception as e:
                    self.logger.error(f"Failed to load main configuration: {e}")
                    self.main_config = {}

            # Extract underlyings configuration from main config
            self.underlyings_config = self.main_config.get('market', {}).get('underlyings', {})

            # Configure option manager with underlyings
            self.option_manager.configure_underlyings(self.underlyings_config)

            self.logger.info(f"Loaded strategy configuration with {len(self.config.get('strategies', {}))} strategies")
            self.logger.info(f"Loaded underlyings configuration with {len(self.underlyings_config)} underlyings")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return False

    def initialize_strategies(self):
        """
        Initialize all strategies based on the loaded configuration.
        """
        if not self.config:
            self.logger.error("Cannot initialize strategies: configuration not loaded")
            return False

        strategy_configs = self.config.get('strategies', {})

        for strategy_id, config in strategy_configs.items():
            if not config.get('enabled', True):
                self.logger.info(f"Strategy {strategy_id} is disabled, skipping")
                continue

            try:
                # Load strategy class
                strategy_class = self._get_strategy_class(config.get('type', strategy_id))

                if not strategy_class:
                    self.logger.error(f"Could not find strategy class for {strategy_id}")
                    continue

                # Create strategy instance
                strategy = strategy_class(
                    strategy_id=strategy_id,
                    config=config,
                    data_manager=self.data_manager,
                    option_manager=self.option_manager,
                    portfolio_manager=self.portfolio_manager,
                    event_manager=self.event_manager
                )

                # Store the strategy
                self.strategies[strategy_id] = strategy
                self.strategy_configs[strategy_id] = config

                # Get required symbols and options
                self.strategy_symbols[strategy_id] = strategy.get_required_symbols()
                self.strategy_options[strategy_id] = strategy.get_required_options()

                # Update symbol-strategy mapping
                for timeframe, symbols in self.strategy_symbols[strategy_id].items():
                    for symbol in symbols:
                        if symbol not in self.symbol_strategies:
                            self.symbol_strategies[symbol] = {}
                        if timeframe not in self.symbol_strategies[symbol]:
                            self.symbol_strategies[symbol][timeframe] = set()
                        self.symbol_strategies[symbol][timeframe].add(strategy_id)

                # Subscribe to indices if needed
                if hasattr(strategy, 'underlyings'):
                    for underlying in strategy.underlyings:
                        if not self.option_manager.subscribe_underlying(underlying):
                            self.logger.error(f"Failed to subscribe to underlying {underlying} for strategy {strategy_id}")
                            continue

                self.logger.info(f"Initialized strategy {strategy_id}")
            except Exception as e:
                self.logger.error(f"Failed to initialize strategy {strategy_id}: {e}")

        self.logger.info(f"Initialized {len(self.strategies)} strategies")
        return len(self.strategies) > 0

    @lru_cache(maxsize=100)
    def _get_strategy_class(self, strategy_type: str) -> Optional[Type[OptionStrategy]]:
        """
        Get the strategy class by type name.

        Args:
            strategy_type: Type name of the strategy

        Returns:
            Type[OptionStrategy]: The strategy class if found, None otherwise
        """
        # First try direct import
        try:
            module_path = f"strategies.{strategy_type}"
            module = importlib.import_module(module_path)

            # Find the strategy class in the module
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and issubclass(obj, OptionStrategy) and
                    obj != OptionStrategy and name.lower() == strategy_type.lower()):
                    return obj
        except ImportError:
            pass

        # Try with option_strategies subdirectory
        try:
            module_path = f"strategies.option_strategies.{strategy_type}"
            module = importlib.import_module(module_path)

            # Find the strategy class in the module
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and issubclass(obj, OptionStrategy) and
                    obj != OptionStrategy and name.lower() == strategy_type.lower()):
                    return obj
        except ImportError:
            pass

        # Try directory search
        strategies_dir = Path("strategies")
        for py_file in strategies_dir.glob("**/*.py"):
            if py_file.stem.lower() == strategy_type.lower():
                module_path = f"strategies.{py_file.stem}"
                if py_file.parent.name != "strategies":
                    module_path = f"strategies.{py_file.parent.name}.{py_file.stem}"

                try:
                    module = importlib.import_module(module_path)

                    # Find the strategy class in the module
                    for name, obj in inspect.getmembers(module):
                        if (inspect.isclass(obj) and issubclass(obj, OptionStrategy) and
                            obj != OptionStrategy):
                            return obj
                except ImportError:
                    continue

        self.logger.error(f"Strategy type {strategy_type} not found")
        return None

    def start_strategies(self):
        """
        Start all initialized strategies.
        """
        self.logger.info("Starting strategies")

        # Subscribe to required symbols first
        self.subscribe_symbols()

        # Start each strategy
        for strategy_id, strategy in self.strategies.items():
            try:
                strategy.on_start()
                self.logger.info(f"Started strategy {strategy_id}")
            except Exception as e:
                self.logger.error(f"Error starting strategy {strategy_id}: {str(e)}", exc_info=True)

    def stop_strategies(self):
        """
        Stop all running strategies.
        """
        self.logger.info("Stopping strategies")

        # Stop each strategy
        for strategy_id, strategy in self.strategies.items():
            try:
                strategy.on_stop()
                self.logger.info(f"Stopped strategy {strategy_id}")
            except Exception as e:
                self.logger.error(f"Error stopping strategy {strategy_id}: {str(e)}", exc_info=True)

        # Unsubscribe from all symbols
        self.unsubscribe_symbols()

    def subscribe_symbols(self):
        """
        Subscribe to all symbols required by the strategies.
        """
        self.logger.info("Subscribing to strategy symbols")

        # Track which symbols we've already subscribed to for each timeframe
        subscribed = {}  # timeframe -> set(symbols)

        for strategy_id, symbols_by_timeframe in self.strategy_symbols.items():
            for timeframe, symbols in symbols_by_timeframe.items():
                if timeframe not in subscribed:
                    subscribed[timeframe] = set()

                for symbol in symbols:
                    if symbol not in subscribed[timeframe]:
                        try:
                            # Get exchange from config or use default
                            exchange = self.config.get('market', {}).get('default_exchange', 'NSE')

                            # Create an instrument object for the symbol
                            instrument = Instrument(symbol=symbol, exchange=exchange)

                            # Subscribe to the instrument
                            self.data_manager.subscribe_to_timeframe(strategy_id, instrument, timeframe)
                            subscribed[timeframe].add(symbol)
                            self.logger.debug(f"Subscribed to {symbol}@{timeframe} for strategy {strategy_id}")
                        except Exception as e:
                            self.logger.error(f"Error subscribing to {symbol}@{timeframe}: {str(e)}")

    def unsubscribe_symbols(self):
        """
        Unsubscribe from all symbols.
        """
        self.logger.info("Unsubscribing from strategy symbols")

        for strategy_id in self.strategies:
            try:
                self.data_manager.unsubscribe_from_timeframe(strategy_id)
                self.logger.debug(f"Unsubscribed all symbols for strategy {strategy_id}")
            except Exception as e:
                self.logger.error(f"Error unsubscribing symbols for strategy {strategy_id}: {str(e)}")

    def start(self):
        """
        Start the strategy manager and all strategies.
        """
        try:
            # Initialize strategies if not already initialized
            if not self.strategies:
                if not self.initialize_strategies():
                    self.logger.error("Failed to initialize strategies")
                    return False

            # Start the monitoring thread
            self.stop_event.clear()
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_thread,
                name="StrategyManagerMonitor"
            )
            self.monitoring_thread.daemon = True
            self.monitoring_thread.start()

            # Start all strategies
            self.start_strategies()

            self.logger.info("Strategy Manager started successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error starting Strategy Manager: {e}")
            return False

    def stop(self):
        """
        Stop the strategy manager and all strategies.
        """
        # Stop the monitoring thread
        self.stop_event.set()
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5.0)

        # Stop all strategies
        self.stop_strategies()

        self.logger.info("Strategy Manager stopped")

    def _monitoring_thread(self):
        """
        Monitoring thread function.
        Periodically checks strategy health and performance.
        """
        self.logger.info("Strategy monitoring thread started")

        while not self.stop_event.is_set():
            try:
                # Check all strategies
                for strategy_id, strategy in self.strategies.items():
                    # Check if strategy is responsive
                    if hasattr(strategy, 'last_heartbeat'):
                        last_heartbeat = getattr(strategy, 'last_heartbeat', 0)
                        if time.time() - last_heartbeat > 60:  # 1 minute timeout
                            self.logger.warning(f"Strategy {strategy_id} has not sent a heartbeat in over 1 minute")

                    # Check other health metrics
                    # ...
            except Exception as e:
                self.logger.error(f"Error in strategy monitoring: {e}")

            # Sleep for a bit
            time.sleep(10)

        self.logger.info("Strategy monitoring thread stopped")

    def get_strategy(self, strategy_id: str) -> Optional[OptionStrategy]:
        """
        Get a strategy by ID.

        Args:
            strategy_id: Strategy ID

        Returns:
            Optional[OptionStrategy]: The strategy instance if found, None otherwise
        """
        return self.strategies.get(strategy_id)

    def get_strategy_status(self, strategy_id: str) -> Dict[str, Any]:
        """
        Get the status of a strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            Dict[str, Any]: Dictionary with strategy status information
        """
        strategy = self.get_strategy(strategy_id)

        if not strategy:
            return {'error': 'Strategy not found'}

        return strategy.get_status()

    def get_options_for_index(self, index_symbol: str) -> List[str]:
        """
        Get the list of option symbols for an index.

        Args:
            index_symbol: Index symbol

        Returns:
            List[str]: List of option symbols
        """
        return self.option_manager.get_active_option_symbols(index_symbol)

    def request_symbol_subscription(self, strategy_id: str, symbol: str, timeframe: str = None):
        """
        Request subscription to a symbol for a strategy.

        Args:
            strategy_id: ID of the strategy requesting the subscription
            symbol: Symbol to subscribe to
            timeframe: Timeframe to subscribe to (uses strategy default if None)

        Returns:
            bool: True if subscription was successful
        """
        if strategy_id not in self.strategies:
            self.logger.error(f"Strategy {strategy_id} not found")
            return False

        strategy = self.strategies[strategy_id]

        # Use strategy's default timeframe if none specified
        if timeframe is None:
            timeframe = strategy.timeframe

        try:
            # Subscribe to the symbol
            self.data_manager.subscribe_to_timeframe(strategy_id, symbol, timeframe)

            # Update tracking
            if strategy_id not in self.strategy_symbols:
                self.strategy_symbols[strategy_id] = {}
            if timeframe not in self.strategy_symbols[strategy_id]:
                self.strategy_symbols[strategy_id][timeframe] = set()
            self.strategy_symbols[strategy_id][timeframe].add(symbol)

            if symbol not in self.symbol_strategies:
                self.symbol_strategies[symbol] = {}
            if timeframe not in self.symbol_strategies[symbol]:
                self.symbol_strategies[symbol][timeframe] = set()
            self.symbol_strategies[symbol][timeframe].add(strategy_id)

            self.logger.info(f"Subscribed to {symbol}@{timeframe} for strategy {strategy_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error subscribing to {symbol}@{timeframe}: {str(e)}")
            return False

    def request_option_subscription(self, strategy_id: str, index_symbol: str, strike: float, option_type: str) -> bool:
        """
        Request subscription to a specific option.

        Args:
            strategy_id: Strategy ID
            index_symbol: Index symbol
            strike: Strike price
            option_type: Option type ('CE' or 'PE')

        Returns:
            bool: True if subscription was successful, False otherwise
        """
        # Ensure this is implemented in option_manager
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
        
        self.logger.info(f"Generated {signal_type} signal for {symbol} with priority {priority.name}")

    def on_timer(self, event: Event):
        """Handle timer events for periodic tasks."""
        if event.event_type == EventType.TIMER:
           self.logger.info("Timer event received")           
                
