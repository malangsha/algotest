import logging
import time
import queue
import threading
from typing import Dict, List, Any, Optional, Set
from datetime import datetime

from models.events import Event, MarketDataEvent, OrderEvent, SignalEvent, FillEvent, EventType, SystemEvent
from models.instrument import Instrument
from models.market_data import MarketData
from models.order import Order, OrderStatus
from brokers.broker_factory import BrokerFactory
from strategies.strategy_factory import StrategyFactory
from core.event_manager import EventManager
from core.data_manager import DataManager
from core.position_manager import PositionManager
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from core.portfolio import Portfolio
from core.performance import PerformanceTracker
from core.execution_handler import ExecutionHandler
from core.paper_trading_simulator import PaperTradingSimulator
from utils.constants import InstrumentType
from utils.exceptions import ConfigError, InitializationError
from core.option_manager import OptionManager
from core.strategy_manager import StrategyManager
from core.logging_manager import get_logger

class UniverseHandler:
    """
    Handles instrument universe selection for strategies.
    """
    def __init__(self, data_source=None):
        """
        Initialize the universe handler.

        Args:
            data_source: Optional data source for market data
        """
        self.data_source = data_source
        self.logger = get_logger("core.universe_handler")

    def get_instruments(self, universe_config: Dict[str, Any],
                        instruments_dict: Dict[str, Instrument]) -> List[Instrument]:
        """
        Get instruments based on universe configuration.

        Args:
            universe_config: Universe configuration (type, value, etc.)
            instruments_dict: Dictionary of available instruments

        Returns:
            List[Instrument]: Selected instruments
        """
        universe_type = universe_config.get('type', 'custom')
        selected_instruments = []

        # Consolidate 'explicit' and 'custom' handling
        if universe_type in ['explicit', 'custom']:
            # Get symbols list from appropriate field based on type
            symbols_key = 'value' if universe_type == 'explicit' else 'symbols'
            symbols = universe_config.get(symbols_key, [])

            for symbol in symbols:
                if symbol in instruments_dict:
                    selected_instruments.append(instruments_dict[symbol])
                else:
                    self.logger.warning(f"Instrument {symbol} not found in available instruments")

            self.logger.info(f"{universe_type} universe: requested {len(symbols)} symbols, found {len(selected_instruments)}")

        elif universe_type == 'index_components':
            # Index components
            index_name = universe_config.get('value')
            max_stocks = universe_config.get('max_stocks')
            min_market_cap = universe_config.get('min_market_cap')

            if not index_name:
                self.logger.error("Index name not specified in universe config")
                return []

            # If we have a data source, use it to get index components
            if self.data_source and hasattr(self.data_source, 'get_index_components'):
                try:
                    components = self.data_source.get_index_components(
                        index_name,
                        max_stocks=max_stocks,
                        min_market_cap=min_market_cap
                    )
                    return [instruments_dict[symbol] for symbol in components if symbol in instruments_dict]
                except Exception as e:
                    self.logger.error(f"Error getting index components: {str(e)}")
                    return []
            else:
                self.logger.warning("No data source available for index components")
                return []

        elif universe_type == 'sector':
            # Sector-based selection
            sector = universe_config.get('value')
            max_stocks = universe_config.get('max_stocks')

            if not sector:
                self.logger.error("Sector not specified in universe config")
                return []

            # If we have a data source, use it to get sector stocks
            if self.data_source and hasattr(self.data_source, 'get_sector_stocks'):
                try:
                    sector_stocks = self.data_source.get_sector_stocks(
                        sector,
                        max_stocks=max_stocks
                    )
                    return [instruments_dict[symbol] for symbol in sector_stocks if symbol in instruments_dict]
                except Exception as e:
                    self.logger.error(f"Error getting sector stocks: {str(e)}")
                    return []
            else:
                self.logger.warning("No data source available for sector stocks")
                return []
        else:
            self.logger.warning(f"Unknown universe type: {universe_type}")
            return []

        # Log found instruments
        if selected_instruments:
            self.logger.info(f"Selected {len(selected_instruments)} instruments: {[i.symbol for i in selected_instruments]}")
        else:
            self.logger.warning("No instruments found for the universe configuration")

        return selected_instruments

class TradingEngine:
    """Core trading engine of the framework."""

    def __init__(self, config: Dict[str, Any], strategy_config: Dict[str, Any] = None):
        """
        Initialize the TradingEngine with configuration.

        Args:
            config: Main configuration dictionary
            strategy_config: Strategy configuration dictionary
        """
        # Store configurations
        self.config = config
        self.strategy_config = strategy_config or {}

        # Initialize logging
        self.logger = get_logger("core.engine")
        self.logger.info("Initializing Trading Engine")

        # Initialize event manager (sole queue mechanism)
        self.event_manager = EventManager()

        # Initialize broker using factory
        self.broker = None
        self._initialize_broker()

        # Initialize core components
        self.data_manager = DataManager(self.config, self.event_manager, self.broker)    
        
        # # Initialize position and portfolio management        
        self.position_manager = PositionManager(self.event_manager)        
        self.portfolio = Portfolio(self.config, self.position_manager)
        self.risk_manager = RiskManager(self.portfolio, self.config)
        self.order_manager = OrderManager(self.event_manager, self.risk_manager)

        # Initialize OptionManager for option trading functionality
        self.option_manager = OptionManager(self.data_manager, 
                                            self.event_manager, 
                                            self.position_manager, 
                                            self.config)
        self.logger.info("Option manager initialized")
        
        # Configure indices from market config
        market_config = self.config.get('market', {})
        underlyings_config = market_config.get('underlyings', [])
        if underlyings_config:
            self.logger.info(f"Configuring {len(underlyings_config)} underlyings in Option Manager")
            self.option_manager.configure_underlyings(underlyings_config)
        else:
            self.logger.debug("No underlyings configuration found in config")
        
        # Initialize StrategyManager to handle strategy lifecycle
        self.strategy_manager = StrategyManager(
            data_manager=self.data_manager,
            event_manager=self.event_manager,
            portfolio_manager=self.position_manager,
            risk_manager=self.risk_manager,
            broker=self.broker,
            config=self.config
        )
        self.logger.info("Strategy manager initialized")
        
        # Set OptionManager in StrategyManager
        self.strategy_manager.option_manager = self.option_manager
        
        # Load strategy configurations
        if self.strategy_config:
            self.strategy_manager.load_config(self.strategy_config, self.config)
            self.logger.info("Strategy configurations loaded")
            
        self.performance_tracker = PerformanceTracker(self.portfolio, self.config)

        # Assuming broker acts as the primary data source connection point
        self.data_source = self.broker 

        # Initialize the universe handler if needed (consider refactoring)
        # self.universe_handler = UniverseHandler(self.data_source)

        # Instrument tracking
        self.instruments: Dict[str, Instrument] = {}
        self.market_data: Dict[str, MarketData] = {} # Cache for latest data
        self.orders: Dict[str, Order] = {} # Cache for active orders
        self.active_instruments = set()

        # Initialize strategies storage
        self.strategies = {} # Strategies managed by StrategyManager

        # Heartbeat interval in seconds
        self.heartbeat = self.config.get("system", {}).get("heartbeat_interval", 1.0)
        self._timer_thread = None # For periodic events

        # Engine state
        self.running = False
        self.paused = False
        self._main_thread = None # Main engine loop thread (for heartbeat/performance)
        self.last_heartbeat_time = None

        # Initialize Execution Handler (after Broker and EventManager)
        try:
            self.execution_handler = ExecutionHandler(
                event_manager=self.event_manager,
                broker_interface=self.broker
            )
            self.logger.info("Execution Handler initialized successfully")
        except Exception as e:
            raise InitializationError(f"Failed to initialize Execution Handler: {e}")

        # Initialize Paper Trading Simulator (only if in paper mode)
        self.paper_trading_simulator = None
        system_mode = self.config.get('system', {}).get('mode', 'live')
        if system_mode == 'paper':
            if hasattr(self.broker, 'paper_trading'): # Check if broker supports paper mode flag
                self.broker.paper_trading = True # Tell the broker it's in paper mode
                try:
                    self.paper_trading_simulator = PaperTradingSimulator(
                        event_manager=self.event_manager,
                        broker_interface=self.broker # Pass broker to access simulated orders
                    )
                    self.logger.info("Paper Trading Simulator initialized successfully")
                except Exception as e:
                    raise InitializationError(f"Failed to initialize Paper Trading Simulator: {e}")
            else:
                self.logger.warning("System mode is 'paper' but active broker does not seem to support paper trading flag.")

        self.logger.info("Trading Engine initialized")

    def initialize(self):
        """
        Initialize the trading engine components.
        """
        self.logger.info("Initializing engine components")

        # Only create the universe handler if it doesn't exist and is needed
        # if not hasattr(self, 'universe_handler') or self.universe_handler is None:
        #     self.universe_handler = UniverseHandler(self.data_source)

        # Set the event manager for the broker
        if self.broker:
            if hasattr(self.broker, 'set_event_manager'):
                self.broker.set_event_manager(self.event_manager)
                self.logger.info("Event manager set for the broker")
            else:
                 self.logger.warning("Broker does not have 'set_event_manager' method.")


        # Load instruments
        self._load_instruments()

        # Start the event manager processing loop
        self.event_manager.start()

        # Set up event handlers within this engine
        self._subscribe_to_events()

        # Load strategies (initializes and gets active instruments)
        self._load_strategies()

        # Connect to broker
        if self.broker and hasattr(self.broker, 'connect'):
            try:
                self.broker.connect()
            except Exception as e:
                self.logger.error(f"Error connecting to broker: {str(e)}")
                # Consider if initialization should fail here

        # Start ExecutionHandler if it has a start method
        if hasattr(self.execution_handler, 'start'):
             self.execution_handler.start()

        # Start PaperTradingSimulator if it exists and has start method
        if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'start'):
            self.paper_trading_simulator.start()

        self.logger.info("Engine initialization complete")

    def _initialize_broker(self):
        """Initialize the broker based on configuration."""
        # Initialize Broker Interface
        try:
            broker_config_list = self.config.get('broker_connections', {}).get('connections', [])
            active_connection_name = self.config.get('broker_connections', {}).get('active_connection')

            if not active_connection_name:
                raise InitializationError("No 'active_connection' specified under 'broker_connections' in config.")

            active_broker_config = None
            for cfg in broker_config_list:
                if cfg.get('connection_name') == active_connection_name:
                    active_broker_config = cfg
                    break

            if not active_broker_config:
                raise InitializationError(f"Configuration for active broker connection '{active_connection_name}' not found.")

            broker_type = active_broker_config.get('broker_type')
            if not broker_type:
                raise InitializationError(f"Missing 'broker_type' for connection '{active_connection_name}'.")

            # Pass config and specific connection details
            self.broker = BrokerFactory.create_broker(broker_type, config=self.config, **active_broker_config)
            if not self.broker.is_connected:            
               raise InitializationError(f"Broker initialized failed: {broker_type}")
            self.logger.info(f"Broker interface initialized: {broker_type}")            

        except ImportError as e:
            raise InitializationError(f"Failed to import broker module: {e}")
        except ValueError as e:
            raise InitializationError(f"Failed to create broker: {e}")
        except Exception as e:
            raise InitializationError(f"Unexpected error initializing broker: {e}")

    def _load_instruments(self):
        """Load instruments from configuration or data source."""
        # Try to load from config file first
        instruments_config = self.config.get('instruments', [])

        for instr_config in instruments_config:
            try:
                instrument = Instrument(
                    instrument_id=instr_config.get('id', instr_config.get('symbol')),
                    instrument_type=instr_config.get('instrument_type', InstrumentType.EQUITY),
                    symbol=instr_config['symbol'],
                    exchange=instr_config.get('exchange', ''),
                    asset_type=instr_config.get('asset_type', ''),
                    currency=instr_config.get('currency', ''),
                    tick_size=instr_config.get('tick_size', 0.01),
                    lot_size=instr_config.get('lot_size', 1.0),
                    margin_requirement=instr_config.get('margin_requirement', 1.0),
                    is_tradable=instr_config.get('is_tradable', True),
                    expiry_date=instr_config.get('expiry_date'),
                    additional_info=instr_config.get('additional_info', {})
                )
                self.instruments[instrument.symbol] = instrument

                # Also store by ID if it's different from symbol
                if instrument.instrument_id != instrument.symbol:
                    self.instruments[instrument.instrument_id] = instrument
            except KeyError as e:
                self.logger.error(f"Missing required key {e} in instrument config: {instr_config}")
            except Exception as e:
                 self.logger.error(f"Error creating instrument from config {instr_config}: {e}")


        # If we have a data source, try to load additional instruments
        # This might be broker-specific
        if self.data_source and hasattr(self.data_source, 'get_instruments'):
            try:
                additional_instruments = self.data_source.get_instruments()
                for instrument in additional_instruments:
                    if instrument.symbol not in self.instruments:
                        self.instruments[instrument.symbol] = instrument
                        if instrument.instrument_id != instrument.symbol:
                            self.instruments[instrument.instrument_id] = instrument
            except Exception as e:
                self.logger.error(f"Error loading instruments from data source: {str(e)}")

        # Add instruments to position manager
        for instrument in self.instruments.values():
            self.position_manager.add_instrument(instrument)
            
        self.logger.info(f"Loaded {len(self.instruments)} instruments")

    def _load_strategies(self):
        """Load strategies using the StrategyManager."""
        self.logger.info("Loading strategies...")
        # Initialize strategies using StrategyManager
        self.strategy_manager.initialize_strategies()
        
        # Get loaded strategies from the manager
        self.strategies = self.strategy_manager.strategies # Keep a reference
        
        # Track active instruments across all strategies
        temp_active_instruments = set()
        for strategy_id, strategy in self.strategies.items():
            # If the strategy has instruments defined
            if hasattr(strategy, 'instruments') and strategy.instruments:
                # Get instrument symbols
                instrument_symbols = [instr.symbol for instr in strategy.instruments if hasattr(instr, 'symbol')]
                temp_active_instruments.update(instrument_symbols)
                
                # StrategyManager should ideally handle subscriptions needed by strategies
                # through DataManager or OptionManager
            else:
                self.logger.warning(f"Strategy {strategy_id} has no instruments defined.")

        self.active_instruments = temp_active_instruments
        self.logger.info(f"Loaded {len(self.strategies)} strategies. Active instruments: {len(self.active_instruments)}")
        # DataManager should handle subscriptions based on strategy needs or explicit calls

    def _subscribe_to_events(self):
        """Subscribe internal engine methods to relevant events."""
        self.logger.debug("Subscribing engine methods to events")
        
        # Subscribe to events needed for local cache maintenance
        self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data, component_name="TradingEngine")
        self.event_manager.subscribe(EventType.ORDER, self._on_order_event, component_name="TradingEngine")
        self.event_manager.subscribe(EventType.FILL, self._on_fill_event, component_name="TradingEngine")
        
        # Subscribe to system events for engine control
        self.event_manager.subscribe(EventType.SYSTEM, self._on_system_event, component_name="TradingEngine")
        
        # Subscribe to timer events for periodic tasks
        self.event_manager.subscribe(EventType.TIMER, self._on_timer_event, component_name="TradingEngine")
        
        # Subscribe to signal events for order creation
        self.event_manager.subscribe(EventType.SIGNAL, self._on_signal_event, component_name="TradingEngine")

    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events to maintain local cache.
        This cache is used by get_market_data() method.

        Args:
            event: Market data event
        """
        try:
            self.logger.debug(f"Received MarketDataEvent: {event}")
            # Basic validation
            if not isinstance(event, MarketDataEvent) or not hasattr(event, 'instrument') or not hasattr(event.instrument, 'symbol'):
                self.logger.warning(f"Invalid MarketDataEvent received: {event}")
                return

            instrument = event.instrument
            symbol = instrument.symbol
            
            # Update local cache only
            if hasattr(event, 'data'):
                self.market_data[symbol] = event.data
            
            self.logger.debug(f"Updated market data cache for {symbol}: {event.data_type}")

        except Exception as e:
            self.logger.error(f"Error updating market data cache for {getattr(event, 'instrument', 'N/A')}: {e}", exc_info=True)

    def _on_order_event(self, event: OrderEvent):
        """
        Handle order events to maintain local order cache.
        This cache is used by get_order() method and for order status tracking.

        Args:
            event: Order event
        """
        try:
            if not isinstance(event, OrderEvent) or not hasattr(event, 'data') or not isinstance(event.data, Order):
                self.logger.warning(f"Invalid OrderEvent received: {event}")
                return

            order = event.data
            self.logger.debug(f"Updating order cache for {order.order_id}: Status {order.status}")

            # Update local order cache only
            self.orders[order.order_id] = order

        except Exception as e:
            self.logger.error(f"Error updating order cache for {getattr(event.data, 'order_id', 'N/A')}: {e}", exc_info=True)

    def _on_signal_event(self, event: SignalEvent):
        """
        Handle strategy signal events directly.
        Validates signal via RiskManager and converts to an OrderEvent via OrderManager.

        Args:
            event: Signal event generated by a strategy
        """
        try:
            if not isinstance(event, SignalEvent):
                self.logger.warning(f"Invalid SignalEvent received: {event}")
                return
                
            signal_data = event.data # Assuming event.data holds signal details
            self.logger.debug(f"Processing signal event: {signal_data}")

            # Check risk limits before creating order
            if self.risk_manager.check_signal(signal_data): # Pass signal data directly
                # Convert signal to order via OrderManager
                order_event = self.order_manager.create_order_from_signal(signal_data) # Pass signal data
                if order_event:
                    # Publish the new OrderEvent for the ExecutionHandler to pick up
                    self.event_manager.publish(order_event)
                    self.logger.info(f"Signal converted to OrderEvent: {order_event.data.order_id}")
                else:
                     self.logger.warning(f"Signal could not be converted to order: {signal_data}")
            else:
                self.logger.info(f"Signal rejected by risk manager: {signal_data}")

        except Exception as e:
            self.logger.error(f"Error processing signal event from strategy {getattr(event, 'strategy_id', 'N/A')}: {e}", exc_info=True)

    def _on_fill_event(self, event: FillEvent):
        """
        Handle fill events to update order status in local cache.
        This is important for tracking order completion and partial fills.

        Args:
            event: Fill event
        """
        try:
            if not isinstance(event, FillEvent) or not hasattr(event, 'data'):
                self.logger.warning(f"Invalid FillEvent received: {event}")
                return

            fill_data = event.data
            self.logger.debug(f"Processing fill event for order cache: {fill_data}")

            # Update local order status cache if applicable
            order_id = getattr(fill_data, 'order_id', None)
            if order_id and order_id in self.orders:
                order = self.orders[order_id]
                order.status = OrderStatus.FILLED 
                order.executed_quantity = getattr(fill_data, 'quantity', order.quantity)
                order.average_price = getattr(fill_data, 'price', order.average_price)

        except Exception as e:
            self.logger.error(f"Error updating order cache for fill {getattr(event.data, 'order_id', 'N/A')}: {e}", exc_info=True)

    def _on_timer_event(self, event: Event):
        """Handle timer events for periodic tasks."""
        if event.event_type == EventType.TIMER:
            # 1. Update Performance Tracker
            if hasattr(self.performance_tracker, 'update'):
                 self.performance_tracker.update()
                 
            # 2. Notify Strategy Manager (for strategies needing timer updates)
            if hasattr(self.strategy_manager, 'on_timer'):
                 try:
                    self.strategy_manager.on_timer(event)
                 except Exception as strat_e:
                    self.logger.error(f"Error in StrategyManager.on_timer: {strat_e}", exc_info=True)

    def _on_system_event(self, event: SystemEvent):
        """Handle system events."""
        if event.event_type == EventType.SYSTEM:
             self.logger.info(f"Processing system event: {getattr(event, 'system_event_type', 'N/A')} - {getattr(event, 'message', '')}")
             # Add specific handling if needed (e.g., reconfigure on settings change)

    def _run_engine(self):
        """Main engine loop - Simplified for heartbeat/timer events."""
        try:
            self.logger.info("Main engine thread started (for timer events)")
            last_timer_publish = time.time()

            while self.running:
                current_time = time.time()
                
                # Publish timer event periodically
                if not self.paused and (current_time - last_timer_publish >= self.heartbeat):
                    elapsed = current_time - last_timer_publish
                    self._publish_timer_event(elapsed)
                    last_timer_publish = current_time

                # Efficiently sleep/wait
                # Event manager processing happens in its own thread now
                time.sleep(min(self.heartbeat / 10, 0.1)) # Sleep briefly

        except Exception as e:
            self.logger.error(f"Error in main engine loop: {str(e)}", exc_info=True)
            self.stop() # Attempt to stop if the main loop crashes

        self.logger.info("Main engine thread stopped")
        
    def _publish_timer_event(self, elapsed: float):
        """
        Publish a timer event to notify components of time passing.
        Useful for strategies that need to perform periodic tasks.
        
        Args:
            elapsed: Time elapsed since last timer event in seconds
        """
        if not self.event_manager:
            return
            
        # Create timer event
        event = Event(
            event_type=EventType.TIMER,
            timestamp=int(time.time() * 1000) # Milliseconds
        )
        
        # Add timer details
        event.elapsed = elapsed
        event.timestamp_dt = datetime.now()
        
        # Publish the event
        self.event_manager.publish(event)
        self.logger.debug(f"Published timer event: elapsed={elapsed:.2f}s")
        
        # Update last heartbeat time (optional, might remove if only using timer)
        self.last_heartbeat_time = time.time()
        
    def _publish_system_event(self, action: str, additional_data: Dict[str, Any] = None):
        """
        Publish a system event.
        
        Args:
            action: Action type (e.g., 'start', 'shutdown')
            additional_data: Additional data to include
        """
        # Create system event
        event_data = {
            'system_event_type': action,
            'message': f"System {action}",
            'severity': 'INFO',
            'data': additional_data or {}
        }
        
        # Add mode information if available in config
        event_data['data']['mode'] = self.config.get('system', {}).get('mode', 'unknown')
        
        # Create the event
        event = SystemEvent(
            event_type=EventType.SYSTEM,  # Add the required event_type
            timestamp=int(time.time() * 1000),
            **event_data
        )
        
        # Publish the event
        self.event_manager.publish(event)
        self.logger.info(f"Published system event: {action}")
        
    def start(self) -> bool:
        """
        Start the trading engine and all its components.
        
        Returns:
            bool: True if engine started successfully
        """
        try:
            self.logger.info("Starting trading engine")
            
            # Load instruments
            self._load_instruments()

            # Start event manager first
            if not self.event_manager.start():
                self.logger.error("Failed to start event manager")
                return False
                
            # Data Manager doesn't need to be started as it's initialized in __init__
            # Just verify it's properly initialized
            if not hasattr(self, 'data_manager'):
                self.logger.error("Data Manager not initialized")
                self.event_manager.stop()  # Clean up event manager
                return False
                
            # Start strategy manager
            if not self.strategy_manager.start():
                self.logger.error("Failed to start strategy manager")
                self.event_manager.stop()  # Clean up event manager
                return False
                
            # Start option manager tracking
            if hasattr(self, 'option_manager'):
                if not self.option_manager.start_tracking():
                    self.logger.error("Failed to start option manager tracking")
                    self.strategy_manager.stop()  # Clean up strategy manager
                    self.event_manager.stop()  # Clean up event manager
                    return False
                    
            # Start risk manager
            if self.risk_manager and hasattr(self.risk_manager, 'start'):
                if not self.risk_manager.start():
                    self.logger.error("Failed to start risk manager")
                    if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'stop_tracking'):
                        self.option_manager.stop_tracking()
                    self.strategy_manager.stop()
                    self.event_manager.stop()
                    return False
                    
            # Start paper trading simulator if applicable
            if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'start'):
                if not self.paper_trading_simulator.start():
                    self.logger.error("Failed to start paper trading simulator")
                    if self.risk_manager and hasattr(self.risk_manager, 'stop'):
                        self.risk_manager.stop()
                    if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'stop_tracking'):
                        self.option_manager.stop_tracking()
                    self.strategy_manager.stop()
                    self.event_manager.stop()
                    return False
                    
            # Start the main engine thread for timer events
            self.running = True
            if self._main_thread is None or not self._main_thread.is_alive():
                self._main_thread = threading.Thread(target=self._run_engine, daemon=True)
                self._main_thread.start()
                if not self._main_thread.is_alive():
                    self.logger.error("Failed to start main engine thread")
                    if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'stop'):
                        self.paper_trading_simulator.stop()
                    if self.risk_manager and hasattr(self.risk_manager, 'stop'):
                        self.risk_manager.stop()
                    if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'stop_tracking'):
                        self.option_manager.stop_tracking()
                    self.strategy_manager.stop()
                    self.event_manager.stop()
                    return False
                    
            # Set up event handlers within this engine
            self._subscribe_to_events()
            
            # Set running state and publish start event
            self.paused = False
            self._publish_system_event('start')
            
            self.logger.info("Trading engine started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting trading engine: {e}")
            # Clean up any components that might have started
            if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'stop'):
                self.paper_trading_simulator.stop()
            if self.risk_manager and hasattr(self.risk_manager, 'stop'):
                self.risk_manager.stop()
            if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'stop_tracking'):
                self.option_manager.stop_tracking()
            if hasattr(self, 'strategy_manager') and hasattr(self.strategy_manager, 'stop'):
                self.strategy_manager.stop()
            if hasattr(self, 'event_manager') and hasattr(self.event_manager, 'stop'):
                self.event_manager.stop()
            return False

    def stop(self):
        """Stop the trading engine and its components gracefully."""
        if not self.running:
            self.logger.warning("Engine is not running")
            return
            
        self.logger.info("Stopping trading engine")
        
        # Request shutdown
        self.running = False
        self.paused = True # Prevent further actions
        
        # Publish system shutdown event
        self._publish_system_event('shutdown')
        
        # Unsubscribe OptionManager (if needed, maybe handled internally)
        if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'stop_tracking'):
            self.option_manager.stop_tracking() # Hypothetical method
        
        # Stop strategy manager first
        if hasattr(self.strategy_manager, 'stop'):
            self.strategy_manager.stop()
            self.logger.info("Strategy Manager stopped")
            
        # Cancel pending orders via ExecutionHandler or Broker
        self._cancel_all_pending_orders()
        
        # Stop other components
        if self.risk_manager and hasattr(self.risk_manager, 'stop'):
            self.risk_manager.stop()
            
        if hasattr(self.portfolio, 'stop'): # If portfolio needs explicit stopping
            self.portfolio.stop()

        # Stop ExecutionHandler (if it has a stop method)
        if hasattr(self.execution_handler, 'stop'):
            self.execution_handler.stop()

        # Stop PaperTradingSimulator (if applicable and has stop method)
        if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'stop'):
            self.paper_trading_simulator.stop()
            
        # Disconnect broker
        if self.broker and hasattr(self.broker, 'disconnect'):
            self.broker.disconnect()
        
        # Stop the main engine timer thread
        # Wait for the main thread to finish (it checks self.running)
        if self._main_thread and self._main_thread.is_alive():
             self.logger.debug("Waiting for main engine thread to stop...")
             self._main_thread.join(timeout=max(self.heartbeat * 2, 2.0)) # Wait a bit longer than heartbeat
             if self._main_thread.is_alive():
                  self.logger.warning("Main engine thread did not stop gracefully.")
        
        # Stop Event Manager last (allows other components to publish final events)
        self.event_manager.stop()
        self.logger.info("Event Manager stopped")


        self.logger.info("Trading Engine stopped")

    def pause(self):
        """Pause the trading engine (stops timer events, signals)."""
        if not self.running:
            self.logger.warning("Trading engine is not running")
            return
            
        if self.paused:
            self.logger.info("Trading engine already paused")
            return

        self.paused = True
        self._publish_system_event('pause')
        self.logger.info("Trading engine paused")

    def resume(self):
        """Resume the trading engine."""
        if not self.running:
            self.logger.warning("Trading engine is not running")
            return
            
        if not self.paused:
             self.logger.info("Trading engine is not paused")
             return

        self.paused = False
        self._publish_system_event('resume')
        self.logger.info("Trading engine resumed")

    def _cancel_all_pending_orders(self):
        """Cancel all pending orders when stopping the engine."""
        self.logger.info("Cancelling all pending orders...")
        if not self.broker:
            self.logger.warning("Cannot cancel orders: No broker available.")
            return
            
        # Get pending orders (logic might be in OrderManager)
        pending_orders = []
        if hasattr(self.order_manager, 'get_pending_orders'):
            pending_orders = self.order_manager.get_pending_orders()
        else:
            # Fallback: check local cache
            pending_orders = [order for order in self.orders.values()
                              if order.status in [OrderStatus.PENDING, 
                                                  OrderStatus.SUBMITTED, 
                                                  OrderStatus.PARTIAL_FILL]] # Add relevant statuses

        if not pending_orders:
             self.logger.info("No pending orders found to cancel.")
             return

        cancelled_count = 0
        failed_count = 0
        for order in pending_orders:
            try:
                # Use ExecutionHandler or direct broker call
                if hasattr(self.execution_handler, 'cancel_order'):
                    self.execution_handler.cancel_order(order.order_id)
                elif hasattr(self.broker, 'cancel_order'):
                    self.broker.cancel_order(order.order_id)
                else:
                     self.logger.warning(f"No method found to cancel order {order.order_id}")
                     continue # Skip if no cancellation method
                     
                self.logger.info(f"Cancellation request sent for order {order.order_id}")
                cancelled_count += 1
            except Exception as e:
                self.logger.error(f"Error sending cancellation for order {order.order_id}: {str(e)}")
                failed_count += 1
                
        self.logger.info(f"Requested cancellation for {cancelled_count} orders. Failed requests: {failed_count}")

    def place_order(self, order: Order) -> bool:
        """
        Place an order through the engine by publishing an OrderEvent.
        The event will be picked up by _on_order_event (if tracking) and ExecutionHandler.

        Args:
            order: The order object to place

        Returns:
            bool: True if the order event was successfully published, False otherwise
        """
        if not self.running or self.paused:
            self.logger.error(f"Cannot place order - engine is not running or paused. Running: {self.running}, Paused: {self.paused}")
            return False

        # Validate order through RiskManager first
        if not self.risk_manager.validate_order(order):
             self.logger.warning(f"Order rejected by Risk Manager: {order}")
             # Optionally publish a rejected order event
             return False

        # Create order event
        # Ensure order has a unique ID before publishing
        if not order.order_id:
             order.order_id = self.order_manager.generate_order_id() # Assuming OrderManager can generate IDs
             
        event = OrderEvent(order=order) # EventType is set in OrderEvent constructor

        # Publish the event for ExecutionHandler and internal tracking (_on_order_event)
        published = self.event_manager.publish(event)
        if published:
             self.logger.info(f"Published OrderEvent for order {order.order_id}")
             # Add to local cache immediately? Or wait for broker confirmation via _on_order_event?
             # Let's add it here for immediate tracking, status will update later.
             self.orders[order.order_id] = order 
        else:
             self.logger.error(f"Failed to publish OrderEvent for order {order.order_id}")
             
        return published

    def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """
        Get the latest market data for a symbol from the internal cache.

        Args:
            symbol: The instrument symbol

        Returns:
            Optional[MarketData]: The latest market data object or None if not available
        """
        return self.market_data.get(symbol)

    def get_instrument(self, symbol_or_id: str) -> Optional[Instrument]:
        """
        Get instrument by symbol or ID from the internal cache.

        Args:
            symbol_or_id: The instrument symbol or ID

        Returns:
            Optional[Instrument]: The instrument or None if not found
        """
        return self.instruments.get(symbol_or_id)

    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order by ID from the internal cache.

        Args:
            order_id: The order ID

        Returns:
            Optional[Order]: The order or None if not found
        """
        return self.orders.get(order_id)

    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the trading engine."""
        status = {
            "running": self.running,
            "paused": self.paused,
            "last_heartbeat": self.last_heartbeat_time.isoformat() if self.last_heartbeat_time else None,
            "active_instruments_count": len(self.active_instruments),
            "loaded_strategies_count": len(self.strategies),
            "portfolio_value": self.portfolio.total_value if hasattr(self.portfolio, 'total_value') else 0,
            "open_positions_count": len(self.position_manager.get_open_positions()) if hasattr(self.position_manager, 'get_open_positions') else 0,
            "pending_orders_count": len([o for o in self.orders.values() if o.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILL]]) # Adjusted statuses
        }
        
        # Add component-specific status if available
        if hasattr(self.event_manager, 'get_status'):
             status["event_manager"] = self.event_manager.get_status()
        if hasattr(self.broker, 'get_status'):
             status["broker"] = self.broker.get_status()
        if hasattr(self, 'option_manager') and hasattr(self.option_manager, 'get_status'):
            status["options"] = self.option_manager.get_status()
        if hasattr(self, 'strategy_manager') and hasattr(self.strategy_manager, 'get_status'):
            status["strategy_manager"] = self.strategy_manager.get_status()
            
        return status

    def get_queue_stats(self) -> Dict[str, int]:
        """
        Get statistics about the main event queue size.

        Returns:
            Dict with event queue statistics
        """
        return {
            "event_queue_size": self.event_manager.get_queue_size() if hasattr(self.event_manager, 'get_queue_size') else -1
        }

    # Methods related to specific managers (like OptionManager) might be better accessed
    # directly via the manager instance (e.g., engine.option_manager.get_option_chain(...))
    # instead of adding pass-through methods here, unless there's a strong architectural reason.

    # Example direct access:
    # engine.option_manager.get_option_chain('NIFTY')
    # engine.option_manager.get_atm_strike('NIFTY')
    # engine.strategy_manager.get_strategy('my_strategy_id')
    # engine.strategy_manager.get_strategy_status('my_strategy_id')
