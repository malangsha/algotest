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
from utils.config_loader import ConfigLoader
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
        self.logger = logging.getLogger(__name__)

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
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing Trading Engine")

        # Initialize event queue and manager
        self.event_queue = queue.Queue()
        self.event_manager = EventManager()

        # Initialize broker using factory
        self.broker = None
        self._initialize_broker()

        # Initialize queues for different event types
        self.market_data_queue = queue.Queue(maxsize=config.get('queue_sizes', {}).get('market_data', 10000))
        self.order_queue = queue.Queue(maxsize=config.get('queue_sizes', {}).get('orders', 1000))
        self.signal_queue = queue.Queue(maxsize=config.get('queue_sizes', {}).get('signals', 1000))
        self.execution_queue = queue.Queue(maxsize=config.get('queue_sizes', {}).get('executions', 1000))

        # Initialize components
        self.data_manager = DataManager(self.config, self.event_manager, self.broker)    
        self.position_manager = PositionManager(self.event_manager)
        self.portfolio = Portfolio(self.config, self.position_manager)
        self.risk_manager = RiskManager(self.portfolio, self.config)
        self.order_manager = OrderManager(self.event_manager, self.risk_manager)
        self.performance_tracker = PerformanceTracker(self.portfolio, self.config)

        # For now, assume the broker is the data source for market data
        self.data_source = self.broker

        # Initialize the universe handler with the data source
        self.universe_handler = UniverseHandler(self.data_source)

        # Instrument tracking
        self.instruments: Dict[str, Instrument] = {}
        self.market_data: Dict[str, MarketData] = {}
        self.orders: Dict[str, Order] = {}
        self.active_instruments = set()

        # Initialize strategies
        self.strategies = {}

        # Heartbeat interval in seconds
        self.heartbeat = self.config.get("system", {}).get("heartbeat_interval", 1.0)

        # Engine state
        self.running = False
        self.paused = False
        self._main_thread = None
        self.last_heartbeat_time = None

        # Processing threads
        self.market_data_thread = None
        self.order_processing_thread = None
        self.strategy_thread = None
        self.event_processing_thread = None

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

        # Only create the universe handler if it doesn't exist
        if not hasattr(self, 'universe_handler') or self.universe_handler is None:
            self.universe_handler = UniverseHandler(self.data_source)

        # Set the event manager for the broker
        if self.broker:
            self.broker.set_event_manager(self.event_manager)
            self.logger.info("Event manager set for the broker")

        # Load instruments
        self._load_instruments()

        # Start the event manager
        self.event_manager.start()

        # Set up event handlers
        self._subscribe_to_events()

        # Load strategies
        self._load_strategies()

        # Connect to broker
        if self.broker and hasattr(self.broker, 'connect'):
            try:
                self.broker.connect()
            except Exception as e:
                self.logger.error(f"Error connecting to broker: {str(e)}")

        # Start ExecutionHandler if it has a start method
        if hasattr(self.execution_handler, 'start'):
             self.execution_handler.start()

        # Start PaperTradingSimulator if it exists
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

            self.broker = BrokerFactory.create_broker(broker_type, config=self.config, **active_broker_config)
            self.logger.info(f"Broker interface initialized: {broker_type}")

            # Pass the event manager to the broker if it needs it
            if hasattr(self.broker, 'set_event_manager') and callable(self.broker.set_event_manager):
                self.broker.set_event_manager(self.event_manager)
                self.logger.info(f"Passed EventManager to {broker_type} broker.")

        except ImportError as e:
            raise InitializationError(f"Failed to import broker module: {e}")
        except ValueError as e:
            raise InitializationError(f"Failed to create broker: {e}")
        except Exception as e:
            raise InitializationError(f"Unexpected error initializing broker: {e}")

    def _load_instruments(self):
        """Load instruments from configuration or data source."""
        # Try to load from config file
        instruments_config = self.config.get('instruments', [])

        for instr_config in instruments_config:
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

        # If we have a data source, try to load additional instruments
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

    def _load_strategies(self):
        """Load strategies from configuration."""
        strategies_config = self.strategy_config.get('strategies', [])
        
        self.logger.info(f"Initializing {len(strategies_config)} strategies")
        for strategy_config in strategies_config:
            strategy_type = strategy_config.get('type', strategy_config.get('name'))  # Fallback to name if type is missing
            strategy_id = strategy_config.get('id', f"{strategy_config.get('name', strategy_type)}_{int(time.time())}")

            # Get universe for the strategy
            universe_config = strategy_config.get('universe', {})
            universe = self.universe_handler.get_instruments(universe_config, self.instruments)

            # Log universe details for debugging
            self.logger.debug(f"Strategy {strategy_id} universe: {[instr.symbol for instr in universe]}")

            # Track instruments used by this strategy
            instrument_symbols = [instr.symbol for instr in universe]
            self.active_instruments.update(instrument_symbols)

            # Create strategy instance
            try:
                strategy = StrategyFactory.create_strategy(
                    strategy_type=strategy_type,
                    strategy_id=strategy_id,
                    data_manager=self.data_manager,
                    event_manager=self.event_manager,
                    config=strategy_config,
                    instruments=universe
                )

                if strategy:
                    self.strategies[strategy_id] = strategy
                    self.logger.info(f"Loaded strategy: {strategy_id} of type {strategy_type}")

                    # Subscribe market data feed to instruments in strategy universe
                    # Check if market_data_feed exists (it's initialized in LiveEngine)
                    if hasattr(self, 'market_data_feed') and self.market_data_feed:
                        for symbol in instrument_symbols:
                            # Find the instrument object
                            instrument = self.instruments.get(symbol)
                            if instrument:
                                self.market_data_feed.subscribe(instrument)
                            else:
                                self.logger.warning(f"Instrument '{symbol}' defined in strategy universe not found in main instrument list.")

                    strategy.initialize()
                else:
                    self.logger.error(f"Failed to create strategy of type {strategy_type}")
            except Exception as e:
                self.logger.error(f"Error creating strategy {strategy_id}: {str(e)}")

    def _subscribe_to_events(self):
        """Subscribe to relevant events."""
        # Register with the event_manager
        self.event_manager.subscribe(EventType.MARKET_DATA, self._on_market_data, component_name="TradingEngine")
        self.event_manager.subscribe(EventType.ORDER, self._on_order_event, component_name="TradingEngine")
        self.event_manager.subscribe(EventType.SIGNAL, self._on_signal_event, component_name="TradingEngine")
        self.event_manager.subscribe(EventType.FILL, self._on_fill_event, component_name="TradingEngine")

    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events.

        Args:
            event: Market data event
        """
        # Put event in queue for asynchronous processing
        try:
            if not self.market_data_queue.full():
                self.market_data_queue.put(event, block=False)
                self.logger.debug(f"Queued market data event: {event}")
            else:
                self.logger.warning("Market data queue full, dropping event")
        except Exception as e:
            self.logger.error(f"Error adding market data to queue: {str(e)}")

    def _on_order_event(self, event: OrderEvent):
        """
        Handle order events.

        Args:
            event: Order event
        """
        # Put event in queue for asynchronous processing
        try:
            if not self.order_queue.full():
                self.order_queue.put(event, block=False)
                self.logger.debug(f"Queued order event: {event}")
            else:
                self.logger.warning("Order queue full, dropping event")
        except Exception as e:
            self.logger.error(f"Error adding order event to queue: {str(e)}")

    def _on_signal_event(self, event: SignalEvent):
        """
        Handle strategy signal events.

        Args:
            event: Signal event
        """
        # Put event in queue for asynchronous processing
        try:
            if not self.signal_queue.full():
                self.signal_queue.put(event, block=False)
                self.logger.debug(f"Queued signal event: {event}")
            else:
                self.logger.warning("Signal queue full, dropping event")
        except Exception as e:
            self.logger.error(f"Error adding signal event to queue: {str(e)}")

    def _on_fill_event(self, event: FillEvent):
        """
        Handle fill events.

        Args:
            event: Fill event
        """
        # Put event in queue for asynchronous processing
        try:
            if not self.execution_queue.full():
                self.execution_queue.put(event, block=False)
                self.logger.debug(f"Queued fill event: {event}")
            else:
                self.logger.warning("Execution queue full, dropping event")
        except Exception as e:
            self.logger.error(f"Error adding fill event to queue: {str(e)}")

    def _process_market_data(self):
        """Process market data from the queue."""
        self.logger.info("Market data processing thread started")

        while self.running:
            try:
                # Get next event with timeout
                try:
                    event = self.market_data_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                self.logger.debug(f"Processing market data event: {event}")

                # Update market data manager
                if hasattr(self.data_manager, 'update'):
                    self.data_manager.update(event)

                # Update positions with latest prices
                if hasattr(self.position_manager, 'update_market_prices'):
                    self.position_manager.update_market_prices(event)

                # Update local market data cache
                if hasattr(event, 'instrument') and hasattr(event, 'data'):
                    symbol = event.instrument.symbol
                    self.market_data[symbol] = event.data

                    # Directly notify strategies about this specific symbol
                    for strategy in self.strategies.values():
                        if hasattr(strategy, 'instruments') and symbol in [instr.symbol for instr in strategy.instruments]:
                            self.logger.debug(f"Notifying strategy {strategy.strategy_id} for {symbol}")
                            if hasattr(strategy, 'on_market_data'):
                                strategy.on_market_data(event)

                # Mark as processed
                self.market_data_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error processing market data: {str(e)}")
                time.sleep(0.01)  # Prevent high CPU usage on errors

        self.logger.info("Market data processing thread stopped")

    def _process_orders(self):
        """Process orders from the queue."""
        self.logger.info("Order processing thread started")

        while self.running:
            try:
                # Get next event with timeout
                try:
                    event = self.order_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                self.logger.debug(f"Processing order event: {event}")

                # Update order in local cache
                if hasattr(event, 'data') and hasattr(event.data, 'order_id'):
                    order = event.data
                    self.orders[order.order_id] = order

                # Submit order to broker
                if self.broker and hasattr(self.broker, 'submit_order'):
                    self.broker.submit_order(event.data)

                # Notify relevant strategy
                if hasattr(event.data, 'strategy_id'):
                    strategy_id = event.data.strategy_id
                    if strategy_id in self.strategies:
                        strategy = self.strategies[strategy_id]
                        if hasattr(strategy, 'on_order_update'):
                            strategy.on_order_update(event.data)

                # Mark as processed
                self.order_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error processing order: {str(e)}")
                time.sleep(0.01)  # Prevent high CPU usage on errors

        self.logger.info("Order processing thread stopped")

    def _process_signals(self):
        """Process signals from the queue."""
        self.logger.info("Signal processing thread started")

        while self.running:
            try:
                # Get next event with timeout
                try:
                    event = self.signal_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                self.logger.debug(f"Processing signal event: {event}")

                # Check risk limits
                if self.risk_manager.check_signal(event):
                    # Convert signal to order
                    order_event = self.order_manager.create_order_from_signal(event)
                    if order_event:
                        # Publish to event manager
                        self.event_manager.publish(order_event)
                else:
                    self.logger.info(f"Signal rejected by risk manager: {event}")

                # Mark as processed
                self.signal_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error processing signal: {str(e)}")
                time.sleep(0.01)  # Prevent high CPU usage on errors

        self.logger.info("Signal processing thread stopped")

    def _process_fills(self):
        """Process fill events from the queue."""
        self.logger.info("Fill processing thread started")

        while self.running:
            try:
                # Get next event with timeout
                try:
                    event = self.execution_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                self.logger.debug(f"Processing fill event: {event}")

                # Update position
                if hasattr(self.position_manager, 'apply_fill'):
                    self.position_manager.apply_fill(event)

                # Update portfolio
                if hasattr(self.portfolio, 'update'):
                    self.portfolio.update()

                # Update order status
                if hasattr(event, 'order_id') and event.order_id in self.orders:
                    order = self.orders[event.order_id]
                    order.status = OrderStatus.FILLED

                # Notify strategy
                if hasattr(event, 'strategy_id') and event.strategy_id in self.strategies:
                    strategy = self.strategies[event.strategy_id]
                    if hasattr(strategy, 'on_fill'):
                        strategy.on_fill(event)

                # Mark as processed
                self.execution_queue.task_done()

            except Exception as e:
                self.logger.error(f"Error processing fill: {str(e)}")
                time.sleep(0.01)  # Prevent high CPU usage on errors

        self.logger.info("Fill processing thread stopped")

    def _process_events(self):
        """Process events from the event queue."""
        self.logger.info("Event processing thread started")

        while self.running:
            # Let the event manager process events
            try:
                # Process a batch of events
                processed = self.event_manager.process_events(max_events=100)

                # If no events were processed, sleep briefly
                if processed == 0:
                    time.sleep(0.01)

            except Exception as e:
                self.logger.error(f"Error in event processing: {str(e)}")
                time.sleep(0.01)  # Prevent high CPU usage on errors

        self.logger.info("Event processing thread stopped")

    def _run_strategies(self):
        """Run strategy update loop."""
        self.logger.info("Strategy thread started")

        while self.running and not self.paused:
            current_time = datetime.now()

            for strategy_id, strategy in self.strategies.items():
                try:
                    if hasattr(strategy, 'update'):
                        strategy.update(current_time)
                except Exception as e:
                    self.logger.error(f"Error updating strategy {strategy_id}: {str(e)}")

            # Sleep to avoid excessive CPU usage
            time.sleep(self.heartbeat)

        self.logger.info("Strategy thread stopped")

    def _run_engine(self):
        """Main engine loop."""
        try:
            self.logger.info("Main engine thread started")

            while self.running:
                self.last_heartbeat_time = datetime.now()

                if not self.paused:
                    # Update performance metrics
                    self.performance_tracker.update()

                # Sleep for heartbeat interval
                time.sleep(self.heartbeat)

        except Exception as e:
            self.logger.error(f"Error in main engine loop: {str(e)}", exc_info=True)
            self.stop()

        self.logger.info("Main engine thread stopped")

    def _timer_loop(self):
        """Run a timer loop for periodic events"""
        last_time = time.time()
        
        while self.running and not self.paused:
            current_time = time.time()
            elapsed = current_time - last_time
            
            if elapsed >= self.heartbeat:
                # Publish timer event
                self._publish_timer_event(elapsed)
                last_time = current_time
            
            time.sleep(0.01)  # Small sleep to prevent CPU spinning
    
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
            timestamp=int(time.time() * 1000)
        )
        
        # Add timer details
        event.elapsed = elapsed
        event.timestamp_dt = datetime.now()
        
        # Publish the event
        self.event_manager.publish(event)
        self.logger.debug(f"Published timer event: elapsed={elapsed:.2f}s")
        
        # Update last heartbeat time
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
        
        # Add mode information
        event_data['data']['mode'] = self.config.get('mode', 'unknown')
        
        # Create the event
        event = SystemEvent(
            event_type=EventType.SYSTEM,
            timestamp=int(time.time() * 1000),
            **event_data
        )
        
        # Publish the event
        self.event_manager.publish(event)
        self.logger.info(f"Published system event: {action}")
        
    def start(self):
        """Start the trading engine"""
        if self.running:
            self.logger.warning("Engine is already running")
            return False
            
        self.logger.info("Starting trading engine")
        
        # Set running state
        self.running = True
        self.paused = False
        
        # Publish system start event
        self._publish_system_event('start')
        
        # Start the event manager if not already started
        if hasattr(self.event_manager, 'start'):
            self.event_manager.start()
        
        # Connect to broker if not already connected
        if self.broker and hasattr(self.broker, 'connect'):
            self.broker.connect()
        
        # Start risk manager
        if self.risk_manager and hasattr(self.risk_manager, 'start'):
            self.risk_manager.start()
            
        # Start processing threads
        self.market_data_thread = threading.Thread(target=self._process_market_data, daemon=True)
        self.market_data_thread.start()
        
        self.order_processing_thread = threading.Thread(target=self._process_orders, daemon=True)
        self.order_processing_thread.start()
        
        self.strategy_thread = threading.Thread(target=self._run_strategies, daemon=True)
        self.strategy_thread.start()
        
        # Start the main engine thread
        self._main_thread = threading.Thread(target=self._run_engine, daemon=True)
        self._main_thread.start()
        
        # Start PaperTradingSimulator if it exists
        if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'start'):
            self.paper_trading_simulator.start()

        self.logger.info("Trading engine started")
        return True

    def stop(self):
        """Stop the trading engine"""
        if not self.running:
            self.logger.warning("Engine is not running")
            return
            
        self.logger.info("Stopping trading engine")
        
        # Request shutdown
        self.running = False
        self.paused = True
        
        # Publish system shutdown event
        self._publish_system_event('shutdown')
        
        # Stop strategies
        for strategy in self.strategies.values():
            try:
                strategy.stop()
            except Exception as e:
                self.logger.error(f"Error stopping strategy {strategy.id}: {str(e)}")
        
        # Cancel pending orders
        self._cancel_all_pending_orders()
        
        # Stop components
        if self.risk_manager and hasattr(self.risk_manager, 'stop'):
            self.risk_manager.stop()
            
        if hasattr(self.portfolio, 'stop'):
            self.portfolio.stop()
            
        # Disconnect broker
        if self.broker and hasattr(self.broker, 'disconnect'):
            self.broker.disconnect()
        
        # Wait for threads to stop
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=10.0)
            
        if self.market_data_thread and self.market_data_thread.is_alive():
            self.market_data_thread.join(timeout=5.0)
            
        if self.order_processing_thread and self.order_processing_thread.is_alive():
            self.order_processing_thread.join(timeout=5.0)
            
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.strategy_thread.join(timeout=5.0)
        
        # Stop Event Manager last
        self.event_manager.stop()
        self.logger.info("Event Manager stopped")

        # Stop ExecutionHandler if it has a stop method
        if hasattr(self.execution_handler, 'stop'):
            self.execution_handler.stop()

        # Stop PaperTradingSimulator if it exists
        if self.paper_trading_simulator and hasattr(self.paper_trading_simulator, 'stop'):
            self.paper_trading_simulator.stop()

        self.logger.info("Trading Engine stopped")

    def pause(self):
        """Pause the trading engine."""
        if not self.running:
            self.logger.warning("Trading engine is not running")
            return

        self.paused = True
        self.logger.info("Trading engine paused")

    def resume(self):
        """Resume the trading engine."""
        if not self.running:
            self.logger.warning("Trading engine is not running")
            return

        self.paused = False
        self.logger.info("Trading engine resumed")

    def _cancel_all_pending_orders(self):
        """Cancel all pending orders when stopping the engine."""
        if not self.broker:
            return

        pending_orders = [order for order in self.orders.values()
                         if order.status in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]]

        for order in pending_orders:
            try:
                if hasattr(self.broker, 'cancel_order'):
                    self.broker.cancel_order(order.order_id)
                    self.logger.info(f"Cancelled order {order.order_id}")
            except Exception as e:
                self.logger.error(f"Error cancelling order {order.order_id}: {str(e)}")

    def place_order(self, order: Order) -> bool:
        """
        Place an order through the engine.

        Args:
            order: The order to place

        Returns:
            bool: True if the order was successfully queued, False otherwise
        """
        if not self.running:
            self.logger.error("Cannot place order - engine is not running")
            return False

        # Create order event and publish
        event = OrderEvent(EventType.ORDER, order)
        return self.event_manager.publish(event)

    def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """
        Get the latest market data for a symbol.

        Args:
            symbol: The instrument symbol

        Returns:
            Optional[MarketData]: The latest market data or None if not available
        """
        return self.market_data.get(symbol)

    def get_instrument(self, symbol_or_id: str) -> Optional[Instrument]:
        """
        Get instrument by symbol or ID.

        Args:
            symbol_or_id: The instrument symbol or ID

        Returns:
            Optional[Instrument]: The instrument or None if not found
        """
        return self.instruments.get(symbol_or_id)

    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order by ID.

        Args:
            order_id: The order ID

        Returns:
            Optional[Order]: The order or None if not found
        """
        return self.orders.get(order_id)

    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the trading engine."""
        return {
            "running": self.running,
            "paused": self.paused,
            "last_heartbeat": self.last_heartbeat_time.isoformat() if self.last_heartbeat_time else None,
            "active_instruments": list(self.active_instruments),
            "active_strategies": [s_id for s_id, strategy in self.strategies.items() if hasattr(strategy, 'is_active') and strategy.is_active()],
            "portfolio_value": self.portfolio.total_value if hasattr(self.portfolio, 'total_value') else 0,
            "open_positions": len(self.position_manager.get_open_positions()) if hasattr(self.position_manager, 'get_open_positions') else 0,
            "pending_orders": len([order for order in self.orders.values() if order.status in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]])
        }

    def get_queue_stats(self) -> Dict[str, int]:
        """
        Get statistics about queue sizes.

        Returns:
            Dict with queue statistics
        """
        return {
            "market_data_queue": self.market_data_queue.qsize(),
            "order_queue": self.order_queue.qsize(),
            "signal_queue": self.signal_queue.qsize(),
            "execution_queue": self.execution_queue.qsize(),
            "event_queue": self.event_manager.get_queue_size() if hasattr(self.event_manager, 'get_queue_size') else -1
        }

# if __name__ == "__main__":
#     # Configure logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )

#     # Create and start the engine
#     engine = TradingEngine("config/config.yaml", "config/strategies.yaml")

#     try:
#         engine.initialize()
#         engine.start()

#         # Keep running until interrupted
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("Shutting down...")
#     finally:
#         engine.stop()
