import os
import time
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime

from live.market_data_feeds import MarketDataFeed
from models.instrument import Instrument
from models.market_data import MarketData
from models.market_data import Quote
from models.events import MarketDataEvent, OrderEvent, TradeEvent, EventType
from utils.config_loader import ConfigLoader
from utils.exceptions import LiveTradingException
from utils.constants import MarketDataType, InstrumentType
from core.engine import TradingEngine
from core.session_manager_factory import SessionManagerFactory
from utils.constants import MODES
from core.logging_manager import get_logger

class LiveEngine(TradingEngine):
    """
    Live trading engine implementation that orchestrates the entire trading process
    in a real-time production environment.
    """
    def __init__(self, config: Dict[str, Any], strategy_config: Dict[str, Any] = None):
        """
        Initialize the LiveEngine with configuration.

        Args:
            config: Main configuration dictionary
            strategy_config: Strategy configuration dictionary
        """
        # Call the parent constructor to initialize base components
        super().__init__(config, strategy_config)

        self.logger = get_logger(__name__)
        self.logger.info("Initializing Live Trading Engine")

        # Initialize live-specific components
        self.heartbeat_interval = self.config.get("system", {}).get("heartbeat_interval", 5)  # seconds
        self.last_market_data_time = None
        self.last_strategy_run_time = {strategy_id: None for strategy_id in self.strategies}

        # Enhanced monitoring metrics
        self.metrics = {
            "tick_count": 0,
            "order_count": 0,
            "execution_count": 0,
            "error_count": 0,
            "last_tick_time": None
        }

        # Initialize market data feed
        self.instrument_objects = self._get_instrument_objects()
        self.market_data_feed = self.initialize_market_data_feed()
        
        # Connect market data feed to data manager
        if self.market_data_feed and hasattr(self, 'data_manager'):
            self.data_manager.market_data_feed = self.market_data_feed
            self.logger.info("Connected market data feed to data manager")

        # Initialize monitoring thread
        self.monitoring_thread = None

        # Register live-specific event handlers
        self._register_additional_event_handlers()

        self.logger.info("Live Engine initialized successfully")

    def _get_instrument_objects(self) -> List[Instrument]:
        """Convert active instruments to Instrument objects."""
        instrument_objects = []

        # Check if we have active instruments
        if not self.active_instruments:
            self.logger.warning("No active instruments found. Check strategy universe configuration.")
            # Fallback to instruments from config if no active instruments
            if hasattr(self, 'instruments') and self.instruments:
                self.active_instruments = set(self.instruments.keys())
                self.logger.info(f"Using all available instruments as fallback: {self.active_instruments}")

        self.logger.info(f"Active instruments: {self.active_instruments}")

        # Convert string symbols to Instrument objects
        for instrument in self.active_instruments:
            if isinstance(instrument, str) and instrument in self.instruments:
                instrument_objects.append(self.instruments[instrument])
            elif isinstance(instrument, Instrument):
                instrument_objects.append(instrument)

        return instrument_objects

    def initialize_market_data_feed(self):
        """Initialize the market data feed."""
        # Get feed type from market_data config
        market_data_config = self.config.get('market_data', {})
        feed_type = market_data_config.get('live_feed_type', 'simulated')  # Default to simulated if not specified

        self.logger.info(f"Using market data feed type: {feed_type}")        

        # Get feed-specific settings
        feed_settings = market_data_config.get('feed_settings', {}).get(feed_type, {})

        # Check if session manager should be used
        session_manager = None
        broker_type = None

        # Get active broker connection details
        broker_connections = self.config.get('broker_connections', {})
        self.logger.info(f"Broker connections: {broker_connections}")
        active_connection_name = broker_connections.get('active_connection')
        self.logger.info(f"Active connection name: {active_connection_name}")
        

        if active_connection_name:
            for connection in broker_connections.get('connections', []):
                if connection.get('connection_name') == active_connection_name:
                    broker_type = connection.get('broker_type', '').lower()
                    break
        
        self.logger.info(f"feed_type: {feed_type}")

        # Check if broker and feed are compatible for session sharing
        if broker_type and 'finvasia' in feed_type.lower() and broker_type.lower() == 'finvasia':
            # Check if session manager is enabled
            if broker_connections.get('session_manager', {}).get('enabled', False):
                # Get session manager from factory
                session_manager_factory = SessionManagerFactory.get_instance()
                if session_manager_factory:
                    session_manager = session_manager_factory.get_session_manager(broker_type)
                    if session_manager:
                        self.logger.info(f"Using shared session manager for market data feed: {feed_type}")
                        # Add session_manager to feed_settings
                        feed_settings['session_manager'] = session_manager                        
        
        
        # Merge broker API settings if needed
        if self.broker and broker_type != MODES.SIMULATED.value:
            feed_settings.update({
                'user_id': self.broker.user_id,
                'password': self.broker.password,
                'api_key': self.broker.api_key,
                'twofa': self.broker.twofa,
                'vc': self.broker.vc,
                'api_url': self.broker.api_url,
                'websocket_url': self.broker.ws_url
            })

        self.logger.info(f"feed_settings: {feed_settings}")

        # Create the market data feed
        data_feed = MarketDataFeed(
            feed_type=feed_type,
            broker=self.broker,
            instruments=self.instrument_objects,
            event_manager=self.event_manager,
            settings=feed_settings
        )

        return data_feed

    def _register_additional_event_handlers(self):
        """Register live-specific event handlers."""
        self.logger.info("Registering additional event handlers")
        
        # Register for trade events
        self.event_manager.subscribe(
            EventType.TRADE,
            self._on_trade_event,            
            component_name="LiveEngine"
        )

        # Register for risk breach events
        self.event_manager.subscribe(
            EventType.RISK_BREACH,
            self._on_risk_breach,            
            component_name="LiveEngine"
        )

    # override the _on_market_data method to update the metrics
    def _on_market_data(self, event: MarketDataEvent):        
        self.metrics["last_tick_time"] = datetime.now()
        self.metrics["tick_count"] += 1

        # forward to the normal event processing mechanism
        # self.logger.info(f"forwarding market data event to parent engine for futher processing")
        super()._on_market_data(event)

    def _on_order_event(self, event: OrderEvent):
        self.metrics["last_tick_time"] = datetime.now()
        self.metrics["order_count"] += 1

        # forward to the normal event processing mechanism
        # self.logger.info(f"forwarding order event to parent engine for futher processing")
        super()._on_order_event(event)

    def _on_trade_event(self, event: TradeEvent):
        """Handle trade events."""
        self.logger.info(f"Trade executed: {event.trade.trade_id}, instrument: {event.trade.instrument.symbol}")     

        # Track metrics
        self.metrics["execution_count"] += 1

    def _on_risk_breach(self, event):
        """Handle risk breach events."""
        breach_type = event.breach_type if hasattr(event, 'breach_type') else "Unknown"
        self.logger.warning(f"Risk breach detected: {breach_type}")

        # Handle different risk breaches
        if breach_type == "MAX_DRAWDOWN":
            self._emergency_stop("Maximum drawdown exceeded")
        elif breach_type == "MAX_DAILY_LOSS":
            self._emergency_stop("Maximum daily loss exceeded")
        elif breach_type == "MAX_POSITION_SIZE":
            self._reject_oversized_orders()

        # Track metrics
        self.metrics["error_count"] += 1

    def _emergency_stop(self, reason: str):
        """Emergency stop of trading when serious risk breach is detected."""
        self.logger.critical(f"EMERGENCY STOP: {reason}")

        # Cancel all open orders
        if hasattr(self.order_manager, 'get_open_orders'):
            open_orders = self.order_manager.get_open_orders()
            for order in open_orders:
                if self.broker and hasattr(self.broker, 'cancel_order'):
                    self.broker.cancel_order(order)

        # Close all positions
        self._close_all_positions()

        # Stop the engine
        self.stop()

    def _close_all_positions(self):
        """Close all open positions."""
        if hasattr(self.position_manager, 'get_open_positions'):
            positions = self.position_manager.get_open_positions()

            for position in positions:
                self.logger.info(f"Closing position: {position.instrument.symbol}")

                # Create a signal to close the position
                close_signal = {
                    "instrument": position.instrument,
                    "direction": "BUY" if position.direction == "SHORT" else "SELL",
                    "quantity": position.quantity,
                    "strategy_id": position.strategy_id if hasattr(position, 'strategy_id') else "SYSTEM"
                }

                # Convert signal to order event
                if hasattr(self.order_manager, 'create_order_from_signal'):
                    order_event = self.order_manager.create_order_from_signal(close_signal)
                    if order_event and hasattr(self.event_manager, 'publish'):
                        self.event_manager.publish(order_event)

    def _reject_oversized_orders(self):
        """Reject any orders that exceed position size limits."""
        self.logger.warning("Rejecting oversized orders")

    def start(self):
        """Start the live trading engine."""
        if self.running:
            self.logger.warning("Live engine is already running")
            return False

        self.logger.info("Starting live trading engine")

        try:
            # Start market data feed first
            if self.market_data_feed:
                if self.market_data_feed.start():
                    self.logger.info("Market data feed started successfully")
                else:
                    self.logger.error("Failed to start market data feed")
                    self.stop()
                    return False
            else:
                self.logger.error("Market data feed not initialized")
                self.stop()
                return False
            
            # Start base engine components (base engine components have access to market data feed now)
            if not super().start():
                self.logger.error("Failed to start base engine")
                return False

            # Start monitoring thread
            self.monitoring_thread = threading.Thread(target=self._run_monitoring_loop, daemon=True)
            self.monitoring_thread.start()

            self.running = True

            # print event manager statistics
            # self.event_manager.print_event_statistics()
            
            self.logger.info("Event manager started successfully")

            self.logger.info("Live trading engine started successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to start live trading engine: {str(e)}")
            self.stop()
            return False

    def stop(self):
        """Stop the live trading engine."""
        if not self.running:
            self.logger.warning("Live engine is not running")
            return

        self.logger.info("Stopping live trading engine")

        # Stop market data feed first
        try:
            if self.market_data_feed:
                self.market_data_feed.stop()
                self.logger.info("Market data feed stopped")
        except Exception as e:
            self.logger.error(f"Error stopping market data feed: {str(e)}")

        # Wait for monitoring thread to finish
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5.0)

        # Save trading session data
        self._save_session_data()

        # Stop base engine components
        super().stop()

    def _run_monitoring_loop(self):
        """Monitoring loop to check system health."""
        self.logger.info("Monitoring loop started")

        while self.running:
            try:
                # Perform heartbeat checks
                self._perform_heartbeat()

                # Sleep until next check
                time.sleep(self.heartbeat_interval)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(1)  # Shorter sleep on error

        self.logger.info("Monitoring loop stopped")

    def _perform_heartbeat(self):
        """Perform periodic heartbeat and monitoring checks."""
        current_time = datetime.now()

        # Update heartbeat timestamp
        self.last_heartbeat_time = current_time

        # Log heartbeat at debug level with key metrics
        self.logger.debug(f"Heartbeat: {self.get_metrics_summary()}")

        # Check for market data staleness
        self._check_market_data_freshness()

        # Check strategy execution
        self._check_strategy_execution()

        # Check broker connection
        self._check_broker_connection()

        # Log portfolio status (less frequently)
        if current_time.second % 30 == 0:  # Every 30 seconds
            self._log_portfolio_status()

    def _check_market_data_freshness(self):
        """Check if market data is fresh and not stale."""
        if self.metrics["last_tick_time"] is None:
            self.logger.warning("No market data received yet")
            return

        staleness = (datetime.now() - self.metrics["last_tick_time"]).total_seconds()
        max_staleness = self.config.get("system", {}).get("max_market_data_staleness", 60)  # seconds

        if staleness > max_staleness:
            self.logger.warning(f"Market data is stale: {staleness:.2f} seconds since last update")

            # Try to reconnect market data feed if too stale
            if staleness > max_staleness * 2:
                self.logger.error("Market data is critically stale, attempting to restart feed")
                try:
                    if self.market_data_feed:
                        self.market_data_feed.stop()
                        time.sleep(1)
                        self.market_data_feed.start()
                        self.logger.info("Market data feed restarted")
                except Exception as e:
                    self.logger.error(f"Failed to restart market data feed: {str(e)}")

    def _check_strategy_execution(self):
        """Check if strategies are executing as expected."""
        current_time = datetime.now()
        for name, last_time in self.last_strategy_run_time.items():
            if last_time is None:
                self.logger.warning(f"Strategy {name} has not executed yet")
                continue

            time_since_last_run = (current_time - last_time).total_seconds()
            max_interval = self.config.get("system", {}).get("max_strategy_interval", 300)  # seconds

            if time_since_last_run > max_interval:
                self.logger.warning(f"Strategy {name} has not run for {time_since_last_run:.2f} seconds")

    def _check_broker_connection(self):
        """Check if broker connection is still active."""
        if not self.broker.is_connected():
            self.logger.warning("Broker connection lost, attempting to reconnect")
            try:
                self.broker.connect()
                self.logger.info("Successfully reconnected to broker")
            except Exception as e:
                self.logger.error(f"Failed to reconnect to broker: {str(e)}")

                # If connection issues persist, consider stopping
                if not self.broker.is_connected():
                    self.logger.critical("Persistent broker connection issues, considering emergency stop")
                    emergency_stop = self.config.get("live", {}).get("emergency_stop_on_broker_failure", False)
                    if emergency_stop:
                        self._emergency_stop("Persistent broker connection failure")

    def _log_portfolio_status(self):
        """Log current portfolio status."""
        portfolio_value = getattr(self.portfolio, 'total_value', 0)
        positions_count = len(self.position_manager.get_open_positions()) if hasattr(self.position_manager, 'get_open_positions') else 0
        pending_orders = len([o for o in self.orders.values() if o.status == 'PENDING'])

        self.logger.info(f"Portfolio status - Value: {portfolio_value:.2f}, Open positions: {positions_count}, Pending orders: {pending_orders}")

        # Log P&L information if available
        if hasattr(self.portfolio, 'get_daily_pnl'):
            daily_pnl = self.portfolio.get_daily_pnl()
            self.logger.info(f"Daily P&L: {daily_pnl:.2f}")

    def _save_session_data(self):
        """Save trading session data for analysis and reporting."""
        session_id = int(time.time())
        output_dir = self.config.get("system", {}).get("output_dir", "output")

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save portfolio data
        portfolio_file = os.path.join(output_dir, f"portfolio_{session_id}.json")
        if hasattr(self.portfolio, 'save'):
            self.portfolio.save(portfolio_file)

        # Save performance data
        performance_file = os.path.join(output_dir, f"performance_{session_id}.json")
        if hasattr(self.performance_tracker, 'save'):
            self.performance_tracker.save(performance_file)

        # Save trades history
        trades_file = os.path.join(output_dir, f"trades_{session_id}.csv")
        if hasattr(self.position_manager, 'save_trades'):
            self.position_manager.save_trades(trades_file)

        self.logger.info(f"Trading session data saved with session ID: {session_id}")

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of current trading metrics."""
        return {
            "tick_count": self.metrics["tick_count"],
            "order_count": self.metrics["order_count"],
            "execution_count": self.metrics["execution_count"],
            "error_count": self.metrics["error_count"],
            "last_tick_time": self.metrics["last_tick_time"].isoformat() if self.metrics["last_tick_time"] else None
        }

    def get_status(self) -> Dict[str, Any]:
        """Get current status of the trading engine."""
        # Get base status from parent class
        status = super().get_status()

        # Add live-specific status information
        status.update({
            "market_data_last_update": self.metrics["last_tick_time"].isoformat() if self.metrics["last_tick_time"] else None,
            "broker_connected": self.broker.is_connected() if hasattr(self.broker, 'is_connected') else False,
            "strategies_status": {
                name: {"last_run": self.last_strategy_run_time.get(name).isoformat() if self.last_strategy_run_time.get(name) else None}
                for name in self.strategies
            },
            "metrics": self.get_metrics_summary()
        })

        return status

    def add_instrument(self, instrument: Instrument):
        """Add a new instrument to the market data feed."""
        if self.market_data_feed:
            self.market_data_feed.subscribe(instrument)
            self.logger.info(f"Added instrument to market data feed: {instrument.symbol}")
        else:
            self.logger.warning(f"Cannot add instrument {instrument.symbol}: market data feed not initialized")

    def remove_instrument(self, instrument: Instrument):
        """Remove an instrument from the market data feed."""
        if self.market_data_feed:
            self.market_data_feed.unsubscribe(instrument)
            self.logger.info(f"Removed instrument from market data feed: {instrument.symbol}")
        else:
            self.logger.warning(f"Cannot remove instrument {instrument.symbol}: market data feed not initialized")

    def run(self):
        """Start the live trading engine and run until stopped."""
        try:
            self.logger.info("Starting live trading engine run")
            success = self.start()

            if not success:
                self.logger.error("Failed to start live trading engine")
                return False

            # Run the engine loop
            super()._run_engine()

            return True

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, stopping gracefully...")
        except Exception as e:
            self.logger.error(f"Error in live trading: {str(e)}", exc_info=True)
            self.metrics["error_count"] += 1
        finally:
            self.stop()
            self.logger.info("Live trading engine stopped")
            return True

