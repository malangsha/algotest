import os
import time
import threading
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from core.engine import TradingEngine
from models.events import MarketDataEvent, OrderEvent, TradeEvent, EventType
from utils.exceptions import LiveTradingException
from live.market_data_feeds import MarketDataFeed
from models.instrument import Instrument
from utils.constants import MarketDataType, InstrumentType
from models.market_data import MarketData
from models.market_data import Quote
from utils.config_loader import ConfigLoader

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

        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing Live Trading Engine")

        # Initialize additional live-specific events and components
        self.market_events_queue = []
        self.heartbeat_interval = self.config.get("system", {}).get("heartbeat_interval", 5)  # seconds
        self.last_market_data_time = None
        self.last_strategy_run_time = {strategy_id: None for strategy_id in self.strategies}       

        # Enhanced monitoring
        self.monitoring_thread = None
        self.metrics = {
            "tick_count": 0,
            "order_count": 0,
            "execution_count": 0,
            "error_count": 0,
            "last_tick_time": None
        }

        # Initialize the engine
        self.initialize()

        self.instrument_objects = self._get_instrument_objects()
        # Initialize market data feed
        self.market_data_feed = self.initialize_market_data_feed()


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

        instruments_config = self.config.get('instruments', [])
        # Convert string symbols to Instrument objects
        for instrument in self.active_instruments:
            if isinstance(instrument, str):
                # Convert string to Instrument object if it's in self.instruments
                if instrument in self.instruments:
                    instrument_objects.append(self.instruments[instrument])
            elif isinstance(instrument, Instrument):
                instrument_objects.append(instrument)

        return instrument_objects

    def initialize_market_data_feed(self):
        """Initialize the market data feed."""
        # Get the instruments from the config
        instruments_config = self.config.get('instruments', [])

        # Convert string symbols to Instrument objects if needed
        self.instrument_objects = []
        for instrument in self.active_instruments:
            if isinstance(instrument, str):
                # Convert string to Instrument object
                for instr_config in instruments_config:
                    if instr_config.get('symbol') == instrument:
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
                        self.instrument_objects.append(instrument)
            elif isinstance(instrument, Instrument):
                self.instrument_objects.append(instrument)

        self.logger.info(f"Creating market data feed with {len(self.instrument_objects)} instruments")

        # Get feed type from market_data config
        market_data_config = self.config.get('market_data', {})
        feed_type = market_data_config.get('live_feed_type', 'simulated')  # Default to simulated if not specified

        self.logger.info(f"Using market data feed type: {feed_type}")

        # Get feed-specific settings
        feed_settings = market_data_config.get('feed_settings', {}).get(feed_type, {})

        # Merge broker API settings if needed by the feed (e.g., FinvasiaFeed)
        # This assumes feed settings might need credentials or URLs from the active broker connection
        broker_config = None
        if hasattr(self.broker, 'broker_name'):
            broker_name = self.broker.broker_name
            broker_connections = self.config.get('broker_connections', {})
            
            # Handle both list and dictionary formats
            if isinstance(broker_connections, list):
                for cfg in broker_connections:
                    if isinstance(cfg, dict) and cfg.get('connection_name') == broker_name:
                        broker_config = cfg
                        break
            elif isinstance(broker_connections, dict):
                connections = broker_connections.get('connections', [])
                active_connection = broker_connections.get('active_connection')
                
                # Find the active connection config
                for cfg in connections:
                    if isinstance(cfg, dict) and cfg.get('connection_name') == active_connection:
                        broker_config = cfg
                        break

        if self.broker:
            # Pass relevant broker details that the feed might require
            feed_settings['user_id'] = self.broker.user_id
            feed_settings['password'] = self.broker.password
            feed_settings['api_key'] = self.broker.api_key
            feed_settings['twofa'] = self.broker.twofa
            feed_settings['vc'] = self.broker.vc
            feed_settings['api_url'] = self.broker.api_url
            feed_settings['websocket_url'] = self.broker.ws_url

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
        self.logger.info(f"registering additional event handlers")
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

    def _on_market_data(self, event: MarketDataEvent):
        """
        Called when market data is received.
        This is a callback entry point that can be used by the MarketDataFeed.

        Args:
            event: Market data event
        """
        # Update last tick time
        self.metrics["last_tick_time"] = datetime.now()
        self.metrics["tick_count"] += 1

        # forward to the normal event processing mechanism
        self.logger.info(f"forwarding market data event to parent engine for futher processing")
        super()._on_market_data(event)

    def _on_order_event(self, order):
        """
        Called when an order update is received from the broker.

        Args:
            order: Updated order object
        """
        # Update metrics
        self.metrics["order_count"] += 1

        self.logger.info(f"forwarding order event to parent engine for futher processing")
        super()._on_order_event(order)

    def _on_trade_event(self, event: TradeEvent):
        """
        Handle trade events.

        Args:
            event: Trade event
        """
        self.logger.info(f"Trade executed: {event.trade.trade_id}, instrument: {event.trade.instrument.symbol}")

        # Update position
        if hasattr(self.position_manager, 'apply_fill'):
            self.position_manager.apply_fill(event)

        # Update portfolio
        if hasattr(self.portfolio, 'update'):
            self.portfolio.update()

        # Notify strategy of fill
        if hasattr(event, 'strategy_id') and event.strategy_id in self.strategies:
            strategy = self.strategies[event.strategy_id]
            if hasattr(strategy, 'on_fill'):
                strategy.on_fill(event)

        # Track metrics
        self.metrics["execution_count"] += 1

    def _on_risk_breach(self, event):
        """
        Handle risk breach events.

        Args:
            event: Risk breach event
        """
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
        """
        Emergency stop of trading when serious risk breach is detected.

        Args:
            reason: Reason for the emergency stop
        """
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
        # This would typically involve updating risk limits in the risk manager
        # For now, we simply log the action

    def start(self):
        """Start the live trading engine."""
        if self.running:
            self.logger.warning("Live engine is already running")
            return False

        self.logger.info("Starting live trading engine")

        # Call parent's start method to start all processing threads
        if not super().start():
            self.logger.error("Failed to start base engine")
            return False

        # Start market data feed
        try:
            if self.market_data_feed:
                self.market_data_feed.start()
                self.logger.info("Market data feed started successfully")
            else:
                self.logger.error("Market data feed not initialized")
                self.stop()
                return False

        except Exception as e:
            self.logger.error(f"Failed to start market data feed: {str(e)}")
            self.broker.disconnect()
            self.running = False
            raise LiveTradingException("Failed to start live trading engine: market data feed failure")

        # Start monitoring thread
        self.monitoring_thread = threading.Thread(target=self._run_monitoring_loop, daemon=True)
        self.monitoring_thread.start()

        self.logger.info("Live trading engine started successfully")
        return True

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

        # Call parent's stop method to handle base engine components
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
                    # Decision logic for whether to stop or continue with retry
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
            "last_tick_time": self.metrics["last_tick_time"].isoformat() if self.metrics["last_tick_time"] else None,
            "market_data_queue_size": self.market_data_queue.qsize() if hasattr(self, 'market_data_queue') else None,
            "order_queue_size": self.order_queue.qsize() if hasattr(self, 'order_queue') else None,
            "signal_queue_size": self.signal_queue.qsize() if hasattr(self, 'signal_queue') else None,
            "execution_queue_size": self.execution_queue.qsize() if hasattr(self, 'execution_queue') else None
        }

    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the trading engine.
        Enhances the parent's get_status method with live-specific information.

        Returns:
            Dictionary with status information
        """
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
        """
        Add a new instrument to the market data feed.

        Args:
            instrument: Instrument to add
        """
        if self.market_data_feed:
            self.market_data_feed.subscribe(instrument)
            self.logger.info(f"Added instrument to market data feed: {instrument.symbol}")
        else:
            self.logger.warning(f"Cannot add instrument {instrument.symbol}: market data feed not initialized")

    def remove_instrument(self, instrument: Instrument):
        """
        Remove an instrument from the market data feed.

        Args:
            instrument: Instrument to remove
        """
        if self.market_data_feed:
            self.market_data_feed.unsubscribe(instrument)
            self.logger.info(f"Removed instrument from market data feed: {instrument.symbol}")
        else:
            self.logger.warning(f"Cannot remove instrument {instrument.symbol}: market data feed not initialized")

    def run(self):
        """
        Start the live trading engine and run until stopped.
        Entry point used by main.py
        """
        try:
            self.logger.info("Starting live trading engine run")
            success = self.start()

            if not success:
                self.logger.error("Failed to start live trading engine")
                return False

            # # Run continuously until stopped by user (e.g., Ctrl+C)
            # while self.running:
            #     time.sleep(1)

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
    