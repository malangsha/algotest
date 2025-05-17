"""
Base class for option trading strategies.
"""
import time
import uuid
import threading
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any, Set
from datetime import datetime
import pandas as pd

from models.events import (Event,
    EventType, MarketDataEvent, SignalEvent,
    BarEvent, PositionEvent, FillEvent,
    OrderEvent, TimerEvent)

from models.order import Order
from models.position import Position
from utils.constants import (SignalType,
    Timeframe, EventPriority, Exchange)

from models.instrument import (Instrument,
    InstrumentType, AssetClass)
from core.logging_manager import get_logger

class OptionStrategy(ABC):
    """
    Base Strategy class for option trading strategies.
    Provides standardized interface for option strategy implementation.
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager,
                 event_manager, broker=None, strategy_manager=None):
        """
        Initialize the strategy.

        Args:
            strategy_id: Unique identifier for the strategy
            config: Strategy configuration dictionary
            data_manager: Market data manager instance
            option_manager: Option manager for option-specific operations
            portfolio_manager: Portfolio manager for position management
            event_manager: Event manager for event handling
            broker: Broker instance
            strategy_manager: StrategyManager instance
        """
        self.logger = get_logger(f"strategies.{self.__class__.__name__}.{strategy_id}")
        self.id = strategy_id
        self.config = config
        self.data_manager = data_manager
        self.option_manager = option_manager
        self.portfolio_manager = portfolio_manager
        self.event_manager = event_manager
        self.broker = broker # Store broker if passed
        self.strategy_manager = strategy_manager

        # Strategy state
        self.is_running = False
        self.last_run_time = None
        self.last_heartbeat = time.time() # Initialize heartbeat

        # Strategy name and description
        self.name = config.get('name', self.__class__.__name__)
        self.description = config.get('description', '')

        # Performance tracking
        self.signals_generated = 0
        self.successful_trades = 0
        self.failed_trades = 0

        self.max_bars_to_keep = config.get('data', {}).get('max_bars_to_keep', # Adjusted path
                                         config.get('max_bars', 1000))


        # Store instrument_ids (e.g., "NSE:RELIANCE") instead of simple names
        self.used_symbols: Set[str] = set() # Populated by StrategyManager or dynamic requests

        # Timeframe settings
        # Ensure 'timeframe' from config is correctly parsed and used as primary
        self.timeframe = str(config.get('timeframe', '1m')) # Ensure string
        additional_tfs_cfg = config.get('additional_timeframes', [])
        if not isinstance(additional_tfs_cfg, list): # Ensure it's a list
            additional_tfs_cfg = [str(additional_tfs_cfg)] if additional_tfs_cfg else []
        else:
            additional_tfs_cfg = [str(tf) for tf in additional_tfs_cfg]

        self.additional_timeframes = set(additional_tfs_cfg)
        self.all_timeframes = {self.timeframe} | self.additional_timeframes
        if not self.timeframe: # Safety check if primary timeframe is empty
            self.logger.warning(f"Strategy {self.id}: Primary timeframe is empty or missing in config. Defaulting to '1m'.")
            self.timeframe = '1m'
            if '1m' not in self.all_timeframes: self.all_timeframes.add('1m')


        # Position tracking
        self.positions: Dict[str, Position] = {}  # instrument_id -> Position object

        # Thread for strategy execution (if strategy uses its own loop)
        self.strategy_thread = None
        self.thread_stop_event = threading.Event()

        # _register_event_handlers() is called by StrategyManager after strategy instance creation
        # and before strategy.initialize()
        self.logger.info(f"Strategy \"{self.name}\" ({self.id}) base initialized. Primary TF: {self.timeframe}, All TFs: {self.all_timeframes}")


    def initialize(self):
        """Called by StrategyManager after __init__ and event registration."""
        self.logger.info(f"Strategy \"{self.name}\" ({self.id}) performing custom initialization.")
        # Example: Load historical data for initial indicators if needed.
        # This is also where a strategy might request initial symbols if not purely dynamic.
        pass

    def _register_event_handlers(self):
        """
        Register event handlers with the event manager.
        This method is called by StrategyManager.
        """
        self.logger.info(f"Strategy {self.id}: Registering event handlers.")
        # Subscribe to events with the strategy's own ID as component_name for targeted delivery (if EM supports it)
        # or for filtering/logging purposes.

        # MarketDataEvents are for raw ticks. Strategies might need these for options,
        # or if they do their own bar construction (less common if DataManager handles it).
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._handle_market_data, # Internal handler that calls the public on_market_data
            component_name=self.id # Helps EventManager route if it uses component_name for filtering
        )

        # BarEvents:
        # The direct subscription to BarEvent by BaseStrategy is REMOVED here.
        # StrategyManager will be responsible for dispatching BarEvents to strategy.on_bar()
        # based on its knowledge of which strategy is interested in which symbol/timeframe.
        # This avoids the duplicate dispatch issue.

        # self.event_manager.subscribe(
        #     EventType.BAR,
        #     self._handle_bar, # This would be the source of the redundant call
        #     component_name=self.id
        # )

        # PositionEvents inform the strategy about changes to its positions.
        self.event_manager.subscribe(
            EventType.POSITION,
            self._handle_position, # Internal handler
            component_name=self.id
        )

        # FillEvents confirm order executions.
        self.event_manager.subscribe(
            EventType.FILL,
            self._handle_fill, # Internal handler
            component_name=self.id
        )

        # OrderEvents provide updates on order status (e.g., accepted, rejected, pending).
        self.event_manager.subscribe(
            EventType.ORDER,
            self._handle_order, # Internal handler
            component_name=self.id
        )

        # TimerEvents can be used for strategies that need periodic checks or actions
        # not tied directly to market data events.
        if self.config.get('uses_timer', False): # Check config if strategy uses a timer
            self.event_manager.subscribe(
                EventType.TIMER,
                self._handle_timer, # Internal handler
                component_name=self.id
            )

        self.logger.info(f"Strategy {self.id} event handlers registered (Note: BarEvent direct subscription removed from BaseStrategy).")


    def _handle_market_data(self, event: Event):
        """Internal handler for MarketDataEvent."""
        if not isinstance(event, MarketDataEvent) or not event.instrument:
            return
        self.last_heartbeat = time.time()
        instrument_id = event.instrument.instrument_id

        # Strategies should define their logic in on_market_data.
        # BaseStrategy can provide common filtering if desired, e.g., based on self.used_symbols.
        # However, for MarketDataEvent, a strategy might be interested in any option it's trading,
        # even if not explicitly in self.used_symbols (which might track underlyings).
        # For now, pass all MarketDataEvents if the strategy is the component.
        # The strategy's on_market_data can then filter further.
        self.on_market_data(event)


    def _handle_bar(self, event: Event):
        """
        Internal handler for BarEvent.
        NOTE: This method will NO LONGER BE CALLED if the direct BarEvent subscription
        is removed from _register_event_handlers. StrategyManager._on_bar will be the
        sole dispatcher to strategy.on_bar().
        """
        if not isinstance(event, BarEvent) or not event.instrument: return
        self.last_heartbeat = time.time()
        instrument_id = event.instrument.instrument_id
        timeframe = event.timeframe

        # This filtering is now primarily handled by StrategyManager before calling on_bar.
        # If this method were still active, this check would be relevant.
        if instrument_id in self.used_symbols and timeframe in self.all_timeframes:
            self.on_bar(event)
        # else:
            # self.logger.debug(f"Strategy {self.id} received BarEvent for {instrument_id}@{timeframe} but not actively tracking or timeframe mismatch.")


    def _handle_position(self, event: Event):
        if not isinstance(event, PositionEvent) or not hasattr(event, "position") or not event.position:
            self.logger.debug(f"Strategy {self.id} received non-PositionEvent or event with no position data: {type(event)}")
            return
        
        position_obj = event.position
        if not hasattr(position_obj, 'instrument_id') or not position_obj.instrument_id:
            self.logger.warning(f"Strategy {self.id} received PositionEvent with invalid position data (missing instrument_id): {event.position}")
            return
        
        position_instrument_id = position_obj.instrument_id
        if position_obj.quantity == 0:
            removed_pos = self.positions.pop(position_instrument_id, None)
            if removed_pos:
                self.logger.info(f"Strategy {self.id}: Position for {position_instrument_id} (Qty: {removed_pos.quantity}) closed and removed from local cache.")
        else:
            self.positions[position_instrument_id] = position_obj
            self.logger.info(f"Strategy {self.id}: Position for {position_instrument_id} updated in local cache. New Qty: {position_obj.quantity}, Avg Price: {getattr(position_obj, 'average_price', 'N/A')}")
        
        self.on_position(event)


    def _handle_fill(self, event: Event):
        """Internal handler for FillEvent."""
        if not isinstance(event, FillEvent) or not hasattr(event, "instrument_id"): return
        # Strategies usually care about fills for any instrument they might have ordered.
        # Further filtering can be done in the strategy's on_fill method.
        self.on_fill(event)


    def _handle_order(self, event: Event):
        """Internal handler for OrderEvent."""
        if not isinstance(event, OrderEvent) or not hasattr(event, "order") or not event.order \
           or not hasattr(event.order, "instrument_id"):
            return
        # Similar to FillEvent, strategies generally want to see all their order updates.
        self.on_order(event)


    def _handle_timer(self, event: Event):
        """Internal handler for TimerEvent."""
        if not isinstance(event, TimerEvent): return
        self.on_timer(event)


    def start(self):
        """
        Start the strategy.
        Called by StrategyManager.
        """
        if self.is_running:
            self.logger.warning(f"Strategy {self.id} is already running.")
            return

        self.is_running = True
        self.thread_stop_event.clear()

        # If the strategy has its own periodic execution loop (run_iteration)
        iteration_interval_cfg = self.config.get("iteration_interval_seconds")
        if iteration_interval_cfg is not None:
            try:
                iteration_interval = float(iteration_interval_cfg)
                if iteration_interval > 0:
                    self.strategy_thread = threading.Thread(
                        target=self._strategy_thread_func,
                        name=f"Strategy_{self.id}_Thread"
                    )
                    self.strategy_thread.daemon = True
                    self.strategy_thread.start()
                    self.logger.info(f"Strategy {self.id} internal thread started with interval {iteration_interval}s.")
                else:
                    self.logger.info(f"Strategy {self.id} iteration_interval_seconds is <= 0, internal thread not started. Will be event-driven.")
            except ValueError:
                self.logger.error(f"Strategy {self.id} has invalid iteration_interval_seconds: {iteration_interval_cfg}. Internal thread not started.")
        else:
            self.logger.info(f"Strategy {self.id} is purely event-driven (no iteration_interval_seconds configured).")


        # Call the strategy's on_start method for any specific startup logic
        try:
            self.on_start()
        except Exception as e:
            self.logger.error(f"Error during on_start for strategy {self.id}: {e}", exc_info=True)
            # Decide if strategy should be stopped or marked as failed
            self.is_running = False # Example: stop if on_start fails
            return

        self.logger.info(f"Strategy {self.id} started.")


    def stop(self):
        """
        Stop the strategy.
        Called by StrategyManager.
        """
        if not self.is_running:
            self.logger.warning(f"Strategy {self.id} is not running.")
            return

        self.is_running = False # Set flag first
        self.thread_stop_event.set() # Signal internal thread to stop

        if self.strategy_thread and self.strategy_thread.is_alive():
            self.logger.debug(f"Waiting for strategy {self.id} internal thread to join...")
            self.strategy_thread.join(timeout=5.0)
            if self.strategy_thread.is_alive():
                self.logger.warning(f"Strategy {self.id} internal thread did not join in time.")
            else:
                self.logger.debug(f"Strategy {self.id} internal thread joined.")
        self.strategy_thread = None


        try:
            self.on_stop() # Call strategy-specific cleanup
        except Exception as e:
            self.logger.error(f"Error during on_stop for strategy {self.id}: {e}", exc_info=True)

        self.logger.info(f"Strategy {self.id} stopped.")


    def _strategy_thread_func(self):
        """
        Strategy's own periodic execution thread function.
        Runs the strategy's run_iteration method periodically if configured.
        """
        iteration_interval = float(self.config.get("iteration_interval_seconds", 1.0)) # Should be validated before starting thread
        self.logger.info(f"Strategy {self.id} internal execution thread started (interval: {iteration_interval}s).")

        while not self.thread_stop_event.wait(iteration_interval): # wait method returns True if event set, False on timeout
            if not self.is_running: # Double check running flag
                break
            try:
                self.run_iteration()
                self.last_run_time = time.time() # Record last successful iteration
            except Exception as e:
                self.logger.error(f"Error in strategy {self.id} run_iteration: {e}", exc_info=True)
                # Potentially add error handling like temporary pause or max error count
        self.logger.info(f"Strategy {self.id} internal execution thread stopped.")


    def run_iteration(self):
        """
        Run a single iteration of the strategy.
        This method is called periodically by the strategy's internal thread if configured.
        Strategies that are purely event-driven (reacting to on_bar, on_market_data)
        might not need to implement this or have an internal thread.
        """
        self.last_heartbeat = time.time() # Update heartbeat if iteration is running
        # Default implementation does nothing. Override in concrete strategies if needed.
        pass


    def generate_signal(self, instrument_id: str, signal_type: SignalType, data: Dict[str, Any] = None, priority: EventPriority = EventPriority.NORMAL):
        if not instrument_id:
            self.logger.error("Cannot generate signal: instrument_id is required.")
            return
        if not signal_type: # Assuming SignalType is an Enum
            self.logger.error("Cannot generate signal: signal_type is required.")
            return

        instrument_obj = self.data_manager.get_instrument(instrument_id)
        if not instrument_obj:
            self.logger.error(f"Cannot generate signal: Instrument not found for id '{instrument_id}'. Signal generation aborted.")
            # Fallback to create a basic instrument is risky as it might lack crucial details (lot_size, tick_size).
            # It's better to ensure instruments are properly managed by DataManager/OptionManager.
            return

        signal_event = SignalEvent(
            timestamp=datetime.now(), # Consider using event timestamp from market data if signal is derived from it
            instrument=instrument_obj,
            signal_type=signal_type,
            strategy_id=self.id,
            data=data or {}, # Ensure data is always a dict
            priority=priority
        )
        self.event_manager.publish(signal_event)
        self.signals_generated += 1
        self.logger.info(f"Strategy {self.id} generated {signal_type.value} signal for {instrument_id} (Priority: {priority.name}). Data: {data}")


    def get_bars(self, instrument_id: str, timeframe: str = None, limit: int = None,
                   start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Retrieves historical bar data for a given instrument and timeframe.
        Delegates to DataManager.
        """
        tf_to_use = timeframe if timeframe else self.timeframe
        if not tf_to_use: # Should not happen if self.timeframe is initialized properly
            self.logger.error(f"Strategy {self.id}: Cannot get bars for {instrument_id}, no timeframe specified or defaulted.")
            return None
            
        self.logger.debug(f"Strategy {self.id} requesting bars for {instrument_id}@{tf_to_use} (Limit: {limit}, Start: {start_time}, End: {end_time})")
        return self.data_manager.get_historical_data(symbol=instrument_id, timeframe=tf_to_use, n_bars=limit, start_time=start_time, end_time=end_time)


    def get_position(self, instrument_id: str) -> Optional[Position]:
        """Retrieves the current position for a given instrument_id from the local cache."""
        return self.positions.get(instrument_id)


    def has_position(self, instrument_id: str) -> bool:
        """Checks if there is an active position for the given instrument_id."""
        pos = self.get_position(instrument_id)
        return pos is not None and pos.quantity != 0


    def get_position_side(self, instrument_id: str) -> Optional[str]:
        """
        Get the side ('long', 'short') of the current position for an instrument_id.
        Returns None if no position or flat.
        """
        position = self.get_position(instrument_id)
        if not position or position.quantity == 0:
            return None
        return 'long' if position.quantity > 0 else 'short'


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


    # --- Abstract methods to be implemented by concrete strategies ---
    @abstractmethod
    def on_bar(self, event: BarEvent):
        """
        Called when a new bar is available for a subscribed instrument and timeframe.
        This is the primary method where strategies implement their trading logic.
        """
        pass # pragma: no cover

    # --- Optional methods that concrete strategies can override ---
    def on_market_data(self, event: MarketDataEvent):
        """
        Called when a new market data tick/quote is available.
        Useful for strategies that need to react to raw ticks,
        e.g., for options pricing or very high-frequency logic.
        """
        pass # pragma: no cover

    def on_position(self, event: PositionEvent):
        """Called when there's an update to the strategy's positions."""
        pass # pragma: no cover

    def on_fill(self, event: FillEvent):
        """Called when an order is filled (partially or fully)."""
        pass # pragma: no cover

    def on_order(self, event: OrderEvent):
        """Called when there's an update to an order's status."""
        pass # pragma: no cover

    def on_timer(self, event: TimerEvent):
        """
        Called periodically if the strategy is configured with `uses_timer: true`
        and a Timer service is publishing TimerEvents.
        """
        pass # pragma: no cover

    def on_start(self):
        """
        Called once when the strategy is started by the StrategyManager.
        Use for one-time setup, loading historical data, or initial symbol requests.
        """
        self.logger.debug(f"Strategy {self.id} on_start() called from BaseStrategy.")
        pass # pragma: no cover

    def on_stop(self):
        """
        Called once when the strategy is stopped by the StrategyManager.
        Use for cleanup, closing open positions, or saving state.
        """
        self.logger.debug(f"Strategy {self.id} on_stop() called from BaseStrategy.")
        pass # pragma: no cover

     # --- Helper methods (examples, can be expanded) ---
    def _get_instrument_details(self, instrument_id: str) -> Optional[Instrument]:
        """Helper to get full instrument details from DataManager."""
        return self.data_manager.get_instrument(instrument_id)

    def get_status(self) -> Dict[str, Any]:
        """Return current status of the strategy."""
        # Simplified status, can be expanded
        return {
            "id": self.id,
            "name": self.name,
            "is_running": self.is_running,
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "last_heartbeat": datetime.fromtimestamp(self.last_heartbeat).isoformat(),
            "signals_generated": self.signals_generated,
            "used_symbols_count": len(self.used_symbols), # Number of unique instrument_ids this strategy is using
            "primary_timeframe": self.timeframe,
            "additional_timeframes": list(self.additional_timeframes),
            "positions_count": len(self.positions) # Number of open positions held by this strategy
        }

