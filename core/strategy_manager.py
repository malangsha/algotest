"""
Strategy Manager module for managing trading strategies.

This module provides the central component for strategy management including:
- Strategy configuration and loading
- Symbol subscription management
- Strategy lifecycle management
- Data distribution to strategies
"""

import threading
import time
import yaml
from typing import Dict, List, Set, Any, Optional, Type
from functools import lru_cache
import importlib
import inspect
from pathlib import Path
from datetime import datetime
import concurrent.futures
from collections import defaultdict

from core.option_manager import OptionManager
from core.event_manager import EventManager
from models.events import Event, EventType, MarketDataEvent, SignalEvent, BarEvent, OptionsSubscribedEvent
from strategies.base_strategy import OptionStrategy
# from utils.config_loader import load_config
from core.logging_manager import get_logger
from strategies.strategy_factory import StrategyFactory
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument, AssetClass
from models.position import Position
from utils.constants import EventPriority, Exchange, SignalType

class StrategyManager:
    """
    Manages strategies and their lifecycle.
    Handles strategy initialization, starting, stopping, and symbol subscriptions.
    """

    def __init__(self, data_manager, event_manager, portfolio_manager, risk_manager, broker, config=None):
        """
        Initialize the StrategyManager.
        """
        self.logger = get_logger("core.strategy_manager")
        self.data_manager = data_manager
        self.event_manager = event_manager
        self.portfolio_manager = portfolio_manager
        self.risk_manager = risk_manager
        self.broker = broker
        self.config = config # Main config dictionary

        # Strategy storage
        self.strategies: Dict[str, OptionStrategy] = {}  # strategy_id -> Strategy instance
        self.strategy_configs: Dict[str, Dict] = {}  # strategy_id -> config dict from strategies.yaml

        # Symbol tracking (using symbol_key like "NSE:RELIANCE" or "NFO:NIFTY...")
        # Stores symbols explicitly defined by strategy OR dynamically added options
        self.strategy_symbols: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set)) # strategy_id -> {timeframe -> set(symbol_keys)}
        # Reverse mapping for efficient event distribution
        self.symbol_strategies: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set)) # symbol_key -> {timeframe -> set(strategy_ids)}

        # Store which strategies need dynamic options for which underlying
        self.strategy_dynamic_underlyings: Dict[str, Set[str]] = defaultdict(set) # strategy_id -> set(underlying_symbols)

        # OptionManager instance - MUST be set after initialization
        self.option_manager: Optional[OptionManager] = None

        # Configuration storage
        self.underlyings_config: List[Dict] = [] # From main config.yaml market.underlyings
        self.strategies_yaml_config: Dict = {} # From strategies.yaml

        # Background threads
        self.monitoring_thread = None
        self.stop_event = threading.Event()

        # Initialize strategy registry and factory
        self.strategy_registry = StrategyRegistry()
        self.strategy_factory = StrategyFactory() # May not be needed if using registry directly

        # Thread pool for parallel strategy processing
        strategy_workers = self.config.get('system', {}).get('strategy_processing_workers', 4)
        self.strategy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=strategy_workers, thread_name_prefix='StrategyWorker')

        # Lock for thread safety when modifying shared structures
        self.lock = threading.RLock()

        self._register_event_handlers()
        self.logger.info("Strategy manager initialized")

    def set_option_manager(self, option_manager: OptionManager):
        """Set the OptionManager instance after initialization."""
        self.option_manager = option_manager
        self.logger.info("OptionManager instance set in StrategyManager.")


    def _register_event_handlers(self):
        """Register event handlers with the event manager."""
        # Market data events (primarily for underlying price updates if needed here)
        # OptionManager handles underlying price updates internally now.
        # self.event_manager.subscribe(EventType.MARKET_DATA, self._handle_market_data, component_name="StrategyManager")

        # Signal events from strategies
        self.event_manager.subscribe(EventType.SIGNAL, self._handle_strategy_signal, component_name="StrategyManager")

        # Bar events to distribute to strategies
        self.event_manager.subscribe(EventType.BAR, self._on_bar, component_name="StrategyManager")

        # Custom event from OptionManager when new options are subscribed
        self.event_manager.subscribe(EventType.CUSTOM, self._handle_custom_event, component_name="StrategyManager")

        self.logger.info("StrategyManager event handlers registered")

    def _handle_custom_event(self, event: Event):
        """Handles custom events, specifically looking for OPTIONS_SUBSCRIBED."""
        if hasattr(event, 'custom_event_type') and event.custom_event_type == "OPTIONS_SUBSCRIBED":
             # Type hint for clarity
             options_event: OptionsSubscribedEvent = event
             self._handle_options_subscribed(options_event)

    def _handle_options_subscribed(self, event: OptionsSubscribedEvent):
        """
        Handles the event published by OptionManager when new options are subscribed.
        Subscribes relevant strategies to the new option symbols via DataManager.
        """
        underlying_symbol = event.underlying_symbol # e.g., "NIFTY INDEX"
        new_option_instruments = event.instruments # List[Instrument]

        if not new_option_instruments:
            return

        self.logger.info(f"Received OptionsSubscribedEvent for underlying '{underlying_symbol}' with {len(new_option_instruments)} new options.")

        with self.lock:
            strategies_to_update = []
            # Find strategies that depend on this underlying for dynamic options
            for strategy_id, dependent_underlyings in self.strategy_dynamic_underlyings.items():
                if underlying_symbol in dependent_underlyings:
                    if strategy_id in self.strategies:
                        strategies_to_update.append(strategy_id)
                    else:
                         self.logger.warning(f"Strategy {strategy_id} depends on {underlying_symbol} but is not loaded.")

            if not strategies_to_update:
                self.logger.debug(f"No strategies found needing dynamic options for {underlying_symbol}.")
                return

            self.logger.debug(f"Strategies needing updates for {underlying_symbol}: {strategies_to_update}")

            # For each new option and each relevant strategy, subscribe via DataManager
            for instrument in new_option_instruments:
                symbol_key = instrument.instrument_id # e.g., NFO:NIFTY...
                if not symbol_key:
                    self.logger.warning(f"Option instrument {instrument.symbol} missing instrument_id. Skipping subscription.")
                    continue

                for strategy_id in strategies_to_update:
                    strategy = self.strategies[strategy_id]
                    # Get all timeframes this strategy uses
                    required_timeframes = set(self.strategy_symbols.get(strategy_id, {}).keys())
                    if not required_timeframes: # If strategy_symbols wasn't populated yet, use config
                         required_timeframes = {strategy.timeframe} | strategy.additional_timeframes

                    if not required_timeframes:
                         self.logger.warning(f"Strategy {strategy_id} has no timeframes defined. Cannot subscribe {symbol_key}.")
                         continue

                    # Subscribe the strategy to this option for all its required timeframes
                    for timeframe in required_timeframes:
                         # Check if already subscribed (though DataManager might handle duplicates)
                         if symbol_key not in self.strategy_symbols[strategy_id][timeframe]:
                             self.logger.debug(f"Subscribing strategy '{strategy_id}' to option '{symbol_key}' on timeframe '{timeframe}'")
                             success = self.data_manager.subscribe_to_timeframe(
                                 instrument=instrument, # Pass the full Instrument object
                                 timeframe=timeframe,
                                 strategy_id=strategy_id
                             )
                             if success:
                                 # Update tracking structures
                                 self.strategy_symbols[strategy_id][timeframe].add(symbol_key)
                                 self.symbol_strategies[symbol_key][timeframe].add(strategy_id)
                                 # self.logger.debug(f"Successfully updated tracking for {strategy_id} -> {symbol_key}@{timeframe}")
                             else:
                                 self.logger.error(f"DataManager failed to subscribe {strategy_id} to {symbol_key}@{timeframe}")
                         # else:
                             # self.logger.debug(f"Strategy {strategy_id} already subscribed to {symbol_key}@{timeframe}")

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
        """Distribute bar events to relevant strategies."""
        if not isinstance(event, BarEvent) or not event.instrument:
            return

        symbol_key = event.instrument.instrument_id # Use the unique ID
        timeframe = event.timeframe

        if not symbol_key:
             # Should not happen if DataManager populates instrument correctly
             self.logger.warning(f"BarEvent missing instrument_id. Symbol: {event.instrument.symbol}")
             return

        # self.logger.debug(f"Received BarEvent for {symbol_key}@{timeframe}")

        # Find strategies subscribed to this symbol_key and timeframe
        # Use .get() to avoid defaultdict creating empty entries unnecessarily
        strategies_for_symbol = self.symbol_strategies.get(symbol_key)
        if strategies_for_symbol:
            subscribed_strategy_ids = strategies_for_symbol.get(timeframe)
            if subscribed_strategy_ids:
                # Use list comprehension for potentially faster creation
                futures = [
                    self.strategy_executor.submit(self.strategies[strategy_id].on_bar, event)
                    for strategy_id in subscribed_strategy_ids if strategy_id in self.strategies
                ]
                # Log if any strategy ID was not found (should indicate an issue)
                for strategy_id in subscribed_strategy_ids:
                     if strategy_id not in self.strategies:
                          self.logger.warning(f"Strategy {strategy_id} subscribed to {symbol_key}@{timeframe} but not found in loaded strategies.")

                # Optional: Handle futures if needed (e.g., logging exceptions)
                # for future in concurrent.futures.as_completed(futures):
                #     try: future.result(timeout=1.0) # Check for exceptions
                #     except Exception as e: self.logger.error(f"Error in parallel on_bar execution: {e}")

            # else: self.logger.debug(f"No strategies subscribed to timeframe {timeframe} for {symbol_key}")
        # else: self.logger.debug(f"No strategies subscribed to symbol {symbol_key}")

    def load_config(self, strategy_config_path='config/strategies.yaml', main_config_path='config/config.yaml'):
        """Load strategy and main configuration."""
        if not self.option_manager:
             self.logger.critical("OptionManager must be set before loading config!")
             return False
        try:
            # Load strategy configuration
            if isinstance(strategy_config_path, dict):
                self.strategies_yaml_config = strategy_config_path
            else:
                with open(strategy_config_path, 'r') as f:
                    self.strategies_yaml_config = yaml.safe_load(f)

            # Load main configuration to get market underlyings
            if isinstance(main_config_path, dict):
                # If main config is already passed in __init__, use it
                if not self.config: self.config = main_config_path
            else:
                try:
                    with open(main_config_path, 'r') as f:
                         # Store main config if not already set
                         if not self.config: self.config = yaml.safe_load(f)
                except Exception as e:
                    self.logger.error(f"Failed to load main configuration from {main_config_path}: {e}")
                    if not self.config: self.config = {} # Ensure self.config is a dict

            # Extract underlyings configuration from main config
            self.underlyings_config = self.config.get('market', {}).get('underlyings', [])
            if not isinstance(self.underlyings_config, list):
                 self.logger.error("market.underlyings in config.yaml should be a list.")
                 self.underlyings_config = []

            # Configure option manager with underlyings
            self.option_manager.configure_underlyings(self.underlyings_config)

            num_strategies = len(self.strategies_yaml_config.get('strategies', {}))
            num_underlyings = len(self.underlyings_config)
            self.logger.info(f"Loaded strategy configuration with {num_strategies} strategies")
            self.logger.info(f"Loaded underlyings configuration with {num_underlyings} underlyings")
            return True

        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}", exc_info=True)
            return False

    def initialize_strategies(self):
        """
        Initialize all strategies based on the loaded configuration.
        Identifies strategies needing dynamic options.
        """
        if not self.strategies_yaml_config:
            self.logger.error("Cannot initialize strategies: strategy configuration not loaded")
            return False
        if not self.option_manager:
             self.logger.critical("OptionManager not set. Cannot initialize strategies.")
             return False

        strategy_configs = self.strategies_yaml_config.get('strategies', {})
        initialized_count = 0

        with self.lock: # Protect access to shared strategy dicts
            self.strategies.clear()
            self.strategy_configs.clear()
            self.strategy_symbols.clear()
            self.symbol_strategies.clear()
            self.strategy_dynamic_underlyings.clear()

            for strategy_id, config in strategy_configs.items():
                if not config.get('enabled', True):
                    self.logger.info(f"Strategy '{strategy_id}' is disabled, skipping.")
                    continue

                try:
                    strategy_type = config.get('type')
                    if not strategy_type:
                         self.logger.error(f"Strategy '{strategy_id}' missing 'type' definition.")
                         continue

                    # Get strategy class from registry
                    strategy_class = self.strategy_registry.get_strategy_class(strategy_type)
                    if not strategy_class:
                        self.logger.error(f"Could not find strategy class for type '{strategy_type}' (ID: {strategy_id})")
                        continue

                    # Create strategy instance
                    # Pass main config for context if needed by strategy (e.g., default exchange)
                    strategy = strategy_class(
                        strategy_id=strategy_id,
                        config=config, # Strategy-specific config from strategies.yaml
                        data_manager=self.data_manager,
                        option_manager=self.option_manager,
                        portfolio_manager=self.portfolio_manager,
                        event_manager=self.event_manager,
                        # main_config=self.config # Optionally pass main config
                    )

                    # Store the strategy and its config
                    self.strategies[strategy_id] = strategy
                    self.strategy_configs[strategy_id] = config

                    # --- Determine Symbol Requirements ---
                    # 1. Explicit symbols (e.g., specific stocks, futures, or *maybe* specific options)
                    #    These should be returned by get_required_symbols() as symbol strings.
                    # 2. Underlyings for dynamic options (e.g., "NIFTY INDEX" for ATM straddle)
                    #    These should be identified, perhaps via a new method or attribute.

                    # Get explicitly required symbols (like underlyings for option strategies)
                    required_symbols_map = strategy.get_required_symbols() # timeframe -> list[symbol_str]
                    explicit_symbol_keys = set()

                    for timeframe, symbols in required_symbols_map.items():
                        for symbol_str in symbols:
                            # Convert symbol string (e.g., "NIFTY INDEX") to Instrument and then symbol_key
                            # Need exchange info - try strategy config, then main config
                            exchange_str = config.get('exchange') # Check strategy config first
                            
                            if not exchange_str:
                                 # Find underlying config in main list
                                 u_conf = next((uc for uc in self.underlyings_config if uc.get('symbol') == symbol_str or uc.get('name') == symbol_str), None)
                                 if u_conf:
                                      exchange_str = u_conf.get('spot_exchange', u_conf.get('exchange'))
                                 else:
                                      exchange_str = self.config.get('market', {}).get('default_exchange', 'NSE') # Fallback

                            # Get/Create Instrument using DataManager helper
                            instrument = self.data_manager.get_or_create_instrument(f"{exchange_str}:{symbol_str}")
                            if instrument and instrument.instrument_id:
                                symbol_key = instrument.instrument_id
                                self.strategy_symbols[strategy_id][timeframe].add(symbol_key)
                                self.symbol_strategies[symbol_key][timeframe].add(strategy_id)
                                explicit_symbol_keys.add(symbol_key)
                            else:
                                 self.logger.error(f"Could not get/create instrument for explicit symbol '{symbol_str}' for strategy '{strategy_id}'")


                    # Check if the strategy needs dynamic options (e.g., based on config or attribute)
                    if config.get('use_options', False) and hasattr(strategy, 'underlyings'):
                         # Store the underlyings this strategy depends on for dynamic options
                         strategy_underlyings = getattr(strategy, 'underlyings', [])
                         if strategy_underlyings:
                              # Process underlyings to ensure they're all strings
                              underlying_symbols = set()
                              for underlying in strategy_underlyings:
                                   if isinstance(underlying, str):
                                        underlying_symbols.add(underlying)
                                   elif isinstance(underlying, dict) and 'symbol' in underlying:
                                        underlying_symbols.add(underlying['symbol'])
                                   else:
                                        self.logger.warning(f"Strategy '{strategy_id}' has invalid underlying format: {underlying}")
                              
                              self.strategy_dynamic_underlyings[strategy_id].update(underlying_symbols)
                              self.logger.debug(f"Strategy '{strategy_id}' registered for dynamic options on underlyings: {underlying_symbols}")
                              
                              # Also ensure the underlying itself is subscribed if not already explicit
                              for underlying_symbol in underlying_symbols:
                                   u_conf = next((uc for uc in self.underlyings_config if uc.get('symbol') == underlying_symbol or uc.get('name') == underlying_symbol), None)
                                   if u_conf:
                                        exchange = u_conf.get('exchange') # Should be Enum from OptionManager config                                        
                                        underlying_key = f"{exchange}:{underlying_symbol}"
                                        if underlying_key not in explicit_symbol_keys:
                                             # Add underlying subscription for all its timeframes
                                             instrument = self.data_manager.get_or_create_instrument(underlying_key)
                                             if instrument:
                                                  for tf in strategy.all_timeframes: # Use strategy's timeframes
                                                       self.strategy_symbols[strategy_id][tf].add(underlying_key)
                                                       self.symbol_strategies[underlying_key][tf].add(strategy_id)
                                                       explicit_symbol_keys.add(underlying_key) # Mark as handled
                                             else: self.logger.error(f"Could not get instrument for underlying {underlying_key}")                                        
                                   else: self.logger.error(f"Config not found for dynamic underlying {underlying_symbol}")


                    self.logger.info(f"Initialized strategy '{strategy_id}' (Type: {strategy_type}). Explicit symbols: {len(explicit_symbol_keys)}. Dynamic Underlyings: {self.strategy_dynamic_underlyings.get(strategy_id, 'None')}")
                    initialized_count += 1

                except Exception as e:
                    self.logger.error(f"Failed to initialize strategy '{strategy_id}': {e}", exc_info=True)

        self.logger.info(f"Initialized {initialized_count} strategies.")
        return initialized_count > 0

    def start_strategies(self):
        """Start all initialized strategies."""
        self.logger.info("Starting strategies...")
        if not self.strategies:
             self.logger.warning("No strategies initialized to start.")
             return

        # Subscribe to initial symbols (underlyings, explicitly defined symbols)
        # Option symbols will be subscribed later via event from OptionManager
        self._subscribe_initial_symbols()

        # Start each strategy's internal logic (e.g., setting state)
        start_count = 0
        for strategy_id, strategy in self.strategies.items():
            try:
                strategy.start() # Calls strategy's on_start and potentially starts threads
                self.logger.info(f"Started strategy '{strategy_id}'")
                start_count += 1
            except Exception as e:
                self.logger.error(f"Error starting strategy '{strategy_id}': {e}", exc_info=True)
        self.logger.info(f"Attempted to start {start_count} strategies.")

    def stop_strategies(self):
        """Stop all running strategies."""
        self.logger.info("Stopping strategies...")

        # Stop each strategy's internal logic
        stop_count = 0
        for strategy_id, strategy in self.strategies.items():
            try:
                strategy.stop() # Calls strategy's on_stop and potentially stops threads
                self.logger.info(f"Stopped strategy '{strategy_id}'")
                stop_count += 1
            except Exception as e:
                self.logger.error(f"Error stopping strategy '{strategy_id}': {e}", exc_info=True)

        # Unsubscribe from all symbols via DataManager
        self._unsubscribe_all_symbols()
        self.logger.info(f"Attempted to stop {stop_count} strategies and unsubscribed symbols.")

    def _subscribe_initial_symbols(self):
        """Subscribe only to the initially known symbols (not dynamic options)."""
        self.logger.info("Subscribing to initial strategy symbols (excluding dynamic options)...")
        subscribed_count = 0
        total_subscriptions = 0

        with self.lock:
            all_initial_subscriptions = defaultdict(set) # symbol_key -> set(timeframes)
            # Gather all unique symbol_key/timeframe pairs needed initially
            for strategy_id, tf_map in self.strategy_symbols.items():
                 # Check if this strategy requires dynamic options
                 requires_dynamic = strategy_id in self.strategy_dynamic_underlyings

                 for timeframe, symbol_keys in tf_map.items():
                      for symbol_key in symbol_keys:
                           # If strategy requires dynamic options, only subscribe the underlying initially
                           is_option = ":NFO" in symbol_key or ":BFO" in symbol_key # Basic check
                           if requires_dynamic and is_option:
                                continue # Skip options for dynamic strategies initially

                           all_initial_subscriptions[symbol_key].add(timeframe)
                           total_subscriptions += 1

            # Subscribe via DataManager
            for symbol_key, timeframes in all_initial_subscriptions.items():
                 instrument = self.data_manager.get_or_create_instrument(symbol_key)
                 if not instrument:
                      self.logger.error(f"Could not get instrument for initial subscription: {symbol_key}")
                      continue

                 for timeframe in timeframes:
                      # Find one strategy ID that needs this combo for the call
                      strategy_id = next((sid for sid, tf_map in self.strategy_symbols.items()
                                          if timeframe in tf_map and symbol_key in tf_map[timeframe]), None)
                      if strategy_id:
                           success = self.data_manager.subscribe_to_timeframe(instrument, timeframe, strategy_id)
                           if success:
                                subscribed_count += 1
                           # else: # Error logged by subscribe_to_timeframe
                           #      pass
                      else: # Should not happen if logic is correct
                           self.logger.error(f"Internal error: No strategy found for initial subscription {symbol_key}@{timeframe}")


        self.logger.info(f"Attempted {subscribed_count}/{total_subscriptions} initial symbol/timeframe subscriptions via DataManager.")

    def _unsubscribe_all_symbols(self):
        """Unsubscribe all strategies from all symbols they were subscribed to."""
        self.logger.info("Unsubscribing strategies from all symbols...")
        unsubscribed_count = 0
        total_unsubscriptions = 0

        with self.lock:
            # Iterate through the reverse map to find all symbol/timeframe pairs
            all_symbol_tf_pairs = []
            for symbol_key, tf_map in self.symbol_strategies.items():
                 instrument = self.data_manager.get_or_create_instrument(symbol_key)
                 if not instrument:
                      self.logger.error(f"Could not get instrument for unsubscription: {symbol_key}")
                      continue
                 for timeframe, strategy_ids in tf_map.items():
                      if strategy_ids:
                           # Need one strategy ID for the unsubscribe call
                           strategy_id = next(iter(strategy_ids))
                           all_symbol_tf_pairs.append((instrument, timeframe, strategy_id))
                           total_unsubscriptions += 1

            # Unsubscribe via DataManager
            for instrument, timeframe, strategy_id in all_symbol_tf_pairs:
                 success = self.data_manager.unsubscribe_from_timeframe(instrument, timeframe, strategy_id)
                 if success:
                      unsubscribed_count += 1
                 # else: # Error logged by unsubscribe_from_timeframe
                 #      pass

            # Clear tracking structures
            self.strategy_symbols.clear()
            self.symbol_strategies.clear()

        self.logger.info(f"Attempted {unsubscribed_count}/{total_unsubscriptions} symbol/timeframe unsubscriptions via DataManager.")

    def start(self):
        """Start the strategy manager: initialize and start strategies, start monitoring."""
        try:
            self.logger.info("Starting Strategy Manager...")
            if not self.option_manager:
                 self.logger.critical("OptionManager not set. Aborting start.")
                 return False

            # Initialize strategies first
            if not self.strategies:
                if not self.initialize_strategies():
                    self.logger.error("Strategy initialization failed. Aborting start.")
                    return False

            # Start the monitoring thread
            self.stop_event.clear()
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_thread_func,
                name="StrategyManagerMonitor"
            )
            self.monitoring_thread.daemon = True
            self.monitoring_thread.start()

            # Start all strategies (subscribes initial symbols)
            self.start_strategies()

            # Start OptionManager tracking (subscribes underlyings, triggers option events)
            if not self.option_manager.start_tracking():
                 self.logger.error("OptionManager failed to start tracking underlyings.")
                 # Decide if this is critical - maybe continue?
                 # For now, log error and continue. Strategies might fail if options don't arrive.

            self.logger.info("Strategy Manager started successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error starting Strategy Manager: {e}", exc_info=True)
            return False

    def stop(self):
        """Stop the strategy manager: stop monitoring, stop strategies, unsubscribe."""
        self.logger.info("Stopping Strategy Manager...")
        # Stop the monitoring thread first
        self.stop_event.set()
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5.0)
            if self.monitoring_thread.is_alive():
                 self.logger.warning("Monitoring thread did not stop gracefully.")

        # Stop all strategies (includes unsubscribing symbols)
        self.stop_strategies()

        # Shutdown the executor
        self.strategy_executor.shutdown(wait=True)

        # Clear strategies
        with self.lock:
            self.strategies.clear()
            self.strategy_configs.clear()
            self.strategy_dynamic_underlyings.clear()

        self.logger.info("Strategy Manager stopped.")

    def _monitoring_thread_func(self):
        """Monitoring thread function."""
        self.logger.info("Strategy monitoring thread started.")
        while not self.stop_event.wait(10.0): # Check every 10 seconds
            try:
                with self.lock: # Access strategies safely
                    for strategy_id, strategy in self.strategies.items():
                        # Check heartbeat
                        if hasattr(strategy, 'last_heartbeat'):
                            if time.time() - strategy.last_heartbeat > 60: # 1 min timeout
                                self.logger.warning(f"Strategy '{strategy_id}' heartbeat timeout (> 60s).")
                        # Add other checks (memory, PnL limits, etc.) if needed
            except Exception as e:
                self.logger.error(f"Error in strategy monitoring loop: {e}", exc_info=True)
        self.logger.info("Strategy monitoring thread stopped.")

    def get_strategy(self, strategy_id: str) -> Optional[OptionStrategy]:
        """Get a strategy instance by ID."""
        with self.lock:
            return self.strategies.get(strategy_id)

    def get_strategy_status(self, strategy_id: str) -> Dict[str, Any]:
        """Get the status of a specific strategy."""
        strategy = self.get_strategy(strategy_id) # Handles locking
        if not strategy:
            return {'error': f"Strategy '{strategy_id}' not found."}
        try:
            return strategy.get_status()
        except Exception as e:
            self.logger.error(f"Error getting status for strategy '{strategy_id}': {e}")
            return {'error': f"Could not retrieve status for {strategy_id}."}

    def get_all_strategies_status(self) -> Dict[str, Dict[str, Any]]:
         """Get the status of all loaded strategies."""
         statuses = {}
         with self.lock:
              strategy_ids = list(self.strategies.keys()) # Get keys under lock
         for strategy_id in strategy_ids:
              statuses[strategy_id] = self.get_strategy_status(strategy_id)
         return statuses
    
    # --- Signal Processing ---
    def _process_signal(self, signal: SignalEvent):
        """Process a signal event generated by a strategy."""
        # TODO: Implement signal processing logic
        # - Forward to OrderManager/Broker
        # - Log signal details
        self.logger.info(f"Processing signal from {signal.strategy_id}: {signal.signal_type} {signal.instrument.symbol}")
        # Example: self.order_manager.create_order_from_signal(signal)

    # --- Methods below might be less relevant with the new event flow ---
    # --- but kept for potential direct interaction/debugging ---

    def request_symbol_subscription(self, strategy_id: str, symbol_key: str, timeframe: str):
        """Manually request subscription for a strategy (use with caution)."""
        if strategy_id not in self.strategies:
            self.logger.error(f"Strategy '{strategy_id}' not found for manual subscription.")
            return False
        instrument = self.data_manager.get_or_create_instrument(symbol_key)
        if not instrument:
            self.logger.error(f"Could not get instrument for manual subscription: {symbol_key}")
            return False

        self.logger.info(f"Manual subscription request: {strategy_id} -> {symbol_key}@{timeframe}")
        success = self.data_manager.subscribe_to_timeframe(instrument, timeframe, strategy_id)
        if success:
             with self.lock:
                  self.strategy_symbols[strategy_id][timeframe].add(symbol_key)
                  self.symbol_strategies[symbol_key][timeframe].add(strategy_id)
        return success

    def request_symbol_unsubscription(self, strategy_id: str, symbol_key: str, timeframe: str):
         """Manually request unsubscription for a strategy (use with caution)."""
         if strategy_id not in self.strategies:
              self.logger.error(f"Strategy '{strategy_id}' not found for manual unsubscription.")
              return False
         instrument = self.data_manager.get_or_create_instrument(symbol_key)
         if not instrument:
              self.logger.error(f"Could not get instrument for manual unsubscription: {symbol_key}")
              return False

         self.logger.info(f"Manual unsubscription request: {strategy_id} -> {symbol_key}@{timeframe}")
         success = self.data_manager.unsubscribe_from_timeframe(instrument, timeframe, strategy_id)
         if success:
              with self.lock:
                   if strategy_id in self.strategy_symbols and timeframe in self.strategy_symbols[strategy_id]:
                        self.strategy_symbols[strategy_id][timeframe].discard(symbol_key)
                   if symbol_key in self.symbol_strategies and timeframe in self.symbol_strategies[symbol_key]:
                        self.symbol_strategies[symbol_key][timeframe].discard(strategy_id)
                        # Clean up if set becomes empty
                        if not self.symbol_strategies[symbol_key][timeframe]:
                             del self.symbol_strategies[symbol_key][timeframe]
                        if not self.symbol_strategies[symbol_key]:
                             del self.symbol_strategies[symbol_key]
         return success
 