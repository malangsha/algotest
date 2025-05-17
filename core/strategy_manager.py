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
from models.events import (Event,
        EventType, MarketDataEvent,
        SignalEvent, BarEvent, OptionsSubscribedEvent)
from strategies.base_strategy import OptionStrategy
from core.logging_manager import get_logger
from strategies.strategy_factory import StrategyFactory
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument, AssetClass, InstrumentType
from models.position import Position
from utils.constants import EventPriority, Exchange, SignalType, SYMBOL_MAPPINGS

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
        self.config = config

        # Strategy storage
        self.strategies: Dict[str, OptionStrategy] = {}  # strategy_id -> Strategy instance
        self.strategy_configs: Dict[str, Dict] = {}  # strategy_id -> config dict from strategies.yaml

        # Symbol tracking (using symbol_key like "NSE:RELIANCE" or "NFO:NIFTY...")
        self.strategy_symbols: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set)) # strategy_id -> {timeframe -> set(symbol_keys)}
        self.symbol_strategies: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set)) # symbol_key -> {timeframe -> set(strategy_ids)}
        self.strategy_dynamic_underlyings: Dict[str, Set[str]] = defaultdict(set) # strategy_id -> set(underlying_symbols)

        # OptionManager instance - MUST be set after initialization
        self.option_manager: Optional[OptionManager] = None
        self.underlyings_config: List[Dict] = [] # From main config.yaml market.underlyings
        self.strategies_yaml_config: Dict = {} # From strategies.yaml

        # Background threads
        self.monitoring_thread = None
        self.stop_event = threading.Event()
        self.strategy_registry = StrategyRegistry()
        self.strategy_factory = StrategyFactory()

        # Thread pool for parallel strategy processing
        strategy_workers = self.config.get('system', {}).get('strategy_processing_workers', 4)
        self.strategy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=strategy_workers, thread_name_prefix='StrategyWorker')
        self.lock = threading.RLock()

        self._register_event_handlers()
        self.logger.info("Strategy manager initialized")

    def set_option_manager(self, option_manager: OptionManager):
        """Set the OptionManager instance after initialization."""
        self.option_manager = option_manager
        self.logger.info("OptionManager instance set in StrategyManager.")


    def _register_event_handlers(self):
        self.event_manager.subscribe(EventType.SIGNAL, self._handle_strategy_signal, component_name="StrategyManager")
        self.event_manager.subscribe(EventType.BAR, self._on_bar, component_name="StrategyManager")
        self.event_manager.subscribe(EventType.CUSTOM, self._handle_custom_event, component_name="StrategyManager")
        self.logger.info("StrategyManager event handlers registered")

    def _handle_custom_event(self, event: Event):
        if hasattr(event, 'custom_event_type') and event.custom_event_type == "OPTIONS_SUBSCRIBED":
             options_event: OptionsSubscribedEvent = event
             self._handle_options_subscribed(options_event)

    def _handle_options_subscribed(self, event: OptionsSubscribedEvent):
        underlying_symbol = event.underlying_symbol
        new_option_instruments = event.instruments
        if not new_option_instruments: return

        self.logger.info(f"Received OptionsSubscribedEvent for underlying '{underlying_symbol}' with {len(new_option_instruments)} new options.")
        with self.lock:
            strategies_to_update = [sid for sid, unds in self.strategy_dynamic_underlyings.items() if underlying_symbol in unds and sid in self.strategies]
            if not strategies_to_update:
                self.logger.debug(f"No strategies found dynamically tracking options for underlying {underlying_symbol}.")
                return

            for instrument in new_option_instruments:
                symbol_key = instrument.instrument_id
                if not symbol_key:
                    self.logger.warning(f"Option instrument {instrument.symbol} missing instrument_id. Skipping subscription.")
                    continue
                for strategy_id in strategies_to_update:
                    strategy = self.strategies[strategy_id]
                    # Determine the timeframes required by the strategy for these new option symbols
                    # This might be the strategy's primary timeframe or specific timeframes if configured for options
                    required_timeframes = {strategy.timeframe} | strategy.additional_timeframes
                    if not required_timeframes:
                        self.logger.warning(f"Strategy {strategy_id} has no timeframes defined. Cannot subscribe {symbol_key}.")
                        continue

                    for timeframe in required_timeframes:
                         if symbol_key not in self.strategy_symbols[strategy_id].get(timeframe, set()):
                             self.logger.info(f"StrategyManager: Subscribing strategy '{strategy_id}' to newly available option '{symbol_key}' on timeframe '{timeframe}'")
                             success = self.data_manager.subscribe_to_timeframe(
                                 instrument=instrument, timeframe=timeframe, strategy_id=strategy_id
                             )
                             if success:
                                 self.strategy_symbols[strategy_id].setdefault(timeframe, set()).add(symbol_key)
                                 self.symbol_strategies[symbol_key].setdefault(timeframe, set()).add(strategy_id)
                                 self.logger.debug(f"Strategy '{strategy_id}' successfully subscribed to {symbol_key}@{timeframe} via DataManager.")
                             else:
                                 self.logger.error(f"StrategyManager: DataManager failed to subscribe strategy '{strategy_id}' to {symbol_key}@{timeframe}")
                         else:
                             self.logger.debug(f"Strategy '{strategy_id}' already subscribed to {symbol_key}@{timeframe}.")


    def _handle_strategy_signal(self, event: Event):
        if not isinstance(event, SignalEvent): return
        if self.risk_manager and not self.risk_manager.validate_signal(event):
            self.logger.warning(f"Signal rejected by risk manager: {event}")
            return
        self._process_signal(event)

    def _on_bar(self, event: BarEvent):
        # self.logger.debug(f"StrategyManager received BarEvent: {event}") # Can be very verbose
        if not (isinstance(event, BarEvent) and event.instrument and event.instrument.instrument_id):
            if isinstance(event, BarEvent) and event.instrument:
                self.logger.warning(f"BarEvent missing instrument_id. Symbol: {event.instrument.symbol}")
            return

        symbol_key = event.instrument.instrument_id
        timeframe = event.timeframe

        subscribed_strategy_ids = self._get_subscribed_strategies(symbol_key, timeframe)
        if not subscribed_strategy_ids:
            self.logger.debug(f"No strategies subscribed to {symbol_key}@{timeframe}")
            return

        # Pre-filter valid strategies to avoid checking inside the loop
        # Make a copy of the strategy IDs set to avoid issues if it's modified elsewhere during iteration
        # (though with the current lock, it should be safe, this is defensive)
        strategies_to_notify = []
        with self.lock: # Lock to safely access self.strategies
            for sid in list(subscribed_strategy_ids): # Iterate over a copy
                if sid in self.strategies:
                    strategies_to_notify.append(self.strategies[sid])
                else:
                    self.logger.warning(f"Strategy {sid} subscribed to {symbol_key}@{timeframe} but not loaded/found in self.strategies.")
        
        # self.logger.debug(f"_on_bar: symbol_key: {symbol_key}, timeframe: {timeframe}, strategies_to_notify: {strategies_to_notify}")
        if strategies_to_notify:
            self.logger.debug(f"Dispatching BarEvent for {symbol_key}@{timeframe} to {len(strategies_to_notify)} strategies.")
            # Submit all valid strategies at once
            # No lock needed here for self.strategy_executor.submit, it's thread-safe
            futures = [
                self.strategy_executor.submit(strategy.on_bar, event) # Call the instance's on_bar method
                for strategy in strategies_to_notify
            ]
            # Optionally, handle futures if needed (e.g., for results or exceptions)
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result(timeout=1) # Add a timeout
                except Exception as e:
                    self.logger.error(f"Error in strategy on_bar execution for {symbol_key}@{timeframe}: {e}", exc_info=True)


    @lru_cache(maxsize=1024)
    def _get_subscribed_strategies(self, symbol_key: str, timeframe: str) -> Set[str]:
        """
        Cache lookups for subscribed strategies to avoid dictionary navigation overhead.
        """
        # Ensure thread safety when accessing shared self.symbol_strategies
        with self.lock:
            strategies_for_symbol_timeframe = self.symbol_strategies.get(symbol_key, {}).get(timeframe, set())
            return strategies_for_symbol_timeframe.copy() # Return a copy for safety


    def load_config(self, strategy_config_path="config/strategies.yaml", main_config_path="config/config.yaml"):
        if not self.option_manager:
             self.logger.critical("OptionManager must be set before loading config!")
             return False
        try:
            if isinstance(strategy_config_path, dict):
                self.strategies_yaml_config = strategy_config_path
            else:
                with open(strategy_config_path, "r") as f: self.strategies_yaml_config = yaml.safe_load(f)

            if isinstance(main_config_path, dict):
                if not self.config: self.config = main_config_path # Use if self.config not set during __init__
                # If self.config was set, you might want to merge or update, not just overwrite.
                # For now, this assumes if self.config exists, it's the primary one.
            else:
                try:
                    with open(main_config_path, "r") as f:
                         main_cfg_data = yaml.safe_load(f)
                         if not self.config: self.config = main_cfg_data
                         # else: self.config.update(main_cfg_data) # Example: merging
                except Exception as e:
                    self.logger.error(f"Failed to load main config from {main_config_path}: {e}")
                    if not self.config: self.config = {} # Ensure self.config is a dict

            self.underlyings_config = self.config.get("market", {}).get("underlyings", [])
            if not isinstance(self.underlyings_config, list):
                 self.logger.error("market.underlyings in config.yaml should be a list.")
                 self.underlyings_config = []

            self.option_manager.configure_underlyings(self.underlyings_config)
            num_strategies = len(self.strategies_yaml_config.get("strategies", {}))
            num_underlyings = len(self.underlyings_config)
            self.logger.info(f"Loaded strategy config with {num_strategies} strategies, {num_underlyings} underlyings from config.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}", exc_info=True)
            return False

    def initialize_strategies(self):
        if not self.strategies_yaml_config:
            self.logger.error("Cannot initialize strategies: strategies.yaml config not loaded.")
            return False
        if not self.option_manager:
            self.logger.critical("OptionManager not set. Cannot initialize strategies.")
            return False

        strategy_configs = self.strategies_yaml_config.get("strategies", {})
        if not strategy_configs:
            self.logger.warning("No strategies found in strategies.yaml configuration.")
            return True # No strategies to init, but not an error state for SM itself.

        initialized_count = 0
        with self.lock:
            self.strategies.clear()
            self.strategy_configs.clear()
            self.strategy_symbols.clear()
            self.symbol_strategies.clear()
            self.strategy_dynamic_underlyings.clear()

            for strategy_id, config_data in strategy_configs.items():
                if not config_data.get("enabled", True):
                    self.logger.info(f"Strategy '{strategy_id}' is disabled in config. Skipping.")
                    continue
                try:
                    strategy_type = config_data.get("type")
                    if not strategy_type:
                        self.logger.error(f"Strategy '{strategy_id}' missing 'type' in config. Skipping.")
                        continue

                    strategy_class = self.strategy_registry.get_strategy_class(strategy_type)
                    if not strategy_class:
                        self.logger.error(f"Strategy type '{strategy_type}' not found in registry for '{strategy_id}'. Skipping.")
                        continue

                    # 1. Instantiate the strategy
                    strategy = strategy_class(
                        strategy_id=strategy_id, config=config_data,
                        data_manager=self.data_manager,
                        option_manager=self.option_manager,
                        portfolio_manager=self.portfolio_manager,
                        event_manager=self.event_manager,
                        broker=self.broker,
                        strategy_manager=self
                    )
                    
                    # 2. Register its event handlers
                    # This allows the strategy instance to subscribe to events it needs directly,
                    # like MarketDataEvent, PositionEvent, FillEvent, OrderEvent.
                    # For BarEvent, the subscription in BaseStrategy will be modified to avoid redundancy.
                    if hasattr(strategy, '_register_event_handlers') and callable(strategy._register_event_handlers):
                        strategy._register_event_handlers()
                        self.logger.info(f"Called _register_event_handlers for strategy '{strategy_id}'.")
                    else:
                        self.logger.warning(f"Strategy '{strategy_id}' of type '{strategy_type}' does not have a _register_event_handlers method.")

                    # 3. Call strategy-specific initialization
                    strategy.initialize()

                    self.strategies[strategy_id] = strategy
                    self.strategy_configs[strategy_id] = config_data
                    initialized_count += 1
                    self.logger.info(f"Initialized strategy: {strategy_id} (Type: {strategy_type})")

                    # Handle dynamic options subscriptions for underlyings specified in strategy config
                    dynamic_underlyings_cfg = config_data.get("underlyings", []) # Using "underlyings" key as per HFMOS
                    if not isinstance(dynamic_underlyings_cfg, list):
                        dynamic_underlyings_cfg = [dynamic_underlyings_cfg] if dynamic_underlyings_cfg else []

                    for u_conf in dynamic_underlyings_cfg:
                        underlying_sym_name = u_conf.get("name") # e.g., "NIFTY INDEX"
                        if underlying_sym_name:
                            self.strategy_dynamic_underlyings[strategy_id].add(underlying_sym_name)
                            # OptionManager.subscribe_underlying ensures the underlying's feed is active via DataManager
                            if self.option_manager.subscribe_underlying(underlying_sym_name):
                                self.logger.info(f"Strategy '{strategy_id}' requests dynamic options for '{underlying_sym_name}'. Underlying subscribed via OptionManager.")
                            else:
                                self.logger.error(f"Failed to subscribe to underlying '{underlying_sym_name}' for strategy '{strategy_id}' via OptionManager.")

                    # Subscribe to explicitly defined symbols (non-dynamic options, e.g., specific stocks, futures, or even specific options)
                    # This uses the "symbols" key in the strategy's config from strategies.yaml
                    explicit_symbols_cfg = config_data.get("symbols", [])
                    if not isinstance(explicit_symbols_cfg, list):
                        explicit_symbols_cfg = [explicit_symbols_cfg] if explicit_symbols_cfg else []

                    for symbol_config in explicit_symbols_cfg:
                        symbol_name = symbol_config.get("name")
                        exchange_str = symbol_config.get("exchange")
                        # Timeframes for this specific symbol, defaults to strategy's main + additional timeframes
                        timeframes_to_sub = symbol_config.get("timeframes", [strategy.timeframe] + list(strategy.additional_timeframes))

                        if not symbol_name or not exchange_str:
                            self.logger.warning(f"Strategy '{strategy_id}': Explicit symbol config missing name or exchange: {symbol_config}. Skipping.")
                            continue
                        try:
                            exchange_enum = Exchange(exchange_str.upper())
                        except ValueError:
                            self.logger.warning(f"Strategy '{strategy_id}': Invalid exchange '{exchange_str}' for symbol '{symbol_name}'. Skipping.")
                            continue

                        # Determine instrument type and asset class (basic inference)
                        # This could be enhanced by adding explicit type/class in symbol_config
                        inst_type_str = symbol_config.get("instrument_type", "EQUITY").upper()
                        asset_cls_str = symbol_config.get("asset_class", "EQUITY").upper()
                        try:
                            inst_type = InstrumentType(inst_type_str)
                            asset_cls = AssetClass(asset_cls_str)
                        except ValueError:
                            self.logger.warning(f"Strategy '{strategy_id}': Invalid instrument_type '{inst_type_str}' or asset_class '{asset_cls_str}' for '{symbol_name}'. Using defaults.")
                            inst_type = InstrumentType.EQUITY
                            asset_cls = AssetClass.EQUITY
                        
                        # Construct instrument_id
                        instrument_id = f"{exchange_enum.value}:{symbol_name}"
                        
                        # Create Instrument object
                        # For options defined here, more details (strike, option_type, expiry) would be needed in symbol_config
                        instrument_details = {
                            "symbol": symbol_name, "exchange": exchange_enum,
                            "instrument_type": inst_type, "asset_class": asset_cls,
                            "instrument_id": instrument_id
                        }
                        # Add option-specific details if provided in symbol_config
                        if inst_type == InstrumentType.OPTION:
                            instrument_details["option_type"] = symbol_config.get("option_type") # CE or PE
                            instrument_details["strike_price"] = symbol_config.get("strike_price")
                            # Expiry date needs careful handling (parsing string to date object)
                            expiry_str = symbol_config.get("expiry_date")
                            if expiry_str:
                                try: instrument_details["expiry_date"] = datetime.strptime(expiry_str, "%Y-%m-%d").date() # Example format
                                except ValueError: self.logger.error(f"Invalid expiry_date format for {symbol_name}: {expiry_str}")
                            instrument_details["underlying_symbol_key"] = symbol_config.get("underlying_symbol_key")


                        instrument = Instrument(**instrument_details)

                        for tf_str in set(timeframes_to_sub): # Ensure unique timeframes
                            if not tf_str: continue # Skip empty timeframe strings
                            self.logger.info(f"Strategy '{strategy_id}': Subscribing to explicit symbol '{instrument.instrument_id}' on timeframe '{tf_str}'")
                            success = self.data_manager.subscribe_to_timeframe(
                                instrument=instrument, timeframe=tf_str, strategy_id=strategy_id
                            )
                            if success:
                                self.strategy_symbols[strategy_id].setdefault(tf_str, set()).add(instrument.instrument_id)
                                self.symbol_strategies[instrument.instrument_id].setdefault(tf_str, set()).add(strategy_id)
                                self.logger.debug(f"Strategy '{strategy_id}' successfully subscribed to {instrument.instrument_id}@{tf_str} via DataManager.")
                            else:
                                self.logger.error(f"StrategyManager: DataManager failed to subscribe strategy '{strategy_id}' to {instrument.instrument_id}@{tf_str}")
                except Exception as e:
                    self.logger.error(f"Failed to initialize strategy '{strategy_id}': {e}", exc_info=True)

            self.logger.info(f"Total strategies initialized: {initialized_count}")
        return initialized_count > 0

    def register_dynamic_subscription(self, strategy_id: str, instrument_id: str, timeframe: str):
        """
        Called by a strategy (via BaseStrategy.request_symbol) to inform StrategyManager
        about a new dynamic symbol/timeframe subscription.
        """
        with self.lock:
            if strategy_id not in self.strategies:
                self.logger.warning(f"StrategyManager: Attempt to register dynamic subscription for unknown strategy_id '{strategy_id}'.")
                return

            self.strategy_symbols[strategy_id].setdefault(timeframe, set()).add(instrument_id)
            self.symbol_strategies[instrument_id].setdefault(timeframe, set()).add(strategy_id)
            self.logger.info(f"StrategyManager: Registered dynamic subscription for Strategy '{strategy_id}' to {instrument_id}@{timeframe}.")

    def unregister_dynamic_subscription(self, strategy_id: str, instrument_id: str, timeframe: str):
        """
        Called by a strategy if it dynamically unsubscribes from a symbol/timeframe.
        """
        with self.lock:
            if strategy_id in self.strategy_symbols and timeframe in self.strategy_symbols[strategy_id]:
                self.strategy_symbols[strategy_id][timeframe].discard(instrument_id)
                if not self.strategy_symbols[strategy_id][timeframe]:
                    del self.strategy_symbols[strategy_id][timeframe]
                if not self.strategy_symbols[strategy_id]: # If no timeframes left for this strategy
                    del self.strategy_symbols[strategy_id]

            if instrument_id in self.symbol_strategies and timeframe in self.symbol_strategies[instrument_id]:
                self.symbol_strategies[instrument_id][timeframe].discard(strategy_id)
                if not self.symbol_strategies[instrument_id][timeframe]:
                    del self.symbol_strategies[instrument_id][timeframe]
                if not self.symbol_strategies[instrument_id]: # If no strategies left for this symbol/timeframe
                    del self.symbol_strategies[instrument_id]
            self.logger.info(f"StrategyManager: Unregistered dynamic subscription for Strategy '{strategy_id}' from {instrument_id}@{timeframe}.")
    
    # _unsubscribe_strategy_symbols method should be reviewed to ensure it correctly
    # calls DataManager.unsubscribe_from_timeframe and then cleans up SM's own records.
    # The current implementation of _unsubscribe_strategy_symbols seems to handle this.

    # ... (start_strategy, stop_strategy, _unsubscribe_strategy_symbols, start, stop, _monitoring_thread, _process_signal, shutdown remain largely the same) ...
    # Ensure that when _unsubscribe_strategy_symbols calls DataManager.unsubscribe_from_timeframe,
    # the DataManager also correctly decrements its own reference counts and potentially unsubscribes from the feed.
    # The existing _unsubscribe_strategy_symbols in StrategyManager and unsubscribe_from_timeframe in DataManager
    # seem to handle their respective parts of the cleanup.

    def start_strategy(self, strategy_id: str):
        with self.lock:
            strategy = self.strategies.get(strategy_id)
            if strategy and not strategy.is_running:
                try:
                    strategy.start()
                    self.logger.info(f"Strategy '{strategy_id}' started successfully.")
                except Exception as e:
                    self.logger.error(f"Error starting strategy '{strategy_id}': {e}", exc_info=True)
            elif not strategy:
                self.logger.error(f"Strategy '{strategy_id}' not found. Cannot start.")
            else: # strategy.is_running is True
                self.logger.info(f"Strategy '{strategy_id}' is already running.")


    def stop_strategy(self, strategy_id: str):
        with self.lock:
            strategy = self.strategies.get(strategy_id)
            if strategy and strategy.is_running:
                try:
                    strategy.stop()
                    self.logger.info(f"Strategy '{strategy_id}' stopped successfully.")
                except Exception as e:
                    self.logger.error(f"Error stopping strategy '{strategy_id}': {e}", exc_info=True)
                finally: # Ensure symbols are unsubscribed even if strategy.stop() fails
                    self._unsubscribe_strategy_symbols(strategy_id)
            elif not strategy:
                self.logger.error(f"Strategy '{strategy_id}' not found. Cannot stop.")
            else: # strategy is not running
                self.logger.info(f"Strategy '{strategy_id}' is not running. Unsubscribing its symbols if any.")
                self._unsubscribe_strategy_symbols(strategy_id) # Clean up subscriptions if it was loaded but not run


    def _unsubscribe_strategy_symbols(self, strategy_id: str):
        if strategy_id not in self.strategy_symbols and strategy_id not in self.strategy_dynamic_underlyings:
            self.logger.debug(f"No symbols or dynamic underlyings recorded for strategy {strategy_id} to unsubscribe.")
            return

        self.logger.info(f"Unsubscribing all symbols for strategy {strategy_id}")
        
        # Unsubscribe from explicitly configured symbols (strategy_symbols)
        if strategy_id in self.strategy_symbols:
            for timeframe, symbol_keys_set in list(self.strategy_symbols[strategy_id].items()): # Iterate copy
                for symbol_key in list(symbol_keys_set): # Iterate copy
                    instrument = self.data_manager.get_instrument(symbol_key)
                    if instrument:
                        self.logger.debug(f"Unsubscribing strategy '{strategy_id}' from explicit symbol {symbol_key}@{timeframe}")
                        self.data_manager.unsubscribe_from_timeframe(instrument, timeframe, strategy_id)
                    else:
                        self.logger.warning(f"Could not find instrument for symbol_key '{symbol_key}' to unsubscribe for strategy '{strategy_id}'")
                    
                    # Clean up local tracking for symbol_strategies
                    if symbol_key in self.symbol_strategies and timeframe in self.symbol_strategies[symbol_key]:
                        self.symbol_strategies[symbol_key][timeframe].discard(strategy_id)
                        if not self.symbol_strategies[symbol_key][timeframe]:
                            del self.symbol_strategies[symbol_key][timeframe]
                    if not self.symbol_strategies.get(symbol_key): # If no timeframes left for this symbol_key
                        self.symbol_strategies.pop(symbol_key, None)
            
            self.strategy_symbols.pop(strategy_id, None)

        # Handle dynamic underlyings and their options
        if strategy_id in self.strategy_dynamic_underlyings:
            underlying_symbols_for_strat = self.strategy_dynamic_underlyings.pop(strategy_id, set())
            for underlying_sym_name in underlying_symbols_for_strat:
                self.logger.info(f"Strategy '{strategy_id}' is no longer tracking dynamic options for underlying '{underlying_sym_name}'.")
                # OptionManager needs to know if this underlying is still needed by other strategies
                # For now, we assume OptionManager handles its own ref counting or DataManager does for the underlying feed.
                # The crucial part is to remove this strategy's interest from options derived from this underlying.
                
                # Iterate through all options currently subscribed via OptionManager that belong to this underlying
                # and unsubscribe this strategy from them.
                active_options_for_underlying = self.option_manager.get_active_option_instruments(underlying_sym_name)
                for option_instrument in active_options_for_underlying:
                    option_symbol_key = option_instrument.instrument_id
                    # Check all timeframes this strategy might have been subscribed to for this option
                    # (typically derived from strategy.timeframe and strategy.additional_timeframes)
                    strategy_instance = self.strategies.get(strategy_id)
                    if strategy_instance:
                        relevant_timeframes = {strategy_instance.timeframe} | strategy_instance.additional_timeframes
                        for tf in relevant_timeframes:
                            if option_symbol_key in self.symbol_strategies.get(option_symbol_key, {}).get(tf, set()):
                                self.logger.debug(f"Unsubscribing strategy '{strategy_id}' from dynamic option {option_symbol_key}@{tf} (underlying: {underlying_sym_name})")
                                self.data_manager.unsubscribe_from_timeframe(option_instrument, tf, strategy_id)
                                # Clean up local tracking in symbol_strategies
                                if tf in self.symbol_strategies.get(option_symbol_key, {}):
                                     self.symbol_strategies[option_symbol_key][tf].discard(strategy_id)
                                     if not self.symbol_strategies[option_symbol_key][tf]:
                                         del self.symbol_strategies[option_symbol_key][tf]
                                if not self.symbol_strategies.get(option_symbol_key):
                                     self.symbol_strategies.pop(option_symbol_key, None)
        self.logger.info(f"Finished unsubscribing symbols for strategy {strategy_id}.")


    def start(self):
        """
        Start the strategy manager and all strategies.
        """
        try:
            self.logger.info("Starting Strategy Manager...")
            if not self.option_manager:
                 self.logger.critical("OptionManager not set. Aborting start.")
                 return False

            if not self.strategies: # If strategies aren't loaded yet
                if not self.initialize_strategies():
                    self.logger.error("Failed to initialize strategies during start. Aborting.")
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
            with self.lock:
                for strategy_id in list(self.strategies.keys()): # Iterate over a copy of keys
                    self.start_strategy(strategy_id)

            self.logger.info("Strategy Manager started successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Error starting Strategy Manager: {e}", exc_info=True)
            return False


    def stop(self):
        """
        Stop the strategy manager and all strategies.
        """
        self.logger.info("Stopping Strategy Manager...")
        self.stop_event.set() # Signal monitoring thread to stop
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.logger.debug("Waiting for monitoring thread to join...")
            self.monitoring_thread.join(timeout=5.0)
            if self.monitoring_thread.is_alive():
                self.logger.warning("Monitoring thread did not join in time.")

        with self.lock:
            for strategy_id in list(self.strategies.keys()): # Iterate over a copy of keys
                self.stop_strategy(strategy_id) # This will also handle unsubscriptions

        self.logger.info("Strategy Manager stopped.")


    def _monitoring_thread(self):
        """
        Monitoring thread function.
        Periodically checks strategy health and performance.
        """
        self.logger.info("Strategy monitoring thread started.")
        heartbeat_timeout = self.config.get('system', {}).get('strategy_heartbeat_timeout', 60)


        while not self.stop_event.wait(10): # Check every 10 seconds
            try:
                with self.lock: # Ensure thread-safe access to self.strategies
                    # Create a list of strategy items to iterate over to avoid issues if self.strategies is modified
                    current_strategies = list(self.strategies.items())
                
                for strategy_id, strategy in current_strategies:
                    if not strategy.is_running: # Skip checks for non-running strategies
                        continue

                    # Check if strategy is responsive (heartbeat)
                    if hasattr(strategy, 'last_heartbeat'):
                        last_heartbeat_ts = getattr(strategy, 'last_heartbeat', 0)
                        if time.time() - last_heartbeat_ts > heartbeat_timeout:
                            self.logger.warning(f"Strategy '{strategy_id}' has not sent a heartbeat in over {heartbeat_timeout} seconds. Last heartbeat: {datetime.fromtimestamp(last_heartbeat_ts) if last_heartbeat_ts else 'Never'}")
                            # TODO: Consider actions like attempting a restart or alerting.
                    else:
                        self.logger.debug(f"Strategy '{strategy_id}' does not have 'last_heartbeat' attribute for monitoring.")

                    # Check other health metrics if available (e.g., error counts, memory usage)
                    # ...

            except Exception as e:
                self.logger.error(f"Error in strategy monitoring loop: {e}", exc_info=True)
        self.logger.info("Strategy monitoring thread stopped.")


    def _process_signal(self, signal: SignalEvent):
        self.logger.info(f"StrategyManager processing signal: {signal}")
        # Forward the signal to the PortfolioManager or directly to an OrderExecutionSystem
        # The EventManager can route this based on subscriptions.
        # Example: self.portfolio_manager.on_signal(signal)
        # Example: self.broker.execute_order_from_signal(signal)
        
        # Publishing to EventManager allows other components (like PortfolioManager) to pick it up.
        self.event_manager.publish(signal) # Let EM route it based on subscriptions


    def shutdown(self):
        self.logger.info("StrategyManager shutting down...")
        self.stop() # This handles stopping strategies and the monitoring thread
        
        if self.strategy_executor:
            self.logger.debug("Shutting down strategy executor...")
            self.strategy_executor.shutdown(wait=True)
            self.logger.debug("Strategy executor shutdown complete.")
            
        self.logger.info("StrategyManager shutdown complete.")

# Example Usage (for testing, if this file is run directly)
if __name__ == "__main__":
    # This requires mock objects for DataManager, EventManager, etc.
    # And a proper strategies.yaml and config.yaml
    class MockDataManager:
        def subscribe_to_timeframe(self, instrument, timeframe, strategy_id):
            print(f"[MockDM] Strat '{strategy_id}' SUBSCRIBED to {instrument.instrument_id}@{timeframe}")
            return True
        def unsubscribe_from_timeframe(self, instrument, timeframe, strategy_id):
            print(f"[MockDM] Strat '{strategy_id}' UNSUBSCRIBED from {instrument.instrument_id}@{timeframe}")
            return True
        def get_instrument(self, symbol_key):
            parts = symbol_key.split(':')
            return Instrument(symbol=parts[1], exchange=Exchange(parts[0]), instrument_type=InstrumentType.EQUITY, asset_class=AssetClass.EQUITY, instrument_id=symbol_key)
        def subscribe_instrument(self, instrument): # Added for OptionManager interaction
            print(f"[MockDM] Instrument feed SUBSCRIBED for {instrument.instrument_id}")
            return True
        def unsubscribe_instrument(self, instrument): # Added for OptionManager interaction
            print(f"[MockDM] Instrument feed UNSUBSCRIBED for {instrument.instrument_id}")
            return True


    class MockOptionManager:
        def configure_underlyings(self, cfg): print(f"[MockOM] Configured underlyings: {len(cfg)}")
        def subscribe_underlying(self, sym):
            print(f"[MockOM] Subscribed to underlying feed: {sym}")
            # Simulate getting an instrument for the underlying
            # This part is tricky for a mock, as OM relies on DM for actual instrument objects
            # and feed interactions. For this test, we'll assume DM handles it.
            # dm_instance.subscribe_instrument(Instrument(symbol=sym, exchange=Exchange.NSE, instrument_type=InstrumentType.INDEX, asset_class=AssetClass.INDEX, instrument_id=f"NSE:{sym}"))
            return True
        def get_active_option_instruments(self, underlying_symbol: Optional[str] = None) -> List[Instrument]:
            if underlying_symbol == "NIFTY INDEX":
                 return [Instrument("NIFTY24MAY23000CE", Exchange.NFO, InstrumentType.OPTION, AssetClass.INDEX, instrument_id="NFO:NIFTY24MAY23000CE", option_type="CE", strike_price=23000, expiry_date=datetime(2024,5,30).date(), underlying_symbol_key="NSE:NIFTY INDEX")]
            return []


    mock_event_manager = EventManager() # Real one for this test
    mock_data_manager = MockDataManager()
    # mock_option_manager = MockOptionManager() # Instantiated later with DM

    # Mock Portfolio, Risk, Broker
    class MockPortfolioManager: pass
    class MockRiskManager:
        def validate_signal(self,s): return True
    class MockBroker: pass

    # Create dummy config files for testing
    dummy_config_yaml_content = {
        "market": {
            "underlyings": [
                {"name": "NIFTY INDEX", "exchange": "NSE", "derivative_exchange": "NFO", "strike_interval": 50, "atm_range": 2},
                {"name": "RELIANCE", "exchange": "NSE", "derivative_exchange": "NFO", "strike_interval": 10, "atm_range": 1}
            ]
        },
        "system": {"strategy_processing_workers": 2, "strategy_heartbeat_timeout": 5 } # Short timeout for test
    }
    dummy_strategies_yaml_content = {
        "strategies": {
            "TestStrat1": {
                "type": "BaseStrategy",
                "enabled": True,
                "timeframe": "1m",
                "additional_timeframes": ["5m"],
                "symbols": [
                    {"name": "RELIANCE", "exchange": "NSE", "timeframes": ["1m", "5m"]}
                ]
            },
            "TestOptionStrat1": {
                "type": "OptionStrategy",
                "enabled": True,
                "timeframe": "1m",
                 "underlyings": [{"name": "NIFTY INDEX", "spot_exchange": "NSE", "option_exchange": "NFO"}] # For dynamic options
            }
        }
    }

    # Register mock strategies (using OptionStrategy as a base for testing purposes)
    # In a real scenario, these would be actual strategy classes.
    StrategyRegistry.register("BaseStrategy", OptionStrategy)
    StrategyRegistry.register("OptionStrategy", OptionStrategy)

    # Initialize OptionManager with the mock DataManager
    mock_option_manager = OptionManager(data_manager=mock_data_manager, event_manager=mock_event_manager, position_manager=None, config=dummy_config_yaml_content)


    sm = StrategyManager(mock_data_manager, mock_event_manager, MockPortfolioManager(), MockRiskManager(), MockBroker(), config=dummy_config_yaml_content)
    sm.set_option_manager(mock_option_manager)

    print("--- Loading Config ---")
    sm.load_config(strategy_config_path=dummy_strategies_yaml_content, main_config_path=dummy_config_yaml_content)

    print("\n--- Initializing Strategies ---")
    sm.initialize_strategies()

    print(f"\nStrategy Symbols after init: {sm.strategy_symbols}")
    print(f"Symbol Strategies after init: {sm.symbol_strategies}")
    print(f"Dynamic Underlyings after init: {sm.strategy_dynamic_underlyings}")

    # Simulate OptionsSubscribedEvent
    # This event would normally be published by OptionManager
    nifty_call_instrument = Instrument("NIFTY24MAY23000CE", Exchange.NFO, InstrumentType.OPTION, AssetClass.INDEX, instrument_id="NFO:NIFTY24MAY23000CE", option_type="CE", strike_price=23000, expiry_date=datetime(2024,5,30).date(), underlying_symbol_key="NSE:NIFTY INDEX")
    options_subscribed_event = OptionsSubscribedEvent(underlying_symbol="NIFTY INDEX", instruments=[nifty_call_instrument], timestamp=datetime.now(), custom_event_type="OPTIONS_SUBSCRIBED")
    print("\n--- Simulating OptionsSubscribedEvent ---")
    mock_event_manager.publish(options_subscribed_event)
    time.sleep(0.2) # Allow event to be processed by SM's handler

    print(f"Strategy Symbols after option event: {sm.strategy_symbols}")
    print(f"Symbol Strategies after option event: {sm.symbol_strategies}")


    print("\n--- Starting Strategy Manager and Strategies ---")
    sm.start()
    time.sleep(0.1) # Allow strategies to start

    # Simulate BarEvent for RELIANCE
    reliance_instrument = Instrument("RELIANCE", Exchange.NSE, InstrumentType.EQUITY, AssetClass.EQUITY, instrument_id="NSE:RELIANCE")
    bar_event_reliance = BarEvent(EventType.BAR, datetime.now().timestamp(), reliance_instrument, "1m", 100,101,99,100.5,10000)
    print("\n--- Simulating BarEvent for RELIANCE ---")
    mock_event_manager.publish(bar_event_reliance)
    time.sleep(0.2) # Allow event to be processed

    # Simulate BarEvent for NIFTY Option
    bar_event_option = BarEvent(EventType.BAR, datetime.now().timestamp(), nifty_call_instrument, "1m", 50,51,49,50.5,500)
    print("\n--- Simulating BarEvent for NIFTY Option ---")
    mock_event_manager.publish(bar_event_option)
    time.sleep(0.2) # Allow event to be processed

    print("\n--- Stopping Strategy Manager and Strategies ---")
    sm.stop()
    time.sleep(0.1)
    print(f"Strategy Symbols after stop: {sm.strategy_symbols}") # Should be empty or reflect unsubscriptions
    print(f"Symbol Strategies after stop: {sm.symbol_strategies}")


    print("\n--- Shutting Down Strategy Manager ---")
    sm.shutdown()

