Sequence Diagram: MarketData, Bar, and OptionsSubscribed Events

Lifelines:
- MarketDataFeed (MDF)  // Represents FinvasiaFeed instance
- DataManager (DM)
- OptionManager (OM)
- StrategyManager (SM)
- HFMOS_instance        // HighFrequencyMomentumOptionStrategy instance

== MarketDataEvent Flow (Tick for an Underlying, e.g., NIFTY INDEX) ==

1.  MDF (FinvasiaFeed) -- WebSocket Tick Received for NIFTY INDEX --> MDF
2.  MDF -- _on_market_data(tick_data) --> self
3.  MDF -- self.callback(MarketDataEvent for NIFTY INDEX) --> MarketDataFeed._handle_market_data()
    * Note: `self.callback` is `MarketDataFeed._handle_market_data`
4.  MarketDataFeed -- publish(MarketDataEvent for NIFTY INDEX) --> EventManager (EM)

    --- Path 1: DataManager (for Bar generation & general tick storage) ---
5.  EM -- MarketDataEvent for NIFTY INDEX --> DM._on_market_data(event)
6.  DM -- put(event) --> DM.tick_queue
    * Async processing starts via `DM._process_tick_queue()` -> `_process_market_data_batch()` -> `_process_symbol_events()`
7.  DM (_process_single_market_data_event for NIFTY INDEX) -- update self.last_tick_data[NIFTY_INDEX_KEY] --> self
8.  DM (_process_single_market_data_event for NIFTY INDEX) -- process_tick(NIFTY_INDEX_KEY, converted_tick) --> TimeframeManager (TFM)
9.  TFM -- (Processes tick, may complete a bar) returns completed_bars_info --> DM
    * If a bar for NIFTY INDEX is completed, this leads to `BarEvent Flow` (see below)

    --- Path 2: OptionManager (for ATM strike updates) ---
10. EM -- MarketDataEvent for NIFTY INDEX --> OM._on_market_data(event)
11. OM -- _process_underlying_data("NIFTY INDEX", event.data) --> self
12. OM -- update_underlying_price("NIFTY INDEX", price) --> self
13. OM -- _get_atm_strike_internal(price, interval) --> self
14. OM -- returns new_atm_strike --> OM
15. OM -- (if ATM changed) _update_atm_subscriptions("NIFTY INDEX", new_atm_strike, old_atm_strike) --> self
    * This involves `DM.subscribe_instrument()` for new options and `DM.unsubscribe_instrument()` for old options.
    * If new options are subscribed, this leads to `OptionsSubscribedEvent Flow` (see below)

    --- Path 3: Strategy Instance (HFMOS directly, if subscribed to underlying ticks) ---
    * Assumption: `StrategyManager` called `HFMOS_instance._register_event_handlers()` during its initialization.
    * `HFMOS_instance.used_symbols` includes "NSE:NIFTY INDEX" (added via `HFMOS.on_start()` -> `self.request_symbol()`).
16. EM -- MarketDataEvent for NIFTY INDEX --> HFMOS_instance._handle_market_data(event)
17. HFMOS_instance (_handle_market_data) -- self.on_market_data(event) --> HFMOS_instance.on_market_data()
18. HFMOS_instance (on_market_data) -- (Logic for NIFTY INDEX ticks; in HFMOS, this path is minor as `on_market_data` primarily checks `active_positions` for options) --> self

== MarketDataEvent Flow (Tick for an Option, e.g., NIFTY_CE_ATM, after it's subscribed) ==

1.  MDF (FinvasiaFeed) -- WebSocket Tick Received for NIFTY_CE_ATM --> MDF
2.  MDF -- _on_market_data(tick_data) --> self
3.  MDF -- self.callback(MarketDataEvent for NIFTY_CE_ATM) --> MarketDataFeed._handle_market_data()
4.  MarketDataFeed -- publish(MarketDataEvent for NIFTY_CE_ATM) --> EM

    --- Path 1: DataManager (for Bar generation of option & general tick storage) ---
5.  EM -- MarketDataEvent for NIFTY_CE_ATM --> DM._on_market_data(event)
6.  DM -- put(event) --> DM.tick_queue
7.  DM (_process_single_market_data_event for NIFTY_CE_ATM) -- update self.last_tick_data[NIFTY_CE_ATM_KEY] --> self
8.  DM (_process_single_market_data_event for NIFTY_CE_ATM) -- process_tick(NIFTY_CE_ATM_KEY, converted_tick) --> TFM
9.  TFM -- (Processes tick, may complete an option bar) returns completed_bars_info --> DM
    * If a bar for NIFTY_CE_ATM is completed, `DM` publishes a `BarEvent`. `HFMOS_instance.on_bar` ignores option bars.

    --- Path 2: OptionManager (updates option data cache) ---
10. EM -- MarketDataEvent for NIFTY_CE_ATM --> OM._on_market_data(event)
11. OM (_on_market_data) -- self.option_data_cache[NIFTY_CE_ATM_KEY] = event.data --> self

    --- Path 3: Strategy Instance (HFMOS for SL/TP checks on active option positions) ---
    * Assumption: `StrategyManager` called `HFMOS_instance._register_event_handlers()`.
    * `HFMOS_instance.active_positions` contains NIFTY_CE_ATM_KEY if it's an active trade.
12. EM -- MarketDataEvent for NIFTY_CE_ATM --> HFMOS_instance._handle_market_data(event)
13. HFMOS_instance (_handle_market_data) -- self.on_market_data(event) --> HFMOS_instance.on_market_data()
14. HFMOS_instance (on_market_data) -- (if NIFTY_CE_ATM_KEY in self.active_positions) _check_option_exit_conditions(...) --> self

== BarEvent Flow (e.g., for NIFTY INDEX 1m Bar) ==
    (Continuation from DM processing a NIFTY INDEX tick that completes a 1m bar)

19. DM (_process_single_market_data_event for NIFTY INDEX) -- (bar completed) _publish_bar_event(NIFTY_INDEX_KEY, "1m", bar_data, instrument) --> self
20. DM (_publish_bar_event) -- publish(BarEvent for NIFTY INDEX 1m) --> EM

    --- Path 1: StrategyManager routes to Strategy Instance (HFMOS) ---
21. EM -- BarEvent for NIFTY INDEX 1m --> SM._on_bar(event)
22. SM -- _get_subscribed_strategies(NIFTY_INDEX_KEY, "1m") --> self (cache lookup)
23. SM -- returns {HFMOS_instance.id} --> SM
24. SM -- self.strategy_executor.submit(HFMOS_instance.on_bar, event) --> HFMOS_instance.on_bar() [via thread pool]
25. HFMOS_instance (on_bar for NIFTY INDEX) -- (Core strategy logic: _update_data_store, _calculate_indicators, _check_entry_signals for options based on underlying's bar) --> self

    --- Path 2: Strategy Instance receives BarEvent directly (Potentially Redundant Path) ---
    * Assumption: `StrategyManager` called `HFMOS_instance._register_event_handlers()` during its initialization.
    * `HFMOS_instance.used_symbols` includes "NSE:NIFTY INDEX".
    * `HFMOS_instance.all_timeframes` includes "1m".
26. EM -- BarEvent for NIFTY INDEX 1m --> HFMOS_instance._handle_bar(event)
27. HFMOS_instance (_handle_bar) -- (if conditions match) self.on_bar(event) --> HFMOS_instance.on_bar()
    * Note: This results in `HFMOS_instance.on_bar()` being called a second time for the same NIFTY INDEX 1m bar if Path 1 also occurred.

== OptionsSubscribedEvent Flow ==
    (Originates from OM._update_atm_subscriptions() after underlying price change causes ATM shift and new options need tracking)

28. OM (_update_atm_subscriptions for "NIFTY INDEX") -- Determines new options (e.g., NIFTY_CE_NEW_ATM) need subscription --> self
29. OM -- self.data_manager.subscribe_instrument(NIFTY_CE_NEW_ATM_instrument) --> DM
30. DM (subscribe_instrument) -- _subscribe_to_instrument_feed(NIFTY_CE_NEW_ATM_instrument) --> self
31. DM (_subscribe_to_instrument_feed) -- (if count was 0) self.market_data_feed.subscribe(NIFTY_CE_NEW_ATM_instrument) --> MDF
32. MDF (FinvasiaFeed) -- subscribe (API call to broker) --> External Broker WebSocket
33. MDF <-- Subscription Acknowledgment -- External Broker WebSocket
34. MDF -- returns True (success) --> DM
35. DM -- returns True --> OM
36. OM (_update_atm_subscriptions) -- self.event_manager.publish(OptionsSubscribedEvent for NIFTY_CE_NEW_ATM) --> EM

    --- Event Propagation to StrategyManager ---
37. EM -- OptionsSubscribedEvent --> SM._handle_custom_event(event)
38. SM (_handle_custom_event) -- _handle_options_subscribed(event) --> self
39. SM (_handle_options_subscribed) -- (Identifies HFMOS_instance is interested in "NIFTY INDEX" underlying) --> self
40. SM -- self.data_manager.subscribe_to_timeframe(NIFTY_CE_NEW_ATM_instrument, "1m", HFMOS_instance.id) --> DM
41. DM (subscribe_to_timeframe for NIFTY_CE_NEW_ATM on "1m" for HFMOS) -- _subscribe_to_instrument_feed(NIFTY_CE_NEW_ATM_instrument) --> self
    * Note: `instrument_feed_subscribers_count` for NIFTY_CE_NEW_ATM is now >0. MDF.subscribe is not called again.
42. DM (subscribe_to_timeframe) -- self.timeframe_manager.ensure_timeframe_tracked(NIFTY_CE_NEW_ATM_KEY, "1m") --> TFM
43. DM -- returns True --> SM
44. SM (_handle_options_subscribed) -- Updates self.strategy_symbols and self.symbol_strategies to link HFMOS_instance with NIFTY_CE_NEW_ATM for "1m" bars --> self
    * Now HFMOS_instance is set up to receive BarEvents for this option (though its `on_bar` logic currently ignores option bars).
    * More importantly, `MarketDataEvent` (ticks) for this option will now be processed by `HFMOS_instance.on_market_data` if it's an active position.

## Redundancy Findings

Based on the analysis of the provided Python modules, the primary redundant dispatch path identified is for `BarEvent`s being routed to strategy instances:

1.  **Redundant `BarEvent` Dispatch to Strategy Instances:**
    * **Description:** The same `BarEvent` (for a given symbol and timeframe, typically an underlying like "NSE:NIFTY INDEX") can be dispatched to a strategy instance's `on_bar(event)` method through two different paths if the strategy's event handlers are registered by `StrategyManager`.
    * **Path A (Via `StrategyManager`):**
        1.  `DataManager` publishes the `BarEvent`.
        2.  `EventManager` routes it to `StrategyManager._on_bar()`.
        3.  `StrategyManager._on_bar()` identifies subscribed strategy instances (e.g., `HFMOS_instance`) for that symbol and timeframe and calls `strategy_instance.on_bar(event)` (typically via its `strategy_executor`).
    * **Path B (Directly to `BaseStrategy` handlers on the instance):**
        1.  `DataManager` publishes the `BarEvent`.
        2.  `EventManager` routes it *also* to `strategy_instance._handle_bar(event)` (a method inherited from `BaseStrategy`). This happens because `BaseStrategy._register_event_handlers()` subscribes each strategy instance directly to `EventType.BAR`.
        3.  `strategy_instance._handle_bar(event)` then calls `self.on_bar(event)` (which is `strategy_instance.on_bar(event)`) if the instrument and timeframe match the strategy's interests.
    * **Impact:** The strategy's `on_bar()` method is executed twice with the exact same `BarEvent` data. This could lead to duplicated processing, incorrect state calculations, or unintended order generation.
    * **Relevant Files and Lines (Conceptual, assuming `_register_event_handlers` is called for strategy instances):**
        * `strategies/base_strategy.py`:
            * Around line 71: `self.event_manager.subscribe(EventType.BAR, self._handle_bar, component_name=self.id)` (within `_register_event_handlers`)
            * Around line 94: `def _handle_bar(self, event: Event): ... self.on_bar(event)`
        * `core/strategy_manager.py`:
            * Around line 74: `self.event_manager.subscribe(EventType.BAR, self._on_bar, component_name="StrategyManager")`
            * Around line 80: `def _on_bar(self, event: BarEvent): ... self.strategy_executor.submit(strategy.on_bar, event)`
    * **Condition:** This redundancy manifests if `StrategyManager.initialize_strategies()` calls `strategy._register_event_handlers()` for each strategy instance. The provided `strategy_manager.py` code does *not* explicitly show this call within `initialize_strategies`. However, a comment in `strategies/base_strategy.py` (around line 54) states: `_register_event_handlers() is called by StrategyManager after strategy instance creation and before strategy.initialize()`. If this intended design is implemented, the redundancy exists. If `_register_event_handlers` is not called for strategy instances, then Path B does not occur, and `StrategyManager` is the sole dispatcher of `BarEvent`s to strategies.

No other significant redundant dispatch paths for the specified events and components were identified under the "happy-path" assumption. The handling of `MarketDataEvent` appears to be routed uniquely to relevant components (DataManager, OptionManager, and directly to strategy instances via BaseStrategy handlers if registered). The `OptionsSubscribedEvent` flow also appears to be a coordinated hand-off rather than a redundant dispatch.
