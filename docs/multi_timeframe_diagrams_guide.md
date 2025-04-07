# Multi-Timeframe Diagrams & Implementation Guide

This guide explains the various diagrams created to illustrate the multi-timeframe feature and provides instructions on how to use this feature in your trading strategies.

## Architecture Diagram

![Multi-Timeframe Architecture](../diagrams/multi_timeframe_architecture.png)

The architecture diagram shows the high-level structure of the multi-timeframe system:

1. **Market Data Sources**: External data feeds that provide tick data to the system.
2. **Core System**:
   - **EventManager**: Central hub for event distribution throughout the system.
   - **Data Management**: Processes market data and manages different timeframes.
   - **Strategy Components**: Includes the base Strategy class and specialized Multi-Timeframe Strategy.
   - **Portfolio & Risk**: Handles position management and risk controls.

Data flows from market data sources through the EventManager to the DataManager, which uses the TimeframeManager to create bars of different timeframes. These bar events flow back to strategies, which then generate trading signals. Signals lead to portfolio updates, which generate position events that can feed back to strategies.

## Sequence Diagram

![Multi-Timeframe Sequence Flow](../diagrams/Multi-Timeframe\ Sequence.png)

The sequence diagram illustrates the chronological flow of data and events through the system:

1. **Initialization Phase**:
   - Strategy initialization with primary and additional timeframes
   - Registration with DataManager and subscription to timeframes

2. **Data Processing Phase**:
   - Market data events arrive and are processed as ticks
   - TimeframeManager aggregates ticks into bars for each timeframe
   - When a timeframe is complete, bar events are generated

3. **Strategy Execution Phase**:
   - Strategy receives bar events for different timeframes
   - Primary timeframe used for signal generation
   - Secondary timeframes used for trend confirmation
   - Signals generated based on combined analysis

4. **Order Execution Phase**:
   - Portfolio evaluates signals and generates orders
   - Positions are updated when orders are filled
   - Risk checks are performed

## Data Flow Diagram

![Multi-Timeframe Data Flow](../multi_timeframe_data_flow.png)

The data flow diagram provides a detailed view of how data moves through the system:

1. **Data Sources**: Market data enters the system through feeds.
2. **Data Processing Pipeline**:
   - EventManager routes events to appropriate components
   - DataManager processes market data
   - TimeframeManager converts ticks to bars of different timeframes
   - Bar Cache stores historical bars for retrieval
3. **Strategy Processing**:
   - Primary timeframe processor generates signals
   - Secondary timeframe processor confirms trends
   - Signal Generator combines inputs to create trade signals
4. **Portfolio Management**:
   - Portfolio manages overall account state
   - Position Manager tracks individual positions
   - Risk Manager enforces risk limits

## Component Relationships

![Multi-Timeframe Component Relationships](../multi_timeframe_component.png)

The component relationship diagram shows how the different classes interact:

1. **Core Framework**: EventManager and Event Queue form the foundation
2. **Data Management**: DataManager uses TimeframeManager
3. **Strategy Framework**: MultiTimeframeStrategy inherits from Strategy Base Class
4. **Portfolio & Risk**: Portfolio manages Positions and is monitored by Risk Manager

## Class Diagram

![Multi-Timeframe Class Diagram](../diagrams/Multi-Timeframe\ Class\ Diagram.png)

The class diagram details the structure of each component, showing properties and methods:

1. **Event Classes**: Base Event class and specialized events like BarEvent, MarketDataEvent, etc.
2. **Manager Classes**: EventManager, DataManager, TimeframeManager
3. **Strategy Classes**: Base Strategy and MultiTimeframeStrategy
4. **Portfolio Classes**: Portfolio, PositionManager, RiskManager

## Implementation Guide

### Setting Up a Multi-Timeframe Strategy

1. **Create a Strategy Class**:
   
   ```python
   from algotrading.strategy import MultiTimeframeStrategy
   
   class MyMultiTimeframeStrategy(MultiTimeframeStrategy):
       def __init__(self, config):
           super().__init__(config)
           # Initialize indicators for different timeframes
           self.primary_indicators = {}
           self.secondary_indicators = {}
   ```

2. **Configure the Strategy**:

   ```python
   strategy_config = {
       'name': 'My MTF Strategy',
       'timeframe': '1m',  # Primary timeframe
       'additional_timeframes': ['5m', '15m'],  # Additional timeframes
       'max_bars': 100,  # Maximum bars to store
       'instruments': ['AAPL', 'MSFT', 'AMZN'],
       'parameters': {
           'fast_ma_period': 10,
           'slow_ma_period': 30,
           'volatility_lookback': 20
       }
   }
   ```

3. **Implement Timeframe-Specific Processing**:

   ```python
   def _process_primary_timeframe(self, symbol, event):
       # Get the most recent bars for this symbol and timeframe
       bars = self.get_bars(symbol, self.timeframe, limit=50)
       
       if len(bars) < 30:  # Not enough data yet
           return
           
       # Calculate indicators on primary timeframe
       close_prices = [bar.close_price for bar in bars]
       fast_ma = self._calculate_sma(close_prices, self.parameters['fast_ma_period'])
       slow_ma = self._calculate_sma(close_prices, self.parameters['slow_ma_period'])
       
       # Store for later signal generation
       self.primary_indicators[symbol] = {
           'fast_ma': fast_ma,
           'slow_ma': slow_ma,
           'last_close': close_prices[-1]
       }
       
       # Check for signals after receiving primary timeframe data
       self._check_for_signals(symbol)
   
   def _process_secondary_timeframe(self, symbol, timeframe, event):
       # Get bars for this secondary timeframe
       bars = self.get_bars(symbol, timeframe, limit=50)
       
       if len(bars) < 20:  # Not enough data yet
           return
           
       # Calculate trend indicators on secondary timeframe
       close_prices = [bar.close_price for bar in bars]
       trend_ma = self._calculate_sma(close_prices, 20)
       
       # Store for trend confirmation
       if symbol not in self.secondary_indicators:
           self.secondary_indicators[symbol] = {}
           
       self.secondary_indicators[symbol][timeframe] = {
           'trend_ma': trend_ma,
           'last_close': close_prices[-1]
       }
   ```

4. **Signal Generation Using Multiple Timeframes**:

   ```python
   def _check_for_signals(self, symbol):
       # Check if we have data for all required timeframes
       if (symbol not in self.primary_indicators or 
           symbol not in self.secondary_indicators or
           not all(tf in self.secondary_indicators[symbol] for tf in self.additional_timeframes)):
           return
       
       # Get primary timeframe indicators
       p_ind = self.primary_indicators[symbol]
       fast_ma = p_ind['fast_ma']
       slow_ma = p_ind['slow_ma']
       last_close = p_ind['last_close']
       
       # Check trend confirmation on all secondary timeframes
       trend_confirmed = True
       for timeframe in self.additional_timeframes:
           s_ind = self.secondary_indicators[symbol][timeframe]
           # Only generate signals when price is above the trend MA on higher timeframes
           if last_close < s_ind['trend_ma']:
               trend_confirmed = False
               break
       
       # Generate signals with primary timeframe, confirmed by secondary timeframes
       if fast_ma > slow_ma and trend_confirmed:
           # Bullish signal with trend confirmation
           self._generate_signal(symbol, "BUY", 1.0)
       elif fast_ma < slow_ma and not trend_confirmed:
           # Bearish signal with trend confirmation
           self._generate_signal(symbol, "SELL", 1.0)
   ```

5. **Handle Different Bar Events**:

   ```python
   def on_bar(self, event):
       if not isinstance(event, BarEvent):
           return
           
       symbol = event.instrument.symbol
       timeframe = event.timeframe
       
       # Process based on timeframe
       if timeframe == self.timeframe:
           self._process_primary_timeframe(symbol, event)
       elif timeframe in self.additional_timeframes:
           self._process_secondary_timeframe(symbol, timeframe, event)
   ```

### Running a Multi-Timeframe Strategy

```python
from algotrading.engine import TradingEngine
from algotrading.data import DataManager
from algotrading.portfolio import Portfolio
from algotrading.risk import RiskManager
from my_strategy import MyMultiTimeframeStrategy

# Set up the trading engine
engine = TradingEngine()

# Create data manager with TimeframeManager
data_manager = DataManager()
engine.register_component(data_manager)

# Create portfolio with position and risk management
risk_manager = RiskManager(max_drawdown=0.05, max_position_size=0.1)
portfolio = Portfolio(initial_capital=100000, risk_manager=risk_manager)
engine.register_component(portfolio)

# Create and register strategy
strategy_config = {
    'name': 'My MTF Strategy',
    'timeframe': '1m', 
    'additional_timeframes': ['5m', '15m'],
    'max_bars': 100,
    'instruments': ['AAPL', 'MSFT', 'AMZN'],
    'parameters': {
        'fast_ma_period': 10,
        'slow_ma_period': 30,
        'volatility_lookback': 20
    }
}
strategy = MyMultiTimeframeStrategy(strategy_config)
engine.register_component(strategy)

# Run strategy (assuming data source is configured)
engine.run()
```

## Processing Flow End-to-End

1. **Market Data**: Tick data events arrive from external sources
2. **Tick Aggregation**: TimeframeManager converts ticks to bars for each timeframe
3. **Bar Events**: BarEvents are dispatched to the strategy for each timeframe
4. **Strategy Processing**:
   - Primary timeframe bars generate buy/sell signals
   - Secondary timeframe bars confirm market trends
5. **Signal Generation**: Strategy combines insights from all timeframes
6. **Portfolio Management**: Portfolio converts signals to orders
7. **Position Management**: Positions are updated after order fills
8. **Performance Tracking**: Portfolio updates equity curve and performance metrics
9. **Risk Management**: Risk limits are enforced on positions and portfolio

By utilizing multiple timeframes, your strategies can benefit from both the responsiveness of shorter timeframes and the stability of longer timeframes, resulting in more robust trading signals with better risk management.

## Common Design Patterns

1. **Trend Confirmation**: Use longer timeframes to confirm the trend direction while using shorter timeframes for entry signals.

2. **Volatility Scaling**: Adjust position sizes based on volatility measured across different timeframes.

3. **Momentum Alignment**: Only take trades where momentum is aligned across multiple timeframes.

4. **Range Identification**: Use longer timeframes to identify trading ranges, and shorter timeframes for range entries and exits.

5. **Hierarchical Filtering**: Filter trading signals based on conditions across multiple timeframes, from longest to shortest.

## Performance Considerations

1. **Memory Usage**: More timeframes require more historical data storage. Use the `max_bars` parameter to control memory usage.

2. **Computational Load**: Each additional timeframe increases processing requirements. Choose only the necessary timeframes.

3. **Event Ordering**: Ensure proper handling of bar events that may arrive out of sequence for different timeframes.

4. **Startup Delay**: Longer timeframes need more time to accumulate enough bars for analysis. Implement proper warmup periods. 