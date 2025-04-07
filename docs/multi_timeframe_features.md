# Multi-Timeframe Feature Documentation

## Overview

The multi-timeframe feature allows strategies to subscribe to and process market data at multiple time intervals simultaneously. This capability enables more sophisticated trading algorithms that can analyze market behavior across different timeframes, combining short-term trading opportunities with longer-term trend confirmation.

## Components

The multi-timeframe system consists of the following components:

1. **TimeframeManager**: Converts tick data into bars of different timeframes
2. **DataManager**: Manages market data and interfaces with the TimeframeManager
3. **BarEvent**: Represents OHLCV data for a specific timeframe
4. **Strategy base class**: Enhanced to support multiple timeframe subscriptions

## Workflow

Here's how the multi-timeframe feature works:

1. **Subscription**: A strategy specifies its primary timeframe and any additional timeframes it wants to monitor.

2. **Data Flow**:
   - Raw market data (ticks) arrive as MarketDataEvents
   - TimeframeManager aggregates these ticks into bars for each requested timeframe
   - When a timeframe bar is complete, a BarEvent is generated and published
   - Strategy receives the BarEvent and processes it according to its logic

3. **Data Storage**: 
   - The Strategy class maintains a cache of bar data for each symbol and timeframe
   - This data is accessible through the `get_bars(symbol, timeframe)` method

## Using Multi-Timeframe Features

### Strategy Configuration

To use multiple timeframes in your strategy, configure it with the following parameters:

```python
strategy_config = {
    'name': 'My Multi-Timeframe Strategy',
    'timeframe': '1m',  # Primary timeframe (1 minute)
    'additional_timeframes': ['5m', '15m'],  # Additional timeframes to monitor
    'max_bars': 100,  # Maximum number of bars to store per timeframe
    'instruments': [...]  # Your instruments
}
```

### Handling Bar Events

Override the `on_bar` method in your strategy to handle incoming bar events:

```python
def on_bar(self, event: BarEvent):
    # Type check is important
    if not isinstance(event, BarEvent):
        return
        
    # Extract data from event
    symbol = event.instrument.symbol
    timeframe = event.timeframe
    
    # Process differently based on the timeframe
    if timeframe == self.timeframe:
        # Process primary timeframe logic
        self._process_primary_timeframe(symbol, event)
    elif timeframe in self.additional_timeframes:
        # Process secondary timeframe logic
        self._process_secondary_timeframe(symbol, event)
```

### Accessing Bar Data

Use the `get_bars` method to access historical bar data for a symbol and timeframe:

```python
# Get the last 20 bars for AAPL on the 5-minute timeframe
bars = self.get_bars('AAPL', '5m', limit=20)

# Process the bar data
for bar in bars:
    # Access OHLCV data
    open_price = bar.open_price
    high_price = bar.high_price
    low_price = bar.low_price
    close_price = bar.close_price
    volume = bar.volume
```

## Supported Timeframes

The system supports the following standard timeframes:

- `1s`: 1 second
- `5s`: 5 seconds
- `15s`: 15 seconds
- `30s`: 30 seconds
- `1m`: 1 minute
- `3m`: 3 minutes
- `5m`: 5 minutes
- `15m`: 15 minutes
- `30m`: 30 minutes
- `1h`: 1 hour
- `2h`: 2 hours
- `4h`: 4 hours
- `6h`: 6 hours
- `8h`: 8 hours
- `12h`: 12 hours
- `1d`: 1 day
- `3d`: 3 days
- `1w`: 1 week
- `1M`: 1 month

## Example Strategy

A sample implementation of a multi-timeframe strategy is provided in `strategies/multi_timeframe_strategy.py`. This strategy:

1. Uses a primary (faster) timeframe for generating signals
2. Uses a secondary (slower) timeframe for trend confirmation
3. Implements a simple moving average crossover system with trend confirmation

To test this strategy, run the example script:

```bash
python examples/run_multi_timeframe_strategy.py
```

## Implementation Details

### TimeframeManager

The TimeframeManager handles the conversion of tick data to various timeframes:

- It maintains buffers for each symbol and timeframe combination
- When a new tick arrives, it updates all relevant timeframe buffers
- When a timeframe period completes, it creates and emits a BarEvent

### BarEvent Class

The BarEvent class represents OHLCV data for a specific timeframe:

```python
class BarEvent(Event):
    def __init__(self, instrument, timeframe, bar_start_time, open_price, 
                 high_price, low_price, close_price, volume):
        # Initialize with OHLCV data and timeframe information
```

### Strategy Base Class Extensions

The Strategy base class has been extended with:

- Support for a primary timeframe and additional timeframes
- Methods to subscribe to market data for multiple timeframes
- A cache for bar data organized by symbol and timeframe
- Utility methods to access and process this data

## Best Practices

1. **Choose Timeframes Carefully**: Select timeframes that complement each other. For example, use a shorter timeframe for signal generation and longer timeframes for trend confirmation.

2. **Manage Memory Usage**: Set a reasonable `max_bars` value to prevent excessive memory usage, especially for short timeframes.

3. **Check Bar Availability**: Always check that you have enough bar data before running your calculations, especially after startup.

4. **Prioritize Processing**: Handle more important timeframes first to ensure timely signal generation.

5. **Type Checking**: Always check that an event is a BarEvent before processing it, as your strategy may receive other event types.

## Troubleshooting

Common issues and solutions:

- **Not Receiving Bar Events**: Ensure that the strategy is properly registered for the timeframe and that the TimeframeManager is correctly initialized.

- **Missing Data**: Check that enough time has passed to accumulate bars for the requested timeframe.

- **Performance Issues**: If the system becomes slow, consider reducing the number of timeframes or instruments being monitored.

## Future Enhancements

Planned improvements for the multi-timeframe feature:

1. Optimization of timeframe calculations for better performance
2. Support for custom timeframes beyond the standard ones
3. Enhanced visualization tools for multi-timeframe analysis
4. Backtesting enhancements to support multi-timeframe strategies 