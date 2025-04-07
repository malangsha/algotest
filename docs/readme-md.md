# AlgoTrading

A modular and extensible algorithmic trading platform for Indian markets, supporting both NSE and BSE cash and options trading.

## Features

- **Modular Architecture**: Easy to extend and customize
- **Multiple Broker Support**: Zerodha, Upstox, Angel Broking and more
- **Strategy Development**: Implement and test trading strategies
- **Backtesting Engine**: Test strategies against historical data
- **Live Trading**: Execute strategies in real-time
- **Risk Management**: Advanced risk control features
- **Web Dashboard**: Monitor and control your trading system
- **Event-Driven Design**: React to market events efficiently
- **Indian Market Focus**: Specialized for NSE and BSE trading

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/algotrading.git
cd algotrading

# Install dependencies
pip install -e .
```

## Quick Start

### Backtesting a Strategy

```bash
python -m algotrading.main --mode backtest --strategy MovingAverageCrossover
```

### Live Trading

```bash
python -m algotrading.main --mode live --strategy RelativeStrengthIndex
```

### Start Web Dashboard

```bash
python -m algotrading.main --mode web
```

## Configuration

Edit the configuration files in `config/` directory to customize:

- Broker connections
- Strategy parameters
- Risk management settings
- Data sources
- Logging preferences

## Creating a Custom Strategy

```python
from algotrading.strategies.strategy import Strategy
from algotrading.models.order import Order, OrderType, OrderSide

class MyCustomStrategy(Strategy):
    def __init__(self, config):
        super().__init__(config)
        # Initialize strategy parameters
        
    def on_bar(self, instrument, bar_data):
        # Trading logic here
        if self.should_buy(bar_data):
            order = Order(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=self.calculate_position_size(instrument)
            )
            self.place_order(order)
```

## Documentation

For more detailed documentation, see the `/docs` directory or visit our [wiki](https://github.com/yourusername/algotrading/wiki).

## License

This project is licensed under the MIT License - see the LICENSE file for details.
