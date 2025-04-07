from typing import Dict, List, Optional, Any, Tuple
import logging
import pandas as pd
import numpy as np
import uuid
import os
from datetime import datetime, timedelta
import yaml

from .broker_interface import BrokerInterface
from models.order import Order, OrderStatus, OrderType
from models.trade import Trade
from models.events import MarketDataEvent
from models.position import Position
from models.instrument import Instrument
from models.market_data import Bar
from core.data_manager import DataManager
from live.market_data_feeds import MarketDataFeed
from core.event_manager import EventManager

class SimulatedBroker(BrokerInterface):
    """
    Simulated broker implementation for backtesting and paper trading.
    Simulates order execution, fills, and position management.
    """

    def __init__(self,
                 initial_capital: float = 1000000.0,
                 slippage: float = 0.0,
                 commission_rate: float = 0.0,
                 market_data: Optional[DataManager] = None,
                 feed_type: str = "simulated",
                 config: Optional[Dict[str, Any]] = None,
                 **kwargs):
        """
        Initialize the simulated broker.

        Args:
            initial_capital: Starting capital for the account
            slippage: Slippage to apply on executions (percentage)
            commission_rate: Commission rate as percentage of trade value
            market_data: Market data source for price lookups
            feed_type: Type of market data feed to use (simulated, websocket, rest)
            config: Full configuration dictionary
            **kwargs: Additional configuration parameters
        """
        self.logger = logging.getLogger("brokers.simulated_broker")
        self.logger.info("Initializing SimulatedBroker")
        self.event_manager = None
        self.api_key = None
        self.market_data_settings = {}
        self.market_data_broker = None
        self.feed_type = feed_type

        # Account info
        self.initial_capital = initial_capital
        self.cash_balance = initial_capital
        self.account_value = initial_capital

        # Execution parameters
        self.slippage = slippage
        self.commission_rate = commission_rate

        # Data source
        self.market_data = market_data
        self.config = config or {}

        # Determine feed type from config or use provided
        if config and "broker" in config:
            broker_list = config.get("broker", [])
            for broker_item in broker_list:
                if broker_item.get("name", "").lower() == "simulated":
                    # If market_data_source is specified in config, use that
                    if "market_data_source" in broker_item:
                        self.feed_type = broker_item["market_data_source"]
                        break

        # Print feed type and market data source info for debugging
        self.logger.info(f"Feed type from input: {self.feed_type}")
        if config and "broker" in config:
            broker_list = config.get("broker", [])
            for broker_item in broker_list:
                if broker_item.get("name", "").lower() == "simulated":
                    market_data_source = broker_item.get("market_data_source", "Not specified")
                    self.logger.info(f"Market data source from config: {market_data_source}")
                    break
        # If feed_type wasn't set by config, use provided value
        if not hasattr(self, 'feed_type'):
            self.feed_type = feed_type

        self.market_data_feed = None

        # Order and position tracking
        self.orders: Dict[str, Order] = {}
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []

        # Track last processed timestamp
        self.current_timestamp = None

        # Connection state
        self._connected = False

        # Market data subscriptions
        self.subscriptions = set()

        # Load secrets
        secrets_path = self.config.get("secrets")
        if secrets_path and os.path.exists(secrets_path):
            with open(secrets_path, 'r') as f:
                self.secrets = yaml.safe_load(f)
        else:
            self.secrets = {}

        # Initialize market data source
        self._initialize_market_data_source()

    def set_event_manager(self, event_manager: EventManager):
        """Set the event manager for the broker."""
        self.event_manager = event_manager

    def _initialize_market_data_source(self):
        """Initialize the market data source."""
        try:
            if self.feed_type == "simulated":
                self.logger.info("Using simulated market data feed")
                # For simulated feed, we don't need to create another broker
                return
            else:
                # For non-simulated feeds, create the appropriate broker
                from brokers.broker_factory import BrokerFactory
                self.market_data_broker = BrokerFactory.create_broker(self.feed_type, self.config)
                self.logger.info(f"Using {self.feed_type} for market data")
        except ValueError as e:
            self.logger.error(f"Failed to initialize market data source {self.feed_type}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize market data source {self.feed_type}: {str(e)}")
            raise

    def get_api_key(self) -> str:
        """Get the API key for authentication."""
        # If we have a market data broker, use its API key
        if hasattr(self, 'market_data_broker') and self.market_data_broker:
            return self.market_data_broker.get_api_key()

        # Otherwise use our own API key from secrets
        if not hasattr(self, 'api_key') or not self.api_key:
            self.api_key = self.secrets.get("api_key", "simulated_api_key")
        return self.api_key

    def get_market_data_settings(self) -> Dict[str, Any]:
        """
        Get market data settings from configuration.

        Returns:
            Dict containing market data settings
        """
        settings = {}

        # Add default settings based on feed type
        if self.feed_type == "simulated":
            # Ensure we have default simulated feed settings
            if "initial_prices" not in settings:
                settings["initial_prices"] = {}
            if "volatility" not in settings:
                settings["volatility"] = 0.002
            if "tick_interval" not in settings:
                settings["tick_interval"] = 1.0
            if "price_jump_probability" not in settings:
                settings["price_jump_probability"] = 0.05
            if "max_price_jump" not in settings:
                settings["max_price_jump"] = 0.02

        # Add configuration from market_data section if available
        if hasattr(self, 'config') and self.config and 'market_data' in self.config:
            market_data_config = self.config.get('market_data', {})
            settings.update({
                'enable_persistence': market_data_config.get('enable_persistence', False),
                'persistence_path': market_data_config.get('persistence_path', './data/ticks'),
                'persistence_interval': market_data_config.get('persistence_interval', 300),
                'max_ticks_in_memory': market_data_config.get('max_ticks_in_memory', 10000)
            })        
       

        return settings

    def on_market_data(self, event: MarketDataEvent):
        """
        Handle market data events.

        Args:
            event: Market data event containing price updates
        """
        try:
            # Update current timestamp
            self.current_timestamp = datetime.fromtimestamp(event.timestamp)

            # Log the market data event
            self.logger.debug(f"Received market data for {event.symbol}: {event.data}")

            # Process pending orders with new market data
            for order_id, order in list(self.orders.items()):
                if order.status in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]:
                    self._process_order(order, self.current_timestamp)

            # Update positions with new market prices
            self._update_positions(self.current_timestamp)

            # If using a broker for market data, forward the event
            if self.feed_type != "simulated" and hasattr(self, 'market_data_broker') and self.market_data_broker:
                self.market_data_broker.on_market_data(event)

            self.logger.debug(f"Processed market data for {event.symbol}")
        except Exception as e:
            self.logger.error(f"Error processing market data: {str(e)}")
            self.logger.exception("Detailed error:")

    def connect(self) -> bool:
        """
        Establish connection to the simulated broker.
        For simulated broker, this is always successful.

        Returns:
            bool: True indicating successful connection
        """
        self._connected = True

        # Initialize market data broker if needed and not already done
        if self.feed_type != "simulated" and (not hasattr(self, 'market_data_broker') or not self.market_data_broker):
            self._initialize_market_data_source()

        # Initialize market data feed if not already done
        # if not self.market_data_feed:
        #     self._initialize_market_data_feed()

        self.logger.info("Connected to SimulatedBroker")
        return True

    def _initialize_market_data_feed(self):
        """Initialize the market data feed."""
        try:
            # Get feed settings from config or use defaults based on feed type
            feed_settings = self.get_market_data_settings()

            self.logger.info(f"Initializing {self.feed_type} market data feed with settings: {feed_settings}")

            # Create market data feed
            self.market_data_feed = MarketDataFeed(
                feed_type=self.feed_type,
                broker=self,
                instruments=list(self.positions.values()),
                event_manager=self.event_manager
            )

            # Start the feed
            self.market_data_feed.start()
            self.logger.info(f"Initialized {self.feed_type} market data feed")
        except Exception as e:
            self.logger.error(f"Failed to initialize market data feed: {str(e)}")
            raise

    def disconnect(self) -> bool:
        """
        Disconnect from the simulated broker.

        Returns:
            bool: True indicating successful disconnection
        """
        self._connected = False

        # Stop market data feed if running
        if hasattr(self, 'market_data_feed') and self.market_data_feed:
            self.market_data_feed.stop()
            self.market_data_feed = None

        # Disconnect market data broker if present
        if hasattr(self, 'market_data_broker') and self.market_data_broker:
            self.market_data_broker.disconnect()

        self.logger.info("Disconnected from SimulatedBroker")
        return True

    def is_connected(self) -> bool:
        """
        Check if broker is connected.

        Returns:
            bool: Connection status
        """
        return self._connected

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get account information.

        Returns:
            Dict[str, Any]: Account details
        """
        # Calculate account value based on cash + positions value
        positions_value = sum(position.market_value for position in self.positions.values())
        self.account_value = self.cash_balance + positions_value

        return {
            "account_id": "simulated_account",
            "cash_balance": self.cash_balance,
            "positions_value": positions_value,
            "account_value": self.account_value,
            "initial_capital": self.initial_capital,
            "pnl": self.account_value - self.initial_capital,
            "pnl_percent": ((self.account_value / self.initial_capital) - 1) * 100
        }

    def get_account_balance(self) -> float:
        """
        Get current cash balance.

        Returns:
            float: Cash balance
        """
        return self.cash_balance

    def get_account_positions_summary(self) -> Dict[str, Any]:
        """
        Get summary of account positions.

        Returns:
            Dict[str, Any]: Positions summary
        """
        total_value = sum(position.market_value for position in self.positions.values())
        total_pnl = sum(position.unrealized_pnl for position in self.positions.values())

        return {
            "total_positions": len(self.positions),
            "total_value": total_value,
            "total_unrealized_pnl": total_pnl,
            "position_count_by_type": self._count_positions_by_type()
        }

    def _count_positions_by_type(self) -> Dict[str, int]:
        """
        Count positions by instrument type.

        Returns:
            Dict[str, int]: Count by type
        """
        counts = {}
        for position in self.positions.values():
            instrument_type = position.instrument.instrument_type
            counts[instrument_type] = counts.get(instrument_type, 0) + 1
        return counts

    def place_order(self, order: Order) -> str:
        """
        Place an order with the simulated broker.

        Args:
            order: Order to be placed

        Returns:
            str: Order ID if successful
        """
        if not order.order_id:
            order.order_id = str(uuid.uuid4())

        order.status = OrderStatus.PENDING
        order.submitted_time = datetime.now()
        self.orders[order.order_id] = order

        self.logger.info(f"Order placed: {order.order_id} - {order.instrument.symbol} {order.order_type} "
                         f"{order.direction} {order.quantity} @ {order.price}")

        # If market data is available, process market orders immediately
        if self.market_data and order.order_type == OrderType.MARKET and self.current_timestamp:
            self._process_order(order, self.current_timestamp)

        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            bool: True if successfully canceled
        """
        if order_id not in self.orders:
            self.logger.warning(f"Cannot cancel order {order_id}: not found")
            return False

        order = self.orders[order_id]
        if order.status not in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]:
            self.logger.warning(f"Cannot cancel order {order_id}: status is {order.status}")
            return False

        order.status = OrderStatus.CANCELLED
        self.logger.info(f"Order cancelled: {order_id}")
        return True

    def modify_order(self, order_id: str, new_params: Dict[str, Any]) -> bool:
        """
        Modify a pending order.

        Args:
            order_id: ID of order to modify
            new_params: Parameters to modify

        Returns:
            bool: True if successfully modified
        """
        if order_id not in self.orders:
            self.logger.warning(f"Cannot modify order {order_id}: not found")
            return False

        order = self.orders[order_id]
        if order.status not in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]:
            self.logger.warning(f"Cannot modify order {order_id}: status is {order.status}")
            return False

        # Update order parameters
        for param, value in new_params.items():
            if hasattr(order, param):
                setattr(order, param, value)

        self.logger.info(f"Order modified: {order_id}")
        return True

    def get_order(self, order_id: str) -> Optional[Order]:
        """
        Get order details by ID.

        Args:
            order_id: Order ID to retrieve

        Returns:
            Optional[Order]: Order object if found
        """
        return self.orders.get(order_id)

    def get_order_status(self, order_id: str) -> Optional[OrderStatus]:
        """
        Get status of an order.

        Args:
            order_id: Order ID to check

        Returns:
            Optional[OrderStatus]: Order status if found
        """
        order = self.get_order(order_id)
        return order.status if order else None

    def get_orders(self) -> List[Order]:
        """
        Get all orders.

        Returns:
            List[Order]: All orders
        """
        return list(self.orders.values())

    def get_positions(self) -> List[Position]:
        """
        Get all current positions.

        Returns:
            List[Position]: All positions
        """
        return list(self.positions.values())

    def get_position(self, instrument_id: str) -> Optional[Position]:
        """
        Get position for a specific instrument.

        Args:
            instrument_id: Instrument ID to retrieve position for

        Returns:
            Optional[Position]: Position object if found
        """
        return self.positions.get(instrument_id)

    def get_trades(self) -> List[Trade]:
        """
        Get all executed trades.

        Returns:
            List[Trade]: All trades
        """
        return self.trades

    def get_execution_history(self, start_time: Optional[datetime] = None,
                            end_time: Optional[datetime] = None) -> List[Trade]:
        """
        Get execution history within a time range.

        Args:
            start_time: Start time for history query (None for all)
            end_time: End time for history query (None for all)

        Returns:
            List[Trade]: Filtered trades
        """
        if not start_time and not end_time:
            return self.trades

        filtered_trades = []
        for trade in self.trades:
            if start_time and trade.timestamp < start_time:
                continue
            if end_time and trade.timestamp > end_time:
                continue
            filtered_trades.append(trade)

        return filtered_trades

    def get_instrument_details(self, symbol: str, exchange: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get details for an instrument.

        Args:
            symbol: Instrument symbol
            exchange: Optional exchange

        Returns:
            Optional[Dict[str, Any]]: Instrument details if found
        """
        # For simulated broker, return basic information
        # In a real implementation, this might query from a reference data source
        return {
            "symbol": symbol,
            "exchange": exchange or "NSE",
            "instrument_type": "EQ",
            "tick_size": 0.05,
            "lot_size": 1,
            "trading_hours": self.get_trading_hours(symbol, exchange)
        }

    def get_trading_hours(self, symbol: Optional[str] = None,
                        exchange: Optional[str] = None) -> Dict[str, Any]:
        """
        Get trading hours for an exchange or instrument.

        Args:
            symbol: Optional symbol
            exchange: Optional exchange

        Returns:
            Dict[str, Any]: Trading hours information
        """
        # Default trading hours for Indian markets
        if exchange == "BSE" or exchange == "NSE" or exchange is None:
            return {
                "market_open": "09:15:00",
                "market_close": "15:30:00",
                "pre_market_start": "09:00:00",
                "post_market_end": "16:00:00",
                "timezone": "Asia/Kolkata"
            }
        # For other exchanges, return default hours
        return {
            "market_open": "09:00:00",
            "market_close": "17:00:00",
            "timezone": "UTC"
        }

    def get_market_data(self, symbol: str, interval: str = "1d",
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Get historical market data.

        Args:
            symbol: Instrument symbol
            interval: Bar interval ("1d", "1h", "5m", etc.)
            start_date: Start date for data
            end_date: End date for data

        Returns:
            Optional[pd.DataFrame]: Historical data if available
        """
        if not self.market_data:
            return None

        try:
            # Delegate to market data provider
            return self.market_data.get_historical_data(symbol, interval, start_date, end_date)
        except Exception as e:
            self.logger.error(f"Error getting market data: {str(e)}")
            return None

    def subscribe_market_data(self, symbols: List[str], callback=None) -> bool:
        """
        Subscribe to market data for symbols.

        Args:
            symbols: List of symbols to subscribe to
            callback: Optional callback function for data updates

        Returns:
            bool: Success status
        """
        if not self.market_data_feed:
            self.logger.warning("No market data feed available")
            return False

        # Create instruments for the symbols
        instruments = []
        for symbol in symbols:
            instrument = Instrument(
                symbol=symbol,
                exchange=self.config.get("exchange", "NSE"),
                instrument_type=self.config.get("instrument_type", "EQ")
            )
            instruments.append(instrument)
            self.subscriptions.add(symbol)

        # Subscribe to each instrument
        for instrument in instruments:
            self.market_data_feed.subscribe(instrument)

        self.logger.info(f"Subscribed to market data for: {', '.join(symbols)}")
        return True

    def unsubscribe_market_data(self, symbols: List[str]) -> bool:
        """
        Unsubscribe from market data for symbols.

        Args:
            symbols: List of symbols to unsubscribe from

        Returns:
            bool: Success status
        """
        if not self.market_data_feed:
            return False

        # Create instruments for the symbols
        instruments = []
        for symbol in symbols:
            instrument = Instrument(
                symbol=symbol,
                exchange=self.config.get("exchange", "NSE"),
                instrument_type=self.config.get("instrument_type", "EQ")
            )
            instruments.append(instrument)
            if symbol in self.subscriptions:
                self.subscriptions.remove(symbol)

        # Unsubscribe from each instrument
        for instrument in instruments:
            self.market_data_feed.unsubscribe(instrument)

        self.logger.info(f"Unsubscribed from market data for: {', '.join(symbols)}")
        return True

    def update(self, timestamp: datetime) -> None:
        """
        Update the broker state based on current market data.
        Processes pending orders and updates positions.

        Args:
            timestamp: Current timestamp for processing
        """
        self.current_timestamp = timestamp

        # Process pending orders
        for order_id, order in list(self.orders.items()):
            if order.status in [OrderStatus.PENDING, OrderStatus.PARTIAL_FILL]:
                self._process_order(order, timestamp)

        # Update position market values
        self._update_positions(timestamp)

    def _process_order(self, order: Order, timestamp: datetime) -> None:
        """
        Process an order against current market data.

        Args:
            order: Order to process
            timestamp: Current timestamp
        """
        if not self.market_data:
            self.logger.warning("Cannot process order: no market data available")
            return

        instrument = order.instrument
        try:
            # Get current price data
            current_price = self._get_price_for_execution(instrument, order, timestamp)
            if current_price is None:
                return  # Couldn't get price data

            # Check if limit/stop conditions are met
            if not self._check_order_conditions(order, current_price):
                return

            # Execute the order
            executed_price = self._apply_slippage(current_price, order)
            commission = self._calculate_commission(executed_price, order.quantity)

            # Create trade record
            trade = Trade(
                trade_id=str(uuid.uuid4()),
                order_id=order.order_id,
                instrument=instrument,
                quantity=order.quantity,
                price=executed_price,
                commission=commission,
                direction=order.direction,
                timestamp=timestamp
            )
            self.trades.append(trade)

            # Update order status
            order.executed_quantity = order.quantity
            order.executed_price = executed_price
            order.status = OrderStatus.FILLED
            order.filled_time = timestamp

            # Update position
            self._update_position_after_trade(trade)

            # Update cash balance
            trade_value = executed_price * order.quantity
            direction_multiplier = -1 if order.direction == "BUY" else 1
            self.cash_balance += (trade_value * direction_multiplier) - commission

            self.logger.info(f"Order executed: {order.order_id} - {order.instrument.symbol} "
                           f"{order.direction} {order.quantity} @ {executed_price}")

        except Exception as e:
            self.logger.error(f"Error processing order {order.order_id}: {str(e)}")

    def _get_price_for_execution(self, instrument: Instrument, order: Order,
                               timestamp: datetime) -> Optional[float]:
        """
        Get price for order execution based on market data.

        Args:
            instrument: Instrument to get price for
            order: Order to execute
            timestamp: Current timestamp

        Returns:
            Optional[float]: Price for execution if available
        """
        try:
            # Get latest bar for the instrument
            bar: Optional[Bar] = self.market_data.get_latest_bar(instrument.symbol, timestamp)

            if not bar:
                self.logger.warning(f"No price data available for {instrument.symbol} at {timestamp}")
                return None

            # Use appropriate price based on order type and direction
            if order.order_type == OrderType.MARKET:
                # For market orders, use current price (with slippage)
                return bar.close
            elif order.order_type == OrderType.LIMIT:
                # For limit orders, use the limit price
                return order.price

            return bar.close

        except Exception as e:
            self.logger.error(f"Error getting price for {instrument.symbol}: {str(e)}")
            return None

    def _check_order_conditions(self, order: Order, current_price: float) -> bool:
        """
        Check if order conditions (limit/stop) are met.

        Args:
            order: Order to check
            current_price: Current market price

        Returns:
            bool: True if conditions are met
        """
        # For market orders, always execute
        if order.order_type == OrderType.MARKET:
            return True

        # For limit orders, check price conditions
        if order.order_type == OrderType.LIMIT:
            if order.direction == "BUY" and current_price <= order.price:
                return True
            elif order.direction == "SELL" and current_price >= order.price:
                return True
            return False

        # For stop orders
        if order.order_type == OrderType.STOP:
            if order.direction == "BUY" and current_price >= order.price:
                return True
            elif order.direction == "SELL" and current_price <= order.price:
                return True
            return False

        # For stop-limit orders (not fully implemented)
        if order.order_type == OrderType.STOP_LIMIT:
            # TODO: Implement full stop-limit logic
            return False

        return False

    def _apply_slippage(self, price: float, order: Order) -> float:
        """
        Apply slippage to execution price.

        Args:
            price: Base price
            order: Order being executed

        Returns:
            float: Price with slippage applied
        """
        if self.slippage == 0:
            return price

        # Apply slippage in the disadvantageous direction
        slippage_factor = 1.0 + (self.slippage if order.direction == "BUY" else -self.slippage)
        return price * slippage_factor

    def _calculate_commission(self, price: float, quantity: int) -> float:
        """
        Calculate commission for a trade.

        Args:
            price: Execution price
            quantity: Order quantity

        Returns:
            float: Commission amount
        """
        if self.commission_rate == 0:
            return 0.0

        trade_value = price * quantity
        commission = trade_value * self.commission_rate

        # Apply minimum commission if configured
        min_commission = self.config.get("min_commission", 0)
        if min_commission > 0 and commission < min_commission:
            return min_commission

        # Apply maximum commission if configured
        max_commission = self.config.get("max_commission", float('inf'))
        if max_commission < float('inf') and commission > max_commission:
            return max_commission

        return commission

    def _update_position_after_trade(self, trade: Trade) -> None:
        """
        Update positions after a trade execution.

        Args:
            trade: Executed trade
        """
        instrument_id = trade.instrument.instrument_id

        if instrument_id not in self.positions:
            # Create new position
            self.positions[instrument_id] = Position(
                instrument=trade.instrument,
                quantity=trade.quantity if trade.direction == "BUY" else -trade.quantity,
                average_price=trade.price,
                market_price=trade.price,
                timestamp=trade.timestamp
            )
        else:
            # Update existing position
            position = self.positions[instrument_id]
            old_quantity = position.quantity
            direction_multiplier = 1 if trade.direction == "BUY" else -1
            new_quantity = old_quantity + (trade.quantity * direction_multiplier)

            # If position is being reduced but not closed
            if (old_quantity > 0 and new_quantity > 0) or (old_quantity < 0 and new_quantity < 0):
                # Keep average price the same
                position.quantity = new_quantity
            # If position is being closed or reversed
            elif new_quantity == 0:
                # Remove the position if fully closed
                del self.positions[instrument_id]
                return
            # If position is being reversed or newly established
            else:
                # Set average price to the new trade price
                position.average_price = trade.price
                position.quantity = new_quantity

            # Update market price
            position.market_price = trade.price
            position.timestamp = trade.timestamp

    def _update_positions(self, timestamp: datetime) -> None:
        """
        Update all positions with current market prices.

        Args:
            timestamp: Current timestamp
        """
        if not self.market_data:
            return

        # Update market values and P&L for all positions
        for instrument_id, position in list(self.positions.items()):
            try:
                # Get latest price
                bar = self.market_data.get_latest_bar(position.instrument.symbol, timestamp)
                if bar:
                    # Update position with current market price
                    old_market_price = position.market_price
                    position.market_price = bar.close
                    position.timestamp = timestamp

                    # Calculate market value and unrealized P&L
                    position.market_value = position.market_price * position.quantity
                    position.unrealized_pnl = (position.market_price - position.average_price) * position.quantity

                    # Calculate daily P&L if market price changed
                    if position.market_price != old_market_price:
                        position.daily_pnl = (position.market_price - old_market_price) * position.quantity

            except Exception as e:
                self.logger.error(f"Error updating position {instrument_id}: {str(e)}")

    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Calculate performance metrics for the account.

        Returns:
            Dict[str, Any]: Performance metrics
        """
        account_info = self.get_account_info()

        # Basic metrics
        total_pnl = account_info["pnl"]
        total_return = account_info["pnl_percent"]

        # Calculate more advanced metrics if we have trade history
        sharpe_ratio = None
        max_drawdown = None
        win_rate = None

        if len(self.trades) > 0:
            # Calculate daily returns (simplified)
            daily_returns = self._calculate_daily_returns()

            # Sharpe ratio (assuming risk-free rate of 0)
            if len(daily_returns) > 1:
                annualized_return = np.mean(daily_returns) * 252  # Assuming 252 trading days
                annualized_volatility = np.std(daily_returns) * np.sqrt(252)
                sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility != 0 else 0

            # Maximum drawdown
            max_drawdown = self._calculate_max_drawdown()

            # Win rate
            win_rate = self._calculate_win_rate()

        return {
            "total_pnl": total_pnl,
            "total_return_percent": total_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "total_trades": len(self.trades),
            "active_positions": len(self.positions)
        }

    def _calculate_daily_returns(self) -> List[float]:
        """
        Calculate daily returns from account value history.

        Returns:
            List[float]: Daily return percentages
        """
        # In a real implementation, this would use stored daily account values
        # For now, just return a placeholder
        return [0.001]  # Placeholder

    def _calculate_max_drawdown(self) -> float:
        """
        Calculate maximum drawdown from equity curve.

        Returns:
            float: Maximum drawdown percentage
        """
        # Placeholder implementation
        return 0.0

    def _calculate_win_rate(self) -> float:
        """
        Calculate win rate from closed trades.

        Returns:
            float: Win rate as percentage
        """
        if not self.trades:
            return 0.0

        # This is a simplified implementation
        # In a real scenario, you'd track closed positions
        profitable_trades = 0

        for trade in self.trades:
            direction_multiplier = 1 if trade.direction == "BUY" else -1
            if (trade.market_price - trade.price) * direction_multiplier > 0:
                profitable_trades += 1

        return (profitable_trades / len(self.trades)) * 100

    def reset(self) -> None:
        """
        Reset the broker to initial state.
        Useful for multiple backtest runs.
        """
        self.cash_balance = self.initial_capital
        self.account_value = self.initial_capital
        self.orders = {}
        self.positions = {}
        self.trades = []
        self.current_timestamp = None
        self.logger.info("SimulatedBroker reset to initial state")

    def simulate_market_condition(self, condition: str, impact: float) -> None:
        """
        Simulate market conditions like gaps, limit-up/down, volatility.

        Args:
            condition: Type of condition to simulate
            impact: Magnitude of the impact
        """
        self.logger.info(f"Simulating market condition: {condition} with impact {impact}")

        if condition.lower() == "gap":
            # Simulate gap by adjusting all position prices
            for position in self.positions.values():
                direction = 1 if impact > 0 else -1
                position.market_price *= (1 + (impact * direction))
                position.market_value = position.market_price * position.quantity
                position.unrealized_pnl = (position.market_price - position.average_price) * position.quantity

        elif condition.lower() == "volatility":
            # Simulate volatility by adding random noise to position prices
            np.random.seed(int(datetime.now().timestamp()))
            for position in self.positions.values():
                noise = np.random.normal(0, impact)
                position.market_price *= (1 + noise)
                position.market_value = position.market_price * position.quantity
                position.unrealized_pnl = (position.market_price - position.average_price) * position.quantity

        elif condition.lower() == "liquidity_crunch":
            # Simulate liquidity issues by increasing slippage temporarily
            self.slippage *= (1 + impact)
            # Reset after next update

        # Update account value
        self.update(datetime.now())

    def get_api_key(self) -> str:
        """
        Get API key for market data integration.

        Returns:
            str: API key for the simulated broker
        """
        return self.config.get("api_key", "simulated_api_key")

    def get_api_secret(self) -> str:
        """
        Get API secret for market data integration.

        Returns:
            str: API secret for the simulated broker
        """
        return self.config.get("api_secret", "simulated_api_secret")

    def get_market_data_settings(self) -> Dict[str, Any]:
        """
        Get market data feed settings.

        Returns:
            Dict[str, Any]: Market data configuration settings
        """
        default_settings = {
            "data_source": "simulated",
            "update_interval": 60,  # seconds
            "cache_duration": 300,  # seconds
            "use_websockets": False,
            "max_historical_bars": 1000,
            "default_timeframe": "1d",
            "supported_timeframes": ["1m", "5m", "15m", "30m", "1h", "1d"],
            "exchange": "NSE",
            "market_hours": self.get_trading_hours(),
            "subscription_limit": 100,
            "rate_limit": {"requests": 50, "per_minute": 1}
        }

        # Override defaults with any settings provided in config
        if "market_data" in self.config:
            default_settings.update(self.config["market_data"])

        return default_settings

    def get_events(self) -> List[Dict[str, Any]]:
        """
        Get broker events like fills, cancellations, rejections.

        Returns:
            List[Dict[str, Any]]: List of broker events since last check
        """
        # In a real broker, this would return actual events that happened
        # For simulated broker, we'll return any newly filled orders
        events = []

        # Find orders that were filled since last check
        for order_id, order in self.orders.items():
            # Only process newly filled orders (that haven't been reported as events)
            if order.status == OrderStatus.FILLED and not getattr(order, '_event_processed', False):
                events.append({
                    'type': 'FILL',
                    'order_id': order_id,
                    'timestamp': order.filled_time,
                    'symbol': order.instrument.symbol,
                    'quantity': order.executed_quantity,
                    'price': order.executed_price,
                    'direction': order.direction
                })
                # Mark as processed
                setattr(order, '_event_processed', True)

        return events


