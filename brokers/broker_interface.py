from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import pandas as pd

from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument
from models.position import Position
from models.market_data import MarketData, Bar, Quote, Trade

class BrokerInterface(ABC):
    """Abstract base class defining the interface for broker implementations."""

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the broker.

        Returns:
            bool: True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the broker.

        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if currently connected to the broker.

        Returns:
            bool: True if connected, False otherwise
        """
        pass

    @abstractmethod
    def place_order(self, order: Order) -> str:
        """
        Place a new order with the broker.

        Args:
            order: Order to place

        Returns:
            str: Order ID assigned by the broker
        """
        pass

    @abstractmethod
    def modify_order(self, order_id: str, modified_order: Order) -> bool:
        """
        Modify an existing order.

        Args:
            order_id: ID of the order to modify
            modified_order: Order with updated parameters

        Returns:
            bool: True if modification successful, False otherwise
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.

        Args:
            order_id: ID of the order to cancel

        Returns:
            bool: True if cancellation successful, False otherwise
        """
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """
        Get the current status of an order.

        Args:
            order_id: ID of the order

        Returns:
            OrderStatus: Current status of the order
        """
        pass

    # @abstractmethod
    def get_orders(self, instrument: Optional[Instrument] = None) -> List[Order]:
        """
        Get all active orders, optionally filtered by instrument.

        Args:
            instrument: Optional instrument to filter by

        Returns:
            List[Order]: List of active orders
        """
        pass

    @abstractmethod
    def get_positions(self, instrument: Optional[Instrument] = None) -> List[Position]:
        """
        Get current positions, optionally filtered by instrument.

        Args:
            instrument: Optional instrument to filter by

        Returns:
            List[Position]: List of current positions
        """
        pass

    @abstractmethod
    def get_account_balance(self) -> Dict[str, float]:
        """
        Get current account balance information.

        Returns:
            Dict[str, float]: Map of balance components to values
        """
        pass

    # @abstractmethod
    def get_market_data(self,
                       instrument: Instrument,
                       data_type: str,
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None,
                       interval: Optional[str] = None) -> MarketData:
        """
        Get market data for an instrument.

        Args:
            instrument: Instrument to get data for
            data_type: Type of data to retrieve (e.g., 'quote', 'trade', 'bar')
            start_time: Optional start time for historical data
            end_time: Optional end time for historical data
            interval: Optional interval for bar data

        Returns:
            MarketData: Requested market data
        """
        pass

    # @abstractmethod
    def subscribe_market_data(self,
                             instrument: Instrument,
                             data_type: str,
                             callback: callable) -> str:
        """
        Subscribe to real-time market data updates.

        Args:
            instrument: Instrument to subscribe for
            data_type: Type of data to subscribe to
            callback: Function to call with updates

        Returns:
            str: Subscription ID
        """
        pass

    # @abstractmethod
    def unsubscribe_market_data(self, subscription_id: str) -> bool:
        """
        Unsubscribe from market data updates.

        Args:
            subscription_id: ID of the subscription to cancel

        Returns:
            bool: True if unsubscription successful, False otherwise
        """
        pass

    # @abstractmethod
    def get_trading_hours(self, instrument: Instrument) -> Dict[str, datetime]:
        """
        Get trading hours for an instrument.

        Args:
            instrument: Instrument to get trading hours for

        Returns:
            Dict[str, datetime]: Map containing market open/close times
        """
        pass

    # @abstractmethod
    def get_instrument_details(self, instrument: Instrument) -> Dict[str, Any]:
        """
        Get detailed information about an instrument.

        Args:
            instrument: Instrument to get details for

        Returns:
            Dict[str, Any]: Map of instrument properties
        """
        pass

    # @abstractmethod
    def get_account_positions_summary(self) -> Dict[str, Any]:
        """
        Get summary of account positions, P&L, margin requirements, etc.

        Returns:
            Dict[str, Any]: Account summary information
        """
        pass

    # @abstractmethod
    def get_execution_history(self,
                            start_time: Optional[datetime] = None,
                            end_time: Optional[datetime] = None,
                            instrument: Optional[Instrument] = None) -> List[Dict[str, Any]]:
        """
        Get execution history for the account.

        Args:
            start_time: Optional start time filter
            end_time: Optional end time filter
            instrument: Optional instrument filter

        Returns:
            List[Dict[str, Any]]: List of executions
        """
        pass
