"""
Improved finvasia_broker.py with consistent mapping dictionaries usage,
bug fixes, and enhanced code quality while preserving public interfaces.
"""

import time
import json
import threading
import logging
import pyotp

from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from NorenRestApiPy.NorenApi import NorenApi

from brokers.broker_interface import BrokerInterface
from models.order import OrderStatus 
from models.order import OrderType 
from models.instrument import Instrument
from utils.exceptions import BrokerError, OrderError
from utils.constants import OrderType as BrokerOrderType 
from core.logging_manager import get_logger

class FinvasiaBroker(BrokerInterface):
    """
    Finvasia broker implementation for the trading system.
    Integrates with Finvasia's NorenAPI for order execution and market data.
    
    This broker handles:
    - Order placement, modification, and cancellation
    - Market data retrieval
    - Account information and positions
    - Session management with automatic reconnection
    """
    
    ORDER_TYPE_MAPPING = {
        BrokerOrderType.MARKET.name: "MKT",
        BrokerOrderType.LIMIT.name: "LMT", 
        BrokerOrderType.SL_M.name: "SL-MKT",
        BrokerOrderType.SL.name: "SL-LMT",
        "MARKET": "MKT",  # Additional string mappings for flexibility
        "LIMIT": "LMT",
        "SL_MARKET": "SL-MKT",
        "SL_LIMIT": "SL-LMT"
    }

    STATUS_MAPPING = {
        'COMPLETE': OrderStatus.FILLED,
        'REJECTED': OrderStatus.REJECTED,
        'CANCELED': OrderStatus.CANCELLED,
        'CANCELLED': OrderStatus.CANCELLED,
        'OPEN': OrderStatus.OPEN,
        'PENDING': OrderStatus.PENDING,
        'PARTIALLY FILLED': OrderStatus.PARTIALLY_FILLED,
        'TRIGGER_PENDING': OrderStatus.PENDING,
        'AMO RECEIVED': OrderStatus.PENDING,
        'PUT ORDER REQ RECEIVED': OrderStatus.PENDING,
        'MODIFY VALIDATION PENDING': OrderStatus.PENDING,
        'CANCEL VALIDATION PENDING': OrderStatus.PENDING,
        'MODIFIED': OrderStatus.OPEN,
        'OPEN PENDING': OrderStatus.PENDING,
        'MODIFY PENDING': OrderStatus.PENDING,
        'CANCEL PENDING': OrderStatus.PENDING,
        'VALIDATION PENDING': OrderStatus.PENDING,
        # Additional status mappings for comprehensive coverage
        'NEW': OrderStatus.PENDING,
        'REPLACED': OrderStatus.OPEN,
        'EXPIRED': OrderStatus.CANCELLED,
        'FILLED': OrderStatus.FILLED
    }

    PRODUCT_TYPE_MAPPING = {
        "CNC": "C",
        "NRML": "M", 
        "MIS": "I",
        "BRACKET": "B",
        "COVER": "H",
        "BO": "B",
        "CO": "H",
        "DELIVERY": "C",
        "MARGIN": "M",
        "INTRADAY": "I"
    }
    
    BUY_SELL_MAPPING = {
        "BUY": "B",
        "SELL": "S",
        "B": "B",  # Already mapped values
        "S": "S"
    }
    
    RETENTION_MAPPING = {
        "DAY": "DAY",
        "IOC": "IOC", 
        "EOS": "EOS",
        "GTC": "GTC",  # Good Till Cancelled (if supported)
        "FOK": "FOK"   # Fill or Kill (if supported)
    }
    
    # Session expiration indicators for robust session management
    SESSION_EXPIRED_INDICATORS = [
        'session expired', 'invalid session', 'token expired', 'not logged in',
        'login failed', 'invalid user id or password', 'unauthorized', 
        'please login again', 'session invalid', 'authentication failed'
    ]
    
    # Terminal order state indicators
    TERMINAL_STATE_INDICATORS = [
        'order completed', 'order rejected', 'order cancelled', 'already completed',
        'already rejected', 'already cancelled', 'in terminal state', 
        'cannot cancel this order', 'oms order not found or already processed'
    ]
    
    def __init__(self, config: Dict[str, Any], **kwargs):
        """
        Initialize Finvasia broker with configuration and credentials.
        
        Args:
            config: Configuration dictionary
            **kwargs: Additional parameters including credentials and API config
        """
        self.logger = get_logger("brokers.finvasia_broker")
        self.logger.info("Initializing Finvasia broker")
        
        # Extract credentials from kwargs
        self.user_id: str = kwargs.get('user_id', '')
        self.password: str = kwargs.get('password', '')
        self.api_key: str = kwargs.get('api_key', '')
        self.twofa: str = kwargs.get('twofa', '')
        self.vc: str = kwargs.get('vc', '')
        self.imei: str = kwargs.get('imei', '')
        
        # Extract API configuration
        api_config: Dict[str, Any] = kwargs.get('api', {})
        self.api_url: str = api_config.get('api_url', 'https://api.shoonya.com/NorenWClientTP/')
        self.ws_url: str = api_config.get('websocket_url', 'wss://api.shoonya.com/NorenWSTP/')
        
        # Session management
        self._session_token: Optional[str] = None
        self._session_lock = threading.Lock()
        self._session_manager: Optional[Any] = kwargs.get('session_manager', None)
        self._last_health_check: Optional[datetime] = None
        
        # Initialize NorenApi instance
        self._api = NorenApi(host=self.api_url, websocket=self.ws_url)
        
        # Attempt initial connection
        if self.connect():
            self.logger.info("Finvasia broker initialized and connected successfully")
        else:
            self.logger.error("Finvasia broker initialization failed to connect")    
    
    def connect(self) -> bool:
        """
        Connect to the Finvasia API, using session manager if available.
        Ensures the NorenApi instance (self._api) is properly authenticated.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if self._session_manager and hasattr(self._session_manager, 'get_session'):
                return self._connect_via_session_manager()
            else:
                return self._connect_direct_login()
                
        except Exception as e:
            self.logger.error(f"Error connecting to Finvasia API: {str(e)}", exc_info=True)
            return False

    def _connect_via_session_manager(self) -> bool:
        """Connect using shared session manager."""
        self.logger.info("Attempting connection using shared session manager")
        
        if not self._session_manager:
            self.logger.error("Session manager is not available for connection.")
            return False
            
        session = self._session_manager.get_session()
        if not self._validate_session_object(session):
            self.logger.error("Session manager did not provide a valid session object")
            return False
            
        with self._session_lock:
            self._session_token = session.token
            if hasattr(session, 'user_id'):
                self.user_id = session.user_id
            if hasattr(session, 'password'):
                self.password = session.password
                
        set_session_response = self._api.set_session(
            userid=self.user_id,
            password=self.password, 
            usertoken=self._session_token
        )
        
        if self._is_session_set_successful(set_session_response):
            self.logger.info(f"Session successfully set in NorenApi for user {self.user_id}")
            return True
        else:
            error_msg = self._extract_error_message(
                set_session_response, 
                "Failed to set session via session manager"
            )
            self.logger.error(f"Failed to set session in NorenApi (via session manager): {error_msg}")
            with self._session_lock:
                self._session_token = None
            return False

    def _connect_direct_login(self) -> bool:
        """Connect using direct login with TOTP."""
        self.logger.info(f"Attempting direct login for user {self.user_id}")
        
        if not self.twofa:
            self.logger.error("TOTP secret (twofa) is missing for direct login.")
            return False
            
        try:
            twofa_token = pyotp.TOTP(self.twofa).now()
        except Exception as e:
            self.logger.error(f"Failed to generate TOTP token: {e}", exc_info=True)
            return False
            
        response = self._api.login(
            userid=self.user_id,
            password=self.password,
            twoFA=twofa_token,
            vendor_code=self.vc,
            api_secret=self.api_key,
            imei=self.imei
        )
        
        if self._is_login_successful(response):
            with self._session_lock:
                self._session_token = response['susertoken']
            self.logger.info(f"Direct login successful for user {self.user_id}")
            return True
        else:
            error_msg = self._extract_error_message(response, "Unknown error during direct login")
            self.logger.error(f"Direct login failed for user {self.user_id}: {error_msg}")
            return False

    def disconnect(self) -> bool:
        """Disconnect from Finvasia API."""
        try:
            if self._session_manager and hasattr(self._session_manager, 'is_session_valid'):
                self.logger.info("Using shared session manager; logout handled externally.")
                with self._session_lock:
                    self._session_token = None
                return True
                
            if self._session_token:
                with self._session_lock:
                    self._session_token = None
                self.logger.info("Disconnected from Finvasia API (local session token cleared)")
                return True
                
            self.logger.info("Already disconnected or no active session to clear.")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting: {str(e)}", exc_info=True)
            return False

    def is_connected(self) -> bool:
        """Check if broker is connected."""
        if self._session_manager and hasattr(self._session_manager, 'is_session_valid'):
            return self._session_manager.is_session_valid()
        with self._session_lock:
            return self._session_token is not None

    def place_order(self, instrument: Instrument, order_type: BrokerOrderType, quantity: int, 
                   side: str, price: Optional[float] = None, trigger_price: Optional[float] = None, 
                   **kwargs) -> Dict[str, Any]:
        """
        Place a new order with the broker.
        
        Args:
            instrument: Trading instrument
            order_type: Type of order (MARKET, LIMIT, STOP, STOP_LIMIT)
            quantity: Order quantity
            side: Order side (BUY/SELL)
            price: Order price (required for LIMIT and STOP_LIMIT orders)
            trigger_price: Trigger price (required for STOP and STOP_LIMIT orders)
            **kwargs: Additional order parameters
            
        Returns:
            Dict containing order_id, status, and raw_response
            
        Raises:
            OrderError: If order placement fails
        """
        if not self.is_connected():
            self.logger.error("Not connected to broker. Cannot place order")
            raise OrderError("Not connected to broker")
        
        try:
            # Map order parameters using mapping dictionaries
            order_params = self._map_order_params_for_placement(
                instrument, order_type, quantity, side, price, trigger_price, **kwargs
            )
            
            self.logger.debug(
                "About to place order with parameters:\n%s",
                "\n".join(f"  • {key}: {value!r}" for key, value in order_params.items())
            )

            self.logger.debug(f"Placing order for {instrument.symbol} with API params: {order_params}")
            response = self._api.place_order(**order_params)
            
            # Handle session expiry and retry
            if self._is_session_expired(response):
                self.logger.warning("Session expired during order placement, refreshing and retrying")
                if not self._refresh_session():
                    raise OrderError("Session refresh failed, cannot retry order placement")
                response = self._api.place_order(**order_params) 
            
            return self._process_place_order_response(response, instrument.symbol)
            
        except Exception as e:
            self.logger.error(f"Exception during place_order for {instrument.symbol}: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise OrderError(f"Exception placing order for {instrument.symbol}: {str(e)}")

    def modify_order(self, order_id: str, instrument: Instrument,
                     quantity: Optional[int] = None, price: Optional[float] = None,
                     trigger_price: Optional[float] = None, order_type: Optional[BrokerOrderType] = None,
                     **kwargs) -> Dict[str, Any]:
        """
        Modify an existing order.
        
        Args:
            order_id: ID of order to modify
            instrument: Trading instrument
            quantity: New quantity (optional)
            price: New price (optional)
            trigger_price: New trigger price (optional)
            order_type: New order type (optional)
            **kwargs: Additional parameters
            
        Returns:
            Dictionary containing order_id, status, and raw_response
            
        Raises:
            OrderError: If order modification fails
        """
        if not self.is_connected():
            raise OrderError("Not connected to broker")
            
        try:
            modify_params = self._map_order_params_for_modification(
                order_id, quantity, price, trigger_price, order_type, instrument, **kwargs
            )
            
            # Validate that we have actual modification parameters
            if self._is_modification_params_empty(modify_params):
                self.logger.error("No effective parameters provided for order modification beyond identification.")
                raise OrderError("No effective parameters provided for modification")

            self.logger.debug(f"Modifying order {order_id} with API params: {modify_params}")
            response = self._api.modify_order(**modify_params)
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired during order modification, attempting refresh.")
                if not self._refresh_session():
                    raise OrderError("Session refresh failed.")
                response = self._api.modify_order(**modify_params)
                
            return self._process_modify_order_response(response, order_id)
            
        except Exception as e:
            self.logger.error(f"Exception modifying order {order_id}: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise OrderError(f"Exception modifying order {order_id}: {str(e)}")

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order.
        
        Args:
            order_id: ID of order to cancel
            
        Returns:
            Dictionary containing order_id, status, and raw_response
            
        Raises:
            OrderError: If order cancellation fails
        """
        if not self.is_connected():
            raise OrderError("Not connected to broker")
            
        try:
            cancel_params = {'orderno': order_id}
            self.logger.debug(f"Cancelling order {order_id} using params: {cancel_params}")
            
            response = self._api.cancel_order(**cancel_params)
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired, attempting refresh.")
                if not self._refresh_session():
                    raise OrderError("Session refresh failed.")
                response = self._api.cancel_order(**cancel_params)
                
            return self._process_cancel_order_response(response, order_id)
            
        except Exception as e:
            self.logger.error(f"Exception cancelling order {order_id}: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise OrderError(f"Exception cancelling order {order_id}: {str(e)}")

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get status of a specific order."""
        if not self.is_connected():
            raise OrderError("Not connected to broker")
            
        response = None
        try:
            status_params = {'orderno': order_id}
            self.logger.debug(f"Getting order status for {order_id} using params: {status_params}")
            
            response = self._api.single_order_history(**status_params)
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired, attempting refresh.")
                if not self._refresh_session():
                    raise OrderError("Session refresh failed.")
                response = self._api.single_order_history(**status_params)
                
            return self._process_order_status_response(response, order_id)
            
        except json.JSONDecodeError as jde:
            self.logger.error(
                f"JSONDecodeError from single_order_history for {order_id}: {jde}. "
                f"API Response Text: {getattr(response, 'text', 'N/A') if hasattr(response, 'text') else 'Response object has no text attribute'}"
            )
            return {
                'order_id': order_id, 
                'status': OrderStatus.UNKNOWN.value,
                'raw_response': f"JSONDecodeError: {jde}",
                'message': 'Error parsing API response (malformed JSON from API)'
            }
        except Exception as e:
            self.logger.error(f"Exception getting status for order {order_id}: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise OrderError(f"Exception getting status for order {order_id}: {str(e)}")

    def get_order_book(self) -> List[Dict[str, Any]]:
        """Get all orders from order book."""
        if not self.is_connected():
            raise BrokerError("Not connected to broker")
            
        try:
            self.logger.debug("Getting order book")
            response = self._api.get_order_book()
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired, attempting refresh.")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed.")
                response = self._api.get_order_book()
                
            return self._process_order_book_response(response)
            
        except Exception as e:
            self.logger.error(f"Exception getting order book: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise BrokerError(f"Exception getting order book: {str(e)}")

    def get_positions(self) -> List[Dict[str, Any]]:
        """Get all positions."""
        if not self.is_connected():
            raise BrokerError("Not connected to broker")
            
        try:
            self.logger.debug("Getting positions")
            response = self._api.get_positions()
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired, attempting refresh.")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed.")
                response = self._api.get_positions()
                
            return self._process_positions_response(response)
            
        except Exception as e:
            self.logger.error(f"Exception getting positions: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise BrokerError(f"Exception getting positions: {str(e)}")

    def get_account_balance(self) -> Dict[str, Any]:
        """Get account balance and margin information."""
        if not self.is_connected():
            raise BrokerError("Not connected to broker")
            
        try:
            self.logger.debug(f"Getting account balance for user {self.user_id}")
            response = self._api.get_limits()
            
            if self._is_session_expired(response):
                self.logger.warning("Session expired, attempting refresh.")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed.")
                response = self._api.get_limits()
                
            return self._process_account_balance_response(response)
            
        except Exception as e:
            self.logger.error(f"Exception getting account balance: {str(e)}", exc_info=True)
            self._handle_api_authentication_error(e)
            raise BrokerError(f"Exception getting account balance: {str(e)}")

    # Private helper methods for mapping and processing
    def _map_order_params_for_placement(self, instrument: Instrument, order_type: BrokerOrderType,
                                       quantity: int, side: str, price: Optional[float],
                                       trigger_price: Optional[float], **kwargs) -> Dict[str, Any]:
        """Map order parameters for placement using consistent mappings."""
        # Map order type consistently
        order_type_key = order_type.name if hasattr(order_type, 'name') else str(order_type)
        api_price_type = self.ORDER_TYPE_MAPPING.get(order_type_key)
        if api_price_type is None:
            raise OrderError(f"Unsupported order_type '{order_type_key}' for Finvasia.")

        # Map side consistently
        api_side = self.BUY_SELL_MAPPING.get(side.upper())
        if api_side is None:
            raise OrderError(f"Unsupported side '{side}'.")

        # Map product type consistently
        product_type_key = kwargs.get('product_type', 'MIS').upper()
        api_product_type = self.PRODUCT_TYPE_MAPPING.get(product_type_key)
        if api_product_type is None:
            raise OrderError(f"Unsupported product_type '{product_type_key}'.")

        # Map retention consistently
        retention_key = kwargs.get('retention', 'DAY').upper()
        api_retention = self.RETENTION_MAPPING.get(retention_key, "DAY")

        # Prepare prices using helper functions
        api_price_value = self._prepare_order_price(api_price_type, price)
        api_trigger_price_value = self._prepare_trigger_price(api_price_type, trigger_price)

        return {
            'buy_or_sell': api_side,
            'product_type': api_product_type,
            'exchange': instrument.exchange,
            'tradingsymbol': instrument.symbol,
            'quantity': str(int(quantity)),
            'discloseqty': str(int(kwargs.get('disclose_quantity', 0))),
            'price_type': api_price_type,
            'price': str(api_price_value),
            'trigger_price': str(api_trigger_price_value) if api_trigger_price_value is not None else None,
            'retention': api_retention,
            'remarks': kwargs.get('remarks', f'order_{instrument.symbol[:15]}')
        }

    def _map_order_params_for_modification(self, order_id: str, quantity: Optional[int],
                                         price: Optional[float], trigger_price: Optional[float],
                                         order_type: Optional[BrokerOrderType],
                                         instrument: Instrument, **kwargs) -> Dict[str, Any]:
        """Map order parameters for modification using consistent mappings."""
        if not instrument:
            self.logger.error("Modify order called without instrument object. This is required.")
            raise OrderError("Instrument object is required for modify_order to determine exchange and tradingsymbol.")

        modify_params: Dict[str, Any] = {
            'orderno': order_id,
            'exchange': instrument.exchange,
            'tradingsymbol': instrument.symbol
        }

        # Add modification parameters
        if quantity is not None:
            modify_params['newquantity'] = str(quantity)
        if price is not None:
            modify_params['newprice'] = str(price)

        # Map new order type consistently
        if order_type is not None:
            order_type_key = order_type.name if hasattr(order_type, 'name') else str(order_type)
            api_new_order_type = self.ORDER_TYPE_MAPPING.get(order_type_key)
            if api_new_order_type:
                modify_params['newprice_type'] = api_new_order_type
            else:
                self.logger.warning(f"Invalid new order_type '{order_type_key}' for modification. 'newprice_type' will not be set based on it.")

        # Handle newprice_type if not set
        if 'newprice_type' not in modify_params:
            if price is not None:
                modify_params['newprice_type'] = "LMT"
            else:
                self.logger.warning("modify_order: newprice_type not specified and price not changing. Defaulting to LMT. This might be incorrect and API may require original type.")
                modify_params['newprice_type'] = "LMT"

        # Handle trigger price for SL orders
        if trigger_price is not None:
            effective_price_type = modify_params.get('newprice_type', kwargs.get('original_price_type', 'LMT'))
            if effective_price_type in ['SL-LMT', 'SL-MKT']:
                modify_params['newtrigger_price'] = str(trigger_price)
            else:
                self.logger.warning(f"Trigger price provided for modification but effective order type ({effective_price_type}) is not SL. Ignoring trigger price.")

        return modify_params

    def _is_modification_params_empty(self, modify_params: Dict[str, Any]) -> bool:
        """Check if modification parameters contain only identification fields."""
        identification_fields = {'orderno', 'exchange', 'tradingsymbol'}
        modification_fields = set(modify_params.keys()) - identification_fields
        return len(modification_fields) == 0
        
    def _prepare_order_price(self, api_price_type: str, price: Optional[float]) -> float:
        """Prepare order price based on order type."""
        if api_price_type in ['LMT', 'SL-LMT']:
            if price is None or price <= 0:
                raise OrderError(f"Valid positive price required for {api_price_type}.")
            return float(price)
        if api_price_type in ['MKT', 'SL-MKT']:
            return 0.0
        return float(price) if price is not None else 0.0

    def _prepare_trigger_price(self, api_price_type: str, trigger_price: Optional[float]) -> Optional[float]:
        """Prepare trigger price based on order type."""
        if api_price_type in ['SL-LMT', 'SL-MKT']:
            if trigger_price is None or trigger_price <= 0:
                raise OrderError(f"Valid positive trigger price required for {api_price_type}.")
            return float(trigger_price)
        elif trigger_price is not None:
            self.logger.warning(f"Trigger price provided for non-SL order type {api_price_type}.")
            return float(trigger_price)
        return None

    def _process_place_order_response(self, response: Any, symbol: str) -> Dict[str, Any]:
        """Process place order API response."""
        if response and response.get('stat') == 'Ok' and response.get('norenordno'):
            order_id = response['norenordno']
            self.logger.info(f"Order placed successfully for {symbol}: {order_id}")
            return {
                'order_id': order_id,
                'status': OrderStatus.PENDING.value,
                'raw_response': response
            }
        else:
            extracted_api_msg = self._extract_error_message(
                response, 
                f"Unknown API error (stat={response.get('stat') if response else 'None'})"
            )
            
            final_error_message = f"Failed to place order for {symbol}: {extracted_api_msg}"
            self.logger.error(f"{final_error_message}. API Response Object: {response}")
            raise OrderError(final_error_message)

    def _process_modify_order_response(self, response: Any, order_id: str) -> Dict[str, Any]:
        """Process modify order API response."""
        if response and response.get('stat') == 'Ok' and response.get('result') == order_id:
            self.logger.info(f"Order {order_id} modification request accepted.")
            return {
                'order_id': order_id,
                'status': OrderStatus.PENDING.value,
                'raw_response': response
            }
        else:
            error_msg = self._extract_error_message(response, f'Unknown error modifying order {order_id}')
            self.logger.error(f"Failed to modify order {order_id}: {error_msg}. Response: {response}")
            raise OrderError(f"Failed to modify order {order_id}: {error_msg}")

    def _process_cancel_order_response(self, response: Any, order_id: str) -> Dict[str, Any]:
        """Process cancel order API response."""
        if response and response.get('stat') == 'Ok' and response.get('result') == order_id:
            self.logger.info(f"Order {order_id} cancellation request accepted.")
            return {
                'order_id': order_id,
                'status': OrderStatus.CANCELLED.value,
                'raw_response': response
            }
        else:
            extracted_api_msg = self._extract_error_message(
                response, 
                f"Unknown API error (stat={response.get('stat') if response else 'None'})"
            )

            # Handle specific error cases
            if "order not found" in extracted_api_msg.lower():
                self.logger.info(f"Attempted to cancel order {order_id} but it was not found: {extracted_api_msg}")
                return {
                    'order_id': order_id,
                    'status': OrderStatus.UNKNOWN.value,
                    'raw_response': response,
                    'message': f"Order not found: {extracted_api_msg}"
                }

            if self._is_order_already_terminal(extracted_api_msg):
                self.logger.info(f"Order {order_id} was already in terminal state: {extracted_api_msg}")
                try:
                    current_status_info = self.get_order_status(order_id)
                except OrderError:
                    current_status_info = {'status': OrderStatus.UNKNOWN.value}
                    
                return {
                    'order_id': order_id,
                    'status': current_status_info.get('status', OrderStatus.UNKNOWN.value),
                    'raw_response': response,
                    'message': f"Order already terminal. Current status: {current_status_info.get('status', 'UNKNOWN')}"
                }

            final_error_message = f"Failed to cancel order {order_id}: {extracted_api_msg}"
            self.logger.error(f"{final_error_message}. API Response: {response}")
            raise OrderError(final_error_message)

    def _process_order_status_response(self, response: Any, order_id: str) -> Dict[str, Any]:
        """Process order status API response."""
        if not response:
            self.logger.error(f"Empty response from single_order_history for order {order_id}")
            return {
                'order_id': order_id,
                'status': OrderStatus.UNKNOWN.value,
                'raw_response': response,
                'message': 'Empty response from API'
            }

        if response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, f"Failed to get status for order {order_id}")
            self.logger.error(f"Order status request failed for {order_id}: {error_msg}")
            return {
                'order_id': order_id,
                'status': OrderStatus.UNKNOWN.value,
                'raw_response': response,
                'message': error_msg
            }

        # Extract order details from response
        order_data = response
        if isinstance(response, list) and len(response) > 0:
            order_data = response[0]  # Take first order if list
        elif isinstance(response, dict) and 'data' in response:
            order_data = response['data']
            if isinstance(order_data, list) and len(order_data) > 0:
                order_data = order_data[0]

        api_status = order_data.get('status', 'UNKNOWN').upper()
        mapped_status = self.STATUS_MAPPING.get(api_status, OrderStatus.UNKNOWN)

        return {
            'order_id': order_id,
            'status': mapped_status.value,
            'quantity': int(order_data.get('qty', 0)),
            'filled_quantity': int(order_data.get('fillshares', 0)),
            'price': float(order_data.get('prc', 0)),
            'average_price': float(order_data.get('avgprc', 0)),
            'symbol': order_data.get('tsym', ''),
            'side': order_data.get('trantype', ''),
            'order_type': order_data.get('prctyp', ''),
            'product_type': order_data.get('prd', ''),
            'timestamp': order_data.get('norentm', ''),
            'raw_response': response
        }

    def _process_order_book_response(self, response: Any) -> List[Dict[str, Any]]:
        """Process order book API response."""
        if not response:
            self.logger.warning("Empty response from get_order_book")
            return []

        if response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, "Failed to get order book")
            self.logger.error(f"Order book request failed: {error_msg}")
            raise BrokerError(f"Failed to get order book: {error_msg}")

        orders = []
        order_data = response if isinstance(response, list) else response.get('data', [])
        
        if not isinstance(order_data, list):
            order_data = [order_data] if order_data else []

        for order in order_data:
            if not isinstance(order, dict):
                continue

            api_status = order.get('status', 'UNKNOWN').upper()
            mapped_status = self.STATUS_MAPPING.get(api_status, OrderStatus.UNKNOWN)

            orders.append({
                'order_id': order.get('norenordno', ''),
                'status': mapped_status.value,
                'symbol': order.get('tsym', ''),
                'side': order.get('trantype', ''),
                'quantity': int(order.get('qty', 0)),
                'filled_quantity': int(order.get('fillshares', 0)),
                'price': float(order.get('prc', 0)),
                'average_price': float(order.get('avgprc', 0)),
                'order_type': order.get('prctyp', ''),
                'product_type': order.get('prd', ''),
                'exchange': order.get('exch', ''),
                'timestamp': order.get('norentm', ''),
                'raw_data': order
            })

        self.logger.debug(f"Retrieved {len(orders)} orders from order book")
        return orders

    def _process_positions_response(self, response: Any) -> List[Dict[str, Any]]:
        """Process positions API response."""
        if not response:
            self.logger.warning("Empty response from get_positions")
            return []

        if response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, "Failed to get positions")
            self.logger.error(f"Positions request failed: {error_msg}")
            raise BrokerError(f"Failed to get positions: {error_msg}")

        positions = []
        position_data = response if isinstance(response, list) else response.get('data', [])
        
        if not isinstance(position_data, list):
            position_data = [position_data] if position_data else []

        for position in position_data:
            if not isinstance(position, dict):
                continue

            net_qty = int(position.get('netqty', 0))
            if net_qty == 0:
                continue  # Skip zero positions

            positions.append({
                'symbol': position.get('tsym', ''),
                'exchange': position.get('exch', ''),
                'product_type': position.get('prd', ''),
                'quantity': net_qty,
                'buy_quantity': int(position.get('daybuyqty', 0)),
                'sell_quantity': int(position.get('daysellqty', 0)),
                'buy_average': float(position.get('daybuyavgprc', 0)),
                'sell_average': float(position.get('daysellavgprc', 0)),
                'realized_pnl': float(position.get('rpnl', 0)),
                'unrealized_pnl': float(position.get('urmtom', 0)),
                'ltp': float(position.get('lp', 0)),
                'raw_data': position
            })

        self.logger.debug(f"Retrieved {len(positions)} positions")
        return positions

    def _process_account_balance_response(self, response: Any) -> Dict[str, Any]:
        """Process account balance API response."""
        if not response:
            self.logger.error("Empty response from get_limits")
            raise BrokerError("Empty response from get_limits API")

        if response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, "Failed to get account balance")
            self.logger.error(f"Account balance request failed: {error_msg}")
            raise BrokerError(f"Failed to get account balance: {error_msg}")

        # Extract balance information
        cash = float(response.get('cash', 0))
        payin = float(response.get('payin', 0))
        payout = float(response.get('payout', 0))
        brkcollamt = float(response.get('brkcollamt', 0))
        unclearedcash = float(response.get('unclearedcash', 0))
        daycash = float(response.get('daycash', 0))
        
        # Calculate available balance
        available_balance = cash + payin - payout - brkcollamt - unclearedcash

        return {
            'total_balance': cash,
            'available_balance': max(0, available_balance),
            'payin': payin,
            'payout': payout,
            'collateral': brkcollamt,
            'uncleared_cash': unclearedcash,
            'day_cash': daycash,
            'margin_used': float(response.get('marginused', 0)),
            'margin_available': float(response.get('marginpremium', 0)),
            'raw_response': response
        }

    # Session management helper methods
    def _validate_session_object(self, session: Any) -> bool:
        """Validate session object from session manager."""
        if not session:
            return False
        
        required_attributes = ['token', 'user_id', 'password']
        return all(hasattr(session, attr) and getattr(session, attr) for attr in required_attributes)

    def _is_session_set_successful(self, response: Any) -> bool:
        """Check if session was set successfully."""
        if not response:
            return False
            
        # NorenApi.set_session might return different response formats
        # Check for common success indicators
        if isinstance(response, dict):
            return response.get('stat') == 'Ok' or response.get('status') == 'success'
        elif isinstance(response, bool):
            return response
        elif isinstance(response, str):
            return 'success' in response.lower() or 'ok' in response.lower()
            
        # If we can't determine, assume success if no obvious error
        return True

    def _is_login_successful(self, response: Any) -> bool:
        """Check if login was successful."""
        if not response:
            return False
            
        if isinstance(response, dict):
            return (response.get('stat') == 'Ok' and 
                   response.get('susertoken') is not None)
                   
        return False

    def _is_session_expired(self, response: Any) -> bool:
        """Check if response indicates session expiration."""
        if not response:
            return False
            
        if isinstance(response, dict):
            error_msg = response.get('emsg', '').lower()
            status = response.get('stat', '').lower()
            
            if status == 'not_ok' and any(indicator in error_msg for indicator in self.SESSION_EXPIRED_INDICATORS):
                return True
                
        return False

    def _refresh_session(self) -> bool:
        """Refresh the session by reconnecting."""
        self.logger.info("Attempting to refresh session")
        
        # Clear current session
        with self._session_lock:
            self._session_token = None
        
        # Attempt to reconnect
        return self.connect()

    def _get_current_user_id_for_api_calls(self) -> str:
        """Get current user ID for API calls."""
        if self._session_manager and hasattr(self._session_manager, 'get_session'):
            session = self._session_manager.get_session()
            if session and hasattr(session, 'user_id'):
                return session.user_id
        return self.user_id

    def _handle_api_authentication_error(self, exception: Exception) -> None:
        """Handle API authentication errors by clearing session."""
        error_str = str(exception).lower()
        auth_error_indicators = [
            'authentication', 'unauthorized', 'invalid token', 
            'session', 'login required'
        ]
        
        if any(indicator in error_str for indicator in auth_error_indicators):
            self.logger.warning("Authentication error detected, clearing session")
            with self._session_lock:
                self._session_token = None

    # Response processing helper methods
    def _extract_error_message(self, response: Any, default_message: str) -> str:
        """Extract error message from API response."""
        if not response:
            return default_message
            
        if isinstance(response, dict):
            # Try various error message fields
            for field in ['emsg', 'error', 'message', 'msg']:
                if field in response and response[field]:
                    return str(response[field])
                    
            # Check if it's a failed response with stat
            if response.get('stat') == 'Not_Ok':
                return f"API returned Not_Ok status: {response}"
                
        return default_message

    def _is_order_already_terminal(self, error_message: str) -> bool:
        """Check if error indicates order is already in terminal state."""
        error_lower = error_message.lower()
        return any(indicator in error_lower for indicator in self.TERMINAL_STATE_INDICATORS)

    # Market data methods (implementing BrokerInterface requirements)
    def get_market_data(self, instrument: Instrument) -> Dict[str, Any]:
        """
        Get market data for a specific instrument.
        """
        if not self.is_connected():
            self.logger.error("Not connected to broker. Cannot get market data")
            raise BrokerError("Not connected to broker")
            
        try:
            # Prepare parameters for market data request
            market_data_params = {
                'exchange': instrument.exchange,
                'token': instrument.token if hasattr(instrument, 'token') else instrument.symbol
            }
            
            self.logger.debug(f"Getting market data for {instrument.symbol}")
            response = self._api.get_quotes(**market_data_params)
            
            # Handle session expiry and retry
            if self._is_session_expired(response):
                self.logger.warning("Session expired during market data retrieval, refreshing and retrying")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed, cannot retry get_market_data")
                response = self._api.get_quotes(**market_data_params)
            
            return self._process_market_data_response(response, instrument.symbol)
                
        except Exception as e:
            self.logger.error(f"Exception getting market data for {instrument.symbol}: {str(e)}", exc_info=True)
            raise BrokerError(f"Exception getting market data for {instrument.symbol}: {str(e)}")

    def _process_market_data_response(self, response: Any, symbol: str) -> Dict[str, Any]:
        """Process market data response."""
        if response and isinstance(response, dict) and response.get('stat') == 'Ok':
            market_data = {
                'symbol': symbol,
                'last_price': float(response.get('lp', 0.0)),
                'open_price': float(response.get('o', 0.0)),
                'high_price': float(response.get('h', 0.0)),
                'low_price': float(response.get('l', 0.0)),
                'close_price': float(response.get('c', 0.0)),
                'volume': int(response.get('v', 0)),
                'bid_price': float(response.get('bp1', 0.0)),
                'ask_price': float(response.get('sp1', 0.0)),
                'bid_quantity': int(response.get('bq1', 0)),
                'ask_quantity': int(response.get('sq1', 0)),
                'timestamp': response.get('ft', ''),
                'raw_response': response
            }
            
            self.logger.debug(f"Market data retrieved for {symbol}: LTP={market_data['last_price']}")
            return market_data
        elif response and isinstance(response, dict) and response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, f'Failed to get market data for {symbol}')
            self.logger.error(f"API error getting market data for {symbol}: {error_msg}")
            raise BrokerError(f"API error getting market data for {symbol}: {error_msg}")
        else:
            self.logger.error(f"Failed to get market data for {symbol}, unexpected response: {response}")
            raise BrokerError(f"Failed to get market data for {symbol}, unexpected response: {response}")

    # Additional utility methods for order management
    def get_order_history(self, order_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get order history for all orders or a specific order.       
        """
        if not self.is_connected():
            self.logger.error("Not connected to broker. Cannot get order history")
            raise BrokerError("Not connected to broker")
            
        try:
            if order_id:
                # Get history for specific order
                history_params = {'norenordno': order_id}
                self.logger.debug(f"Getting order history for order {order_id}")
                response = self._api.single_order_history(**history_params)
            else:
                # Get all order history
                self.logger.debug("Getting complete order history")
                response = self._api.get_order_book()
            
            # Handle session expiry and retry
            if self._is_session_expired(response):
                self.logger.warning("Session expired during order history retrieval, refreshing and retrying")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed, cannot retry get_order_history")
                
                if order_id:
                    response = self._api.single_order_history(**history_params)
                else:
                    response = self._api.get_order_book()
            
            return self._process_order_history_response(response, order_id)
                
        except Exception as e:
            self.logger.error(f"Exception getting order history: {str(e)}", exc_info=True)
            raise BrokerError(f"Exception getting order history: {str(e)}")

    def _process_order_history_response(self, response: Any, order_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Process order history response using mapping dictionaries."""
        if response and isinstance(response, list):
            history = []
            for record in response:
                # Use STATUS_MAPPING for consistent status translation
                raw_status = record.get('status', '').upper()
                mapped_status = self.STATUS_MAPPING.get(raw_status, OrderStatus.UNKNOWN)
                
                # Use BUY_SELL_MAPPING for side translation (reverse lookup)
                raw_trantype = record.get('trantype', '')
                side = 'BUY' if raw_trantype == 'B' else 'SELL'
                
                history.append({
                    'order_id': record.get('norenordno', ''),
                    'status': mapped_status.value,
                    'instrument': record.get('tsym', ''),
                    'exchange': record.get('exch', ''),
                    'order_type': record.get('prctyp', ''),
                    'side': side,
                    'quantity': int(record.get('qty', 0)),
                    'filled_quantity': int(record.get('fillshares', 0)),
                    'price': float(record.get('prc', 0)),
                    'trigger_price': float(record.get('trgprc', 0)),
                    'average_price': float(record.get('avgprc', 0)),
                    'order_time': record.get('norentm', ''),
                    'update_time': record.get('exch_tm', ''),
                    'raw_data': record
                })
            
            if order_id:
                self.logger.info(f"Retrieved {len(history)} history records for order {order_id}")
            else:
                self.logger.info(f"Retrieved {len(history)} order history records")
            return history
        elif response and isinstance(response, dict) and response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, 'Failed to get order history')
            self.logger.error(f"API error getting order history: {error_msg}")
            raise BrokerError(f"API error getting order history: {error_msg}")
        else:
            self.logger.info("No order history found or empty response")
            return []

    # Trade book methods
    def get_trade_book(self) -> List[Dict[str, Any]]:
        if not self.is_connected():
            self.logger.error("Not connected to broker. Cannot get trade book")
            raise BrokerError("Not connected to broker")
            
        try:
            self.logger.debug("Getting trade book from NorenApi")
            response = self._api.get_trade_book()
            
            # Handle session expiry and retry
            if self._is_session_expired(response):
                self.logger.warning("Session expired during trade book retrieval, refreshing and retrying")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed, cannot retry get_trade_book")
                response = self._api.get_trade_book()
            
            return self._process_trade_book_response(response)
                
        except Exception as e:
            self.logger.error(f"Exception getting trade book: {str(e)}", exc_info=True)
            raise BrokerError(f"Exception getting trade book: {str(e)}")

    def _process_trade_book_response(self, response: Any) -> List[Dict[str, Any]]:
        """Process trade book response using mapping dictionaries."""
        if response and isinstance(response, list):
            trades = []
            for trade_data in response:
                # Use BUY_SELL_MAPPING for side translation (reverse lookup)
                raw_trantype = trade_data.get('trantype', '')
                side = 'BUY' if raw_trantype == 'B' else 'SELL'
                
                trades.append({
                    'trade_id': trade_data.get('fltm', ''),  # Fill time as trade ID
                    'order_id': trade_data.get('norenordno', ''),
                    'instrument': trade_data.get('tsym', ''),
                    'exchange': trade_data.get('exch', ''),
                    'side': side,
                    'quantity': int(trade_data.get('qty', 0)),
                    'price': float(trade_data.get('flprc', 0.0)),
                    'trade_time': trade_data.get('fltm', ''),
                    'product_type': trade_data.get('prd', ''),
                    'raw_data': trade_data
                })
            
            self.logger.info(f"Retrieved {len(trades)} trades from trade book")
            return trades
        elif response and isinstance(response, dict) and response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, 'Failed to get trade book')
            self.logger.error(f"API error getting trade book: {error_msg}")
            raise BrokerError(f"API error getting trade book: {error_msg}")
        else:
            self.logger.info("No trades found or empty response")
            return []

    # Holdings methods
    def get_holdings(self) -> List[Dict[str, Any]]:
        if not self.is_connected():
            self.logger.error("Not connected to broker. Cannot get holdings")
            raise BrokerError("Not connected to broker")
            
        try:
            self.logger.debug("Getting holdings from NorenApi")
            response = self._api.get_holdings()
            
            # Handle session expiry and retry
            if self._is_session_expired(response):
                self.logger.warning("Session expired during holdings retrieval, refreshing and retrying")
                if not self._refresh_session():
                    raise BrokerError("Session refresh failed, cannot retry get_holdings")
                response = self._api.get_holdings()
            
            return self._process_holdings_response(response)
                
        except Exception as e:
            self.logger.error(f"Exception getting holdings: {str(e)}", exc_info=True)
            raise BrokerError(f"Exception getting holdings: {str(e)}")

    def _process_holdings_response(self, response: Any) -> List[Dict[str, Any]]:
        """Process holdings response."""
        if response and isinstance(response, list):
            holdings = []
            for holding_data in response:
                quantity = int(holding_data.get('holdqty', 0))
                avg_price = float(holding_data.get('upldprc', 0.0))
                current_price = float(holding_data.get('ltp', 0.0))
                
                # Calculate P&L
                investment_value = quantity * avg_price
                current_value = quantity * current_price
                unrealized_pnl = current_value - investment_value
                
                holdings.append({
                    'instrument': holding_data.get('tsym', ''),
                    'exchange': holding_data.get('exch', ''),
                    'quantity': quantity,
                    'average_price': avg_price,
                    'current_price': current_price,
                    'investment_value': investment_value,
                    'current_value': current_value,
                    'unrealized_pnl': unrealized_pnl,
                    'pnl_percentage': (unrealized_pnl / investment_value * 100) if investment_value > 0 else 0.0,
                    'raw_data': holding_data
                })
            
            self.logger.info(f"Retrieved {len(holdings)} holdings")
            return holdings
        elif response and isinstance(response, dict) and response.get('stat') == 'Not_Ok':
            error_msg = self._extract_error_message(response, 'Failed to get holdings')
            self.logger.error(f"API error getting holdings: {error_msg}")
            raise BrokerError(f"API error getting holdings: {error_msg}")
        else:
            self.logger.info("No holdings found or empty response")
            return []

    # Health check and diagnostic methods
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on broker connection."""
        try:
            if not self.is_connected():
                return {
                    'status': 'unhealthy',
                    'message': 'Not connected to broker',
                    'timestamp': datetime.now().isoformat()
                }
                
            # Try a simple API call to verify connection
            response = self._api.get_limits()
            
            if self._is_session_expired(response):
                # Try to refresh session
                if self._refresh_session():
                    return {
                        'status': 'healthy',
                        'message': 'Session refreshed successfully',
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    return {
                        'status': 'unhealthy',
                        'message': 'Session expired and refresh failed',
                        'timestamp': datetime.now().isoformat()
                    }
                    
            if response and response.get('stat') == 'Ok':
                self._last_health_check = datetime.now()
                return {
                    'status': 'healthy',
                    'message': 'Connection verified',
                    'timestamp': self._last_health_check.isoformat()
                }
            else:
                error_msg = self._extract_error_message(response, "Unknown API error")
                return {
                    'status': 'unhealthy',
                    'message': f'API error: {error_msg}',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}", exc_info=True)
            return {
                'status': 'unhealthy',
                'message': f'Health check exception: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }

    def get_broker_info(self) -> Dict[str, Any]:
        """Get broker information and capabilities."""
        return {
            'name': 'Finvasia',
            'version': '1.0.0',
            'capabilities': {
                'order_types': list(self.ORDER_TYPE_MAPPING.keys()),
                'product_types': list(self.PRODUCT_TYPE_MAPPING.keys()),
                'exchanges': ['NSE', 'BSE', 'NFO', 'BFO', 'MCX'],
                'supports_modify': True,
                'supports_cancel': True,
                'supports_bracket_orders': True,
                'supports_cover_orders': True
            },
            'connection_status': 'connected' if self.is_connected() else 'disconnected',
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None
        }

    def __str__(self) -> str:
        """String representation of broker."""
        status = "connected" if self.is_connected() else "disconnected"
        return f"FinvasiaBroker(user_id={self.user_id}, status={status})"

    def __repr__(self) -> str:
        """Detailed string representation of broker."""
        return (f"FinvasiaBroker("
                f"user_id='{self.user_id}', "
                f"api_url='{self.api_url}', "
                f"connected={self.is_connected()})")