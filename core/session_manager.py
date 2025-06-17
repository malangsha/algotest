"""
Finvasia Shared Session Manager

This module provides a centralized session management solution for Finvasia API access,
ensuring that only one active session exists per user account while allowing multiple
components (broker and market data feed) to share the same session.

The SessionManager handles authentication, token refresh, and session expiry detection
in a thread-safe manner, with support for automatic retry on session expiration.
"""

import threading
import time
import pyotp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Callable, TypeVar

from NorenRestApiPy.NorenApi import NorenApi
from core.logging_manager import get_logger

# Type definition for callback functions
Session = TypeVar('Session')
Callable_T = TypeVar('Callable_T', bound=Callable)

class Session:
    """
    Represents a Finvasia API session with token and expiry information.
    """
    def __init__(self, token: str, user_id: str, password: str, expires_at: datetime = None, refresh_token: str = None, session_id: str = ""):
        """
        Initialize a Session object.
        
        Args:
            token: The session token from Finvasia API
            user_id: The user ID associated with this session
            password: The password used for this session (needed for set_session)
            expires_at: When this session expires (default: 8 hours from now)
            refresh_token: Optional refresh token if available
            session_id: Optional session identifier
        """
        self.token = token
        self.user_id = user_id
        self.password = password
        self.expires_at = expires_at or (datetime.now() + timedelta(hours=8))
        self.refresh_token = refresh_token
        self.session_id = session_id or ""
    
    def is_expired(self, buffer_minutes: int = 5) -> bool:
        """
        Check if the session is expired or about to expire.
        
        Args:
            buffer_minutes: Minutes before actual expiry to consider session as expired
            
        Returns:
            bool: True if session is expired or will expire within buffer_minutes
        """
        buffer = timedelta(minutes=buffer_minutes)
        return datetime.now() >= (self.expires_at - buffer)


class SessionManager:
    """
    Centralized session management for Finvasia API.
    
    Handles authentication, token refresh, and session expiry detection
    in a thread-safe manner, with support for automatic retry on session expiration.
    """
    
    def __init__(self, credentials: Dict[str, str]):
        """
        Initialize the SessionManager.
        
        Args:
            credentials: Dictionary containing Finvasia API credentials
                Required keys: 'user_id', 'password', 'twofa', 'api_key', 'imei', 'vc'
                Optional keys: 'api_url', 'ws_url'
        """
        self.logger = get_logger('core.session_manager')
        
        # Store credentials
        self._credentials = credentials
        self.user_id = credentials.get('user_id')
        self.password = credentials.get('password')
        self.twofa = credentials.get('twofa')
        self.api_key = credentials.get('api_key')
        self.imei = credentials.get('imei')
        self.vc = credentials.get('vc')
        self.api_url = credentials.get('api_url', 'https://api.shoonya.com/NorenWClientTP/')
        self.ws_url = credentials.get('ws_url', 'wss://api.shoonya.com/NorenWSTP/')
        
        # Session state
        self._current_session: Optional[Session] = None
        self._refresh_lock = threading.Lock()
        self._callbacks: List[Callable[[Session], None]] = []
        self._last_refresh = datetime.now()
        
        # Create NorenApi instance
        self._api = NorenApi(host=self.api_url, websocket=self.ws_url)
        
        self.logger.info("SessionManager initialized")
    
    def get_session(self) -> Session:
        """
        Get the current valid session, refreshing if needed.
        
        Returns:
            Session: A valid session object with token
            
        Raises:
            Exception: If authentication fails and cannot be recovered
        """
        with self._refresh_lock:
            # Check if we have a valid session
            if not self._current_session or self._current_session.is_expired():
                self._current_session = self._authenticate()
                self._notify_callbacks(self._current_session)
            
            return self._current_session
    
    def refresh_session(self) -> Session:
        """
        Force a session refresh regardless of current state.
        
        Returns:
            Session: A new session object with fresh token
            
        Raises:
            Exception: If authentication fails and cannot be recovered
        """
        with self._refresh_lock:
            self._current_session = self._authenticate()
            self._notify_callbacks(self._current_session)
            return self._current_session
    
    def is_session_valid(self) -> bool:
        """
        Check if the current session is valid.
        
        Returns:
            bool: True if a valid session exists, False otherwise
        """
        return self._current_session is not None and not self._current_session.is_expired()
    
    def register_callback(self, callback: Callable[[Session], None]) -> None:
        """
        Register a callback to be notified when session is refreshed.
        
        Args:
            callback: Function to call with the new session when refreshed
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)
    
    def start_refresh_scheduler(self, interval_hours: int = 8) -> None:
        """
        Start a background thread to periodically refresh the session.
        
        Args:
            interval_hours: How often to refresh the session in hours
        """
        def refresh_loop():
            while True:
                try:
                    # Sleep first to avoid immediate refresh after start
                    time.sleep(interval_hours * 60 * 60)
                    self.logger.info(f"Scheduled session refresh after {interval_hours} hours")
                    self.refresh_session()
                except Exception as e:
                    self.logger.error(f"Error in refresh scheduler: {str(e)}")
        
        refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        refresh_thread.start()
        self.logger.info(f"Session refresh scheduler started with {interval_hours} hour interval")
    
    def stop_refresh_scheduler(self) -> None:
        """
        Stop the background refresh scheduler.
        
        Note: This is a placeholder as Python daemon threads cannot be explicitly stopped.
        The thread will terminate when the main program exits.
        """
        self.logger.info("Session refresh scheduler will terminate when program exits")
    
    def _authenticate(self) -> Session:
        """
        Authenticate with Finvasia API and get a new session token.
        
        Returns:
            Session: A new session object with token
            
        Raises:
            Exception: If authentication fails and cannot be recovered
        """
        try:
            # Generate TOTP for 2FA
            twofa_token = pyotp.TOTP(self.twofa).now() if self.twofa else ""
            
            # Attempt login
            self.logger.info(f"Authenticating user {self.user_id} with Finvasia API")
            response = self._api.login(
                userid=self.user_id,
                password=self.password,
                twoFA=twofa_token,
                vendor_code=self.vc,
                api_secret=self.api_key,
                imei=self.imei
            )
            
            if response and 'susertoken' in response:
                token = response['susertoken']
                # Calculate expiry time (typically 8-12 hours, using 8 as conservative default)
                expires_at = datetime.now() + timedelta(hours=8)
                
                self.logger.info(f"Authentication successful for user {self.user_id}")
                self._last_refresh = datetime.now()
                
                # Create and return new session
                return Session(
                    token=token,
                    user_id=self.user_id,
                    password=self.password,
                    expires_at=expires_at
                )
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Authentication failed: {error_msg}")
                raise Exception(f"Authentication failed: {error_msg}")
                
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            raise
    
    def _notify_callbacks(self, session: Session) -> None:
        """
        Notify all registered callbacks about a session refresh.
        
        Args:
            session: The new session object
        """
        for callback in self._callbacks:
            try:
                callback(session)
            except Exception as e:
                self.logger.error(f"Error in session callback: {str(e)}")


class FinvasiaApiClient:
    """
    Client for making Finvasia API requests with automatic session management.
    
    This class wraps NorenApi methods with session token handling and
    automatic retry on session expiration.
    """
    
    def __init__(self, session_manager: SessionManager):
        """
        Initialize the API client.
        
        Args:
            session_manager: SessionManager instance for token management
        """
        self.logger = get_logger('core.finvasia_api_client')
        self.session_manager = session_manager
        self._api = NorenApi(
            host=session_manager.api_url,
            websocket=session_manager.ws_url
        )
    
    def place_order(self, order_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place an order with automatic session handling.
        
        Args:
            order_params: Order parameters for Finvasia API
            
        Returns:
            Dict: API response
            
        Raises:
            Exception: If order placement fails after retry
        """
        return self._execute_with_retry('place_order', order_params)
    
    def modify_order(self, order_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Modify an existing order with automatic session handling.
        
        Args:
            order_id: ID of the order to modify
            params: Modification parameters
            
        Returns:
            Dict: API response
            
        Raises:
            Exception: If order modification fails after retry
        """
        params['norenordno'] = order_id
        return self._execute_with_retry('modify_order', params)
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel an order with automatic session handling.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            Dict: API response
            
        Raises:
            Exception: If order cancellation fails after retry
        """
        params = {'norenordno': order_id}
        return self._execute_with_retry('cancel_order', params)
    
    def get_order_book(self) -> Dict[str, Any]:
        """
        Get the order book with automatic session handling.
        
        Returns:
            Dict: API response with order book
            
        Raises:
            Exception: If request fails after retry
        """
        return self._execute_with_retry('get_order_book', {})
    
    def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions with automatic session handling.
        
        Returns:
            Dict: API response with positions
            
        Raises:
            Exception: If request fails after retry
        """
        return self._execute_with_retry('get_positions', {})
    
    def start_websocket(self, callbacks: Dict[str, Callable]) -> None:
        """
        Start WebSocket connection with automatic session handling.
        
        Args:
            callbacks: Dictionary of callback functions for WebSocket events
            
        Raises:
            Exception: If WebSocket connection fails
        """
        session = self.session_manager.get_session()
        
        # Register for session refresh notifications
        def on_session_refresh(new_session):
            self.logger.info("Session refreshed, reconnecting WebSocket")
            try:
                self._api.close_websocket()
                time.sleep(1)  # Brief pause for cleanup
                self._start_websocket_with_session(new_session, callbacks)
            except Exception as e:
                self.logger.error(f"Error reconnecting WebSocket after session refresh: {str(e)}")
        
        self.session_manager.register_callback(on_session_refresh)
        
        # Start initial WebSocket connection
        self._start_websocket_with_session(session, callbacks)
    
    def _start_websocket_with_session(self, session: Session, callbacks: Dict[str, Callable]) -> None:
        """
        Start WebSocket with a specific session token.
        
        Args:
            session: Session object with token
            callbacks: Dictionary of callback functions for WebSocket events
        """
        # Set callbacks
        if 'order_update' in callbacks:
            self._api.on_order_update = callbacks['order_update']
        if 'market_data' in callbacks:
            self._api.on_ticks = callbacks['market_data']
        if 'open' in callbacks:
            self._api.on_open = callbacks['open']
        if 'close' in callbacks:
            self._api.on_close = callbacks['close']
        if 'error' in callbacks:
            self._api.on_error = callbacks['error']
        
        # Start WebSocket
        self._api.start_websocket(
            access_token=session.token,
            order_update_callback=callbacks.get('order_update'),
            subscribe_callback=callbacks.get('market_data'),
            socket_open_callback=callbacks.get('open'),
            socket_close_callback=callbacks.get('close')
        )
    
    def _execute_with_retry(self, method_name: str, params: Dict[str, Any], max_retries: int = 1) -> Dict[str, Any]:
        """
        Execute an API method with automatic session refresh and retry on failure.
        
        Args:
            method_name: Name of the NorenApi method to call
            params: Parameters for the method
            max_retries: Maximum number of retries on session expiry
            
        Returns:
            Dict: API response
            
        Raises:
            Exception: If the API call fails after retries
        """
        for attempt in range(max_retries + 1):
            try:
                # Get current session
                session = self.session_manager.get_session()
                
                # Ensure API method exists
                if not hasattr(self._api, method_name):
                    raise AttributeError(f"NorenApi has no method '{method_name}'")
                
                # Get method reference
                method = getattr(self._api, method_name)
                
                # Add session token to params if needed
                if 'jKey' not in params:
                    params['jKey'] = session.token
                
                # Execute API call
                response = method(**params)
                
                # Check for session expiry in response
                if isinstance(response, dict) and response.get('stat') == 'Not_Ok':
                    error_msg = response.get('emsg', '')
                    if 'Session Expired' in error_msg and attempt < max_retries:
                        self.logger.warning(f"Session expired during {method_name}. Refreshing and retrying...")
                        session = self.session_manager.refresh_session()
                        params['jKey'] = session.token
                        time.sleep(1 * (2 ** attempt))  # Exponential backoff
                        continue
                
                return response
                
            except Exception as e:
                if 'Session Expired' in str(e) and attempt < max_retries:
                    self.logger.warning(f"Session expired exception during {method_name}. Refreshing and retrying...")
                    session = self.session_manager.refresh_session()
                    time.sleep(1 * (2 ** attempt))  # Exponential backoff
                    continue
                
                if attempt == max_retries:
                    self.logger.error(f"Failed to execute {method_name} after {max_retries + 1} attempts: {str(e)}")
                    raise
                
                self.logger.warning(f"Error in {method_name} (attempt {attempt + 1}): {str(e)}")
                time.sleep(1 * (2 ** attempt))  # Exponential backoff
        
        # This should not be reached due to the raise in the loop
        raise Exception(f"Unexpected error in _execute_with_retry for {method_name}")
