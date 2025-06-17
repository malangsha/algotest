"""
Conditional session manager initialization module for core engine.

This module implements the logic to conditionally initialize and use the SessionManager
based on configuration settings and whether the broker and data source are the same.
"""

import threading
from typing import Dict, Any, Optional

from core.logging_manager import get_logger
from core.session_manager import SessionManager

class SessionManagerFactory:
    """
    Factory class for creating and managing SessionManager instances.
    
    This class handles the conditional creation of SessionManager instances
    based on configuration settings and broker/data source compatibility.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> 'SessionManagerFactory':
        """
        Get the singleton instance of SessionManagerFactory.
        
        Returns:
            SessionManagerFactory: The singleton instance
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = SessionManagerFactory()
        return cls._instance
    
    def __init__(self):
        """Initialize the SessionManagerFactory."""
        self.logger = get_logger('core.session_manager_factory')
        self._session_managers = {}
    
    def create_session_manager(self, config: Dict[str, Any], broker_config: Dict[str, Any]) -> Optional[SessionManager]:
        """
        Create a SessionManager instance if enabled and appropriate.
        
        Args:
            config: Main configuration dictionary
            broker_config: Broker-specific configuration
            
        Returns:
            Optional[SessionManager]: A SessionManager instance if enabled and appropriate, None otherwise
        """
        # Check if session manager is enabled in config
        session_manager_config = config.get('broker_connections', {}).get('session_manager', {})
        enabled = session_manager_config.get('enabled', False)
        
        if not enabled:
            self.logger.info("Session manager is disabled in configuration")
            return None
        
        # Get broker and market data feed types
        broker_type = broker_config.get('broker_type', '').lower()
        market_data_feed_type = config.get('market_data', {}).get('live_feed_type', '').lower()
        
        # Check if broker and market data feed are compatible for session sharing
        if not self._is_compatible_for_session_sharing(broker_type, market_data_feed_type):
            self.logger.info(f"Session manager not used: broker ({broker_type}) and market data feed ({market_data_feed_type}) are not compatible for session sharing")
            return None
        
        # Create credentials dictionary from broker config
        # credentials = self._extract_credentials(broker_config)
        credentials = self._load_broker_config(config)

        # Check if we already have a session manager for this broker
        if broker_type in self._session_managers:
            self.logger.info(f"Returning existing session manager for {broker_type}")
            return self._session_managers[broker_type]
        
        # Create new session manager
        try:
            session_manager = SessionManager(credentials)
            
            # Start refresh scheduler if configured
            refresh_interval = session_manager_config.get('refresh_interval_hours', 8)
            session_manager.start_refresh_scheduler(interval_hours=refresh_interval)
            
            # Store and return
            self._session_managers[broker_type] = session_manager
            self.logger.info(f"Created new session manager for {broker_type}")
            return session_manager
            
        except Exception as e:
            self.logger.error(f"Failed to create session manager: {str(e)}")
            return None
    
    def _is_compatible_for_session_sharing(self, broker_type: str, market_data_feed_type: str) -> bool:
        """
        Check if broker and market data feed are compatible for session sharing.
        
        Args:
            broker_type: Type of broker
            market_data_feed_type: Type of market data feed
            
        Returns:
            bool: True if compatible, False otherwise
        """
        # For Finvasia, check if both broker and feed are Finvasia
        if broker_type == 'finvasia' and 'finvasia' in market_data_feed_type:
            return True
        
        # Add more compatibility checks for other brokers as needed
        
        return False
    
    def _extract_credentials(self, broker_config: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract credentials from broker configuration.
        
        Args:
            broker_config: Broker configuration dictionary
            
        Returns:
            Dict[str, str]: Credentials dictionary
        """
        # Start with empty credentials
        credentials = {}
        
        # Extract credentials from broker config
        credentials['user_id'] = broker_config.get('user_id', '')
        credentials['password'] = broker_config.get('password', '')
        credentials['api_key'] = broker_config.get('api_key', '')
        credentials['twofa'] = broker_config.get('twofa', '')
        credentials['vc'] = broker_config.get('vc', '')
        credentials['imei'] = broker_config.get('imei', '')
        
        # Extract API URLs if available
        api_config = broker_config.get('api', {})
        if 'api_url' in api_config:
            credentials['api_url'] = api_config['api_url']
        
        # For Finvasia, extract websocket URL if available
        if broker_config.get('broker_type', '').lower() == 'finvasia':
            credentials['ws_url'] = api_config.get('websocket_url', 'wss://api.shoonya.com/NorenWSTP/')
        
        return credentials
    
    def _load_broker_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load broker configuration from the provided config dictionary.

        Args:
           config: Configuration dictionary
           
        Returns:
            Dict[str, Any]: Processed configuration values
        """
        # Handle the new config structure with broker_connections
        if config and isinstance(config, dict):
            # Get broker connections from config
            broker_connections = config.get('broker_connections', {})
            active_connection = broker_connections.get('active_connection')
            connections = broker_connections.get('connections', [])

            # Find the active connection config
            active_broker_config = None
            for conn in connections:
                if conn.get('connection_name') == active_connection:
                    active_broker_config = conn
                    break

            if active_broker_config:
                # Load secrets from the specified file if available
                secrets_file = active_broker_config.get("secrets")
                if secrets_file:
                    try:
                        secrets = self._load_secrets(secrets_file)
                        # Use values from secrets if not provided directly
                        user_id = secrets.get('user_id')
                        password = secrets.get('password')
                        api_key = secrets.get('api_key')
                        imei = secrets.get('imei')
                        twofa = secrets.get('factor2')
                        vc = secrets.get('vc')
                    except Exception as e:
                        self.logger.error(f"Failed to load secrets from {secrets_file}: {str(e)}")

                # Get API settings
                api_config = active_broker_config.get("api", {})
                api_url = api_config.get("api_url")
                debug = api_config.get("debug", False)

        return {
            'api_url': api_url or "https://api.shoonya.com/NorenWClientTP/",
            'ws_url': "wss://api.shoonya.com/NorenWSTP/",
            'debug': debug,
            'user_id': user_id,
            'password': password,
            'api_key': api_key,
            'imei': imei,
            'twofa': twofa,
            'vc': vc
        }
    
    def _load_secrets(self, secrets_file: str) -> dict:
        """
        Load secrets from a file.

        Args:
            secrets_file: Path to the secrets file

        Returns:
            dict: Dictionary containing the secrets
        """
        secrets = {}
        try:
            with open(secrets_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split(':', 1)
                        secrets[key.strip()] = value.strip().strip('"\'')
            return secrets
        except Exception as e:
            self.logger.error(f"Error loading secrets from {secrets_file}: {str(e)}")
            raise

    def get_session_manager(self, broker_type: str) -> Optional[SessionManager]:
        """
        Get an existing SessionManager for a broker type.
        
        Args:
            broker_type: Type of broker
            
        Returns:
            Optional[SessionManager]: The SessionManager instance if it exists, None otherwise
        """
        return self._session_managers.get(broker_type.lower())
