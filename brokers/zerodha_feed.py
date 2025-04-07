import threading
import time
import logging
import json
import random
import math
import websocket
import requests
import uuid
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
import hashlib
from collections import deque

from models.instrument import Instrument
from utils.constants import MarketDataType, EventType, InstrumentType, OptionType
from models.events import MarketDataEvent
from utils.exceptions import MarketDataException
from core.event_manager import EventManager


class ZerodhaFeed(MarketDataFeedBase):
    """Advanced WebSocket feed handler for Zerodha Kite API with intelligent connection management,
    predictive reconnection, adaptive throttling, and comprehensive error recovery mechanisms."""

    def __init__(
        self,
        instruments: List[Instrument],
        callback: Callable,
        websocket_url: str = "wss://ws.kite.trade/",
        api_key: str = None,
        api_secret: str = None,
        access_token: str = None,
        heartbeat_interval: int = 25,  # Slightly more frequent than Finvasia
        reconnect_attempts: int = 10,  # More attempts than Finvasia
        reconnect_delay: int = 3,  # Shorter initial delay for faster recovery
        socket_timeout: int = 8,  # More aggressive timeout
        ping_interval: int = 25,  # More frequent ping
        ping_timeout: int = 8,  # Faster ping timeout
        user_agent: str = "ZerodhaFeed/2.0",
        cache_size: int = 1000,  # Size of the LRU cache
        connection_pool_size: int = 5,  # Size of the connection pool
        throttle_limit: int = 100,  # Requests per minute limit
        batch_processing: bool = True,  # Enable batch processing
        compression_enabled: bool = True,  # Enable data compression
        max_queue_size: int = 10000,  # Maximum queue size for message backlog
        log_level: int = logging.INFO,
    ):
        """Initialize the advanced Zerodha feed handler with intelligent features.

        Args:
            instruments: List of instruments to subscribe to
            callback: Callback function for market data events
            websocket_url: WebSocket URL for Zerodha Kite API
            api_key: API key for authentication
            api_secret: API secret for authentication
            access_token: Optional pre-authenticated access token
            heartbeat_interval: Interval in seconds for heartbeat checks
            reconnect_attempts: Maximum number of reconnection attempts
            reconnect_delay: Initial delay in seconds between reconnection attempts
            socket_timeout: WebSocket connection timeout in seconds
            ping_interval: WebSocket ping interval in seconds
            ping_timeout: WebSocket ping timeout in seconds
            user_agent: User agent for the WebSocket connection
            cache_size: Size of the LRU cache for market data
            connection_pool_size: Size of the connection pool
            throttle_limit: Requests per minute limit
            batch_processing: Enable batch processing of market data
            compression_enabled: Enable data compression
            max_queue_size: Maximum queue size for message backlog
            log_level: Logging level
        """
        super().__init__(instruments, callback)
        
        # Configure logging
        self.logger = logging.getLogger("ZerodhaFeed")
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        self.logger.info("Initializing ZerodhaFeed")
        
        # Authentication settings
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        
        # Connection settings
        self.websocket_url = websocket_url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.socket_timeout = socket_timeout
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.user_agent = user_agent
        
        # Advanced settings
        self.cache_size = cache_size
        self.connection_pool_size = connection_pool_size
        self.throttle_limit = throttle_limit
        self.batch_processing = batch_processing
        self.compression_enabled = compression_enabled
        self.max_queue_size = max_queue_size
        
        # WebSocket connection
        self.ws = None
        self.ws_lock = threading.RLock()  # Reentrant lock for advanced thread safety
        self.is_connected = False
        self.connection_timeout = 20  # seconds
        self.connection_pool = []
        self.connection_pool_lock = threading.RLock()
        
        # Session management
        self.session_id = str(uuid.uuid4())
        self.active_users = {}  # {user_id: {'last_activity': timestamp, 'subscriptions': [symbols]}}
        self.user_sessions = {}  # {user_id: {'ws': websocket, 'session_id': id, 'status': status}}
        self.user_lock = threading.RLock()
        
        # Subscription management
        self.subscriptions = {}  # {symbol: {'users': [user_ids], 'last_data': data}}
        self.subscription_lock = threading.RLock()
        self.subscription_groups = {}  # Group subscriptions for batch processing
        
        # Message queue for backpressure handling
        self.message_queue = deque(maxlen=self.max_queue_size)
        self.queue_lock = threading.RLock()
        
        # Cache management
        self.market_data_cache = {}  # Symbol-based cache with TTL
        self.cache_lock = threading.RLock()
        self.lru_cache_size = cache_size
        self.lru_cache = {}
        self.lru_order = deque()
        
        # Rate limiting
        self.request_timestamps = deque(maxlen=self.throttle_limit)
        self.throttle_lock = threading.RLock()
        
        # Callback handlers with priority queue
        self.callback_handlers = {
            "connect": [],
            "disconnect": [],
            "error": [],
            "message": [],
            "reconnect": [],
            "throttle": [],
            "cache_miss": [],
            "heartbeat": [],
        }
        self.callback_lock = threading.RLock()
        
        # Thread management
        self.threads = {
            "heartbeat": None,
            "reconnect": None,
            "listener": None,
            "processor": None,
            "cache_cleanup": None,
            "throttle_monitor": None,
        }
        self.thread_lock = threading.RLock()
        self.is_running = False
        self.reconnect_count = 0
        self.last_heartbeat = None
        self.adaptive_reconnect = True  # Use exponential backoff for reconnection
        
        # Statistics and monitoring
        self.stats = {
            "messages_received": 0,
            "messages_sent": 0,
            "errors": 0,
            "reconnects": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "throttled_requests": 0,
            "batch_operations": 0,
            "latency_ms": deque(maxlen=100),  # Track last 100 latency measurements
            "start_time": None,
            "last_error": None,
            "last_heartbeat": None,
            "connection_quality": 100,  # Percentage of connection quality
        }
        self.stats_lock = threading.RLock()
        
        # Performance monitoring
        self.latency_threshold_ms = 100  # Threshold for high latency warning
        self.performance_samples = 50  # Number of samples for moving average
        self.performance_metrics = {
            "cpu_usage": deque(maxlen=self.performance_samples),
            "memory_usage": deque(maxlen=self.performance_samples),
            "message_rate": deque(maxlen=self.performance_samples),
            "error_rate": deque(maxlen=self.performance_samples),
        }
        
        # Initialize cache cleanup thread
        self._init_cache_cleanup()
        
        self.logger.info("ZerodhaFeed initialized successfully")

    def connect(self) -> bool:
        """Establish connection to Zerodha WebSocket with intelligent retry logic.

        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info("Establishing connection to Zerodha WebSocket")
        
        with self.ws_lock:
            if self.is_connected and self.ws is not None:
                self.logger.debug("Already connected, skipping connection")
                return True
            
            # Check for rate limiting
            if not self._check_rate_limit("connect"):
                self.logger.warning("Connection attempt throttled")
                return False
            
            # Try to get a connection from the pool first
            pooled_connection = self._get_connection_from_pool()
            if pooled_connection:
                self.logger.debug("Using connection from pool")
                self.ws = pooled_connection
                self.is_connected = True
                self._trigger_callbacks("connect", {"source": "pool"})
                return True
            
            # Create a new connection
            try:
                self.logger.debug(f"Creating new WebSocket connection to {self.websocket_url}")
                self.is_connected = False
                
                # Setup WebSocket with optimized settings
                websocket.enableTrace(False)  # Disable trace for production
                self.ws = websocket.WebSocketApp(
                    self.websocket_url,
                    on_open=lambda ws: self._on_open(ws),
                    on_message=lambda ws, msg: self._on_message(ws, msg),
                    on_error=lambda ws, err: self._on_error(ws, err),
                    on_close=lambda ws, close_status_code, close_reason: self._on_close(
                        ws, close_status_code, close_reason
                    ),
                    header={
                        "User-Agent": self.user_agent,
                        "X-Kite-Version": "3",  # Zerodha API version
                        "X-Client-ID": self.api_key,
                    },
                )
                
                # Start WebSocket connection in a separate thread
                self.threads["listener"] = threading.Thread(
                    target=self._run_websocket, 
                    name="ZerodhaFeedListener",
                    daemon=True
                )
                self.threads["listener"].start()
                
                # Wait for connection to establish with timeout
                connection_start = time.time()
                while not self.is_connected:
                    if time.time() - connection_start > self.connection_timeout:
                        self.logger.error("Connection timeout")
                        return False
                    time.sleep(0.1)
                
                # Update statistics
                with self.stats_lock:
                    self.stats["start_time"] = datetime.now()
                
                self.logger.info("Connection established successfully")
                return True
                
            except Exception as e:
                self.logger.error(f"Connection error: {str(e)}")
                with self.stats_lock:
                    self.stats["errors"] += 1
                    self.stats["last_error"] = {
                        "time": datetime.now(),
                        "message": str(e),
                        "type": "connection",
                    }
                return False

    def _run_websocket(self) -> None:
        """Run the WebSocket connection with optimized settings.
        """
        self.logger.debug("Starting WebSocket connection thread")
        try:
            # Set WebSocket loop options with optimized parameters
            self.ws.run_forever(
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                reconnect=False,  # We handle reconnection ourselves
                skip_utf8_validation=True,  # Performance optimization
                sslopt={"cert_reqs": 0},  # Disable certificate validation for performance
            )
        except Exception as e:
            self.logger.error(f"WebSocket run error: {str(e)}")
            with self.stats_lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = {
                    "time": datetime.now(),
                    "message": str(e),
                    "type": "websocket_run",
                }
            self._schedule_reconnect()

    def _on_open(self, ws) -> None:
        """Handle WebSocket connection open event.

        Args:
            ws: WebSocket instance
        """
        self.logger.info("WebSocket connection opened")
        self.is_connected = True
        
        # Reset reconnect counter on successful connection
        self.reconnect_count = 0
        
        # Authenticate immediately
        if not self._authenticate():
            self.logger.error("Authentication failed")
            self.disconnect()
            self._schedule_reconnect()
            return
        
        # Start heartbeat monitor
        self._start_heartbeat_monitor()
        
        # Start message processor
        self._start_message_processor()
        
        # Start additional monitoring threads
        self._start_throttle_monitor()
        
        # Update connection quality
        with self.stats_lock:
            self.stats["connection_quality"] = 100
        
        # Trigger callbacks
        self._trigger_callbacks("connect", {"source": "new"})
        
        # Resubscribe to all active subscriptions
        self._resubscribe_all()

    def _on_message(self, ws, message: str) -> None:
        """Handle WebSocket message with intelligent processing.

        Args:
            ws: WebSocket instance
            message: Message received
        """
        receive_time = time.time()
        
        # Update statistics
        with self.stats_lock:
            self.stats["messages_received"] += 1
        
        # Handle binary data (Zerodha sends market data in binary format)
        try:
            if isinstance(message, bytes):
                if self.compression_enabled:
                    # Decompress binary data
                    import zlib
                    message = zlib.decompress(message)
                
                # Parse binary data
                data = self._parse_binary_data(message)
            else:
                # Parse JSON data
                data = json.loads(message)
            
            # Measure processing latency
            with self.stats_lock:
                process_time = time.time()
                latency_ms = (process_time - receive_time) * 1000
                self.stats["latency_ms"].append(latency_ms)
                
                # Check for high latency
                if latency_ms > self.latency_threshold_ms:
                    self.logger.warning(f"High message processing latency: {latency_ms:.2f}ms")
            
            # Prioritize message handling
            message_type = data.get("type")
            
            # Process messages based on priority
            if message_type == "heartbeat":
                self._process_heartbeat(data)
            elif message_type == "error":
                self._process_error(data)
            elif message_type == "auth":
                self._process_auth_response(data)
            elif message_type in ["tick", "order_update", "trade_update"]:
                # Add to processing queue instead of immediate processing
                # for better throughput
                with self.queue_lock:
                    self.message_queue.append((receive_time, data))
            else:
                # Handle other message types
                self.logger.debug(f"Received unknown message type: {message_type}")
                self._trigger_callbacks("message", data)
                
        except Exception as e:
            self.logger.error(f"Message processing error: {str(e)}")
            with self.stats_lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = {
                    "time": datetime.now(),
                    "message": str(e),
                    "type": "message_processing",
                }

    def _parse_binary_data(self, binary_data: bytes) -> Dict:
        """Parse binary data from Zerodha WebSocket.
        
        Args:
            binary_data: Binary data received from WebSocket
            
        Returns:
            Dict: Parsed market data
        """
        # Implement Zerodha binary data parsing logic
        # This is a simplified implementation
        try:
            # Zerodha sends data in a specific binary format
            # First byte is the packet type
            packet_type = binary_data[0]
            
            if packet_type == 0:  # Index packet
                # Parse index data
                pass
            elif packet_type == 1:  # Full market data packet
                return self._parse_full_market_data(binary_data)
            elif packet_type == 2:  # Incremental market data packet
                return self._parse_incremental_market_data(binary_data)
            else:
                self.logger.warning(f"Unknown packet type: {packet_type}")
                return {"type": "unknown", "data": binary_data.hex()}
                
        except Exception as e:
            self.logger.error(f"Binary data parsing error: {str(e)}")
            return {"type": "error", "message": f"Binary parsing error: {str(e)}"}
    
    def _parse_full_market_data(self, data: bytes) -> Dict:
        """Parse full market data binary packet.
        
        Args:
            data: Binary data
            
        Returns:
            Dict: Parsed market data
        """
        # Implement Zerodha full market data parsing logic
        # This is a simplified implementation
        return {
            "type": "tick",
            "data": {
                "instrument_token": int.from_bytes(data[1:5], byteorder="big"),
                "last_price": int.from_bytes(data[5:9], byteorder="big") / 100,
                "volume": int.from_bytes(data[9:13], byteorder="big"),
                "buy_quantity": int.from_bytes(data[13:17], byteorder="big"),
                "sell_quantity": int.from_bytes(data[17:21], byteorder="big"),
                "open": int.from_bytes(data[21:25], byteorder="big") / 100,
                "high": int.from_bytes(data[25:29], byteorder="big") / 100,
                "low": int.from_bytes(data[29:33], byteorder="big") / 100,
                "close": int.from_bytes(data[33:37], byteorder="big") / 100,
                # Additional fields are parsed similarly
            }
        }
    
    def _parse_incremental_market_data(self, data: bytes) -> Dict:
        """Parse incremental market data binary packet.
        
        Args:
            data: Binary data
            
        Returns:
            Dict: Parsed market data
        """
        # Implement Zerodha incremental market data parsing logic
        # This is a simplified implementation
        return {
            "type": "tick",
            "mode": "incremental",
            "data": {
                "instrument_token": int.from_bytes(data[1:5], byteorder="big"),
                "last_price": int.from_bytes(data[5:9], byteorder="big") / 100,
                # Only changed fields are included in incremental updates
            }
        }

    def _on_error(self, ws, error) -> None:
        """Handle WebSocket error with intelligent error recovery.

        Args:
            ws: WebSocket instance
            error: Error object
        """
        self.logger.error(f"WebSocket error: {str(error)}")
        
        with self.stats_lock:
            self.stats["errors"] += 1
            self.stats["last_error"] = {
                "time": datetime.now(),
                "message": str(error),
                "type": "websocket_error",
            }
            
            # Adjust connection quality based on error severity
            error_severity = self._assess_error_severity(error)
            self.stats["connection_quality"] = max(
                0, self.stats["connection_quality"] - error_severity
            )
        
        # Intelligent error handling based on error type
        if isinstance(error, (ConnectionRefusedError, ConnectionResetError)):
            self.logger.warning("Connection error, scheduling immediate reconnect")
            self.is_connected = False
            self._schedule_reconnect(immediate=True)
        elif isinstance(error, websocket.WebSocketTimeoutException):
            self.logger.warning("WebSocket timeout, checking connection health")
            self._check_connection_health()
        elif isinstance(error, websocket.WebSocketConnectionClosedException):
            self.logger.warning("WebSocket connection closed")
            self.is_connected = False
            self._schedule_reconnect()
        else:
            # Generic error handling
            self._trigger_callbacks("error", {"error": str(error)})
            
            # Check if reconnection is needed
            if not self.is_connected:
                self._schedule_reconnect()

    def _assess_error_severity(self, error) -> int:
        """Assess the severity of an error for connection quality calculation.
        
        Args:
            error: Error object
            
        Returns:
            int: Error severity (0-100)
        """
        # Implement error severity assessment logic
        if isinstance(error, (ConnectionRefusedError, ConnectionResetError)):
            return 50  # Severe connection issues
        elif isinstance(error, websocket.WebSocketTimeoutException):
            return 20  # Timeout issues
        elif isinstance(error, websocket.WebSocketConnectionClosedException):
            return 30  # Connection closed
        elif "authentication" in str(error).lower():
            return 80  # Authentication issues
        else:
            return 10  # Generic errors

    def _on_close(self, ws, close_status_code, close_reason) -> None:
        """Handle WebSocket close event with intelligent reconnection.

        Args:
            ws: WebSocket instance
            close_status_code: Close status code
            close_reason: Close reason
        """
        self.logger.info(f"WebSocket closed: {close_status_code} - {close_reason}")
        
        self.is_connected = False
        
        # Update connection quality
        with self.stats_lock:
            self.stats["connection_quality"] = max(
                0, self.stats["connection_quality"] - 30
            )
        
        # Trigger callbacks
        self._trigger_callbacks(
            "disconnect", 
            {"code": close_status_code, "reason": close_reason}
        )
        
        # Check for clean close (1000) vs error close
        if close_status_code not in [1000, None]:
            self.logger.warning(f"Abnormal close: {close_status_code} - {close_reason}")
            self._schedule_reconnect()
        else:
            self.logger.info("Clean WebSocket close")
            
            # Check if we need to reconnect
            if self.is_running:
                self._schedule_reconnect()

    def _authenticate(self) -> bool:
        """Authenticate with Zerodha API.

        Returns:
            bool: True if authentication successful, False otherwise
        """
        self.logger.info("Authenticating with Zerodha API")
        
        if not self.api_key or not self.api_secret and not self.access_token:
            self.logger.error("Missing API credentials")
            return False
        
        try:
            # If we already have an access token, use it
            if self.access_token:
                auth_data = {
                    "type": "auth",
                    "api_key": self.api_key,
                    "access_token": self.access_token,
                }
                return self._send_message(auth_data)
            
            # Otherwise, generate an access token
            # Zerodha requires a session token which we need to get via REST API
            auth_url = "https://kite.zerodha.com/api/login"
            session = requests.Session()
            
            # Step 1: Login to get request token
            login_data = {
                "user_id": self.api_key,
                "password": self.api_secret,  # In practice, this would be user password
            }
            response = session.post(auth_url, data=login_data)
            
            if response.status_code != 200:
                self.logger.error(f"Authentication failed: {response.text}")
                return False
            
            request_token = response.json().get("data", {}).get("request_token")
            
            if not request_token:
                self.logger.error("Failed to get request token")
                return False
            
            # Step 2: Get access token using request token
            token_url = "https://kite.zerodha.com/api/token"
            token_data = {
                "api_key": self.api_key,
                "request_token": request_token,
            }
            
            response = session.post(token_url, data=token_data)
            
            if response.status_code != 200:
                self.logger.error(f"Token generation failed: {response.text}")
                return False
            
            # Save the access token
            self.access_token = response.json().get("data", {}).get("access_token")
            
            if not self.access_token:
                self.logger.error("Failed to get access token")
                return False
            
            # Step 3: Authenticate WebSocket with access token
            auth_data = {
                "type": "auth",
                "api_key": self.api_key,
                "access_token": self.access_token,
            }
            
            return self._send_message(auth_data)
            
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            with self.stats_lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = {
                    "time": datetime.now(),
                    "message": str(e),
                    "type": "authentication",
                }
            return False

    def _schedule_reconnect(self, immediate: bool = False) -> None:
        """Schedule reconnection with exponential backoff.

        Args:
            immediate: Whether to reconnect immediately
        """
        if immediate:
            self.logger.info("Scheduling immediate reconnection")
            delay = 0
        else:
            # Calculate delay with exponential backoff if enabled
            if self.adaptive_reconnect:
                delay = min(
                    60,  # Cap at 60 seconds
                    self.reconnect_delay * (2 ** min(self.reconnect_count, 5))
                )
            else:
                delay = self.reconnect_delay
                
            self.logger.info(f"Scheduling reconnection in {delay} seconds (attempt {self.reconnect_count + 1})")
        
        # Ensure we're not already reconnecting
        with self.thread_lock:
            if self.threads["reconnect"] and self.threads["reconnect"].is_alive():
                self.logger.debug("Reconnection already in progress")
                return
                
            self.threads["reconnect"] = threading.Thread(
                target=self._attempt_reconnect,
                args=(delay,),
                name="ZerodhaFeedReconnect",
                daemon=True
            )
            self.threads["reconnect"].start()

    def _attempt_reconnect(self, delay: int) -> None:
        """Attempt to reconnect with intelligent retry logic.

        Args:
            delay: Delay in seconds before reconnection
        """
        # Wait for the specified delay
        time.sleep(delay)
        
        self.logger.info(f"Attempting reconnection (attempt {self.reconnect_count + 1}/{self.reconnect_attempts})")
        
        # Update statistics
        with self.stats_lock:
            self.stats["reconnects"] += 1
        
        # Increment reconnect counter
        self.reconnect_count += 1
        
        # Check if we've reached the maximum number of attempts
        if self.reconnect_count > self.reconnect_attempts:
            self.logger.error("Maximum reconnection attempts reached")
            self._trigger_callbacks("reconnect", {"success": False, "max_attempts_reached": True})
            return
        
        # Clean up existing connection
        self.disconnect(reconnecting=True)
        
        # Attempt to reconnect
        if self.connect():
            self.logger.info("Reconnection successful")
            self._trigger_callbacks("reconnect", {"success": True})
        else:
            self.logger.warning("Reconnection failed")
            self._trigger_callbacks("reconnect", {"success": False})
            self._schedule_reconnect()  # Schedule another reconnection attempt

    def _start_heartbeat_monitor(self) -> None:
        """Start the heartbeat monitoring thread.
        """
        with self.thread_lock:
            if self.threads["heartbeat"] and self.threads["heartbeat"].is_alive():
                self.logger.debug("Heartbeat monitor already running")
                return
                
            self.threads["heartbeat"] = threading.Thread(
                target=self._heartbeat_monitor,
                name="ZerodhaFeedHeartbeat",
                daemon=True
            )
            self.threads["heartbeat"].start()

    def _heartbeat_monitor(self) -> None:
        """Monitor heartbeats with adaptive interval adjustment.
        """
        self.logger.debug("Starting heartbeat monitor")
        
        self.last_heartbeat = time.time()
        
        while self.is_connected and self.is_running:
            current_time = time.time()
            time_since_last_heartbeat = current_time - self.last_heartbeat
            
            # Check if we've missed heartbeats
            if time_since_last_heartbeat > self.heartbeat_interval * 2:
                self.logger.warning(f"Missed heartbeat: {time_since_last_heartbeat:.1f}s since last heartbeat")
                
                # Adjust connection quality
                with self.stats_lock:
                    self.stats["connection_quality"] = max(
                        0, self.stats["connection_quality"] - 10
                    )
                
                # If we've missed too many heartbeats, try to reconnect
                if time_since_last_heartbeat > self.heartbeat_interval * 3:
                    self.logger.error("Too many missed heartbeats, reconnecting")
                    self.is_connected = False
                    self._schedule_reconnect()
                    break
                
                # Otherwise, send a ping to check connection
                self._send_ping()
            
            # Send heartbeat request if needed
            if time_since_last_heartbeat > self.heartbeat_interval:
                self._send_heartbeat_request()
            
            # Adaptive sleep based on connection quality
            with self.stats_lock:
                connection_quality = self.stats["connection_quality"]
                
            if connection_quality < 50:
                # More frequent checks for poor connections
                sleep_time = max(1, self.heartbeat_interval / 4)
            else:
                sleep_time = max(1, self.heartbeat_interval / 2)
                
            time.sleep(sleep_time)

    def _send_heartbeat_request(self) -> bool:
        """Send a heartbeat request to the server to keep the connection alive.
        
        Returns:
            bool: True if the heartbeat was sent successfully, False otherwise
        """
        try:
            heartbeat_message = {
                "a": "heartbeat",
                "v": [self.session_id, int(time.time())],
            }
            
            success = self._send_message(heartbeat_message)
            if success:
                self.stats["last_heartbeat_sent"] = datetime.now()
                logging.debug(f"Heartbeat sent successfully at {self.stats['last_heartbeat_sent']}")
                return True
            else:
                logging.warning("Failed to send heartbeat")
                return False
        except Exception as e:
            logging.error(f"Error sending heartbeat: {str(e)}")
            self.stats["errors"] += 1
            self.stats["last_error"] = {"time": datetime.now(), "message": f"Heartbeat error: {str(e)}"}
            return False

    def _process_heartbeat(self, data: Dict) -> None:
        """Process a heartbeat response from the server.
        
        Args:
            data (Dict): The heartbeat response data
        """
        current_time = datetime.now()
        self.last_heartbeat = current_time
        self.stats["last_heartbeat"] = current_time
        
        # Calculate latency
        if "timestamp" in data:
            server_time = datetime.fromtimestamp(data.get("timestamp", 0))
            latency = (current_time - server_time).total_seconds() * 1000  # in milliseconds
            self.stats["current_latency"] = latency
            
            # Update latency statistics
            if "min_latency" not in self.stats or latency < self.stats["min_latency"]:
                self.stats["min_latency"] = latency
            if "max_latency" not in self.stats or latency > self.stats["max_latency"]:
                self.stats["max_latency"] = latency
                
            # Exponential moving average for latency
            if "avg_latency" not in self.stats:
                self.stats["avg_latency"] = latency
            else:
                alpha = 0.2  # Weight for new value
                self.stats["avg_latency"] = (1 - alpha) * self.stats["avg_latency"] + alpha * latency
        
        logging.debug(f"Heartbeat received at {current_time}")
    
    def _heartbeat_monitor(self) -> None:
        """Monitor the connection health using heartbeats and trigger reconnection if needed."""
        logging.info("Starting heartbeat monitor thread")
        while self.is_running:
            try:
                current_time = datetime.now()
                
                # Send heartbeat if connected and it's time
                if self.is_connected:
                    # If no heartbeat has been received for the threshold
                    if self.last_heartbeat is not None:
                        time_since_last_heartbeat = (current_time - self.last_heartbeat).total_seconds()
                        
                        if time_since_last_heartbeat > self.heartbeat_interval * 1.5:  # 50% more than interval
                            logging.warning(f"No heartbeat received for {time_since_last_heartbeat} seconds")
                            self._send_heartbeat_request()
                            
                            # If still no response after a longer period, try to reconnect
                            if time_since_last_heartbeat > self.heartbeat_interval * 3:
                                logging.error("Connection seems dead. Initiating reconnection...")
                                self._schedule_reconnect()
                                break
                    else:
                        # If we've never received a heartbeat, send one
                        self._send_heartbeat_request()
                
                # Sleep until next check
                time.sleep(min(self.heartbeat_interval / 2, 15))  # Check at least every 15 seconds
                
            except Exception as e:
                logging.error(f"Error in heartbeat monitor: {str(e)}")
                # Don't break the loop on error, keep monitoring
                time.sleep(5)  # Short delay before next attempt on error
    
    def _process_market_data(self, data: Dict) -> None:
        """Process market data event received from WebSocket.
        
        Parses the incoming market data and converts it to a standardized format,
        then triggers the callback mechanism.
        
        Args:
            data (Dict): Raw market data from the Zerodha WebSocket
        """
        try:
            # Current time for performance metrics
            process_start = time.time()
            
            # Extract the exchange and symbol
            exchange = data.get("exchange", "")
            symbol = data.get("tradingsymbol", "")
            instrument_token = data.get("instrument_token", 0)
            full_symbol = f"{exchange}:{symbol}"
            
            # Cache the data for this symbol
            with self.subscription_lock:
                if full_symbol in self.subscriptions:
                    self.subscriptions[full_symbol]["last_data"] = data
                    self.subscriptions[full_symbol]["last_update"] = datetime.now()
            
            # Find corresponding instrument
            instrument = None
            for instr in self.instruments:
                if instr.symbol == symbol or instr.token == str(instrument_token):
                    instrument = instr
                    break
            
            if not instrument:
                logging.warning(f"Received data for unknown instrument: {full_symbol}")
                return
            
            # Create standard event based on type of data
            event_type = data.get("type", "")
            market_data_type = None
            
            if event_type in ("tick", "quote"):
                market_data_type = MarketDataType.QUOTE
            elif event_type == "ohlc":
                market_data_type = MarketDataType.OHLC
            elif event_type == "depth":
                market_data_type = MarketDataType.MARKET_DEPTH
            else:
                market_data_type = MarketDataType.RAW
            
            # Build standard event
            event = MarketDataEvent(
                instrument=instrument,
                type=EventType.MARKET_DATA,
                market_data_type=market_data_type,
                timestamp=datetime.fromtimestamp(data.get("timestamp", time.time())),
                data=data
            )
            
            # Call the callback
            if self.callback:
                self.callback(event)
            
            # Notify all registered users for this symbol
            with self.subscription_lock:
                if full_symbol in self.subscriptions:
                    user_ids = self.subscriptions[full_symbol].get("users", [])
                    for user_id in user_ids:
                        self._send_to_user(user_id, data)
            
            # Update stats
            self.stats["messages_received"] += 1
            process_time = (time.time() - process_start) * 1000  # ms
            
            # Track processing time statistics
            if "processing_times" not in self.stats:
                self.stats["processing_times"] = []
            
            # Keep only the last 1000 processing times
            self.stats["processing_times"].append(process_time)
            if len(self.stats["processing_times"]) > 1000:
                self.stats["processing_times"].pop(0)
            
            # Update min/max/avg processing time
            if "min_processing_time" not in self.stats or process_time < self.stats["min_processing_time"]:
                self.stats["min_processing_time"] = process_time
            if "max_processing_time" not in self.stats or process_time > self.stats["max_processing_time"]:
                self.stats["max_processing_time"] = process_time
            
            # Exponential moving average for processing time
            if "avg_processing_time" not in self.stats:
                self.stats["avg_processing_time"] = process_time
            else:
                alpha = 0.1  # Weight for new value
                self.stats["avg_processing_time"] = (1 - alpha) * self.stats["avg_processing_time"] + alpha * process_time
            
        except Exception as e:
            logging.error(f"Error processing market data: {str(e)}")
            self.stats["errors"] += 1
            self.stats["last_error"] = {"time": datetime.now(), "message": f"Data processing error: {str(e)}"}
    
    def _process_error(self, data: Dict) -> None:
        """Process error events received from WebSocket.
        
        Args:
            data (Dict): Error data received from the WebSocket
        """
        error_code = data.get("code", 0)
        error_message = data.get("message", "Unknown error")
        
        logging.error(f"Zerodha API error: {error_code} - {error_message}")
        
        # Update error statistics
        self.stats["errors"] += 1
        self.stats["last_error"] = {
            "time": datetime.now(),
            "code": error_code,
            "message": error_message
        }
        
        # Handle specific error codes
        if error_code in (403, 401, 500):  # Authentication or server errors
            if error_code in (401, 403):  # Authentication errors
                logging.warning("Authentication error, attempting to re-authenticate")
                self._authenticate()
            elif error_code == 500:  # Server error
                logging.warning("Server error, scheduling reconnection")
                self._schedule_reconnect()
        
        # Trigger error callbacks
        for callback in self.on_error_callbacks:
            try:
                callback(data)
            except Exception as e:
                logging.error(f"Error in error callback: {str(e)}")
    
    def _process_auth_response(self, data: Dict) -> None:
        """Process authentication response.
        
        Args:
            data (Dict): Authentication response data
        """
        if data.get("status") == "success":
            logging.info("Authentication successful")
            self.is_authenticated = True
            
            # Store the session token if provided
            if "data" in data and "access_token" in data["data"]:
                self.auth_token = data["data"]["access_token"]
            
            # After successful authentication, subscribe to instruments
            self._resubscribe_all()
            
            # Reset reconnect count on successful auth
            self.reconnect_count = 0
        else:
            error_message = data.get("message", "Unknown authentication error")
            logging.error(f"Authentication failed: {error_message}")
            self.is_authenticated = False
            
            # Update error stats
            self.stats["errors"] += 1
            self.stats["last_error"] = {
                "time": datetime.now(),
                "message": f"Authentication error: {error_message}"
            }
            
            # Schedule reconnection with exponential backoff
            retry_delay = min(30, self.reconnect_delay * (2 ** min(self.reconnect_count, 5)))
            self._schedule_reconnect(retry_delay)
    
    def _send_message(self, data: Dict) -> bool:
        """Send a message through the WebSocket connection.
        
        Args:
            data (Dict): The message to send
        
        Returns:
            bool: True if the message was sent successfully, False otherwise
        """
        if not self.is_connected or not self.ws:
            logging.warning("Cannot send message: WebSocket not connected")
            return False
        
        try:
            with self.ws_lock:
                message = json.dumps(data)
                self.ws.send(message)
                self.stats["messages_sent"] += 1
                return True
        except Exception as e:
            logging.error(f"Error sending message: {str(e)}")
            self.stats["errors"] += 1
            self.stats["last_error"] = {"time": datetime.now(), "message": f"Send error: {str(e)}"}
            return False
    
    def _send_to_user(self, user_id: str, data: Dict) -> bool:
        """Send data to a specific user if they have a dedicated WebSocket.
        
        For multi-client architecture where each user might have their own
        WebSocket connection.
        
        Args:
            user_id (str): User identifier
            data (Dict): Data to send
            
        Returns:
            bool: True if the message was sent successfully, False otherwise
        """
        with self.user_lock:
            if user_id in self.user_sessions:
                user_session = self.user_sessions[user_id]
                if user_session.get("status") == "connected" and user_session.get("ws"):
                    try:
                        message = json.dumps(data)
                        user_session["ws"].send(message)
                        # Update user's last activity timestamp
                        self._update_user_activity(user_id)
                        return True
                    except Exception as e:
                        logging.error(f"Error sending message to user {user_id}: {str(e)}")
                        return False
        return False
    
    def _resubscribe_all(self) -> None:
        """Resubscribe to all previously subscribed instruments.
        
        Used after reconnection or authentication events.
        """
        if not self.is_authenticated:
            logging.warning("Cannot resubscribe: Not authenticated")
            return
        
        with self.subscription_lock:
            symbols = list(self.subscriptions.keys())
        
        if not symbols:
            logging.info("No symbols to resubscribe")
            return
        
        # Group symbols by subscription mode (quote, full, etc.)
        mode_groups = {
            "mode_quote": [],
            "mode_full": [],
            "mode_ltp": []
        }
        
        for symbol in symbols:
            with self.subscription_lock:
                sub_info = self.subscriptions.get(symbol, {})
                mode = sub_info.get("mode", "mode_quote")  # Default to quote mode
                if mode in mode_groups:
                    mode_groups[mode].append(symbol)
        
        # Subscribe in batches for each mode
        for mode, mode_symbols in mode_groups.items():
            if not mode_symbols:
                continue
                
            # Batch size based on mode (full data needs smaller batches)
            batch_size = 50 if mode == "mode_full" else 200
            
            for i in range(0, len(mode_symbols), batch_size):
                batch = mode_symbols[i:i+batch_size]
                self._subscribe_batch(batch, mode)
                time.sleep(0.1)  # Small delay between batches
        
        logging.info(f"Resubscribed to {len(symbols)} symbols")
    
    def _subscribe_batch(self, symbols: List[str], mode: str = "mode_quote") -> bool:
        """Subscribe to a batch of symbols with specified mode.
        
        Args:
            symbols (List[str]): List of symbols to subscribe
            mode (str): Subscription mode (mode_quote, mode_full, mode_ltp)
        
        Returns:
            bool: True if the subscription was successful, False otherwise
        """
        if not symbols:
            return True
            
        if not self.is_authenticated:
            logging.warning("Cannot subscribe: Not authenticated")
            return False
        
        # Extract instrument tokens from symbols
        tokens = []
        for symbol in symbols:
            # Try to find the instrument token
            for instrument in self.instruments:
                if instrument.symbol == symbol or f"{instrument.exchange}:{instrument.symbol}" == symbol:
                    tokens.append(int(instrument.token))
                    break
        
        if not tokens:
            logging.warning(f"No valid tokens found for symbols: {symbols}")
            return False
        
        # Prepare subscription message
        subscribe_msg = {
            "a": "subscribe",
            "v": tokens
        }
        
        # Add mode if specified
        if mode and mode != "mode_quote":  # Quote is default
            subscribe_msg["m"] = mode
        
        # Send the subscription request
        success = self._send_message(subscribe_msg)
        
        if success:
            logging.info(f"Subscribed to {len(tokens)} symbols in {mode} mode")
            # Update subscription info
            with self.subscription_lock:
                for symbol in symbols:
                    if symbol in self.subscriptions:
                        self.subscriptions[symbol]["mode"] = mode
                        self.subscriptions[symbol]["subscribed"] = True
                        self.subscriptions[symbol]["last_subscribed"] = datetime.now()
        else:
            logging.error(f"Failed to subscribe to {len(tokens)} symbols")
        
        return success
    
    def start(self) -> bool:
        """Start the WebSocket feed.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.is_running:
            logging.warning("Feed is already running")
            return True
        
        logging.info("Starting Zerodha feed...")
        self.is_running = True
        self.stats["start_time"] = datetime.now()
        
        # Start the WebSocket connection
        success = self.connect()
        if not success:
            logging.error("Failed to establish initial connection")
            self.is_running = False
            return False
        
        # Start the heartbeat monitor thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        self.heartbeat_thread.start()
        
        logging.info("Zerodha feed started successfully")
        return True
    
    def stop(self) -> None:
        """Stop the WebSocket feed and clean up resources."""
        if not self.is_running:
            logging.warning("Feed is not running")
            return
        
        logging.info("Stopping Zerodha feed...")
        self.is_running = False
        
        # Close the WebSocket connection
        self.disconnect()
        
        # Wait for threads to complete
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            try:
                self.heartbeat_thread.join(timeout=2)
            except:
                pass
        
        if self.reconnect_thread and self.reconnect_thread.is_alive():
            try:
                self.reconnect_thread.join(timeout=2)
            except:
                pass
        
        logging.info("Zerodha feed stopped")
    
    def disconnect(self) -> None:
        """Disconnect the WebSocket connection."""
        with self.ws_lock:
            if self.ws is not None:
                try:
                    self.ws.close()
                except Exception as e:
                    logging.error(f"Error closing WebSocket: {str(e)}")
                finally:
                    self.ws = None
        
        self.is_connected = False
        self.is_authenticated = False
        logging.info("Disconnected from Zerodha WebSocket")
    
    def subscribe(self, instrument: Instrument, symbols: Union[str, List[str]] = None, user_id: Optional[str] = None, mode: str = "mode_quote") -> bool:
        """Subscribe to market data for the specified instrument or symbols.
        
        Args:
            instrument (Instrument): The instrument to subscribe to. Can be None if symbols are provided.
            symbols (Union[str, List[str]], optional): Symbol or list of symbols to subscribe to.
            user_id (str, optional): User identifier for multi-client implementations.
            mode (str, optional): Subscription mode (mode_quote, mode_full, mode_ltp).
        
        Returns:
            bool: True if subscription was successful, False otherwise
        """
        # Validate inputs and prepare symbols list
        if symbols is None and instrument is not None:
            symbols = [instrument.symbol]
        elif isinstance(symbols, str):
            symbols = [symbols]
        
        if not symbols:
            logging.error("No symbols provided for subscription")
            return False
        
        # Update user activity if provided
        if user_id:
            self._update_user_activity(user_id, symbols)
        
        # Process each symbol
        success = True
        new_symbols = []
        
        with self.subscription_lock:
            for symbol in symbols:
                # If this is a new subscription
                if symbol not in self.subscriptions:
                    self.subscriptions[symbol] = {
                        "users": [user_id] if user_id else [],
                        "last_data": None,
                        "last_update": None,
                        "mode": mode,
                        "subscribed": False,
                        "last_subscribed": None
                    }
                    new_symbols.append(symbol)
                else:
                    # If this symbol is already subscribed by this user, just update the mode if needed
                    if user_id and user_id not in self.subscriptions[symbol]["users"]:
                        self.subscriptions[symbol]["users"].append(user_id)
                    
                    # Update mode if different and higher priority
                    # Priority: full > quote > ltp
                    current_mode = self.subscriptions[symbol].get("mode", "mode_quote")
                    if self._mode_priority(mode) > self._mode_priority(current_mode):
                        self.subscriptions[symbol]["mode"] = mode
                        # If already subscribed, we need to resubscribe with the new mode
                        if self.subscriptions[symbol].get("subscribed", False):
                            new_symbols.append(symbol)
        
        # If there are new symbols to subscribe
        if new_symbols:
            # Batch subscribe for better performance
            batch_success, _ = self.batch_subscribe(new_symbols, mode=mode, batch_size=200)
            success = success and batch_success
        
        return success
    
    def _mode_priority(self, mode: str) -> int:
        """Get the priority of a subscription mode.
        
        Higher number means higher priority.
        
        Args:
            mode (str): Subscription mode
            
        Returns:
            int: Priority level
        """
        mode_priorities = {
            "mode_ltp": 1,
            "mode_quote": 2,
            "mode_full": 3
        }
        return mode_priorities.get(mode, 0)
    
    def unsubscribe(self, symbols: Union[str, List[str]], user_id: Optional[str] = None) -> bool:
        """Unsubscribe from market data for the specified symbols.
        
        Args:
            symbols (Union[str, List[str]]): Symbol or list of symbols to unsubscribe from.
            user_id (str, optional): User identifier for multi-client implementations.
        
        Returns:
            bool: True if unsubscription was successful, False otherwise
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        if not symbols:
            logging.error("No symbols provided for unsubscription")
            return False
        
        to_unsubscribe = []
        
        with self.subscription_lock:
            for symbol in symbols:
                if symbol in self.subscriptions:
                    if user_id:
                        # If a user_id is provided, only remove this user from subscribers
                        if user_id in self.subscriptions[symbol]["users"]:
                            self.subscriptions[symbol]["users"].remove(user_id)
                        
                        # If no users left, unsubscribe
                        if not self.subscriptions[symbol]["users"]:
                            to_unsubscribe.append(symbol)
                    else:
                        # If no user_id, unsubscribe completely
                        to_unsubscribe.append(symbol)
        
        if not to_unsubscribe:
            return True  # Nothing to unsubscribe
        
        # Extract instrument tokens
        tokens = []
        for symbol in to_unsubscribe:
            for instrument in self.instruments:
                if instrument.symbol == symbol or f"{instrument.exchange}:{instrument.symbol}" == symbol:
                    tokens.append(int(instrument.token))
                    break
        
        success = True
        if tokens:
            # Send unsubscribe message
            unsubscribe_msg = {
                "a": "unsubscribe",
                "v": tokens
            }
            
            success = self._send_message(unsubscribe_msg)
            
            if success:
                logging.info(f"Unsubscribed from {len(tokens)} symbols")
                # Update subscription info
                with self.subscription_lock:
                    for symbol in to_unsubscribe:
                        if symbol in self.subscriptions:
                            if user_id is None or not self.subscriptions[symbol]["users"]:
                                del self.subscriptions[symbol]
                            else:
                                self.subscriptions[symbol]["subscribed"] = False
            else:
                logging.error(f"Failed to unsubscribe from {len(tokens)} symbols")
        
        return success
    
    def register_user(self, user_id: str, user_data: Optional[Dict] = None) -> bool:
        """Register a new user for multi-client implementations.
        
        Args:
            user_id (str): Unique user identifier
            user_data (Dict, optional): Additional user data
            
        Returns:
            bool: True if registration was successful, False otherwise
        """
        with self.user_lock:
            if user_id in self.active_users:
                logging.warning(f"User {user_id} already registered")
                # Update user data if provided
                if user_data:
                    self.active_users[user_id].update(user_data)
                return True
            
            self.active_users[user_id] = {
                "last_activity": datetime.now(),
                "subscriptions": [],
                "data": user_data or {},
                "created": datetime.now()
            }
            
            self.user_sessions[user_id] = {
                "session_id": str(uuid.uuid4()),
                "status": "initialized",
                "ws": None,
                "last_connected": None
            }
            
            logging.info(f"User {user_id} registered successfully")
            return True
    
    def logout_user(self, user_id: str) -> bool:
        """Logout and remove a user.
        
        Args:
            user_id (str): User identifier
        
        Returns:
            bool: True if logout was successful, False otherwise
        """
        if not user_id:
            return False
        
        # Unsubscribe user from all subscriptions
        with self.subscription_lock:
            for symbol, sub_info in list(self.subscriptions.items()):
                if user_id in sub_info["users"]:
                    sub_info["users"].remove(user_id)
                    # Remove subscription if no users left
                    if not sub_info["users"]:
                        self.unsubscribe(symbol)
        
        # Close user's WebSocket if any
        with self.user_lock:
            if user_id in self.user_sessions:
                session = self.user_sessions[user_id]
                if session.get("ws"):
                    try:
                        session["ws"].close()
                    except:
                        pass
                
                del self.user_sessions[user_id]
            
            if user_id in self.active_users:
                del self.active_users[user_id]
        
        logging.info(f"User {user_id} logged out successfully")
        return True
    
    def _update_user_activity(self, user_id: str, symbols: Optional[List[str]] = None) -> None:
        """Update user's last activity timestamp and subscriptions.
        
        Args:
            user_id (str): User identifier
            symbols (List[str], optional): List of symbols the user is subscribing to
        """
        if not user_id:
            return
            
        with self.user_lock:
            if user_id in self.active_users:
                self.active_users[user_id]["last_activity"] = datetime.now()
                
                if symbols:
                    current_subs = self.active_users[user_id].get("subscriptions", [])
                    for symbol in symbols:
                        if symbol not in current_subs:
                            current_subs.append(symbol)
                    self.active_users[user_id]["subscriptions"] = current_subs
    
    def get_last_data(self, symbol: str) -> Optional[Dict]:
        """Get the last data received for a symbol.
        
        Implements caching mechanism for performance optimization.
        
        Args:
            symbol (str): The symbol to get data for
            
        Returns:
            Optional[Dict]: The last data received or None if not available
        """
        with self.subscription_lock:
            sub_info = self.subscriptions.get(symbol)
            if sub_info:
                return sub_info.get("last_data")
        
        return None
    
    def get_statistics(self) -> Dict:
        """Get feed statistics and performance metrics.
        
        Returns:
            Dict: Feed statistics
        """
        stats = self.stats.copy()
        
        # Add current state
        stats["is_connected"] = self.is_connected
        stats["is_authenticated"] = self.is_authenticated
        stats["subscriptions_count"] = len(self.subscriptions)
        stats["active_users_count"] = len(self.active_users)
        
        # Calculate uptime
        if stats.get("start_time"):
            uptime_seconds = (datetime.now() - stats["start_time"]).total_seconds()
            stats["uptime_seconds"] = uptime_seconds
            stats["uptime_formatted"] = self._format_uptime(uptime_seconds)
        
        # Calculate message rate (per second)
        if "start_time" in stats and stats["messages_received"] > 0:
            elapsed = max(1, (datetime.now() - stats["start_time"]).total_seconds())
            stats["messages_per_second"] = stats["messages_received"] / elapsed
        
        return stats
    
    def _format_uptime(self, seconds: float) -> str:
        """Format uptime seconds to human-readable format.
        
        Args:
            seconds (float): Uptime in seconds
            
        Returns:
            str: Formatted uptime string
        """
        days, remainder = divmod(int(seconds), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m {seconds}s"
        elif hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def register_callback(self, event_type: str, callback) -> bool:
        """Register a callback for a specific event type.
        
        Args:
            event_type (str): Event type (connect, disconnect, error, message, reconnect)
            callback: Callback function
            
        Returns:
            bool: True if registration was successful, False otherwise
        """
        if not callable(callback):
            logging.error("Callback must be callable")
            return False
        
        callback_map = {
            "connect": self.on_connect_callbacks,
            "disconnect": self.on_disconnect_callbacks,
            "error": self.on_error_callbacks,
            "message": self.on_message_callbacks,
            "reconnect": self.on_reconnect_callbacks
        }
        
        if event_type in callback_map:
            callback_list = callback_map[event_type]
            if callback not in callback_list:
                callback_list.append(callback)
                return True
        else:
            logging.error(f"Unknown event type: {event_type}")
        
        return False
    
    def unregister_callback(self, event_type: str, callback) -> bool:
        """Unregister a callback for a specific event type.
        
        Args:
            event_type: Event type ('connect', 'disconnect', 'message', 'error', 'reconnect')
            callback: Callback function to unregister
            
        Returns:
            bool: True if callback was successfully unregistered, False otherwise
        """
        if not callable(callback):
            logging.error("Cannot unregister non-callable object as callback")
            return False
            
        if event_type == 'connect':
            if callback in self.on_connect_callbacks:
                self.on_connect_callbacks.remove(callback)
                logging.debug(f"Unregistered connect callback: {callback.__name__}")
                return True
        elif event_type == 'disconnect':
            if callback in self.on_disconnect_callbacks:
                self.on_disconnect_callbacks.remove(callback)
                logging.debug(f"Unregistered disconnect callback: {callback.__name__}")
                return True
        elif event_type == 'message':
            if callback in self.on_message_callbacks:
                self.on_message_callbacks.remove(callback)
                logging.debug(f"Unregistered message callback: {callback.__name__}")
                return True
        elif event_type == 'error':
            if callback in self.on_error_callbacks:
                self.on_error_callbacks.remove(callback)
                logging.debug(f"Unregistered error callback: {callback.__name__}")
                return True
        elif event_type == 'reconnect':
            if callback in self.on_reconnect_callbacks:
                self.on_reconnect_callbacks.remove(callback)
                logging.debug(f"Unregistered reconnect callback: {callback.__name__}")
                return True
        else:
            logging.warning(f"Unknown event type for callback unregistration: {event_type}")
            return False
            
        logging.warning(f"Callback not found for event type: {event_type}")
        return False

    def clean_inactive_users(self, inactive_threshold: int = 3600) -> int:
        """Clean up inactive users who haven't performed any actions for a specified time.
        
        Args:
            inactive_threshold: Threshold in seconds after which a user is considered inactive
            
        Returns:
            int: Number of inactive users removed
        """
        now = time.time()
        removed_count = 0
        
        with self.user_lock:
            inactive_users = []
            for user_id, user_data in self.active_users.items():
                last_activity = user_data.get('last_activity', 0)
                if now - last_activity > inactive_threshold:
                    inactive_users.append(user_id)
                    
            for user_id in inactive_users:
                # Unsubscribe user from all symbols first
                user_subscriptions = self.active_users[user_id].get('subscriptions', [])
                self.unsubscribe(user_subscriptions, user_id)
                
                # Remove user from active users
                del self.active_users[user_id]
                if user_id in self.user_sessions:
                    del self.user_sessions[user_id]
                    
                removed_count += 1
                logging.info(f"Removed inactive user: {user_id}")
                
            # Update statistics
            self.stats['inactive_users_cleaned'] = self.stats.get('inactive_users_cleaned', 0) + removed_count
            
        return removed_count
        
    def get_user_info(self, user_id: str) -> Optional[Dict]:
        """Get detailed information about a specific user.
        
        Args:
            user_id: User ID to retrieve information for
            
        Returns:
            Optional[Dict]: User information or None if user not found
        """
        with self.user_lock:
            if user_id not in self.active_users:
                return None
                
            user_data = self.active_users[user_id].copy()
            session_data = self.user_sessions.get(user_id, {})
            
            # Combine the information
            result = {
                'user_id': user_id,
                'last_activity': user_data.get('last_activity', 0),
                'last_activity_time': datetime.fromtimestamp(user_data.get('last_activity', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'subscriptions': user_data.get('subscriptions', []),
                'subscription_count': len(user_data.get('subscriptions', [])),
                'session_id': session_data.get('session_id'),
                'status': session_data.get('status'),
                'connected_since': session_data.get('connected_since'),
                'is_active': (time.time() - user_data.get('last_activity', 0)) < 3600,
            }
            
            return result
    
    def get_subscription_info(self, symbol: str) -> Optional[Dict]:
        """Get detailed information about a specific symbol subscription.
        
        Args:
            symbol: Symbol to retrieve subscription information for
            
        Returns:
            Optional[Dict]: Subscription information or None if symbol not found
        """
        with self.subscription_lock:
            if symbol not in self.subscriptions:
                return None
                
            subscription_data = self.subscriptions[symbol].copy()
            
            # Add calculated fields
            result = {
                'symbol': symbol,
                'subscribers': subscription_data.get('users', []),
                'subscriber_count': len(subscription_data.get('users', [])),
                'last_updated': subscription_data.get('last_updated'),
                'last_price': subscription_data.get('last_data', {}).get('ltp'),
                'last_tick_time': subscription_data.get('last_tick_time'),
                'tick_count': subscription_data.get('tick_count', 0),
                'is_active': bool(subscription_data.get('users', [])),
            }
            
            # Add last data if available
            if 'last_data' in subscription_data:
                result['last_data'] = subscription_data['last_data']
                
            return result
    
    def reset(self) -> bool:
        """Reset the feed handler to its initial state without disconnecting.
        
        Returns:
            bool: True if reset was successful, False otherwise
        """
        try:
            with self.subscription_lock:
                self.subscriptions = {}
                
            with self.user_lock:
                self.active_users = {}
                self.user_sessions = {}
                
            # Reset statistics
            self.stats = {
                "messages_received": 0,
                "messages_sent": 0,
                "errors": 0,
                "reconnects": 0,
                "start_time": self.stats.get("start_time"),
                "last_error": None,
                "last_heartbeat": None,
            }
            
            self._resubscribe_all()
            logging.info("Feed handler reset successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error resetting feed handler: {str(e)}")
            self.stats["last_error"] = {"time": time.time(), "message": str(e)}
            self.stats["errors"] += 1
            return False
    
    def batch_subscribe(self, user_id: str, symbols: List[str], batch_size: int = 100) -> Tuple[bool, int]:
        """Subscribe to multiple symbols in batches to avoid overwhelming the server.
        
        Args:
            user_id: User ID requesting the subscription
            symbols: List of symbols to subscribe to
            batch_size: Number of symbols to subscribe to in a single batch
            
        Returns:
            Tuple[bool, int]: (Success status, Number of successfully subscribed symbols)
        """
        if not user_id or not symbols:
            logging.warning("Invalid user_id or symbols for batch subscription")
            return False, 0
            
        successful_subscriptions = 0
        total_batches = math.ceil(len(symbols) / batch_size)
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}/{total_batches} with {len(batch)} symbols")
            
            # Use exponential backoff for retries on batch failures
            max_retries = 3
            retry_delay = 1  # seconds
            
            for retry in range(max_retries):
                try:
                    if self.subscribe(None, batch, user_id):
                        successful_subscriptions += len(batch)
                        break
                    else:
                        if retry < max_retries - 1:
                            retry_delay_with_jitter = retry_delay * (2 ** retry) * (0.5 + random.random())
                            logging.warning(f"Batch subscription failed, retrying in {retry_delay_with_jitter:.2f}s (attempt {retry+1}/{max_retries})")
                            time.sleep(retry_delay_with_jitter)
                        else:
                            logging.error(f"Failed to subscribe to batch after {max_retries} attempts")
                except Exception as e:
                    logging.error(f"Error in batch subscription: {str(e)}")
                    if retry < max_retries - 1:
                        retry_delay_with_jitter = retry_delay * (2 ** retry) * (0.5 + random.random())
                        time.sleep(retry_delay_with_jitter)
                        
            # Add a small delay between batches to avoid rate limiting
            if i + batch_size < len(symbols):
                time.sleep(0.5)
                
        success_ratio = successful_subscriptions / len(symbols) if symbols else 0
        return success_ratio > 0.9, successful_subscriptions
    
    def update_user_data(self, user_id: str, data: Dict) -> bool:
        """Update user data with additional information.
        
        Args:
            user_id: User ID to update
            data: Dictionary of user data to update
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        if not user_id or not data:
            return False
            
        with self.user_lock:
            if user_id not in self.active_users:
                logging.warning(f"Cannot update data for non-existing user: {user_id}")
                return False
                
            # Update user data while preserving critical fields
            current_data = self.active_users[user_id]
            current_data.update(data)
            
            # Ensure last_activity field isn't overwritten
            if 'last_activity' in data:
                current_data['last_activity'] = max(current_data.get('last_activity', 0), data['last_activity'])
                
            # Ensure subscriptions field is maintained correctly
            if 'subscriptions' in data:
                if not isinstance(data['subscriptions'], list):
                    logging.warning("Subscriptions must be a list, ignoring update to this field")
                    current_data['subscriptions'] = current_data.get('subscriptions', [])
                    
            logging.debug(f"Updated user data for {user_id}")
            return True
    
    def get_connection_status(self) -> Dict:
        """Get detailed information about the current connection status.
        
        Returns:
            Dict: Connection status information
        """
        now = time.time()
        uptime = now - self.stats["start_time"] if self.stats.get("start_time") else 0
        
        result = {
            "is_connected": self.is_connected,
            "session_id": self.session_id,
            "uptime": uptime,
            "uptime_formatted": str(timedelta(seconds=int(uptime))),
            "reconnect_count": self.reconnect_count,
            "last_heartbeat": self.last_heartbeat,
            "heartbeat_age": now - self.last_heartbeat if self.last_heartbeat else None,
            "active_users": len(self.active_users),
            "active_subscriptions": len(self.subscriptions),
            "message_rate": self.stats["messages_received"] / uptime if uptime > 0 else 0,
            "error_rate": self.stats["errors"] / uptime if uptime > 0 else 0,
            "websocket_url": self.websocket_url,
        }
        
        # Add connection health indicator
        if not self.is_connected:
            result["health"] = "disconnected"
        elif result.get("heartbeat_age", float("inf")) > self.heartbeat_interval * 2:
            result["health"] = "degraded"
        else:
            result["health"] = "healthy"
            
        return result
    
    def ping_server(self) -> bool:
        """Send a ping request to the server to check connectivity.
        
        Returns:
            bool: True if ping was successful, False otherwise
        """
        if not self.is_connected:
            logging.warning("Cannot ping server: Not connected")
            return False
            
        try:
            ping_payload = {
                "a": "ping",
                "t": time.time()
            }
            
            with self.ws_lock:
                if self.ws:
                    self.ws.send(json.dumps(ping_payload))
                    self.stats["messages_sent"] += 1
                    logging.debug("Ping sent to server")
                    return True
                else:
                    logging.warning("Cannot ping server: WebSocket not initialized")
                    return False
                    
        except Exception as e:
            logging.error(f"Error pinging server: {str(e)}")
            self.stats["last_error"] = {"time": time.time(), "message": str(e)}
            self.stats["errors"] += 1
            return False
    
    def get_server_time(self) -> Optional[datetime]:
        """Get the current server time via API call.
        
        Returns:
            Optional[datetime]: Server time or None if request failed
        """
        try:
            # Use the Kite API for a lightweight request to get server time
            headers = {
                "X-Kite-Version": "3",
                "Authorization": f"token {self.api_key}:{self.auth_token}"
            }
            
            url = "https://api.kite.trade/quote/ltp?i=NSE:NIFTY 50"
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                server_time_str = response.headers.get('Date')
                if server_time_str:
                    # Parse the RFC 7231 formatted date 
                    server_time = datetime.strptime(server_time_str, "%a, %d %b %Y %H:%M:%S %Z")
                    logging.debug(f"Retrieved server time: {server_time}")
                    return server_time
                    
            logging.warning(f"Failed to get server time: {response.status_code}")
            return None
            
        except Exception as e:
            logging.error(f"Error getting server time: {str(e)}")
            return None
    
    def set_debug_mode(self, enabled: bool = True) -> None:
        """Enable or disable debug mode for enhanced logging and diagnostics.
        
        Args:
            enabled: Whether to enable debug mode
        """
        if enabled:
            logging.getLogger().setLevel(logging.DEBUG)
            self.debug_mode = True
            logging.debug("Debug mode enabled for ZerodhaFeed")
        else:
            logging.getLogger().setLevel(logging.INFO)
            self.debug_mode = False
            logging.info("Debug mode disabled for ZerodhaFeed")
    
    def apply_rate_limiting(self, max_requests: int = 3, time_window: int = 1) -> None:
        """Configure rate limiting for outgoing requests to avoid API throttling.
        
        Args:
            max_requests: Maximum number of requests allowed in the time window
            time_window: Time window in seconds
        """
        self.rate_limit = {
            "max_requests": max_requests,
            "time_window": time_window,
            "request_timestamps": []
        }
        logging.info(f"Rate limiting configured: {max_requests} requests per {time_window}s")
    
    def _check_rate_limit(self) -> bool:
        """Check if the current request would exceed the rate limit.
        
        Returns:
            bool: True if request is allowed, False if rate limit would be exceeded
        """
        if not hasattr(self, 'rate_limit') or not self.rate_limit:
            return True
            
        now = time.time()
        window_start = now - self.rate_limit["time_window"]
        
        # Clean up old timestamps
        self.rate_limit["request_timestamps"] = [
            t for t in self.rate_limit["request_timestamps"] if t >= window_start
        ]
        
        # Check if we're over the limit
        if len(self.rate_limit["request_timestamps"]) >= self.rate_limit["max_requests"]:
            return False
            
        # Add the new timestamp
        self.rate_limit["request_timestamps"].append(now)
        return True
    
    def implement_circuit_breaker(self, error_threshold: int = 5, reset_time: int = 300) -> None:
        """Implement a circuit breaker pattern to prevent cascading failures.
        
        Args:
            error_threshold: Number of errors before tripping the circuit breaker
            reset_time: Time in seconds before the circuit breaker resets
        """
        self.circuit_breaker = {
            "error_threshold": error_threshold,
            "reset_time": reset_time,
            "error_count": 0,
            "tripped": False,
            "trip_time": None
        }
        logging.info(f"Circuit breaker configured: {error_threshold} errors, {reset_time}s reset time")
    
    def _check_circuit_breaker(self) -> bool:
        """Check if the circuit breaker is tripped.
        
        Returns:
            bool: True if circuit is closed (requests allowed), False if circuit is open
        """
        if not hasattr(self, 'circuit_breaker') or not self.circuit_breaker:
            return True
            
        now = time.time()
        
        # Check if the circuit breaker is tripped and if it's time to reset
        if self.circuit_breaker["tripped"]:
            if now - self.circuit_breaker["trip_time"] > self.circuit_breaker["reset_time"]:
                logging.info("Circuit breaker reset after cooling period")
                self.circuit_breaker["tripped"] = False
                self.circuit_breaker["error_count"] = 0
                return True
            else:
                return False
                
        return True
        
    def _trip_circuit_breaker(self) -> None:
        """Trip the circuit breaker due to excessive errors."""
        if not hasattr(self, 'circuit_breaker') or not self.circuit_breaker:
            return
            
        self.circuit_breaker["error_count"] += 1
        
        if self.circuit_breaker["error_count"] >= self.circuit_breaker["error_threshold"]:
            self.circuit_breaker["tripped"] = True
            self.circuit_breaker["trip_time"] = time.time()
            logging.warning(f"Circuit breaker tripped after {self.circuit_breaker['error_count']} errors")
    
                    