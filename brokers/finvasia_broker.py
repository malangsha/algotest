import logging
import json
import time
import threading
import hashlib
import queue
import pyotp

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from dateutil import parser
from NorenRestApiPy.NorenApi import NorenApi

from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument, InstrumentType
from models.position import Position
from models.market_data import MarketData, Bar, Quote, Trade
from core.event_manager import EventManager
from .broker_interface import BrokerInterface

class FinvasiaBroker(BrokerInterface):
    """
    Finvasia broker implementation for the trading system.
    Integrates with Finvasia's NorenAPI for order execution and market data.
    """
    # Order type mappings between internal and Finvasia API
    ORDER_TYPE_MAPPING = {
        OrderType.MARKET: "MKT",
        OrderType.LIMIT: "LMT",
        OrderType.STOP: "SL-MKT",
        OrderType.STOP_LIMIT: "SL-LMT"
    }

    # Reverse mapping for order status
    STATUS_MAPPING = {
        "OPEN": OrderStatus.PENDING,
        "COMPLETE": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.REJECTED,
        "PENDING": OrderStatus.PENDING,
        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED
    }

    # Exchange mapping
    EXCHANGE_MAPPING = {
        "NSE": "NSE",
        "BSE": "BSE",
        "NFO": "NFO",  # NSE Futures & Options
        "BFO": "BFO",  # BSE Futures & Options
        "MCX": "MCX",  # Multi Commodity Exchange
        "CDS": "CDS"   # Currency Derivatives
    }

    def __init__(self, broker_name = "finvasia", config = None, api_url: str = None, user_id: str = None, password: str = None,
                 api_key: str = None, imei: str = None, twofa: str = None, vc: str = None, debug: bool = False,
                 enable_autoconnect: bool = True, paper_trading: bool = False, **kwargs):
        """
        Initialize the Finvasia broker.

        Args:
            broker_name: Name of the broker
            config: Configuration dictionary
            api_url: API URL
            user_id: User ID
            password: Password
            api_key: API key
            imei: IMEI
            twofa: 2FA
            vc: VC
            debug: Debug flag
            enable_autoconnect: Whether to connect automatically
            paper_trading: Whether to enable paper trading
            **kwargs: Additional arguments
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.broker_name = broker_name
        self.enable_autoconnect = enable_autoconnect

        # Load broker configuration
        broker_settings = self._load_broker_config(
            config=config,
            user_id=user_id,
            password=password,
            api_key=api_key,
            imei=imei,
            twofa=twofa,
            vc=vc,
            api_url=api_url,
            debug=debug,
            paper_trading=paper_trading
        )

        # Set configuration values
        self.api_url = broker_settings['api_url']
        self.ws_url = broker_settings['ws_url']
        self.debug = broker_settings['debug']
        self.user_id = broker_settings['user_id']
        self.password = broker_settings['password']
        self.api_key = broker_settings['api_key']
        self.imei = broker_settings['imei']
        self.twofa = broker_settings['twofa']
        self.vc = broker_settings['vc']
        self.paper_trading = broker_settings['paper_trading']

        # Connection state
        self._connected = False
        self._session_token = None
        self.token_refresh_timer = None # Keep timers if used for REST session
        self.cache_cleanup_timer = None

        # Instrument cache
        self._instrument_cache = {}
        self._instruments_df = None
        self._last_instrument_update = None
        self._instrument_cache_ttl = 24 * 60 * 60  # 24 hours in seconds

        # API instance
        self.api = NorenApi(
            host=self.api_url,
            websocket=self.ws_url
        )

        # Initialize the event manager
        self.event_manager = None

        # Initialize paper trading attributes
        self.simulated_orders = {}
        self.next_sim_fill_id = 1

        # Connect if enabled
        if self.enable_autoconnect:
            self.connect()

    def _load_broker_config(self, config: Dict[str, Any], user_id: str = None, password: str = None,
                          api_key: str = None, imei: str = None, twofa: str = None, vc: str = None,
                          api_url: str = None, debug: bool = False, paper_trading: bool = False) -> Dict[str, Any]:
        """
        Load broker configuration from the provided config dictionary.

        Args:
            config: Configuration dictionary
            user_id: Optional user ID override
            password: Optional password override
            api_key: Optional API key override
            imei: Optional IMEI override
            twofa: Optional 2FA override
            vc: Optional VC override
            api_url: Optional API URL override
            debug: Optional debug flag override
            paper_trading: Optional paper trading flag override

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
                        user_id = user_id or secrets.get('user_id')
                        password = password or secrets.get('password')
                        api_key = api_key or secrets.get('api_key')
                        imei = imei or secrets.get('imei')
                        twofa = twofa or secrets.get('factor2')
                        vc = vc or secrets.get('vc')
                    except Exception as e:
                        self.logger.error(f"Failed to load secrets from {secrets_file}: {str(e)}")

                # Get API settings
                api_config = active_broker_config.get("api", {})
                api_url = api_url or api_config.get("api_url")
                debug = debug if debug is not None else api_config.get("debug", False)
                paper_trading = paper_trading if paper_trading is not None else api_config.get("paper_trading", False)

        return {
            'api_url': api_url or "https://api.shoonya.com/NorenWClientTP/",
            'ws_url': "wss://api.shoonya.com/NorenWSTP/",
            'debug': debug,
            'user_id': user_id,
            'password': password,
            'api_key': api_key,
            'imei': imei,
            'twofa': twofa,
            'vc': vc,
            'paper_trading': paper_trading
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

    def set_event_manager(self, event_manager: EventManager):
        """Set the event manager for the broker."""
        self.event_manager = event_manager

    def is_connected(self) -> bool:
        """Check if the broker connection is active."""
        return self._connected

    def _generate_device_id(self) -> str:
        """Generate a unique device ID based on machine-specific information."""
        import platform
        import uuid

        system_info = f"{platform.node()}-{platform.system()}-{platform.machine()}"
        mac = uuid.getnode()

        device_id = hashlib.md5(f"{system_info}-{mac}".encode()).hexdigest()
        return device_id

    def connect(self) -> bool:
        """Connect to the Finvasia trading API."""
        if self.is_connected():
            self.logger.info("Already connected to Finvasia API")
            return True

        try:
            # Generate TOTP
            twofa = pyotp.TOTP(self.twofa).now()

            # print all login params
            # self.logger.info(f"userid: {self.user_id}")
            # self.logger.info(f"password: {self.password}")
            # self.logger.info(f"twoFA: {self.twofa}")
            # self.logger.info(f"vendor_code: {self.vc}")
            # self.logger.info(f"api_secret: {self.api_key}")
            # self.logger.info(f"imei: {self.imei}")         

            # Direct login using NorenApi
            response = self.api.login(
                userid=self.user_id,
                password=self.password,
                twoFA=twofa,
                vendor_code=self.vc,
                api_secret=self.api_key,
                imei=self.imei
            )

            if response and 'susertoken' in response:
                self._session_token = response['susertoken']
                self._connected = True
                self.logger.info("Successfully connected to Finvasia API")

                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Connection failed: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error connecting to Finvasia API: {str(e)}")
            self._connected = False
            return False

    def disconnect(self) -> bool:
        """Disconnect from the Finvasia trading API."""
        if not self.is_connected():
            return True

        try:
            # Stop market data processor
            if self._market_data_thread and self._market_data_thread.is_alive():
                self._market_data_buffer.put(None)  # Signal to exit
                self._market_data_thread.join(timeout=2)

            # Clear session data
            self._session_token = None
            self._connected = False
            self._ws_connected = False
            self._subscriptions.clear()
            self._depth_subscriptions.clear()

            self.logger.info("Successfully disconnected from Finvasia API")
            return True

        except Exception as e:
            self.logger.error(f"Error disconnecting from Finvasia API: {str(e)}")
            return False

    def _make_api_request(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request to the Finvasia API with error handling."""
        if not self.is_connected() and endpoint != "login":
            self.logger.error("Not connected to Finvasia API")
            return {"stat": "NOT_OK", "emsg": "Not connected to API"}

        try:
            # Direct call to NorenApi methods based on the endpoint
            if hasattr(self.api, endpoint):
                method = getattr(self.api, endpoint)
                response = method(**data)
                return response
            else:
                self.logger.error(f"Unknown API endpoint: {endpoint}")
                return {"stat": "NOT_OK", "emsg": f"Unknown API endpoint: {endpoint}"}
        except Exception as e:
            self.logger.error(f"Error in API request to {endpoint}: {str(e)}")
            return {"stat": "NOT_OK", "emsg": str(e)}

    def _init_websocket(self) -> bool:
        """Initialize the WebSocket connection for streaming market data."""
        # WebSocket already initialized by NorenApi during login
        self._ws_connected = True

        # Register callbacks for WebSocket events
        self.api.on_disconnect = self._on_ws_disconnect
        self.api.on_error = self._on_ws_error
        self.api.on_open = self._on_ws_open
        self.api.on_close = self._on_ws_close
        self.api.on_ticks = self._on_ticks_callback
        self.api.on_order_update = self._on_order_update

        # Start market data processor thread
        self._start_market_data_processor()

        return True

    def _on_ws_disconnect(self):
        """Handle WebSocket disconnection."""
        self.logger.warning("WebSocket disconnected")
        self._ws_connected = False


    def _on_ws_error(self, error):
        """Handle WebSocket errors."""
        self.logger.error(f"WebSocket error: {error}")
        self._ws_connected = False


    def _on_ws_close(self, code, reason):
        """Handle WebSocket close."""
        self.logger.info(f"WebSocket closed: {code} - {reason}")
        self._ws_connected = False


    def _on_ws_open(self):
        """Handle WebSocket open."""
        self.logger.info("WebSocket opened")
        self._ws_connected = True
        self._ws_retry_count = 0

        # Resubscribe to feeds
        self._resubscribe_feeds()

    def _on_ticks_callback(self, ticks):
        """Handle incoming market data ticks."""
        if not self._market_data_buffer.full():
            self._market_data_buffer.put(ticks)
        else:
            self.logger.warning("Market data buffer full, dropping message")

    def _on_order_update(self, order_data):
        """Handle order update events."""
        if "norenordno" in order_data:
            order_id = order_data["norenordno"]
            # Process order update (e.g., publish OrderEvent)
            # This might be triggered by WebSocket in live mode
            # or polled via REST API in paper/live mode.
            self.logger.debug(f"Received order update for {order_id}: {order_data}")
            # TODO: Convert to OrderEvent and publish via self.event_manager

    def _resubscribe_feeds(self) -> None:
        """Resubscribe to market data feeds after reconnection."""
        if not self._ws_connected:
            self.logger.warning("Cannot resubscribe: WebSocket not connected")
            return

        try:
            # Resubscribe to market data
            if self._subscriptions:
                tokens = list(self._subscriptions)
                # Submit in batches to avoid large payloads
                batch_size = 50
                for i in range(0, len(tokens), batch_size):
                    batch = tokens[i:i+batch_size]
                    self.api.send_ws_message({
                        "t": "t",
                        "k": "|".join(batch)
                    })
                self.logger.info(f"Resubscribed to {len(tokens)} market data feeds")

            # Resubscribe to market depth
            if self._depth_subscriptions:
                tokens = list(self._depth_subscriptions)
                # Submit in batches to avoid large payloads
                batch_size = 50
                for i in range(0, len(tokens), batch_size):
                    batch = tokens[i:i+batch_size]
                    self.api.send_ws_message({
                        "t": "d",
                        "k": "|".join(batch)
                    })
                self.logger.info(f"Resubscribed to {len(tokens)} market depth feeds")

        except Exception as e:
            self.logger.error(f"Error resubscribing to feeds: {str(e)}")

    def _start_market_data_processor(self) -> None:
        """Start the market data processing thread."""
        if self._market_data_thread and self._market_data_thread.is_alive():
            return  # Already running

        def process_data():
            while self._connected:
                try:
                    # Get data from queue, block until data is available or timeout
                    data = self._market_data_buffer.get(timeout=1)

                    # Check for exit signal
                    if data is None:
                        break

                    # Process the market data
                    self._process_market_data(data)

                    # Mark task as done
                    self._market_data_buffer.task_done()

                except queue.Empty:
                    # No data available, continue
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing market data: {str(e)}")

        # Start thread
        self._market_data_thread = threading.Thread(target=process_data, daemon=True)
        self._market_data_thread.start()

    def _process_market_data(self, data: Dict[str, Any]) -> None:
        """Process incoming market data."""
        try:
            # Handle different types of messages
            if isinstance(data, list):
                # Batch of ticks
                for tick in data:
                    self._process_single_tick(tick)
            elif isinstance(data, dict):
                # Single tick or other message
                self._process_single_tick(data)
        except Exception as e:
            self.logger.error(f"Error processing market data: {str(e)}")

    def _process_single_tick(self, tick: Dict[str, Any]) -> None:
        """
        Process a single market data tick.

        Args:
            tick: Single market data tick from the Finvasia API
        """
        try:
            # Check message type
            msg_type = tick.get('t')

            if msg_type == 'tk' or msg_type == 'tf':  # TouchLine quote or full quote
                token = tick.get('tk')
                if token in self._token_symbol_map:
                    symbol = self._token_symbol_map[token]
                    # Convert tick to Quote object
                    quote = self._convert_tick_to_quote(tick, symbol)

                    # Create market data event
                    self._publish_market_data_event(symbol, quote)

                    # Call global callback if registered
                    if hasattr(self, '_global_market_data_callback') and self._global_market_data_callback:
                        try:
                            self._global_market_data_callback(quote)
                        except Exception as e:
                            self.logger.error(f"Error in global market data callback: {str(e)}")

                    # Call specific callbacks
                    symbol_key = quote.instrument.exchange + "|" + quote.instrument.symbol if hasattr(quote, 'instrument') else symbol
                    if symbol_key in self.market_data_callbacks:
                        for callback in self.market_data_callbacks[symbol_key]:
                            try:
                                callback(quote)
                            except Exception as e:
                                self.logger.error(f"Error in specific market data callback: {str(e)}")

            elif msg_type == 'dk' or msg_type == 'df':  # Depth quote
                # Handle market depth data if needed
                pass

        except Exception as e:
            self.logger.error(f"Error processing tick: {str(e)}")

    def _convert_tick_to_quote(self, tick: Dict[str, Any], symbol: str) -> Quote:
        """
        Convert a Finvasia tick to a standardized Quote object.

        Args:
            tick: Finvasia tick data
            symbol: Symbol for the tick

        Returns:
            Quote: Standardized quote object
        """
        try:
            # Extract exchange
            exchange = tick.get('e', '')

            # Find or create instrument
            instrument = None
            for instr in self._instrument_cache.values():
                if instr.symbol == symbol and instr.exchange == exchange:
                    instrument = instr
                    break

            if not instrument:
                # Create basic instrument if not found
                from models.instrument import Instrument
                instrument = Instrument(symbol=symbol, exchange=exchange)

            # Create quote with basic fields
            quote = Quote(
                instrument=instrument,
                last_price=float(tick.get('lp', 0)),
                last_quantity=int(tick.get('ltq', 0)),
                volume=int(tick.get('v', 0)),
                bid=float(tick.get('bp', 0)) if 'bp' in tick else None,
                ask=float(tick.get('sp', 0)) if 'sp' in tick else None,
                bid_size=int(tick.get('bq', 0)) if 'bq' in tick else None,
                ask_size=int(tick.get('sq', 0)) if 'sq' in tick else None,
                open_price=float(tick.get('o', 0)) if 'o' in tick else None,
                high_price=float(tick.get('h', 0)) if 'h' in tick else None,
                low_price=float(tick.get('l', 0)) if 'l' in tick else None,
                close_price=float(tick.get('c', 0)) if 'c' in tick else None,
                timestamp=datetime.now()
            )

            # Add option-specific fields if this is an option
            if hasattr(instrument, 'instrument_type') and instrument.instrument_type in ['OPTION', 'CE', 'PE']:
                # Calculate option greeks
                greeks = self._calculate_option_greeks(instrument, quote.last_price)
                quote.greeks = greeks

            return quote

        except Exception as e:
            self.logger.error(f"Error converting tick to quote: {str(e)}")
            # Return a minimal quote to avoid crashes
            from models.instrument import Instrument
            from models.market_data import Quote
            basic_instrument = Instrument(symbol=symbol, exchange=tick.get('e', ''))
            return Quote(instrument=basic_instrument, last_price=0.0, timestamp=datetime.now())

    def _calculate_option_greeks(self, instrument, price):
        """
        Calculate option greeks for an instrument.

        Args:
            instrument: Option instrument
            price: Current price of the option

        Returns:
            dict: Dictionary containing option greeks
        """
        try:
            # For actual implementation, we would use a proper options pricing library like py_vollib
            # This is a placeholder implementation

            # Extract option details
            option_type = getattr(instrument, 'option_type', None)  # 'CE' or 'PE'
            strike_price = getattr(instrument, 'strike_price', 0.0)
            expiry_date = getattr(instrument, 'expiry_date', None)

            # If we don't have all required information, return empty greeks
            if not all([option_type, strike_price, expiry_date]):
                return {
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0
                }

            # In a real implementation, we would:
            # 1. Get the underlying price
            # 2. Calculate days to expiry
            # 3. Get/estimate volatility
            # 4. Use Black-Scholes or another model to calculate greeks

            # For now, returning placeholder values
            # In a complete implementation, use py_vollib, QuantLib, or another options library

            # Placeholder calculation (not accurate)
            days_to_expiry = (expiry_date - datetime.now().date()).days
            if days_to_expiry <= 0:
                days_to_expiry = 1  # Avoid division by zero

            # Very simplified delta approximation
            delta = 0.5
            if option_type == 'CE':
                if price > strike_price:
                    delta = 0.7
                else:
                    delta = 0.3
            else:  # 'PE'
                if price > strike_price:
                    delta = -0.3
                else:
                    delta = -0.7

            # Placeholder values for other greeks
            gamma = 0.05
            theta = -0.01 * (100 / days_to_expiry)
            vega = 0.1
            rho = 0.05

            return {
                'delta': delta,
                'gamma': gamma,
                'theta': theta,
                'vega': vega,
                'rho': rho
            }

        except Exception as e:
            self.logger.error(f"Error calculating option greeks: {str(e)}")
            return {
                'delta': 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0
            }

    def _publish_market_data_event(self, symbol: str, quote: Quote) -> None:
        """
        Publish a market data event through the event manager.

        Args:
            symbol: Symbol for the market data
            quote: Quote object with market data
        """
        if not self.event_manager:
            return

        try:
            from models.events import MarketDataEvent, EventType
            from utils.constants import MarketDataType

            # Create standardized data format
            data = {
                MarketDataType.LAST_PRICE: quote.last_price if hasattr(quote, 'last_price') else 0.0,
                MarketDataType.VOLUME: quote.volume if hasattr(quote, 'volume') else 0,
                MarketDataType.BID: quote.bid if hasattr(quote, 'bid') else None,
                MarketDataType.ASK: quote.ask if hasattr(quote, 'ask') else None,
                MarketDataType.BID_SIZE: quote.bid_size if hasattr(quote, 'bid_size') else None,
                MarketDataType.ASK_SIZE: quote.ask_size if hasattr(quote, 'ask_size') else None,
            }

            # Add OHLC data if available
            if (hasattr(quote, 'open_price') and quote.open_price is not None and
                hasattr(quote, 'high_price') and quote.high_price is not None and
                hasattr(quote, 'low_price') and quote.low_price is not None and
                hasattr(quote, 'close_price') and quote.close_price is not None):
                data[MarketDataType.OHLC] = {
                    'open': quote.open_price,
                    'high': quote.high_price,
                    'low': quote.low_price,
                    'close': quote.close_price
                }

            # Add option greeks if available
            if hasattr(quote, 'greeks') and quote.greeks:
                data[MarketDataType.GREEKS] = quote.greeks

            # Create and publish the event
            event = MarketDataEvent(
                event_type=EventType.MARKET_DATA,
                instrument=quote.instrument,
                data=data,
                timestamp=quote.timestamp
            )

            self.event_manager.publish(event)

        except Exception as e:
            self.logger.error(f"Error publishing market data event: {str(e)}")

    def place_order(self, order: Order) -> str:
        """
        Place a new order with the broker.

        Args:
            order: Order to place

        Returns:
            str: Order ID assigned by the broker
        """
        if self.paper_trading:
            # For paper trading, store the order in simulated_orders
            order_id = order.order_id
            order.status = OrderStatus.PENDING
            order.filled_quantity = 0
            order.average_fill_price = 0
            self.simulated_orders[order_id] = order
            return order_id

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return None

        try:
            # Get exchange and symbol
            exchange, trading_symbol = self._get_exchange_symbol(order.symbol)

            # Map order type
            order_type = self.ORDER_TYPE_MAPPING.get(order.order_type, "LMT")

            # Order side
            buy_or_sell = "B" if order.quantity > 0 else "S"
            quantity = abs(order.quantity)

            # Prepare order parameters
            params = {
                "buy_or_sell": buy_or_sell,
                "product_type": order.product_type or "I",  # I for Intraday, C for Delivery
                "exchange": exchange,
                "tradingsymbol": trading_symbol,
                "quantity": quantity,
                "discloseqty": 0,
                "price_type": order_type,
                "retention": "DAY",  # DAY, IOC, EOS
                "remarks": order.tag or "API"
            }

            # Add price for limit orders
            if order.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
                params["price"] = order.price

            # Add trigger price for stop orders
            if order.order_type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                params["trigger_price"] = order.stop_price

            # Place order using NorenApi
            response = self.api.place_order(**params)

            if response and response.get('stat') == 'Ok':
                order_id = response.get('norenordno')
                self.logger.info(f"Order placed successfully: {order_id}")
                return order_id
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Order placement failed: {error_msg}")
                return None

        except Exception as e:
            self.logger.error(f"Error placing order: {str(e)}")
            return None

    def modify_order(self, order_id: str, price: float = None, quantity: int = None,
                     trigger_price: float = None) -> bool:
        """
        Modify an existing order.

        Args:
            order_id: ID of the order to modify
            price: New price (for limit orders)
            quantity: New quantity
            trigger_price: New trigger price (for stop orders)

        Returns:
            bool: True if modification successful, False otherwise
        """
        if self.paper_trading:
            if order_id not in self.simulated_orders:
                return False
                
            order = self.simulated_orders[order_id]
            if order.status not in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]:
                return False
                
            if price is not None:
                order.price = price
            if quantity is not None:
                order.quantity = quantity
            if trigger_price is not None:
                order.stop_price = trigger_price
                
            return True

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return False

        try:
            # Prepare modification parameters
            params = {
                "orderno": order_id,
            }

            # Add parameters that need to be modified
            if price is not None:
                params["price"] = price

            if quantity is not None:
                params["quantity"] = quantity

            if trigger_price is not None:
                params["trigger_price"] = trigger_price

            # Modify order using NorenApi
            response = self.api.modify_order(**params)

            if response and response.get('stat') == 'Ok':
                self.logger.info(f"Order {order_id} modified successfully")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Order modification failed: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error modifying order: {str(e)}")
            return False

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.

        Args:
            order_id: ID of the order to cancel

        Returns:
            bool: True if cancellation successful, False otherwise
        """
        if self.paper_trading:
            if order_id not in self.simulated_orders:
                return False
                
            order = self.simulated_orders[order_id]
            if order.status not in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]:
                return False
                
            order.status = OrderStatus.CANCELED
            return True

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return False

        try:
            # Cancel order using NorenApi
            response = self.api.cancel_order(orderno=order_id)

            if response and response.get('stat') == 'Ok':
                self.logger.info(f"Order {order_id} cancelled successfully")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Order cancellation failed: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error cancelling order: {str(e)}")
            return False

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get status of a specific order."""
        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return None

        try:
            # Get order history using NorenApi
            response = self.api.single_order_history(orderno=order_id)

            if response and isinstance(response, list) and len(response) > 0:
                # Get latest status
                latest = response[0]

                # Map status
                status = self.STATUS_MAPPING.get(latest.get('status'), OrderStatus.UNKNOWN)

                # Format response
                order_details = {
                    'order_id': latest.get('norenordno'),
                    'status': status,
                    'filled_quantity': int(latest.get('fillshares', 0)),
                    'remaining_quantity': int(latest.get('qty', 0)) - int(latest.get('fillshares', 0)),
                    'average_price': float(latest.get('avgprc', 0)),
                    'order_timestamp': latest.get('exch_tm'),
                    'exchange': latest.get('exch'),
                    'symbol': latest.get('tsym'),
                    'instrument_token': latest.get('token'),
                    'product': latest.get('prd'),
                    'price': float(latest.get('prc', 0)),
                    'trigger_price': float(latest.get('trgprc', 0))
                }
                return order_details
            else:
                self.logger.error(f"Order {order_id} not found")
                return None

        except Exception as e:
            self.logger.error(f"Error getting order status: {str(e)}")
            return None

    def get_orders(self) -> List[Dict[str, Any]]:
        """Get list of all orders."""
        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return []

        try:
            # Get orders using NorenApi
            response = self.api.get_order_book()

            if response and isinstance(response, list):
                orders = []
                for order_data in response:
                    # Map status
                    status = self.STATUS_MAPPING.get(order_data.get('status'), OrderStatus.UNKNOWN)

                    # Format order
                    order = {
                        'order_id': order_data.get('norenordno'),
                        'status': status,
                        'filled_quantity': int(order_data.get('fillshares', 0)),
                        'remaining_quantity': int(order_data.get('qty', 0)) - int(order_data.get('fillshares', 0)),
                        'average_price': float(order_data.get('avgprc', 0)),
                        'order_timestamp': order_data.get('exch_tm'),
                        'exchange': order_data.get('exch'),
                        'symbol': order_data.get('tsym'),
                        'instrument_token': order_data.get('token'),
                        'product': order_data.get('prd'),
                        'price': float(order_data.get('prc', 0)),
                        'trigger_price': float(order_data.get('trgprc', 0))
                    }
                    orders.append(order)

                return orders
            else:
                self.logger.error("Failed to get orders")
                return []

        except Exception as e:
            self.logger.error(f"Error getting orders: {str(e)}")
            return []

    def get_positions(self) -> List[Position]:
        """Get current positions."""
        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return []

        try:
            # Get positions using NorenApi
            response = self.api.get_positions()

            if response and isinstance(response, list):
                positions = []
                for pos_data in response:
                    # Create Position object
                    position = Position(
                        symbol=f"{pos_data.get('exch')}:{pos_data.get('tsym')}",
                        quantity=int(pos_data.get('netqty', 0)),
                        average_price=float(pos_data.get('netavgprc', 0)),
                        last_price=float(pos_data.get('lp', 0)),
                        pnl=float(pos_data.get('rpnl', 0)),
                        product_type=pos_data.get('prd')
                    )
                    positions.append(position)

                return positions
            else:
                self.logger.error("Failed to get positions")
                return []

        except Exception as e:
            self.logger.error(f"Error getting positions: {str(e)}")
            return []

    def get_account_balance(self) -> Dict[str, Any]:
        """Get account balance and margin information."""
        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API")
            return {}

        try:
            # Get limits/margins using NorenApi
            response = self.api.get_limits()

            if response and isinstance(response, dict):
                # Format account balance
                balance = {
                    'cash_available': float(response.get('cash', 0)),
                    'margin_used': float(response.get('marginused', 0)),
                    'margin_available': float(response.get('avlcash', 0)),
                    'collateral': float(response.get('collateral', 0)),
                    'exposure_margin': float(response.get('exposure', 0)),
                    'span_margin': float(response.get('span', 0)),
                    'adhoc_margin': float(response.get('adhoc', 0)),
                    'option_premium': float(response.get('premium', 0)),
                    'realization_pnl': float(response.get('rpnl', 0)),
                    'unrealized_pnl': float(response.get('urmtom', 0)),
                    'net_value': float(response.get('netvalue', 0)),
                }
                return balance
            else:
                self.logger.error("Failed to get account balance")
                return {}

        except Exception as e:
            self.logger.error(f"Error getting account balance: {str(e)}")
            return {}

    def get_instruments(self, exchange: str = None) -> List[Instrument]:
        """Get list of tradable instruments."""
        if not self._check_instrument_cache(exchange):
            try:
                # Get instruments using NorenApi
                response = self.api.get_security_info(exchange=exchange) if exchange else None

                if not response:
                    # Get all exchanges if not specified
                    instruments = []
                    for exch in ["NSE", "BSE", "NFO", "CDS", "MCX"]:
                        exch_instruments = self.get_instruments(exch)
                        instruments.extend(exch_instruments)
                    return instruments

                instruments = []
                if isinstance(response, list):
                    for instr_data in response:
                        # Create Instrument object
                        instrument = Instrument(
                            symbol=f"{instr_data.get('exch')}:{instr_data.get('tsym')}",
                            name=instr_data.get('cname', ''),
                            exchange=instr_data.get('exch'),
                            instrument_type=instr_data.get('instrumenttype', ''),
                            lot_size=int(instr_data.get('ls', '1')),
                            tick_size=float(instr_data.get('ti', '0.05')),
                            expiry=parser.parse(instr_data.get('exd')) if instr_data.get('exd') else None,
                            strike_price=float(instr_data.get('strprc', 0)) if instr_data.get('strprc') else None,
                            option_type=instr_data.get('optt'),
                            token=instr_data.get('token')
                        )

                        # Update token maps
                        self._token_symbol_map[instr_data.get('token')] = instrument.symbol
                        self._symbol_token_map[instrument.symbol] = instr_data.get('token')

                        instruments.append(instrument)

                    # Update instrument cache
                    self._instrument_cache[exchange] = instruments

                    return instruments
                else:
                    self._log.error(f"Invalid response format for instruments: {response}")
                    return []

            except Exception as e:
                self._log.error(f"Error fetching instruments: {str(e)}")
                return []
        else:
            # Return from cache
            return self._instrument_cache.get(exchange, [])

    def _check_instrument_cache(self, exchange: str) -> bool:
        """Check if instruments are already cached and valid."""
        if exchange in self._instrument_cache:
            cache_time = self._instrument_cache_time.get(exchange)
            if cache_time and (datetime.now() - cache_time).total_seconds() < 86400:  # 24 hours
                return True
        return False

    # Methods for order placement and management
    def place_order(self, order_params):
        """
        Place an order with the Finvasia broker.

        Args:
            order_params (dict): Parameters for order placement

        Returns:
            dict: Order response with order_id and status
        """
        try:
            # Convert order parameters to Finvasia API format
            finvasia_params = self._convert_to_finvasia_order_format(order_params)

            # Place the order through the API
            response = self.api.place_order(**finvasia_params)

            # Validate the response
            if not response or 'norenordno' not in response:
                error_msg = response.get('emsg', 'Unknown error in order placement')
                self.logger.error(f"Order placement failed: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the response for our system
            result = {
                'order_id': response['norenordno'],
                'status': 'success',
                'exchange_order_id': response.get('exch_orderid', ''),
                'order_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Cache the order details for future reference
            self._cache_order_details(result['order_id'], order_params)

            self.logger.info(f"Order placed successfully: {result['order_id']}")
            return result

        except Exception as e:
            self.logger.exception(f"Exception during order placement: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def _convert_to_finvasia_order_format(self, params):
        """
        Convert standardized order parameters to Finvasia-specific format.

        Args:
            params (dict): Standardized order parameters

        Returns:
            dict: Parameters formatted for Finvasia API
        """
        # Map order types
        order_type_map = {
            'MARKET': 'MKT',
            'LIMIT': 'LMT',
            'STOP_LOSS': 'SL-LMT',
            'STOP_LOSS_MARKET': 'SL-MKT'
        }

        # Map transaction types
        txn_type_map = {
            'BUY': 'B',
            'SELL': 'S'
        }

        # Construct the Finvasia order parameters
        finvasia_params = {
            'buy_or_sell': txn_type_map.get(params['transaction_type'], 'B'),
            'product_type': params.get('product_type', 'C'),  # Default to CNC (Cash)
            'exchange': params['exchange'],
            'tradingsymbol': params['symbol'],
            'quantity': int(params['quantity']),
            'discloseqty': int(params.get('disclosed_quantity', 0)),
            'price_type': order_type_map.get(params['order_type'], 'LMT'),
            'price': float(params.get('price', 0)),
            'trigger_price': float(params.get('trigger_price', 0)),
            'retention': params.get('validity', 'DAY'),
            'remarks': params.get('tag', '')
        }

        # Handle AMO (After Market Orders)
        if params.get('is_amo', False):
            finvasia_params['amo'] = 'YES'

        return finvasia_params

    def _cache_order_details(self, order_id, order_params):
        """
        Cache order details for faster reference in subsequent operations.

        Args:
            order_id (str): The order ID returned by the broker
            order_params (dict): Original order parameters
        """
        self.order_cache[order_id] = {
            'params': order_params,
            'timestamp': datetime.now(),
            'status': 'PENDING',
            'last_updated': datetime.now()
        }

    def modify_order(self, order_id, modified_params):
        """
        Modify an existing order.

        Args:
            order_id (str): Order ID to modify
            modified_params (dict): Parameters to modify

        Returns:
            dict: Response with status of modification
        """
        try:
            # Check if order exists in our cache
            if order_id not in self.order_cache:
                # Fetch the order details if not in cache
                order_details = self.get_order_details(order_id)
                if not order_details or order_details.get('status') == 'error':
                    return {'status': 'error', 'message': 'Order not found'}

            # Prepare parameters for modification
            finvasia_params = {
                'orderno': order_id,
            }

            # Add only the parameters that are being modified
            if 'quantity' in modified_params:
                finvasia_params['quantity'] = int(modified_params['quantity'])

            if 'price' in modified_params:
                finvasia_params['price'] = float(modified_params['price'])

            if 'trigger_price' in modified_params:
                finvasia_params['trigger_price'] = float(modified_params['trigger_price'])

            # Call the API to modify the order
            response = self.api.modify_order(**finvasia_params)

            # Process the response
            if not response or 'result' not in response or response['result'] != 'Success':
                error_msg = response.get('emsg', 'Unknown error in order modification')
                self.logger.error(f"Order modification failed: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Update the cache with modified parameters
            if order_id in self.order_cache:
                self.order_cache[order_id]['params'].update(modified_params)
                self.order_cache[order_id]['last_updated'] = datetime.now()

            self.logger.info(f"Order {order_id} modified successfully")
            return {'status': 'success', 'order_id': order_id}

        except Exception as e:
            self.logger.exception(f"Exception during order modification: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def cancel_order(self, order_id):
        """
        Cancel an existing order.

        Args:
            order_id (str): Order ID to cancel

        Returns:
            dict: Response with status of cancellation
        """
        try:
            # Call the API to cancel the order
            response = self.api.cancel_order(orderno=order_id)

            # Process the response
            if not response or 'result' not in response or response['result'] != 'Success':
                error_msg = response.get('emsg', 'Unknown error in order cancellation')
                self.logger.error(f"Order cancellation failed: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Update the cache
            if order_id in self.order_cache:
                self.order_cache[order_id]['status'] = 'CANCELLED'
                self.order_cache[order_id]['last_updated'] = datetime.now()

            self.logger.info(f"Order {order_id} cancelled successfully")
            return {'status': 'success', 'order_id': order_id}

        except Exception as e:
            self.logger.exception(f"Exception during order cancellation: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_order_details(self, order_id):
        """
        Get details of a specific order.

        Args:
            order_id (str): Order ID to fetch details for

        Returns:
            dict: Order details
        """
        try:
            # Check if we have fresh data in cache
            if order_id in self.order_cache:
                cache_time = self.order_cache[order_id]['last_updated']
                if (datetime.now() - cache_time).seconds < 60:  # Use cache if less than 60 seconds old
                    return self._format_order_details(self.order_cache[order_id])

            # Call the API to get order details
            response = self.api.single_order_history(orderno=order_id)

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching order details')
                self.logger.error(f"Failed to get order details: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the response and update cache
            order_details = self._process_order_history(response)

            if order_id in self.order_cache:
                self.order_cache[order_id].update({
                    'status': order_details.get('status', 'UNKNOWN'),
                    'last_updated': datetime.now()
                })

            return order_details

        except Exception as e:
            self.logger.exception(f"Exception fetching order details: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def _process_order_history(self, order_history):
        """
        Process order history data from Finvasia API.

        Args:
            order_history (list/dict): Order history data from API

        Returns:
            dict: Formatted order details
        """
        if not order_history:
            return {'status': 'error', 'message': 'No order history data'}

        # Handle single order case
        if isinstance(order_history, dict):
            order_history = [order_history]

        # Get the most recent entry (last status)
        latest_order = order_history[-1]

        # Map Finvasia status to standardized status
        status_map = {
            'COMPLETE': 'COMPLETED',
            'REJECTED': 'REJECTED',
            'CANCELED': 'CANCELLED',
            'OPEN': 'OPEN',
            'PENDING': 'PENDING',
            'PARTIALLY EXECUTED': 'PARTIAL'
        }

        # Format the order details
        formatted_order = {
            'order_id': latest_order.get('norenordno', ''),
            'exchange_order_id': latest_order.get('exch_orderid', ''),
            'status': status_map.get(latest_order.get('status', ''), 'UNKNOWN'),
            'symbol': latest_order.get('tradingsymbol', ''),
            'exchange': latest_order.get('exchange', ''),
            'transaction_type': 'BUY' if latest_order.get('trantype', '') == 'B' else 'SELL',
            'quantity': int(latest_order.get('qty', 0)),
            'price': float(latest_order.get('prc', 0)),
            'trigger_price': float(latest_order.get('trgprc', 0)),
            'filled_quantity': int(latest_order.get('filledqty', 0)),
            'pending_quantity': int(latest_order.get('qty', 0)) - int(latest_order.get('filledqty', 0)),
            'order_timestamp': latest_order.get('ordertime', ''),
            'last_updated_timestamp': latest_order.get('exchupd', '')
        }

        # Add average executed price if available
        if 'avgprc' in latest_order:
            formatted_order['average_price'] = float(latest_order['avgprc'])

        return formatted_order

    def get_order_history(self, from_date=None, to_date=None):
        """
        Get order history for a specified date range.

        Args:
            from_date (str, optional): Start date in YYYY-MM-DD format
            to_date (str, optional): End date in YYYY-MM-DD format

        Returns:
            list: List of orders in the specified date range
        """
        try:
            # Set default date range to today if not provided
            if not from_date:
                from_date = datetime.now().strftime('%Y-%m-%d')
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d')

            # Call the API to get order book
            response = self.api.get_order_book()

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching order history')
                self.logger.error(f"Failed to get order history: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Filter orders by date range
            from_dt = datetime.strptime(from_date, '%Y-%m-%d')
            to_dt = datetime.strptime(to_date, '%Y-%m-%d') + timedelta(days=1)  # Include to_date fully

            filtered_orders = []
            for order in response:
                if 'ordertime' in order:
                    try:
                        order_dt = datetime.strptime(order['ordertime'], '%d-%b-%Y %H:%M:%S')
                        if from_dt <= order_dt < to_dt:
                            filtered_orders.append(self._process_single_order(order))
                    except (ValueError, TypeError):
                        # Skip orders with invalid date format
                        continue

            return filtered_orders

        except Exception as e:
            self.logger.exception(f"Exception fetching order history: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def _process_single_order(self, order):
        """
        Process a single order data from order book.

        Args:
            order (dict): Order data from API

        Returns:
            dict: Formatted order data
        """
        # Similar to _process_order_history but for a single order
        status_map = {
            'COMPLETE': 'COMPLETED',
            'REJECTED': 'REJECTED',
            'CANCELED': 'CANCELLED',
            'OPEN': 'OPEN',
            'PENDING': 'PENDING',
            'PARTIALLY EXECUTED': 'PARTIAL'
        }

        return {
            'order_id': order.get('norenordno', ''),
            'exchange_order_id': order.get('exch_orderid', ''),
            'status': status_map.get(order.get('status', ''), 'UNKNOWN'),
            'symbol': order.get('tradingsymbol', ''),
            'exchange': order.get('exchange', ''),
            'transaction_type': 'BUY' if order.get('trantype', '') == 'B' else 'SELL',
            'quantity': int(order.get('qty', 0)),
            'price': float(order.get('prc', 0)),
            'trigger_price': float(order.get('trgprc', 0)),
            'filled_quantity': int(order.get('filledqty', 0)),
            'pending_quantity': int(order.get('qty', 0)) - int(order.get('filledqty', 0)),
            'order_timestamp': order.get('ordertime', ''),
            'last_updated_timestamp': order.get('exchupd', '')
        }

    # Methods for position and holding management

    def get_positions(self):
        """
        Get current day's positions.

        Returns:
            list: List of current positions
        """
        try:
            # Call the API to get positions
            response = self.api.get_positions()

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching positions')
                self.logger.error(f"Failed to get positions: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the positions
            formatted_positions = []
            for position in response:
                formatted_positions.append({
                    'symbol': position.get('tradingsymbol', ''),
                    'exchange': position.get('exchange', ''),
                    'product': position.get('prd', ''),
                    'quantity': int(position.get('netqty', 0)),
                    'avg_price': float(position.get('avgprc', 0)),
                    'ltp': float(position.get('lp', 0)),
                    'pnl': float(position.get('rpnl', 0)),
                    'unrealized_pnl': float(position.get('urmtom', 0)),
                    'realized_pnl': float(position.get('rpnl', 0)),
                    'buy_quantity': int(position.get('buyqty', 0)),
                    'sell_quantity': int(position.get('sellqty', 0)),
                    'buy_value': float(position.get('buyavgprc', 0)) * int(position.get('buyqty', 0)),
                    'sell_value': float(position.get('sellavgprc', 0)) * int(position.get('sellqty', 0))
                })

            return formatted_positions

        except Exception as e:
            self.logger.exception(f"Exception fetching positions: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_holdings(self):
        """
        Get current holdings (stocks owned).

        Returns:
            list: List of current holdings
        """
        try:
            # Call the API to get holdings
            response = self.api.get_holdings()

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching holdings')
                self.logger.error(f"Failed to get holdings: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the holdings
            formatted_holdings = []
            for holding in response:
                formatted_holdings.append({
                    'symbol': holding.get('tradingsymbol', ''),
                    'exchange': holding.get('exch', ''),
                    'isin': holding.get('isin', ''),
                    'quantity': int(holding.get('holdqty', 0)),
                    'avg_price': float(holding.get('avgprc', 0)),
                    'ltp': float(holding.get('lp', 0)),
                    'current_value': float(holding.get('lp', 0)) * int(holding.get('holdqty', 0)),
                    'investment_value': float(holding.get('avgprc', 0)) * int(holding.get('holdqty', 0)),
                    'pnl': (float(holding.get('lp', 0)) - float(holding.get('avgprc', 0))) * int(holding.get('holdqty', 0)),
                    'pnl_percent': ((float(holding.get('lp', 0)) / float(holding.get('avgprc', 0))) - 1) * 100 if float(holding.get('avgprc', 0)) > 0 else 0
                })

            return formatted_holdings

        except Exception as e:
            self.logger.exception(f"Exception fetching holdings: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    # Methods for market data

    def get_quote(self, exchange, symbol):
        """
        Get market quote for a symbol.

        Args:
            exchange (str): Exchange code (NSE, BSE, etc.)
            symbol (str): Trading symbol

        Returns:
            dict: Market quote details
        """
        try:
            # Call the API to get quote
            response = self.api.get_quotes(exchange=exchange, token=symbol)

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching quote')
                self.logger.error(f"Failed to get quote: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the quote
            quote = {
                'symbol': response.get('tsym', ''),
                'ltp': float(response.get('lp', 0)),
                'change': float(response.get('chg', 0)),
                'change_percent': float(response.get('chgp', 0)),
                'open': float(response.get('op', 0)),
                'high': float(response.get('hp', 0)),
                'low': float(response.get('lp', 0)),
                'close': float(response.get('c', 0)),
                'volume': int(response.get('v', 0)),
                'bid': float(response.get('bp1', 0)),
                'ask': float(response.get('sp1', 0)),
                'bid_qty': int(response.get('bq1', 0)),
                'ask_qty': int(response.get('sq1', 0)),
                'timestamp': response.get('ltt', '')
            }

            return quote

        except Exception as e:
            self.logger.exception(f"Exception fetching quote: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_historical_data(self, exchange, symbol, interval, from_date, to_date):
        """
        Get historical price data for a symbol.

        Args:
            exchange (str): Exchange code (NSE, BSE, etc.)
            symbol (str): Trading symbol
            interval (str): Time interval (1m, 5m, 15m, 30m, 1h, 1d)
            from_date (str): Start date in YYYY-MM-DD format
            to_date (str): End date in YYYY-MM-DD format

        Returns:
            list: List of OHLCV data points
        """
        try:
            # Map interval to Finvasia format
            interval_map = {
                '1m': '1',
                '5m': '5',
                '15m': '15',
                '30m': '30',
                '1h': '60',
                '1d': 'D'
            }

            finvasia_interval = interval_map.get(interval, 'D')

            # Format dates for Finvasia API
            from_date_obj = datetime.strptime(from_date, '%Y-%m-%d')
            to_date_obj = datetime.strptime(to_date, '%Y-%m-%d')

            # Call the API to get historical data
            response = self.api.get_time_price_series(
                exchange=exchange,
                token=symbol,
                starttime=from_date_obj.strftime('%d-%m-%Y'),
                endtime=to_date_obj.strftime('%d-%m-%Y'),
                interval=finvasia_interval
            )

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching historical data')
                self.logger.error(f"Failed to get historical data: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the historical data
            formatted_data = []
            for candle in response:
                formatted_data.append({
                    'timestamp': candle.get('time', ''),
                    'open': float(candle.get('into', 0)),
                    'high': float(candle.get('inth', 0)),
                    'low': float(candle.get('intl', 0)),
                    'close': float(candle.get('intc', 0)),
                    'volume': int(candle.get('v', 0))
                })

            return formatted_data

        except Exception as e:
            self.logger.exception(f"Exception fetching historical data: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    # WebSocket methods for live market data

    def start_websocket(self):
        """
        Start WebSocket connection for live market data.

        Returns:
            bool: Success status
        """
        try:
            if self.websocket_connected:
                self.logger.info("WebSocket already connected")
                return True

            # Initialize and connect WebSocket
            self.api.start_websocket(order_update_callback=self._on_order_update,
                                    subscribe_callback=self._on_market_data,
                                    socket_open_callback=self._on_socket_open)

            # Wait for connection to establish
            start_time = time.time()
            while not self.websocket_connected and time.time() - start_time < 10:
                time.sleep(0.5)

            if not self.websocket_connected:
                self.logger.error("Failed to establish WebSocket connection")
                return False

            self.logger.info("WebSocket connection established successfully")
            return True

        except Exception as e:
            self.logger.exception(f"Exception starting WebSocket: {str(e)}")
            return False

    def subscribe_symbols(self, symbols):
        """
        Subscribe to market data for symbols.

        Args:
            symbols (list): List of dicts with exchange and symbol
                [{'exchange': 'NSE', 'symbol': 'RELIANCE'}, ...]

        Returns:
            bool: Success status
        """
        try:
            if not self.websocket_connected:
                if not self.start_websocket():
                    return False

            # Format symbols for subscription
            formatted_symbols = []
            for instrument in symbols:
                formatted_symbols.append(f"{instrument['exchange']}|{instrument['symbol']}")

                # Add option-specific fields if needed
                if hasattr(instrument, 'instrument_type') and instrument.instrument_type == InstrumentType.OPTION.value:
                    formatted_symbols.append(f"{instrument['exchange']}|{instrument['symbol']}")
                    if hasattr(instrument, 'option_type'):
                        formatted_symbols.append(f"{instrument['exchange']}|{instrument['symbol']}")




            # Subscribe to market data
            self.api.subscribe(formatted_symbols)

            # Add to subscribed symbols
            self.subscribed_symbols.extend(formatted_symbols)

            self.logger.info(f"Subscribed to symbols: {formatted_symbols}")
            return True

        except Exception as e:
            self.logger.exception(f"Exception subscribing to symbols: {str(e)}")
            return False

    def unsubscribe_symbols(self, symbols):
        """
        Unsubscribe from market data for symbols.

        Args:
            symbols (list): List of dicts with exchange and symbol
                [{'exchange': 'NSE', 'symbol': 'RELIANCE'}, ...]

        Returns:
            bool: Success status
        """
        try:
            if not self.websocket_connected:
                self.logger.warning("WebSocket not connected, can't unsubscribe")
                return False

            # Format symbols for unsubscription
            formatted_symbols = []
            for item in symbols:
                formatted_symbols.append(f"{item['exchange']}|{item['symbol']}")

            # Unsubscribe from market data
            self.api.unsubscribe(formatted_symbols)

            # Remove from subscribed symbols
            for symbol in formatted_symbols:
                if symbol in self.subscribed_symbols:
                    self.subscribed_symbols.remove(symbol)

            self.logger.info(f"Unsubscribed from symbols: {formatted_symbols}")
            return True

        except Exception as e:
            self.logger.exception(f"Exception unsubscribing from symbols: {str(e)}")
            return False

    def _on_socket_open(self):
        """
        Callback when WebSocket connection is opened.
        """
        self.websocket_connected = True
        self.logger.info("WebSocket connection opened")

        # Re-subscribe to any symbols if reconnecting
        if self.subscribed_symbols:
            self.api.subscribe(self.subscribed_symbols)
            self.logger.info(f"Re-subscribed to symbols: {self.subscribed_symbols}")

    def _on_market_data(self, ticks):
        """
        Callback for market data updates from Finvasia API.

        Args:
            ticks: Market data ticks from the Finvasia API, can be a single tick or list of ticks
        """
        try:
            # Handle both single tick (dict) and multiple ticks (list)
            if isinstance(ticks, list):
                # Process list of ticks
                for tick in ticks:
                    self._process_single_tick(tick)
            elif isinstance(ticks, dict):
                # Process single tick
                self._process_single_tick(ticks)
            else:
                self.logger.warning(f"Received unknown tick format: {type(ticks)}")

        except Exception as e:
            self.logger.error(f"Error processing market data: {str(e)}")

    def _on_order_update(self, message):
        """
        Callback for order updates.

        Args:
            message (dict): Order update message
        """
        try:
            # Process order update message
            order_id = message.get('norenordno', '')
            if not order_id:
                return

            # Format the order update
            order_update = {
                'order_id': order_id,
                'exchange_order_id': message.get('exch_orderid', ''),
                'status': message.get('status', ''),
                'filled_quantity': int(message.get('filledqty', 0)),
                'pending_quantity': int(message.get('qty', 0)) - int(message.get('filledqty', 0)),
                'average_price': float(message.get('avgprc', 0)),
                'timestamp': message.get('exchupd', '')
            }

            # Update order cache
            if order_id in self.order_cache:
                self.order_cache[order_id].update({
                    'status': order_update['status'],
                    'last_updated': datetime.now()
                })

            # Call registered callbacks
            if self.order_update_callback:
                try:
                    self.order_update_callback(order_update)
                except Exception as e:
                    self.logger.error(f"Error in order update callback: {str(e)}")

        except Exception as e:
            self.logger.exception(f"Exception processing order update: {str(e)}")

    def register_market_data_callback(self, exchange=None, symbol=None, callback=None):
        """
        Register callback for market data updates.
        Can be called in two ways:
        1. register_market_data_callback(callback) - Registers a global callback
        2. register_market_data_callback(exchange, symbol, callback) - Registers a specific callback

        Args:
            exchange (str, optional): Exchange code
            symbol (str, optional): Symbol to register callback for
            callback (Callable): Callback function to register

        Returns:
            bool: Success status
        """
        try:
            # Handle case where only callback is provided
            if callback is None:
                # If only one argument is provided, it's the callback
                callback = exchange
                exchange = None
                symbol = None

            if not callable(callback):
                self.logger.error("Invalid callback provided")
                return False

            # Initialize callbacks dict if not already done
            if not hasattr(self, 'market_data_callbacks'):
                self.market_data_callbacks = {}

            if exchange and symbol:
                # Register for specific exchange and symbol
                symbol_key = f"{exchange}|{symbol}"

                # Initialize callback list if needed
                if symbol_key not in self.market_data_callbacks:
                    self.market_data_callbacks[symbol_key] = []

                # Add callback to the list
                self.market_data_callbacks[symbol_key].append(callback)

                # Subscribe to the symbol if not already subscribed
                if hasattr(self, 'subscribe_symbols'):
                    formatted_symbol = [{'exchange': exchange, 'symbol': symbol}]
                    if symbol_key not in getattr(self, 'subscribed_symbols', set()):
                        self.subscribe_symbols(formatted_symbol)

                self.logger.info(f"Registered callback for {symbol_key}")
            else:
                # Register global callback
                self._global_market_data_callback = callback
                self.logger.info("Registered global market data callback")

            return True

        except Exception as e:
            self.logger.error(f"Error registering market data callback: {str(e)}")
            return False

    def register_order_update_callback(self, callback):
        """
        Register callback for order updates.

        Args:
            callback (function): Callback function for order updates

        Returns:
            bool: Success status
        """
        try:
            self.order_update_callback = callback

            # Start WebSocket if not already connected
            if not self.websocket_connected:
                self.start_websocket()

            self.logger.info("Registered order update callback")
            return True

        except Exception as e:
            self.logger.exception(f"Exception registering order update callback: {str(e)}")
            return False

    def stop_websocket(self):
        """
        Stop WebSocket connection.

        Returns:
            bool: Success status
        """
        try:
            if not self.websocket_connected:
                # self.logger.info("WebSocket not connected")
                print("WebSocket not connected")
                return True

            # Close WebSocket connection
            self.api.close_websocket()
            self.websocket_connected = False

            # self.logger.info("WebSocket connection closed")
            print("WebSocket connection closed")
            return True

        except Exception as e:
            # self.logger.exception(f"Exception stopping WebSocket: {str(e)}")
            print(f"Exception stopping WebSocket: {str(e)}")
            return False

    # Account methods

    def get_funds(self):
        """
        Get account funds and margins.

        Returns:
            dict: Account funds and margins
        """
        try:
            # Call the API to get funds
            response = self.api.get_limits()

            # Process the response
            if not response or 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching funds')
                self.logger.error(f"Failed to get funds: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the funds
            funds = {
                'cash_available': float(response.get('cash', 0)),
                'margin_used': float(response.get('marginused', 0)),
                'margin_available': float(response.get('availablemargin', 0)),
                'total_collateral': float(response.get('collateral', 0)),
                'payin_amount': float(response.get('payin', 0)),
                'payout_amount': float(response.get('payout', 0))
            }

            # Add segment-specific margins if available
            if 'equity' in response:
                equity = response['equity']
                funds['equity'] = {
                    'margin_used': float(equity.get('marginused', 0)),
                    'margin_available': float(equity.get('availablemargin', 0)),
                    'net_premium': float(equity.get('premium', 0)),
                    'span_margin': float(equity.get('span', 0)),
                    'exposure_margin': float(equity.get('exposure', 0))
                }

            if 'commodity' in response:
                commodity = response['commodity']
                funds['commodity'] = {
                    'margin_used': float(commodity.get('marginused', 0)),
                    'margin_available': float(commodity.get('availablemargin', 0)),
                    'net_premium': float(commodity.get('premium', 0)),
                    'span_margin': float(commodity.get('span', 0)),
                    'exposure_margin': float(commodity.get('exposure', 0))
                }

            return funds

        except Exception as e:
            self.logger.exception(f"Exception fetching funds: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_order_margin(self, order_params):
        """
        Calculate margin required for an order.

        Args:
            order_params (dict): Order parameters

        Returns:
            dict: Margin details
        """
        try:
            # Convert order parameters to Finvasia format
            finvasia_params = self._convert_to_finvasia_order_format(order_params)

            # Add required parameters for margin calculation
            margin_params = {
                'exchange': finvasia_params['exchange'],
                'tradingsymbol': finvasia_params['tradingsymbol'],
                'quantity': finvasia_params['quantity'],
                'price': finvasia_params['price'],
                'product': finvasia_params['product_type'],
                'buy_or_sell': finvasia_params['buy_or_sell'],
                'price_type': finvasia_params['price_type']
            }

            # Call the API to get margin
            response = self.api.get_order_margin(**margin_params)

            # Process the response
            if not response or 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error calculating margin')
                self.logger.error(f"Failed to get order margin: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the margin details
            margin = {
                'total_margin': float(response.get('total', 0)),
                'span_margin': float(response.get('span', 0)),
                'exposure_margin': float(response.get('exposure', 0)),
                'additional_margin': float(response.get('additional', 0)),
                'premium': float(response.get('premium', 0)),
                'bo_margin': float(response.get('bo', 0)),
                'cash': float(response.get('cash', 0)),
                'var_margin': float(response.get('var', 0))
            }

            return margin

        except Exception as e:
            self.logger.exception(f"Exception calculating order margin: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_market_status(self):
        """
        Get market status (open/closed).

        Returns:
            dict: Market status for different exchanges
        """
        try:
            # Call the API to get market status
            response = self.api.get_exchanges()

            # Process the response
            if not response or isinstance(response, dict) and 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching market status')
                self.logger.error(f"Failed to get market status: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the market status
            market_status = {}
            for exchange in response:
                if 'exch' in exchange and 'stat' in exchange:
                    market_status[exchange['exch']] = {
                        'status': 'open' if exchange['stat'] == 'Ok' else 'closed',
                        'message': exchange.get('msg', '')
                    }

            return market_status

        except Exception as e:
            self.logger.exception(f"Exception fetching market status: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_trading_symbol(self, exchange, token=None, symbol=None):
        """
        Get trading symbol details.

        Args:
            exchange (str): Exchange code
            token (str, optional): Scrip token
            symbol (str, optional): Trading symbol

        Returns:
            dict: Trading symbol details
        """
        try:
            # Call the API to get trading symbol details
            if token:
                response = self.api.searchscrip(exchange=exchange, searchtext=token)
            elif symbol:
                response = self.api.searchscrip(exchange=exchange, searchtext=symbol)
            else:
                return {'status': 'error', 'message': 'Either token or symbol must be provided'}

            # Process the response
            if not response or 'values' not in response:
                error_msg = response.get('emsg', 'Unknown error fetching trading symbol')
                self.logger.error(f"Failed to get trading symbol: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the trading symbol details
            symbols = []
            for scrip in response['values']:
                symbols.append({
                    'exchange': scrip.get('exch', ''),
                    'token': scrip.get('token', ''),
                    'symbol': scrip.get('tsym', ''),
                    'name': scrip.get('cname', ''),
                    'expiry': scrip.get('exd', ''),
                    'strike_price': float(scrip.get('strprc', 0)),
                    'tick_size': float(scrip.get('ti', 0)),
                    'lot_size': int(scrip.get('ls', 0)),
                    'instrument_type': scrip.get('inst', '')
                })

            return symbols

        except Exception as e:
            self.logger.exception(f"Exception fetching trading symbol: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_profile(self):
        """
        Get user profile details.

        Returns:
            dict: User profile details
        """
        try:
            # Call the API to get user profile
            response = self.api.get_user_details()

            # Process the response
            if not response or 'emsg' in response:
                error_msg = response.get('emsg', 'Unknown error fetching profile')
                self.logger.error(f"Failed to get profile: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the profile details
            profile = {
                'client_id': response.get('actid', ''),
                'name': response.get('name', ''),
                'email': response.get('email', ''),
                'phone': response.get('mobileno', ''),
                'pan': response.get('pan', ''),
                'address': response.get('address', ''),
                'branch_id': response.get('brkname', ''),
                'exchanges': response.get('exarr', []),
                'products': response.get('prarr', []),
                'order_types': response.get('orarr', [])
            }

            return profile

        except Exception as e:
            self.logger.exception(f"Exception fetching profile: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    # Utility methods

    def place_bracket_order(self, order_params):
        """
        Place a bracket order (with target and stop loss).

        Args:
            order_params (dict): Order parameters with target and stop loss

        Returns:
            dict: Order response with order_id and status
        """
        try:
            # Extract bracket order specific parameters
            target = order_params.pop('target', 0)
            stop_loss = order_params.pop('stop_loss', 0)
            trailing_sl = order_params.pop('trailing_sl', 0)

            # Convert order parameters to Finvasia API format
            finvasia_params = self._convert_to_finvasia_order_format(order_params)

            # Add bracket order specific parameters
            finvasia_params['product_type'] = 'B'  # Bracket order product type
            finvasia_params['target'] = target
            finvasia_params['stoploss'] = stop_loss
            finvasia_params['trailing_stoploss'] = trailing_sl

            # Place the order through the API
            response = self.api.place_order(**finvasia_params)

            # Validate the response
            if not response or 'norenordno' not in response:
                error_msg = response.get('emsg', 'Unknown error in bracket order placement')
                self.logger.error(f"Bracket order placement failed: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the response for our system
            result = {
                'order_id': response['norenordno'],
                'status': 'success',
                'exchange_order_id': response.get('exch_orderid', ''),
                'order_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Cache the order details for future reference
            self._cache_order_details(result['order_id'], order_params)

            self.logger.info(f"Bracket order placed successfully: {result['order_id']}")
            return result

        except Exception as e:
            self.logger.exception(f"Exception during bracket order placement: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def place_cover_order(self, order_params):
        """
        Place a cover order (with stop loss).

        Args:
            order_params (dict): Order parameters with stop loss

        Returns:
            dict: Order response with order_id and status
        """
        try:
            # Extract cover order specific parameters
            stop_loss = order_params.pop('stop_loss', 0)

            # Convert order parameters to Finvasia API format
            finvasia_params = self._convert_to_finvasia_order_format(order_params)

            # Add cover order specific parameters
            finvasia_params['product_type'] = 'H'  # Cover order product type
            finvasia_params['stoploss'] = stop_loss

            # Place the order through the API
            response = self.api.place_order(**finvasia_params)

            # Validate the response
            if not response or 'norenordno' not in response:
                error_msg = response.get('emsg', 'Unknown error in cover order placement')
                self.logger.error(f"Cover order placement failed: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Format the response for our system
            result = {
                'order_id': response['norenordno'],
                'status': 'success',
                'exchange_order_id': response.get('exch_orderid', ''),
                'order_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Cache the order details for future reference
            self._cache_order_details(result['order_id'], order_params)

            self.logger.info(f"Cover order placed successfully: {result['order_id']}")
            return result

        except Exception as e:
            self.logger.exception(f"Exception during cover order placement: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_option_chain(self, exchange, underlying, expiry=None):
        """
        Get option chain for an underlying.

        Args:
            exchange (str): Exchange code (NSE, NFO)
            underlying (str): Underlying symbol
            expiry (str, optional): Expiry date (YYYY-MM-DD format)

        Returns:
            dict: Option chain data
        """
        try:
            # Call the API to get option chain
            params = {
                'exchange': exchange,
                'searchtext': underlying,
                'product_type': 'options'
            }

            response = self.api.searchscrip(**params)

            # Process the response
            if not response or 'values' not in response:
                error_msg = response.get('emsg', 'Unknown error fetching option chain')
                self.logger.error(f"Failed to get option chain: {error_msg}")
                return {'status': 'error', 'message': error_msg}

            # Filter options by expiry if provided
            options = response['values']
            if expiry:
                expiry_date = datetime.strptime(expiry, '%Y-%m-%d').strftime('%d%b%Y')
                options = [opt for opt in options if opt.get('exd', '') == expiry_date]

            # Extract available expiry dates
            expiry_dates = list(set([opt.get('exd', '') for opt in options]))

            # Organize options by expiry, strike price and option type
            option_chain = {
                'underlying': underlying,
                'expiry_dates': expiry_dates,
                'options': {}
            }

            for opt in options:
                exp = opt.get('exd', '')
                strike = float(opt.get('strprc', 0))
                opt_type = 'CE' if 'CE' in opt.get('tsym', '') else 'PE'

                if exp not in option_chain['options']:
                    option_chain['options'][exp] = {}

                if strike not in option_chain['options'][exp]:
                    option_chain['options'][exp][strike] = {}

                option_chain['options'][exp][strike][opt_type] = {
                    'symbol': opt.get('tsym', ''),
                    'token': opt.get('token', ''),
                    'tick_size': float(opt.get('ti', 0)),
                    'lot_size': int(opt.get('ls', 0)),
                    'instrument_type': opt.get('inst', ''),
                    'expiry': exp,
                    'strike_price': strike,
                    'option_type': opt_type
                }

            return option_chain

        except Exception as e:
            self.logger.exception(f"Exception fetching option chain: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def exit_positions(self, position_type=None):
        """
        Exit all or specific type of positions.

        Args:
            position_type (str, optional): Type of positions to exit ('DAY', 'LONG', 'SHORT')

        Returns:
            dict: Status of position exit
        """
        try:
            # Get current positions
            positions = self.get_positions()

            if 'status' in positions and positions['status'] == 'error':
                return positions

            # Filter positions based on type if specified
            exit_positions = []
            for position in positions:
                quantity = int(position.get('quantity', 0))

                # Skip positions with zero quantity
                if quantity == 0:
                    continue

                # Filter by position type
                if position_type == 'DAY' and position.get('product') != 'MIS':
                    continue
                elif position_type == 'LONG' and quantity <= 0:
                    continue
                elif position_type == 'SHORT' and quantity >= 0:
                    continue

                exit_positions.append(position)

            if not exit_positions:
                return {'status': 'success', 'message': 'No positions to exit'}

            # Exit each position
            results = []
            for position in exit_positions:
                order_params = {
                    'exchange': position['exchange'],
                    'symbol': position['symbol'],
                    'quantity': abs(position['quantity']),
                    'transaction_type': 'SELL' if position['quantity'] > 0 else 'BUY',
                    'product_type': position['product'],
                    'order_type': 'MARKET'
                }

                # Place order to exit position
                result = self.place_order(order_params)
                results.append({
                    'symbol': position['symbol'],
                    'order_result': result
                })

            return {
                'status': 'success',
                'message': f'{len(results)} positions exited',
                'details': results
            }

        except Exception as e:
            self.logger.exception(f"Exception exiting positions: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_symbol_ltp(self, exchange, symbol):
        """
        Get last traded price for a symbol.

        Args:
            exchange (str): Exchange code
            symbol (str): Trading symbol

        Returns:
            float: Last traded price
        """
        try:
            # Try to get from cache first
            cache_key = f"{exchange}|{symbol}"
            if cache_key in self.ltp_cache:
                cache_time, ltp = self.ltp_cache[cache_key]
                if (datetime.now() - cache_time).seconds < 5:  # Use cache if less than 5 seconds old
                    return ltp

            # Get quote from API
            quote = self.get_quote(exchange, symbol)

            if 'status' in quote and quote['status'] == 'error':
                return None

            ltp = float(quote.get('ltp', 0))

            # Update cache
            self.ltp_cache[cache_key] = (datetime.now(), ltp)

            return ltp

        except Exception as e:
            self.logger.exception(f"Exception getting symbol LTP: {str(e)}")
            return None

    def generate_access_token(self):
        """
        Generate access token for API session.

        Returns:
            bool: Success status
        """
        try:
            response = self.api.get_api_session()

            if response and 'token' in response:
                self.access_token = response['token']
                self.logger.info("Generated new API access token")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error generating access token')
                self.logger.error(f"Failed to generate access token: {error_msg}")
                return False

        except Exception as e:
            self.logger.exception(f"Exception generating access token: {str(e)}")
            return False

    # Caching and maintenance methods

    def clear_cache(self):
        """
        Clear all caches.

        Returns:
            bool: Success status
        """
        try:
            self.order_cache.clear()
            self.ltp_cache.clear()
            self.logger.info("Cleared all caches")
            return True

        except Exception as e:
            self.logger.exception(f"Exception clearing cache: {str(e)}")
            return False

    def cleanup_old_cache_entries(self):
        """
        Clean up old cache entries.

        Returns:
            int: Number of entries removed
        """
        try:
            # Remove order cache entries older than 24 hours
            now = datetime.now()
            old_entries = []

            for order_id, order_data in self.order_cache.items():
                if 'last_updated' in order_data and (now - order_data['last_updated']).total_seconds() > 86400:
                    old_entries.append(order_id)

            for order_id in old_entries:
                del self.order_cache[order_id]

            # Clear LTP cache that's older than 10 minutes
            old_ltp_entries = []
            for key, (cache_time, _) in self.ltp_cache.items():
                if (now - cache_time).total_seconds() > 600:  # 10 minutes
                    old_ltp_entries.append(key)

            for key in old_ltp_entries:
                del self.ltp_cache[key]

            total_removed = len(old_entries) + len(old_ltp_entries)
            self.logger.info(f"Cleaned up {total_removed} old cache entries")
            return total_removed

        except Exception as e:
            self.logger.exception(f"Exception cleaning up cache: {str(e)}")
            return 0

    def enable_auto_refresh_token(self, interval_minutes=30):
        """
        Enable automatic token refresh at regular intervals.

        Args:
            interval_minutes (int): Interval in minutes for token refresh

        Returns:
            bool: Success status
        """
        try:
            if self.token_refresh_timer:
                self.token_refresh_timer.cancel()

            # Define the token refresh function
            def refresh_token():
                try:
                    self.generate_access_token()
                    # Schedule next refresh
                    self.token_refresh_timer = threading.Timer(interval_minutes * 60, refresh_token)
                    self.token_refresh_timer.daemon = True
                    self.token_refresh_timer.start()
                except Exception as e:
                    self.logger.exception(f"Failed in token refresh timer: {str(e)}")

            # Start the initial timer
            self.token_refresh_timer = threading.Timer(interval_minutes * 60, refresh_token)
            self.token_refresh_timer.daemon = True
            self.token_refresh_timer.start()

            self.logger.info(f"Enabled auto token refresh every {interval_minutes} minutes")
            return True

        except Exception as e:
            self.logger.exception(f"Exception enabling auto token refresh: {str(e)}")
            return False

    def disable_auto_refresh_token(self):
        """
        Disable automatic token refresh.

        Returns:
            bool: Success status
        """
        try:
            if self.token_refresh_timer:
                self.token_refresh_timer.cancel()
                self.token_refresh_timer = None

            self.logger.info("Disabled auto token refresh")
            return True

        except Exception as e:
            self.logger.exception(f"Exception disabling auto token refresh: {str(e)}")
            return False

    def setup_periodic_cache_cleanup(self, interval_hours=12):
        """
        Setup periodic cache cleanup.

        Args:
            interval_hours (int): Interval in hours for cache cleanup

        Returns:
            bool: Success status
        """
        try:
            if self.cache_cleanup_timer:
                self.cache_cleanup_timer.cancel()

            # Define the cache cleanup function
            def cleanup_cache():
                try:
                    self.cleanup_old_cache_entries()
                    # Schedule next cleanup
                    self.cache_cleanup_timer = threading.Timer(interval_hours * 3600, cleanup_cache)
                    self.cache_cleanup_timer.daemon = True
                    self.cache_cleanup_timer.start()
                except Exception as e:
                    self.logger.exception(f"Failed in cache cleanup timer: {str(e)}")

            # Start the initial timer
            self.cache_cleanup_timer = threading.Timer(interval_hours * 3600, cleanup_cache)
            self.cache_cleanup_timer.daemon = True
            self.cache_cleanup_timer.start()

            self.logger.info(f"Enabled periodic cache cleanup every {interval_hours} hours")
            return True

        except Exception as e:
            self.logger.exception(f"Exception setting up cache cleanup: {str(e)}")
            return False

    def disable_periodic_cache_cleanup(self):
        """
        Disable periodic cache cleanup.

        Returns:
            bool: Success status
        """
        try:
            if self.cache_cleanup_timer:
                self.cache_cleanup_timer.cancel()
                self.cache_cleanup_timer = None

            self.logger.info("Disabled periodic cache cleanup")
            return True

        except Exception as e:
            self.logger.exception(f"Exception disabling cache cleanup: {str(e)}")
            return False

    def get_account_positions_summary(self) -> Dict[str, Any]:
        """
        Retrieve a summary of all positions in the account.

        Returns:
            Dict[str, Any]: A dictionary containing position summary information.
        """
        try:
            response = self.api.get_positions()
            if response and 'stat' in response and response['stat'] == 'Ok':
                # Transform the API response into a standardized format
                positions = []
                for position in response.get('net', []):
                    positions.append({
                        'symbol': position.get('tsym', ''),
                        'exchange': position.get('exch', ''),
                        'product': position.get('prd', ''),
                        'quantity': int(position.get('netqty', 0)),
                        'average_price': float(position.get('avgprc', 0)),
                        'last_price': float(position.get('lp', 0)),
                        'pnl': float(position.get('rpnl', 0)),
                        'value': float(position.get('val', 0))
                    })

                return {
                    'status': 'success',
                    'positions': positions,
                    'total_pnl': sum(position['pnl'] for position in positions),
                    'position_count': len(positions)
                }
            else:
                self.logger.error(f"Failed to get positions: {response}")
                return {'status': 'error', 'message': response.get('emsg', 'Unknown error')}
        except Exception as e:
            self.logger.exception(f"Error getting account positions: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_execution_history(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Retrieve execution history for the specified time period.

        Args:
            start_time (Optional[datetime]): Start time for the history query. Defaults to beginning of current day.
            end_time (Optional[datetime]): End time for the history query. Defaults to current time.

        Returns:
            List[Dict[str, Any]]: A list of executions as dictionaries.
        """
        try:
            # Set default times if not provided
            if start_time is None:
                start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if end_time is None:
                end_time = datetime.now()

            # Format dates for API request
            from_date = start_time.strftime('%d-%m-%Y')
            to_date = end_time.strftime('%d-%m-%Y')

            # Call the API to get trade history
            response = self.api.get_trade_history(from_date=from_date, to_date=to_date)

            if response and 'stat' in response and response['stat'] == 'Ok':
                executions = []

                for trade in response.get('trades', []):
                    execution = {
                        'execution_id': trade.get('norenordno', ''),
                        'order_id': trade.get('orderno', ''),
                        'symbol': trade.get('tsym', ''),
                        'exchange': trade.get('exch', ''),
                        'transaction_type': trade.get('trantype', ''),
                        'quantity': int(trade.get('qty', 0)),
                        'price': float(trade.get('prc', 0)),
                        'execution_time': self._parse_datetime(trade.get('exch_tm', '')),
                        'product': trade.get('prd', ''),
                        'status': trade.get('status', '')
                    }
                    executions.append(execution)

                return executions
            else:
                self.logger.error(f"Failed to get execution history: {response}")
                return []
        except Exception as e:
            self.logger.exception(f"Error getting execution history: {str(e)}")
            return []

    def _parse_datetime(self, datetime_str: str) -> datetime:
        """
        Parse datetime string from Finvasia API.

        Args:
            datetime_str (str): Datetime string in Finvasia format.

        Returns:
            datetime: Parsed datetime object.
        """
        try:
            if datetime_str:
                # Handle various datetime formats from Finvasia
                formats = [
                    '%d-%m-%Y %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%d-%m-%Y'
                ]

                for fmt in formats:
                    try:
                        return datetime.strptime(datetime_str, fmt)
                    except ValueError:
                        continue

            return datetime.now()  # Return current time if parsing fails
        except Exception as e:
            self.logger.error(f"Error parsing datetime: {str(e)}")
            return datetime.now()

    def get_instrument_details(self, instrument: Instrument) -> Dict[str, Any]:
        """
        Retrieve detailed information about a specific instrument.

        Args:
            instrument (Instrument): The instrument object to query.

        Returns:
            Dict[str, Any]: A dictionary containing instrument details.
        """
        try:
            symbol = instrument.symbol
            exchange = instrument.exchange

            # Get instrument details from the API
            response = self.api.get_security_info(exchange=exchange, token=symbol)

            if response and 'stat' in response and response['stat'] == 'Ok':
                return {
                    'status': 'success',
                    'symbol': response.get('tsym', ''),
                    'exchange': response.get('exch', ''),
                    'instrument_type': response.get('instname', ''),
                    'lot_size': int(response.get('ls', 1)),
                    'tick_size': float(response.get('ti', 0.05)),
                    'expiry': response.get('exd', ''),
                    'trading_symbol': response.get('cname', ''),
                    'trading_session': response.get('trgcls', ''),
                    'price_precision': int(response.get('pp', 2)),
                    'upper_circuit': float(response.get('ulp', 0)),
                    'lower_circuit': float(response.get('llp', 0))
                }
            else:
                self.logger.error(f"Failed to get instrument details: {response}")
                return {
                    'status': 'error',
                    'message': response.get('emsg', 'Failed to retrieve instrument details'),
                    'symbol': instrument.symbol,
                    'exchange': instrument.exchange
                }
        except Exception as e:
            self.logger.exception(f"Error getting instrument details: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'symbol': instrument.symbol,
                'exchange': instrument.exchange
            }

    def get_market_data(self, instrument: Instrument, data_type: str, time_period: str = '1D') -> Dict[str, Any]:
        """
        Retrieve market data for a specific instrument.

        Args:
            instrument (Instrument): The instrument to query.
            data_type (str): Type of data to retrieve ('quote', 'ohlc', 'depth').
            time_period (str): Time period for historical data ('1D', '1W', '1M', etc.).

        Returns:
            Dict[str, Any]: A dictionary containing the requested market data.
        """
        try:
            symbol = instrument.symbol
            exchange = instrument.exchange

            if data_type.lower() == 'quote':
                response = self.api.get_quotes(exchange=exchange, token=symbol)

                if response and 'stat' in response and response['stat'] == 'Ok':
                    return {
                        'status': 'success',
                        'symbol': symbol,
                        'exchange': exchange,
                        'last_price': float(response.get('lp', 0)),
                        'change': float(response.get('chg', 0)),
                        'change_percent': float(response.get('chgp', 0)),
                        'open': float(response.get('o', 0)),
                        'high': float(response.get('h', 0)),
                        'low': float(response.get('l', 0)),
                        'close': float(response.get('c', 0)),
                        'volume': int(response.get('v', 0)),
                        'timestamp': self._parse_datetime(response.get('ltt', ''))
                    }
                else:
                    self.logger.error(f"Failed to get quotes: {response}")
                    return {'status': 'error', 'message': response.get('emsg', 'Failed to retrieve quotes')}

            elif data_type.lower() == 'ohlc':
                # Map time period to interval
                interval_map = {
                    '1D': '1',   # 1 day
                    '1W': '7',   # 1 week
                    '1M': '30',  # 1 month
                    '3M': '90',  # 3 months
                    '6M': '180', # 6 months
                    '1Y': '365'  # 1 year
                }
                interval = interval_map.get(time_period, '1')

                response = self.api.get_time_price_series(exchange=exchange, token=symbol, interval=interval)

                if response and isinstance(response, list) and len(response) > 0:
                    candles = []
                    for candle in response:
                        candles.append({
                            'time': self._parse_datetime(candle.get('time', '')),
                            'open': float(candle.get('into', 0)),
                            'high': float(candle.get('inth', 0)),
                            'low': float(candle.get('intl', 0)),
                            'close': float(candle.get('intc', 0)),
                            'volume': int(candle.get('v', 0))
                        })

                    return {
                        'status': 'success',
                        'symbol': symbol,
                        'exchange': exchange,
                        'interval': time_period,
                        'candles': candles
                    }
                else:
                    self.logger.error(f"Failed to get OHLC data: {response}")
                    return {'status': 'error', 'message': 'Failed to retrieve OHLC data'}

            elif data_type.lower() == 'depth':
                response = self.api.get_market_depth(exchange=exchange, token=symbol)

                if response and 'stat' in response and response['stat'] == 'Ok':
                    bids = []
                    asks = []

                    for i in range(1, 6):  # Finvasia typically provides 5 levels
                        bid_price = float(response.get(f'bp{i}', 0))
                        bid_qty = int(response.get(f'bq{i}', 0))
                        ask_price = float(response.get(f'sp{i}', 0))
                        ask_qty = int(response.get(f'sq{i}', 0))

                        if bid_price > 0:
                            bids.append({'price': bid_price, 'quantity': bid_qty})
                        if ask_price > 0:
                            asks.append({'price': ask_price, 'quantity': ask_qty})

                    return {
                        'status': 'success',
                        'symbol': symbol,
                        'exchange': exchange,
                        'bids': bids,
                        'asks': asks,
                        'timestamp': datetime.now()
                    }
                else:
                    self.logger.error(f"Failed to get market depth: {response}")
                    return {'status': 'error', 'message': response.get('emsg', 'Failed to retrieve market depth')}
            else:
                return {'status': 'error', 'message': f'Unsupported data_type: {data_type}'}
        except Exception as e:
            self.logger.exception(f"Error getting market data: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def get_trading_hours(self, instrument: Instrument) -> Dict[str, datetime]:
        """
        Retrieve trading hours for a specific instrument.

        Args:
            instrument (Instrument): The instrument to query.

        Returns:
            Dict[str, datetime]: A dictionary with market open and close times.
        """
        try:
            exchange = instrument.exchange

            # Get exchange details
            response = self.api.get_exchange_status(exchange=exchange)

            if response and 'stat' in response and response['stat'] == 'Ok':
                # Parse times from response
                now = datetime.now()
                today = now.date()

                # Default trading hours if not available from API
                default_hours = {
                    'NSE': {'open': '09:15', 'close': '15:30'},
                    'BSE': {'open': '09:15', 'close': '15:30'},
                    'NFO': {'open': '09:15', 'close': '15:30'},
                    'MCX': {'open': '09:00', 'close': '23:30'},
                    'CDS': {'open': '09:00', 'close': '17:00'}
                }

                market_times = default_hours.get(exchange, {'open': '09:15', 'close': '15:30'})

                # Try to get times from API response
                if 'sname' in response and 'mkt' in response:
                    market_status = response.get('mkt', 'closed')
                    if 'open' in market_status.lower():
                        # If we have open/close times in the response, use them
                        if 'open_time' in response and 'close_time' in response:
                            market_times = {
                                'open': response.get('open_time', market_times['open']),
                                'close': response.get('close_time', market_times['close'])
                            }

                # Convert string times to datetime objects
                open_time_str = market_times['open']
                close_time_str = market_times['close']

                open_hour, open_minute = map(int, open_time_str.split(':'))
                close_hour, close_minute = map(int, close_time_str.split(':'))

                market_open = datetime.combine(today, time(hour=open_hour, minute=open_minute))
                market_close = datetime.combine(today, time(hour=close_hour, minute=close_minute))

                return {
                    'market_open': market_open,
                    'market_close': market_close,
                    'status': response.get('mkt', 'unknown')
                }
            else:
                self.logger.error(f"Failed to get exchange status: {response}")
                # Return default trading hours
                now = datetime.now()
                today = now.date()
                return {
                    'market_open': datetime.combine(today, time(hour=9, minute=15)),
                    'market_close': datetime.combine(today, time(hour=15, minute=30)),
                    'status': 'unknown'
                }
        except Exception as e:
            self.logger.exception(f"Error getting trading hours: {str(e)}")
            now = datetime.now()
            today = now.date()
            return {
                'market_open': datetime.combine(today, time(hour=9, minute=15)),
                'market_close': datetime.combine(today, time(hour=15, minute=30)),
                'status': 'error'
            }

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

    def get_market_data_settings(self) -> dict:
        """
        Get settings for market data feed initialization.

        Returns:
            dict: Market data settings
        """
        market_data_settings = {
            'feed_type': 'finvasia_ws',
            'user_id': self.user_id,
            'password': self.password,
            'api_key': self.api_key,
            'imei': self.imei,
            'twofa': self.twofa,
            'vc': self.vc,
            'api_url': self.api_url,
            'websocket_url': self.ws_url,
            'debug': self.debug
        }

        # Add configuration from market_data section if available
        if hasattr(self, 'config') and self.config and 'market_data' in self.config:
            market_data_config = self.config.get('market_data', {})
            market_data_settings.update({
                'enable_persistence': market_data_config.get('enable_persistence', False),
                'persistence_path': market_data_config.get('persistence_path', './data/ticks'),
                'persistence_interval': market_data_config.get('persistence_interval', 300),
                'max_ticks_in_memory': market_data_config.get('max_ticks_in_memory', 10000)
            })

        return market_data_settings

    def get_instruments(self) -> List[Instrument]:
        """Get all available instruments."""
        return list(self._instrument_cache.values())

    # --- Placeholder implementations for abstract methods (handled by MarketDataFeed) ---
    def subscribe_market_data(self,
                             instrument: Instrument,
                             data_type: str,
                             callback: callable) -> str:
        """Placeholder: Subscriptions handled by MarketDataFeed."""
        self.logger.warning("subscribe_market_data called on BrokerInterface; this should ideally be handled by MarketDataFeed.")
        # Return a dummy subscription ID
        return f"sub_{instrument.symbol}_{data_type}"

    def unsubscribe_market_data(self, subscription_id: str) -> bool:
        """Placeholder: Unsubscriptions handled by MarketDataFeed."""
        self.logger.warning("unsubscribe_market_data called on BrokerInterface; this should ideally be handled by MarketDataFeed.")
        return True
    # ---------------------------------------------------------------------------

    def get_pending_paper_orders(self) -> Dict[str, Order]:
        """
        Get all pending paper orders.
        This is used by the PaperTradingSimulator to check for potential fills.

        Returns:
            Dict[str, Order]: Dictionary of order_id -> Order for pending orders
        """
        if not self.paper_trading:
            return {}
            
        return {
            order_id: order for order_id, order in self.simulated_orders.items()
            if order.status in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]
        }

