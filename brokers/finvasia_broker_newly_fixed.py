import logging
import json
import time
import threading
import hashlib
import queue
import pyotp # Ensure this is installed: pip install pyotp

from datetime import datetime, timedelta, time as dt_time # Added time import
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
from dateutil import parser
from NorenRestApiPy.NorenApi import NorenApi # Ensure this is correctly installed and available

from models.order import Order, OrderStatus # Assuming OrderType is part of models.order or utils.constants
from models.instrument import Instrument, InstrumentType # Assuming InstrumentType is part of models.instrument or utils.constants
from models.position import Position
from models.market_data import MarketData, Bar, Quote, Trade # Assuming these are defined
from core.event_manager import EventManager
from .broker_interface import BrokerInterface
from utils.constants import OrderType, OrderSide, Exchange # Import necessary enums

class FinvasiaBroker(BrokerInterface):
    """
    Finvasia broker implementation for the trading system.
    Integrates with Finvasia's NorenAPI for order execution and market data.
    """
    # Order type mappings between internal and Finvasia API
    ORDER_TYPE_MAPPING = {
        OrderType.MARKET: "MKT",
        OrderType.LIMIT: "LMT",
        OrderType.SL:"SL-LMT",
        OrderType.SL_M: "SL-MKT"
    }

    # Reverse mapping for order status from Finvasia to internal
    STATUS_MAPPING = {
        "OPEN": OrderStatus.PENDING, # Or OrderStatus.OPEN if you have that
        "COMPLETE": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.REJECTED,
        "PENDING": OrderStatus.PENDING, # Explicitly map Finvasia's PENDING
        "PARTIALLY FILLED": OrderStatus.PARTIALLY_FILLED, # Finvasia might use this
        "AMO RECEIVED": OrderStatus.PENDING, # Example for After Market Orders
        "TRIGGER_PENDING": OrderStatus.PENDING # For stop orders
        # Add other statuses as encountered from Finvasia API
    }

    # Exchange mapping (ensure keys match your internal Exchange enum values if used)
    EXCHANGE_MAPPING = {
        "NSE": "NSE",
        "BSE": "BSE",
        "NFO": "NFO",
        "BFO": "BFO",
        "MCX": "MCX",
        "CDS": "CDS"
    }

    def __init__(self, broker_name = "finvasia", config = None, api_url: str = None, user_id: str = None, password: str = None,
                 api_key: str = None, imei: str = None, twofa: str = None, vc: str = None, debug: bool = False,
                 enable_autoconnect: bool = True, paper_trading: bool = False, **kwargs):
        super().__init__()
        self.logger = logging.getLogger(__name__) # Corrected: Use __name__ for logger
        self.broker_name = broker_name
        self.enable_autoconnect = enable_autoconnect

        broker_settings = self._load_broker_config(
            config=config, user_id=user_id, password=password, api_key=api_key,
            imei=imei, twofa=twofa, vc=vc, api_url=api_url, debug=debug,
            paper_trading=paper_trading
        )

        self.api_url = broker_settings['api_url']
        self.ws_url = broker_settings['ws_url'] # Ensure this is part of broker_settings
        self.debug = broker_settings['debug']
        self.user_id = broker_settings['user_id']
        self.password = broker_settings['password']
        self.api_key = broker_settings['api_key']
        self.imei = broker_settings['imei']
        self.twofa = broker_settings['twofa'] # This should be the TOTP secret key
        self.vc = broker_settings['vc']
        self.paper_trading = broker_settings['paper_trading']

        self._connected = False
        self._session_token = None
        self._ws_connected = False # Added for WebSocket state
        self.token_refresh_timer = None
        self.cache_cleanup_timer = None

        self._instrument_cache: Dict[str, List[Instrument]] = {} # exchange -> list of instruments
        self._instrument_cache_time: Dict[str, datetime] = {} # exchange -> last update time
        self._instruments_df = None # Not used in current snippet, can be removed if not needed
        self._last_instrument_update = None # Not used
        self._instrument_cache_ttl = 24 * 60 * 60

        self._token_symbol_map: Dict[str, str] = {} # token -> "EXCHANGE:SYMBOL"
        self._symbol_token_map: Dict[str, str] = {} # "EXCHANGE:SYMBOL" -> token


        self.api = NorenApi(host=self.api_url, websocket=self.ws_url, eodhost=self.api_url) # eodhost might be needed

        self.event_manager = None
        self._market_data_buffer = queue.Queue()
        self._market_data_thread = None
        self.market_data_callbacks: Dict[str, List[Callable]] = {} # symbol_key -> list of callbacks
        self._subscriptions: Dict[str, bool] = {} # token -> True
        self._depth_subscriptions: Dict[str, bool] = {} # token -> True

        self.simulated_orders: Dict[str, Order] = {}
        self.next_sim_fill_id = 1
        
        # Order cache for quick reference
        self.order_cache: Dict[str, Dict[str, Any]] = {}
        # LTP cache for reducing API calls
        self.ltp_cache: Dict[str, Tuple[datetime, float]] = {}
        # Subscribed symbols for WebSocket
        self.subscribed_symbols: List[str] = []
        # WebSocket connection state
        self.websocket_connected: bool = False
        # Callback for order updates via WebSocket
        self.order_update_callback: Optional[Callable] = None
        # Global market data callback
        self._global_market_data_callback: Optional[Callable] = None


        if self.enable_autoconnect:
            self.connect()

    def _load_broker_config(self, config: Dict[str, Any], user_id: str = None, password: str = None,
                          api_key: str = None, imei: str = None, twofa: str = None, vc: str = None,
                          api_url: str = None, debug: bool = False, paper_trading: bool = False) -> Dict[str, Any]:
        # (Implementation from your provided code, assuming it's correct)
        # Ensure ws_url is also loaded or defaulted here.
        # Default ws_url if not found in config
        ws_url_default = "wss://api.shoonya.com/NorenWSTP/"
        loaded_ws_url = None

        if config and isinstance(config, dict):
            broker_connections = config.get('broker_connections', {})
            active_connection = broker_connections.get('active_connection')
            connections = broker_connections.get('connections', [])
            active_broker_config = next((conn for conn in connections if conn.get('connection_name') == active_connection), None)

            if active_broker_config:
                secrets_file = active_broker_config.get("secrets")
                if secrets_file:
                    try:
                        secrets = self._load_secrets(secrets_file)
                        user_id = user_id or secrets.get('user_id')
                        password = password or secrets.get('password')
                        api_key = api_key or secrets.get('api_key')
                        imei = imei or secrets.get('imei')
                        twofa = twofa or secrets.get('factor2') # Finvasia calls it factor2
                        vc = vc or secrets.get('vc')
                    except Exception as e:
                        self.logger.error(f"Failed to load secrets from {secrets_file}: {e}")

                api_config = active_broker_config.get("api", {})
                api_url = api_url or api_config.get("api_url")
                loaded_ws_url = api_config.get("ws_url") # Try to load ws_url
                debug = debug if debug is not None else api_config.get("debug", False)
                paper_trading = paper_trading if paper_trading is not None else api_config.get("paper_trading", False)
        
        return {
            'api_url': api_url or "https://api.shoonya.com/NorenWClientTP/",
            'ws_url': loaded_ws_url or ws_url_default, # Use loaded or default
            'debug': debug,
            'user_id': user_id,
            'password': password,
            'api_key': api_key,
            'imei': imei,
            'twofa': twofa, # This is the TOTP secret key
            'vc': vc,
            'paper_trading': paper_trading
        }

    def _load_secrets(self, secrets_file: str) -> dict:
        # (Implementation from your provided code)
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
            self.logger.error(f"Error loading secrets from {secrets_file}: {e}")
            raise
            
    def set_event_manager(self, event_manager: EventManager):
        self.event_manager = event_manager

    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        if self.is_connected():
            self.logger.info("Already connected to Finvasia API")
            return True
        try:
            if not self.twofa:
                self.logger.error("TOTP secret (twofa) is not configured.")
                return False
            
            totp_value = pyotp.TOTP(self.twofa).now()
            self.logger.debug(f"Attempting login with UserID: {self.user_id}, VC: {self.vc}, API Key: {self.api_key}, IMEI: {self.imei}, TOTP: {totp_value}")

            response = self.api.login(
                userid=self.user_id,
                password=self.password,
                twoFA=totp_value, # Use the generated TOTP value
                vendor_code=self.vc,
                api_secret=self.api_key,
                imei=self.imei
            )

            if response and response.get('stat') == 'Ok' and 'susertoken' in response:
                self._session_token = response['susertoken']
                self._connected = True
                self.logger.info("Successfully connected to Finvasia API.")
                # Initialize WebSocket after successful REST login
                if not self._init_websocket():
                     self.logger.warning("WebSocket initialization failed after login.")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown login error') if response else 'No response from login API'
                self.logger.error(f"Finvasia API connection failed: {error_msg}")
                self._connected = False
                return False
        except Exception as e:
            self.logger.error(f"Error connecting to Finvasia API: {e}", exc_info=True)
            self._connected = False
            return False

    def disconnect(self) -> bool:
        if not self.is_connected():
            self.logger.info("Already disconnected from Finvasia API.")
            return True
        try:
            if self._ws_connected:
                self.api.close_websocket() # Close WebSocket if NorenApi provides such method
                self._ws_connected = False
                self.logger.info("Finvasia WebSocket disconnected.")

            if self._market_data_thread and self._market_data_thread.is_alive():
                self._market_data_buffer.put(None)
                self._market_data_thread.join(timeout=2)
            
            # Call logout API if available and token exists
            if self._session_token:
                logout_response = self.api.logout()
                if logout_response and logout_response.get('stat') == 'Ok':
                    self.logger.info("Successfully logged out from Finvasia API.")
                else:
                    self.logger.warning(f"Finvasia API logout failed or no response: {logout_response}")
            
            self._session_token = None
            self._connected = False
            self._subscriptions.clear()
            self._depth_subscriptions.clear()
            self.logger.info("Finvasia Broker disconnected.")
            return True
        except Exception as e:
            self.logger.error(f"Error disconnecting from Finvasia API: {e}", exc_info=True)
            return False

    def _init_websocket(self) -> bool:
        """Initialize the WebSocket connection for streaming market data."""
        if not self._connected or not self._session_token:
            self.logger.error("Cannot initialize WebSocket: Not connected to REST API or no session token.")
            return False
        if self._ws_connected:
            self.logger.info("WebSocket already initialized and connected.")
            return True
        try:
            # Set callbacks before starting websocket
            self.api.on_disconnect = self._on_ws_disconnect
            self.api.on_error = self._on_ws_error
            self.api.on_open = self._on_ws_open
            # self.api.on_close = self._on_ws_close # NorenApi might not have on_close, on_disconnect is more common
            self.api.on_ticks = self._on_ticks_callback
            self.api.on_order_update = self._on_order_update # If NorenApi supports this

            # The NorenApi library typically handles the WebSocket connection internally
            # after a successful login. We might need to explicitly start it if the library requires.
            # Some versions of NorenApiPy might start WebSocket automatically or require a method call.
            # Assuming NorenApi handles this, or if there's a method like `start_websocket()`:
            
            # Example: self.api.start_websocket(order_update_callback=self._on_order_update, subscribe_callback=self._on_ticks_callback, socket_open_callback=self._on_ws_open)
            # If NorenApi automatically connects WebSocket on login, this explicit start might not be needed.
            # For now, we assume it's managed by NorenApi or started implicitly.
            # We will set _ws_connected = True in _on_ws_open.

            self._start_market_data_processor()
            self.logger.info("WebSocket initialization process started. Waiting for 'on_open' callback.")
            # The actual connection status (_ws_connected) will be set by the _on_ws_open callback.
            return True # Indicate that initialization attempt was made.

        except Exception as e:
            self.logger.error(f"Error initializing WebSocket: {e}", exc_info=True)
            self._ws_connected = False
            return False
            
    def _on_ws_open(self):
        self.logger.info("Finvasia WebSocket connection opened.")
        self._ws_connected = True
        # self._ws_retry_count = 0 # Reset retry count if you have one
        self._resubscribe_feeds()

    def _on_ws_disconnect(self, code=None, reason=None): # NorenApi might pass code and reason
        self.logger.warning(f"Finvasia WebSocket disconnected. Code: {code}, Reason: {reason}")
        self._ws_connected = False
        # Implement reconnection logic if needed

    def _on_ws_error(self, error_message):
        self.logger.error(f"Finvasia WebSocket error: {error_message}")
        # self._ws_connected = False # Error might not always mean disconnection

    def _on_ticks_callback(self, ticks):
        # self.logger.debug(f"Raw ticks received: {ticks}")
        if not self._market_data_buffer.full():
            self._market_data_buffer.put(ticks)
        else:
            self.logger.warning("Market data buffer full, dropping tick message.")

    def _on_order_update(self, order_data):
        self.logger.debug(f"Raw order update received: {order_data}")
        # TODO: Convert order_data to your internal OrderEvent/FillEvent and publish
        # This requires knowing the structure of order_data from Finvasia WS
        # Example:
        # if self.event_manager:
        #     event = self._convert_finvasia_order_update_to_event(order_data)
        #     if event:
        #         self.event_manager.publish(event)
        pass


    def _start_market_data_processor(self) -> None:
        if self._market_data_thread and self._market_data_thread.is_alive():
            return
        def process_data():
            while self._connected: # Loop while REST API is connected
                try:
                    data = self._market_data_buffer.get(timeout=1)
                    if data is None: break # Exit signal
                    self._process_market_data(data)
                    self._market_data_buffer.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing market data from buffer: {e}", exc_info=True)
        self._market_data_thread = threading.Thread(target=process_data, daemon=True, name="FinvasiaMarketDataProcessor")
        self._market_data_thread.start()

    def _process_market_data(self, data: Any) -> None:
        # Finvasia ticks often come as a dictionary
        if isinstance(data, dict):
            self._process_single_tick(data)
        elif isinstance(data, list): # Sometimes it might be a list of ticks
            for tick in data:
                if isinstance(tick, dict):
                    self._process_single_tick(tick)
        else:
            self.logger.warning(f"Received market data in unexpected format: {type(data)}")


    def _process_single_tick(self, tick: Dict[str, Any]) -> None:
        try:
            msg_type = tick.get('t')
            token = tick.get('tk')

            if not token:
                # self.logger.debug(f"Tick missing token: {tick}")
                return

            instrument_id_key = self._token_symbol_map.get(token) # "EXCHANGE:SYMBOL"
            if not instrument_id_key:
                # self.logger.debug(f"No symbol mapping for token {token}. Tick: {tick}")
                return

            instrument_obj = self.get_instrument_by_id(instrument_id_key) # Fetch full Instrument object
            if not instrument_obj:
                self.logger.warning(f"Instrument object not found for ID {instrument_id_key} from token {token}.")
                return

            if msg_type == 'tk' or msg_type == 'tf': # TouchLine quote or full quote (tf includes ohlc)
                quote = self._convert_tick_to_quote(tick, instrument_obj)
                if quote:
                    self._publish_market_data_event(instrument_obj, quote) # Pass Instrument object

                    # Call specific callbacks if registered
                    # symbol_key for callbacks should match how they were registered
                    if instrument_id_key in self.market_data_callbacks:
                        for callback in self.market_data_callbacks[instrument_id_key]:
                            try:
                                callback(quote) # Pass the Quote object
                            except Exception as e:
                                self.logger.error(f"Error in specific market data callback for {instrument_id_key}: {e}", exc_info=True)
            # Handle other message types like 'dk' (depth) if needed
        except Exception as e:
            self.logger.error(f"Error processing single tick: {tick}. Error: {e}", exc_info=True)


    def _convert_tick_to_quote(self, tick: Dict[str, Any], instrument: Instrument) -> Optional[Quote]:
        try:
            # Timestamp from tick ('ft' - feed time, or 'ltt' - last trade time)
            # Finvasia feed time 'ft' is usually preferred for quotes. 'ltt' for trades.
            # Convert Finvasia timestamp (seconds since epoch) to datetime
            ts_value = tick.get('ft') or tick.get('ltt')
            timestamp = datetime.fromtimestamp(int(ts_value)) if ts_value else datetime.now()

            quote = Quote(
                instrument=instrument,
                last_price=float(tick['lp']) if 'lp' in tick else None,
                last_quantity=int(tick['ltq']) if 'ltq' in tick else None,
                volume=int(tick['v']) if 'v' in tick else None,
                bid=float(tick['bp']) if 'bp' in tick else None,
                ask=float(tick['sp']) if 'sp' in tick else None,
                bid_size=int(tick['bq']) if 'bq' in tick else None,
                ask_size=int(tick['sq']) if 'sq' in tick else None,
                open_price=float(tick['o']) if 'o' in tick else None,
                high_price=float(tick['h']) if 'h' in tick else None,
                low_price=float(tick['l']) if 'l' in tick else None,
                close_price=float(tick['c']) if 'c' in tick else None, # Previous day close
                timestamp=timestamp
            )
            # Add other fields like 'oi' (open interest) if available and needed
            if 'oi' in tick:
                quote.open_interest = int(tick['oi'])
            
            # Add option greeks if applicable (requires underlying price, vol, etc.)
            if instrument.instrument_type == InstrumentType.OPTION:
                 # Placeholder for greeks; actual calculation needs more data
                quote.greeks = self._calculate_option_greeks(instrument, quote.last_price if quote.last_price else 0.0)


            return quote
        except Exception as e:
            self.logger.error(f"Error converting tick to quote for {instrument.instrument_id if instrument else 'Unknown'}: {tick}. Error: {e}", exc_info=True)
            return None

    def _calculate_option_greeks(self, instrument: Instrument, option_price: float) -> Dict[str, float]:
        # Placeholder: Actual greeks calculation requires an options pricing model (e.g., Black-Scholes)
        # and inputs like underlying price, volatility, time to expiry, risk-free rate.
        # This is a simplified stub.
        # self.logger.debug(f"Calculating placeholder greeks for {instrument.symbol}")
        return {
            'delta': 0.5, 'gamma': 0.05, 'theta': -0.01, 'vega': 0.1, 'rho': 0.02
        }

    def _publish_market_data_event(self, instrument: Instrument, quote: Quote) -> None:
        if not self.event_manager:
            return
        try:
            from models.events import MarketDataEvent, EventType # Local import
            from utils.constants import MarketDataType # Local import

            # Prepare data dictionary for MarketDataEvent
            event_data = {
                MarketDataType.LAST_PRICE.value: quote.last_price,
                MarketDataType.LAST_QUANTITY.value: quote.last_quantity,
                MarketDataType.VOLUME.value: quote.volume,
                MarketDataType.BID.value: quote.bid,
                MarketDataType.ASK.value: quote.ask,
                MarketDataType.BID_SIZE.value: quote.bid_size,
                MarketDataType.ASK_SIZE.value: quote.ask_size,
                MarketDataType.OPEN.value: quote.open_price,
                MarketDataType.HIGH.value: quote.high_price,
                MarketDataType.LOW.value: quote.low_price,
                MarketDataType.CLOSE.value: quote.close_price, # Previous close
            }
            if hasattr(quote, 'open_interest') and quote.open_interest is not None:
                 event_data[MarketDataType.OPEN_INTEREST.value] = quote.open_interest
            if hasattr(quote, 'greeks') and quote.greeks:
                 event_data[MarketDataType.GREEKS.value] = quote.greeks


            md_event = MarketDataEvent(
                event_type=EventType.MARKET_DATA,
                instrument=instrument, # Pass the full Instrument object
                exchange=instrument.exchange.value if hasattr(instrument.exchange, 'value') else str(instrument.exchange), # Ensure string
                data_type=MarketDataType.QUOTE, # Assuming it's a quote
                data=event_data,
                timestamp=int(quote.timestamp.timestamp() * 1000) # Convert datetime to int ms
            )
            self.event_manager.publish(md_event)
        except Exception as e:
            self.logger.error(f"Error publishing market data event for {instrument.instrument_id}: {e}", exc_info=True)


    def place_order(self, order: Order) -> Optional[str]:
        """
        Place a new order with the broker.
        Args:
            order: Order object with details.
        Returns:
            Broker's order ID if successful, None otherwise.
        """
        if self.paper_trading:
            order_id = order.order_id or str(uuid.uuid4()) # Ensure order_id exists
            order.status = OrderStatus.PENDING # Simulate pending status
            order.broker_order_id = f"PAPER_{order_id}"
            self.simulated_orders[order_id] = order
            self.logger.info(f"Paper Trading: Order {order_id} for {order.quantity} of {order.instrument_id} accepted.")
            # Simulate immediate acceptance for paper trading by publishing an OrderEvent
            if self.event_manager:
                from models.events import OrderEvent, EventType
                order_event = OrderEvent(
                    event_type=EventType.ORDER,
                    timestamp=int(datetime.now().timestamp() * 1000),
                    order_id=order_id,
                    broker_order_id=order.broker_order_id,
                    symbol=order.symbol, # Assuming Order has symbol
                    exchange=order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange),
                    side=order.side,
                    quantity=order.quantity,
                    order_type=order.order_type,
                    status=OrderStatus.PENDING, # Initial status
                    price=order.price,
                    strategy_id=order.strategy_id
                )
                self.event_manager.publish(order_event)
            return order_id


        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API. Cannot place order.")
            return None

        try:
            # FIX for Error 2: Use order.exchange and order.symbol directly
            raw_exchange = order.exchange
            if hasattr(raw_exchange, 'value'): # If exchange is an Enum
                exchange_str = raw_exchange.value
            else:
                exchange_str = str(raw_exchange) # If it's already a string

            # Assuming order.symbol or order.instrument_id holds the trading symbol like 'SENSEX2552082200CE'
            # and does NOT contain the exchange prefix.
            # The Order model should clarify which attribute holds the pure trading symbol.
            # Let's assume order.symbol is the pure trading symbol.
            trading_symbol_str = order.symbol
            if not trading_symbol_str and hasattr(order, 'instrument_id'): # Fallback if symbol is empty but instrument_id exists
                # If instrument_id might contain "EXCHANGE:SYMBOL", parse it
                if ':' in order.instrument_id:
                    parts = order.instrument_id.split(':', 1)
                    if exchange_str.upper() != parts[0].upper(): # Consistency check
                        self.logger.warning(f"Order exchange '{exchange_str}' differs from instrument_id prefix '{parts[0]}'. Using order.exchange.")
                    trading_symbol_str = parts[1]
                else:
                    trading_symbol_str = order.instrument_id


            if not trading_symbol_str:
                self.logger.error(f"Trading symbol is empty for order {order.order_id}. Cannot place order.")
                return None


            # Map internal OrderType enum to Finvasia's string representation
            api_order_type = self.ORDER_TYPE_MAPPING.get(order.order_type)
            if not api_order_type:
                self.logger.error(f"Unsupported order type: {order.order_type} for Finvasia.")
                return None

            # Determine buy_or_sell based on OrderSide enum
            if order.side == OrderSide.BUY:
                buy_or_sell = "B"
            elif order.side == OrderSide.SELL:
                buy_or_sell = "S"
            else:
                self.logger.error(f"Invalid order side: {order.side}")
                return None
            
            # Product type: C (CNC/Cash & Carry for equity), M (NRML for F&O), I (MIS for intraday)
            # This should be part of the Order object or strategy configuration.
            product_type = getattr(order, 'product_type', 'I') # Default to Intraday (MIS)
            if exchange_str in ["NFO", "BFO", "MCX", "CDS"] and product_type == "C":
                product_type = "M" # NRML for derivatives
            elif exchange_str in ["NSE", "BSE"] and product_type not in ["C", "I"]: # Equity
                 product_type = "C" # Default to CNC for equity if not MIS


            params = {
                "uid": self.user_id,
                "actid": self.user_id, # Often same as uid
                "exch": exchange_str,
                "tsym": trading_symbol_str,
                "qty": str(int(order.quantity)), # Finvasia expects quantity as string
                "prc": str(float(order.price)) if order.price is not None else "0", # String, 0 for MKT
                "dscqty": "0", # Disclosed quantity
                "prd": product_type, 
                "trantype": buy_or_sell,
                "prctyp": api_order_type,
                "ret": getattr(order, 'time_in_force', 'DAY'), # Retention: DAY, IOC, EOS
                "remarks": getattr(order, 'tag', self.broker_name), # Optional remarks
                "ordersource": "API" # Added for Finvasia
            }

            if order.order_type in [OrderType.SL_M, OrderType.SL] and order.trigger_price is not None:
                params["trgprc"] = str(float(order.trigger_price))
            
            # For STOP_LIMIT, price is the limit price, trigger_price is the stop price
            # For STOP (SL-MKT), price should be 0, trigger_price is the stop price

            self.logger.debug(f"Placing order with params: {params}")
            response = self.api.place_order(**params)
            self.logger.debug(f"Finvasia place_order response: {response}")

            if response and response.get('stat') == 'Ok' and response.get('norenordno'):
                broker_order_id = response['norenordno']
                self.logger.info(f"Order placed successfully with Finvasia. Broker Order ID: {broker_order_id}, Client Order ID: {order.order_id}")
                # Update order with broker_order_id (optional, if your Order model supports it)
                if hasattr(order, 'broker_order_id'):
                    order.broker_order_id = broker_order_id
                return broker_order_id
            else:
                error_msg = response.get('emsg', 'Unknown error from Finvasia API during order placement.')
                self.logger.error(f"Finvasia order placement failed for client order {order.order_id}: {error_msg}. Params: {params}")
                return None

        except Exception as e:
            self.logger.error(f"Exception placing order {order.order_id} with Finvasia: {e}", exc_info=True)
            return None


    def modify_order(self, order_id: str, price: Optional[float] = None, quantity: Optional[int] = None,
                     trigger_price: Optional[float] = None, order_type: Optional[OrderType] = None,
                     book_loss_price: Optional[float]=None, book_profit_price: Optional[float]=None,
                     trail_price: Optional[float]=None) -> bool:
        """
        Modify an existing order.
        `order_id` here is the broker's order ID (norenordno).
        """
        if self.paper_trading:
            # Logic for paper trading modification
            if order_id in self.simulated_orders: # Assuming order_id is client_order_id for simulated
                sim_order = self.simulated_orders[order_id]
                if price is not None: sim_order.price = price
                if quantity is not None: sim_order.quantity = quantity
                if trigger_price is not None: sim_order.trigger_price = trigger_price
                if order_type is not None: sim_order.order_type = order_type
                self.logger.info(f"Paper Trading: Order {order_id} modified.")
                return True
            self.logger.warning(f"Paper Trading: Order {order_id} not found for modification.")
            return False

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API. Cannot modify order.")
            return False

        try:
            # Fetch existing order details to get necessary parameters like exchange, symbol
            # Finvasia's modify_order requires 'norenordno'
            # We need to ensure the order_id passed is the 'norenordno'
            
            # Minimal params for modify_order
            params = {"norenordno": order_id} # Finvasia uses 'norenordno'
            
            # Finvasia modify API might require original qty if not changing, and new type if changing
            # This is a simplified version. Check Finvasia docs for exact requirements.
            # For example, to change price, you might need to provide new quantity too (even if same).

            if quantity is not None:
                params["qty"] = str(int(quantity))
            
            if price is not None: # This is the new limit price
                params["prc"] = str(float(price))

            if trigger_price is not None: # This is the new trigger price
                params["trgprc"] = str(float(trigger_price))
            
            if order_type is not None:
                api_order_type = self.ORDER_TYPE_MAPPING.get(order_type)
                if not api_order_type:
                    self.logger.error(f"Cannot modify order: Unsupported new order type {order_type}")
                    return False
                params["prctyp"] = api_order_type


            self.logger.debug(f"Modifying order {order_id} with params: {params}")
            response = self.api.modify_order(**params)
            self.logger.debug(f"Finvasia modify_order response: {response}")

            if response and response.get('stat') == 'Ok' and response.get('result') == order_id: # Finvasia often returns order_id in 'result' on success
                self.logger.info(f"Order {order_id} modified successfully with Finvasia.")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error from Finvasia API during order modification.')
                self.logger.error(f"Finvasia order modification failed for {order_id}: {error_msg}. Params: {params}")
                return False
        except Exception as e:
            self.logger.error(f"Exception modifying order {order_id} with Finvasia: {e}", exc_info=True)
            return False

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.
        `order_id` here is the broker's order ID (norenordno).
        """
        if self.paper_trading:
            # Logic for paper trading cancellation
            if order_id in self.simulated_orders: # Assuming order_id is client_order_id for simulated
                self.simulated_orders[order_id].status = OrderStatus.CANCELED
                self.logger.info(f"Paper Trading: Order {order_id} cancelled.")
                return True
            self.logger.warning(f"Paper Trading: Order {order_id} not found for cancellation.")
            return False

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API. Cannot cancel order.")
            return False
        try:
            params = {"norenordno": order_id} # Finvasia uses 'norenordno'
            self.logger.debug(f"Cancelling order {order_id} with params: {params}")
            response = self.api.cancel_order(**params)
            self.logger.debug(f"Finvasia cancel_order response: {response}")

            if response and response.get('stat') == 'Ok' and response.get('result') == order_id:
                self.logger.info(f"Order {order_id} cancelled successfully with Finvasia.")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error from Finvasia API during order cancellation.')
                self.logger.error(f"Finvasia order cancellation failed for {order_id}: {error_msg}. Params: {params}")
                return False
        except Exception as e:
            self.logger.error(f"Exception cancelling order {order_id} with Finvasia: {e}", exc_info=True)
            return False
            
    # --- Instrument and Market Data Methods ---
    def get_instrument_by_id(self, instrument_id: str) -> Optional[Instrument]:
        """
        Retrieves an instrument by its unique ID (e.g., "EXCHANGE:SYMBOL").
        Assumes instruments are loaded into _instrument_cache via get_instruments_for_exchange.
        """
        exchange_code, symbol_code = instrument_id.split(":", 1)
        if exchange_code in self._instrument_cache:
            for inst in self._instrument_cache[exchange_code]:
                if inst.symbol == symbol_code: # Assuming inst.symbol is the part after "EXCHANGE:"
                    return inst
        # If not found, try to load it (could be slow)
        self.logger.debug(f"Instrument {instrument_id} not in cache. Attempting to fetch for exchange {exchange_code}.")
        if self.get_instruments_for_exchange(exchange_code): # This will populate cache
             if exchange_code in self._instrument_cache:
                for inst in self._instrument_cache[exchange_code]:
                    if inst.symbol == symbol_code:
                        return inst
        self.logger.warning(f"Instrument {instrument_id} not found after attempting fetch.")
        return None


    def get_instruments_for_exchange(self, exchange: str) -> List[Instrument]:
        """
        Get list of tradable instruments for a specific exchange.
        Uses caching to avoid repeated API calls.
        """
        # Check cache first
        if exchange in self._instrument_cache and \
           exchange in self._instrument_cache_time and \
           (datetime.now() - self._instrument_cache_time[exchange]).total_seconds() < self._instrument_cache_ttl:
            self.logger.debug(f"Returning instruments for {exchange} from cache.")
            return self._instrument_cache[exchange]

        if not self.is_connected():
            self.logger.error(f"Cannot fetch instruments for {exchange}: Not connected.")
            return []
        try:
            self.logger.info(f"Fetching instruments for exchange: {exchange} from API...")
            # Finvasia's get_security_info might require specific exchange mapping
            # Or, if it downloads a master file, that needs to be handled.
            # For Shoonya, master contracts are usually downloaded as CSV/TXT.
            # Let's assume self.api.get_security_info(exchange=exchange) works or a similar method.
            
            # Example: If master contracts need to be downloaded and parsed
            # response = self.api.download_master_contracts(exchange) 
            # instruments_data = self._parse_master_contracts(response, exchange)
            
            # Using get_security_info as a placeholder for fetching.
            # This might return a lot of data.
            # A real implementation might involve downloading a master contract file.
            api_response = self.api.get_security_info(exchange=exchange)
            
            instruments = []
            if isinstance(api_response, list): # Assuming it returns a list of dicts
                for instr_data in api_response:
                    try:
                        # Map Finvasia instrument data to internal Instrument model
                        symbol_name = instr_data.get('tsym')
                        if not symbol_name: continue

                        instrument_id = f"{instr_data.get('exch','UNKNOWN')}:{symbol_name}"
                        
                        # Determine InstrumentType and AssetClass (this needs careful mapping)
                        # Finvasia 'instname' can be 'OPTIDX', 'FUTIDX', 'OPTSTK', 'FUTSTK', 'EQ', etc.
                        inst_type_str = instr_data.get('instname', 'EQ')
                        asset_cls = AssetClass.EQUITY # Default
                        inst_type = InstrumentType.EQUITY # Default

                        if 'OPT' in inst_type_str:
                            inst_type = InstrumentType.OPTION
                            asset_cls = AssetClass.OPTIONS
                        elif 'FUT' in inst_type_str:
                            inst_type = InstrumentType.FUTURE
                            asset_cls = AssetClass.FUTURES
                        elif inst_type_str == 'EQ':
                            inst_type = InstrumentType.EQUITY
                            asset_cls = AssetClass.EQUITY
                        # Add more mappings as needed (INDEX, CURRENCY, COMMODITY)

                        expiry_date = None
                        if instr_data.get('exd'):
                            try:
                                expiry_date = datetime.strptime(instr_data['exd'], '%d%b%Y').date() # Example format ddMMMyyyy
                            except ValueError:
                                try:
                                     expiry_date = parser.parse(instr_data['exd']).date()
                                except:
                                     self.logger.debug(f"Could not parse expiry date {instr_data['exd']} for {symbol_name}")


                        instrument = Instrument(
                            instrument_id=instrument_id,
                            symbol=symbol_name,
                            exchange=Exchange(instr_data.get('exch')) if instr_data.get('exch') else Exchange.NSE,
                            name=instr_data.get('cname', symbol_name),
                            instrument_type=inst_type,
                            asset_class=asset_cls,
                            lot_size=int(instr_data.get('ls', 1)),
                            tick_size=float(instr_data.get('ti', 0.05)),
                            expiry_date=expiry_date,
                            strike_price=float(instr_data.get('strprc')) if instr_data.get('strprc') else None,
                            option_type=instr_data.get('opttype') if inst_type == InstrumentType.OPTION else None, # 'CE' or 'PE'
                            token=instr_data.get('token') # Finvasia specific token
                        )
                        instruments.append(instrument)
                        # Update token maps
                        if instrument.token:
                            self._token_symbol_map[instrument.token] = instrument.instrument_id
                            self._symbol_token_map[instrument.instrument_id] = instrument.token

                    except Exception as e_inner:
                        self.logger.error(f"Error processing instrument data: {instr_data}. Error: {e_inner}", exc_info=True)
            
            elif api_response is None or (isinstance(api_response, dict) and api_response.get('stat') != 'Ok'):
                 self.logger.error(f"Failed to fetch instruments for {exchange}. Response: {api_response}")
                 return []


            self._instrument_cache[exchange] = instruments
            self._instrument_cache_time[exchange] = datetime.now()
            self.logger.info(f"Fetched and cached {len(instruments)} instruments for {exchange}.")
            return instruments

        except Exception as e:
            self.logger.error(f"Error fetching instruments for {exchange}: {e}", exc_info=True)
            return []

    def _resubscribe_feeds(self) -> None:
        if not self._ws_connected:
            self.logger.warning("Cannot resubscribe market data: WebSocket not connected.")
            return
        try:
            # Resubscribe to market data (ticks)
            if self._subscriptions:
                tokens_to_subscribe = [token for token, is_subscribed in self._subscriptions.items() if is_subscribed]
                if tokens_to_subscribe:
                    # Finvasia expects tokens in "EXCHANGE|TOKEN" format for subscription
                    # However, NorenApi.subscribe might just take list of tokens directly.
                    # Let's assume NorenApi.subscribe takes a list of "EXCHANGE|TOKEN" or just "TOKEN"
                    # We stored only tokens in _subscriptions. We need to reconstruct "EXCHANGE|TOKEN".
                    
                    # This part needs to be verified with NorenApiPy's subscribe method.
                    # If it takes `exch|token` strings:
                    # sub_list = [f"{self._token_exchange_map[token]}|{token}" for token in tokens_to_subscribe if token in self._token_exchange_map]
                    
                    # If it takes just tokens (and exchange is implicit or set elsewhere):
                    # For NorenApi, it's usually `k` parameter with `EXCHANGE|TOKEN` pairs.
                    # The `api.subscribe` method in NorenApiPy might abstract this.
                    # Let's assume we need to send the raw message if `api.subscribe` is not flexible.
                    
                    # NorenApi.subscribe(instrument_list) where instrument_list is like ['NSE|22', 'NFO|35045']
                    # Our _token_symbol_map has token -> "EXCHANGE:SYMBOL". We need EXCHANGE|TOKEN
                    
                    formatted_sub_list = []
                    for token in tokens_to_subscribe:
                        instrument_id = self._token_symbol_map.get(token)
                        if instrument_id:
                            exchange_part, _ = instrument_id.split(":",1)
                            formatted_sub_list.append(f"{exchange_part}|{token}")
                    
                    if formatted_sub_list:
                        self.logger.info(f"Resubscribing to {len(formatted_sub_list)} tick feeds: {formatted_sub_list}")
                        self.api.subscribe(formatted_sub_list) # Or appropriate method
                    else:
                        self.logger.info("No valid tick feeds to resubscribe.")


            # Resubscribe to market depth if needed (similar logic)
            if self._depth_subscriptions:
                depth_tokens_to_subscribe = [token for token, is_subscribed in self._depth_subscriptions.items() if is_subscribed]
                # Format and subscribe for depth
                # self.api.subscribe_depth(formatted_depth_list) # Example
                self.logger.info(f"Resubscribed to {len(depth_tokens_to_subscribe)} depth feeds.")

        except Exception as e:
            self.logger.error(f"Error resubscribing to feeds: {e}", exc_info=True)


    def subscribe_market_data(self, instrument: Instrument, data_type: str = 'quote', callback: Optional[Callable] = None) -> bool:
        """
        Subscribe to market data for a given instrument.
        `data_type` can be 'quote', 'depth'.
        `callback` is specific to this subscription.
        """
        if not self.is_connected() or not self._ws_connected:
            self.logger.error(f"Cannot subscribe to {instrument.instrument_id}: Not connected or WebSocket not ready.")
            return False

        token = getattr(instrument, 'token', None) or self._symbol_token_map.get(instrument.instrument_id)
        if not token:
            self.logger.error(f"No token found for instrument {instrument.instrument_id}. Cannot subscribe.")
            # Attempt to fetch instruments for the exchange to populate token map
            self.get_instruments_for_exchange(instrument.exchange.value if hasattr(instrument.exchange, 'value') else str(instrument.exchange))
            token = self._symbol_token_map.get(instrument.instrument_id) # Try again
            if not token:
                self.logger.error(f"Still no token for {instrument.instrument_id} after refresh. Subscription failed.")
                return False
        
        # Store token to exchange mapping if not already present (for resubscription)
        # if token not in self._token_exchange_map:
        #    self._token_exchange_map[token] = instrument.exchange.value if hasattr(instrument.exchange, 'value') else str(instrument.exchange)


        # Construct the subscription key for NorenApi (usually "EXCHANGE|TOKEN")
        exchange_str = instrument.exchange.value if hasattr(instrument.exchange, 'value') else str(instrument.exchange)
        sub_key = f"{exchange_str}|{token}"

        try:
            if data_type == 'quote':
                if not self._subscriptions.get(token): # Check if already subscribed by token
                    self.api.subscribe(instrument_list=[sub_key]) # NorenApi takes list of "EXCHANGE|TOKEN"
                    self._subscriptions[token] = True
                    self.logger.info(f"Subscribed to quote for {instrument.instrument_id} (Token: {token}, SubKey: {sub_key}).")
                else:
                    self.logger.debug(f"Already subscribed to quote for {instrument.instrument_id} (Token: {token}).")
            elif data_type == 'depth':
                if not self._depth_subscriptions.get(token):
                    # NorenApi might have a specific method for depth or use a different 't' type in send_ws_message
                    # self.api.subscribe_depth([sub_key]) # Hypothetical
                    # Or, if using send_ws_message:
                    # self.api.send_ws_message({"t": "d", "k": sub_key, "act": "s"}) # 's' for subscribe
                    self.logger.warning(f"Depth subscription for {instrument.instrument_id} not fully implemented in this example.")
                    self._depth_subscriptions[token] = True # Mark as subscribed for now
                else:
                    self.logger.debug(f"Already subscribed to depth for {instrument.instrument_id} (Token: {token}).")

            else:
                self.logger.warning(f"Unsupported data_type '{data_type}' for subscription.")
                return False

            # Register specific callback if provided
            if callback and callable(callback):
                instrument_id_key = instrument.instrument_id # "EXCHANGE:SYMBOL"
                if instrument_id_key not in self.market_data_callbacks:
                    self.market_data_callbacks[instrument_id_key] = []
                if callback not in self.market_data_callbacks[instrument_id_key]:
                    self.market_data_callbacks[instrument_id_key].append(callback)
            return True
        except Exception as e:
            self.logger.error(f"Error subscribing to {data_type} for {instrument.instrument_id}: {e}", exc_info=True)
            return False

    def unsubscribe_market_data(self, instrument: Instrument, data_type: str = 'quote') -> bool:
        if not self.is_connected() or not self._ws_connected:
            self.logger.error(f"Cannot unsubscribe from {instrument.instrument_id}: Not connected or WebSocket not ready.")
            return False

        token = getattr(instrument, 'token', None) or self._symbol_token_map.get(instrument.instrument_id)
        if not token:
            self.logger.error(f"No token found for instrument {instrument.instrument_id}. Cannot unsubscribe.")
            return False
        
        exchange_str = instrument.exchange.value if hasattr(instrument.exchange, 'value') else str(instrument.exchange)
        unsub_key = f"{exchange_str}|{token}"

        try:
            if data_type == 'quote':
                if self._subscriptions.get(token):
                    self.api.unsubscribe(instrument_list=[unsub_key]) # NorenApi takes list
                    self._subscriptions[token] = False # Mark as unsubscribed
                    self.logger.info(f"Unsubscribed from quote for {instrument.instrument_id} (Token: {token}).")
                else:
                    self.logger.debug(f"Not currently subscribed to quote for {instrument.instrument_id} (Token: {token}).")
            elif data_type == 'depth':
                 if self._depth_subscriptions.get(token):
                    # self.api.unsubscribe_depth([unsub_key]) # Hypothetical
                    # Or, if using send_ws_message:
                    # self.api.send_ws_message({"t": "d", "k": unsub_key, "act": "u"}) # 'u' for unsubscribe
                    self.logger.warning(f"Depth unsubscription for {instrument.instrument_id} not fully implemented.")
                    self._depth_subscriptions[token] = False
                 else:
                    self.logger.debug(f"Not currently subscribed to depth for {instrument.instrument_id} (Token: {token}).")

            # Remove callbacks associated with this instrument (optional, or manage callbacks separately)
            # if instrument.instrument_id in self.market_data_callbacks:
            #     del self.market_data_callbacks[instrument.instrument_id]
            return True
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {data_type} for {instrument.instrument_id}: {e}", exc_info=True)
            return False
            
    # Other methods (get_order_status, get_orders, get_positions, etc.) should be implemented
    # by calling the respective self.api methods and converting responses.
    # Ensure they handle paper_trading mode appropriately.

    # Example for get_order_status
    def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a specific order.
        `order_id` is the broker's order ID (norenordno).
        """
        if self.paper_trading:
            if order_id in self.simulated_orders: # If order_id is client_order_id
                order = self.simulated_orders[order_id]
                return {
                    'order_id': order.order_id, # client order id
                    'broker_order_id': getattr(order, 'broker_order_id', None),
                    'status': order.status,
                    'filled_quantity': getattr(order, 'filled_quantity', 0),
                    'average_price': getattr(order, 'average_fill_price', 0.0),
                    # Add other relevant fields
                }
            # If order_id is broker_order_id (PAPER_...)
            for _, sim_order in self.simulated_orders.items():
                if getattr(sim_order, 'broker_order_id', None) == order_id:
                     return {
                        'order_id': sim_order.order_id,
                        'broker_order_id': order_id,
                        'status': sim_order.status,
                        'filled_quantity': getattr(sim_order, 'filled_quantity', 0),
                        'average_price': getattr(sim_order, 'average_fill_price', 0.0),
                    }
            return None

        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API.")
            return None
        try:
            # NorenApi single_order_history returns a list of status updates for the order
            response = self.api.single_order_history(orderno=order_id)
            if response and isinstance(response, list) and len(response) > 0:
                latest_status_info = response[0] # The first item is usually the latest status
                
                internal_status_str = latest_status_info.get('status', '').upper()
                # Finvasia might use "AMO RECEIVED", "PENDING", "OPEN", "TRIGGER_PENDING" for pending states.
                # "COMPLETE" for filled, "REJECTED", "CANCELED".
                # "PARTIALLY FILLED"
                
                internal_status = self.STATUS_MAPPING.get(internal_status_str, OrderStatus.UNKNOWN)

                return {
                    'order_id': latest_status_info.get('norenordno'), # This is broker_order_id
                    'client_order_id': latest_status_info.get('remarks'), # If client_order_id stored in remarks
                    'status': internal_status,
                    'symbol': latest_status_info.get('tsym'),
                    'exchange': latest_status_info.get('exch'),
                    'quantity': int(latest_status_info.get('qty', 0)),
                    'filled_quantity': int(latest_status_info.get('fillshares', 0)),
                    'average_price': float(latest_status_info.get('avgprc', 0.0)),
                    'price': float(latest_status_info.get('prc', 0.0)),
                    'trigger_price': float(latest_status_info.get('trgprc', 0.0)),
                    'order_type': latest_status_info.get('prctyp'), # MKT, LMT, SL-LMT, SL-MKT
                    'side': 'BUY' if latest_status_info.get('trantype') == 'B' else 'SELL',
                    'timestamp': latest_status_info.get('norentm'), # Order entry time by broker
                    'message': latest_status_info.get('rejreason', '') # Rejection reason
                }
            else:
                self.logger.warning(f"Could not get status for order {order_id}. Response: {response}")
                return None
        except Exception as e:
            self.logger.error(f"Error getting order status for {order_id}: {e}", exc_info=True)
            return None

    def get_positions(self) -> List[Position]:
        if self.paper_trading:
            # Convert simulated_orders that are filled into positions
            paper_positions = {}
            for order_id, order_obj in self.simulated_orders.items():
                if order_obj.status == OrderStatus.FILLED or order_obj.status == OrderStatus.PARTIALLY_FILLED:
                    if order_obj.filled_quantity == 0: continue

                    pos_key = order_obj.instrument_id # Assuming instrument_id like "EXCHANGE:SYMBOL"
                    qty = order_obj.filled_quantity if order_obj.side == OrderSide.BUY else -order_obj.filled_quantity
                    
                    if pos_key not in paper_positions:
                        paper_positions[pos_key] = Position(
                            instrument_id=pos_key,
                            symbol=order_obj.symbol,
                            exchange=order_obj.exchange,
                            quantity=0,
                            average_price=0,
                            product_type=getattr(order_obj, 'product_type', 'MIS')
                        )
                    
                    # Update quantity and average price (simplified for now)
                    current_pos = paper_positions[pos_key]
                    new_total_value = (current_pos.average_price * current_pos.quantity) + \
                                      (order_obj.average_fill_price * qty)
                    new_total_quantity = current_pos.quantity + qty

                    current_pos.quantity = new_total_quantity
                    if new_total_quantity != 0:
                        current_pos.average_price = abs(new_total_value / new_total_quantity)
                    else:
                        current_pos.average_price = 0
            return list(paper_positions.values())


        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API.")
            return []
        try:
            response = self.api.get_positions()
            positions = []
            if response and isinstance(response, list): # Finvasia returns a list of position dicts
                for pos_data in response:
                    try:
                        # Map Finvasia position data to internal Position model
                        instrument_id = f"{pos_data.get('exch')}:{pos_data.get('tsym')}"
                        instrument_obj = self.get_instrument_by_id(instrument_id)

                        position = Position(
                            instrument_id=instrument_id,
                            symbol=pos_data.get('tsym'),
                            exchange=Exchange(pos_data.get('exch')) if pos_data.get('exch') else None,
                            instrument=instrument_obj, # Store the full instrument object if available
                            quantity=int(pos_data.get('netqty', 0)),
                            average_price=float(pos_data.get('netavgprc', 0.0)),
                            last_price=float(pos_data.get('lp', 0.0)), # Last traded price
                            pnl=float(pos_data.get('rpnl', 0.0)) + float(pos_data.get('urmtom', 0.0)), # Realized + Unrealized
                            product_type=pos_data.get('prd'),
                            # Add other relevant fields like day_buy_qty, day_sell_qty, etc.
                            day_buy_quantity=int(pos_data.get('daybuyqty',0)),
                            day_sell_quantity=int(pos_data.get('daysellqty',0)),
                            day_buy_value=float(pos_data.get('daybuyval',0.0)),
                            day_sell_value=float(pos_data.get('daysellval',0.0)),
                        )
                        positions.append(position)
                    except Exception as e_inner:
                         self.logger.error(f"Error processing position data item: {pos_data}. Error: {e_inner}", exc_info=True)

            else:
                self.logger.warning(f"Could not get positions or empty response. Response: {response}")
            return positions
        except Exception as e:
            self.logger.error(f"Error getting positions: {e}", exc_info=True)
            return []
            
    # Placeholder for get_account_balance, assuming it's similar to get_funds
    def get_account_balance(self) -> Optional[Dict[str, Any]]:
        if not self.is_connected():
            self.logger.error("Not connected to Finvasia API.")
            return None
        try:
            response = self.api.get_limits() # Finvasia API for margins/balance
            if response and response.get('stat') == 'Ok':
                # Map response fields to a standardized dictionary
                # This mapping depends on the exact structure of your desired balance dict
                # and Finvasia's get_limits() response.
                balance_info = {
                    "user_id": response.get("actid"),
                    "cash": float(response.get("cash", 0)),
                    "payin": float(response.get("payin", 0)),
                    "payout": float(response.get("payout", 0)),
                    "available_cash": float(response.get("net", 0)), # 'net' or 'availablecash'
                    "margin_used": float(response.get("marginused", 0)),
                    "unrealized_pnl": float(response.get("unrealmtom", 0)) + float(response.get("urmtom",0)), # Sum of equity and commodity
                    "realized_pnl": float(response.get("realmtom", 0)) + float(response.get("rpnl",0)),
                    # Add more fields as needed based on Finvasia's response (e.g., span, exposure)
                    "span_margin": float(response.get("span",0)),
                    "exposure_margin": float(response.get("exposure",0)),
                    "option_premium": float(response.get("optpr",0)), # Option premium
                }
                return balance_info
            else:
                self.logger.error(f"Failed to get account balance from Finvasia. Response: {response}")
                return None
        except Exception as e:
            self.logger.error(f"Error getting account balance: {e}", exc_info=True)
            return None

    # --- Other methods from your original snippet, ensure they are adapted for Finvasia ---
    # get_trading_symbol, get_profile, place_bracket_order, place_cover_order,
    # get_option_chain, exit_positions, get_symbol_ltp, generate_access_token,
    # caching methods, get_market_status, etc.
    # These would need to be implemented using self.api calls similar to above.

    def get_pending_paper_orders(self) -> Dict[str, Order]:
        if not self.paper_trading:
            return {}
        return {
            order_id: order for order_id, order in self.simulated_orders.items()
            if order.status in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]
        }


