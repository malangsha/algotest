import threading
import time
import logging
import json
import random
import math
import websocket
import requests
import uuid
import os

from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from NorenRestApiPy.NorenApi import NorenApi

from models.instrument import Instrument
from utils.constants import MarketDataType, EventType, InstrumentType, OptionType
from models.events import MarketDataEvent
from utils.exceptions import MarketDataException
from core.event_manager import EventManager

class MarketDataFeedBase(ABC):
    """Base abstract class for market data feeds."""

    def __init__(self, instruments: List[Instrument], callback: Callable):
        """
        Initialize the market data feed.

        Args:
            instruments: List of instruments to subscribe to
            callback: Callback function to handle market data events
        """
        self.instruments = instruments
        self.callback = callback
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False

    @abstractmethod
    def start(self):
        """Start the market data feed."""
        pass

    @abstractmethod
    def stop(self):
        """Stop the market data feed."""
        pass

    @abstractmethod
    def subscribe(self, instrument: Instrument):
        """
        Subscribe to market data for an instrument.

        Args:
            instrument: Instrument to subscribe to
        """
        pass

    @abstractmethod
    def unsubscribe(self, instrument: Instrument):
        """
        Unsubscribe from market data for an instrument.

        Args:
            instrument: Instrument to unsubscribe from
        """
        pass

class SimulatedFeed(MarketDataFeedBase):
    """Simulated market data feed for testing and development."""

    def __init__(self, instruments: List[Instrument] = None, callback: Callable = None, **kwargs):
        """
        Initialize the simulated feed.

        Args:
            instruments: List of instruments to simulate
            callback: Callback function for market data events
            **kwargs: Additional configuration parameters
        """
        super().__init__(instruments, callback)
        
        # Initialize price tracking
        self.prices = {}
        self.underlying_prices = {}
        self.underlying_volatilities = {}
        self.last_ohlc_minute_map = {}
        
        # Initialize initial prices from settings
        self.initial_prices = kwargs.get('initial_prices', {})
        
        # Initialize price data for each instrument
        for instrument in instruments:
            symbol = instrument.symbol
            initial_price = self.initial_prices.get(symbol, random.uniform(10, 1000))
            self.underlying_prices[symbol] = initial_price
            self.underlying_volatilities[symbol] = random.uniform(0.001, 0.05)
            
            # Initialize price data structure
            self.prices[symbol] = {
                'current': initial_price,
                'previous': initial_price,
                'bid': initial_price * 0.999,
                'ask': initial_price * 1.001,
                'volume': 0,
                'tick_volume': 0,
                'open': initial_price,
                'high': initial_price,
                'low': initial_price,
                'close': initial_price,
                'timestamp': time.time()
            }
        
        # Simulation parameters
        self.tick_interval = kwargs.get('tick_interval', 1.0)
        self.volatility = kwargs.get('volatility', 0.02)
        self.price_jump_probability = kwargs.get('price_jump_probability', 0.01)
        self.max_price_jump = kwargs.get('max_price_jump', 0.05)
        
        # Simulation state
        self.is_running = False
        self.simulation_thread = None

    def start(self):
        """Start the simulated feed."""
        if self.is_running:
            self.logger.warning("Simulated feed is already running")
            return

        self.is_running = True
        self.simulation_thread = threading.Thread(target=self._simulation_loop)
        self.simulation_thread.daemon = True
        self.simulation_thread.start()
        self.logger.info("Simulated feed started")

    def stop(self):
        """Stop the simulated feed."""
        if not self.is_running:
            self.logger.warning("Simulated feed is not running")
            return

        self.is_running = False
        if self.simulation_thread and self.simulation_thread.is_alive():
            self.simulation_thread.join(timeout=2.0)
            if self.simulation_thread.is_alive():
                self.simulation_thread._stop()
                self.simulation_thread = None

        # Clear all data structures
        self.prices.clear()
        self.underlying_prices.clear()
        self.underlying_volatilities.clear()
        self.last_ohlc_minute_map.clear()
        self.logger.info("Simulated feed stopped")

    def _simulation_loop(self):
        """Main simulation loop that periodically updates prices and sends events."""
        while self.is_running:
            try:
                time.sleep(self.tick_interval)
                if not self.is_running:  # Check again after sleep
                    break
                self._update_underlying_prices()
                if not self.is_running:  # Check after price update
                    break
                self._send_market_data_events()
            except Exception as e:
                self.logger.error(f"Error in simulation loop: {str(e)}")
                break  # Exit loop on error

    def _update_underlying_prices(self):
        """Update prices for all underlying assets."""
        for symbol, price in self.underlying_prices.items():
            try:
                base_volatility = self.underlying_volatilities.get(symbol, self.volatility)
                
                # Regular price movement - random walk with drift
                volatility = base_volatility * price
                drift = 0.0001 * price
                price_change = random.normalvariate(drift, volatility)
                
                # Apply price change
                new_price = max(0.01, price + price_change)
                
                # Update prices dictionary
                self.prices[symbol]['previous'] = price
                self.prices[symbol]['current'] = new_price
                
                # Update bid/ask
                spread = new_price * 0.001
                self.prices[symbol]['bid'] = new_price - spread
                self.prices[symbol]['ask'] = new_price + spread
                
                # Update volume
                vol_change = abs(price_change) / price
                self.prices[symbol]['volume'] += int(random.normalvariate(1000, 500) * (1 + 10 * vol_change))
                self.prices[symbol]['tick_volume'] = int(random.normalvariate(10, 5) * (1 + 10 * vol_change))
                
                # Update OHLC
                current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
                if symbol not in self.last_ohlc_minute_map or self.last_ohlc_minute_map[symbol] != current_minute:
                    self.last_ohlc_minute_map[symbol] = current_minute
                    self.prices[symbol]['open'] = new_price
                    self.prices[symbol]['high'] = new_price
                    self.prices[symbol]['low'] = new_price
                else:
                    self.prices[symbol]['high'] = max(self.prices[symbol]['high'], new_price)
                    self.prices[symbol]['low'] = min(self.prices[symbol]['low'], new_price)
                
                self.prices[symbol]['close'] = new_price
                self.underlying_prices[symbol] = new_price
                
            except Exception as e:
                self.logger.error(f"Error updating prices for {symbol}: {str(e)}")
                continue

    def _send_market_data_events(self):
        """Send market data events for all subscribed instruments."""
        for instrument in self.instruments:
            symbol = instrument.symbol
            if symbol not in self.prices:
                continue
                
            price_data = self.prices[symbol]
            market_data = {
                'symbol': symbol,
                'exchange': instrument.exchange,
                'last_price': price_data['current'],
                'bid': price_data['bid'],
                'ask': price_data['ask'],
                'volume': price_data['volume'],
                'timestamp': int(time.time() * 1000)
            }
            
            event = MarketDataEvent(
                event_type=EventType.MARKET_DATA,
                timestamp=int(time.time() * 1000),
                instrument=instrument,
                data=market_data
            )
            
            if self.callback:
                self.callback(event)

    def subscribe(self, instrument: Instrument):
        """Subscribe to market data for an instrument."""
        if instrument not in self.instruments:
            self.instruments.append(instrument)
            symbol = instrument.symbol
            initial_price = self.initial_prices.get(symbol, 100.0)
            
            self.prices[symbol] = {
                'current': initial_price,
                'previous': initial_price,
                'bid': initial_price * 0.999,
                'ask': initial_price * 1.001,
                'volume': 0,
                'tick_volume': 0,
                'open': initial_price,
                'high': initial_price,
                'low': initial_price,
                'close': initial_price,
                'timestamp': time.time()
            }
            
            self.underlying_prices[symbol] = initial_price
            self.underlying_volatilities[symbol] = self.volatility
            self.logger.info(f"Added {symbol} to simulation")

    def unsubscribe(self, instrument: Instrument):
        """Unsubscribe from an instrument."""
        if instrument in self.instruments:
            self.instruments.remove(instrument)
            symbol = instrument.symbol
            
            # Clean up all price data for this instrument
            if symbol in self.prices:
                del self.prices[symbol]
            if symbol in self.underlying_prices:
                del self.underlying_prices[symbol]
            if symbol in self.underlying_volatilities:
                del self.underlying_volatilities[symbol]
            if symbol in self.last_ohlc_minute_map:
                del self.last_ohlc_minute_map[symbol]
                
            self.logger.info(f"Unsubscribed from instrument: {symbol}")

    def generate_crossover_scenario(self, symbol: str, duration: int = 100, crossover_at: int = 50, trend_strength: float = 0.01):
        """
        Generate a specific price scenario to cause a moving average crossover.
        
        Args:
            symbol: The symbol to generate scenario for
            duration: Total number of data points to generate
            crossover_at: At which point the trend should change (index)
            trend_strength: How strong the trend should be (percentage change per tick)
        """
        if symbol not in self.prices:
            self.logger.warning(f"Symbol {symbol} not found in prices")
            return
            
        # Get current price
        current_price = self.prices[symbol]['current']
        
        # Simulate a trend reversal that will cause a MA crossover
        # First downtrend, then uptrend
        prices = []
        
        # Initialize with current price
        price = current_price
        
        # Generate scenario
        for i in range(duration):
            # Before crossover point - downtrend
            if i < crossover_at:
                # Downtrend with some noise
                change = -trend_strength * price * (1 + 0.5 * random.normalvariate(0, 1))
            else:
                # After crossover point - uptrend
                change = trend_strength * price * (1 + 0.5 * random.normalvariate(0, 1))
                
            # Apply change
            price = max(0.01, price + change)
            prices.append(price)
            
            # Update price data
            self.prices[symbol]['previous'] = self.prices[symbol]['current']
            self.prices[symbol]['current'] = price
            
            # Update bid/ask
            spread = price * 0.001  # 0.1% spread
            self.prices[symbol]['bid'] = price - spread
            self.prices[symbol]['ask'] = price + spread
            
            # Update OHLC
            # If this is the first price in a series, it's the open
            if i == 0 or (i % 5 == 0):  # Create a new bar every 5 ticks for faster MA calculation
                self.prices[symbol]['open'] = price
                self.prices[symbol]['high'] = price
                self.prices[symbol]['low'] = price
            else:
                # Update high and low
                self.prices[symbol]['high'] = max(self.prices[symbol]['high'], price)
                self.prices[symbol]['low'] = min(self.prices[symbol]['low'], price)
            
            # Always update close
            self.prices[symbol]['close'] = price
            
            # Update volume
            self.prices[symbol]['volume'] += int(random.normalvariate(1000, 500))
            self.prices[symbol]['tick_volume'] = int(random.normalvariate(10, 5))
            
            # Update timestamp
            self.prices[symbol]['timestamp'] = time.time()
            
            # Send a market data event for this update
            for instrument in self.instruments:
                if instrument.symbol == symbol:
                    self._send_market_data_events()
                    time.sleep(self.tick_interval)  # Wait between events
                    break
                    
        self.logger.info(f"Generated crossover scenario for {symbol}: {len(prices)} price points")
        return prices

    # Add a new method that can be called to trigger a specific test scenario
    def trigger_test_scenario(self, scenario_type: str = "moving_average_crossover"):
        """
        Trigger a specific test scenario.
        
        Args:
            scenario_type: Type of scenario to trigger
        """
        if scenario_type == "moving_average_crossover":
            # For each instrument, generate a crossover scenario
            for instrument in self.instruments:
                self.generate_crossover_scenario(instrument.symbol)
                self.logger.info(f"Triggered MA crossover scenario for {instrument.symbol}")
        else:
            self.logger.warning(f"Unknown scenario type: {scenario_type}")

    def _convert_tick_to_market_data(self, tick):
        """
        Convert Finvasia tick format to our standard market data format.
        
        Args:
            tick: Finvasia tick data
            
        Returns:
            dict: Standardized market data
        """
        try:
            market_data = {}
            
            # Extract basic price data
            if 'lp' in tick:
                market_data[MarketDataType.LAST_PRICE] = float(tick['lp'])
                
            # Extract bid/ask
            if 'bp' in tick and 'sp' in tick:
                market_data[MarketDataType.BID] = float(tick['bp'])
                market_data[MarketDataType.ASK] = float(tick['sp'])
                
            # Extract volumes
            if 'v' in tick:
                market_data[MarketDataType.VOLUME] = float(tick['v'])
                
            # Extract OHLC if available
            ohlc_data = {}
            if 'o' in tick:
                ohlc_data['open'] = float(tick['o'])
            if 'h' in tick:
                ohlc_data['high'] = float(tick['h'])
            if 'l' in tick:
                ohlc_data['low'] = float(tick['l'])
            if 'c' in tick:
                ohlc_data['close'] = float(tick['c'])
                
            # Only add OHLC if we have at least one value
            if ohlc_data:
                market_data[MarketDataType.OHLC] = ohlc_data
                
            # Add timestamp
            market_data[MarketDataType.TIMESTAMP] = time.time()
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error converting tick to market data: {str(e)}")
            self.logger.debug(f"Raw tick data: {tick}")
            return None

class FinvasiaFeed(MarketDataFeedBase):
    """
    Finvasia WebSocket feed implementation that integrates with NorenRestApiPy.
    This class handles market data streaming from Finvasia, with proper authentication,
    token mapping, and subscription management.
    """

    def __init__(self,
                instruments: List[Instrument],
                callback: Callable,
                user_id: str,
                password: str,
                api_key: str,
                imei: str = None,
                twofa: str = None,
                vc: str = None,
                api_url: str = "https://api.shoonya.com/NorenWClientTP/",
                ws_url: str = "wss://api.shoonya.com/NorenWSTP/",
                debug: bool = False):
        """
        Initialize the Finvasia feed handler.

        Args:
            instruments: List of instruments to subscribe to
            callback: Callback function for market data events
            user_id: Finvasia user ID
            password: Finvasia password
            api_key: Finvasia API key
            imei: IMEI for device identification
            twofa: Two-factor authentication secret
            vc: Vendor code
            api_url: Finvasia REST API URL
            ws_url: Finvasia WebSocket URL
            debug: Enable debug mode
        """
        super().__init__(instruments, callback)
        
        # Finvasia credentials
        self.user_id = user_id
        self.password = password
        self.api_key = api_key
        self.imei = imei
        self.twofa = twofa
        self.vc = vc
        self.api_url = api_url
        self.ws_url = ws_url
        self.debug = debug
        
        # Connection state
        self._connected = False
        self._session_token = None
        self.websocket_connected = False
        self.is_running = False
        
        # Mapping and subscription tracking
        self._symbol_token_map = {}  # Maps "Exchange:Symbol" -> token
        self._token_symbol_map = {}  # Maps token -> "Exchange:Symbol"
        self.subscribed_symbols = []  # List of currently subscribed symbols
        
        # Instance of NorenApi
        self.api = None
        
        # In-memory database for storing ticks
        self.tick_store = {}  # Symbol -> list of ticks
        self.tick_counter = 0
        self.max_ticks_in_memory = 10000  # Per symbol
        
        # Persistence settings
        self.persistence_enabled = False
        self.persistence_interval = 300  # 5 minutes
        self.persistence_path = "./data/ticks"
        self.persistence_thread = None
        
        # Cache management
        self.cache_file = "./cache/finvasia_symbols.json"
        self.cache_expiry = 86400  # 24 hours in seconds
        self.last_cache_update = 0
        
        # Thread locks
        self._ws_lock = threading.RLock()
        self._symbol_lock = threading.RLock()
        self._tick_lock = threading.RLock()

    def start(self):
        """Start the Finvasia feed."""
        if self.is_running:
            self.logger.warning("Finvasia feed is already running")
            return

        self.is_running = True
        
        # Initialize NorenApi
        self.api = NorenApi(host=self.api_url, websocket=self.ws_url)
        
        # Connect to API first
        if not self.connect():
            self.logger.error("Failed to connect to Finvasia API during start")
            self.is_running = False
            return
        
        # --- Explicitly start WebSocket here --- 
        self.logger.info("Attempting to establish WebSocket connection...")
        if not self.start_websocket():
            self.logger.error("Failed to establish WebSocket connection during start")
            # Decide if we should stop entirely or proceed without websocket
            # For now, let's stop if websocket fails initially.
            self.is_running = False
            return 
        # --- End WebSocket Start --- 
        
        # Load symbol mappings (moved after connect)
        self._load_symbol_mappings()
        
        # Initialize persistence
        if self.persistence_enabled:
            os.makedirs(self.persistence_path, exist_ok=True)
            self.persistence_thread = threading.Thread(target=self._persistence_loop, daemon=True)
            self.persistence_thread.start()
        
        # Subscribe to instruments (WebSocket should be connected now)
        if self.instruments:
            symbols_to_subscribe = []
            for instrument in self.instruments:
                symbols_to_subscribe.append({
                    'exchange': instrument.exchange,
                    'symbol': instrument.symbol
                })
            
            if symbols_to_subscribe:
                # Now call subscribe_symbols, it shouldn't need to reconnect
                self.subscribe_symbols(symbols_to_subscribe)
        
        self.logger.info("Finvasia feed started successfully")

    def stop(self):
        """Stop the Finvasia feed."""
        if not self.is_running:
            self.logger.warning("Finvasia feed is not running")
            return

        self.is_running = False
        
        # Unsubscribe from all symbols
        if self.subscribed_symbols:
            symbols_to_unsubscribe = []
            for symbol_token in self.subscribed_symbols:
                parts = symbol_token.split("|")
                if len(parts) == 2:
                    exchange, token = parts[0].split(":", 1) if ":" in parts[0] else (parts[0], "")
                    symbols_to_unsubscribe.append({
                        'exchange': exchange,
                        'symbol': self._token_symbol_map.get(token, token)
                    })
            
            if symbols_to_unsubscribe:
                self.unsubscribe_symbols(symbols_to_unsubscribe)
        
        # Stop WebSocket connection
        self.stop_websocket()

        # Clear all internal state
        self.subscribed_symbols = []
        self._token_symbol_map.clear()
        self._symbol_token_map.clear()
        self.tick_store = {}
        self.tick_counter = 0
        
        # Persist all remaining ticks
        if self.persistence_enabled:
            self._persist_all_ticks()
        
        self.logger.info("Finvasia feed stopped")

    def connect(self) -> bool:
        """
        Connect to the Finvasia API.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        if self._connected:
            self.logger.info("Already connected to Finvasia API")
            return True
        
        try:
            # Generate TOTP if twofa is provided
            twofa_token = ""
            if self.twofa:
                import pyotp
                twofa_token = pyotp.TOTP(self.twofa).now()
            
            # Login to Finvasia API
            response = self.api.login(
                userid=self.user_id,
                password=self.password,
                twoFA=twofa_token,
                vendor_code=self.vc,
                api_secret=self.api_key,
                imei=self.imei
            )
            
            if response and 'susertoken' in response:
                self._session_token = response['susertoken']
                self._connected = True
                self.logger.info("Successfully connected to Finvasia API")
                
                # Initialize WebSocket with callbacks
                self._init_websocket()
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if response else 'No response'
                self.logger.error(f"Connection failed: {error_msg}")
                return False
        except Exception as e:
            self.logger.error(f"Error connecting to Finvasia API: {str(e)}")
            return False

    def _init_websocket(self) -> bool:
        """
        Initialize the WebSocket connection for streaming market data.
        
        Returns:
            bool: True if initialized successfully, False otherwise
        """
        # Register callbacks
        self.api.on_disconnect = self._on_ws_disconnect
        self.api.on_error = self._on_ws_error
        self.api.on_open = self._on_ws_open
        self.api.on_close = self._on_ws_close
        self.api.on_ticks = self._on_market_data
        self.api.on_order_update = self._on_order_update
        
        return True

    def start_websocket(self) -> bool:
        """
        Start WebSocket connection for live market data.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        try:
            if self.websocket_connected:
                self.logger.info("WebSocket already connected")
                return True
            
            # Initialize and connect WebSocket
            self.logger.info("Calling self.api.start_websocket...") # Add log
            self.api.start_websocket(
                order_update_callback=self._on_order_update,
                subscribe_callback=self._on_market_data,
                socket_open_callback=self._on_ws_open,
                socket_close_callback=self._on_ws_close
            )
            self.logger.info("Call to self.api.start_websocket completed. Waiting for connection...") # Add log
            
            # Wait for connection to be established (increase timeout to 15s)
            start_time = time.time()
            wait_timeout = 15.0 # Increased timeout
            while not self.websocket_connected and time.time() - start_time < wait_timeout:
                time.sleep(0.5)
            
            if not self.websocket_connected:
                self.logger.error(f"Failed to establish WebSocket connection within {wait_timeout} seconds")
                return False
            
            self.logger.info("WebSocket connection established successfully")
            return True
        except Exception as e:
            # Log the exception immediately if api.start_websocket fails
            self.logger.error(f"Exception during self.api.start_websocket call: {repr(e)}", exc_info=True) 
            return False

    def stop_websocket(self):
        """Stop the WebSocket connection."""
        if self.websocket_connected:
            try:
                self.api.close_websocket()
                self.websocket_connected = False
                self.logger.info("WebSocket connection closed")
            except Exception as e:
                self.logger.error(f"Error stopping WebSocket: {str(e)}")

    def _on_ws_open(self):
        """Callback when WebSocket connection is opened."""
        self.websocket_connected = True
        self.logger.info("WebSocket connection opened")
        
        # Re-subscribe to any previously subscribed symbols
        if self.subscribed_symbols:
            self.api.subscribe(self.subscribed_symbols)
            self.logger.info(f"Re-subscribed to symbols: {self.subscribed_symbols}")

    def _on_ws_close(self, code=None, reason=None):
        """Callback when WebSocket connection is closed."""
        self.logger.info(f"WebSocket closed: {code} - {reason}")
        self.websocket_connected = False

    def _on_ws_disconnect(self):
        """Handle WebSocket disconnection."""
        self.logger.warning("WebSocket disconnected")
        self.websocket_connected = False

    def _on_ws_error(self, error):
        """Handle WebSocket errors."""
        self.logger.error(f"WebSocket error: {error}")

    def _on_market_data(self, ticks):
        """
        Process incoming market data ticks.
        
        Args:
            ticks: Tick data from Finvasia
        """
        try:
            # Process single tick or list of ticks
            if isinstance(ticks, list):
                for tick in ticks:
                    self._process_single_tick(tick)
            elif isinstance(ticks, dict):
                self._process_single_tick(ticks)
        except Exception as e:
            self.logger.error(f"Error processing market data: {str(e)}")

    def _convert_tick_to_market_data(self, tick: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Finvasia tick format to our standard market data format.
        
        Args:
            tick: Finvasia tick data
            
        Returns:
            dict: Standardized market data
        """
        try:
            # --- DEBUGGING --- 
            self.logger.debug(f"[Convert] Type of MarketDataType: {type(MarketDataType)}, OHLC value: {MarketDataType.OHLC.value if hasattr(MarketDataType, 'OHLC') else 'N/A'}")
            # --- END DEBUGGING ---
            market_data = {}
            
            # Use string literals as keys
            if 'lp' in tick:
                market_data['LAST_PRICE'] = float(tick['lp'])
                
            if 'bp1' in tick and 'sp1' in tick:
                market_data['BID'] = float(tick['bp1'])
                market_data['ASK'] = float(tick['sp1'])
                
            if 'v' in tick:
                market_data['VOLUME'] = float(tick['v'])
                
            ohlc_data = {}
            if 'o' in tick: ohlc_data['open'] = float(tick['o'])
            if 'h' in tick: ohlc_data['high'] = float(tick['h'])
            if 'l' in tick: ohlc_data['low'] = float(tick['l'])
            if 'c' in tick: ohlc_data['close'] = float(tick['c'])
                
            if ohlc_data:
                market_data['OHLC'] = ohlc_data
                
            timestamp = tick.get('ft')
            if timestamp:
                try:
                    market_data['TIMESTAMP'] = float(timestamp)
                except ValueError:
                    market_data['TIMESTAMP'] = time.time()
            else:
                market_data['TIMESTAMP'] = time.time()
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error converting tick to market data: {repr(e)}")
            self.logger.debug(f"Raw tick data: {tick}")
            return None

    def _process_single_tick(self, tick):
        """
        Process a single market data tick.
        
        Args:
            tick: Single tick data
        """
        try:
            # --- DEBUGGING --- 
            self.logger.debug(f"[Process] Type of MarketDataType: {type(MarketDataType)}, OHLC value: {MarketDataType.OHLC.value if hasattr(MarketDataType, 'OHLC') else 'N/A'}")
            # --- END DEBUGGING ---
            
            exchange = tick.get('e')
            token = tick.get('tk')

            if not exchange or not token:
                return
                
            # Find corresponding instrument
            symbol = None
            
            # Try to find the symbol from token
            key = f"{exchange}:{token}"
            if key in self._token_symbol_map:
                symbol = self._token_symbol_map[key]
            
            if not symbol:
                # If we don't have a mapping, we can't process this tick
                self.logger.error(f"Symbol not found for {key}")
                return
                
            # Find the instrument object
            instrument = None
            for instr in self.instruments:
                if instr.symbol == symbol and instr.exchange == exchange:
                    instrument = instr
                    break
                    
            if not instrument:
                # If we can't find the instrument, we can't create a proper event
                self.logger.error(f"Instrument not found for {key}")
                return
            
            # Convert tick to standard market data format
            market_data = self._convert_tick_to_market_data(tick)

            if market_data:
                # --- Add check for essential price data --- 
                has_price = (
                    'LAST_PRICE' in market_data or 
                    ('BID' in market_data and 'ASK' in market_data) or 
                    'OHLC' in market_data
                )
                if not has_price:
                    self.logger.debug(f"Skipping tick for {symbol} - no essential price data found: {market_data}")
                    return # Don't process if no price info
                # --- End check --- 

                # Store tick in memory
                self._store_tick(symbol, market_data)
                
                # Determine data type based on string keys
                data_type_enum = MarketDataType.LAST_PRICE # Default
                
                if 'OHLC' in market_data and isinstance(market_data.get('OHLC'), dict):
                    data_type_enum = MarketDataType.BAR
                elif 'BID' in market_data and 'ASK' in market_data:
                    data_type_enum = MarketDataType.QUOTE
                elif 'LAST_PRICE' in market_data:
                    data_type_enum = MarketDataType.TICK

                # Create market data event
                event = MarketDataEvent(
                    instrument=instrument,
                    data_type=data_type_enum,
                    data=market_data,
                    timestamp=market_data.get('TIMESTAMP', time.time()),
                    event_type=EventType.MARKET_DATA
                )
                
                if self.callback:
                    self.callback(event)
            else:
                # Log if conversion failed
                self.logger.warning(f"Market data conversion failed for tick: {tick}")
            
        except Exception as e:
            # Log repr(e) for more detail
            self.logger.error(f"Error processing tick: {repr(e)}")
            self.logger.debug(f"Tick data: {tick}")

    def _on_order_update(self, message):
        """
        Handle order update events.
        
        Args:
            message: Order update data
        """
        # Not directly relevant for market data, but might be useful for some implementations
        self.logger.debug(f"Order update received: {message}")

    def subscribe(self, instrument: Instrument):
        """
        Subscribe to market data for an instrument.
        
        Args:
            instrument: Instrument to subscribe to
        """
        # Add to tracked instruments if not already present
        if instrument not in self.instruments:
            self.instruments.append(instrument)
        
        # Use the subscribe_symbols method with a list of one
        symbols_to_subscribe = [{
            'exchange': instrument.exchange,
            'symbol': instrument.symbol
        }]
        
        return self.subscribe_symbols(symbols_to_subscribe)

    def subscribe_symbols(self, symbols: list) -> bool:
        """
        Subscribe to market data for multiple symbols.
        
        Args:
            symbols: List of dictionaries with keys 'exchange' and 'symbol'
            
        Returns:
            bool: True if subscription successful, False otherwise
        """
        try:
            self.logger.info(f"Subscribing to symbols: {symbols}")
            if not self.websocket_connected:
                self.logger.warning("WebSocket not connected. Attempting to connect...")
                if not self.start_websocket():
                    return False
                
            tokens_to_subscribe = []
            
            for item in symbols:
                exchange = item.get('exchange')
                symbol = item.get('symbol')
                
                if not exchange or not symbol:
                    self.logger.error("Invalid symbol entry; must contain 'exchange' and 'symbol'")
                    continue
                
                token = self._get_token_for_symbol(exchange, symbol)
                if not token:
                    self.logger.error(f"Token not found for symbol {exchange}:{symbol}")
                else:
                    # Format as expected by Finvasia API: "Exchange|Token"
                    subscription_string = f"{exchange}|{token}"
                    tokens_to_subscribe.append(subscription_string)
                    # Store the mapping for later use
                    self._token_symbol_map[token] = symbol
                    self._symbol_token_map[f"{exchange}:{symbol}"] = token
            
            if not tokens_to_subscribe:
                self.logger.error("No valid symbols to subscribe")
                return False
            
            self.logger.info(f"Final subscription list: {tokens_to_subscribe}")
            # Subscribe one at a time to avoid timeout issues
            for token in tokens_to_subscribe:
                try:
                    response = self.api.subscribe([token])
                    if response and response.get("stat") == "Ok":
                        self.subscribed_symbols.append(token)
                        self.logger.info(f"Successfully subscribed to {token}")
                    else:
                        self.logger.warning(f"Subscription response for {token}: {response}")
                except Exception as e:
                    self.logger.error(f"Error subscribing to {token}: {str(e)}")
                    continue
            
            return len(self.subscribed_symbols) > 0           
                
        except Exception as e:
            self.logger.exception(f"Exception subscribing to symbols: {str(e)}")
            return False

    def unsubscribe(self, instrument: Instrument):
        """
        Unsubscribe from market data for an instrument.
        
        Args:
            instrument: Instrument to unsubscribe from
        """
        # Use the unsubscribe_symbols method with a list of one
        symbols_to_unsubscribe = [{
            'exchange': instrument.exchange,
            'symbol': instrument.symbol
        }]
        
        return self.unsubscribe_symbols(symbols_to_unsubscribe)

    def unsubscribe_symbols(self, symbols: list) -> bool:
        """
        Unsubscribe from market data for multiple symbols.
        
        Args:
            symbols: List of dictionaries with keys 'exchange' and 'symbol'
            
        Returns:
            bool: True if unsubscription successful, False otherwise
        """
        try:
            if not self.websocket_connected:
                self.logger.warning("WebSocket not connected; cannot unsubscribe")
                return False
            
            tokens_to_unsubscribe = []
            for item in symbols:
                exchange = item.get('exchange')
                symbol = item.get('symbol')
                
                if not exchange or not symbol:
                    self.logger.error("Invalid symbol entry; must contain 'exchange' and 'symbol'")
                    continue
                
                token = self._get_token_for_symbol(exchange, symbol)
                if not token:
                    self.logger.error(f"Token not found for symbol {exchange}:{symbol}")
                else:
                    # Format as expected by Finvasia API
                    tokens_to_unsubscribe.append(f"{exchange}|{token}")
            
            if not tokens_to_unsubscribe:
                self.logger.error("No valid symbols to unsubscribe")
                return False
            
            self.logger.info(f"Unsubscribing from: {tokens_to_unsubscribe}")
            
            # Convert list to comma-separated string as expected by Finvasia API
            tokens_str = ",".join(tokens_to_unsubscribe)
            response = self.api.unsubscribe(tokens_str)
            
            if response and response.get("stat") == "Ok":
                # Remove from our tracking list
                for token in tokens_to_unsubscribe:
                    if token in self.subscribed_symbols:
                        self.subscribed_symbols.remove(token)
                
                self.logger.info("Successfully unsubscribed from market data")
                return True
            else:
                self.logger.error("Failed to unsubscribe from symbols")
                return False
                
        except Exception as e:
            self.logger.exception(f"Exception unsubscribing from symbols: {str(e)}")
            return False

    def _get_token_for_symbol(self, exchange: str, symbol: str) -> str:
        """
        Get the token for a given symbol, performing a lookup if needed.
        
        Args:
            exchange: Exchange code
            symbol: Trading symbol
            
        Returns:
            str: Token if found, else None
        """
        with self._symbol_lock:
            key = f"{exchange}:{symbol}"
            token = self._symbol_token_map.get(key)
            
            if token is None:
                # Token not found in cache, try to fetch it
                self.logger.info(f"Token for {key} not found in cache, fetching...")
                
                try:
                    # Search using the provided symbol
                    response = self.api.searchscrip(exchange=exchange, searchtext=symbol)
                    
                    if response and response.get("stat") == "Ok" and "values" in response:
                        for item in response.get("values", []):
                            if item.get("tsym") == symbol:
                                token = item.get("token")
                                # Store in our mappings
                                self._symbol_token_map[key] = token
                                self._token_symbol_map[f"{exchange}:{token}"] = symbol
                                self.logger.info(f"Found token for {key}: {token}")
                                
                                # Update cache file
                                self._update_symbol_cache()
                                break
                        
                        if token is None:
                            # Try partial match
                            for item in response.get("values", []):
                                if symbol.upper() in item.get("tsym", "").upper():
                                    token = item.get("token")
                                    self._symbol_token_map[key] = token
                                    self._token_symbol_map[f"{exchange}:{token}"] = item.get("tsym")
                                    self.logger.info(f"Found token for {key} via partial match: {token}")
                                    
                                    # Update cache file
                                    self._update_symbol_cache()
                                    break
                except Exception as e:
                    self.logger.error(f"Error fetching token for {key}: {str(e)}")
            
            return token

    def _load_symbol_mappings(self):
        """Load symbol mappings from cache or fetch them fresh."""
        cache_exists = os.path.exists(self.cache_file)
        cache_valid = False
        
        # Check if cache file exists and is valid
        if cache_exists:
            try:
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    
                # Check cache validity
                last_update = cache_data.get('last_update', 0)
                current_time = time.time()
                if current_time - last_update < self.cache_expiry:
                    # Cache is still valid
                    self._symbol_token_map = cache_data.get('symbol_token_map', {})
                    self._token_symbol_map = cache_data.get('token_symbol_map', {})
                    self.last_cache_update = last_update
                    cache_valid = True
                    self.logger.info(f"Loaded {len(self._symbol_token_map)} symbols from cache")
            except Exception as e:
                self.logger.error(f"Error loading symbol cache: {str(e)}")
                
        # If cache doesn't exist or is invalid, fetch fresh data
        if not cache_valid:
            self._fetch_all_symbols()

    def _update_symbol_cache(self):
        """Update the symbol cache file with current mappings."""
        try:
            # Ensure cache directory exists
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            
            # Prepare cache data
            cache_data = {
                'last_update': time.time(),
                'symbol_token_map': self._symbol_token_map,
                'token_symbol_map': self._token_symbol_map
            }
            
            # Write to cache file
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f)
                
            self.last_cache_update = cache_data['last_update']
            self.logger.info(f"Updated symbol cache with {len(self._symbol_token_map)} symbols")
            
        except Exception as e:
            self.logger.error(f"Error updating symbol cache: {str(e)}")

    def _fetch_all_symbols(self):
        """Fetch and cache all symbols for NSE CASH and F&O segments."""
        try:
            # Clear existing mappings
            self._symbol_token_map = {}
            self._token_symbol_map = {}
            
            # List of exchanges to fetch
            exchanges = ["NSE", "BSE", "NFO"]
            
            for exchange in exchanges:
                self.logger.info(f"Fetching symbols for {exchange}...")
                
                # For NSE CASH (equity), fetch in batches using alphabetical search
                if exchange in ["NSE", "BSE"]:
                    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    for letter in alphabet:
                        self._fetch_symbols_for_exchange(exchange, letter)
                        
                # For NSE F&O (options and futures), fetch key indices
                elif exchange == "NFO":
                    key_symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "RELIANCE", "TCS", "INFY", "SBIN"]
                    for symbol in key_symbols:
                        self._fetch_symbols_for_exchange(exchange, symbol)
            
            # Update cache after fetching all
            self._update_symbol_cache()
            self.logger.info(f"Fetched and cached {len(self._symbol_token_map)} symbols")
            
        except Exception as e:
            self.logger.error(f"Error fetching all symbols: {str(e)}")

    def _fetch_symbols_for_exchange(self, exchange: str, search_text: str):
        """
        Fetch symbols for a specific exchange with search text.
        
        Args:
            exchange: Exchange code
            search_text: Text to search for
        """
        try:
            response = self.api.searchscrip(exchange=exchange, searchtext=search_text)
            
            if response and response.get("stat") == "Ok" and "values" in response:
                for item in response.get("values", []):
                    symbol = item.get("tsym")
                    token = item.get("token")
                    
                    if symbol and token:
                        key = f"{exchange}:{symbol}"
                        self._symbol_token_map[key] = token
                        self._token_symbol_map[f"{exchange}:{token}"] = symbol
                
                self.logger.info(f"Fetched {len(response.get('values', []))} symbols for {exchange} with search '{search_text}'")
            else:
                self.logger.warning(f"No symbols found for {exchange} with search '{search_text}'")
                
        except Exception as e:
            self.logger.error(f"Error fetching symbols for {exchange} with search '{search_text}': {str(e)}")

    def _store_tick(self, symbol: str, tick_data: dict):
        """
        Store a tick in memory and manage storage limits.
        
        Args:
            symbol: Symbol for the tick
            tick_data: Tick data to store
        """
        with self._tick_lock:
            # Initialize list for this symbol if it doesn't exist
            if symbol not in self.tick_store:
                self.tick_store[symbol] = []
            
            # Add tick to the list
            self.tick_store[symbol].append(tick_data)
            self.tick_counter += 1
            
            # Enforce memory limits
            if len(self.tick_store[symbol]) > self.max_ticks_in_memory:
                # If persistence is enabled, persist before clearing
                if self.persistence_enabled:
                    self._persist_ticks(symbol)
                    
                # Keep only the most recent ticks
                keep_count = self.max_ticks_in_memory // 2
                self.tick_store[symbol] = self.tick_store[symbol][-keep_count:]

    def _persistence_loop(self):
        """Background thread for periodic tick persistence."""
        while self.is_running and self.persistence_enabled:
            try:
                time.sleep(self.persistence_interval)
                self._persist_all_ticks()
            except Exception as e:
                self.logger.error(f"Error in persistence loop: {str(e)}")

    def _persist_all_ticks(self):
        """Persist all ticks in memory to disk."""
        with self._tick_lock:
            for symbol in list(self.tick_store.keys()):
                if self.tick_store[symbol]:
                    self._persist_ticks(symbol)

    def _persist_ticks(self, symbol: str):
        """
        Persist ticks for a specific symbol to disk.
        
        Args:
            symbol: Symbol to persist ticks for
        """
        try:
            if not self.tick_store.get(symbol):
                return
                
            # Create filename with date to organize data
            date_str = datetime.now().strftime("%Y%m%d")
            symbol_safe = symbol.replace("/", "_").replace(":", "_")
            filename = f"{self.persistence_path}/{date_str}_{symbol_safe}.csv"
            
            # Check if file exists to determine if we need headers
            file_exists = os.path.exists(filename)
            
            # Open in append mode
            with open(filename, 'a') as f:
                # Write headers if file is new
                if not file_exists:
                    # Use string literals for headers matching the dictionary keys
                    headers = ["TIMESTAMP", "LAST_PRICE", "BID", "ASK", "VOLUME", 
                              "OHLC_open", "OHLC_high", "OHLC_low", "OHLC_close"]
                    f.write(",".join(headers) + "\n")
                
                # Write each tick as a CSV row
                for tick in self.tick_store[symbol]: # tick is the market_data dict
                    # Use string literals for keys
                    row = [
                        tick.get('TIMESTAMP', ""),
                        tick.get('LAST_PRICE', ""),
                        tick.get('BID', ""),
                        tick.get('ASK', ""),
                        tick.get('VOLUME', ""),
                    ]
                    
                    # Use string literal 'OHLC' to get the OHLC dict
                    ohlc = tick.get('OHLC', {}) 
                    row.extend([
                        ohlc.get('open', ""),
                        ohlc.get('high', ""),
                        ohlc.get('low', ""),
                        ohlc.get('close', "")
                    ])
                    
                    # Write as CSV row
                    f.write(",".join(str(x) for x in row) + "\n")
            
            # Clear the in-memory store for this symbol
            self.tick_store[symbol] = []
            self.logger.debug(f"Persisted ticks for {symbol} to {filename}")
            
        except Exception as e:
            # Use repr(e) for better error logging here too
            self.logger.error(f"Error persisting ticks for {symbol}: {repr(e)}") 

    def get_historical_ticks(self, symbol: str, count: int = 100) -> List[dict]:
        """
        Get historical ticks for a symbol from memory.
        
        Args:
            symbol: Symbol to get ticks for
            count: Maximum number of ticks to return
            
        Returns:
            List[dict]: List of historical ticks, most recent first
        """
        with self._tick_lock:
            if symbol not in self.tick_store:
                return []
                
            # Return the most recent 'count' ticks
            return self.tick_store[symbol][-count:]
    
    def enable_persistence(self, enabled: bool = True, interval: int = 300, path: str = None):
        """
        Enable or disable tick persistence to disk.
        
        Args:
            enabled: Whether to enable persistence
            interval: Interval in seconds between persisting ticks
            path: Directory path for storing tick data
        """
        self.persistence_enabled = enabled
        
        if enabled:
            if interval:
                self.persistence_interval = interval
                
            if path:
                self.persistence_path = path
                
            os.makedirs(self.persistence_path, exist_ok=True)
            
            # Start persistence thread if not already running
            if not self.persistence_thread or not self.persistence_thread.is_alive():
                self.persistence_thread = threading.Thread(target=self._persistence_loop, daemon=True)
                self.persistence_thread.start()
                
            self.logger.info(f"Tick persistence enabled. Interval: {self.persistence_interval}s, Path: {self.persistence_path}")
        else:
            self.logger.info("Tick persistence disabled")
    
    def set_max_ticks_in_memory(self, max_ticks: int):
        """
        Set the maximum number of ticks to keep in memory per symbol.
        
        Args:
            max_ticks: Maximum number of ticks
        """
        if max_ticks <= 0:
            self.logger.error("Max ticks must be greater than 0")
            return
            
        self.max_ticks_in_memory = max_ticks
        self.logger.info(f"Set max ticks in memory to {max_ticks}")
    
    def clear_tick_store(self, symbol: str = None):
        """
        Clear the in-memory tick store.
        
        Args:
            symbol: Symbol to clear ticks for, or None to clear all
        """
        with self._tick_lock:
            if symbol:
                if symbol in self.tick_store:
                    self.tick_store[symbol] = []
                    self.logger.info(f"Cleared ticks for {symbol}")
            else:
                self.tick_store = {}
                self.logger.info("Cleared all ticks from memory")
    
    def get_subscription_status(self) -> dict:
        """
        Get the current subscription status.
        
        Returns:
            dict: Current subscription status
        """
        return {
            "subscribed_symbols": self.subscribed_symbols,
            "websocket_connected": self.websocket_connected,
            "is_running": self.is_running,
            "tick_count": self.tick_counter
        }
    
    def refresh_symbol_mappings(self):
        """Force refresh of symbol mappings."""
        self.logger.info("Refreshing symbol mappings...")
        self._fetch_all_symbols()
        self.logger.info(f"Symbol mappings refreshed. {len(self._symbol_token_map)} symbols loaded.")
    
    def get_option_chain(self, underlying: str, expiry: str = None) -> List[dict]:
        """
        Get option chain for an underlying asset.
        
        Args:
            underlying: Underlying symbol (e.g., 'NIFTY')
            expiry: Optional expiry date (YYYYMMDD format)
            
        Returns:
            List[dict]: Option chain data
        """
        try:
            # If no expiry is provided, try to get the nearest expiry
            if not expiry:
                # Attempt to fetch expiry dates and select the nearest
                expiry_dates = self._get_expiry_dates(underlying)
                if expiry_dates:
                    expiry = expiry_dates[0]  # First one is usually the nearest
                else:
                    self.logger.error(f"Could not determine expiry for {underlying}")
                    return []
            
            # Fetch call options
            call_options = self._fetch_symbols_for_exchange('NFO', f"{underlying} {expiry} CE")
            # Fetch put options
            put_options = self._fetch_symbols_for_exchange('NFO', f"{underlying} {expiry} PE")
            
            # Combine and structure as option chain
            option_chain = []
            
            # Group by strike price
            strikes = set()
            for item in call_options + put_options:
                if isinstance(item, dict) and 'strprc' in item:
                    strikes.add(float(item['strprc']))
            
            # Sort strikes
            sorted_strikes = sorted(strikes)
            
            # Build option chain
            for strike in sorted_strikes:
                chain_item = {'strike': strike, 'call': None, 'put': None}
                
                # Find call for this strike
                for call in call_options:
                    if isinstance(call, dict) and float(call.get('strprc', 0)) == strike:
                        chain_item['call'] = call
                        break
                        
                # Find put for this strike
                for put in put_options:
                    if isinstance(put, dict) and float(put.get('strprc', 0)) == strike:
                        chain_item['put'] = put
                        break
                        
                option_chain.append(chain_item)
            
            return option_chain
            
        except Exception as e:
            self.logger.error(f"Error getting option chain for {underlying}: {str(e)}")
            return []
    
    def _get_expiry_dates(self, underlying: str) -> List[str]:
        """
        Get available expiry dates for an underlying.
        
        Args:
            underlying: Underlying symbol
            
        Returns:
            List[str]: List of expiry dates in YYYYMMDD format
        """
        try:
            # This is a placeholder - actual implementation would depend on
            # whether Finvasia API provides a direct method to get expiries
            # For now, we can infer from available options
            
            # Search for options of this underlying
            response = self.api.searchscrip(exchange='NFO', searchtext=underlying)
            
            if not response or response.get('stat') != 'Ok':
                return []
                
            # Extract expiry dates from results
            expiries = set()
            for item in response.get('values', []):
                if 'exd' in item:
                    expiries.add(item['exd'])
            
            # Sort expiries (ascending order)
            return sorted(list(expiries))
            
        except Exception as e:
            self.logger.error(f"Error getting expiry dates for {underlying}: {str(e)}")
            return []
    
    def get_market_status(self) -> dict:
        """
        Get current market status.
        
        Returns:
            dict: Market status information
        """
        try:
            # Call Finvasia API to get market status
            response = self.api.get_exchanges()
            
            if not response or not isinstance(response, list):
                return {"error": "Could not retrieve market status"}
                
            # Process and return structured market status
            statuses = {}
            for exch in response:
                if 'exch' in exch and 'stat' in exch:
                    statuses[exch['exch']] = {
                        'status': exch['stat'],
                        'last_update': time.time()
                    }
            
            return {
                "market_status": statuses,
                "timestamp": time.time(),
                "connected": self._connected,
                "websocket_connected": self.websocket_connected
            }
            
        except Exception as e:
            self.logger.error(f"Error getting market status: {str(e)}")
            return {"error": str(e)}
    
    def get_instrument_details(self, symbol: str, exchange: str = 'NSE') -> dict:
        """
        Get detailed information about an instrument.
        
        Args:
            symbol: Instrument symbol
            exchange: Exchange code
            
        Returns:
            dict: Instrument details
        """
        try:
            # Search for the instrument
            response = self.api.searchscrip(exchange=exchange, searchtext=symbol)
            
            if not response or response.get('stat') != 'Ok':
                return {"error": "Instrument not found"}
                
            # Find exact match
            for item in response.get('values', []):
                if item.get('tsym') == symbol:
                    # Format and return details
                    return {
                        "symbol": item.get('tsym'),
                        "token": item.get('token'),
                        "exchange": exchange,
                        "instrument_type": item.get('inst_type', ''),
                        "tick_size": float(item.get('ti', 0.05)),
                        "lot_size": int(item.get('ls', 1)),
                        "precision": int(item.get('pp', 2)),
                        "multiplier": float(item.get('mult', 1)),
                        "strike_price": float(item.get('strprc', 0)) if 'strprc' in item else None,
                        "expiry_date": item.get('exd', ''),
                        "option_type": item.get('optt', ''),
                        "full_name": item.get('cname', '')
                    }
            
            # If no exact match, return first result
            if response.get('values'):
                item = response['values'][0]
                return {
                    "symbol": item.get('tsym'),
                    "token": item.get('token'),
                    "exchange": exchange,
                    "instrument_type": item.get('inst_type', ''),
                    "tick_size": float(item.get('ti', 0.05)),
                    "lot_size": int(item.get('ls', 1)),
                    "precision": int(item.get('pp', 2)),
                    "multiplier": float(item.get('mult', 1)),
                    "strike_price": float(item.get('strprc', 0)) if 'strprc' in item else None,
                    "expiry_date": item.get('exd', ''),
                    "option_type": item.get('optt', ''),
                    "full_name": item.get('cname', '')
                }
                
            return {"error": "Instrument not found"}
            
        except Exception as e:
            self.logger.error(f"Error getting instrument details: {str(e)}")
            return {"error": str(e)}
    
    def download_securities_file(self, exchange: str = 'NSE') -> bool:
        """
        Download and process the securities file for an exchange.
        This gives us a complete list of all available instruments.
        
        Args:
            exchange: Exchange code
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Downloading securities file for {exchange}...")
            
            # Call Finvasia API to download and process securities file
            # This is a placeholder - implementation depends on whether
            # Finvasia API supports this directly
            
            # For NSE, we can manually download from their website if needed
            if exchange == 'NSE':
                # This is where you'd implement the actual download logic
                # After downloading, process the file to extract symbols and tokens
                pass
                
            self.logger.info(f"Securities file for {exchange} processed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading securities file: {str(e)}")
            return False
    
    def is_connected(self) -> bool:
        """
        Check if connected to Finvasia API.
        
        Returns:
            bool: True if connected, False otherwise
        """
        return self._connected and self._session_token is not None
    
    def is_websocket_connected(self) -> bool:
        """
        Check if WebSocket is connected.
        
        Returns:
            bool: True if WebSocket is connected, False otherwise
        """
        return self.websocket_connected
    
    def get_tick_statistics(self) -> dict:
        """
        Get statistics about collected ticks.
        
        Returns:
            dict: Tick statistics
        """
        with self._tick_lock:
            stats = {
                "total_ticks": self.tick_counter,
                "symbols_tracked": len(self.tick_store),
                "ticks_per_symbol": {symbol: len(ticks) for symbol, ticks in self.tick_store.items()},
                "memory_usage_estimate": sum(len(ticks) for ticks in self.tick_store.values()) * 500,  # Rough estimate: 500 bytes per tick
                "persistence_enabled": self.persistence_enabled,
                "persistence_path": self.persistence_path if self.persistence_enabled else None
            }
            return stats
    
    def __del__(self):
        """Cleanup on destruction."""
        self.stop()

class MarketDataFeed:
    """Factory class for creating market data feeds."""

    def __init__(self, feed_type: str, broker: Any, instruments: List[Instrument] = [], event_manager: EventManager = None, settings: Dict = None):
        """
        Initialize a market data feed of the specified type.

        Args:
            feed_type: Type of feed (websocket, rest, simulated)
            broker: Broker instance
            instruments: List of instruments to subscribe to
            event_manager: EventManager instance
            settings: Dictionary containing feed-specific settings
        """
        self.logger = logging.getLogger("live.market_data_feed")
        self.feed_type = feed_type.lower()
        self.broker = broker
        self.instruments = instruments
        self.feed = None
        self.event_manager = event_manager
        self.settings = settings if settings is not None else {}
        
        # Register with event manager
        self._register_with_event_manager()

    def _register_with_event_manager(self):
        """Register with event manager to publish market data events."""
        # No need to subscribe to anything here since we're the publisher
        self.logger.info("Registered with Event Manager for publishing market data events")

    def start(self):
        """Start the market data feed."""
        if self.feed:
            self.feed.start()
            return
        
        if self.feed_type == 'simulated':
            # Get the appropriate parameters for SimulatedFeed
            websocket_url = self.settings.get('websocket_url', '')  
            api_key = self.settings.get('api_key', '')
            client_id = self.settings.get('client_id', '')
            password = self.settings.get('password', '')
            # Pass all settings to SimulatedFeed constructor
            # SimulatedFeed needs to accept **kwargs or specific args from settings
            
            try:
                self.feed = SimulatedFeed(
                    instruments=self.instruments,
                    callback=self._handle_market_data,
                    **self.settings # Pass all settings directly
                )
                self.logger.info("Created SimulatedFeed with appropriate parameters")
            except Exception as e:
                self.logger.error(f"Failed to create SimulatedFeed: {e}")
                raise RuntimeError(f"Failed to create SimulatedFeed: {e}")

        elif self.feed_type == 'finvasia_ws':
            # Extract Finvasia-specific settings from broker config
            user_id = self.settings.get('user_id')
            password = self.settings.get('password')
            api_key = self.settings.get('api_key')
            imei = self.settings.get('imei', '')
            twofa = self.settings.get('twofa', '')
            vc = self.settings.get('vc', '')
            api_url = self.settings.get('api_url', "https://api.shoonya.com/NorenWClientTP/")
            ws_url = self.settings.get('websocket_url', "wss://api.shoonya.com/NorenWSTP/")
            debug = self.settings.get('debug', False)
            
            # If settings are missing in broker config, try to get them directly from broker
            # Credentials should now be passed directly in the settings dictionary
            
            # Validate required parameters
            if not user_id or not password or not api_key:
                self.logger.error("Missing required Finvasia credentials. Need user_id, password, and api_key.")
                return            
            
            # Create the FinvasiaFeed instance
            self.logger.info(f"Creating FinvasiaFeed with user_id: {user_id}, api_key: {api_key}")
            self.feed = FinvasiaFeed(
                instruments=self.instruments,
                callback=self._handle_market_data,
                user_id=user_id,
                password=password,
                api_key=api_key,
                imei=imei,
                twofa=twofa,
                vc=vc,
                api_url=api_url,
                ws_url=ws_url,
                debug=debug
            )

            # Persistence is configured using settings passed
            if self.settings.get("enable_persistence", False) and hasattr(self.feed, 'enable_persistence'):
                persistence_path = self.settings.get("persistence_path", "./data/ticks")
                persistence_interval = self.settings.get("persistence_interval", 300)
                self.logger.info(f"Enabling persistence for Finvasia feed. Path: {persistence_path}, Interval: {persistence_interval}s")
                self.feed.enable_persistence(
                    enabled=True,
                    interval=persistence_interval,
                    path=persistence_path
                )
            
            # Set maximum ticks in memory if configured
            max_ticks = self.settings.get('max_ticks_in_memory', 10000)
            if hasattr(self.feed, 'set_max_ticks_in_memory'):
                self.feed.set_max_ticks_in_memory(max_ticks)

        else:
            raise ValueError(f"Unsupported feed type: {self.feed_type}")

        # Start the feed immediately after creation
        if self.feed:
            self.feed.start()
        else:
            self.logger.error(f"Failed to create feed of type {self.feed_type}")

    def stop(self):
        """Stop the market data feed."""
        if self.feed:
            self.feed.stop()

    def _handle_market_data(self, event: MarketDataEvent):
        """
        Handle incoming market data events.

        Args:
            event: Market data event
        """
        # Properly classify the data type based on content
        data = event.data
        
        # Determine the most specific data type
        if MarketDataType.OHLC in data:
            # This is bar/candle data
            event.data_type = MarketDataType.BAR
        elif MarketDataType.LAST_PRICE in data and (MarketDataType.BID in data or MarketDataType.ASK in data):
            # This has bid/ask so it's a quote
            event.data_type = MarketDataType.QUOTE
        elif MarketDataType.LAST_PRICE in data:
            # This is a simple tick/trade
            event.data_type = MarketDataType.TICK
        
        # Forward to broker for processing
        if hasattr(self.broker, 'on_market_data'):
            self.broker.on_market_data(event)
    
        # Log event at debug level
        self.logger.debug(f"Received market data event: {event}")
        
        # Forward to event manager for system-wide distribution         
        if hasattr(self, 'event_manager') and self.event_manager:
            self.logger.debug(f"Distributing market data event to event manager: {event}")
            self.event_manager.publish(event)
        elif hasattr(self, 'callback') and callable(self.callback):
            self.logger.debug(f"Distributing market data event via callback: {event}")
            self.callback(event)

    def subscribe(self, instrument: Instrument):
        """
        Subscribe to market data for an instrument.
        Args:
            instrument: Instrument to subscribe to
        """
        if self.feed:
            self.logger.info(f"Subscribing to instrument: {instrument.symbol}")
            self.feed.subscribe(instrument)
        else:
            self.logger.warning(f"Cannot subscribe to {instrument.symbol}: Feed not initialized")

    def unsubscribe(self, instrument: Instrument):
        """
        Unsubscribe from market data for an instrument.

        Args:
            instrument: Instrument to unsubscribe from
        """
        if self.feed:
            self.logger.info(f"Unsubscribing from instrument: {instrument.symbol}")
            self.feed.unsubscribe(instrument)
        else:
            self.logger.warning(f"Cannot unsubscribe from {instrument.symbol}: Feed not initialized")

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Mock broker class for demonstration
    class Broker:
        def __init__(self, name):
            self.name = name

        def get_api_key(self):
            return "demo_api_key"

        def get_market_data_settings(self):
            return {
                'websocket_url': 'wss://example.com/ws',
                'api_key': 'demo_api_key',
                'base_url': 'https://example.com/api',
                'polling_interval': 1.0,
                'initial_prices': {'NIFTY': 16100.00},
                'volatility': 0.0015,
                'tick_interval': 1.0,
                'price_jump_probability': 0.05,
                'max_price_jump': 0.02,
                'risk_free_rate': 0.05
            }

        def on_market_data(self, event):
            # This would be implemented by the actual broker
            pass

    # Create some sample instruments
    nifty = Instrument(
        symbol="NIFTY",
        exchange="NSE",
        instrument_type=InstrumentType.INDEX
    )

    # Create a call option on NIFTY
    nifty_call = Instrument(
        symbol="NIFTY23JUNCE16000",
        exchange="NSE",
        instrument_type=InstrumentType.OPTION,
        underlying_symbol="NIFTY",
        option_type=OptionType.CALL.value,
        strike_price=16000,
        expiry_date="2023-06-30"
    )

    # Create a put option on NIFTY
    nifty_put = Instrument(
        symbol="NIFTY23JUNPE16000",
        exchange="NSE",
        instrument_type=InstrumentType.OPTION,
        underlying_symbol="NIFTY",
        option_type=OptionType.PUT.value,
        strike_price=16000,
        expiry_date="2023-06-30"
    )

    # Create instruments list
    instruments = [nifty, nifty_call, nifty_put]

    # Create a mock broker
    broker = Broker("Demo Broker")

    # Create a market data feed handler function
    def market_data_callback(event):
        # Print some basic information about the event
        instr = event.instrument
        data = event.data

        if MarketDataType.LAST_PRICE in data:
            price = data[MarketDataType.LAST_PRICE]
            print(f"{instr.symbol}: {price:.2f}")

        # For options, also print some Greeks
        if instr.type == InstrumentType.OPTION and MarketDataType.GREEKS in data:
            greeks = data[MarketDataType.GREEKS]
            print(f"  Delta: {greeks['delta']:.4f}, Gamma: {greeks['gamma']:.4f}, Theta: {greeks['theta']:.4f}")

    # Create the market data feed using the updated MarketDataFeed class
    # Instead of using MarketDataFeedFactory, we directly use the MarketDataFeed class
    feed = MarketDataFeed(
        feed_type="simulated",
        broker=broker,
        instruments=instruments
    )

    # Register our callback with the broker
    broker.on_market_data = market_data_callback

    # Start the feed
    feed.start()

    # Let it run for a while
    try:
        print("Market data feed running. Press Ctrl+C to stop.")
        # For demonstration purposes, just wait a bit
        time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        # Stop the feed
        feed.stop()
        print("Market data feed stopped.")


