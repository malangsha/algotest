import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable, Set, Union
from datetime import datetime, timedelta
import threading
import time
from collections import defaultdict
import queue

class MarketDataSubscriber:
    """
    Handles market data subscription and forwards tick data to the TimeframeManager.
    """
    def __init__(self, callback: Callable):
        """
        Initialize the MarketDataSubscriber.
        
        Args:
            callback: Function to call when a tick is received
        """
        self.logger = logging.getLogger("utils.market_data_subscriber")
        self.callback = callback
        self.subscribed_symbols = set()
        self.running = False
        self.worker_thread = None
        self.tick_queue = queue.Queue(maxsize=10000)  # Buffer for incoming ticks
        
    def subscribe(self, symbol: str) -> bool:
        """
        Subscribe to tick data for a symbol.
        
        Args:
            symbol: Symbol to subscribe to
            
        Returns:
            True if subscription was successful, False otherwise
        """
        if symbol in self.subscribed_symbols:
            self.logger.info(f"Already subscribed to {symbol}")
            return True
            
        self.logger.info(f"Subscribing to {symbol}")
        # Add to local tracking
        self.subscribed_symbols.add(symbol)
        
        # In a real implementation, you would connect to your market data provider here
        # and set up the actual subscription
        
        return True
        
    def unsubscribe(self, symbol: str) -> bool:
        """
        Unsubscribe from tick data for a symbol.
        
        Args:
            symbol: Symbol to unsubscribe from
            
        Returns:
            True if unsubscription was successful, False otherwise
        """
        if symbol not in self.subscribed_symbols:
            self.logger.info(f"Not subscribed to {symbol}")
            return True
            
        self.logger.info(f"Unsubscribing from {symbol}")
        # Remove from local tracking
        self.subscribed_symbols.remove(symbol)
        
        # In a real implementation, you would connect to your market data provider here
        # and cancel the actual subscription
        
        return True
    
    def start(self):
        """Start the market data handling thread."""
        if self.running:
            return
            
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        self.logger.info("Market data subscriber started")
        
    def stop(self):
        """Stop the market data handling thread."""
        if not self.running:
            return
            
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5.0)
            self.worker_thread = None
        self.logger.info("Market data subscriber stopped")
    
    def _worker_loop(self):
        """Worker thread that processes the tick queue."""
        while self.running:
            try:
                # Process all available ticks in batch for efficiency
                ticks_to_process = []
                # Get at least one tick (blocking)
                tick = self.tick_queue.get(timeout=0.5)
                ticks_to_process.append(tick)
                
                # Get any additional ticks (non-blocking)
                while not self.tick_queue.empty() and len(ticks_to_process) < 100:
                    ticks_to_process.append(self.tick_queue.get_nowait())
                    
                # Process all ticks
                for tick in ticks_to_process:
                    self._process_tick(tick)
                    self.tick_queue.task_done()
                    
            except queue.Empty:
                # No ticks available
                pass
            except Exception as e:
                self.logger.error(f"Error in market data worker: {e}", exc_info=True)
    
    def _process_tick(self, tick: Dict[str, Any]):
        """
        Process a single tick.
        
        Args:
            tick: Tick data dictionary
        """
        try:
            symbol = tick.get('symbol')
            if not symbol:
                self.logger.warning("Received tick without symbol")
                return
                
            if symbol not in self.subscribed_symbols:
                self.logger.debug(f"Received tick for unsubscribed symbol {symbol}")
                return
                
            # Forward to callback
            self.callback(symbol, tick)
        except Exception as e:
            self.logger.error(f"Error processing tick: {e}", exc_info=True)
    
    def on_tick(self, tick: Dict[str, Any]):
        """
        Handle an incoming tick - add to queue for processing.
        
        Args:
            tick: Tick data dictionary
        """
        try:
            if not self.running:
                return
                
            # Add to queue, with a short timeout to avoid blocking indefinitely
            self.tick_queue.put(tick, timeout=0.1)
        except queue.Full:
            self.logger.warning("Tick queue full, dropping tick")
        except Exception as e:
            self.logger.error(f"Error queueing tick: {e}")


class OptionsGreeksCalculator:
    """
    Calculates option Greeks (Delta, Gamma, Theta, Vega, Rho).
    """
    
    @staticmethod
    def calculate_greeks(
        option_type: str,        # 'call' or 'put'
        underlying_price: float,
        strike_price: float,
        time_to_expiry: float,   # in years
        risk_free_rate: float,   # annual rate, e.g., 0.05 for 5%
        volatility: float,       # implied volatility, e.g., 0.2 for 20%
    ) -> Dict[str, float]:
        """
        Calculate option Greeks using Black-Scholes model.
        
        Args:
            option_type: 'call' or 'put'
            underlying_price: Current price of the underlying
            strike_price: Strike price of the option
            time_to_expiry: Time to expiry in years
            risk_free_rate: Annual risk-free rate
            volatility: Implied volatility
            
        Returns:
            Dictionary with calculated Greeks
        """
        if option_type not in ['call', 'put']:
            raise ValueError("Option type must be 'call' or 'put'")
            
        # Handle edge cases
        if time_to_expiry <= 0:
            return {
                'delta': 1.0 if option_type == 'call' and underlying_price > strike_price else 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0
            }
            
        # Calculate d1 and d2
        if volatility <= 0 or underlying_price <= 0:
            return {
                'delta': 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0
            }
            
        # Black-Scholes formula components
        d1 = (np.log(underlying_price / strike_price) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
        d2 = d1 - volatility * np.sqrt(time_to_expiry)
        
        # Standard normal CDF and PDF
        from scipy.stats import norm
        N_d1 = norm.cdf(d1)
        N_neg_d1 = norm.cdf(-d1)
        N_d2 = norm.cdf(d2)
        N_neg_d2 = norm.cdf(-d2)
        n_d1 = norm.pdf(d1)
        
        # Calculate Greeks
        if option_type == 'call':
            delta = N_d1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) - risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # divide by 100 to get per 1% change
            rho = strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_d2 / 100  # divide by 100 to get per 1% change
        else:  # 'put'
            delta = N_d1 - 1
            gamma = n_d1 / (underlying_price * volatility * np.sqrt(time_to_expiry))
            theta = -(underlying_price * n_d1 * volatility) / (2 * np.sqrt(time_to_expiry)) + risk_free_rate * strike_price * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2
            vega = underlying_price * np.sqrt(time_to_expiry) * n_d1 / 100  # divide by 100 to get per 1% change
            rho = -strike_price * time_to_expiry * np.exp(-risk_free_rate * time_to_expiry) * N_neg_d2 / 100  # divide by 100 to get per 1% change
        
        return {
            'delta': delta,
            'gamma': gamma,
            'theta': theta / 365.0,  # Convert to daily theta
            'vega': vega,
            'rho': rho
        }
    
    @staticmethod
    def get_symbol_info(symbol: str) -> Dict[str, Any]:
        """
        Extract option information from symbol name.
        This should be customized based on your actual symbol naming convention.
        
        Args:
            symbol: Option symbol
            
        Returns:
            Dictionary with option details (underlying, type, strike, expiry)
        """
        # This is a placeholder implementation - customize based on your actual symbol format
        # Example symbol format: NIFTY2305025000CE (NIFTY May 2023 25000 Call)
        try:
            # Parse based on expected format - this is highly dependent on your data provider
            if "CE" in symbol:
                option_type = "call"
            elif "PE" in symbol:
                option_type = "put"
            else:
                return None  # Not an option
                
            # Extract other details based on your symbol naming convention
            # This is just a placeholder and should be customized
            return {
                'underlying': 'NIFTY',  # Extract from symbol
                'option_type': option_type,
                'strike': 25000.0,  # Extract from symbol
                'expiry': datetime(2023, 5, 25),  # Extract from symbol
                'is_option': True
            }
        except Exception:
            # Not a valid option symbol or parsing failed
            return None


class TimeframeManager:
    """
    Enhanced TimeframeManager that handles market data subscriptions,
    maintains OHLCV+OI bars with Greeks for various timeframes,
    and provides real-time and historical data access.
    """

    # Standard timeframes supported
    VALID_TIMEFRAMES = {
        'tick': 0,    # Raw tick data
        '1s': 1,      # 1 second
        '5s': 5,      # 5 seconds
        '10s': 10,    # 10 seconds
        '30s': 30,    # 30 seconds
        '1m': 60,     # 1 minute
        '3m': 180,    # 3 minutes
        '5m': 300,    # 5 minutes
        '15m': 900,   # 15 minutes
        '30m': 1800,  # 30 minutes
        '1h': 3600,   # 1 hour
        '2h': 7200,   # 2 hours
        '4h': 14400,  # 4 hours
        '6h': 21600,  # 6 hours
        '8h': 28800,  # 8 hours
        '12h': 43200, # 12 hours
        '1d': 86400,  # 1 day (24 hours)
        '1w': 604800, # 1 week
        '1M': 2592000, # 1 month (approx 30 days)
    }

    # Actively maintained timeframes - others will be created on-demand
    ACTIVE_TIMEFRAMES = ['1m', '5m']

    def __init__(self, max_bars: int = 1000, risk_free_rate: float = 0.05):
        """
        Initialize the TimeframeManager.

        Args:
            max_bars: Maximum number of bars to keep per symbol per timeframe
            risk_free_rate: Risk-free rate for Greeks calculations
        """
        self.logger = logging.getLogger("utils.timeframe_manager")
        self.max_bars = max_bars
        self.risk_free_rate = risk_free_rate

        # Data structure to store OHLCV+OI+Greeks bars for each symbol and timeframe
        # {symbol: {timeframe: DataFrame}}
        self.bars = defaultdict(dict)

        # Current open bars for each symbol and timeframe
        # {symbol: {timeframe: Dict}}
        self.current_bars = defaultdict(dict)

        # Symbol details cache for efficient Greeks calculation
        # {symbol: {details}}
        self.symbol_details = {}

        # Underlying prices for options symbols
        # {underlying_symbol: price}
        self.underlying_prices = {}

        # Cache for implied volatility
        # {symbol: volatility}
        self.implied_volatility = {}

        # Lock for thread safety
        self.lock = threading.RLock()

        # Last timestamp processed for each symbol (used for gap detection)
        self.last_tick_time = {}

        # Market data subscriber
        self.market_data = MarketDataSubscriber(callback=self.on_tick)
        
        # Bar completion callbacks
        # {timeframe: [callbacks]}
        self.bar_callbacks = defaultdict(list)
        
        # Initialize active timeframes for each subscribed symbol
        self.active_symbols = set()

        self.logger.info(f"TimeframeManager initialized with max_bars={max_bars}")
        
    def start(self):
        """Start the TimeframeManager and its market data subscription."""
        self.market_data.start()
        self.logger.info("TimeframeManager started")
        
    def stop(self):
        """Stop the TimeframeManager and its market data subscription."""
        self.market_data.stop()
        self.logger.info("TimeframeManager stopped")

    def subscribe(self, symbol: str, options_details: Dict[str, Any] = None) -> bool:
        """
        Subscribe to market data for a symbol and start tracking bars.
        
        Args:
            symbol: Symbol to subscribe to
            options_details: Optional details for options symbols
            
        Returns:
            True if subscription was successful, False otherwise
        """
        with self.lock:
            if symbol in self.active_symbols:
                return True
                
            # Store options details if provided
            if options_details:
                self.symbol_details[symbol] = options_details
            else:
                # Try to extract from symbol name
                details = OptionsGreeksCalculator.get_symbol_info(symbol)
                if details:
                    self.symbol_details[symbol] = details
            
            # Initialize data structures for active timeframes
            for timeframe in self.ACTIVE_TIMEFRAMES:
                if timeframe not in self.bars[symbol]:
                    self.bars[symbol][timeframe] = pd.DataFrame(
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest', 
                                'delta', 'gamma', 'theta', 'vega', 'rho']
                    )
                    self.bars[symbol][timeframe].set_index('timestamp', inplace=True)
            
            # Subscribe to market data
            success = self.market_data.subscribe(symbol)
            if success:
                self.active_symbols.add(symbol)
                
            return success
    
    def unsubscribe(self, symbol: str) -> bool:
        """
        Unsubscribe from market data for a symbol.
        
        Args:
            symbol: Symbol to unsubscribe from
            
        Returns:
            True if unsubscription was successful, False otherwise
        """
        with self.lock:
            if symbol not in self.active_symbols:
                return True
                
            success = self.market_data.unsubscribe(symbol)
            if success:
                self.active_symbols.remove(symbol)
                
            return success

    def on_tick(self, symbol: str, tick: Dict[str, Any]) -> Dict[str, bool]:
        """
        Process a tick received from market data subscription.
        
        Args:
            symbol: Symbol of the instrument
            tick: Tick data dictionary
            
        Returns:
            Dictionary mapping timeframes to boolean indicating if bar was completed
        """
        return self.process_tick(symbol, tick)
        
    def process_tick(self, symbol: str, tick: Dict[str, Any]) -> Dict[str, bool]:
        """
        Process a tick and update bars for all active timeframes.

        Args:
            symbol: Symbol of the instrument
            tick: Tick data dictionary with price, volume, timestamp, open_interest

        Returns:
            Dictionary mapping timeframes to boolean indicating if bar was completed
        """
        # Extract data from tick
        price = tick.get('price', 0.0)
        volume = tick.get('volume', 0.0)
        open_interest = tick.get('open_interest', 0.0)
        timestamp = tick.get('timestamp', 0.0)
        
        # Convert timestamp to seconds if in milliseconds
        if timestamp > 1e12:  # Likely in microseconds
            timestamp /= 1000000
        elif timestamp > 1e9:  # Likely in milliseconds
            timestamp /= 1000
            
        # Store underlying price if this is an underlying
        is_underlying = not bool(self.symbol_details.get(symbol, {}).get('is_option', False))
        if is_underlying:
            self.underlying_prices[symbol] = price
            
        # Update implied volatility if provided
        if 'implied_volatility' in tick:
            self.implied_volatility[symbol] = tick['implied_volatility']

        self.logger.debug(f"Processing tick for {symbol}: price={price:.2f}, volume={volume}, oi={open_interest}, timestamp={timestamp}")

        # Check for timestamp gaps
        if symbol in self.last_tick_time:
            gap = timestamp - self.last_tick_time[symbol]
            if gap > 10:  # Gap of more than 10 seconds
                self.logger.warning(f"Detected time gap of {gap:.2f} seconds for {symbol}")

        # Update last tick time
        self.last_tick_time[symbol] = timestamp

        with self.lock:
            # Dictionary to track which timeframes completed a bar
            completed_bars = {}

            # Update bars for active timeframes only
            for timeframe in self.ACTIVE_TIMEFRAMES:
                seconds = self.VALID_TIMEFRAMES[timeframe]

                # Check if we need to create a new bar or update existing one
                bar_timestamp = self._get_bar_timestamp(timestamp, seconds)

                # Initialize if this symbol+timeframe doesn't exist yet
                if timeframe not in self.current_bars[symbol]:
                    self.logger.debug(f"Creating first bar for {symbol} on {timeframe} timeframe at timestamp {bar_timestamp}")
                    
                    # Calculate Greeks if this is an option
                    greeks = self._calculate_greeks(symbol, price, timestamp)
                    
                    self.current_bars[symbol][timeframe] = {
                        'timestamp': bar_timestamp,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': volume,
                        'open_interest': open_interest,
                        'delta': greeks.get('delta', 0.0),
                        'gamma': greeks.get('gamma', 0.0),
                        'theta': greeks.get('theta', 0.0),
                        'vega': greeks.get('vega', 0.0),
                        'rho': greeks.get('rho', 0.0)
                    }
                    completed_bars[timeframe] = False
                else:
                    current_bar = self.current_bars[symbol][timeframe]

                    # Check if this tick belongs to a new bar
                    if bar_timestamp > current_bar['timestamp']:
                        # Store completed bar
                        self.logger.info(f"BAR COMPLETED: {symbol} {timeframe} - O:{current_bar['open']:.2f} H:{current_bar['high']:.2f} L:{current_bar['low']:.2f} C:{current_bar['close']:.2f} V:{current_bar['volume']:.0f} OI:{current_bar['open_interest']:.0f}")
                        self._store_completed_bar(symbol, timeframe, current_bar)
                        completed_bars[timeframe] = True
                        
                        # Notify callbacks
                        bar_copy = current_bar.copy()
                        for callback in self.bar_callbacks[timeframe]:
                            try:
                                callback(symbol, timeframe, bar_copy)
                            except Exception as e:
                                self.logger.error(f"Error in bar completion callback: {e}", exc_info=True)

                        # Calculate Greeks for the new bar
                        greeks = self._calculate_greeks(symbol, price, timestamp)
                        
                        # Create new bar
                        self.current_bars[symbol][timeframe] = {
                            'timestamp': bar_timestamp,
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': volume,
                            'open_interest': open_interest,
                            'delta': greeks.get('delta', 0.0),
                            'gamma': greeks.get('gamma', 0.0),
                            'theta': greeks.get('theta', 0.0),
                            'vega': greeks.get('vega', 0.0),
                            'rho': greeks.get('rho', 0.0)
                        }
                        self.logger.debug(f"Creating new bar for {symbol} on {timeframe} timeframe at timestamp {bar_timestamp}")
                    else:
                        # Update existing bar
                        current_bar['high'] = max(current_bar['high'], price)
                        current_bar['low'] = min(current_bar['low'], price)
                        current_bar['close'] = price
                        current_bar['volume'] += volume
                        current_bar['open_interest'] = open_interest  # Replace with latest OI
                        
                        # Update Greeks at certain intervals to avoid excessive calculations
                        # For now, update on every tick for accuracy
                        greeks = self._calculate_greeks(symbol, price, timestamp)
                        if greeks:
                            current_bar['delta'] = greeks.get('delta', current_bar['delta'])
                            current_bar['gamma'] = greeks.get('gamma', current_bar['gamma'])
                            current_bar['theta'] = greeks.get('theta', current_bar['theta'])
                            current_bar['vega'] = greeks.get('vega', current_bar['vega'])
                            current_bar['rho'] = greeks.get('rho', current_bar['rho'])
                            
                        completed_bars[timeframe] = False

            return completed_bars

    def _calculate_greeks(self, symbol: str, price: float, timestamp: float) -> Dict[str, float]:
        """
        Calculate option Greeks for a symbol.
        
        Args:
            symbol: Option symbol
            price: Current option price
            timestamp: Current timestamp
            
        Returns:
            Dictionary with Greeks or empty dict if not an option
        """
        # Check if this is an option
        option_details = self.symbol_details.get(symbol)
        if not option_details or not option_details.get('is_option', False):
            return {}
            
        # Get underlying price
        underlying_symbol = option_details.get('underlying')
        if not underlying_symbol or underlying_symbol not in self.underlying_prices:
            return {}
            
        underlying_price = self.underlying_prices[underlying_symbol]
        
        # Get option details
        option_type = option_details.get('option_type', '').lower()
        strike_price = option_details.get('strike', 0.0)
        expiry_date = option_details.get('expiry')
        
        if not option_type or not strike_price or not expiry_date:
            return {}
            
        # Calculate time to expiry in years
        current_time = datetime.fromtimestamp(timestamp)
        time_to_expiry = (expiry_date - current_time).total_seconds() / (365.25 * 24 * 3600)
        
        if time_to_expiry <= 0:
            return {}
            
        # Get volatility - either from tick, cached value, or default
        volatility = self.implied_volatility.get(symbol, 0.2)  # Default to 20%
        
        # Calculate Greeks
        try:
            return OptionsGreeksCalculator.calculate_greeks(
                option_type=option_type,
                underlying_price=underlying_price,
                strike_price=strike_price,
                time_to_expiry=time_to_expiry,
                risk_free_rate=self.risk_free_rate,
                volatility=volatility
            )
        except Exception as e:
            self.logger.error(f"Error calculating Greeks for {symbol}: {e}")
            return {}

    def _get_bar_timestamp(self, tick_timestamp: float, timeframe_seconds: int) -> float:
        """
        Calculate the start timestamp for a bar given a tick timestamp and timeframe in seconds.

        Args:
            tick_timestamp: Timestamp of the tick in seconds (unix time)
            timeframe_seconds: Number of seconds in the timeframe

        Returns:
            Start timestamp for the bar that contains this tick
        """
        if timeframe_seconds == 0:
            return tick_timestamp

        # Round down to the nearest timeframe interval
        return (int(tick_timestamp) // timeframe_seconds) * timeframe_seconds

    def _store_completed_bar(self, symbol: str, timeframe: str, bar: Dict[str, Any]) -> None:
        """
        Store a completed bar in the appropriate timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bar
            bar: Bar data dictionary
        """
        # Initialize DataFrame if it doesn't exist
        if timeframe not in self.bars[symbol]:
            self.bars[symbol][timeframe] = pd.DataFrame(
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_interest',
                         'delta', 'gamma', 'theta', 'vega', 'rho']
            )
            self.bars[symbol][timeframe].set_index('timestamp', inplace=True)

        # Add the new bar
        new_row = pd.DataFrame([bar])
        new_row.set_index('timestamp', inplace=True)

        # Concatenate DataFrames
        if not self.bars[symbol][timeframe].empty:
            self.bars[symbol][timeframe] = pd.concat(
                [self.bars[symbol][timeframe], new_row],
                ignore_index=False
            )
        else:
            self.bars[symbol][timeframe] = new_row
            
        # Trim to max bars
        if len(self.bars[symbol][timeframe]) > self.max_bars:
            self.bars[symbol][timeframe] = self.bars[symbol][timeframe].iloc[-self.max_bars:]

    def get_bars(self, symbol: str, timeframe: str, limit: int = None) -> Optional[pd.DataFrame]:
        """
        Get historical bars for a specific symbol and timeframe.

        Args:
            symbol: Symbol of the instrument
            timeframe: Timeframe of the bars to retrieve
            limit: Optional limit on the number of bars to return (default: all available)

        Returns:
            DataFrame of OHLCV+OI+Greeks bars or None if not available
        """
        with self.lock:
            # Check if timeframe is active or needs to be calculated
            if timeframe not in self.ACTIVE_TIMEFRAMES and timeframe in self.VALID_TIMEFRAMES:
                # Calculate on-demand timeframe from active timeframes
                self._calculate_timeframe_on_demand(symbol, timeframe)
                
            if symbol not in self.bars or timeframe not in self.bars[symbol]:
                return None

            df = self.bars[symbol][timeframe]
            if limit:
                return df.iloc[-limit:].copy()
            return df.copy()

    def _calculate_timeframe_on_demand(self, symbol: str, target_timeframe: str) -> bool:
        """
        Calculate bars for a timeframe that's not actively maintained.
        
        Args:
            symbol: Symbol to calculate for
            target_timeframe: Timeframe to calculate
            
        Returns:
            True if calculation was successful, False otherwise
        """
        if target_timeframe not in self.VALID_TIMEFRAMES:
            return False
            
        if symbol not in self.bars:
            return False
            
        # Find smallest active timeframe that's available
        source_timeframe = None
        for tf in self.ACTIVE_TIMEFRAMES:
            if tf in self.bars[symbol]:
                source_timeframe = tf
                break
                
        if not source_timeframe:
            return False
            
        source_seconds = self.VALID_TIMEFRAMES[source_timeframe]
        target_seconds = self.VALID_TIMEFRAMES[target_timeframe]
        
        if target_seconds <= source_seconds:
            self.logger.warning(f"Cannot calculate {target_timeframe} from {source_timeframe}")
            return False
            
        # Get source data
        source_df = self.bars[symbol][source_timeframe]
        if source_df.empty:
            return False
            
        # Reset index for resampling
        df = source_df.reset_index()
        
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('datetime', inplace=True)
        
        # Determine resample rule
        if target_seconds < 60:
            rule = f"{target_seconds}S"
        elif target_seconds < 3600:
            rule = f"{target_seconds // 60}T"
        elif target_seconds < 86400:
            rule = f"{target_seconds // 3600}H"
        elif target_seconds < 604800:
            rule = f"{target_seconds // 86400}D"
        elif target_seconds < 2592000:
            rule = f"{target_seconds // 604800}W"
        else:
            rule = f"{target_seconds // 2592000}M"
            
        # Resample
        resampled = df.resample(rule).agg({
            'timestamp': 'first',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            '