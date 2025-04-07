import logging
import pandas as pd
import numpy as np
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

from strategies.strategy import Strategy
from utils.constants import SignalType, OrderSide, OrderType, EventType
from models.instrument import Instrument
from models.events import MarketDataEvent, BarEvent

class MultiTimeframeStrategy(Strategy):
    """
    A sample strategy that demonstrates working with multiple timeframes.
    This strategy uses a combination of short-term and long-term timeframes 
    to make trading decisions.
    
    The strategy implements a simple moving average crossover with these rules:
    1. Uses both fast (shorter) and slow (longer) timeframes
    2. Computes moving averages on both timeframes
    3. Generates buy signals when fast MA crosses above slow MA
    4. Generates sell signals when fast MA crosses below slow MA
    5. Uses the slow timeframe for trend confirmation
    """
    
    def __init__(self, config: Dict[str, Any], data_manager, portfolio, event_manager):
        """
        Initialize the strategy.
        
        Args:
            config: Strategy configuration
            data_manager: Market data manager instance
            portfolio: Portfolio manager instance
            event_manager: Event manager instance
        """
        # Set default configuration values
        config_defaults = {
            'name': 'Multi-Timeframe MA Crossover',
            'description': 'Moving average crossover strategy that uses multiple timeframes',
            'timeframe': '1m',  # Primary (fast) timeframe
            'additional_timeframes': ['5m'],  # Secondary (slow) timeframe
            'fast_ma_period': 10,  # Fast MA period for primary timeframe
            'slow_ma_period': 20,  # Slow MA period for primary timeframe
            'confirmation_ma_period': 50,  # MA period for confirmation on slow timeframe
            'trade_size': 10,  # Default position size
            'max_bars': 200,  # Maximum number of bars to keep in memory
            'instruments': []  # To be populated with actual instruments
        }
        
        # Update config with defaults for any missing values
        for key, value in config_defaults.items():
            if key not in config:
                config[key] = value
        
        # Call parent constructor
        super().__init__(config, data_manager, portfolio, event_manager)
        
        # Strategy-specific configurations
        self.fast_ma_period = config.get('fast_ma_period', 10)
        self.slow_ma_period = config.get('slow_ma_period', 20)
        self.confirmation_ma_period = config.get('confirmation_ma_period', 50)
        self.trade_size = config.get('trade_size', 10)
        
        # Store previous MA values to detect crossovers
        self.previous_ma_values = {}  # symbol -> {timeframe -> {fast: value, slow: value}}
        
        # Track signals already generated to avoid duplicates
        self.last_signal_type = {}  # symbol -> SignalType
        
        self.logger.info(f"Multi-Timeframe Strategy initialized with fast timeframe: {self.timeframe}, " +
                         f"slow timeframe: {config.get('additional_timeframes', ['None'])}")
    
    def on_start(self):
        """Called when the strategy is started."""
        self.logger.info("Multi-Timeframe Strategy started")
        self.previous_ma_values = {}
        self.last_signal_type = {}
    
    def on_market_data(self, event: MarketDataEvent):
        """
        Process incoming market data events.
        This method is required by the Strategy abstract class.
        
        Args:
            event: Market data event
        """
        if not isinstance(event, MarketDataEvent):
            return
            
        # For this strategy, we rely on bar data processed by the DataManager
        # We don't need to directly process tick data, as it will be aggregated into bars
        # and we'll receive those via on_bar method
        pass
    
    def on_bar(self, event: BarEvent):
        """
        Process incoming bar events.
        This is where the strategy's main logic is implemented.
        
        Args:
            event: Bar event with OHLCV data
        """
        if not isinstance(event, BarEvent):
            self.logger.warning(f"Received non-BarEvent in on_bar: {type(event)}")
            return
            
        # Extract data from event
        instrument = event.instrument
        symbol = instrument.symbol
        timeframe = event.timeframe
        
        # Enhanced logging for debugging
        self.logger.info(f"Received {timeframe} bar for {symbol}: " +
                         f"O: {event.open_price:.2f}, H: {event.high_price:.2f}, " +
                         f"L: {event.low_price:.2f}, C: {event.close_price:.2f}")
        
        # Get all bars for this symbol and timeframe
        bars = self.get_bars(symbol, timeframe)
        
        # Add debugging to check what bars we have
        if bars:
            self.logger.info(f"Have {len(bars)} {timeframe} bars for {symbol}")
        else:
            self.logger.warning(f"No bars returned from get_bars for {symbol} on {timeframe}")
        
        # Need at least enough bars to calculate the indicators
        min_bars_needed = max(self.fast_ma_period, self.slow_ma_period, self.confirmation_ma_period)
        if bars is None or len(bars) < min_bars_needed:
            self.logger.info(f"Not enough {timeframe} bars for {symbol}: {len(bars) if bars else 0}/{min_bars_needed}")
            return
            
        # Calculate indicators based on timeframe
        if timeframe == self.timeframe:  # Primary (fast) timeframe
            self.logger.info(f"Processing primary timeframe {timeframe} for {symbol}")
            # Process the primary timeframe logic
            self._process_primary_timeframe(symbol, bars)
        else:
            self.logger.info(f"Processing confirmation timeframe {timeframe} for {symbol}")
            # Process confirmation timeframe logic
            self._process_confirmation_timeframe(symbol, bars)
    
    def _process_primary_timeframe(self, symbol: str, bars: List[BarEvent]):
        """
        Process primary (fast) timeframe data.
        
        Args:
            symbol: Instrument symbol
            bars: List of bars for this symbol and timeframe
        """
        # Extract close prices
        close_prices = [bar.close_price for bar in bars]
        
        # Calculate fast and slow MAs
        fast_ma = self._calculate_sma(close_prices, self.fast_ma_period)
        slow_ma = self._calculate_sma(close_prices, self.slow_ma_period)
        
        if fast_ma is None or slow_ma is None:
            self.logger.debug(f"Not enough data to calculate MAs for {symbol} on primary timeframe")
            return
            
        # Get current and previous values
        current_fast_ma = fast_ma[-1]
        current_slow_ma = slow_ma[-1]
        
        # Initialize storage if needed
        if symbol not in self.previous_ma_values:
            self.previous_ma_values[symbol] = {}
            
        if self.timeframe not in self.previous_ma_values[symbol]:
            self.previous_ma_values[symbol][self.timeframe] = {
                'fast': current_fast_ma,
                'slow': current_slow_ma
            }
            self.logger.info(f"Initial MA values for {symbol} on {self.timeframe}: Fast MA: {current_fast_ma:.2f}, Slow MA: {current_slow_ma:.2f}")
            return
            
        # Get previous values
        prev_fast_ma = self.previous_ma_values[symbol][self.timeframe]['fast']
        prev_slow_ma = self.previous_ma_values[symbol][self.timeframe]['slow']
        
        # Log current MA values
        self.logger.debug(f"{symbol} [{self.timeframe}] - Fast MA: {current_fast_ma:.2f}, Slow MA: {current_slow_ma:.2f}, " +
                         f"Previous Fast: {prev_fast_ma:.2f}, Previous Slow: {prev_slow_ma:.2f}")
        
        # Store current values for next time
        self.previous_ma_values[symbol][self.timeframe]['fast'] = current_fast_ma
        self.previous_ma_values[symbol][self.timeframe]['slow'] = current_slow_ma
        
        # Bullish crossover: fast MA crosses above slow MA
        is_bullish_crossover = prev_fast_ma <= prev_slow_ma and current_fast_ma > current_slow_ma
        
        # Bearish crossover: fast MA crosses below slow MA
        is_bearish_crossover = prev_fast_ma >= prev_slow_ma and current_fast_ma < current_slow_ma
        
        if is_bullish_crossover:
            self.logger.info(f"BULLISH CROSSOVER detected for {symbol} on {self.timeframe} - Fast MA {current_fast_ma:.2f} crossed above Slow MA {current_slow_ma:.2f}")
        elif is_bearish_crossover:
            self.logger.info(f"BEARISH CROSSOVER detected for {symbol} on {self.timeframe} - Fast MA {current_fast_ma:.2f} crossed below Slow MA {current_slow_ma:.2f}")
        
        # Check if trend is confirmed by the slow timeframe
        # If we don't have slow timeframe data yet, don't act
        slow_timeframe = next(iter(self.additional_timeframes), None)
        if slow_timeframe is None:
            return
            
        # Only generate signals if we've confirmed the trend on the slower timeframe
        if symbol in self.previous_ma_values and slow_timeframe in self.previous_ma_values[symbol]:
            # If we have an uptrend confirmation and a bullish crossover on the fast timeframe
            if (self.previous_ma_values[symbol][slow_timeframe].get('trend_confirmed', False) == 'UP' and 
                is_bullish_crossover):
                # Check if we already have a long position or we just sent a buy signal
                current_position_side = self.get_position_side(symbol)
                last_signal = self.last_signal_type.get(symbol)
                
                self.logger.info(f"UP trend confirmed for {symbol}, current position: {current_position_side}, last signal: {last_signal}")
                
                if current_position_side != 'LONG' and last_signal != SignalType.ENTRY:
                    self._generate_buy_signal(symbol, bars[-1].close_price)
                    
            # If we have a downtrend confirmation and a bearish crossover on the fast timeframe
            elif (self.previous_ma_values[symbol][slow_timeframe].get('trend_confirmed', False) == 'DOWN' and 
                  is_bearish_crossover):
                # Check if we already have a short position or we just sent a sell signal
                current_position_side = self.get_position_side(symbol)
                last_signal = self.last_signal_type.get(symbol)
                
                self.logger.info(f"DOWN trend confirmed for {symbol}, current position: {current_position_side}, last signal: {last_signal}")
                
                if current_position_side == 'LONG' and last_signal != SignalType.EXIT:
                    self._generate_sell_signal(symbol, bars[-1].close_price)
    
    def _process_confirmation_timeframe(self, symbol: str, bars: List[BarEvent]):
        """
        Process confirmation (slow) timeframe data.
        
        Args:
            symbol: Instrument symbol
            bars: List of bars for this symbol and timeframe
        """
        # Get the confirmation timeframe (the first in the additional_timeframes list)
        confirmation_timeframe = next(iter(self.additional_timeframes))
        
        # Extract close prices
        close_prices = [bar.close_price for bar in bars]
        
        # Calculate MA for trend confirmation
        confirmation_ma = self._calculate_sma(close_prices, self.confirmation_ma_period)
        if confirmation_ma is None:
            self.logger.debug(f"Not enough data to calculate confirmation MA for {symbol}")
            return
            
        # Get current value
        current_confirmation_ma = confirmation_ma[-1]
        current_price = close_prices[-1]
        
        # Initialize storage if needed
        if symbol not in self.previous_ma_values:
            self.previous_ma_values[symbol] = {}
            
        if confirmation_timeframe not in self.previous_ma_values[symbol]:
            self.previous_ma_values[symbol][confirmation_timeframe] = {
                'confirmation_ma': current_confirmation_ma
            }
            self.logger.info(f"Initial confirmation MA for {symbol} on {confirmation_timeframe}: {current_confirmation_ma:.2f}")
            return
            
        # Determine trend based on price vs. MA
        if current_price > current_confirmation_ma:
            trend = 'UP'
        else:
            trend = 'DOWN'
            
        prev_trend = self.previous_ma_values[symbol][confirmation_timeframe].get('trend_confirmed')
        if prev_trend != trend:
            self.logger.info(f"Trend change for {symbol} on {confirmation_timeframe}: {prev_trend if prev_trend else 'NONE'} -> {trend}")
            self.logger.info(f"  Price: {current_price:.2f}, Confirmation MA: {current_confirmation_ma:.2f}")
        
        # Store the confirmed trend
        self.previous_ma_values[symbol][confirmation_timeframe]['trend_confirmed'] = trend
        self.previous_ma_values[symbol][confirmation_timeframe]['confirmation_ma'] = current_confirmation_ma
        
        self.logger.debug(f"{symbol} [{confirmation_timeframe}] - Trend: {trend}, " +
                         f"Price: {current_price:.2f}, Confirmation MA: {current_confirmation_ma:.2f}")
    
    def _calculate_sma(self, prices: List[float], period: int) -> Optional[List[float]]:
        """
        Calculate Simple Moving Average.
        
        Args:
            prices: List of price values
            period: MA period
            
        Returns:
            List of MA values or None if not enough data
        """
        if len(prices) < period:
            return None
            
        # Convert to numpy array for easier calculation
        price_array = np.array(prices)
        
        # Calculate SMA
        sma = []
        for i in range(period - 1, len(price_array)):
            sma.append(np.mean(price_array[i - period + 1:i + 1]))
            
        return sma
    
    def _generate_buy_signal(self, symbol: str, price: float):
        """
        Generate a buy signal.
        
        Args:
            symbol: Instrument symbol
            price: Current price
        """
        # Find the instrument object for this symbol
        instrument = None
        for inst in self.instruments:
            if inst.symbol == symbol:
                instrument = inst
                break
                
        if instrument is None:
            self.logger.error(f"Cannot generate buy signal: Instrument not found for symbol {symbol}")
            return
        
        # Create signal data
        signal_data = {
            'side': OrderSide.BUY,
            'quantity': self.trade_size,
            'price': price,
            'order_type': OrderType.MARKET
        }
        
        self.logger.info(f"Generating BUY signal for {symbol} at {price:.2f}")
        
        # Generate the signal
        try:
            self.generate_signal(symbol, SignalType.ENTRY, signal_data)
            
            # Record this signal type
            self.last_signal_type[symbol] = SignalType.ENTRY
            
            self.logger.info(f"Successfully generated BUY signal for {symbol} at {price:.2f}")
        except Exception as e:
            self.logger.error(f"Error generating BUY signal for {symbol}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _generate_sell_signal(self, symbol: str, price: float):
        """
        Generate a sell signal.
        
        Args:
            symbol: Instrument symbol
            price: Current price
        """
        # Find the instrument object for this symbol
        instrument = None
        for inst in self.instruments:
            if inst.symbol == symbol:
                instrument = inst
                break
                
        if instrument is None:
            self.logger.error(f"Cannot generate sell signal: Instrument not found for symbol {symbol}")
            return
        
        # Get current position size
        position = self.get_position(symbol)
        quantity = position.get('quantity', 0) if position else 0
        
        # Use position size or default size
        sell_quantity = quantity if quantity > 0 else self.trade_size
        
        # Create signal data
        signal_data = {
            'side': OrderSide.SELL,
            'quantity': sell_quantity,
            'price': price,
            'order_type': OrderType.MARKET
        }
        
        self.logger.info(f"Generating SELL signal for {symbol} at {price:.2f}, quantity: {sell_quantity}")
        
        # Generate the signal
        try:
            self.generate_signal(symbol, SignalType.EXIT, signal_data)
            
            # Record this signal type
            self.last_signal_type[symbol] = SignalType.EXIT
            
            self.logger.info(f"Successfully generated SELL signal for {symbol} at {price:.2f}")
        except Exception as e:
            self.logger.error(f"Error generating SELL signal for {symbol}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def run_iteration(self):
        """
        Strategy's main loop run in its own thread.
        Used for any periodic operations that aren't driven by market data events.
        """
        # Just a placeholder - most logic is in on_bar handler
        time.sleep(0.1)
        
    def get_state(self):
        """
        Get the current state of the strategy.
        
        Returns:
            dict: Dictionary containing strategy state information
        """
        state = {
            'name': self.name,
            'timeframe': self.timeframe,
            'additional_timeframes': list(self.additional_timeframes),
            'positions': {},
            'indicators': {}
        }
        
        # Add position information
        for symbol in [instrument.symbol for instrument in self.instruments]:
            position = self.get_position(symbol)
            if position:
                state['positions'][symbol] = position
            
            # Add MA values if available
            if symbol in self.previous_ma_values:
                state['indicators'][symbol] = {}
                for tf, values in self.previous_ma_values[symbol].items():
                    state['indicators'][symbol][tf] = {
                        'fast_ma': values.get('fast'),
                        'slow_ma': values.get('slow')
                    }
                    
        return state 