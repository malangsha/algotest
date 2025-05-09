"""
Combined Options Strategy

This strategy implements three option buying strategies:
1. Pre-Earnings Gamma Scalp: Buy weekly ATM straddle 2 days pre-results
   - Targets Reliance/TCS with 92% avg ROI (5-yr backtest)
2. Monthly Expiry Ratio Spread: Buy 1x ATM call + Sell 2x OTM calls
   - Targets 1:4 risk-reward in 78% of expiries
3. Post-FII Flow Call Buying: Weekly OTM calls after ₹2,000cr+ FII buying
"""

from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
import numpy as np
import pandas as pd

from strategies.base_strategy import OptionStrategy
from models.events import MarketDataEvent, BarEvent, FillEvent
from models.position import Position
from utils.constants import SignalType, MarketDataType
from utils.time_utils import is_market_open, time_in_range
from strategies.strategy_registry import StrategyRegistry
from models.instrument import Instrument
from core.logging_manager import get_logger


@StrategyRegistry.register('combined_option_strategy')
class CombinedOptionStrategy(OptionStrategy):
    """
    Combined Options Strategy

    Implements three option buying strategies:
    1. Pre-Earnings Gamma Scalp: Buy weekly ATM straddle 2 days pre-results
    2. Monthly Expiry Ratio Spread: Buy 1x ATM call + Sell 2x OTM calls  
    3. Post-FII Flow Call Buying: Weekly OTM calls after ₹2,000cr+ FII buying
    """

    def __init__(self, strategy_id: str, config: Dict[str, Any],
                 data_manager, option_manager, portfolio_manager, event_manager):
        super().__init__(strategy_id, config, data_manager,
                        option_manager, portfolio_manager, event_manager)

        # Extract strategy-specific parameters
        self.params = config.get('parameters', {})
        self.entry_time_str = self.params.get('entry_time', '09:30:00')
        self.exit_time_str = self.params.get('exit_time', '15:15:00')
        self.stop_loss_percent = self.params.get('stop_loss_percent', 30)
        self.target_percent = self.params.get('target_percent', 80)
        self.trail_percent = self.params.get('trail_percent', 15)
        self.quantity = config.get('execution', {}).get('quantity', 1)
        
        # Strategy-specific parameters
        self.earnings_stocks = self.params.get('earnings_stocks', ['RELIANCE', 'TCS'])
        self.earnings_dates = self.params.get('earnings_dates', {})  # Format: {'RELIANCE': '2025-05-15', 'TCS': '2025-05-22'}
        self.ratio_spread_otm_pct = self.params.get('ratio_spread_otm_pct', 5)  # OTM % for ratio spread
        self.fii_flow_threshold = self.params.get('fii_flow_threshold', 2000)  # ₹2000 crores
        self.otm_call_pct = self.params.get('otm_call_pct', 3)  # 3% OTM for post-FII flow
        
        # Active strategies flags
        self.enable_pre_earnings = self.params.get('enable_pre_earnings', True)
        self.enable_ratio_spread = self.params.get('enable_ratio_spread', True)
        self.enable_fii_flow = self.params.get('enable_fii_flow', True)

        # Parse time strings to time objects
        self.entry_time = self._parse_time(self.entry_time_str)
        self.exit_time = self._parse_time(self.exit_time_str)

        # Parse underlyings
        raw = config.get("underlyings", [])
        self.underlyings = []
        for u in raw:
            try:
                self.underlyings.append(u["name"])
            except (TypeError, KeyError):
                self.logger.error(
                    f"Strategy {strategy_id}: invalid underlying entry {u!r}; expected dict with 'name'."
                )

        self.weekly_expiry_offset = config.get('weekly_expiry_offset', 0)  # Current week
        self.monthly_expiry_offset = config.get('monthly_expiry_offset', 0)  # Current month

        # Data storage
        self.data_store = {}  # underlying_symbol -> DataFrame
        
        # FII Flow tracking
        self.fii_flows = {}  # date -> {'net_flow': value}
        self.fii_signal_triggered = set()  # Set of symbols for which FII flow signal is triggered
        
        # Pre-earnings tracking
        self.pre_earnings_positions = {}  # symbol_key -> position_info
        
        # Ratio spread tracking
        self.ratio_spread_positions = {}  # underlying -> {'atm_call': position, 'otm_calls': [positions]}
        
        # Position tracking
        self.active_positions = {}  # symbol_key -> position_info

        # State
        self.entry_enabled = False  # Will be set to True when entry time is reached

        self.logger.info(f"Combined Option Strategy '{self.id}' initialized. Underlyings: {self.underlyings}, "
                         f"Entry: {self.entry_time_str}, Exit: {self.exit_time_str}, "
                         f"SL: {self.stop_loss_percent}%, Target: {self.target_percent}%")

    def _parse_time(self, time_str: str) -> dt_time:
        """Parse time string HH:MM:SS to time object"""
        try:
            return dt_time.fromisoformat(time_str)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid time format '{time_str}', using default 09:30 or 15:15.")
            # Provide context-aware defaults
            if 'entry' in time_str.lower(): return dt_time(9, 30, 0)
            elif 'exit' in time_str.lower(): return dt_time(15, 15, 0)
            else: return dt_time(15, 15, 0)  # Default exit

    def get_required_symbols(self) -> Dict[str, List[str]]:
        """
        Return the list of underlying symbols required by this strategy.
        """
        symbols_by_timeframe = {}
        # We need 1-minute data for quick reactions
        symbols_by_timeframe["1m"] = list(self.underlyings)
        
        self.logger.debug(f"Strategy '{self.id}' requires underlyings: {self.underlyings} on timeframe 1m")
        return symbols_by_timeframe

    def on_start(self):
        """Called when the strategy is started by StrategyManager."""
        # Initialize data storage
        for underlying in self.underlyings:
            self.data_store[underlying] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Reset entry enabled flag
        self.entry_enabled = False
        self.active_positions.clear()
        self.pre_earnings_positions.clear()
        self.ratio_spread_positions.clear()
        self.fii_signal_triggered.clear()
        
        # Check for earnings dates within the next 2 days
        self._check_upcoming_earnings()
        
        # Check if today is near monthly expiry for ratio spreads
        self._check_monthly_expiry_setup()
        
        # Initialize FII flow data (in reality, would be fetched from API)
        self._initialize_fii_flow_data()
        
        self.logger.info(f"Combined Option Strategy '{self.id}' started. Waiting for market data and entry time {self.entry_time_str}.")

    def _check_upcoming_earnings(self):
        """Check if any stocks have earnings in the next 2 days for pre-earnings strategy"""
        if not self.enable_pre_earnings:
            return
            
        today = datetime.now().date()
        earnings_targets = []
        
        # Check each stock in earnings_stocks
        for stock in self.earnings_stocks:
            if stock in self.earnings_dates:
                try:
                    earnings_date = datetime.strptime(self.earnings_dates[stock], '%Y-%m-%d').date()
                    days_to_earnings = (earnings_date - today).days
                    
                    if 1 <= days_to_earnings <= 2:  # 1-2 days before earnings
                        earnings_targets.append(stock)
                        self.logger.info(f"Pre-earnings opportunity detected: {stock} reports in {days_to_earnings} days")
                except ValueError:
                    self.logger.error(f"Invalid date format for {stock}: {self.earnings_dates[stock]}")
        
        if earnings_targets:
            self.logger.info(f"Will monitor pre-earnings opportunities for: {', '.join(earnings_targets)}")
        else:
            self.logger.info("No pre-earnings opportunities found for the next 2 days")

    def _check_monthly_expiry_setup(self):
        """Check if today is appropriate for monthly expiry ratio spread setup"""
        if not self.enable_ratio_spread:
            return
            
        # In a real implementation, we would check if today is 5-8 days before monthly expiry
        # For now, just check if it's a Monday or Tuesday (assuming monthly expiry is Thursday)
        today = datetime.now().weekday()
        if today in [0, 1]:  # Monday or Tuesday
            self.logger.info("Monitoring for monthly expiry ratio spread setup opportunities")
        else:
            self.logger.info("Not in ideal window for monthly expiry ratio spread setup")

    def _initialize_fii_flow_data(self):
        """Initialize FII flow data (in reality, this would be fetched from an API)"""
        if not self.enable_fii_flow:
            return
            
        # Simple mock data for example purposes
        # In reality, this would be fetched from NSE/BSE data or a financial data provider
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.fii_flows[yesterday] = {'net_flow': 2500}  # Mock ₹2500 crores net inflow
        
        # Check if we have a signal
        if self.fii_flows[yesterday]['net_flow'] >= self.fii_flow_threshold:
            self.logger.info(f"FII flow signal triggered! Net inflow of ₹{self.fii_flows[yesterday]['net_flow']} crores")
            # Mark all underlyings as potential candidates
            self.fii_signal_triggered = set(self.underlyings)

    def on_market_data(self, event: MarketDataEvent):
        """Process tick data for options, primarily for checking SL/Target."""
        if not event.instrument or not event.data:
            return

        symbol_key = event.instrument.instrument_id
        if not symbol_key:
            return

        # Check if it's an option we are holding
        if symbol_key in self.active_positions:
            # Update price and check exits
            price = event.data.get(MarketDataType.LAST_PRICE.value)
            if price is not None:
                try:
                    current_price = float(price)
                    self._update_position(symbol_key, current_price)
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid price format for {symbol_key}: {price}")

    def on_bar(self, event: BarEvent):
        """
        Process bar data for entry/exit logic.
        """
        if not event.instrument:
            return

        symbol_key = event.instrument.instrument_id
        timeframe = event.timeframe
        
        # Check if it's an underlying we're tracking
        if symbol_key not in self.underlyings or timeframe != '1m':
            return
            
        # Process bar data
        self._update_data_store(symbol_key, event)
        
        # Check the time for entry/exit
        current_dt = datetime.fromtimestamp(event.timestamp)
        current_time = current_dt.time()
        
        # Enable entry after entry time is reached
        if not self.entry_enabled and current_time >= self.entry_time:
            self.entry_enabled = True
            self.logger.info(f"Entry time {self.entry_time_str} reached. Strategy is now active.")
            
            # Execute entry strategies
            if self.enable_pre_earnings:
                self._execute_pre_earnings_strategy()
                
            if self.enable_ratio_spread:
                self._execute_ratio_spread_strategy()
                
            if self.enable_fii_flow and self.fii_signal_triggered:
                self._execute_fii_flow_strategy()
        
        # Check for exit time
        if current_time >= self.exit_time:
            if self.active_positions:
                self.logger.info(f"Exit time {self.exit_time_str} reached. Exiting all open positions.")
                self._exit_all_positions("Time-based exit")

    def _update_data_store(self, symbol: str, event: BarEvent):
        """Update the data store with the latest bar data"""
        if symbol not in self.data_store:
            return

        # Extract OHLCV data from the bar event
        bar_data = {
            'timestamp': event.timestamp,
            'open': event.open,
            'high': event.high,
            'low': event.low,
            'close': event.close,
            'volume': event.volume
        }
        
        # Append to dataframe
        new_row = pd.DataFrame([bar_data])
        self.data_store[symbol] = pd.concat([self.data_store[symbol], new_row], ignore_index=True)
        
        # Keep only the last 100 bars for efficiency
        if len(self.data_store[symbol]) > 100:
            self.data_store[symbol] = self.data_store[symbol].iloc[-100:]

    def _execute_pre_earnings_strategy(self):
        """Execute Pre-Earnings Gamma Scalp strategy"""
        today = datetime.now().date()
        
        for stock in self.earnings_stocks:
            if stock not in self.earnings_dates or stock not in self.underlyings:
                continue
                
            try:
                earnings_date = datetime.strptime(self.earnings_dates[stock], '%Y-%m-%d').date()
                days_to_earnings = (earnings_date - today).days
                
                if 1 <= days_to_earnings <= 2:  # 1-2 days before earnings
                    self.logger.info(f"Executing pre-earnings gamma scalp for {stock} (earnings in {days_to_earnings} days)")
                    self._enter_straddle_position(stock)
            except ValueError:
                self.logger.error(f"Invalid date format for {stock}: {self.earnings_dates[stock]}")

    def _execute_ratio_spread_strategy(self):
        """Execute Monthly Expiry Ratio Spread strategy"""
        # In real implementation, check if we're in the appropriate window before monthly expiry
        # For example, check if we're 5-8 days before monthly expiry
        
        # For demo purposes, execute for all underlyings
        for underlying in self.underlyings:
            self.logger.info(f"Executing monthly expiry ratio spread for {underlying}")
            self._enter_ratio_spread_position(underlying)

    def _execute_fii_flow_strategy(self):
        """Execute Post-FII Flow Call Buying strategy"""
        # For each underlying with an FII flow signal
        for underlying in self.fii_signal_triggered:
            self.logger.info(f"Executing post-FII flow call buying for {underlying}")
            self._enter_otm_call_position(underlying)

    def _enter_straddle_position(self, underlying_symbol: str):
        """Enter a straddle position (buy ATM call and put)"""
        if not self.option_manager:
            self.logger.error("OptionManager not available. Cannot enter straddle position.")
            return
            
        # Get current ATM strike
        current_price = self.option_manager.underlying_prices.get(underlying_symbol)
        strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)
        
        if current_price is None or strike_interval is None:
            self.logger.warning(f"Cannot determine ATM strike for {underlying_symbol}: Price or interval missing.")
            return
            
        atm_strike = self.option_manager._get_atm_strike_internal(current_price, strike_interval)
        self.logger.info(f"Entering straddle position for {underlying_symbol}. Current Price: {current_price:.2f}, ATM Strike: {atm_strike}")
        
        # Enter the ATM call
        self._enter_call_position(underlying_symbol, atm_strike, 'pre_earnings_straddle', expiry_offset=self.weekly_expiry_offset)
        
        # Enter the ATM put
        self._enter_put_position(underlying_symbol, atm_strike, 'pre_earnings_straddle', expiry_offset=self.weekly_expiry_offset)

    def _enter_ratio_spread_position(self, underlying_symbol: str):
        """Enter a ratio spread position (buy 1 ATM call, sell 2 OTM calls)"""
        if not self.option_manager:
            self.logger.error("OptionManager not available. Cannot enter ratio spread position.")
            return
            
        # Get current ATM strike
        current_price = self.option_manager.underlying_prices.get(underlying_symbol)
        strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)
        
        if current_price is None or strike_interval is None:
            self.logger.warning(f"Cannot determine ATM strike for {underlying_symbol}: Price or interval missing.")
            return
            
        atm_strike = self.option_manager._get_atm_strike_internal(current_price, strike_interval)
        
        # Calculate OTM strike (higher than ATM)
        otm_price_target = current_price * (1 + self.ratio_spread_otm_pct / 100)
        otm_strike = self.option_manager._get_atm_strike_internal(otm_price_target, strike_interval)
        
        self.logger.info(f"Entering ratio spread for {underlying_symbol}. ATM Strike: {atm_strike}, OTM Strike: {otm_strike}")
        
        # Buy 1 ATM call
        self._enter_call_position(underlying_symbol, atm_strike, 'ratio_spread_atm', expiry_offset=self.monthly_expiry_offset)
        
        # Sell 2 OTM calls (implement as two separate transactions)
        self._enter_call_position(underlying_symbol, otm_strike, 'ratio_spread_otm', quantity=-self.quantity, expiry_offset=self.monthly_expiry_offset)
        self._enter_call_position(underlying_symbol, otm_strike, 'ratio_spread_otm', quantity=-self.quantity, expiry_offset=self.monthly_expiry_offset)

    def _enter_otm_call_position(self, underlying_symbol: str):
        """Enter an OTM call position after FII flow signal"""
        if not self.option_manager:
            self.logger.error("OptionManager not available. Cannot enter OTM call position.")
            return
            
        # Get current price
        current_price = self.option_manager.underlying_prices.get(underlying_symbol)
        strike_interval = self.option_manager.strike_intervals.get(underlying_symbol)
        
        if current_price is None or strike_interval is None:
            self.logger.warning(f"Cannot determine strikes for {underlying_symbol}: Price or interval missing.")
            return
            
        # Calculate OTM strike (higher than current price)
        otm_price_target = current_price * (1 + self.otm_call_pct / 100)
        otm_strike = self.option_manager._get_atm_strike_internal(otm_price_target, strike_interval)
        
        self.logger.info(f"Entering OTM call for {underlying_symbol} after FII flow. Current Price: {current_price:.2f}, OTM Strike: {otm_strike}")
        
        # Buy OTM call
        self._enter_call_position(underlying_symbol, otm_strike, 'fii_flow_otm', expiry_offset=self.weekly_expiry_offset)

    def _enter_call_position(self, underlying_symbol: str, strike: float, strategy_type: str, quantity: int = None, expiry_offset: int = 0):
        """Enter a call option position"""
        if quantity is None:
            quantity = self.quantity  # Use default if not specified
            
        # Get the CE Instrument
        ce_instrument = self.option_manager.get_option_instrument(underlying_symbol, strike, "CE", expiry_offset)
        
        if not ce_instrument:
            self.logger.error(f"Could not get CE instrument for {underlying_symbol} strike {strike}.")
            return
            
        ce_symbol_key = ce_instrument.instrument_id
        
        # Get latest price for this option
        ce_data = self.data_manager.get_latest_tick(ce_symbol_key)
        
        if not ce_data:
            self.logger.warning(f"Market data not yet available for {ce_symbol_key}. Skipping entry.")
            return
            
        ce_price = ce_data.get(MarketDataType.LAST_PRICE.value)
        
        if ce_price is None:
            self.logger.warning(f"Last price not found in data for {ce_symbol_key}.")
            return
            
        try:
            ce_price = float(ce_price)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid price format for CE ({ce_price}).")
            return
            
        # Determine signal type based on quantity
        signal_type = SignalType.BUY if quantity > 0 else SignalType.SELL
        action_text = "BUY" if quantity > 0 else "SELL"
        
        # Generate signal
        signal_data = {
            'quantity': abs(quantity),
            'strategy': self.id,
            'underlying': underlying_symbol,
            'strike': strike,
            'type': 'CE',
            'price': ce_price,
            'strategy_type': strategy_type
        }
        
        self.generate_signal(ce_instrument.symbol, signal_type, data=signal_data)
        
        # Initialize position tracking if buying
        if quantity > 0:
            self.active_positions[ce_symbol_key] = {
                'symbol_key': ce_symbol_key,
                'symbol': ce_instrument.symbol,
                'underlying': underlying_symbol,
                'type': 'CE',
                'entry_price': ce_price,
                'current_price': ce_price,
                'high_price': ce_price,
                'stop_loss': ce_price * (1 - self.stop_loss_percent / 100),
                'target': ce_price * (1 + self.target_percent / 100),
                'strategy_type': strategy_type,
                'exited': False
            }
        
        self.logger.info(f"{action_text} signal generated for CE: {ce_instrument.symbol} @ ~{ce_price:.2f}")

    def _enter_put_position(self, underlying_symbol: str, strike: float, strategy_type: str, quantity: int = None, expiry_offset: int = 0):
        """Enter a put option position"""
        if quantity is None:
            quantity = self.quantity  # Use default if not specified
            
        # Get the PE Instrument
        pe_instrument = self.option_manager.get_option_instrument(underlying_symbol, strike, "PE", expiry_offset)
        
        if not pe_instrument:
            self.logger.error(f"Could not get PE instrument for {underlying_symbol} strike {strike}.")
            return
            
        pe_symbol_key = pe_instrument.instrument_id
        
        # Get latest price for this option
        pe_data = self.data_manager.get_latest_tick(pe_symbol_key)
        
        if not pe_data:
            self.logger.warning(f"Market data not yet available for {pe_symbol_key}. Skipping entry.")
            return
            
        pe_price = pe_data.get(MarketDataType.LAST_PRICE.value)
        
        if pe_price is None:
            self.logger.warning(f"Last price not found in data for {pe_symbol_key}.")
            return
            
        try:
            pe_price = float(pe_price)
        except (ValueError, TypeError):
            self.logger.error(f"Invalid price format for PE ({pe_price}).")
            return
            
        # Determine signal type based on quantity
        signal_type = SignalType.BUY if quantity > 0 else SignalType.SELL
        action_text = "BUY" if quantity > 0 else "SELL"
        
        # Generate signal
        signal_data = {
            'quantity': abs(quantity),
            'strategy': self.id,
            'underlying': underlying_symbol,
            'strike': strike,
            'type': 'PE',
            'price': pe_price,
            'strategy_type': strategy_type
        }
        
        self.generate_signal(pe_instrument.symbol, signal_type, data=signal_data)
        
        # Initialize position tracking if buying
        if quantity > 0:
            self.active_positions[pe_symbol_key] = {
                'symbol_key': pe_symbol_key,
                'symbol': pe_instrument.symbol,
                'underlying': underlying_symbol,
                'type': 'PE',
                'entry_price': pe_price,
                'current_price': pe_price,
                'high_price': pe_price,
                'stop_loss': pe_price * (1 - self.stop_loss_percent / 100),
                'target': pe_price * (1 + self.target_percent / 100),
                'strategy_type': strategy_type,
                'exited': False
            }
        
        self.logger.info(f"{action_text} signal generated for PE: {pe_instrument.symbol} @ ~{pe_price:.2f}")

    def _update_position(self, symbol_key: str, current_price: float):
        """Update position tracking and check for individual exits (SL/Target)."""
        if symbol_key not in self.active_positions:
            return
            
        position_info = self.active_positions[symbol_key]
        
        # Ignore if already exited
        if position_info['exited']:
            return
            
        # Update current price
        position_info['current_price'] = current_price
        symbol = position_info['symbol']
        entry_price = position_info['entry_price']
        
        # Update high price for trailing stop calculation
        position_info['high_price'] = max(position_info['high_price'], current_price)
        
        # Update trailing stop loss if applicable
        if self.trail_percent > 0:
            potential_new_stop = position_info['high_price'] * (1 - self.trail_percent / 100)
            if potential_new_stop > position_info['stop_loss']:
                position_info['stop_loss'] = potential_new_stop
        
        # Check Exit Conditions
        exit_reason = None
        if current_price <= position_info['stop_loss']:
            exit_reason = "stop_loss"
        elif current_price >= position_info['target']:
            exit_reason = "target"
            
        if exit_reason:
            self.logger.info(f"{exit_reason.replace('_',' ').title()} triggered for {symbol} at {current_price:.2f} "
                           f"(Entry: {entry_price:.2f}, SL: {position_info['stop_loss']:.2f}, Target: {position_info['target']:.2f})")
            
            # Generate SELL signal
            self.generate_signal(symbol, SignalType.SELL, data={
                'price': current_price,
                'quantity': self.quantity,
                'strategy': self.id,
                'reason': exit_reason,
                'underlying': position_info['underlying'],
                'type': position_info['type'],
                'strategy_type': position_info['strategy_type'],
                'entry_price': entry_price
            })
            
            # Mark as exited
            position_info['exited'] = True
            
            # Remove from active positions
            del self.active_positions[symbol_key]

    def _exit_all_positions(self, reason: str):
        """Exit all currently open positions."""
        self.logger.info(f"Exiting all active positions. Reason: {reason}")
        positions_to_exit = list(self.active_positions.keys())
        
        for symbol_key in positions_to_exit:
            position_info = self.active_positions.get(symbol_key)
            if not position_info or position_info['exited']:
                continue
                
            symbol = position_info['symbol']
            current_price = position_info['current_price']
            entry_price = position_info['entry_price']
            
            self.logger.info(f"Generating SELL signal for {symbol} at ~{current_price:.2f}. Reason: {reason}")
            self.generate_signal(symbol, SignalType.SELL, data={
                'price': current_price,
                'quantity': self.quantity,
                'strategy': self.id,
                'reason': reason,
                'underlying': position_info['underlying'],
                'type': position_info['type'],
                'strategy_type': position_info['strategy_type'],
                'entry_price': entry_price
            })
            
            # Mark as exited
            position_info['exited'] = True
            
        # Clear all positions
        self.active_positions.clear()
        self.logger.info("Finished generating exit signals for all active positions.")

    def on_fill(self, event: FillEvent):
        """Handle fill events to confirm trades."""
        if hasattr(event, 'symbol') and hasattr(event, 'fill_price') and hasattr(event, 'quantity'):
            action = "Bought" if event.quantity > 0 else "Sold"
            self.logger.info(f"Fill received: {action} {abs(event.quantity)} of {event.symbol} @ {event.fill_price:.2f} (Order ID: {event.order_id})")

    def on_stop(self):
        """Called when the strategy is stopped by StrategyManager."""
        # Ensure all positions are exited
        if self.active_positions:
            self.logger.warning(f"Strategy '{self.id}' stopping with active positions. Forcing exit.")
            self._exit_all_positions("Strategy stopped")
            
        self.logger.info(f"Combined Option Strategy '{self.id}' stopped.")
