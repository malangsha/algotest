import uuid
import logging
import os
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

from models.position import Position
from models.instrument import Instrument
from models.events import Event, EventType, PositionEvent, AccountEvent
from utils.exceptions import PortfolioError
from utils.constants import OrderSide
from core.position_manager import PositionManager

class Portfolio:
    """
    Manages the entire portfolio including positions, cash, and performance metrics.
    """
    
    def __init__(self, config, position_manager=None, event_manager=None):
        """
        Initialize the portfolio manager.
        
        Args:
            config: Configuration dictionary
            position_manager: Position manager instance
            event_manager: Event manager instance
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.position_manager = position_manager
        self.event_manager = event_manager
        
        # Extract portfolio configuration
        portfolio_config = config.get("portfolio", {})
        self.initial_capital = portfolio_config.get("initial_capital", 1000000.0)
        
        # Portfolio state
        self.cash = self.initial_capital
        self.equity = self.initial_capital
        self.positions_value = 0.0
        self.highest_equity = self.initial_capital
        self.starting_equity = self.initial_capital
        
        # Historical equity data
        self.equity_history = []
        # Add historical values for tracking portfolio performance over time
        self.historical_values = []
        self.last_updated = datetime.now()
        
        # Portfolio limits
        self.max_leverage = portfolio_config.get("max_leverage", 1.0)
        self.max_position_size = portfolio_config.get("max_position_size", 0.1)
        self.max_position_value = portfolio_config.get("max_position_value", self.initial_capital * 0.1)
        
        # Register with event manager if available
        if self.event_manager:
            self._register_event_handlers()
            
        self.logger.info(f"Portfolio initialized with {self.initial_capital} capital")
    
    def _register_event_handlers(self):
        """Register to receive events from the event manager."""
        self.event_manager.subscribe(
            EventType.POSITION, 
            self._on_position_event, 
            component_name="Portfolio"
        )
        
        self.event_manager.subscribe(
            EventType.FILL, 
            self._on_fill_event, 
            component_name="Portfolio"
        )
        
        self.logger.info("Registered with Event Manager")
    
    def _on_position_event(self, event: Event):
        """
        Handle position events from the event manager.
        Updates portfolio state based on position changes.
        
        Args:
            event: Position event
        """
        if not isinstance(event, PositionEvent) and not hasattr(event, 'symbol'):
            return
            
        self.logger.debug(f"Processing position event: {event}")
        
        # Update portfolio value based on position events
        self._recalculate_portfolio_value()
        
        # Publish account update to reflect position changes
        self._publish_account_event()
    
    def _on_fill_event(self, event: Event):
        """
        Handle fill events from the event manager.
        Updates cash and calculates transaction costs.
        
        Args:
            event: Fill event
        """
        if not hasattr(event, 'symbol') or not hasattr(event, 'quantity') or not hasattr(event, 'price'):
            return
            
        self.logger.debug(f"Processing fill event: {event}")
        
        quantity = getattr(event, 'quantity', 0)
        price = getattr(event, 'price', 0)
        side = getattr(event, 'side', None)
        commission = getattr(event, 'commission', 0)
        
        if not side or not quantity or not price:
            return
            
        # Adjust cash based on the fill
        trade_value = quantity * price
        
        # Handle both string values and OrderSide enum objects
        side_value = side.value if hasattr(side, 'value') else side
        
        if side_value == 'BUY':
            self.cash -= trade_value
        else:  # SELL
            self.cash += trade_value
            
        # Subtract commission
        self.cash -= commission
        
        # Update portfolio value
        self._recalculate_portfolio_value()
        
        # Publish account update
        self._publish_account_event()
    
    def _recalculate_portfolio_value(self):
        """
        Recalculate the total portfolio value based on positions and cash.
        """
        self.positions_value = 0.0
        
        # Get all positions from position manager if available
        if self.position_manager:
            for symbol, position in self.position_manager.get_all_positions().items():
                if position.quantity != 0 and hasattr(position, 'last_price') and position.last_price is not None and position.last_price > 0:
                    self.positions_value += position.quantity * position.last_price
        
        # Update equity
        prev_equity = self.equity
        self.equity = self.cash + self.positions_value
        
        # Track highest equity for drawdown calculation
        if self.equity > self.highest_equity:
            self.highest_equity = self.equity
            
        # Record equity history
        self.equity_history.append({
            'timestamp': datetime.now(),
            'equity': self.equity,
            'cash': self.cash,
            'positions_value': self.positions_value
        })
        
        # Limit history size
        if len(self.equity_history) > 1000:
            self.equity_history = self.equity_history[-1000:]
            
        # Log significant changes
        if prev_equity > 0 and abs(self.equity - prev_equity) / prev_equity > 0.01:
            self.logger.info(f"Portfolio equity changed from {prev_equity:.2f} to {self.equity:.2f} ({(self.equity - prev_equity) / prev_equity:.2%})")
    
    def _publish_account_event(self):
        """
        Publish an account event with the current portfolio state.
        """
        if not self.event_manager:
            return
            
        # Create account event
        event = AccountEvent(
            event_type=EventType.ACCOUNT,
            timestamp=int(time.time() * 1000)
        )
        
        # Add account details
        event.balance = self.cash
        event.equity = self.equity
        event.positions_value = self.positions_value
        event.margin_available = self.equity  # For simple portfolio without margin
        event.margin_used = 0.0
        event.highest_equity = self.highest_equity
        
        # Add drawdown and other metrics
        if self.highest_equity > 0:
            event.drawdown = (self.highest_equity - self.equity) / self.highest_equity
            
        if self.starting_equity > 0:
            event.return_pct = (self.equity - self.starting_equity) / self.starting_equity
            
        # Publish the event
        self.event_manager.publish(event)
        self.logger.debug(f"Published account event: equity={self.equity:.2f}")
    
    def get_total_value(self) -> float:
        """
        Get the total portfolio value (cash + positions).
        
        Returns:
            float: Total portfolio value
        """
        return self.equity
    
    def get_cash(self) -> float:
        """
        Get available cash.
        
        Returns:
            float: Available cash
        """
        return self.cash
    
    def set_cash(self, cash: float):
        """
        Set the cash value directly.
        
        Args:
            cash: New cash value
        """
        self.cash = cash
        self._recalculate_portfolio_value()
        
        # Publish account update
        self._publish_account_event()
    
    def get_position_value(self, symbol: str) -> float:
        """
        Get the value of a specific position.
        
        Args:
            symbol: Symbol to get position value for
            
        Returns:
            float: Position value or 0 if not found
        """
        if not self.position_manager:
            return 0.0
            
        position = self.position_manager.get_position(symbol)
        if not position or not hasattr(position, 'quantity') or not hasattr(position, 'last_price'):
            return 0.0
            
        return position.quantity * position.last_price
    
    def get_total_position_value(self) -> float:
        """
        Get the total value of all positions.
        
        Returns:
            float: Total position value
        """
        return self.positions_value
    
    def get_leverage(self) -> float:
        """
        Calculate the current portfolio leverage.
        
        Returns:
            float: Current leverage ratio
        """
        if self.equity == 0:
            return 0.0
            
        return self.positions_value / self.equity
    
    def get_drawdown(self) -> float:
        """
        Calculate the current drawdown from peak equity.
        
        Returns:
            float: Current drawdown as a decimal (0.05 = 5%)
        """
        if self.highest_equity == 0:
            return 0.0
            
        return (self.highest_equity - self.equity) / self.highest_equity
    
    def get_total_exposure(self) -> float:
        """
        Calculate the total exposure as a ratio of portfolio value.
        
        Returns:
            float: Total exposure ratio
        """
        if self.equity == 0:
            return 0.0
            
        return self.positions_value / self.equity
    
    def get_instrument_exposure(self, symbol: str) -> float:
        """
        Calculate the exposure to a specific instrument as a ratio of portfolio value.
        
        Args:
            symbol: Symbol to calculate exposure for
            
        Returns:
            float: Instrument exposure ratio
        """
        if self.equity == 0:
            return 0.0
            
        position_value = self.get_position_value(symbol)
        return position_value / self.equity
    
    def can_trade(self, symbol: str, quantity: float, price: float) -> bool:
        """
        Check if a trade is allowable given portfolio constraints.
        
        Args:
            symbol: Symbol to trade
            quantity: Quantity to trade (positive for buy, negative for sell)
            price: Trade price
            
        Returns:
            bool: True if trade is allowable
        """
        trade_value = abs(quantity * price)
        
        # Check if enough cash for buy
        if quantity > 0 and trade_value > self.cash:
            self.logger.warning(f"Insufficient cash for trade: {trade_value} required, {self.cash} available")
            return False
            
        # Check position size limit
        if self.equity > 0:
            position_value = self.get_position_value(symbol)
            new_position_value = position_value + (quantity * price)
            
            # Check maximum position size
            if new_position_value / self.equity > self.max_position_size:
                self.logger.warning(f"Trade exceeds max position size: {new_position_value / self.equity:.2%} > {self.max_position_size:.2%}")
                return False
                
            # Check maximum position value
            if new_position_value > self.max_position_value:
                self.logger.warning(f"Trade exceeds max position value: {new_position_value} > {self.max_position_value}")
                return False
                
            # Check leverage
            new_positions_value = self.positions_value + (quantity * price)
            new_leverage = new_positions_value / self.equity
            
            if new_leverage > self.max_leverage:
                self.logger.warning(f"Trade exceeds max leverage: {new_leverage:.2f} > {self.max_leverage:.2f}")
                return False
        
        return True

    def add_instrument(self, instrument: Instrument) -> None:
        """Add an instrument to the portfolio"""
        if instrument.id in self.instruments:
            self.logger.warning(f"Instrument {instrument.id} already exists in portfolio")
            return

        self.instruments[instrument.id] = instrument
        self.logger.info(f"Added instrument to portfolio: {instrument.id} ({instrument.symbol})")

    def remove_instrument(self, instrument_id: str) -> bool:
        """Remove an instrument from the portfolio"""
        if instrument_id not in self.instruments:
            self.logger.warning(f"Instrument {instrument_id} not found in portfolio")
            return False

        # Check if there are any open positions for this instrument
        open_positions = self.position_manager.get_open_positions()
        for position in open_positions:
            if position.instrument_id == instrument_id:
                self.logger.error(f"Cannot remove instrument {instrument_id} with open positions")
                return False

        del self.instruments[instrument_id]
        self.logger.info(f"Removed instrument from portfolio: {instrument_id}")
        return True

    def get_instrument(self, instrument_id: str) -> Optional[Instrument]:
        """Get an instrument by its ID"""
        return self.instruments.get(instrument_id)

    def get_position(self, position_id: str) -> Optional[Position]:
        """Get a position by its ID"""
        return self.position_manager.get_position(position_id)

    def get_positions_by_strategy(self, strategy_id: str) -> List[Position]:
        """Get all positions for a given strategy"""
        return self.position_manager.get_positions_by_strategy(strategy_id)

    def get_positions_by_instrument(self, instrument_id: str) -> List[Position]:
        """Get all positions for a given instrument"""
        return self.position_manager.get_positions_by_instrument(instrument_id)

    def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        return self.position_manager.get_open_positions()    

    def calculate_portfolio_value(self) -> float:
        """Calculate the total value of the portfolio"""
        open_positions = self.get_open_positions()

        open_positions_value = sum([
            p.quantity * p.current_price
            for p in open_positions
        ])

        total_value = self.cash + open_positions_value

        # Record historical value
        self.historical_values.append((datetime.now(), total_value))

        return total_value

    def calculate_realized_pnl(self) -> float:
        """Calculate total realized profit and loss"""
        all_positions = self.position_manager.get_all_positions()
        
        return sum([
            p.realized_pnl
            for p in all_positions
            if hasattr(p, 'is_closed') and p.is_closed() and hasattr(p, 'realized_pnl') and p.realized_pnl is not None
        ])

    def calculate_unrealized_pnl(self) -> float:
        """Calculate total unrealized profit and loss"""
        open_positions = self.get_open_positions()

        return sum([
            p.calculate_pnl()
            for p in open_positions
        ])

    def get_portfolio_summary(self) -> Dict:
        """Get a summary of the portfolio"""
        portfolio_value = self.calculate_portfolio_value()
        realized_pnl = self.calculate_realized_pnl()
        unrealized_pnl = self.calculate_unrealized_pnl()

        return {
            "initial_capital": self.initial_capital,
            "cash_balance": self.cash,
            "total_invested": self.positions_value,
            "portfolio_value": portfolio_value,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "total_pnl": realized_pnl + unrealized_pnl,
            "open_positions_count": len(self.get_open_positions()),
            "total_positions_count": len(self.position_manager.get_all_positions()),
            "last_updated": self.last_updated
        }

    def update(self):
        """Update portfolio state"""
        self.calculate_portfolio_value()
        self.last_updated = datetime.now()

    # Add this to the Portfolio class
    def save(self, filepath: str) -> None:
        """
        Save portfolio state to a file.
    
        Args:
            filepath: Path to save the portfolio data
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
            # Get open positions
            open_positions = self.get_open_positions()
            
            # Calculate realized PnL without using position_manager.get_closed_positions()
            realized_pnl = 0.0
            all_positions = self.position_manager.get_all_positions()
            for position in all_positions:
                if hasattr(position, 'is_closed') and position.is_closed() and hasattr(position, 'realized_pnl'):
                    realized_pnl += position.realized_pnl or 0.0
            
            # Prepare serializable data
            portfolio_data = {
                'cash': self.cash,
                'initial_capital': self.initial_capital,
                'positions': {
                    position.instrument_id: {
                        'quantity': position.quantity,
                        'average_price': position.entry_price,
                        'market_price': position.current_price,
                        'market_value': position.quantity * position.current_price,
                        'unrealized_pnl': position.calculate_pnl()
                    } for position in open_positions
                },
                'realized_pnl': realized_pnl,
                'timestamp': datetime.now().isoformat()
            }
    
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(portfolio_data, f, indent=4)
    
            self.logger.info(f"Portfolio saved to {filepath}")
    
        except Exception as e:
            self.logger.error(f"Error saving portfolio: {str(e)}")

    def calculate_realized_pnl(self) -> float:
        """Calculate total realized profit and loss"""
        all_positions = self.position_manager.get_all_positions()
        
        return sum([
            p.realized_pnl
            for p in all_positions
            if hasattr(p, 'is_closed') and p.is_closed() and hasattr(p, 'realized_pnl') and p.realized_pnl is not None
        ])           
    
    @property
    def total_value(self):
        """Calculate total portfolio value (cash + positions)."""
        positions_value = sum(pos.market_value for pos in self.get_open_positions()) if hasattr(self, 'positions') else 0
        return self.cash + positions_value
