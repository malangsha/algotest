import uuid
import logging
import os
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

from models.position import Position
from models.instrument import Instrument
from models.events import Event, EventType, PositionEvent, AccountEvent, FillEvent
from utils.exceptions import PortfolioError
from utils.constants import OrderSide
from core.position_manager import PositionManager
from core.logging_manager import get_logger

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
        self.logger = get_logger(__name__)
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
        
        # Initialize instruments dictionary
        self.instruments = {}
        
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
    
    def _on_position_event(self, event: PositionEvent):
        """
        Handle position events from the event manager.
        Updates portfolio state based on position changes.
        
        Args:
            event: Position event
        """
        self.logger.debug(f"Processing position event: {event}")
        if not isinstance(event, PositionEvent) and not hasattr(event, 'symbol'):
            return
            
        # Update portfolio value based on position events
        self._recalculate_portfolio_value()
        
        # Publish account update to reflect position changes
        self._publish_account_event()
    
    def _on_fill_event(self, event: FillEvent):
        """
        Handle fill events from the event manager.
        Updates cash and calculates transaction costs.
        
        Args:
            event: Fill event
        """
        self.logger.debug(f"Processing fill event: {event}")

        if not isinstance(event, FillEvent):
            self.logger.warning(f"Invalid fill event type received: {type(event)}")
            return
        
        if not hasattr(event, 'symbol') or not hasattr(event, 'quantity') or not hasattr(event, 'price'):
            self.logger.warning(f"Fill event missing required fields: {event}")
            return
      
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
            all_positions = self.position_manager.get_all_positions()
            # Handle both dict and list return types
            if isinstance(all_positions, dict):
                positions_list = list(all_positions.values())
            else:
                positions_list = all_positions
                
            for position in positions_list:
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
            
        if position.last_price is None:
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
        instrument_id = getattr(instrument, 'instrument_id', getattr(instrument, 'symbol', str(instrument)))
        
        if instrument_id in self.instruments:
            self.logger.warning(f"Instrument {instrument_id} already exists in portfolio")
            return

        self.instruments[instrument_id] = instrument
        self.logger.info(f"Added instrument to portfolio: {instrument_id} ({getattr(instrument, 'symbol', 'N/A')})")

    def remove_instrument(self, instrument_id: str) -> bool:
        """Remove an instrument from the portfolio"""
        if instrument_id not in self.instruments:
            self.logger.warning(f"Instrument {instrument_id} not found in portfolio")
            return False

        # Check if there are any open positions for this instrument
        open_positions = self.get_open_positions()
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
        if not self.position_manager:
            return None
        return self.position_manager.get_position(position_id)

    def get_positions_by_strategy(self, strategy_id: str) -> List[Position]:
        """Get all positions for a given strategy"""
        if not self.position_manager:
            return []
            
        all_positions = self.position_manager.get_all_positions()
        # Handle both dict and list return types
        if isinstance(all_positions, dict):
            positions_list = list(all_positions.values())
        else:
            positions_list = all_positions
            
        return [pos for pos in positions_list if hasattr(pos, 'strategy_id') and pos.strategy_id == strategy_id]

    def get_positions_by_instrument(self, instrument_id: str) -> List[Position]:
        """Get all positions for a given instrument"""
        if not self.position_manager:
            return []
            
        all_positions = self.position_manager.get_all_positions()
        # Handle both dict and list return types
        if isinstance(all_positions, dict):
            positions_list = list(all_positions.values())
        else:
            positions_list = all_positions
            
        return [pos for pos in positions_list if pos.instrument_id == instrument_id]

    def get_open_positions(self) -> List[Position]:
        """Get all open positions"""
        if not self.position_manager:
            return []
        return self.position_manager.get_open_positions()    

    def calculate_portfolio_value(self) -> float:
        """Calculate the total value of the portfolio"""
        open_positions = self.get_open_positions()

        open_positions_value = sum([
            p.quantity * (p.last_price or 0)  # Use last_price instead of current_price
            for p in open_positions
            if p.last_price is not None
        ])

        total_value = self.cash + open_positions_value

        # Record historical value
        self.historical_values.append((datetime.now(), total_value))

        return total_value

    def calculate_realized_pnl(self) -> float:
        """Calculate total realized profit and loss"""
        if not self.position_manager:
            return 0.0
            
        all_positions = self.position_manager.get_all_positions()
        # Handle both dict and list return types
        if isinstance(all_positions, dict):
            positions_list = list(all_positions.values())
        else:
            positions_list = all_positions
        
        return sum([
            p.realized_pnl
            for p in positions_list
            if hasattr(p, 'realized_pnl') and p.realized_pnl is not None
        ])

    def calculate_unrealized_pnl(self) -> float:
        """Calculate total unrealized profit and loss"""
        open_positions = self.get_open_positions()

        return sum([
            self._calculate_position_unrealized_pnl(p)
            for p in open_positions
        ])

    def _calculate_position_unrealized_pnl(self, position: Position) -> float:
        """Calculate unrealized PnL for a single position"""
        if not hasattr(position, 'quantity') or not hasattr(position, 'average_price') or not hasattr(position, 'last_price'):
            return 0.0
            
        if position.quantity == 0 or position.last_price is None or position.average_price == 0:
            return 0.0
            
        # Check if position has unrealized_pnl method or property
        if hasattr(position, 'unrealized_pnl'):
            if callable(position.unrealized_pnl):
                return position.unrealized_pnl()
            else:
                return position.unrealized_pnl
        
        # Calculate manually if no unrealized_pnl method/property
        return position.quantity * (position.last_price - position.average_price)

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
            "total_positions_count": len(self.position_manager.get_all_positions()) if self.position_manager else 0,
            "last_updated": self.last_updated
        }

    def update(self):
        """Update portfolio state"""
        self.calculate_portfolio_value()
        self.last_updated = datetime.now()

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
            for position in open_positions.copy():
                self.logger.debug(f"Position: {position}")
                self.logger.debug(f"Position quantity: {position.quantity}")
                self.logger.debug(f"Position average price: {position.average_price}")
                self.logger.debug(f"Position last price: {position.last_price}")
                self.logger.debug(f"Position market value: {position.quantity * (position.last_price or 0)}")
                self.logger.debug(f"Position unrealized PnL: {self._calculate_position_unrealized_pnl(position)}")
            
            # Calculate realized PnL
            realized_pnl = self.calculate_realized_pnl()
            
            # Prepare serializable data
            portfolio_data = {
                'cash': self.cash,
                'initial_capital': self.initial_capital,
                'positions': {
                    position.instrument_id: {
                        'quantity': position.quantity,
                        'average_price': position.average_price,  # Use average_price instead of entry_price
                        'market_price': position.last_price,     # Use last_price instead of current_price
                        'market_value': position.quantity * (position.last_price or 0),
                        'unrealized_pnl': self._calculate_position_unrealized_pnl(position)
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
    
    @property
    def total_value(self):
        """Calculate total portfolio value (cash + positions)."""
        open_positions = self.get_open_positions()
        positions_value = sum([
            p.quantity * (p.last_price or 0)
            for p in open_positions
            if p.last_price is not None
        ])
        return self.cash + positions_value