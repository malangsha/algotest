from typing import Dict, Any, List, Optional
from datetime import datetime
from .trade import Trade
from utils.constants import OrderSide

class Position:
    """Represents a position in a financial instrument"""
    
    def __init__(self, instrument=None, instrument_id=None, strategy_id=None):
        """
        Initialize a position.
        
        Args:
            instrument: The instrument object (optional)
            instrument_id: The instrument ID string (required if instrument not provided)
            strategy_id: Optional strategy ID
        """
        # Handle either instrument object or instrument_id
        if instrument is not None:
            # Extract instrument_id from instrument object
            self.instrument = instrument
            self.instrument_id = getattr(instrument, 'instrument_id', 
                                       getattr(instrument, 'symbol', str(instrument)))
        elif instrument_id is not None:
            self.instrument = None
            self.instrument_id = instrument_id
        else:
            raise ValueError("Either instrument or instrument_id must be provided")
        
        self.strategy_id = strategy_id
        self.quantity = 0.0
        self.average_price = 0.0
        self.entry_price = 0.0  # Alias for average_price for backward compatibility
        self.realized_pnl = 0.0
        self.trades: List[Trade] = []
        self.open_timestamp: Optional[datetime] = None
        self.close_timestamp: Optional[datetime] = None
        self.last_price: Optional[float] = None
        self.last_update_time: Optional[datetime] = None
        self.last_trade_pnl: Optional[float] = None
        self.last_closed_trade_details: Optional[Dict] = None
        
    def __str__(self):
        return (f"Position(instrument={self.instrument_id}, "
                f"qty={self.quantity}, avg_price={self.average_price:.2f}, "
                f"realized_pnl={self.realized_pnl:.2f})")
    
    def apply_trade(self, trade: Trade) -> float:
        """Apply a trade to this position"""
        trade_realized_pnl = 0.0
        
        # Record the trade
        self.trades.append(trade)
        self.last_update_time = trade.timestamp
        
        # Reset last trade pnl before calculation
        self.last_trade_pnl = 0.0
        
        # Set open timestamp if this is the first trade
        if self.open_timestamp is None and trade.quantity > 0:
            self.open_timestamp = trade.timestamp
            
        # Process the trade
        if trade.side == "BUY":
            trade_realized_pnl = self._apply_buy_trade(trade)
        elif trade.side == "SELL":
            trade_realized_pnl = self._apply_sell_trade(trade)
        else:
            raise ValueError(f"Invalid trade side: {trade.side}")
            
        # Update close timestamp if position is fully closed
        if abs(self.quantity) < 1e-10:  # Using small epsilon for float comparison
            self.close_timestamp = trade.timestamp
            # If position closed, the last trade pnl is the one that closed it
            self.last_trade_pnl = trade_realized_pnl
        else:
            self.close_timestamp = None
        
        return trade_realized_pnl
    
    def _apply_buy_trade(self, trade: Trade) -> float:
        """Apply a buy trade to the position"""
        trade_realized_pnl = 0.0 # Initialize PnL for this trade
        closed_quantity = 0
        if self.quantity < 0:  # Short position being reduced
            closed_quantity = min(abs(self.quantity), trade.quantity)
            pnl_per_share = self.average_price - trade.price
            trade_realized_pnl = closed_quantity * pnl_per_share
            self.realized_pnl += trade_realized_pnl
            
        # Update the position
        new_quantity = self.quantity + trade.quantity
        
        # Calculate new average price (only for increasing long positions)
        if new_quantity > 0 and self.quantity >= 0:
            self.average_price = ((self.quantity * self.average_price) + 
                                 (trade.quantity * trade.price)) / new_quantity
        
        # For position flips (short to long), reset average price
        elif new_quantity > 0 and self.quantity < 0:
            # The remaining quantity after covering the short
            remaining_quantity = trade.quantity - abs(self.quantity)
            if remaining_quantity > 0:
                self.average_price = trade.price
                
        self.quantity = new_quantity
        
        # Store details if this trade closed (part of) the position
        if closed_quantity > 0:
            self.last_closed_trade_details = {
                'type': 'CLOSE_SHORT',
                'quantity': closed_quantity,
                'entry_price': self.average_price, # Avg price before this trade
                'exit_price': trade.price,
                'pnl': trade_realized_pnl,
                'timestamp': trade.timestamp
            }
        
        return trade_realized_pnl # Return PnL generated by this trade
    
    def _apply_sell_trade(self, trade: Trade) -> float:
        """Apply a sell trade to the position"""
        trade_realized_pnl = 0.0 # Initialize PnL for this trade
        closed_quantity = 0
        if self.quantity > 0:  # Long position being reduced
            closed_quantity = min(self.quantity, trade.quantity)
            pnl_per_share = trade.price - self.average_price
            trade_realized_pnl = closed_quantity * pnl_per_share
            self.realized_pnl += trade_realized_pnl
            
        # Update the position
        new_quantity = self.quantity - trade.quantity
        
        # Calculate new average price (only for increasing short positions)
        if new_quantity < 0 and self.quantity <= 0:
            # For shorts, we still track the average entry price
            self.average_price = ((abs(self.quantity) * self.average_price) + 
                                 (trade.quantity * trade.price)) / abs(new_quantity)
        
        # For position flips (long to short), reset average price
        elif new_quantity < 0 and self.quantity > 0:
            # The remaining quantity after selling the long
            remaining_quantity = trade.quantity - self.quantity
            if remaining_quantity > 0:
                self.average_price = trade.price
                
        self.quantity = new_quantity
        
        # Store details if this trade closed (part of) the position
        if closed_quantity > 0:
            self.last_closed_trade_details = {
                'type': 'CLOSE_LONG',
                'quantity': closed_quantity,
                'entry_price': self.average_price, # Avg price before this trade
                'exit_price': trade.price,
                'pnl': trade_realized_pnl,
                'timestamp': trade.timestamp
            }
        
        return trade_realized_pnl # Return PnL generated by this trade
    
    def update_market_price(self, price: float, timestamp=None) -> None:
        """
        Update the last known market price of the instrument
        
        Args:
            price: The latest market price
            timestamp: Optional timestamp (defaults to current time)
        """
        self.last_price = price
        if timestamp is None:
            timestamp = datetime.now()
        self.last_update_time = timestamp
    
    @property
    def market_value(self) -> Optional[float]:
        """Calculate the current market value of the position"""
        if self.quantity == 0 or self.last_price is None:
            return 0.0
        return self.quantity * self.last_price
    
    @property
    def unrealized_pnl(self) -> Optional[float]:
        """Calculate the unrealized profit/loss based on the last price"""
        if self.quantity == 0 or self.last_price is None:
            return 0.0
            
        if self.quantity > 0:  # Long position
            return self.quantity * (self.last_price - self.average_price)
        else:  # Short position
            return abs(self.quantity) * (self.average_price - self.last_price)
    
    @property
    def total_pnl(self) -> Optional[float]:
        """Calculate the total profit/loss (realized + unrealized)"""
        if self.unrealized_pnl is None:
            return self.realized_pnl
        return self.realized_pnl + self.unrealized_pnl
    
    def is_closed(self) -> bool:
        """Check if the position is closed (quantity = 0)"""
        return abs(self.quantity) < 1e-10  # Using small epsilon for float comparison
    
    def apply_fill(self, quantity: float, price: float) -> None:
        """
        Apply a fill to this position without creating a Trade object.
        
        Args:
            quantity: The quantity of the fill (positive for buys, negative for sells)
            price: The price of the fill
        """
        # Create a Trade object from the fill
        from .trade import Trade # Local import
        from utils.constants import OrderSide # Local import

        trade_side = OrderSide.BUY if quantity > 0 else OrderSide.SELL
        trade_quantity = abs(quantity)

        # Use a placeholder order_id if needed
        order_id = f"fill_{datetime.now().timestamp()}"

        trade = Trade(
            order_id=order_id, 
            instrument_id=self.instrument_id,
            quantity=trade_quantity,
            price=price,
            side=trade_side.value, # Use the string value
            timestamp=datetime.now(),
            strategy_id=self.strategy_id
        )
        
        # Call apply_trade with the created trade object
        self.apply_trade(trade)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary for serialization"""
        return {
            "instrument_id": self.instrument_id,
            "strategy_id": self.strategy_id,
            "quantity": self.quantity,
            "average_price": self.average_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "market_value": self.market_value,
            "last_price": self.last_price,
            "open_timestamp": self.open_timestamp.isoformat() if self.open_timestamp else None,
            "close_timestamp": self.close_timestamp.isoformat() if self.close_timestamp else None,
            "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
            "trade_count": len(self.trades),
            "last_closed_trade_details": self.last_closed_trade_details
        }
    
    