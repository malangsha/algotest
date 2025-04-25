import logging
import time
from typing import Dict, Set, Any, Optional, List
from datetime import datetime

from models.position import Position
from models.events import MarketDataEvent, FillEvent, EventType, TradeEvent, PositionEvent
from models.instrument import Instrument
from utils.constants import MarketDataType

class PositionManager:
    """Manages positions for all instruments in the trading system"""

    def __init__(self, event_manager=None):
        """
        Initialize the position manager.

        Args:
            event_manager: Optional event manager to publish position events
        """
        self.logger = logging.getLogger("core.position_manager")
        self.event_manager = event_manager

        # Dictionary of positions by instrument
        self.positions: Dict[Instrument, Position] = {}

        # Dictionary to look up instruments by symbol
        self.instruments_by_symbol: Dict[str, Instrument] = {}

        # Track market prices by instrument
        self.latest_prices: Dict[Instrument, float] = {}

        # Register with event manager if provided
        if self.event_manager:
            self._register_event_handlers()

        self.logger.info("Position Manager initialized")

    def _register_event_handlers(self):
        """Register to receive market data events from the event manager."""
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._on_market_data_event,
            component_name="PositionManager"
        )

        self.event_manager.subscribe(
            EventType.FILL,
            self._on_fill_event,
            component_name="PositionManager"
        )

        self.logger.info("Registered with Event Manager")

    def _on_market_data_event(self, event):
        """Handle market data events from the event manager."""
        self.update_market_prices(event)

    def _on_fill_event(self, event):
        """Handle fill events from the event manager."""
        self.apply_fill(event)

    def add_instrument(self, instrument: Instrument) -> None:
        """
        Add an instrument to track.

        Args:
            instrument: Instrument object
        """
        # Check if this symbol already exists in the dictionary
        if instrument.symbol in self.instruments_by_symbol:
            self.logger.debug(f"Instrument with symbol {instrument.symbol} already exists, skipping addition")
            return
            
        self.instruments_by_symbol[instrument.symbol] = instrument
        self.logger.debug(f"Added instrument to track: {instrument.symbol}")

    def add_instruments(self, instruments: List[Instrument]) -> None:
        """
        Add multiple instruments to track.

        Args:
            instruments: List of instrument objects
        """
        for instrument in instruments:
            self.add_instrument(instrument)

    def get_instrument_by_symbol(self, symbol: str) -> Optional[Instrument]:
        """Get instrument by symbol."""
        return self.instruments_by_symbol.get(symbol)

    def update_market_prices(self, market_event: MarketDataEvent) -> None:
        """
        Update market prices based on market data event.

        Args:
            market_event: Market data event with symbol
        """
        instrument = None
        
        # Handle the case where the instrument might be a string or an Instrument object
        if hasattr(market_event, 'instrument'):
            if isinstance(market_event.instrument, str):
                # Convert string to instrument if needed
                symbol = market_event.instrument
                instrument = self.instruments_by_symbol.get(symbol)
                if not instrument:
                    self.logger.warning(f"Creating new instrument for symbol {symbol}")
                    from models.instrument import Instrument
                    instrument = Instrument(symbol=symbol)
                    self.add_instrument(instrument)
            else:
                # It's already an Instrument object
                instrument = market_event.instrument
        
        if not instrument:
            self.logger.warning("Could not extract instrument from market data event")
            return

        symbol = instrument.symbol
        
        self.logger.debug(f"Market event for {symbol}: {market_event.data_type} - {market_event.data}")

        # Extract price using string keys
        price = None
        data = market_event.data if hasattr(market_event, 'data') and isinstance(market_event.data, dict) else {}

        # Try LAST_PRICE first
        if 'LAST_PRICE' in data and data['LAST_PRICE'] is not None:
            try:
                price = float(data['LAST_PRICE'])
                self.logger.debug(f"Found price {price} in LAST_PRICE for {symbol}")
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid LAST_PRICE value for {symbol}: {data['LAST_PRICE']}")
                price = None # Reset price if conversion fails

        # Try OHLC.close next if price still None
        if price is None and 'OHLC' in data and isinstance(data['OHLC'], dict):
            ohlc = data['OHLC']
            if 'close' in ohlc and ohlc['close'] is not None:
                try:
                    price = float(ohlc['close'])
                    self.logger.debug(f"Found price {price} in OHLC.close for {symbol}")
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid OHLC close value for {symbol}: {ohlc['close']}")
                    price = None

        # Try mid price from bid/ask if price still None
        if price is None and 'BID' in data and 'ASK' in data and data['BID'] is not None and data['ASK'] is not None:
            try:
                bid = float(data['BID'])
                ask = float(data['ASK'])
                if bid > 0 and ask > 0: # Ensure bid/ask are valid
                    price = (bid + ask) / 2
                    self.logger.debug(f"Calculated mid price {price} from bid/ask for {symbol}")
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid BID/ASK values for {symbol}: {data['BID']}/{data['ASK']}")
                price = None

        # Check if price was found
        if price is None:
            # Log a more specific warning
            self.logger.warning(f"Could not extract a valid price for {symbol} from event data: {data}") 
            return

        self.logger.info(f"Updating price for {symbol}: {price}")

        # Update the latest price
        self.latest_prices[instrument] = price

        # Update position if we have one
        if instrument in self.positions:
            self.positions[instrument].update_market_price(price)
            self.logger.debug(f"Updated position for {symbol}: {self.positions[instrument]}")

    def apply_fill(self, fill_event: FillEvent) -> None:
        """
        Apply a fill event to update positions.

        Args:
            fill_event: Fill event with symbol and trade details
        """
        # Extract symbol
        symbol = fill_event.symbol if hasattr(fill_event, 'symbol') else None

        if not symbol:
            self.logger.warning("Could not extract symbol from fill event")
            return

        # Look up or create instrument
        instrument = self.instruments_by_symbol.get(symbol)
        if not instrument:
            self.logger.warning(f"No instrument found for symbol: {symbol}. Creating basic instrument.")
            # Create a basic instrument with the symbol
            from models.instrument import Instrument
            instrument = Instrument(symbol=symbol)
            self.add_instrument(instrument)

        # Extract fill details
        quantity = fill_event.quantity if hasattr(fill_event, 'quantity') else 0
        price = fill_event.price if hasattr(fill_event, 'price') else 0
        side = fill_event.side if hasattr(fill_event, 'side') else None
        strategy_id = fill_event.strategy_id if hasattr(fill_event, 'strategy_id') else None

        # Make quantity negative for sell orders
        if side and str(side.value).upper() in ['SELL', 'SHORT']:
            quantity = -abs(quantity)  # Ensure it's negative and use abs to handle negative inputs

        # Get or create position
        if instrument not in self.positions:
            self.positions[instrument] = Position(instrument=instrument)

        # Update position with fill
        position_before = self.positions[instrument].quantity if instrument in self.positions else 0
        self.positions[instrument].apply_fill(quantity, price)
        position_after = self.positions[instrument].quantity

        self.logger.info(f"Applied fill to position {symbol}: quantity={quantity}, price={price}, position changed from {position_before} to {position_after}")
        
        # If the last_price is not set, use the fill price
        if not hasattr(self.positions[instrument], 'last_price') or self.positions[instrument].last_price is None:
            self.positions[instrument].update_market_price(price)
            self.logger.debug(f"Setting initial last_price for {symbol} to fill price: {price}")

        # Now publish the position event with the updated data
        self._publish_position_event(instrument, symbol, strategy_id)

    def apply_trade(self, trade_event: TradeEvent) -> None:
        """
        Apply a trade event to update positions.

        Args:
            trade_event: Trade event with symbol and trade details
        """
        # Extract symbol
        symbol = trade_event.symbol if hasattr(trade_event, 'symbol') else None

        if not symbol:
            self.logger.warning("Could not extract symbol from trade event")
            return

        # Look up or create instrument
        instrument = self.instruments_by_symbol.get(symbol)
        if not instrument:
            self.logger.warning(f"No instrument found for symbol: {symbol}. Creating basic instrument.")
            # Create a basic instrument with the symbol
            from models.instrument import Instrument
            instrument = Instrument(symbol=symbol)
            self.add_instrument(instrument)

        # Extract trade details
        quantity = trade_event.quantity if hasattr(trade_event, 'quantity') else 0
        price = trade_event.price if hasattr(trade_event, 'price') else 0
        side = trade_event.side if hasattr(trade_event, 'side') else None
        strategy_id = trade_event.strategy_id if hasattr(trade_event, 'strategy_id') else None
        timestamp = trade_event.timestamp if hasattr(trade_event, 'timestamp') else None
        order_id = trade_event.order_id if hasattr(trade_event, 'order_id') else str(timestamp)
        commission = trade_event.commission if hasattr(trade_event, 'commission') else 0.0

        # Create a Trade object
        from models.trade import Trade
        from datetime import datetime
        
        # Convert timestamp to datetime if needed
        if timestamp is not None and not isinstance(timestamp, datetime):
            try:
                # If timestamp is in milliseconds since epoch
                if timestamp > 1000000000000:  # Assuming milliseconds
                    timestamp = datetime.fromtimestamp(timestamp / 1000)
                else:  # Assuming seconds
                    timestamp = datetime.fromtimestamp(timestamp)
            except (ValueError, TypeError, OverflowError):
                timestamp = datetime.now()
        elif timestamp is None:
            timestamp = datetime.now()
            
        # Convert side to string if it's an enum
        side_str = side.value if hasattr(side, 'value') else str(side)
            
        trade = Trade(
            order_id=order_id,
            instrument_id=symbol,
            quantity=quantity,
            price=price,
            side=side_str,
            timestamp=timestamp,
            commission=commission,
            strategy_id=strategy_id
        )

        # Get or create position
        if instrument not in self.positions:
            self.positions[instrument] = Position(instrument=instrument)

        # Update position with trade object
        self.positions[instrument].apply_trade(trade)

        self.logger.info(f"Applied trade to position {symbol}: quantity={quantity}, price={price}")
        
        # Publish position event
        self._publish_position_event(instrument, symbol, strategy_id)

    def _publish_position_event(self, instrument, symbol, strategy_id=None):
        """
        Publish a position event for the given instrument.
        
        Args:
            instrument: The instrument for which to publish a position event
            symbol: The symbol of the instrument (for convenience)
            strategy_id: Optional strategy ID
        """
        if not self.event_manager:
            return
            
        position = self.positions.get(instrument)
        if not position:
            return
            
        try:
            # Get the exchange from the instrument if available
            exchange = getattr(instrument, 'exchange', None)
            
            # Create and publish position event
            from models.events import PositionEvent, EventType
            import time
            
            # Handle unrealized_pnl as either property or method
            if hasattr(position, 'unrealized_pnl'):
                if callable(position.unrealized_pnl):
                    unrealized_pnl = position.unrealized_pnl()
                else:
                    unrealized_pnl = position.unrealized_pnl
            else:
                unrealized_pnl = 0
                
            # Use entry_price or average_price
            avg_price = 0
            if hasattr(position, 'entry_price'):
                avg_price = position.entry_price
            elif hasattr(position, 'average_price'):
                avg_price = position.average_price
            
            # Add details of the last closing trade if available
            last_closed_trade = getattr(position, 'last_closed_trade_details', None)

            position_event = PositionEvent(
                event_type=EventType.POSITION,
                timestamp=int(time.time() * 1000),
                symbol=symbol,
                exchange=exchange,
                quantity=position.quantity,
                average_price=avg_price,
                realized_pnl=position.realized_pnl if hasattr(position, 'realized_pnl') else 0,
                unrealized_pnl=unrealized_pnl,
                strategy_id=strategy_id,               
                closing_trade_details=last_closed_trade
            )
            
            self.event_manager.publish(position_event)
            self.logger.debug(f"Published position event: {symbol}, quantity={position.quantity}")
        except Exception as e:
            self.logger.error(f"Error publishing position event: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def get_position(self, instrument_or_symbol) -> Optional[Position]:
        """
        Get position for an instrument or symbol.

        Args:
            instrument_or_symbol: Instrument object or symbol

        Returns:
            Position object or None if no position exists
        """
        if isinstance(instrument_or_symbol, str):
            # It's a symbol, look up the instrument
            instrument = self.instruments_by_symbol.get(instrument_or_symbol)
            if not instrument:
                return None
        else:
            # Assume it's an instrument
            instrument = instrument_or_symbol

        return self.positions.get(instrument)

    def get_positions(self) -> Dict[Instrument, Position]:
        """Get all positions."""
        return self.positions

    def get_open_positions(self) -> List[Position]:
        """
        Get all positions with non-zero quantities.

        Returns:
            List[Position]: List of positions with non-zero quantities
        """
        return [position for position in self.positions.values() if position.quantity != 0]

    def get_latest_price(self, instrument_or_symbol) -> Optional[float]:
        """
        Get latest price for an instrument or symbol.

        Args:
            instrument_or_symbol: Instrument object or symbol

        Returns:
            Latest price or None if no price available
        """
        if isinstance(instrument_or_symbol, str):
            # It's a symbol, look up the instrument
            instrument = self.instruments_by_symbol.get(instrument_or_symbol)
            if not instrument:
                return None
        else:
            # Assume it's an instrument
            instrument = instrument_or_symbol

        return self.latest_prices.get(instrument)

    def clear_position(self, identifier: str) -> None:
        """
        Clear a position (set quantity to zero).

        Args:
            identifier: Instrument ID or symbol
        """
        position = self.get_position(identifier)
        if position:
            position.clear()
            self.logger.info(f"Cleared position for {identifier}")

    def clear_all_positions(self) -> None:
        """Clear all positions."""
        for position in self.positions.values():
            position.clear()
        self.logger.info("Cleared all positions")

    def save_trades(self, filename: str) -> None:
        """
        Save all trades to a CSV file.

        Args:
            filename: Path to the file where trades should be saved
        """
        import csv
        import os

        self.logger.info(f"Saving trades to {filename}")

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Prepare data for CSV
        trade_data = []
        for instrument, position in self.positions.items():
            symbol = instrument.symbol if hasattr(instrument, 'symbol') else str(instrument)

            # Extract relevant position data
            # Adjust these fields based on what your Position class actually contains
            trade_info = {
                'symbol': symbol,
                'quantity': position.quantity if hasattr(position, 'quantity') else 0,
                'entry_price': position.entry_price if hasattr(position, 'entry_price') else 0,
                'current_price': self.latest_prices.get(instrument, 0),
                'pnl': position.unrealized_pnl() if hasattr(position, 'unrealized_pnl') else 0,
                'open_time': position.open_time if hasattr(position, 'open_time') else '',
                'last_update_time': position.last_update_time if hasattr(position, 'last_update_time') else ''
            }
            trade_data.append(trade_info)

        if not trade_data:
            self.logger.info("No trades to save")
            # Create empty file anyway to avoid errors
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['symbol', 'quantity', 'entry_price', 'current_price', 'pnl', 'open_time', 'last_update_time'])
            return

        # Write to CSV
        try:
            with open(filename, 'w', newline='') as f:
                # Assuming all dictionaries have the same keys
                fieldnames = trade_data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(trade_data)
            self.logger.info(f"Successfully saved {len(trade_data)} trades to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving trades: {e}")

    def get_all_positions(self) -> List[Position]:
        """
        Get all positions, including zero and non-zero positions.

        Returns:
            List of all Position objects
        """
        return self.positions

    def register_with_event_manager(self, event_manager):
        """
        Register this position manager with an event manager.
        
        Args:
            event_manager: The event manager to register with
        """
        self.event_manager = event_manager
        self._register_event_handlers()
        self.logger.info("Position Manager registered with Event Manager")
