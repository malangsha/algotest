import logging
import time
import pandas as pd # type: ignore
import threading
import queue
import os
from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime

from core.event_manager import EventManager
from models.events import EventType, MarketDataEvent, FillEvent, SignalEvent, OrderEvent
from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument
from models.position import Position # Assuming Position class is in models.position
from models.trade import Trade as ModelTrade # Assuming Trade class is in models.trade
from brokers.broker_interface import BrokerInterface # FinvasiaBroker implements this
from utils.constants import MarketDataType, OrderSide, InstrumentType
from utils.config_loader import get_config_value # For initial capital
from core.logging_manager import get_logger

class PaperTradingSimulator:
    """
    Simulates order creation from signals and order fills based on market data
    for paper trading mode. Maintains positions, P&L, and logs to Excel.
    """
    def __init__(self, event_manager: EventManager, broker_interface: BrokerInterface,
                 config: Dict[str, Any], excel_filename: str = "output/paper_trades_monitor.xlsx"):
        """
        Initialize the PaperTradingSimulator.

        Args:
            event_manager: The system's EventManager.
            broker_interface: The broker interface (FinvasiaBroker in paper mode).
            config: The main application configuration.
            excel_filename: Name of the Excel file for monitoring.
        """
        self.logger = get_logger(__name__)
        self.event_manager = event_manager
        self.broker_interface = broker_interface
        self.config = config

        # Portfolio and Position Management
        self.initial_capital = get_config_value(self.config, 'portfolio.initial_capital', 1000000.0)
        self.cash_balance: float = self.initial_capital
        self.positions: Dict[str, Position] = {}  # instrument_id -> Position object
        
        self.portfolio_history: List[Dict[str, Any]] = [] # For potential summary reporting

        # Excel Export Setup
        self.excel_filename = excel_filename
        self.excel_queue: queue.Queue = queue.Queue()
        self._stop_excel_thread = threading.Event()
        self.excel_thread = threading.Thread(target=self._excel_writer_thread, daemon=True)
        self.excel_lock = threading.Lock() # Lock for file operations if needed, though queue serializes

        # Ensure Excel file has headers if it's new
        self._init_excel_file()

        self._register_event_handlers()
        self.logger.info(f"Paper Trading Simulator initialized with capital: {self.initial_capital:.2f}. Logging to {self.excel_filename}")

    def _init_excel_file(self):
        """Initializes the Excel file with headers if it doesn't exist or is empty."""
        if not os.path.exists(self.excel_filename) or os.path.getsize(self.excel_filename) == 0:
            headers = pd.DataFrame(columns=[
                'Timestamp', 'OrderID (Internal)', 'BrokerOrderID', 'Symbol', 'Exchange',
                'Side', 'OrderType', 'OrderQty', 'OrderPrice',
                'FillQty', 'FillPrice', 'Commission', 'FillID',
                'PositionQty', 'AvgEntryPrice', 'UnrealizedPnL', 'RealizedPnL (Trade)',
                'CumulativeRealizedPnL (Symbol)', 'CashBalance'
            ])
            try:
                with self.excel_lock: # Should not be strictly necessary here but good practice
                    headers.to_excel(self.excel_filename, index=False, sheet_name="Trades")
                self.logger.info(f"Initialized Excel file with headers: {self.excel_filename}")
            except Exception as e:
                self.logger.error(f"Failed to initialize Excel file {self.excel_filename}: {e}", exc_info=True)


    def _register_event_handlers(self):
        """Register to receive Signal and MarketData events."""
        self.event_manager.subscribe(
            EventType.SIGNAL,
            self._on_signal_event,
            component_name="PaperTradingSimulator_SignalHandler"
        )
        self.event_manager.subscribe(
            EventType.MARKET_DATA,
            self._on_market_data,
            component_name="PaperTradingSimulator_MarketDataHandler"
        )
        self.logger.debug("PaperTradingSimulator subscribed to SIGNAL and MARKET_DATA events")

    def _on_signal_event(self, event: SignalEvent):
        """
        Handle incoming SignalEvent to create and place a paper order.
        """
        if not self.broker_interface or not getattr(self.broker_interface, 'paper_trading', False):
            return

        self.logger.debug(f"[PAPER_SIM] Received signal: {event.symbol} {event.side} Qty: {event.quantity} @ {event.price} (SignalID: {event.event_id})")

        # Create an Order object from the SignalEvent
        # instrument_id for Order should be consistent with MarketDataEvent.instrument.symbol
        # Assuming SignalEvent.symbol is the primary symbol (e.g., RELIANCE) and
        # SignalEvent.exchange provides the exchange (e.g., NSE)
        
        instrument_id = event.symbol # This should be the key used in MarketDataEvent.instrument.symbol
        
        order_side_value = event.side.value if isinstance(event.side, Enum) else event.side

        order_to_place = Order(
            instrument_id=instrument_id,
            quantity=event.quantity if event.quantity is not None else 0,
            side=order_side_value, # Order class expects string 'BUY' or 'SELL'
            order_type=event.order_type if event.order_type else OrderType.MARKET,
            price=event.price,
            stop_price=event.trigger_price,
            strategy_id=event.strategy_id,
            params={'exchange': event.exchange, 'signal_id': event.event_id} # Store exchange and signal_id
        )

        # Call broker's place_order (which handles paper_trading internally)
        broker_order_id = self.broker_interface.place_order(order_to_place)
        
        if broker_order_id:
            self.logger.info(f"[PAPER_SIM] Placed paper order {broker_order_id} (internal: {order_to_place.order_id}) for signal on {event.symbol}")
        else:
            self.logger.error(f"[PAPER_SIM] Failed to place paper order for signal on {event.symbol}")


    def _on_market_data(self, event: MarketDataEvent):
        """
        Handle incoming market data and check for potential paper order fills.
        Updates positions and logs to Excel upon fills.
        Args:
            event: The MarketDataEvent.
        """
        if not self.broker_interface or not getattr(self.broker_interface, 'paper_trading', False):
            return

        if not hasattr(self.broker_interface, 'simulated_orders') or \
           not hasattr(self.broker_interface, 'get_pending_paper_orders'):
            self.logger.error("Broker interface is missing attributes for paper trading (simulated_orders/get_pending_paper_orders).")
            return
       
        last_price = event.data.get(MarketDataType.LAST_PRICE)
        bid_price = event.data.get(MarketDataType.BID, last_price) 
        ask_price = event.data.get(MarketDataType.ASK, last_price) 
        instrument_symbol = event.instrument.symbol if event.instrument else None
        instrument_exchange = event.instrument.exchange if event.instrument else "UNKNOWN_EX"

        if last_price is None or instrument_symbol is None:
            return 

        # Update last price for existing positions for unrealized P&L
        if instrument_symbol in self.positions:
            self.positions[instrument_symbol].update_market_price(last_price, datetime.fromtimestamp(event.timestamp / 1000))
            # Optionally, log position update to Excel here if required beyond fills
            # self._log_position_update_to_excel(self.positions[instrument_symbol])


        pending_orders = self.broker_interface.get_pending_paper_orders() # broker_order_id -> Order

        for broker_order_id, order in list(pending_orders.items()):
            if not all(hasattr(order, attr) for attr in ['instrument_id', 'order_type', 'side', 'quantity', 'price', 'status']):
                self.logger.warning(f"Skipping paper order {broker_order_id}: missing critical attributes.")
                continue

            if order.instrument_id != instrument_symbol:
                continue 

            fill_price: Optional[float] = None
            should_fill = False
            
            order_side_upper = order.side.upper()

            if order.order_type == OrderType.MARKET:
                fill_price = ask_price if order_side_upper == OrderSide.BUY.value else bid_price
                should_fill = True
            elif order.order_type == OrderType.LIMIT:
                if order_side_upper == OrderSide.BUY.value and ask_price is not None and order.price is not None and ask_price <= order.price:
                    fill_price = min(ask_price, order.price) # Fill at ask or limit, whichever is better for buyer (lower)
                    should_fill = True
                elif order_side_upper == OrderSide.SELL.value and bid_price is not None and order.price is not None and bid_price >= order.price:
                    fill_price = max(bid_price, order.price) # Fill at bid or limit, whichever is better for seller (higher)
                    should_fill = True
            elif order.order_type == OrderType.SL:
                if order.stop_price is None: continue
                if order_side_upper == OrderSide.BUY.value and last_price >= order.stop_price:
                    fill_price = ask_price # Market order once triggered
                    should_fill = True
                elif order_side_upper == OrderSide.SELL.value and last_price <= order.stop_price:
                    fill_price = bid_price # Market order once triggered
                    should_fill = True
            elif order.order_type == OrderType.SL_M:
                if order.stop_price is None or order.price is None: continue
                if order_side_upper == OrderSide.BUY.value:
                    if last_price >= order.stop_price and ask_price is not None and ask_price <= order.price:
                        fill_price = min(ask_price, order.price)
                        should_fill = True
                elif order_side_upper == OrderSide.SELL.value:
                    if last_price <= order.stop_price and bid_price is not None and bid_price >= order.price:
                        fill_price = max(bid_price, order.price)
                        should_fill = True
            
            if should_fill and fill_price is not None:
                remaining_qty = order.quantity - order.filled_quantity
                # Simulate available volume - for simplicity, assume full fill of remaining if market allows
                # A more complex simulation might use event.data.get(MarketDataType.VOLUME) or bid/ask sizes
                simulated_market_volume = remaining_qty # Simple: assume market can take it
                
                fill_quantity = min(remaining_qty, simulated_market_volume)
                
                if fill_quantity <= 1e-9: # Effectively zero
                    continue

                self.logger.debug(f"[PAPER_SIM] Simulating fill for order {broker_order_id} ({order.order_id}) "
                                 f"Instrument: {order.instrument_id}, Qty: {fill_quantity}, Price: {fill_price:.2f}")
                
                # Update order object
                order.filled_quantity += fill_quantity
                new_avg_fill_price = ((order.average_fill_price * (order.filled_quantity - fill_quantity)) + (fill_price * fill_quantity)) / order.filled_quantity
                order.average_fill_price = new_avg_fill_price
                order.last_updated = datetime.now()

                if abs(order.filled_quantity - order.quantity) < 1e-9 : # Fully filled
                    order.status = OrderStatus.FILLED
                    order.filled_at = datetime.now()
                else:
                    order.status = OrderStatus.PARTIALLY_FILLED
                
                # Create FillEvent
                sim_fill_id = f"PAPER_FILL_{self.broker_interface.next_sim_fill_id}"
                self.broker_interface.next_sim_fill_id += 1
                
                # Ensure order.side is an OrderSide enum for FillEvent
                try:
                    order_side_enum = OrderSide(order_side_upper)
                except ValueError:
                    self.logger.error(f"Invalid order side '{order_side_upper}' for order {order.order_id}. Defaulting to BUY.")
                    order_side_enum = OrderSide.BUY


                fill_event = FillEvent(
                    timestamp=int(time.time() * 1000), # Current time for fill
                    order_id=order.order_id, # Internal order ID
                    broker_order_id=broker_order_id,
                    symbol=order.instrument_id,
                    exchange=order.params.get('exchange', instrument_exchange), # Get from order or market data
                    side=order_side_enum,
                    quantity=fill_quantity,
                    price=fill_price,
                    commission=self._calculate_commission(fill_quantity, fill_price), 
                    strategy_id=order.strategy_id,
                    fill_id=sim_fill_id
                )
                self.event_manager.publish(fill_event)

                # Update internal portfolio: position and cash balance
                position = self.positions.get(fill_event.symbol)
                if not position:
                    position = Position(
                        instrument_id=fill_event.symbol, # instrument_id for Position
                        instrument=event.instrument, # Pass the full Instrument object
                        strategy_id=fill_event.strategy_id
                    )
                    self.positions[fill_event.symbol] = position

                trade_for_position = ModelTrade( # Use the renamed ModelTrade
                    order_id=fill_event.order_id,
                    instrument_id=fill_event.symbol,
                    quantity=fill_event.quantity,
                    price=fill_event.price,
                    side=fill_event.side.value,
                    timestamp=datetime.fromtimestamp(fill_event.timestamp / 1000),
                    commission=fill_event.commission,
                    strategy_id=fill_event.strategy_id
                )
                realized_pnl_from_this_trade = position.apply_trade(trade_for_position)
                position.update_market_price(fill_price, datetime.fromtimestamp(fill_event.timestamp / 1000))


                # Update cash balance
                trade_value = fill_event.quantity * fill_event.price
                if fill_event.side == OrderSide.BUY:
                    self.cash_balance -= (trade_value + fill_event.commission)
                else: # SELL
                    self.cash_balance += (trade_value - fill_event.commission)

                # Log to Excel via queue
                excel_data_row = {
                    'Timestamp': datetime.fromtimestamp(fill_event.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'OrderID (Internal)': fill_event.order_id,
                    'BrokerOrderID': fill_event.broker_order_id,
                    'Symbol': fill_event.symbol,
                    'Exchange': fill_event.exchange,
                    'Side': fill_event.side.value,
                    'OrderType': order.order_type.value,
                    'OrderQty': order.quantity,
                    'OrderPrice': order.price if order.order_type != OrderType.MARKET else fill_price, # For market, log fill price as order price
                    'FillQty': fill_event.quantity,
                    'FillPrice': fill_event.price,
                    'Commission': fill_event.commission,
                    'FillID': fill_event.fill_id,
                    'PositionQty': position.quantity,
                    'AvgEntryPrice': position.average_price if position.quantity != 0 else 0.0,
                    'UnrealizedPnL': position.unrealized_pnl if position.quantity != 0 else 0.0,
                    'RealizedPnL (Trade)': realized_pnl_from_this_trade,
                    'CumulativeRealizedPnL (Symbol)': position.realized_pnl,
                    'CashBalance': self.cash_balance
                }
                self.excel_queue.put(excel_data_row)
                
                # Publish order update event for the fill
                if hasattr(self.broker_interface, '_publish_paper_order_event'):
                    self.broker_interface._publish_paper_order_event(order) # type: ignore

    def _calculate_commission(self, quantity: float, price: float) -> float:
        # Example: 0.01% of trade value, min 0.01
        commission_rate = self.config.get('live', {}).get('paper_trading_settings', {}).get('commission', 0.0001) # 0.01%
        min_commission = 0.01 
        trade_value = quantity * price
        commission = trade_value * commission_rate
        return max(commission, min_commission)


    def _excel_writer_thread(self):
        """Consumes data from excel_queue and writes to Excel file."""
        self.logger.debug("Excel writer thread started.")
        while not self._stop_excel_thread.is_set():
            try:
                data_row = self.excel_queue.get(timeout=1) # Timeout to allow checking stop flag
                if data_row is None: # Sentinel to stop
                    self.excel_queue.task_done()
                    break 
                self._append_to_excel(data_row)
                self.excel_queue.task_done()
            except queue.Empty:
                continue # No data, loop again
            except Exception as e:
                self.logger.error(f"Error in Excel writer thread: {e}", exc_info=True)
        self.logger.debug("Excel writer thread finished.")

    def _append_to_excel(self, data_row: Dict[str, Any]):
        """Appends a single row of data to the Excel file."""
        df_new_row = pd.DataFrame([data_row])
        self.logger.debug(f"Appending to Excel: {data_row}")
        try:
            with self.excel_lock:
                 # Check if file exists to determine if header is needed
                write_header = not os.path.exists(self.excel_filename) or os.path.getsize(self.excel_filename) == 0
                
                # Using pd.ExcelWriter to append without overwriting existing sheets or data
                # This is generally safer for appending.
                # However, for simple row appending, reading, concatenating, and writing might be more robust
                # if openpyxl append mode has issues or if performance is critical for very frequent writes.
                # For now, let's try a common approach:
                try:
                    df_existing = pd.read_excel(self.excel_filename, sheet_name="Trades")
                    df_to_write = pd.concat([df_existing, df_new_row], ignore_index=True)
                except FileNotFoundError: # File doesn't exist yet
                    df_to_write = df_new_row
                    write_header = True # Should have been caught by _init_excel_file, but double check

                # Ensure columns are in the desired order for writing
                desired_columns = [
                    'Timestamp', 'OrderID (Internal)', 'BrokerOrderID', 'Symbol', 'Exchange',
                    'Side', 'OrderType', 'OrderQty', 'OrderPrice',
                    'FillQty', 'FillPrice', 'Commission', 'FillID',
                    'PositionQty', 'AvgEntryPrice', 'UnrealizedPnL', 'RealizedPnL (Trade)',
                    'CumulativeRealizedPnL (Symbol)', 'CashBalance'
                ]
                # Reorder df_to_write if it's not empty
                if not df_to_write.empty:
                    df_to_write = df_to_write.reindex(columns=desired_columns)


                df_to_write.to_excel(self.excel_filename, index=False, sheet_name="Trades")

        except Exception as e:
            self.logger.error(f"Failed to write to Excel file {self.excel_filename}: {e}", exc_info=True)


    def start(self):
        self.excel_thread.start()
        self.logger.info("Paper Trading Simulator started (listening for signals and market data).")
        return True

    def stop(self):
        self.logger.info("Stopping Paper Trading Simulator...")
        # Signal Excel writer thread to stop and wait for it
        if self.excel_thread.is_alive():
            self._stop_excel_thread.set()
            self.excel_queue.put(None) # Sentinel to unblock queue.get if waiting
            self.excel_thread.join(timeout=5)
            if self.excel_thread.is_alive():
                self.logger.warning("Excel writer thread did not terminate gracefully.")
        
        self.logger.info("Paper Trading Simulator stopped.")
        return True

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Returns a summary of the current portfolio state."""
        total_portfolio_value = self.cash_balance
        total_unrealized_pnl = 0.0
        total_realized_pnl = 0.0

        active_positions = []
        for symbol, pos in self.positions.items():
            if pos.quantity != 0 and pos.last_price is not None: # Active position
                market_value = pos.market_value if pos.market_value is not None else 0
                total_portfolio_value += market_value
                unrealized_pnl = pos.unrealized_pnl if pos.unrealized_pnl is not None else 0
                total_unrealized_pnl += unrealized_pnl
            total_realized_pnl += pos.realized_pnl
            active_positions.append(pos.to_dict())


        return {
            "timestamp": datetime.now().isoformat(),
            "initial_capital": self.initial_capital,
            "cash_balance": self.cash_balance,
            "total_portfolio_value": total_portfolio_value,
            "total_unrealized_pnl": total_unrealized_pnl,
            "total_realized_pnl": total_realized_pnl,
            "overall_pnl": (total_portfolio_value - self.initial_capital),
            "active_positions_count": len([p for p in self.positions.values() if p.quantity !=0]),
            "positions_details": active_positions
        }

    def export_final_summary_to_excel(self, summary_filename: str = "paper_trade_summary.xlsx"):
        """Exports a final summary of portfolio and positions to a new Excel file."""
        summary_data = self.get_portfolio_summary()
        
        # Convert summary_data (which is a dict) to DataFrame rows
        portfolio_df_data = {k: [v] for k, v in summary_data.items() if k != "positions_details"}
        portfolio_df = pd.DataFrame(portfolio_df_data)
        
        positions_df = pd.DataFrame(summary_data["positions_details"])

        try:
            with pd.ExcelWriter(summary_filename, engine='openpyxl') as writer:
                portfolio_df.to_excel(writer, sheet_name='PortfolioSummary', index=False)
                if not positions_df.empty:
                    positions_df.to_excel(writer, sheet_name='FinalPositions', index=False)
            self.logger.info(f"Final portfolio summary exported to {summary_filename}")
        except Exception as e:
            self.logger.error(f"Failed to export final summary to {summary_filename}: {e}", exc_info=True)

if __name__ == '__main__':
    # Example Usage
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Mock components for testing
    mock_event_manager = EventManager()
    
    # Use the FinvasiaBroker in paper mode
    mock_broker_config = {
        "broker_connections": {
            "active_connection": "paper_finvasia",
            "connections": [{
                "connection_name": "paper_finvasia", "broker_type": "finvasia", "paper_trading": True
            }]
        },
        "portfolio": {"initial_capital": 100000},
        "live": {"paper_trading_settings": {"commission": 0.0002}} # 0.02% commission
    }
    # Need to make FinvasiaBroker importable or define it here for the example
    # For this example, let's assume FinvasiaBroker is in the same directory or PYTHONPATH
    try:
        from brokers.finvasia_broker import FinvasiaBroker # Assuming it's in finvasia_broker.py
    except ImportError:
        # Minimal mock if FinvasiaBroker is not available for the example run
        class FinvasiaBroker(BrokerInterface): # type: ignore
            def __init__(self, config=None, paper_trading=True, **kwargs):
                super().__init__()
                self.paper_trading = paper_trading
                self.simulated_orders: Dict[str, Order] = {}
                self.next_sim_order_id_counter = 1
                self.next_sim_fill_id = 1
                self.config = config or {}
                self.logger.info("Using MOCK FinvasiaBroker for example.")

            def place_order(self, order: Order) -> Optional[str]:
                if not self.paper_trading: return None
                broker_id = f"PAPER_{self.next_sim_order_id_counter}"
                self.next_sim_order_id_counter+=1
                order.broker_order_id = broker_id
                order.status = OrderStatus.SUBMITTED
                self.simulated_orders[broker_id] = order
                self.logger.info(f"[MOCK_BROKER] Placed {broker_id} for {order.instrument_id}")
                # Simulate publishing OrderEvent
                if self.event_manager:
                    self.event_manager.publish(OrderEvent(order_id=order.order_id, broker_order_id=broker_id, symbol=order.instrument_id, status=OrderStatus.SUBMITTED, side=OrderSide(order.side), quantity=order.quantity, order_type=order.order_type, price=order.price, event_type=EventType.ORDER, timestamp=int(time.time()*1000)))

                return broker_id
            def get_pending_paper_orders(self): return {k:v for k,v in self.simulated_orders.items() if v.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]}
            def _publish_paper_order_event(self, order: Order): pass # Mocked

    mock_broker = FinvasiaBroker(config=mock_broker_config, paper_trading=True)
    mock_broker.set_event_manager(mock_event_manager)

    simulator = PaperTradingSimulator(mock_event_manager, mock_broker, config=mock_broker_config, excel_filename="output/test_excel_monitor.xlsx")
    simulator.start()

    # 1. Simulate a SignalEvent for buying RELIANCE
    reliance_instrument = Instrument(symbol="RELIANCE-EQ", exchange="NSE", instrument_type=InstrumentType.EQUITY, lot_size=1, tick_size=0.05)
    buy_signal = SignalEvent(
        symbol=reliance_instrument.symbol, # type: ignore
        exchange=reliance_instrument.exchange, # type: ignore
        signal_type="ENTRY", # type: ignore
        side=OrderSide.BUY,
        quantity=10,
        order_type=OrderType.LIMIT,
        price=2800.0,
        strategy_id="ExampleStrategy",
        event_type=EventType.SIGNAL, # Required by Event base
        timestamp=int(time.time()*1000) # Required by Event base
    )
    mock_event_manager.publish(buy_signal)
    time.sleep(0.1) # Allow signal to be processed

    # 2. Simulate MarketDataEvent to fill the buy order
    market_data_fill_buy = MarketDataEvent(
        instrument=reliance_instrument,
        data={
            MarketDataType.LAST_PRICE: 2799.0,
            MarketDataType.BID: 2798.5,
            MarketDataType.ASK: 2799.0, # Ask price allows limit buy at 2800 to fill
        },
        event_type=EventType.MARKET_DATA, # Required
        timestamp=int(time.time()*1000) # Required
    )
    mock_event_manager.publish(market_data_fill_buy)
    time.sleep(0.1) # Allow fill processing and Excel write

    # 3. Simulate a SignalEvent for selling RELIANCE
    sell_signal = SignalEvent(
        symbol=reliance_instrument.symbol, # type: ignore
        exchange=reliance_instrument.exchange, # type: ignore
        signal_type="EXIT", # type: ignore
        side=OrderSide.SELL,
        quantity=5, # Sell partial
        order_type=OrderType.MARKET,
        strategy_id="ExampleStrategy",
        event_type=EventType.SIGNAL,
        timestamp=int(time.time()*1000)
    )
    mock_event_manager.publish(sell_signal)
    time.sleep(0.1)

    # 4. Simulate MarketDataEvent to fill the sell order
    market_data_fill_sell = MarketDataEvent(
        instrument=reliance_instrument,
        data={
            MarketDataType.LAST_PRICE: 2850.0, # Price moved up
            MarketDataType.BID: 2850.0, # Bid price for market sell
            MarketDataType.ASK: 2850.5,
        },
        event_type=EventType.MARKET_DATA,
        timestamp=int(time.time()*1000)
    )
    mock_event_manager.publish(market_data_fill_sell)
    time.sleep(0.5) # Allow processing and Excel write

    print("\nPortfolio Summary after trades:")
    summary = simulator.get_portfolio_summary()
    for k, v in summary.items():
        if k != "positions_details":
            print(f"  {k}: {v}")
    print("  Positions Details:")
    for pos_detail in summary["positions_details"]:
        print(f"    {pos_detail}")
    
    simulator.export_final_summary_to_excel("test_final_summary.xlsx")

    simulator.stop()
    print(f"\nExample run complete. Check '{simulator.excel_filename}' and 'output/test_final_summary.xlsx'.")

