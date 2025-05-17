import pytest
import pandas as pd # type: ignore
import os
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from unittest.mock import MagicMock, patch

# Adjust imports based on your project structure
from core.event_manager import EventManager
from models.events import MarketDataEvent, FillEvent, SignalEvent, EventType
from models.order import Order, OrderStatus, OrderType
from models.instrument import Instrument, InstrumentType
from models.position import Position
from paper_trading_simulator import PaperTradingSimulator # Assuming this is the name of your simulator file
from finvasia_broker import FinvasiaBroker # Assuming this is your broker file
from utils.constants import MarketDataType, OrderSide

# Configure basic logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_config() -> Dict[str, Any]:
    """Provides a mock configuration dictionary."""
    return {
        "portfolio": {"initial_capital": 100000.0},
        "live": {"paper_trading_settings": {"commission": 0.0001}}, # 0.01% commission
        "logging": {"log_level": "DEBUG"} # Enable debug logs for tests
    }

@pytest.fixture
def event_manager() -> EventManager:
    """Provides an EventManager instance."""
    return EventManager()

@pytest.fixture
def paper_broker(event_manager: EventManager, mock_config: Dict[str, Any]) -> FinvasiaBroker:
    """Provides a FinvasiaBroker instance in paper trading mode."""
    broker = FinvasiaBroker(config=mock_config, paper_trading=True)
    broker.set_event_manager(event_manager) # Link event manager
    return broker

@pytest.fixture
def simulator(event_manager: EventManager, paper_broker: FinvasiaBroker, mock_config: Dict[str, Any], tmp_path) -> PaperTradingSimulator:
    """Provides a PaperTradingSimulator instance with a temporary Excel file."""
    excel_file = tmp_path / "test_monitor.xlsx"
    sim = PaperTradingSimulator(event_manager, paper_broker, mock_config, excel_filename=str(excel_file))
    sim.start() # Start the simulator and its Excel writer thread
    yield sim
    sim.stop() # Ensure simulator is stopped after test

@pytest.fixture
def sample_instrument() -> Instrument:
    """Provides a sample Instrument object."""
    return Instrument(symbol="TESTSTOCK-EQ", exchange="NSE", instrument_type=InstrumentType.EQUITY, lot_size=1, tick_size=0.05)

def create_signal_event(instrument: Instrument, side: OrderSide, quantity: float, 
                        order_type: OrderType, price: Optional[float] = None, 
                        trigger_price: Optional[float] = None) -> SignalEvent:
    """Helper to create a SignalEvent."""
    return SignalEvent(
        symbol=instrument.symbol, # type: ignore
        exchange=instrument.exchange, # type: ignore
        signal_type="ENTRY", # type: ignore
        side=side,
        quantity=quantity,
        order_type=order_type,
        price=price,
        trigger_price=trigger_price,
        strategy_id="TestStrategy",
        event_type=EventType.SIGNAL, # Base Event requirement
        timestamp=int(time.time() * 1000) # Base Event requirement
    )

def create_market_data_event(instrument: Instrument, last_price: float, 
                             bid: Optional[float] = None, ask: Optional[float] = None,
                             volume: Optional[int] = None) -> MarketDataEvent:
    """Helper to create a MarketDataEvent."""
    data = {MarketDataType.LAST_PRICE: last_price}
    if bid is not None:
        data[MarketDataType.BID] = bid
    if ask is not None:
        data[MarketDataType.ASK] = ask
    if volume is not None:
        data[MarketDataType.VOLUME] = volume
    
    return MarketDataEvent(
        instrument=instrument,
        data=data,
        event_type=EventType.MARKET_DATA, # Base Event requirement
        timestamp=int(time.time() * 1000) # Base Event requirement
    )

def test_market_vs_limit_order_behavior(simulator: PaperTradingSimulator, event_manager: EventManager, 
                                       paper_broker: FinvasiaBroker, sample_instrument: Instrument):
    """Tests market and limit order fill logic."""
    
    # 1. Test Market Order (BUY)
    logger.info("Testing Market BUY order...")
    signal_market_buy = create_signal_event(sample_instrument, OrderSide.BUY, 10, OrderType.MARKET)
    event_manager.publish(signal_market_buy)
    time.sleep(0.1) # Allow signal processing

    # Check if order was placed in broker
    assert len(paper_broker.simulated_orders) == 1
    broker_order_id_market = list(paper_broker.simulated_orders.keys())[0]
    market_buy_order = paper_broker.simulated_orders[broker_order_id_market]
    assert market_buy_order.order_type == OrderType.MARKET
    assert market_buy_order.status == OrderStatus.SUBMITTED

    # Simulate market data to fill market order
    fill_event_handler = MagicMock()
    event_manager.subscribe(EventType.FILL, fill_event_handler)
    
    market_data_market_fill = create_market_data_event(sample_instrument, last_price=100.0, bid=99.9, ask=100.1)
    event_manager.publish(market_data_market_fill)
    time.sleep(0.2) # Allow fill processing

    fill_event_handler.assert_called_once()
    fill_event_market: FillEvent = fill_event_handler.call_args[0][0]
    assert fill_event_market.symbol == sample_instrument.symbol
    assert fill_event_market.side == OrderSide.BUY
    assert fill_event_market.quantity == 10
    assert fill_event_market.price == 100.1 # Filled at ask price
    assert market_buy_order.status == OrderStatus.FILLED
    event_manager.unsubscribe(EventType.FILL, fill_event_handler)


    # 2. Test Limit Order (BUY - does not fill initially)
    logger.info("Testing Limit BUY order (no fill)...")
    paper_broker.simulated_orders.clear() # Clear previous orders
    fill_event_handler.reset_mock()

    signal_limit_buy_no_fill = create_signal_event(sample_instrument, OrderSide.BUY, 5, OrderType.LIMIT, price=95.0)
    event_manager.publish(signal_limit_buy_no_fill)
    time.sleep(0.1)
    
    assert len(paper_broker.simulated_orders) == 1
    broker_order_id_limit_nf = list(paper_broker.simulated_orders.keys())[0]
    limit_buy_order_nf = paper_broker.simulated_orders[broker_order_id_limit_nf]

    market_data_no_fill = create_market_data_event(sample_instrument, last_price=98.0, bid=97.9, ask=98.1) # Ask > limit price
    event_manager.subscribe(EventType.FILL, fill_event_handler)
    event_manager.publish(market_data_no_fill)
    time.sleep(0.2)

    fill_event_handler.assert_not_called() # Should not fill
    assert limit_buy_order_nf.status == OrderStatus.SUBMITTED
    event_manager.unsubscribe(EventType.FILL, fill_event_handler)


    # 3. Test Limit Order (BUY - fills)
    logger.info("Testing Limit BUY order (fill)...")
    fill_event_handler.reset_mock() # Reset mock for the next check
    # Order is still pending from previous step (limit_buy_order_nf)

    market_data_limit_fill = create_market_data_event(sample_instrument, last_price=94.0, bid=93.9, ask=94.1) # Ask < limit price
    event_manager.subscribe(EventType.FILL, fill_event_handler)
    event_manager.publish(market_data_limit_fill)
    time.sleep(0.2)

    fill_event_handler.assert_called_once()
    fill_event_limit: FillEvent = fill_event_handler.call_args[0][0]
    assert fill_event_limit.symbol == sample_instrument.symbol
    assert fill_event_limit.side == OrderSide.BUY
    assert fill_event_limit.quantity == 5
    assert fill_event_limit.price == 94.1 # Filled at ask price (which is <= limit price)
    assert limit_buy_order_nf.status == OrderStatus.FILLED
    event_manager.unsubscribe(EventType.FILL, fill_event_handler)


def test_partial_fill_and_position_reconciliation(simulator: PaperTradingSimulator, event_manager: EventManager, 
                                                 paper_broker: FinvasiaBroker, sample_instrument: Instrument):
    """Tests partial fill scenarios and position updates."""
    logger.info("Testing Partial Fill scenario...")
    
    # Place a BUY order for a larger quantity
    signal_large_buy = create_signal_event(sample_instrument, OrderSide.BUY, 20, OrderType.MARKET)
    event_manager.publish(signal_large_buy)
    time.sleep(0.1)

    assert len(paper_broker.simulated_orders) == 1
    broker_order_id = list(paper_broker.simulated_orders.keys())[0]
    order_to_fill = paper_broker.simulated_orders[broker_order_id]

    fill_event_handler = MagicMock()
    event_manager.subscribe(EventType.FILL, fill_event_handler)

    # Simulate market data that can only partially fill the order
    # The current simulator logic fills based on remaining_qty, not market volume.
    # To test partial fill, we'd need to modify simulator to consider MarketDataEvent.volume or bid/ask sizes.
    # For now, the simulator's _on_market_data fills what it can of the order.
    # Let's assume the test implies the order itself gets updated to PARTIALLY_FILLED by external means
    # or the simulator is enhanced.
    # Given the current simulator, it will try to fully fill.
    # To truly test partial fill based on market volume, the simulator's fill logic would need:
    # fill_quantity = min(remaining_qty, event.data.get(MarketDataType.VOLUME, remaining_qty))
    
    # For this test, let's simulate two separate fills for the same order by manipulating order status in broker
    # This is a bit of a hack for testing the simulator's reaction to an order that becomes partially filled.
    # A more direct test would involve a market data event with limited volume.
    # However, the current simulator fills the order fully if conditions are met.
    # Let's adjust the test to reflect how the simulator would process it if the *order* was updated.

    # First fill (simulating external partial fill update to the order object)
    order_to_fill.filled_quantity = 8
    order_to_fill.average_fill_price = 100.0
    order_to_fill.status = OrderStatus.PARTIALLY_FILLED
    order_to_fill.last_updated = datetime.now()
    
    # Publish a fill event as if it happened, to update simulator's portfolio
    partial_fill_event_external = FillEvent(
        order_id=order_to_fill.order_id, broker_order_id=broker_order_id,
        symbol=sample_instrument.symbol, exchange=sample_instrument.exchange, # type: ignore
        side=OrderSide.BUY, quantity=8, price=100.0, commission=0.0,
        fill_id="MANUAL_PARTIAL_1", strategy_id="TestStrategy",
        event_type=EventType.FILL, timestamp=int(time.time()*1000)
    )
    # Manually update simulator's portfolio for this "external" fill
    position = simulator.positions.get(sample_instrument.symbol)
    if not position:
        position = Position(instrument_id=sample_instrument.symbol, instrument=sample_instrument) # type: ignore
        simulator.positions[sample_instrument.symbol] = position
    
    # Create a Trade object for the Position class
    from models.trade import Trade as ModelTrade
    trade1 = ModelTrade(order_id=partial_fill_event_external.order_id, instrument_id=partial_fill_event_external.symbol, quantity=8, price=100.0, side="BUY", timestamp=datetime.now())
    position.apply_trade(trade1)
    simulator.cash_balance -= 8 * 100.0
    
    logger.info(f"After manual partial fill: Position Qty: {position.quantity}, Cash: {simulator.cash_balance}")


    # Now, simulate market data to fill the *remaining* part of the order
    logger.info("Simulating market data for remaining part of the order...")
    market_data_second_fill = create_market_data_event(sample_instrument, last_price=101.0, bid=100.9, ask=101.1)
    event_manager.publish(market_data_second_fill)
    time.sleep(0.2) # Allow processing

    # The simulator should try to fill the remaining 12 (20 - 8)
    # It will generate a FillEvent for these 12.
    fill_event_handler.assert_called_once()
    fill_event_partial: FillEvent = fill_event_handler.call_args[0][0]
    
    assert fill_event_partial.symbol == sample_instrument.symbol
    assert fill_event_partial.side == OrderSide.BUY
    assert fill_event_partial.quantity == 12 # Remaining quantity
    assert fill_event_partial.price == 101.1 # Filled at ask for market order

    # Check order status in broker
    assert order_to_fill.status == OrderStatus.FILLED
    assert order_to_fill.filled_quantity == 20
    
    # Check simulator's internal position
    final_position = simulator.positions.get(sample_instrument.symbol)
    assert final_position is not None
    assert final_position.quantity == 20
    expected_avg_price = ((8 * 100.0) + (12 * 101.1)) / 20
    assert abs(final_position.average_price - expected_avg_price) < 1e-6

    logger.info(f"Final Position Qty: {final_position.quantity}, Avg Price: {final_position.average_price:.2f}")
    event_manager.unsubscribe(EventType.FILL, fill_event_handler)


def test_excel_output_correctness(simulator: PaperTradingSimulator, event_manager: EventManager, 
                                 paper_broker: FinvasiaBroker, sample_instrument: Instrument, tmp_path):
    """Tests if data is correctly written to the Excel file."""
    excel_file_path = simulator.excel_filename
    logger.info(f"Testing Excel output to: {excel_file_path}")

    # Ensure Excel file is initially empty or has only headers
    if os.path.exists(excel_file_path):
         df_initial = pd.read_excel(excel_file_path)
         assert df_initial.empty, "Excel file should be empty or just headers at start of this specific test."
    else: # If _init_excel_file created it
        df_headers = pd.read_excel(excel_file_path)
        assert len(df_headers.columns) > 5 # Check if headers are there
        assert df_headers.empty


    # Simulate a BUY trade
    signal_buy = create_signal_event(sample_instrument, OrderSide.BUY, 10, OrderType.LIMIT, price=150.0)
    event_manager.publish(signal_buy)
    time.sleep(0.1) # Allow signal processing

    broker_order_id_buy = list(paper_broker.simulated_orders.keys())[0]
    buy_order_obj = paper_broker.simulated_orders[broker_order_id_buy]


    market_data_buy_fill = create_market_data_event(sample_instrument, last_price=149.0, bid=148.9, ask=149.1) # Ask <= Limit
    event_manager.publish(market_data_buy_fill)
    time.sleep(0.3) # Allow fill processing and Excel write queue

    # Simulate a SELL trade
    signal_sell = create_signal_event(sample_instrument, OrderSide.SELL, 5, OrderType.MARKET)
    event_manager.publish(signal_sell)
    time.sleep(0.1)
    
    # Get the new sell order (assuming only one new order)
    # This logic might be fragile if multiple orders are processed quickly.
    # A better way would be to capture the broker_order_id from the place_order call if it returned it.
    broker_order_id_sell = None
    sell_order_obj = None
    for b_id, order in paper_broker.simulated_orders.items():
        if order.side.upper() == "SELL" and order.status == OrderStatus.SUBMITTED:
            broker_order_id_sell = b_id
            sell_order_obj = order
            break
    assert broker_order_id_sell is not None, "Sell order not found in broker's simulated orders"


    market_data_sell_fill = create_market_data_event(sample_instrument, last_price=155.0, bid=154.8, ask=155.2)
    event_manager.publish(market_data_sell_fill)
    time.sleep(0.3) # Allow fill processing and Excel write

    # Stop the simulator to ensure all queued Excel writes are flushed
    simulator.stop() # This joins the excel_thread

    # Check Excel file content
    assert os.path.exists(excel_file_path), "Excel file was not created."
    df_excel = pd.read_excel(excel_file_path)
    
    logger.info(f"Excel content:\n{df_excel}")

    assert len(df_excel) == 2, "Excel file should have two trade entries."

    # Validate first row (BUY trade)
    row1 = df_excel.iloc[0]
    assert row1['Symbol'] == sample_instrument.symbol
    assert row1['Side'] == OrderSide.BUY.value
    assert row1['FillQty'] == 10
    assert row1['FillPrice'] == 149.1 # Filled at ask
    assert row1['PositionQty'] == 10
    assert abs(row1['CashBalance'] - (simulator.initial_capital - (10 * 149.1) - row1['Commission'])) < 1e-6
    assert row1['OrderID (Internal)'] == buy_order_obj.order_id
    assert row1['BrokerOrderID'] == broker_order_id_buy


    # Validate second row (SELL trade)
    row2 = df_excel.iloc[1]
    assert row2['Symbol'] == sample_instrument.symbol
    assert row2['Side'] == OrderSide.SELL.value
    assert row2['FillQty'] == 5
    assert row2['FillPrice'] == 154.8 # Filled at bid for market sell
    assert row2['PositionQty'] == 5 # 10 - 5
    
    # Realized PnL for this sell trade: 5 * (154.8 - 149.1)
    expected_realized_pnl_trade = 5 * (154.8 - 149.1)
    assert abs(row2['RealizedPnL (Trade)'] - expected_realized_pnl_trade) < 1e-6
    assert abs(row2['CumulativeRealizedPnL (Symbol)'] - expected_realized_pnl_trade) < 1e-6 # First realizing trade for this symbol
    
    previous_cash = simulator.initial_capital - (10 * 149.1) - row1['Commission']
    current_cash_expected = previous_cash + (5 * 154.8) - row2['Commission']
    assert abs(row2['CashBalance'] - current_cash_expected) < 1e-6
    assert row2['OrderID (Internal)'] == sell_order_obj.order_id # type: ignore
    assert row2['BrokerOrderID'] == broker_order_id_sell

    # Restart simulator for subsequent tests if any, or ensure fixture handles this.
    # The fixture `simulator` already handles start/stop for each test.

