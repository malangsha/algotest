import logging
import time
from datetime import datetime

# Assuming these modules are in the PYTHONPATH or same directory
from core.event_manager import EventManager
from models.events import SignalEvent, MarketDataEvent, EventType
from models.order import OrderType
from models.instrument import Instrument, InstrumentType
from core.paper_trading_simulator import PaperTradingSimulator
from brokers.finvasia_broker import FinvasiaBroker # Your broker implementation
from utils.constants import OrderSide, MarketDataType

def run_example():
    """
    Runs a simple paper trading simulation example.
    """
    # 0. Setup basic logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("PaperTradeExample")
    logger.info("Starting Paper Trading Simulation Example...")

    # 1. Initialize components
    event_manager = EventManager()

    # Mock configuration for the broker and simulator
    # In a real app, this would be loaded from a YAML/JSON file
    example_config = {
        "broker_connections": {
            "active_connection": "paper_finvasia_example",
            "connections": [
                {
                    "connection_name": "paper_finvasia_example",
                    "broker_type": "finvasia", # Should match broker class name or a factory key
                    "paper_trading": True # Crucial for this example
                }
            ]
        },
        "portfolio": {
            "initial_capital": 100000.0 # Example initial capital
        },
        "live": { # Section for paper trading settings like commission
            "paper_trading_settings": {
                "commission": 0.0002,  # Example: 0.02% commission per trade
                # "slippage": 0.0001 # Example slippage if simulator supported it directly
            }
        },
        "logging": {"log_level": "INFO"} # Example logging config
    }

    # Initialize FinvasiaBroker in paper trading mode
    # Pass the config to the broker so it knows it's in paper mode
    broker = FinvasiaBroker(config=example_config, paper_trading=True)
    broker.set_event_manager(event_manager) # Important for broker to publish OrderEvents

    # Initialize PaperTradingSimulator
    # It will use the broker_interface (which is our paper-mode FinvasiaBroker)
    # and the event_manager to listen for signals and market data.
    excel_monitor_file = "example_paper_trades.xlsx"
    simulator = PaperTradingSimulator(event_manager, broker, example_config, excel_filename=excel_monitor_file)
    
    # Start the simulator (this also starts its Excel writer thread)
    simulator.start()
    logger.info(f"Simulator started. Monitoring trades in {excel_monitor_file}")

    # 2. Define an instrument to trade
    nifty_spot = Instrument(
        symbol="NIFTY50-IDX", # Example symbol for Nifty Spot Index
        exchange="NSE",
        instrument_type=InstrumentType.INDEX,
        description="NIFTY 50 Index",
        tick_size=0.05,
        lot_size=1 # For indices, lot size is effectively 1 for P&L calculation multiplier
    )
    # For actual trading, you'd use a tradable instrument like a future or option
    reliance_eq = Instrument(
        symbol="RELIANCE-EQ",
        exchange="NSE",
        instrument_type=InstrumentType.EQUITY,
        description="Reliance Industries Equity",
        tick_size=0.05,
        lot_size=1
    )


    # 3. Simulate a SignalEvent (e.g., from a strategy) to BUY Reliance
    logger.info("Publishing BUY Signal for RELIANCE-EQ...")
    buy_signal_reliance = SignalEvent(
        symbol=reliance_eq.symbol, # type: ignore
        exchange=reliance_eq.exchange, # type: ignore
        signal_type="ENTRY", # type: ignore
        side=OrderSide.BUY,
        quantity=10,
        order_type=OrderType.LIMIT,
        price=2850.00, # Limit price for the buy order
        strategy_id="MyExampleStrategy",
        event_type=EventType.SIGNAL, # Base Event requirement
        timestamp=int(time.time() * 1000) # Base Event requirement
    )
    event_manager.publish(buy_signal_reliance)
    time.sleep(0.2) # Give a moment for the signal to be processed and order placed by simulator

    # 4. Simulate MarketDataEvent that could fill the order
    logger.info("Publishing Market Data for RELIANCE-EQ to fill BUY order...")
    market_data_reliance_fill_buy = MarketDataEvent(
        instrument=reliance_eq,
        data={
            MarketDataType.LAST_PRICE: 2849.50,
            MarketDataType.BID: 2849.00,
            MarketDataType.ASK: 2849.50, # Ask price is favorable for the limit buy
        },
        event_type=EventType.MARKET_DATA, # Base Event requirement
        timestamp=int(time.time() * 1000) # Base Event requirement
    )
    event_manager.publish(market_data_reliance_fill_buy)
    time.sleep(0.5) # Allow time for fill processing and Excel logging


    # 5. Simulate another SignalEvent to SELL Reliance (partial position)
    logger.info("Publishing SELL Signal for RELIANCE-EQ...")
    sell_signal_reliance = SignalEvent(
        symbol=reliance_eq.symbol, # type: ignore
        exchange=reliance_eq.exchange, # type: ignore
        signal_type="EXIT", # type: ignore
        side=OrderSide.SELL,
        quantity=5, # Selling 5 out of 10 shares
        order_type=OrderType.MARKET, # Market order for exit
        strategy_id="MyExampleStrategy",
        event_type=EventType.SIGNAL,
        timestamp=int(time.time() * 1000)
    )
    event_manager.publish(sell_signal_reliance)
    time.sleep(0.2)

    # 6. Simulate MarketDataEvent to fill the SELL order
    logger.info("Publishing Market Data for RELIANCE-EQ to fill SELL order...")
    market_data_reliance_fill_sell = MarketDataEvent(
        instrument=reliance_eq,
        data={
            MarketDataType.LAST_PRICE: 2880.00, # Price has gone up
            MarketDataType.BID: 2879.80, # Market sell will likely fill at bid
            MarketDataType.ASK: 2880.20,
        },
        event_type=EventType.MARKET_DATA,
        timestamp=int(time.time() * 1000)
    )
    event_manager.publish(market_data_reliance_fill_sell)
    time.sleep(0.5) # Allow time for fill processing and Excel logging

    # 7. Display final portfolio summary (optional)
    final_summary = simulator.get_portfolio_summary()
    logger.info("\n--- Final Portfolio Summary ---")
    logger.info(f"Initial Capital: {final_summary['initial_capital']:.2f}")
    logger.info(f"Cash Balance: {final_summary['cash_balance']:.2f}")
    logger.info(f"Total Portfolio Value: {final_summary['total_portfolio_value']:.2f}")
    logger.info(f"Total Unrealized P&L: {final_summary['total_unrealized_pnl']:.2f}")
    logger.info(f"Total Realized P&L: {final_summary['total_realized_pnl']:.2f}")
    logger.info(f"Overall P&L: {final_summary['overall_pnl']:.2f}")
    logger.info(f"Active Positions Count: {final_summary['active_positions_count']}")
    if final_summary['positions_details']:
        logger.info("Positions Details:")
        for pos_dict in final_summary['positions_details']:
            logger.info(f"  Symbol: {pos_dict['instrument_id']}, Qty: {pos_dict['quantity']}, "
                        f"AvgPrice: {pos_dict['average_price']:.2f}, RealizedPnL: {pos_dict['realized_pnl']:.2f}, "
                        f"UnrealizedPnL: {pos_dict.get('unrealized_pnl', 0.0):.2f}")
    
    # Export a final summary sheet
    simulator.export_final_summary_to_excel("example_trade_final_summary.xlsx")


    # 8. Stop the simulator (this also stops the Excel writer thread)
    logger.info("Stopping the simulator...")
    simulator.stop()
    event_manager.stop() # Stop the event manager if it has a stop method

    logger.info(f"Example finished. Check '{excel_monitor_file}' and 'example_trade_final_summary.xlsx' for output.")

if __name__ == "__main__":
    run_example()

