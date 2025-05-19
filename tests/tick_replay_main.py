#!/usr/bin/env python
# tick_replay_main.py - Entry point for tick replay using the tick feeder

import os
import sys
import argparse
import logging
import yaml
from pathlib import Path

from utils.config_loader import load_config
from core.engine import TradingEngine
from live.live_engine import LiveEngine
from core.data_manager import DataManager
from core.option_manager import OptionManager
from core.strategy_manager import StrategyManager
from core.order_manager import OrderManager
from core.event_manager import EventManager
from core.position_manager import PositionManager
from core.portfolio import Portfolio
from core.risk_manager import RiskManager
from core.performance import PerformanceTracker
from brokers.finvasia_broker import FinvasiaBroker

from live.market_data_feeds import FinvasiaFeed
from live.tick_feeder import TickFeeder
from core.logging_manager import setup_logging, get_logger

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Tick Replay for Option Trading Framework')
    parser.add_argument('--config', '-c', 
                        default='config',
                        help='Name of configuration file (without extension) or directory path')
    parser.add_argument('--strategy', '-s', 
                        default='strategies',
                        help='Name of strategy configuration file (without extension) or path')
    parser.add_argument('--tick-file', '-t',
                        default='finvasia.ticks',
                        help='Path to the tick file')
    parser.add_argument('--playback-speed', '-p',
                        type=float,
                        default=2.0,
                        help='Playback speed multiplier (1.0 = real-time, 2.0 = 2x speed, etc.)')
    parser.add_argument('--verbose', '-v', 
                        action='store_true',
                        help='Enable verbose logging')
    return parser.parse_args()

def main() -> int:
    """
    Main entry point for the tick replay system.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    args = parse_arguments()
    
    try:
        # Load configurations
        config = load_config(args.config)
        strategy_config = load_config(args.strategy)
        
        # Setup logging
        if args.verbose:
            config.setdefault('logging', {})['log_level'] = 'DEBUG'
        
        setup_logging(
            log_level=config.get('logging', {}).get('log_level', logging.INFO),
            log_dir=config.get('logging', {}).get('log_dir', "logs"),
            console_output=True,
            file_output=True
        )
        
        logger = get_logger("tick_replay")
        logger.info("Tick Replay system starting")
        
        # Validate tick file exists
        tick_file_path = args.tick_file
        if not os.path.exists(tick_file_path):
            logger.error(f"Tick file not found: {tick_file_path}")
            return 1
        
        # Initialize the event manager
        event_manager = EventManager()
        
        # Initialize the data manager
        data_manager = DataManager(config, event_manager, None)       
    
        position_manager = PositionManager(event_manager)        
        portfolio = Portfolio(config, position_manager)
        risk_manager = RiskManager(portfolio, config)
        order_manager = OrderManager(event_manager, risk_manager)
        performance_tracker = PerformanceTracker(portfolio, config)

        option_manager = OptionManager(data_manager, 
                                        event_manager, 
                                        position_manager, 
                                        config)
        
        
        strategy_manager = StrategyManager(
            data_manager=data_manager,
            event_manager=event_manager,
            portfolio_manager=position_manager,
            risk_manager=risk_manager,
            broker=None,
            config=config
        ) 
        strategy_manager.set_option_manager(option_manager)
        
        # Load strategy configurations
        if strategy_config:
           strategy_manager.load_config(strategy_config, config)
           logger.info("Strategy configurations loaded")
        
        # Initialize the FinvasiaFeed (but don't start it)
        # Extract credentials from config
        broker_config = config.get('broker', {}).get('finvasia', {})
        finvasia_feed = FinvasiaFeed(
            instruments=[],  # Will be populated by the tick feeder
            callback=event_manager.publish,
            user_id=broker_config.get('user_id', ''),
            password=broker_config.get('password', ''),
            twofa=broker_config.get('twofa', ''),
            vc=broker_config.get('vc', ''),
            api_key=broker_config.get('api_key', ''),
            imei=broker_config.get('imei', ''),
            api_url=broker_config.get('api_url', "https://api.shoonya.com/NorenWClientTP/"),
            ws_url=broker_config.get('websocket_url', "wss://api.shoonya.com/NorenWSTP/"),
            debug=args.verbose
        )
        
        # Initialize the tick feeder
        tick_feeder = TickFeeder(
            tick_file_path=tick_file_path,
            finvasia_feed=finvasia_feed,
            data_manager=data_manager,
            playback_speed=args.playback_speed
        )
        
        # Register components with event manager
        # event_manager.register_component(data_manager, "DataManager")
        # event_manager.register_component(option_manager, "OptionManager")
        # event_manager.register_component(strategy_manager, "StrategyManager")
        # event_manager.register_component(order_manager, "OrderManager")
        
        # Start the components
        logger.info("Starting components...")
        
        # Start the strategy manager
        strategy_manager.start()        
              
        # Start the tick feeder
        logger.info(f"Starting tick feeder with playback speed {args.playback_speed}x")
        tick_feeder.start()
        
        # Log active strategies
        strategy_manager.initialize_strategies()
        
        # Keep the main thread running
        logger.info("Tick replay running. Press Ctrl+C to stop.")
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping tick replay...")
            
            # Stop the tick feeder
            tick_feeder.stop()
            
            # Stop the components
            strategy_manager.stop()
            logger.info("Tick replay stopped")
        
        return 0
        
    except Exception as e:
        logger = logging.getLogger("tick_replay")
        logger.exception(f"Error running tick replay: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
