#!/usr/bin/env python
# main.py - Entry point for the algotrading system

import os
import sys
import argparse
import logging
from pathlib import Path

from utils.config_loader import ConfigLoader
from core.engine import TradingEngine
from backtest.backtesting_engine import BacktestingEngine
from live.live_engine import LiveEngine
from utils.constants import MODES
from utils.exceptions import ConfigError
from core.logging_manager import setup_logging

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='AlgoTrading Framework')
    parser.add_argument('--mode', '-m', choices=[mode.value for mode in MODES], default='live',
                        help='Trading mode: backtest, live, or web')
    parser.add_argument('--config', '-c', default='config/config.yaml',
                        help='Path to configuration file')
    parser.add_argument('--strategy', '-s', default='config/strategies.yaml',
                        help='Path to strategy configuration file')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--output', '-o', help='Output directory for results')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug mode with additional diagnostics')
    return parser.parse_args()

def load_configurations(config_path, strategy_path):
    """Load and validate configurations."""
    logger = logging.getLogger(__name__)

    # Load main configuration
    if not os.path.exists(config_path):
        raise ConfigError(f"Configuration file not found: {config_path}")
    config = ConfigLoader.load_yaml(config_path)
    logger.info(f"Loaded main configuration from {config_path}")

    # Load strategy configuration
    if not os.path.exists(strategy_path):
        raise ConfigError(f"Strategy configuration file not found: {strategy_path}")
    strategy_config = ConfigLoader.load_yaml(strategy_path)
    logger.info(f"Loaded strategy configuration from {strategy_path}")

    # Basic validation
    if not config:
        raise ConfigError(f"Empty or invalid configuration in {config_path}")
    if not strategy_config or 'strategies' not in strategy_config:
        raise ConfigError(f"Missing strategies in {strategy_path}")

    return config, strategy_config

def main():
    """Main entry point for the algotrading system."""
    args = parse_arguments()

    # Setup logging based on verbosity
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(log_level)
    logger = logging.getLogger(__name__)

    try:
        # Load configurations
        config_path = args.config
        strategy_path = args.strategy
        config, strategy_config = load_configurations(config_path, strategy_path)

        # Set output directory if specified
        if args.output:
            output_dir = Path(args.output)
            output_dir.mkdir(exist_ok=True, parents=True)
            config['system'] = config.get('system', {})
            config['system']['output_dir'] = str(output_dir)
            logger.info(f"Output directory set to {output_dir}")

        # Enable debug mode if requested
        if args.debug:
            config['system'] = config.get('system', {})
            config['system']['debug'] = True
            logger.info("Debug mode enabled")

        # Run in specified mode
        if args.mode == 'backtest':
            logger.info("Starting backtest mode")
            engine = BacktestingEngine(config, strategy_config)
            results = engine.run()
            logger.info(f"Backtest completed. Performance metrics: {results['metrics'] if 'metrics' in results else 'Not available'}")

        elif args.mode == 'live':
            logger.info("Starting live trading mode")
            engine = LiveEngine(config, strategy_config)
            success = engine.run()
            if not success:
                logger.error("Live trading engine execution failed")
                return 1
            logger.info("Live trading completed")

        elif args.mode == 'web':
            logger.info("Starting web interface")
            # Import here to avoid loading web dependencies when not needed
            from web.app import WebApp
            app = WebApp(config, strategy_config)
            app.run()

        else:
            logger.error(f"Invalid mode: {args.mode}")
            return 1

    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Error running algotrading system: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())

