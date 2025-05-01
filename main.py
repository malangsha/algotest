#!/usr/bin/env python
# main.py - Entry point for the algotrading system

import os
import sys
import argparse
import logging
import signal
import atexit
import warnings
from pathlib import Path
from typing import Tuple, Dict, Any, Optional

from utils.config_loader import get_config_loader, load_config, get_config_value, ConfigValidationError, ConfigLoader
from core.engine import TradingEngine
from backtest.backtesting_engine import BacktestingEngine
from live.live_engine import LiveEngine
from utils.constants import MODES
from utils.exceptions import ConfigError
from core.logging_manager import setup_logging, get_logger

warnings.simplefilter("ignore", FutureWarning)

# Global variables for engine management
current_engine = None
shutdown_requested = False


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='AlgoTrading Framework')
    parser.add_argument('--mode', '-m', 
                        choices=[mode.value for mode in MODES], 
                        default='live',
                        help='Trading mode: backtest, live, or web')
    parser.add_argument('--config', '-c', 
                        default='config',
                        help='Name of configuration file (without extension) or directory path')
    parser.add_argument('--strategy', '-s', 
                        default='strategies',
                        help='Name of strategy configuration file (without extension) or path')
    parser.add_argument('--verbose', '-v', 
                        action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--output', '-o', 
                        help='Output directory for results')
    parser.add_argument('--debug', '-d', 
                        action='store_true',
                        help='Enable debug mode with additional diagnostics')
    parser.add_argument('--reload', '-r', 
                        action='store_true',
                        help='Force reload of configuration files (ignore cache)')
    return parser.parse_args()


def _resolve_config_path(config_arg: str, force_reload: bool = False) -> Tuple[Dict[str, Any], ConfigLoader]:
    """
    Helper function to resolve configuration path and load config.
    
    Args:
        config_arg: Configuration name or path
        force_reload: Whether to force reload from disk
        
    Returns:
        Tuple[Dict, ConfigLoader]: The loaded configuration and the config loader
    """
    # Determine if config_arg is a path or just a name
    if os.path.isfile(config_arg) or '/' in config_arg or '\\' in config_arg:
        # It's a path, extract directory and filename
        config_path = Path(config_arg)
        config_dir = str(config_path.parent)
        config_name = config_path.stem

        # Create a new config loader with the specified directory
        custom_loader = get_config_loader() if config_dir == "config" else ConfigLoader(config_dir=config_dir)
        
        # Load the configuration
        config = custom_loader.load_config(config_name, reload=force_reload)
        return config, custom_loader
    else:
        # It's just a name, use the default loader
        config_loader = get_config_loader()
        config = load_config(config_arg, reload=force_reload)
        return config, config_loader


def load_configurations(config_arg: str, strategy_arg: str, force_reload: bool = False) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load and validate configurations using the enhanced config_loader.

    Args:
        config_arg: Configuration name or path
        strategy_arg: Strategy configuration name or path
        force_reload: Whether to force reload from disk

    Returns:
        tuple: (config, strategy_config)

    Raises:
        ConfigError: If configuration loading or validation fails
    """
    logger = get_logger(__name__)

    try:
        # Determine if config_arg is a path or just a name
        if os.path.isfile(config_arg) or '/' in config_arg or '\\' in config_arg:
            # It's a path, extract directory and filename
            config_path = Path(config_arg)
            config_dir = str(config_path.parent)
            config_name = config_path.stem

            # Create a new config loader with the specified directory
            custom_loader = get_config_loader()
            if config_dir != "config":
                custom_loader = ConfigLoader(config_dir=config_dir)

            # Load the configuration
            config = custom_loader.load_config(config_name, reload=force_reload)
            logger.info(f"Loaded main configuration from {config_arg}")
        else:
            # It's just a name, use the default loader
            config = load_config(config_arg, reload=force_reload)
            logger.info(f"Loaded main configuration '{config_arg}'")

        # Similarly for strategy configuration
        if os.path.isfile(strategy_arg) or '/' in strategy_arg or '\\' in strategy_arg:
            strategy_path = Path(strategy_arg)
            strategy_dir = str(strategy_path.parent)
            strategy_name = strategy_path.stem

            custom_loader = get_config_loader()
            if strategy_dir != "config":
                custom_loader = ConfigLoader(config_dir=strategy_dir)

            strategy_config = custom_loader.load_config(strategy_name, reload=force_reload)
            logger.info(f"Loaded strategy configuration from {strategy_arg}")
        else:
            strategy_config = load_config(strategy_arg, reload=force_reload)
            logger.info(f"Loaded strategy configuration '{strategy_arg}'")

        # Basic validation
        if not config:
            raise ConfigError(f"Empty or invalid configuration: {config_arg}")
        if not strategy_config or 'strategies' not in strategy_config:
            raise ConfigError(f"Missing strategies in configuration: {strategy_arg}")

        return config, strategy_config

    except FileNotFoundError as e:
        raise ConfigError(f"Configuration file not found: {e}")
    except ConfigValidationError as e:
        raise ConfigError(f"Configuration validation failed: {e}")
    except Exception as e:
        raise ConfigError(f"Error loading configuration: {str(e)}")


def setup_signal_handlers():
    """
    Set up signal handlers for graceful shutdown.
    """
    logger = get_logger("main")

    def signal_handler(sig, frame):
        """
        Handle termination signals by gracefully shutting down the engine.
        """
        global shutdown_requested
        
        if shutdown_requested:
            logger.warning("Shutdown already in progress, ignoring additional signal")
            return

        shutdown_requested = True

        signal_name = "SIGINT/Ctrl+C" if sig == signal.SIGINT else f"SIGTERM" if sig == signal.SIGTERM else f"Signal {sig}"
        logger.info(f"Received {signal_name}, initiating graceful shutdown...")

        # Gracefully shut down the engine if it exists
        global current_engine
        if current_engine:
            try:
                logger.info("Stopping trading engine...")
                # Try different possible shutdown methods
                if hasattr(current_engine, 'stop') and callable(current_engine.stop):
                    current_engine.stop()
                elif hasattr(current_engine, 'shutdown') and callable(current_engine.shutdown):
                    current_engine.shutdown()
                else:
                    logger.warning("Engine does not have a stop or shutdown method")
                logger.info("Trading engine stopped")
            except Exception as e:
                logger.error(f"Error stopping engine: {e}")

        logger.info("Shutdown complete, exiting...")
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    # Register atexit handler for additional cleanup
    def atexit_handler():
        global shutdown_requested
        if not shutdown_requested:
            logger.info("Process ending, performing final cleanup...")
            global current_engine
            if current_engine and hasattr(current_engine, 'stop') and callable(current_engine.stop):
                try:
                    current_engine.stop()
                except Exception as e:
                    logger.error(f"Error in final cleanup: {e}")

    atexit.register(atexit_handler)
    logger.debug("Signal handlers registered for graceful shutdown")


def is_engine_running() -> bool:
    """
    Check if the current engine is running.
    
    Returns:
        bool: True if engine is running, False otherwise
    """
    global current_engine
    if current_engine is None:
        return False

    if hasattr(current_engine, 'is_running') and callable(current_engine.is_running):
        return current_engine.is_running()
    elif hasattr(current_engine, 'running'):
        return current_engine.running
    return False


def configure_logging(config: Dict[str, Any]) -> Any:
    """
    Configure the logging system based on the provided configuration.
    
    Args:
        config: The application configuration dictionary
        
    Returns:
        Any: The configured logging manager
    """
    log_config = config.get('logging', {})
    
    # Setup logging with the configuration
    return setup_logging(
        log_level=log_config.get('log_level', logging.INFO),
        log_dir=log_config.get('log_dir', "logs"),
        console_output=log_config.get('console_output', True),
        file_output=log_config.get('file_output', True),
        config_dict=log_config if log_config else None
    )


def run_backtest_mode(config: Dict[str, Any], strategy_config: Dict[str, Any]) -> int:
    """
    Run the system in backtest mode.
    
    Args:
        config: Main configuration
        strategy_config: Strategy configuration
        
    Returns:
        int: Exit code (0 for success)
    """
    logger = get_logger("main")
    logger.info("Starting backtest mode")
    
    global current_engine
    current_engine = BacktestingEngine(config, strategy_config)
    
    try:
        results = current_engine.run()
        metrics_str = str(results.get('metrics', 'Not available'))
        logger.info(f"Backtest completed. Performance metrics: {metrics_str}")
        return 0
    except Exception as e:
        logger.exception(f"Error running backtest: {e}")
        return 1


def run_live_mode(config: Dict[str, Any], strategy_config: Dict[str, Any]) -> int:
    """
    Run the system in live trading mode.
    
    Args:
        config: Main configuration
        strategy_config: Strategy configuration
        
    Returns:
        int: Exit code (0 for success)
    """
    logger = get_logger("main")
    logger.info("Starting live trading mode")
    
    global current_engine
    current_engine = LiveEngine(config, strategy_config)
    
    try:
        success = current_engine.start()
        
        if not success:
            logger.error("Failed to start live trading engine")
            return 1
            
        logger.info("Live trading engine running, press Ctrl+C to stop")
        
        # Let the engine run until it stops or is signaled to stop
        # This simpler approach replaces the previous thread monitoring
        # The engine should manage its own threads internally
        while is_engine_running() and not shutdown_requested:
            signal.pause()  # Wait for signals efficiently
            
        return 0
    except Exception as e:
        logger.exception(f"Error in live trading: {e}")
        if is_engine_running():
            logger.info("Stopping live trading engine due to error...")
            current_engine.stop()
        return 1
    finally:
        logger.info("Live trading completed")


def run_web_mode(config: Dict[str, Any], strategy_config: Dict[str, Any]) -> int:
    """
    Run the system in web interface mode.
    
    Args:
        config: Main configuration
        strategy_config: Strategy configuration
        
    Returns:
        int: Exit code (0 for success)
    """
    logger = get_logger("main")
    logger.info("Starting web interface")
    
    try:
        # Import here to avoid loading web dependencies when not needed
        from web.app import WebApp
        
        global current_engine
        current_engine = WebApp(config, strategy_config)
        current_engine.run()
        return 0
    except Exception as e:
        logger.exception(f"Error running web interface: {e}")
        return 1


def main() -> int:
    """
    Main entry point for the algotrading system.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    args = parse_arguments()
    logger = None  # Initialize to ensure it's available in the exception handlers
    
    try:
        # Load configurations first to access logging settings
        config, strategy_config = load_configurations(
            args.config,
            args.strategy,
            force_reload=args.reload
        )
        
        # Setup logging based on configuration and verbosity
        if args.verbose:
            # Override the log level to DEBUG if verbose flag is set
            config.setdefault('logging', {})['log_level'] = 'DEBUG'
        
        # Initialize the central logging system
        logging_manager = configure_logging(config)
        logger = get_logger("main")
        
        logger.info("AlgoTrading system starting")
        logger.debug(f"Command line arguments: {args}")

        # Set up signal handlers for graceful shutdown
        setup_signal_handlers()

        # Set output directory if specified
        if args.output:
            output_dir = Path(args.output)
            output_dir.mkdir(exist_ok=True, parents=True)
            config.setdefault('system', {})['output_dir'] = str(output_dir)
            logger.info(f"Output directory set to {output_dir}")

        # Enable debug mode if requested
        if args.debug:
            config.setdefault('system', {})['debug'] = True
            logger.info("Debug mode enabled")

        # Run in specified mode
        if args.mode == 'backtest':
            return run_backtest_mode(
                config, 
                strategy_config
            )
        elif args.mode == 'live':
            return run_live_mode(
                config, 
                strategy_config
            )
        elif args.mode == 'web':
            return run_web_mode(
                config, 
                strategy_config
            )
        else:
            logger.error(f"Invalid mode: {args.mode}")
            return 1

    except ConfigError as e:
        # Get logger here since we might have failed before configuring logging
        if not logger:
            logger = logging.getLogger("main")
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        # This should be handled by the signal handler, but just in case
        if not logger:
            logger = logging.getLogger("main")
        logger.info("Operation interrupted by user")
        return 0
    except Exception as e:
        if not logger:
            logger = logging.getLogger("main")
        logger.exception(f"Error running algotrading system: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
