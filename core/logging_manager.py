import logging
import os
import sys
from datetime import datetime
from typing import Dict, Optional, List, Union, Any
import json
from pathlib import Path

class LoggingManager:
    """
    Manages logging configuration for the trading system.
    """
    def __init__(
        self,
        log_level: int = logging.INFO,
        log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        log_dir: str = "logs",
        console_output: bool = True,
        file_output: bool = True,
        max_file_size_mb: int = 10,
        backup_count: int = 5,
        capture_warnings: bool = True,
        component_levels: Optional[Dict[str, int]] = None
    ):
        """
        Initialize the LoggingManager.

        Args:
            log_level: Default logging level
            log_format: Format string for log messages
            log_dir: Directory to store log files
            console_output: Whether to output logs to console
            file_output: Whether to output logs to file
            max_file_size_mb: Maximum size of each log file in MB
            backup_count: Number of backup log files to keep
            capture_warnings: Whether to capture Python warnings
            component_levels: Dictionary mapping components to specific log levels
        """
        self.log_level = log_level
        self.log_format = log_format
        self.log_dir = log_dir
        self.console_output = console_output
        self.file_output = file_output
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.backup_count = backup_count
        self.capture_warnings = capture_warnings
        self.component_levels = component_levels or {}

        # Create logger instance
        self.root_logger = logging.getLogger()

        # Keep track of handlers
        self.console_handler = None
        self.file_handler = None
        self.trade_file_handler = None

        # Configure logging
        self.configure()

    def configure(self) -> None:
        """Configure the logging system with the current settings."""
        # Reset root logger
        self.root_logger.handlers = []
        self.root_logger.setLevel(self.log_level)

        # Create formatter
        formatter = logging.Formatter(self.log_format)

        # Add console handler if enabled
        if self.console_output:
            self.console_handler = logging.StreamHandler(sys.stdout)
            self.console_handler.setFormatter(formatter)
            self.root_logger.addHandler(self.console_handler)

        # Add file handler if enabled
        if self.file_output:
            # Create log directory if it doesn't exist
            os.makedirs(self.log_dir, exist_ok=True)

            # Regular logs
            log_file = os.path.join(self.log_dir, f"trading_{datetime.now().strftime('%Y%m%d')}.log")

            if self.max_file_size_bytes > 0 and self.backup_count > 0:
                # Use rotating file handler
                from logging.handlers import RotatingFileHandler
                self.file_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=self.max_file_size_bytes,
                    backupCount=self.backup_count
                )
            else:
                # Use regular file handler
                self.file_handler = logging.FileHandler(log_file)

            self.file_handler.setFormatter(formatter)
            self.root_logger.addHandler(self.file_handler)

            # Special handler for trade logs
            trade_log_file = os.path.join(self.log_dir, f"trades_{datetime.now().strftime('%Y%m%d')}.log")
            trade_formatter = logging.Formatter('%(asctime)s - %(message)s')

            if self.max_file_size_bytes > 0 and self.backup_count > 0:
                from logging.handlers import RotatingFileHandler
                self.trade_file_handler = RotatingFileHandler(
                    trade_log_file,
                    maxBytes=self.max_file_size_bytes,
                    backupCount=self.backup_count
                )
            else:
                self.trade_file_handler = logging.FileHandler(trade_log_file)

            self.trade_file_handler.setFormatter(trade_formatter)

            # Create trade logger and add handler
            self.trade_logger = logging.getLogger('trades')
            self.trade_logger.handlers = []
            self.trade_logger.setLevel(logging.INFO)
            self.trade_logger.propagate = False  # Don't propagate to root logger
            self.trade_logger.addHandler(self.trade_file_handler)

        # Set component-specific log levels
        for component, level in self.component_levels.items():
            logging.getLogger(component).setLevel(level)

        # Capture warnings if enabled
        logging.captureWarnings(self.capture_warnings)

        # Log startup
        self.root_logger.info("Logging system initialized")

    def set_level(self, level: int, component: Optional[str] = None) -> None:
        """
        Set the logging level for a component or the root logger.

        Args:
            level: Logging level to set
            component: Component to set level for, or None for root logger
        """
        if component:
            self.component_levels[component] = level
            logging.getLogger(component).setLevel(level)
        else:
            self.log_level = level
            self.root_logger.setLevel(level)

        self.root_logger.info(f"Set log level to {level} for {'root' if component is None else component}")

    def reset_levels(self) -> None:
        """Reset all log levels to the default."""
        self.component_levels = {}
        self.root_logger.setLevel(self.log_level)
        self.root_logger.info("Reset all log levels to default")

    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger by name with the appropriate level.

        Args:
            name: Logger name

        Returns:
            logging.Logger: Logger instance
        """
        logger = logging.getLogger(name)

def setup_logging(log_level: int = logging.INFO,
                 log_dir: str = "logs",
                 console_output: bool = True,
                 file_output: bool = True) -> LoggingManager:
    """
    Set up the logging system for the application.

    Args:
        log_level: The logging level to use (default: logging.INFO)
        log_dir: Directory to store log files (default: "logs")
        console_output: Whether to output logs to console (default: True)
        file_output: Whether to output logs to file (default: True)

    Returns:
        LoggingManager: The configured logging manager instance
    """
    # Create and configure the logging manager
    logging_manager = LoggingManager(
        log_level=log_level,
        log_dir=log_dir,
        console_output=console_output,
        file_output=file_output
    )

    # Return the logging manager in case it needs to be used elsewhere
    return logging_manager

