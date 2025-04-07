import logging
import os
import sys
import json
import traceback
from datetime import datetime
from typing import Dict, Optional, List, Union, Any, Callable
import logging.handlers
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
        component_levels: Optional[Dict[str, int]] = None,
        error_email_config: Optional[Dict[str, Any]] = None,
        json_format: bool = False,
        syslog_output: bool = False,
        syslog_address: Optional[str] = None,
        syslog_facility: int = logging.handlers.SysLogHandler.LOG_USER
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
            error_email_config: Configuration for email notifications on errors
            json_format: Whether to format logs as JSON
            syslog_output: Whether to output logs to syslog
            syslog_address: Syslog server address (host:port or /path/to/socket)
            syslog_facility: Syslog facility to use
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
        self.error_email_config = error_email_config
        self.json_format = json_format
        self.syslog_output = syslog_output
        self.syslog_address = syslog_address
        self.syslog_facility = syslog_facility

        # Create logger instance
        self.root_logger = logging.getLogger()

        # Keep track of handlers
        self.console_handler = None
        self.file_handler = None
        self.trade_file_handler = None
        self.error_file_handler = None
        self.email_handler = None
        self.syslog_handler = None

        # Error tracking
        self.error_count = 0
        self.last_error_time = None
        self.error_callbacks = []

        # Configure logging
        self.configure()

    def configure(self) -> None:
        """Configure the logging system with the current settings."""
        # Reset root logger
        for handler in self.root_logger.handlers[:]:
            self.root_logger.removeHandler(handler)
            
        self.root_logger.setLevel(self.log_level)

        # Create formatters
        if self.json_format:
            formatter = self._create_json_formatter()
        else:
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
                self.file_handler = logging.handlers.RotatingFileHandler(
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
            trade_formatter = logging.Formatter('%(asctime)s - %(message)s') if not self.json_format else formatter

            if self.max_file_size_bytes > 0 and self.backup_count > 0:
                self.trade_file_handler = logging.handlers.RotatingFileHandler(
                    trade_log_file,
                    maxBytes=self.max_file_size_bytes,
                    backupCount=self.backup_count
                )
            else:
                self.trade_file_handler = logging.FileHandler(trade_log_file)

            self.trade_file_handler.setFormatter(trade_formatter)

            # Create trade logger and add handler
            self.trade_logger = logging.getLogger('trades')
            for handler in self.trade_logger.handlers[:]:
                self.trade_logger.removeHandler(handler)
                
            self.trade_logger.setLevel(logging.INFO)
            self.trade_logger.propagate = False  # Don't propagate to root logger
            self.trade_logger.addHandler(self.trade_file_handler)

            # Special handler for error logs
            error_log_file = os.path.join(self.log_dir, f"errors_{datetime.now().strftime('%Y%m%d')}.log")
            
            if self.max_file_size_bytes > 0 and self.backup_count > 0:
                self.error_file_handler = logging.handlers.RotatingFileHandler(
                    error_log_file,
                    maxBytes=self.max_file_size_bytes,
                    backupCount=self.backup_count
                )
            else:
                self.error_file_handler = logging.FileHandler(error_log_file)
                
            self.error_file_handler.setFormatter(formatter)
            self.error_file_handler.setLevel(logging.ERROR)
            self.root_logger.addHandler(self.error_file_handler)

        # Add email handler if configured
        if self.error_email_config:
            try:
                from logging.handlers import SMTPHandler
                
                self.email_handler = SMTPHandler(
                    mailhost=self.error_email_config.get('mailhost', 'localhost'),
                    fromaddr=self.error_email_config.get('fromaddr', 'algotrading@localhost'),
                    toaddrs=self.error_email_config.get('toaddrs', []),
                    subject=self.error_email_config.get('subject', 'AlgoTrading Error'),
                    credentials=self.error_email_config.get('credentials'),
                    secure=self.error_email_config.get('secure')
                )
                self.email_handler.setLevel(logging.ERROR)
                self.email_handler.setFormatter(formatter)
                self.root_logger.addHandler(self.email_handler)
            except Exception as e:
                sys.stderr.write(f"Failed to configure email logging: {e}\n")

        # Add syslog handler if enabled
        if self.syslog_output:
            try:
                if self.syslog_address:
                    # Check if address is a socket path or host:port
                    if os.path.exists(self.syslog_address) or self.syslog_address.startswith('/'):
                        self.syslog_handler = logging.handlers.SysLogHandler(
                            address=self.syslog_address,
                            facility=self.syslog_facility
                        )
                    else:
                        # Parse host:port
                        if ':' in self.syslog_address:
                            host, port = self.syslog_address.split(':')
                            port = int(port)
                            self.syslog_handler = logging.handlers.SysLogHandler(
                                address=(host, port),
                                facility=self.syslog_facility
                            )
                        else:
                            # Default to standard syslog port
                            self.syslog_handler = logging.handlers.SysLogHandler(
                                address=(self.syslog_address, 514),
                                facility=self.syslog_facility
                            )
                else:
                    # Use default address
                    self.syslog_handler = logging.handlers.SysLogHandler(
                        facility=self.syslog_facility
                    )
                
                self.syslog_handler.setFormatter(formatter)
                self.root_logger.addHandler(self.syslog_handler)
            except Exception as e:
                sys.stderr.write(f"Failed to configure syslog: {e}\n")

        # Set component-specific log levels
        for component, level in self.component_levels.items():
            logging.getLogger(component).setLevel(level)

        # Capture warnings if enabled
        logging.captureWarnings(self.capture_warnings)

        # Add custom error handler
        self._add_error_handler()

        # Log startup
        self.root_logger.info("Logging system initialized")

    def _create_json_formatter(self):
        """Create a JSON formatter for structured logging."""
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                log_data = {
                    'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                    'level': record.levelname,
                    'logger': record.name,
                    'message': record.getMessage(),
                    'module': record.module,
                    'line': record.lineno
                }
                
                # Add exception info if available
                if record.exc_info:
                    log_data['exception'] = {
                        'type': record.exc_info[0].__name__,
                        'message': str(record.exc_info[1]),
                        'traceback': self.formatException(record.exc_info)
                    }
                
                # Add extra fields
                for key, value in record.__dict__.items():
                    if key not in ['args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                                  'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module',
                                  'msecs', 'message', 'msg', 'name', 'pathname', 'process',
                                  'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName']:
                        log_data[key] = value
                
                return json.dumps(log_data)
                
        return JsonFormatter()

    def _add_error_handler(self):
        """Add a custom handler to track errors and trigger callbacks."""
        class ErrorTrackingHandler(logging.Handler):
            def __init__(self, manager):
                super().__init__(level=logging.ERROR)
                self.manager = manager
                
            def emit(self, record):
                self.manager.error_count += 1
                self.manager.last_error_time = datetime.now()
                
                # Call error callbacks
                for callback in self.manager.error_callbacks:
                    try:
                        callback(record)
                    except Exception as e:
                        sys.stderr.write(f"Error in error callback: {e}\n")
        
        self.root_logger.addHandler(ErrorTrackingHandler(self))

    def register_error_callback(self, callback: Callable[[logging.LogRecord], None]) -> None:
        """
        Register a callback to be called when an error is logged.
        
        Args:
            callback: Function to call with the LogRecord when an error occurs
        """
        self.error_callbacks.append(callback)

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
        
        # Set component-specific level if configured
        if name in self.component_levels:
            logger.setLevel(self.component_levels[name])
            
        return logger
        
    def get_error_stats(self) -> Dict[str, Any]:
        """
        Get statistics about logged errors.
        
        Returns:
            Dict[str, Any]: Error statistics
        """
        return {
            'error_count': self.error_count,
            'last_error_time': self.last_error_time.isoformat() if self.last_error_time else None
        }
        
    def configure_from_dict(self, config: Dict[str, Any]) -> None:
        """
        Configure the logging system from a dictionary.
        
        Args:
            config: Dictionary containing logging configuration
        """
        # Update attributes from config
        if 'log_level' in config:
            self.log_level = self._parse_log_level(config['log_level'])
        if 'log_format' in config:
            self.log_format = config['log_format']
        if 'log_dir' in config:
            self.log_dir = config['log_dir']
        if 'console_output' in config:
            self.console_output = config['console_output']
        if 'file_output' in config:
            self.file_output = config['file_output']
        if 'max_file_size_mb' in config:
            self.max_file_size_bytes = config['max_file_size_mb'] * 1024 * 1024
        if 'backup_count' in config:
            self.backup_count = config['backup_count']
        if 'capture_warnings' in config:
            self.capture_warnings = config['capture_warnings']
        if 'component_levels' in config:
            self.component_levels = {
                k: self._parse_log_level(v) for k, v in config['component_levels'].items()
            }
        if 'error_email_config' in config:
            self.error_email_config = config['error_email_config']
        if 'json_format' in config:
            self.json_format = config['json_format']
        if 'syslog_output' in config:
            self.syslog_output = config['syslog_output']
        if 'syslog_address' in config:
            self.syslog_address = config['syslog_address']
        if 'syslog_facility' in config:
            self.syslog_facility = config['syslog_facility']
            
        # Reconfigure logging
        self.configure()
        
    def _parse_log_level(self, level) -> int:
        """
        Parse a log level from string or int.
        
        Args:
            level: Log level as string or int
            
        Returns:
            int: Logging level
        """
        if isinstance(level, int):
            return level
            
        if isinstance(level, str):
            level_upper = level.upper()
            if hasattr(logging, level_upper):
                return getattr(logging, level_upper)
                
        # Default to INFO if invalid
        return logging.INFO

def setup_logging(log_level: Union[int, str] = logging.INFO,
                 log_dir: str = "logs",
                 console_output: bool = True,
                 file_output: bool = True,
                 config_dict: Optional[Dict[str, Any]] = None) -> LoggingManager:
    """
    Set up the logging system for the application.

    Args:
        log_level: The logging level to use (default: logging.INFO)
        log_dir: Directory to store log files (default: "logs")
        console_output: Whether to output logs to console (default: True)
        file_output: Whether to output logs to file (default: True)
        config_dict: Optional dictionary with full configuration

    Returns:
        LoggingManager: The configured logging manager instance
    """
    # Create the logging manager
    logging_manager = LoggingManager(
        log_level=log_level if isinstance(log_level, int) else logging.INFO,
        log_dir=log_dir,
        console_output=console_output,
        file_output=file_output
    )
    
    # Apply configuration if provided
    if config_dict:
        logging_manager.configure_from_dict(config_dict)

    # Return the logging manager in case it needs to be used elsewhere
    return logging_manager

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger by name.
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)
