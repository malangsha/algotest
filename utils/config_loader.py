import os
import yaml
import json
from typing import Dict, Any, Optional, List, Callable, Union
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigValidationError(Exception):
    """Exception raised when configuration validation fails."""
    pass

class ConfigLoader:
    """Handles loading, validating, and accessing configuration files."""

    def __init__(self, config_dir: str = "config", env_prefix: str = "ALGOTEST_"):
        """
        Initialize the ConfigLoader.
        
        Args:
            config_dir: Directory containing configuration files
            env_prefix: Prefix for environment variables that override config values
        """
        self.config_dir = config_dir
        self.env_prefix = env_prefix
        self._config_cache = {}
        self._validators = {}
        self._schema_cache = {}
        
        # Create config directory if it doesn't exist
        os.makedirs(config_dir, exist_ok=True)

    def load_yaml(self, file_path: str) -> Dict[str, Any]:
        """
        Load a YAML configuration file.

        Args:
            file_path: Path to the YAML file

        Returns:
            Dictionary containing the configuration

        Raises:
            FileNotFoundError: If the file doesn't exist
            yaml.YAMLError: If the file contains invalid YAML
        """
        try:
            with open(file_path, 'r') as file:
                config = yaml.safe_load(file)
                if config is None:
                    return {}
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {file_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {file_path}: {e}")
            raise

    def load_json(self, file_path: str) -> Dict[str, Any]:
        """
        Load a JSON configuration file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dictionary containing the configuration

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        try:
            with open(file_path, 'r') as file:
                config = json.load(file)
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file {file_path}: {e}")
            raise

    def load_config(self, config_name: str = "config", reload: bool = False) -> Dict[str, Any]:
        """
        Load a configuration file with environment variable overrides.

        Args:
            config_name: Name of the configuration file without extension
            reload: Whether to reload the configuration from disk even if cached

        Returns:
            Dictionary containing the configuration with environment variable overrides
        """
        # Check cache first
        if not reload and config_name in self._config_cache:
            return self._config_cache[config_name]
            
        # Try YAML first, then JSON
        config_yaml_path = os.path.join(self.config_dir, f"{config_name}.yaml")
        config_yml_path = os.path.join(self.config_dir, f"{config_name}.yml")
        config_json_path = os.path.join(self.config_dir, f"{config_name}.json")
        
        config = {}
        
        # Try to load YAML file
        try:
            if os.path.exists(config_yaml_path):
                config = self.load_yaml(config_yaml_path)
            elif os.path.exists(config_yml_path):
                config = self.load_yaml(config_yml_path)
            elif os.path.exists(config_json_path):
                config = self.load_json(config_json_path)
            else:
                logger.warning(f"No configuration file found for {config_name}")
                # Create default config file
                self._create_default_config(config_name)
                if os.path.exists(config_yaml_path):
                    config = self.load_yaml(config_yaml_path)
        except Exception as e:
            logger.error(f"Error loading configuration {config_name}: {e}")
            # If we can't load the config, return an empty dict
            config = {}
            
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        # Validate configuration if a validator is registered
        if config_name in self._validators:
            try:
                self._validators[config_name](config)
            except ConfigValidationError as e:
                logger.error(f"Configuration validation failed for {config_name}: {e}")
                # Continue with the invalid config, but log the error
        
        # Cache the result
        self._config_cache[config_name] = config
        
        return config

    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to the configuration.
        
        Environment variables should be in the format:
        {env_prefix}SECTION_SUBSECTION_KEY=value
        
        For example:
        ALGOTEST_BROKER_API_TIMEOUT=30
        
        Args:
            config: Original configuration dictionary
            
        Returns:
            Configuration with environment variable overrides applied
        """
        # Create a copy of the config to avoid modifying the original
        result = config.copy()
        
        # Get all environment variables with the prefix
        for key, value in os.environ.items():
            if key.startswith(self.env_prefix):
                # Remove prefix and split by underscore
                config_path = key[len(self.env_prefix):].lower()
                path_parts = config_path.split('_')
                
                # Convert value to appropriate type
                typed_value = self._convert_env_value(value)
                
                # Apply the override
                self._set_nested_value(result, path_parts, typed_value)
                
                logger.debug(f"Applied environment override: {key}={value}")
                
        return result
    
    def _convert_env_value(self, value: str) -> Any:
        """
        Convert environment variable string value to appropriate type.
        
        Args:
            value: String value from environment variable
            
        Returns:
            Converted value with appropriate type
        """
        # Try to convert to int
        if value.isdigit():
            return int(value)
            
        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            pass
            
        # Check for boolean values
        if value.lower() in ('true', 'yes', 'y', '1'):
            return True
        if value.lower() in ('false', 'no', 'n', '0'):
            return False
            
        # Check for null/None
        if value.lower() in ('null', 'none'):
            return None
            
        # Check for list (comma-separated values)
        if ',' in value:
            return [self._convert_env_value(item.strip()) for item in value.split(',')]
            
        # Default to string
        return value
    
    def _set_nested_value(self, config: Dict[str, Any], path_parts: List[str], value: Any) -> None:
        """
        Set a nested value in the configuration dictionary.
        
        Args:
            config: Configuration dictionary to modify
            path_parts: List of keys representing the path to the value
            value: Value to set
        """
        current = config
        for i, part in enumerate(path_parts):
            if i == len(path_parts) - 1:
                # Last part, set the value
                current[part] = value
            else:
                # Create nested dict if it doesn't exist
                if part not in current or not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]

    def register_validator(self, config_name: str, validator: Callable[[Dict[str, Any]], None]) -> None:
        """
        Register a validation function for a configuration.
        
        Args:
            config_name: Name of the configuration file without extension
            validator: Function that validates the configuration and raises ConfigValidationError if invalid
        """
        self._validators[config_name] = validator

    def register_schema(self, config_name: str, schema: Dict[str, Any]) -> None:
        """
        Register a JSON schema for validating a configuration.
        
        Args:
            config_name: Name of the configuration file without extension
            schema: JSON schema dictionary
        """
        self._schema_cache[config_name] = schema
        
        # Create a validator function that uses the schema
        def schema_validator(config: Dict[str, Any]) -> None:
            try:
                import jsonschema
                jsonschema.validate(instance=config, schema=schema)
            except ImportError:
                logger.warning("jsonschema package not installed, skipping schema validation")
            except jsonschema.exceptions.ValidationError as e:
                raise ConfigValidationError(f"Schema validation failed: {e}")
                
        # Register the validator
        self.register_validator(config_name, schema_validator)

    def get_value(self, config: Dict[str, Any], key_path: str, default: Optional[Any] = None) -> Any:
        """
        Safely retrieve a nested configuration value using dot notation.

        Args:
            config: Configuration dictionary
            key_path: Dot-separated path to the configuration value (e.g., "broker.api.timeout")
            default: Default value to return if the key is not found

        Returns:
            The configuration value or the default if not found
        """
        keys = key_path.split('.')
        value = config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    def get_config_value(self, config_name: str, key_path: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a configuration value from a named configuration.
        
        Args:
            config_name: Name of the configuration file without extension
            key_path: Dot-separated path to the configuration value
            default: Default value to return if the key is not found
            
        Returns:
            The configuration value or the default if not found
        """
        config = self.load_config(config_name)
        return self.get_value(config, key_path, default)

    def save_config(self, config_name: str, config: Dict[str, Any], format: str = "yaml") -> None:
        """
        Save a configuration to a file.
        
        Args:
            config_name: Name of the configuration file without extension
            config: Configuration dictionary to save
            format: Format to save in ("yaml" or "json")
            
        Raises:
            ValueError: If the format is not supported
        """
        if format.lower() not in ("yaml", "json"):
            raise ValueError(f"Unsupported format: {format}")
            
        # Create config directory if it doesn't exist
        os.makedirs(self.config_dir, exist_ok=True)
        
        if format.lower() == "yaml":
            file_path = os.path.join(self.config_dir, f"{config_name}.yaml")
            with open(file_path, 'w') as file:
                yaml.dump(config, file, default_flow_style=False)
        else:
            file_path = os.path.join(self.config_dir, f"{config_name}.json")
            with open(file_path, 'w') as file:
                json.dump(config, file, indent=2)
                
        # Update cache
        self._config_cache[config_name] = config
        
        logger.info(f"Saved configuration to {file_path}")

    def _create_default_config(self, config_name: str) -> None:
        """
        Create a default configuration file if it doesn't exist.
        
        Args:
            config_name: Name of the configuration file without extension
        """
        # Define default configurations for known config types
        defaults = {
            "config": {
                "app": {
                    "name": "AlgoTrading Framework",
                    "version": "1.0.0",
                    "log_level": "INFO",
                    "log_file": "logs/algotrading.log"
                },
                "event_manager": {
                    "queue_size": 1000,
                    "worker_threads": 2,
                    "retry_queue_size": 500
                },
                "data_feed": {
                    "type": "csv",
                    "file_path": "data/market_data.csv",
                    "symbols": ["AAPL", "MSFT", "GOOG"]
                },
                "broker": {
                    "type": "simulated",
                    "commission": 0.001,
                    "slippage": 0.001,
                    "api": {
                        "timeout": 30,
                        "retry_attempts": 3
                    }
                }
            },
            "strategies": {
                "strategies": [
                    {
                        "id": "moving_average_crossover",
                        "name": "Moving Average Crossover",
                        "symbols": ["AAPL", "MSFT"],
                        "parameters": {
                            "short_window": 10,
                            "long_window": 50
                        },
                        "enabled": True
                    }
                ]
            },
            "credentials": {
                "broker": {
                    "api_key": "YOUR_API_KEY",
                    "api_secret": "YOUR_API_SECRET",
                    "account_id": "YOUR_ACCOUNT_ID"
                }
            }
        }
        
        # Check if we have a default for this config
        if config_name in defaults:
            # Create the config file
            file_path = os.path.join(self.config_dir, f"{config_name}.yaml")
            
            # Only create if it doesn't exist
            if not os.path.exists(file_path):
                with open(file_path, 'w') as file:
                    yaml.dump(defaults[config_name], file, default_flow_style=False)
                    
                logger.info(f"Created default configuration file: {file_path}")

    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache = {}
        logger.debug("Configuration cache cleared")

    @staticmethod
    def load_strategies(config_dir: str = "config") -> Dict[str, Any]:
        """
        Load the strategies configuration file.

        Args:
            config_dir: Directory containing configuration files

        Returns:
            Dictionary containing the strategies configuration
        """
        # For backward compatibility
        loader = ConfigLoader(config_dir=config_dir)
        return loader.load_config("strategies")

    @staticmethod
    def load_credentials(config_dir: str = "config") -> Dict[str, Any]:
        """
        Load broker credentials from a separate file for security.

        Args:
            config_dir: Directory containing configuration files

        Returns:
            Dictionary containing the credentials configuration
        """
        # For backward compatibility
        loader = ConfigLoader(config_dir=config_dir)
        return loader.load_config("credentials")

    @staticmethod
    def get_config_value(config: Dict[str, Any], key_path: str, default: Optional[Any] = None) -> Any:
        """
        Safely retrieve a nested configuration value using dot notation.

        Args:
            config: Configuration dictionary
            key_path: Dot-separated path to the configuration value (e.g., "broker.api.timeout")
            default: Default value to return if the key is not found

        Returns:
            The configuration value or the default if not found
        """
        # For backward compatibility
        loader = ConfigLoader()
        return loader.get_value(config, key_path, default)


# Create a global instance for easy access
config_loader = ConfigLoader()

def load_config(config_name: str = "config", reload: bool = False) -> Dict[str, Any]:
    """
    Load a configuration file using the global config loader.
    
    Args:
        config_name: Name of the configuration file without extension
        reload: Whether to reload the configuration from disk even if cached
        
    Returns:
        Dictionary containing the configuration
    """
    return config_loader.load_config(config_name, reload)

def get_config_value(config_name: str, key_path: str, default: Optional[Any] = None) -> Any:
    """
    Get a configuration value using the global config loader.
    
    Args:
        config_name: Name of the configuration file without extension
        key_path: Dot-separated path to the configuration value
        default: Default value to return if the key is not found
        
    Returns:
        The configuration value or the default if not found
    """
    return config_loader.get_config_value(config_name, key_path, default)
