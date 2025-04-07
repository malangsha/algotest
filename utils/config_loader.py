import os
import yaml
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Handles loading and validating configuration files."""

    @staticmethod
    def load_yaml(file_path: str) -> Dict[str, Any]:
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

    @staticmethod
    def load_config(config_dir: str = "config") -> Dict[str, Any]:
        """
        Load the main configuration file.

        Args:
            config_dir: Directory containing configuration files

        Returns:
            Dictionary containing the configuration
        """
        config_path = os.path.join(config_dir, "config.yaml")
        return ConfigLoader.load_yaml(config_path)

    @staticmethod
    def load_strategies(config_dir: str = "config") -> Dict[str, Any]:
        """
        Load the strategies configuration file.

        Args:
            config_dir: Directory containing configuration files

        Returns:
            Dictionary containing the strategies configuration
        """
        strategies_path = os.path.join(config_dir, "strategies.yaml")
        return ConfigLoader.load_yaml(strategies_path)

    @staticmethod
    def load_credentials(config_dir: str = "config") -> Dict[str, Any]:
        """
        Load broker credentials from a separate file for security.

        Args:
            config_dir: Directory containing configuration files

        Returns:
            Dictionary containing the credentials configuration
        """
        credentials_path = os.path.join(config_dir, "credentials.yaml")
        try:
            return ConfigLoader.load_yaml(credentials_path)
        except FileNotFoundError:
            logger.warning(f"Credentials file not found: {credentials_path}")
            return {}

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
        keys = key_path.split('.')
        value = config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
