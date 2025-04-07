from typing import Dict, Any, Type
import importlib
import logging
from .broker_interface import BrokerInterface

class BrokerFactory:
    """
    Factory class for creating broker instances.
    """
    _broker_registry: Dict[str, Type[BrokerInterface]] = {}
    _logger = logging.getLogger("brokers.broker_factory")

    @classmethod
    def register_broker(cls, broker_name: str, broker_class: Type[BrokerInterface]) -> None:
        """
        Register a broker implementation.

        Args:
            broker_name: Name to register the broker under
            broker_class: Broker class implementing BrokerInterface
        """
        cls._broker_registry[broker_name.lower()] = broker_class
        cls._logger.info(f"Registered broker: {broker_name}")

    @classmethod
    def create_broker(cls, broker_name: str, config: Dict[str, Any] = None, **kwargs) -> BrokerInterface:
        """
        Create a broker instance by name.

        Args:
            broker_name: Name of the broker to create
            config: Configuration dictionary containing broker settings
            **kwargs: Additional configuration parameters for the broker

        Returns:
            BrokerInterface: Instantiated broker

        Raises:
            ValueError: If broker_name is not registered
        """
        broker_name = broker_name.lower()
        broker_config = {}

        cls._logger.info(f"Creating broker instance: {broker_name}")
        # Extract broker-specific config if available
        if config and "broker" in config:
            broker_list = config.get("broker", [])
            # Find the broker config that matches the requested broker name
            for broker_item in broker_list:
                if broker_item.get("broker_name", "").lower() == broker_name:
                    broker_config = broker_item
                    break

        # Merge kwargs with broker_config (kwargs take precedence)
        broker_config.update(kwargs)

        # First, check if the broker is already registered
        if broker_name in cls._broker_registry:
            broker_class = cls._broker_registry[broker_name]
            cls._logger.info(f"Creating broker instance: {broker_name}")
            try:
                # Try to initialize with broker_config and pass the full config as well
                # return broker_class(config=config, **broker_config)
                return broker_class(config=config, **broker_config)
            except TypeError as e:
                cls._logger.error(f"Error creating broker instance '{broker_name}': {str(e)}")
                # Fall back to just passing the full config object
                try:
                    return broker_class(config=config)
                except TypeError:
                    # Last resort: pass the specific broker config without named parameter
                    return broker_class(broker_config)

        # If not registered, try to dynamically import the broker module
        try:
            # Construct expected module path based on naming convention
            module_name = f"brokers.{broker_name}_broker"
            class_name = f"{broker_name.capitalize()}Broker"

            cls._logger.info(f"config: {config}")
            cls._logger.info(f"trying to import module: {module_name}, class: {class_name}")

            # Check if module exists
            spec = importlib.util.find_spec(module_name)
            if spec is None:
                cls._logger.error(f"Module {module_name} not found")
                raise ValueError(f"Unknown broker: {broker_name}")

            # Import the module
            module = importlib.import_module(module_name)

            # Get the broker class
            if hasattr(module, class_name):
                broker_class = getattr(module, class_name)
                cls.register_broker(broker_name, broker_class)
                cls._logger.info(f"Creating broker instance: {broker_name}")
                try:
                    # Try to initialize with broker_config and pass the full config as well
                    return broker_class(config=config, **broker_config)
                except TypeError as e:
                    cls._logger.error(f"Error creating broker instance '{broker_name}': {str(e)}")
                    # Fall back to just passing the full config object
                    try:
                        return broker_class(config=config)
                    except TypeError:
                        # Last resort: pass the specific broker config without named parameter
                        return broker_class(broker_config)
            else:
                cls._logger.error(f"Class {class_name} not found in module {module_name}")
                raise ValueError(f"Unknown broker class: {class_name}")

        except Exception as e:
            cls._logger.error(f"Failed to dynamically load broker '{broker_name}': {str(e)}")

            # If the broker is "finvasia" and the error is "PENDING", provide more helpful message
            if broker_name == "finvasia" and str(e) == "PENDING":
                cls._logger.warning("Finvasia broker is marked as PENDING implementation")
                cls._logger.warning("Either implement the broker or remove it from configuration")

            raise ValueError(f"Unknown broker: {broker_name}")

    @classmethod
    def get_available_brokers(cls) -> Dict[str, Type[BrokerInterface]]:
        """
        Get all registered broker implementations.

        Returns:
            Dict[str, Type[BrokerInterface]]: Map of broker names to classes
        """
        return cls._broker_registry.copy()
