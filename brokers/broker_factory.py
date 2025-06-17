import importlib
import inspect
from typing import Dict, Any, Type, Optional

from brokers.broker_interface import BrokerInterface
from utils.exceptions import BrokerError
from core.logging_manager import get_logger

class BrokerFactory:
    """
    Factory class for creating broker instances.
    """
    _broker_registry: Dict[str, Type[BrokerInterface]] = {}
    _logger = get_logger("brokers.broker_factory")

    @classmethod
    def register_broker(cls, broker_type: str, broker_class: Type[BrokerInterface]) -> None:
        """
        Register a broker implementation.

        Args:
            broker_type: Name to register the broker under
            broker_class: Broker class implementing BrokerInterface
        """
        cls._broker_registry[broker_type.lower()] = broker_class
        cls._logger.info(f"Registered broker: {broker_type}")

    @classmethod
    def create_broker(cls, broker_type: str, config: Dict[str, Any], **kwargs) -> BrokerInterface:
        """
        Create a broker instance of the specified type.
        
        Args:
            broker_type: Type of broker to create
            config: Global configuration dictionary
            **kwargs: Additional arguments to pass to the broker constructor
            
        Returns:
            BrokerInterface: Initialized broker instance
            
        Raises:
            BrokerError: If broker type is not registered or initialization fails
        """
        broker_type = broker_type.lower()
        
        # Check if broker type is registered
        if broker_type in cls._broker_registry:
            broker_class = cls._broker_registry[broker_type]
        else:
            # Try to dynamically import the broker module
            try:
                module_name = f"brokers.{broker_type}_broker"
                module = importlib.import_module(module_name)
                
                # Find the broker class in the module
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BrokerInterface) and obj != BrokerInterface:
                        broker_class = obj
                        cls.register_broker(broker_type, broker_class)
                        break
                else:
                    raise BrokerError(f"No broker class found in module: {module_name}")
                    
            except ImportError as e:
                raise BrokerError(f"Failed to import broker module for type '{broker_type}': {e}")
            except Exception as e:
                raise BrokerError(f"Error finding broker class for type '{broker_type}': {e}")
        
        # Create broker instance
        try:
            # Check if session_manager is provided in kwargs
            # session_manager = kwargs.get('session_manager', None)
            
            # Initialize broker
            broker = broker_class(config=config, **kwargs)
            
            # # Set session_manager if provided and broker supports it
            # if session_manager and hasattr(broker, 'set_session_manager'):
            #     broker.set_session_manager(session_manager)
            #     cls._logger.info(f"Set shared session manager for broker: {broker_type}")
            
            return broker
            
        except Exception as e:
            raise BrokerError(f"Failed to initialize broker of type '{broker_type}': {e}")

    @classmethod
    def get_available_brokers(cls) -> Dict[str, Type[BrokerInterface]]:
        """
        Get all registered broker implementations.

        Returns:
            Dict[str, Type[BrokerInterface]]: Map of broker names to classes
        """
        return cls._broker_registry.copy()
