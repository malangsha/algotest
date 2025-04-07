from typing import Dict, Type, Optional, Any
import logging
from .strategy import Strategy
import re

class StrategyRegistry:
    """
    Registry for maintaining all available strategy classes.
    Allows strategies to be registered and instantiated by name.
    """
    
    # Make this a class variable to hold registered strategies
    _strategies = {}
    _logger = logging.getLogger("strategies.strategy_registry")
    
    @classmethod
    def register(cls, strategy_class):
        """
        Register a strategy class with the registry.
        
        Args:
            strategy_class: Strategy class to register
            
        Returns:
            The registered strategy class (for decorator use)
        """
        # Register with class name
        strategy_type = strategy_class.__name__
        
        if strategy_type in cls._strategies:
            cls._logger.warning(f"Strategy type '{strategy_type}' already registered, overwriting")
            
        cls._strategies[strategy_type] = strategy_class
        cls._logger.info(f"Registered strategy type: {strategy_type}")
        
        # Also register with snake_case name
        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', strategy_type).lower()
        cls._strategies[snake_case] = strategy_class
        cls._logger.info(f"Registered strategy type (snake_case): {snake_case}")
        
        return strategy_class
    
    @classmethod
    def register_strategy(cls, strategy_type: str, strategy_class):
        """
        Register a strategy class with the registry by type name.
        
        Args:
            strategy_type: The strategy type name to register
            strategy_class: Strategy class to register
            
        Returns:
            The registered strategy class
        """
        if strategy_type in cls._strategies:
            cls._logger.warning(f"Strategy type '{strategy_type}' already registered, overwriting")
            
        cls._strategies[strategy_type] = strategy_class
        cls._logger.info(f"Registered strategy type: {strategy_type}")
        
        return strategy_class
    
    @classmethod
    def unregister(cls, strategy_type: str):
        """
        Unregister a strategy type from the registry.
        
        Args:
            strategy_type: Name of strategy type to unregister
            
        Returns:
            bool: True if strategy was unregistered, False if not found
        """
        if strategy_type in cls._strategies:
            del cls._strategies[strategy_type]
            cls._logger.info(f"Unregistered strategy type: {strategy_type}")
            return True
        else:
            cls._logger.warning(f"Strategy type '{strategy_type}' not found in registry")
            return False
    
    @classmethod
    def get_strategy(cls, strategy_type: str):
        """
        Get a strategy class by type name.
        
        Args:
            strategy_type: Name of strategy type to retrieve
            
        Returns:
            Strategy class if found, None otherwise
        """
        if strategy_type in cls._strategies:
            return cls._strategies[strategy_type]
        else:
            cls._logger.warning(f"Strategy type '{strategy_type}' not found in registry")
            return None
    
    @classmethod
    def list_strategies(cls):
        """
        Get a list of all registered strategy types.
        
        Returns:
            List of strategy type names
        """
        return list(cls._strategies.keys())
    
    @classmethod
    def count(cls):
        """
        Get the number of registered strategy types.
        
        Returns:
            Number of registered strategies
        """
        return len(cls._strategies)
    
    @classmethod
    def create_strategy(cls, strategy_type: str, config: Dict[str, Any], 
                       data_manager, portfolio, event_manager, instruments=None):
        """
        Create a new strategy instance of the specified type.
        
        Args:
            strategy_type: Type of strategy to create
            config: Configuration for the strategy
            data_manager: Data manager instance
            portfolio: Portfolio instance
            event_manager: Event manager instance
            instruments: List of instruments the strategy will trade

        Returns:
            Strategy instance if successful, None otherwise
        """
        try:
            strategy_class = cls.get_strategy(strategy_type)
            
            if not strategy_class:
                return None
                
            strategy = strategy_class(config, data_manager, portfolio, event_manager)
            cls._logger.info(f"Created strategy instance: {strategy.id} ({strategy_type})")
            
            return strategy
            
        except Exception as e:
            cls._logger.error(f"Error creating strategy instance of type {strategy_type}: {str(e)}")
            return None