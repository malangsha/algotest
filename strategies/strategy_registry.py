"""
Strategy Registry Module

This module maintains a registry of all available strategy classes.
"""

import logging
from typing import Dict, Type, Optional
import importlib
import pkgutil
import inspect

from strategies.base_strategy import OptionStrategy

logger = logging.getLogger(__name__)

class StrategyRegistry:
    """Registry for all available strategy classes."""
    
    _strategies: Dict[str, Type[OptionStrategy]] = {}
    
    @classmethod
    def register(cls, strategy_type: str = None):
        """
        Register a strategy class with an optional type name.
        
        Args:
            strategy_type: Optional type name for the strategy. If not provided,
                         the class name will be used.
        """
        def decorator(strategy_class: Type[OptionStrategy]):
            type_name = strategy_type or strategy_class.__name__.lower()
            if type_name in cls._strategies:
                logger.warning(f"Strategy type {type_name} already registered. Overwriting.")
            cls._strategies[type_name] = strategy_class
            logger.info(f"Registered strategy type: {type_name}")
            return strategy_class
        return decorator
    
    @classmethod
    def get_strategy_class(cls, strategy_type: str) -> Optional[Type[OptionStrategy]]:
        """
        Get a strategy class by type name.

        Args:
            strategy_type: Type name of the strategy

        Returns:
            Strategy class if found, None otherwise
        """
        if strategy_type not in cls._strategies:
            # Attempt to import from either strategies or strategies.option_strategies
            for pkg in ("strategies", "strategies.option_strategies"):
                module_name = f"{pkg}.{strategy_type}"
                try:
                    importlib.import_module(module_name)
                    logger.info(f"Imported strategy module: {module_name}")
                    break
                except ImportError as e:                    
                    logger.debug(f"Could not import strategy module {module_name}: {e}")
            else:
                logger.error(f"Failed to import any module for strategy: {strategy_type}")
                return None

        return cls._strategies.get(strategy_type)   
    
    @classmethod
    def load_all_strategies(cls):
        """Load all strategy modules in the strategies package."""
        import strategies
        
        for _, name, _ in pkgutil.iter_modules(strategies.__path__):
            try:
                module = importlib.import_module(f"strategies.{name}")
                for _, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, OptionStrategy) and 
                        obj != OptionStrategy):
                        cls.register()(obj)
            except ImportError as e:
                logger.error(f"Failed to load strategy module {name}: {e}")
                
    @classmethod
    def get_all_strategy_types(cls) -> Dict[str, Type[OptionStrategy]]:
        """Get all registered strategy types."""
        return cls._strategies.copy()