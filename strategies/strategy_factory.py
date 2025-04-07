import importlib
import logging
from typing import Dict, Any, Type, List

from models.instrument import Instrument
from utils.config_loader import ConfigLoader
from .strategy import Strategy
from .strategy_registry import StrategyRegistry

class StrategyFactory:
    """Factory class for creating strategy instances."""

    @staticmethod
    def create_strategy(
        strategy_type: str,
        strategy_id: str = None,        
        data_manager=None,
        portfolio_manager=None,
        event_manager=None,
        config: Dict = None,
        instruments: List = None,
        **kwargs
    ) -> Strategy:
        """
        Create a strategy instance.

        Args:
            strategy_type: Type of strategy to create
            strategy_id: Unique identifier for the strategy instance
            data_manager: Data manager instance
            portfolio_manager: Portfolio manager instance
            event_manager: Event manager instance
            config: Full strategy configuration dictionary
            instruments: Instruments the strategy will trade
            **kwargs: Additional parameters

        Returns:
            Strategy: Instantiated strategy

        Raises:
            ValueError: If strategy_type is not registered
        """
        logger = logging.getLogger("strategies.strategy_factory")

        logger.info(f"received configuration: {config}")

        # Get strategy parameters from config
        parameters = config.get('parameters', {}) if config else {}
        name = config.get('name', strategy_id) if config else strategy_id

        # Check if strategy type is registered
        strategy_class = StrategyRegistry.get_strategy(strategy_type)

        if strategy_class is None:
            # Try to dynamically import the strategy module
            try:
                module_name = f"strategies.{strategy_type.lower()}"

                # Convert snake_case to PascalCase
                class_name = ''.join(word.capitalize() for word in strategy_type.split('_'))

                logger.info(f"Attempting to load strategy: {strategy_type}")
                logger.info(f"Module: {module_name}, Class: {class_name}")

                # Import the module
                module = importlib.import_module(module_name)

                if hasattr(module, class_name):
                    strategy_class = getattr(module, class_name)
                    # create the strategy instance
                    strategy = StrategyRegistry.create_strategy(strategy_type, config, data_manager, portfolio_manager, event_manager, instruments)

                    # Register the strategy
                    StrategyRegistry.register_strategy(strategy_type, strategy_class)
                    logger.info(f"Successfully loaded {class_name} from {module_name}")
                else:
                    raise AttributeError(f"Class {class_name} not found in module {module_name}")

            except ImportError as e:
                logger.error(f"Failed to import module '{module_name}': {str(e)}")
                raise ValueError(f"Strategy module not found: {strategy_type}")
            except Exception as e:
                logger.error(f"Failed to dynamically load strategy '{strategy_type}': {str(e)}")
                raise ValueError(f"Error loading strategy: {strategy_type} - {str(e)}")

        logger.info(f"Creating strategy instance: {name} of type {strategy_type}")
        

        # Create the strategy instance with all available parameters
        # Ensure arguments match the expected __init__ signature of the strategy class
        # Common signature: (self, config, data_manager, portfolio, event_manager)
        # We pass the full config dict, data_manager, portfolio_manager (assuming it's the portfolio), event_manager
        # The strategy itself should extract parameters, instruments etc. from the config dict.
        return strategy_class( 
            config=config, # Pass the full strategy config dict
            data_manager=data_manager, 
            portfolio=portfolio_manager, # Assuming portfolio_manager is the Portfolio instance
            event_manager=event_manager
            # Note: Instruments list is not passed directly, strategy should get it from config or engine
        )

    @staticmethod
    def create_strategies_from_config(config_path: str = "config/strategies.yaml") -> List[Strategy]:
        """
        Create multiple strategies from a configuration file.

        Args:
            config_path: Path to the strategies configuration file

        Returns:
            List[Strategy]: List of instantiated strategies
        """
        logger = logging.getLogger("strategies.strategy_factory")
        logger.info(f"Loading strategies from {config_path}")

        # Load configuration
        config_loader = ConfigLoader()
        config = config_loader.load_config(config_path)

        if 'strategies' not in config:
            raise ValueError(f"No 'strategies' section found in {config_path}")

        if 'instruments' not in config:
            raise ValueError(f"No 'instruments' section found in {config_path}")

        # Create instrument dictionary
        instruments_dict = {}
        for instr_config in config['instruments']:
            instrument = Instrument(
                id=instr_config['id'],
                symbol=instr_config['symbol'],
                exchange=instr_config.get('exchange', ''),
                asset_type=instr_config.get('asset_type', ''),
                currency=instr_config.get('currency', ''),
                tick_size=instr_config.get('tick_size', 0.01),
                lot_size=instr_config.get('lot_size', 1.0),
                margin_requirement=instr_config.get('margin_requirement', 1.0),
                is_tradable=instr_config.get('is_tradable', True),
                expiry_date=instr_config.get('expiry_date'),
                additional_info=instr_config.get('additional_info', {})
            )
            instruments_dict[instr_config['id']] = instrument

        # Create strategies
        strategies = []
        for strategy_config in config['strategies']:
            try:
                strategy = StrategyFactory.create_strategy_from_config(
                    strategy_config=strategy_config,
                    instruments_dict=instruments_dict
                )
                strategies.append(strategy)
            except Exception as e:
                logger.error(f"Failed to create strategy {strategy_config.get('name', 'unknown')}: {str(e)}")
                # Continue with other strategies instead of failing completely
                continue

        logger.info(f"Created {len(strategies)} strategies from configuration")
        return strategies

    @staticmethod
    def create_strategy_from_config(strategy_config: Dict, instruments_dict: Dict) -> Strategy:
        """
        Create a strategy from configuration.

        Args:
            strategy_config: Strategy configuration dictionary
            instruments_dict: Dictionary of available instruments

        Returns:
            Strategy: Instantiated strategy
        """
        logger = logging.getLogger("strategies.strategy_factory")

        strategy_type = strategy_config.get('type')
        strategy_id = strategy_config.get('id')

        if not strategy_type:
            raise ValueError(f"Strategy type not specified for {strategy_id}")

        # Get instruments for this strategy
        instruments = []
        instrument_ids = strategy_config.get('instruments', [])
        for instr_id in instrument_ids:
            if instr_id in instruments_dict:
                instruments.append(instruments_dict[instr_id])
            else:
                logger.warning(f"Instrument {instr_id} not found for strategy {strategy_id}")

        # Create strategy instance
        return StrategyFactory.create_strategy(
            strategy_type=strategy_type,
            strategy_id=strategy_id,
            config=strategy_config,
            instruments=instruments
        )
