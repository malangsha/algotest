from typing import Dict, Any, List, Optional, Union
import re
import logging
from utils.exceptions import ValidationError

logger = logging.getLogger(__name__)

class Validator:
    """Provides validation functions for various components of the framework."""
    
    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """
        Validate if a symbol follows the expected format for Indian markets.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Basic validation for NSE/BSE symbols
        # NSE symbols are generally uppercase alphanumeric with possibly some special chars
        if not symbol or not isinstance(symbol, str):
            return False
            
        # utils/validators.py (continued)
        # Check for valid length (usually between 2-20 characters)
        if len(symbol) < 2 or len(symbol) > 20:
            return False
            
        # Most common pattern for NSE symbols
        pattern = re.compile(r'^[A-Z0-9\.\-\_]+$')
        return bool(pattern.match(symbol))
    
    @staticmethod
    def validate_order(order: Dict[str, Any]) -> List[str]:
        """
        Validate order parameters.
        
        Args:
            order: Order dictionary with parameters
            
        Returns:
            List of validation errors, empty if valid
        """
        errors = []
        
        # Required fields
        required_fields = ['symbol', 'side', 'quantity', 'order_type']
        for field in required_fields:
            if field not in order:
                errors.append(f"Missing required field: {field}")
        
        # Field-specific validations
        if 'symbol' in order and not Validator.validate_symbol(order['symbol']):
            errors.append(f"Invalid symbol format: {order['symbol']}")
            
        if 'side' in order and order['side'] not in ['BUY', 'SELL']:
            errors.append(f"Invalid order side: {order['side']}")
            
        if 'quantity' in order:
            try:
                qty = float(order['quantity'])
                if qty <= 0:
                    errors.append(f"Quantity must be positive: {qty}")
            except (ValueError, TypeError):
                errors.append(f"Invalid quantity: {order['quantity']}")
                
        if 'order_type' in order and order['order_type'] not in ['MARKET', 'LIMIT', 'SL', 'SL-M']:
            errors.append(f"Invalid order type: {order['order_type']}")
            
        # Additional validations based on order type
        if order.get('order_type') == 'LIMIT' and 'price' not in order:
            errors.append("Price is required for LIMIT orders")
            
        if order.get('order_type') in ['SL', 'SL-M'] and 'trigger_price' not in order:
            errors.append("Trigger price is required for SL/SL-M orders")
            
        return errors
    
    @staticmethod
    def validate_config(config: Dict[str, Any]) -> List[str]:
        """
        Validate the main configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            List of validation errors, empty if valid
        """
        errors = []
        
        # Check required sections
        required_sections = ['system', 'market', 'broker', 'data', 'risk']
        for section in required_sections:
            if section not in config:
                errors.append(f"Missing required configuration section: {section}")
        
        # Validate system configuration
        if 'system' in config:
            system = config['system']
            if 'mode' in system and system['mode'] not in ['backtest', 'live', 'paper']:
                errors.append(f"Invalid system mode: {system['mode']}")
        
        # Validate market configuration
        if 'market' in config:
            market = config['market']
            if 'default_exchange' in market and market['default_exchange'] not in ['NSE', 'BSE']:
                errors.append(f"Invalid default exchange: {market['default_exchange']}")
        
        # Validate data configuration
        if 'data' in config:
            data = config['data']
            if 'resolution' in data and data['resolution'] not in ['1m', '5m', '15m', '30m', '1h', '1d']:
                errors.append(f"Invalid data resolution: {data['resolution']}")
        
        return errors
    
    @staticmethod
    def validate_strategy_config(strategy_config: Dict[str, Any]) -> List[str]:
        """
        Validate a strategy configuration.
        
        Args:
            strategy_config: Strategy configuration dictionary
            
        Returns:
            List of validation errors, empty if valid
        """
        errors = []
        
        # Required fields
        required_fields = ['name', 'universe', 'parameters', 'execution', 'risk', 'schedule']
        for field in required_fields:
            if field not in strategy_config:
                errors.append(f"Missing required field: {field}")
        
        # Validate universe configuration
        if 'universe' in strategy_config:
            universe = strategy_config['universe']
            if 'type' not in universe:
                errors.append("Universe type is required")
            elif universe['type'] not in ['index_components', 'custom', 'sector', 'all']:
                errors.append(f"Invalid universe type: {universe['type']}")
                
            if universe.get('type') == 'custom' and ('symbols' not in universe or not universe['symbols']):
                errors.append("Custom universe requires a list of symbols")
        
        # Validate execution configuration
        if 'execution' in strategy_config:
            execution = strategy_config['execution']
            if 'order_type' not in execution:
                errors.append("Execution order type is required")
            elif execution['order_type'] not in ['MARKET', 'LIMIT', 'SL', 'SL-M']:
                errors.append(f"Invalid execution order type: {execution['order_type']}")
        
        # Validate schedule configuration
        if 'schedule' in strategy_config:
            schedule = strategy_config['schedule']
            if 'frequency' not in schedule:
                errors.append("Schedule frequency is required")
            elif schedule['frequency'] not in ['1m', '5m', '15m', '30m', '1h', '1d']:
                errors.append(f"Invalid schedule frequency: {schedule['frequency']}")
        
        return errors