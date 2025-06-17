"""
Event utilities for handling unmapped execution events and validation.
"""
import re
import uuid
from typing import Optional
from models.events import ExecutionEvent

class EventUtils:
    """Utility functions for event processing and validation."""
    
    UNMAPPED_PREFIX = "UNMAPPED_"
    
    @staticmethod
    def is_unmapped_event(event: ExecutionEvent) -> bool:
        """Check if an execution event has an unmapped placeholder order ID."""
        return (
            event.order_id is not None and 
            isinstance(event.order_id, str) and 
            event.order_id.startswith(EventUtils.UNMAPPED_PREFIX)
        )
    
    @staticmethod
    def is_unmapped_orderid(order_id: str) -> bool:
        """Check if an execution event has an unmapped placeholder order ID."""        
        return (
            order_id is not None and 
            isinstance(order_id, str) and 
            order_id.startswith(EventUtils.UNMAPPED_PREFIX)
        )
    
    @staticmethod
    def create_unmapped_placeholder_id(broker_order_id: Optional[str] = None) -> str:
        """Create a placeholder order ID for unmapped events."""
        # if no broker_order_id provided, generate a fresh UUID4
        if not broker_order_id:
            broker_order_id = str(uuid.uuid4())
        return f"{EventUtils.UNMAPPED_PREFIX}{broker_order_id}"

    @staticmethod
    def extract_broker_id_from_placeholder(placeholder_id: str) -> Optional[str]:
        """Extract broker order ID from an unmapped placeholder ID."""
        if not placeholder_id or not placeholder_id.startswith(EventUtils.UNMAPPED_PREFIX):
            return None
        return placeholder_id[len(EventUtils.UNMAPPED_PREFIX):]
    
    @staticmethod
    def validate_broker_order_id(broker_order_id: str) -> bool:
        """Validate broker order ID format (basic validation)."""
        if not broker_order_id or not isinstance(broker_order_id, str):
            return False
        
        # Basic validation - should be alphanumeric and reasonable length
        if len(broker_order_id) < 5 or len(broker_order_id) > 50:
            return False
            
        # Should contain at least some alphanumeric characters
        if not re.search(r'[a-zA-Z0-9]', broker_order_id):
            return False
            
        return True
   
    @staticmethod
    def sanitize_event_for_logging(event: ExecutionEvent) -> dict:
        """Create a sanitized version of execution event for logging purposes."""
        return {
            'order_id': event.order_id,
            'broker_order_id': event.broker_order_id,
            'symbol': event.symbol,
            'exchange': event.exchange,
            'side': event.side.name if hasattr(event.side, 'name') else str(event.side),
            'status': event.status.name if hasattr(event.status, 'name') else str(event.status),
            'quantity': event.quantity,
            'filled_quantity': event.filled_quantity,
            'timestamp': event.timestamp,
            'is_unmapped': EventUtils.is_unmapped_event(event)
        }
