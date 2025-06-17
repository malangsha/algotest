import threading
import time
from typing import Optional, Dict, Set
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass

from core.logging_manager import get_logger

@dataclass
class OrderMapping:
    """Domain model representing an order mapping entry."""
    broker_order_id: str
    internal_id: str
    created_at: datetime
    
    def is_expired(self, ttl: timedelta) -> bool:
        """Check if this mapping has expired based on TTL."""
        return datetime.now() - self.created_at > ttl
    
    def age_hours(self) -> float:
        """Get the age of this mapping in hours."""
        return (datetime.now() - self.created_at).total_seconds() / 3600


class OrderMappingService:
    """
    Thread-safe singleton service for managing broker-to-internal order ID mappings.
    
    Designed for trading systems where broker order IDs need to be mapped to internal
    order tracking IDs. Supports automatic cleanup of expired mappings to prevent
    memory leaks in long-running trading applications.
    """
    _instance: Optional['OrderMappingService'] = None
    _creation_lock = threading.RLock()  # Use RLock for better reentrant behavior

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._creation_lock:
                if cls._instance is None:
                    cls._instance = super(OrderMappingService, cls).__new__(cls)
        return cls._instance

    def __init__(self, ttl_hours: int = 24):
        if not hasattr(self, '_initialized'):
            with self._creation_lock:
                if not hasattr(self, '_initialized'):
                    self._setup_service(ttl_hours)
                    self._initialized = True

    def _setup_service(self, ttl_hours: int):
        """Initialize the service components."""
        self.logger = get_logger("core.order_mapping_service")
        self._mappings: Dict[str, OrderMapping] = {}
        self._operation_lock = threading.RLock()  # Separate lock for operations
        self._ttl = timedelta(hours=ttl_hours)
        self._running = True
        self._cleanup_interval_seconds = min(3600, max(300, ttl_hours * 300))  # Adaptive cleanup interval
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._run_cleanup, 
            daemon=True,
            name="OrderMappingCleanup"
        )
        self._cleanup_thread.start()
        
        self.logger.info(f"OrderMappingService initialized with TTL={ttl_hours}h, cleanup_interval={self._cleanup_interval_seconds}s")

    @contextmanager
    def _synchronized(self):
        """Context manager for thread-safe operations."""
        with self._operation_lock:
            yield

    def add_mapping(self, broker_order_id: str, internal_id: str):
        """Adds a new, thread-safe mapping from a broker order ID to an internal order ID."""
        if not broker_order_id or not internal_id:
            raise ValueError("Both broker_order_id and internal_id must be non-empty strings")
        
        with self._synchronized():
            existing_mapping = self._mappings.get(broker_order_id)

            # only compare if there _is_ an existing mapping
            if existing_mapping and existing_mapping.internal_id == internal_id:
                self.logger.info(
                   f"mapping for internal_id: {internal_id}"
                   f"mapping for broker_order_id: {broker_order_id} already exists."
                )
                return
            
            # if there's a mapping but for a different internal_id, warn and overwrite
            if existing_mapping and existing_mapping.internal_id != internal_id:
                
                self.logger.warning(
                    f"Overwriting mapping for broker_order_id {broker_order_id}. "
                    f"Old: {existing_mapping.internal_id} (age: {existing_mapping.age_hours():.1f}h), "
                    f"New: {internal_id}"
                )
            
            mapping = OrderMapping(
                broker_order_id=broker_order_id,
                internal_id=internal_id,
                created_at=datetime.now()
            )
            
            self._mappings[broker_order_id] = mapping
            self.logger.debug(f"Added mapping: {broker_order_id} -> {internal_id}")

    def get_internal_id(self, broker_order_id: str) -> Optional[str]:
        """Retrieves the internal order ID for a given broker order ID, thread-safe."""
        if not broker_order_id:
            return None
            
        with self._synchronized():
            mapping = self._mappings.get(broker_order_id)
            if mapping is None:
                return None
            
            # Check if mapping has expired
            if mapping.is_expired(self._ttl):
                self.logger.debug(f"Removing expired mapping: {broker_order_id}")
                self._mappings.pop(broker_order_id, None)
                return None
            
            return mapping.internal_id

    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about current mappings (useful for monitoring)."""
        with self._synchronized():
            total_mappings = len(self._mappings)
            expired_count = sum(1 for mapping in self._mappings.values() 
                              if mapping.is_expired(self._ttl))
            return {
                'total_mappings': total_mappings,
                'active_mappings': total_mappings - expired_count,
                'expired_mappings': expired_count
            }

    def remove_mapping(self, broker_order_id: str) -> bool:
        """Remove a specific mapping. Returns True if mapping existed."""
        with self._synchronized():
            mapping = self._mappings.pop(broker_order_id, None)
            if mapping:
                self.logger.debug(f"Removed mapping: {broker_order_id} -> {mapping.internal_id}")
                return True
            return False

    def _run_cleanup(self):
        """Periodically runs to clean up expired mappings."""
        self.logger.info("Order mapping cleanup thread started.")
        
        while self._running:
            try:
                self._perform_cleanup()
                time.sleep(self._cleanup_interval_seconds)
            except Exception as e:
                self.logger.error(f"Error in order mapping cleanup thread: {e}", exc_info=True)
                # Continue running even if cleanup fails
                time.sleep(60)  # Short retry interval on error

    def _perform_cleanup(self):
        """Perform the actual cleanup of expired mappings."""
        with self._synchronized():
            if not self._mappings:
                return
            
            expired_keys = [
                broker_id for broker_id, mapping in self._mappings.items()
                if mapping.is_expired(self._ttl)
            ]
            
            if expired_keys:
                self.logger.info(f"Evicting {len(expired_keys)} expired order mappings out of {len(self._mappings)} total")
                for key in expired_keys:
                    self._mappings.pop(key, None)
            else:
                self.logger.debug(f"No expired mappings found. Current count: {len(self._mappings)}")

    def stop(self):
        """Stop the cleanup thread and service."""
        self.logger.info("Stopping OrderMappingService...")
        self._running = False
        
        # Wait for cleanup thread to finish
        if hasattr(self, '_cleanup_thread') and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5.0)
        
        self.logger.info("OrderMappingService stopped.")

    def __del__(self):
        """Cleanup when service is destroyed."""
        if hasattr(self, '_running'):
            self.stop()


# Singleton instance
order_mapping_service = OrderMappingService()