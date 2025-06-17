
import threading
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from models.events import BufferWindow 
from core.logging_manager import get_logger
from utils.constants import BufferWindowState

class BufferWindowManager:
    """Manages buffer windows for all orders with broker ID tracking"""
    
    def __init__(self, default_timeout: timedelta = timedelta(seconds=30)):
        self._windows: Dict[str, BufferWindow] = {}        
        self._expected_broker_ids: Dict[str, str] = {}  # broker_id -> internal_id
        self._lock = threading.RLock()
        self._default_timeout = default_timeout
        self._logger = get_logger(__name__)
    
    def start_buffer_window(self, internal_id: str) -> BufferWindow:
        """Start buffer window for an order"""
        with self._lock:
            if internal_id in self._windows:
                self._logger.warning(f"Buffer window already exists for {internal_id}")
                return self._windows[internal_id]
            
            window = BufferWindow(
                internal_id=internal_id,
                state=BufferWindowState.ACTIVE,
                start_time=datetime.now(),
                timeout_duration=self._default_timeout
            )
            self._windows[internal_id] = window
            self._logger.info(f"Started buffer window for {internal_id}")
            return window
    
    def register_expected_broker_id(self, internal_id: str, broker_order_id: str) -> None:
        """Register which broker_order_id is expected for this internal_id"""
        with self._lock:
            self._expected_broker_ids[broker_order_id] = internal_id
            self._logger.debug(f"Registered expected broker_id {broker_order_id} for {internal_id}")
    
    def find_window_for_broker_id(self, broker_order_id: str) -> Optional[str]:
        """Find which window is expecting this broker_order_id"""
        with self._lock:
            return self._expected_broker_ids.get(broker_order_id)
    
    def get_window(self, internal_id: str) -> Optional[BufferWindow]:
        """Get buffer window for an order"""
        with self._lock:
            return self._windows.get(internal_id)
    
    def close_window(self, internal_id: str) -> Optional[BufferWindow]:
        """Close buffer window and return it"""
        with self._lock:
            window = self._windows.get(internal_id)
            if window:
                window.state = BufferWindowState.CLOSED
                window.end_time = datetime.now()
                self._logger.info(f"Closed buffer window for {internal_id}")
            return window
    
    def cleanup_window(self, internal_id: str) -> None:
        """Remove buffer window completely"""
        with self._lock:
            if internal_id in self._windows:
                del self._windows[internal_id]
                # Clean up broker_id mappings
                broker_ids_to_remove = [
                    bid for bid, iid in self._expected_broker_ids.items() 
                    if iid == internal_id
                ]
                for bid in broker_ids_to_remove:
                    del self._expected_broker_ids[bid]
                self._logger.debug(f"Cleaned up buffer window for {internal_id}")
    
    def check_expired_windows(self) -> List[str]:
        """Check for expired windows and mark them"""
        expired_orders = []
        with self._lock:
            for internal_id, window in self._windows.items():
                if window.is_expired() and window.state == BufferWindowState.ACTIVE:
                    window.state = BufferWindowState.EXPIRED
                    expired_orders.append(internal_id)
                    self._logger.warning(f"Buffer window expired for {internal_id}")
        return expired_orders
    
    def get_active_windows(self) -> List[str]:
        """Get list of orders with active buffer windows"""
        with self._lock:
            return [internal_id for internal_id, window in self._windows.items() 
                   if window.state == BufferWindowState.ACTIVE]
