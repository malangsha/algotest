"""
Event Logger module for efficient event monitoring and diagnostic information.
Integrates directly with EventManager to avoid subscription overhead.
"""
import logging
import time
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime
import traceback

from models.events import EventType

class EventLogger:
    """
    EventLogger provides efficient logging and statistics for the event system.
    It integrates directly with EventManager rather than subscribing to events,
    to minimize overhead and maximize performance.
    """
    
    def __init__(self, event_manager, log_interval: int = 10, 
                 log_level: int = logging.INFO, 
                 track_event_types: Optional[List[EventType]] = None):
        """
        Initialize the EventLogger.
        
        Args:
            event_manager: EventManager instance to monitor
            log_interval: Interval (in seconds) between periodic log summaries
            log_level: Logging level for routine logs
            track_event_types: Optional list of specific event types to track.
                               If None, tracks all event types.
        """
        self.logger = logging.getLogger("core.event_logger")
        self.event_manager = event_manager
        self.log_interval = log_interval
        self.log_level = log_level
        
        # Track event counts by type
        self.event_counts = {event_type.value: 0 for event_type in EventType}
        
        # If specific event types are requested, track only those
        self.track_event_types = set(track_event_types) if track_event_types else None
        
        # Statistics and diagnostic info
        self.stats = {
            "start_time": time.time(),
            "last_log_time": time.time(),
            "total_events": 0,
            "events_by_type": {},
            "event_rates": {},
            "queue_stats": {
                "max_size": 0,
                "overflow_count": 0,
                "avg_size": 0,
                "size_samples": []
            },
            "processing_stats": {
                "max_latency": 0,
                "avg_latency": 0,
                "latency_samples": []
            }
        }
        
        # Timestamps for latency tracking
        self.last_event_published_time = None
        self.last_event_processed_time = None
        
        # Register with EventManager to intercept events
        self._install_hooks()
        
        # Start logging thread
        self.running = True
        self.log_thread = threading.Thread(target=self._periodic_logging, daemon=True)
        self.log_thread.start()
        
        self.logger.info(f"EventLogger initialized with {log_interval}s logging interval")

    def _install_hooks(self):
        """
        Install hooks into the EventManager to intercept events without subscribing.
        This method patches the EventManager's publish and _dispatch_event methods
        to track events as they flow through the system.
        """
        # Store original methods
        original_publish = self.event_manager.publish
        original_dispatch = self.event_manager._dispatch_event
        
        # Define wrapper for publish method
        def publish_wrapper(event):
            # Track the event before it's published
            self._track_published_event(event)
            # Call the original method
            return original_publish(event)
        
        # Define wrapper for dispatch method
        def dispatch_wrapper(event):
            # Track the event as it's being processed
            self._track_processed_event(event)
            # Call the original method
            return original_dispatch(event)
        
        # Install the wrappers
        self.event_manager.publish = publish_wrapper
        self.event_manager._dispatch_event = dispatch_wrapper
        
        self.logger.info("Installed EventLogger hooks into EventManager")
    
    def _track_published_event(self, event):
        """
        Track an event being published.
        
        Args:
            event: The event being published
        """
        # Skip if we're only tracking specific event types and this isn't one of them
        if self.track_event_types is not None and event.event_type not in self.track_event_types:
            return
            
        # Record event type
        event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
        
        # Update counters
        self.event_counts[event_type] = self.event_counts.get(event_type, 0) + 1
        self.stats["total_events"] += 1
        
        # Update event type stats
        if event_type not in self.stats["events_by_type"]:
            self.stats["events_by_type"][event_type] = 0
        self.stats["events_by_type"][event_type] += 1
        
        # Record timestamp for latency tracking
        self.last_event_published_time = time.time()
        
        # Track queue stats
        queue_size = self.event_manager.event_queue.qsize()
        self.stats["queue_stats"]["size_samples"].append(queue_size)
        if len(self.stats["queue_stats"]["size_samples"]) > 100:
            self.stats["queue_stats"]["size_samples"].pop(0)
        
        self.stats["queue_stats"]["max_size"] = max(
            self.stats["queue_stats"]["max_size"], 
            queue_size
        )
        
        # Calculate average queue size
        if self.stats["queue_stats"]["size_samples"]:
            self.stats["queue_stats"]["avg_size"] = sum(self.stats["queue_stats"]["size_samples"]) / len(self.stats["queue_stats"]["size_samples"])
    
    def _track_processed_event(self, event):
        """
        Track an event being processed.
        
        Args:
            event: The event being processed
        """
        # Skip if we're only tracking specific event types and this isn't one of them
        if self.track_event_types is not None and event.event_type not in self.track_event_types:
            return
            
        # Record timestamp for latency tracking
        current_time = time.time()
        self.last_event_processed_time = current_time
        
        # Calculate processing latency if we have both timestamps
        if self.last_event_published_time:
            latency = current_time - self.last_event_published_time
            self.stats["processing_stats"]["latency_samples"].append(latency)
            if len(self.stats["processing_stats"]["latency_samples"]) > 100:
                self.stats["processing_stats"]["latency_samples"].pop(0)
            
            # Update max latency
            self.stats["processing_stats"]["max_latency"] = max(
                self.stats["processing_stats"]["max_latency"],
                latency
            )
            
            # Calculate average latency
            if self.stats["processing_stats"]["latency_samples"]:
                self.stats["processing_stats"]["avg_latency"] = sum(self.stats["processing_stats"]["latency_samples"]) / len(self.stats["processing_stats"]["latency_samples"])

    def _periodic_logging(self):
        """
        Thread function for periodic logging of event statistics.
        """
        while self.running:
            try:
                # Sleep until next logging interval
                time.sleep(self.log_interval)
                
                # Update statistics
                current_time = time.time()
                elapsed = current_time - self.stats["last_log_time"]
                
                # Calculate event rates
                for event_type, count in self.event_counts.items():
                    if event_type not in self.stats["event_rates"]:
                        self.stats["event_rates"][event_type] = {
                            "previous_count": 0,
                            "rate": 0
                        }
                    
                    # Get previous count
                    previous_count = self.stats["event_rates"][event_type]["previous_count"]
                    
                    # Calculate rate
                    rate = (count - previous_count) / elapsed if elapsed > 0 else 0
                    
                    # Update rate
                    self.stats["event_rates"][event_type]["rate"] = rate
                    
                    # Update previous count for next calculation
                    self.stats["event_rates"][event_type]["previous_count"] = count
                
                # Create log message
                self._log_statistics()
                
                # Update last log time
                self.stats["last_log_time"] = current_time
                
            except Exception as e:
                self.logger.error(f"Error in event logger periodic logging: {e}")
                self.logger.error(traceback.format_exc())
                # Continue despite errors

    def _log_statistics(self):
        """Log event statistics."""
        # Log overall event stats
        self.logger.log(self.log_level, f"Event statistics - Total events: {self.stats['total_events']}")
        
        # Log event counts by type (only non-zero counts)
        event_type_logs = []
        for event_type, count in sorted(self.event_counts.items()):
            if count > 0:
                rate = self.stats["event_rates"].get(event_type, {}).get("rate", 0)
                event_type_logs.append(f"{event_type}: {count} ({rate:.2f}/s)")
        
        if event_type_logs:
            self.logger.log(self.log_level, f"Events by type: {', '.join(event_type_logs)}")
        
        # Log queue stats
        queue_size = self.event_manager.event_queue.qsize()
        queue_capacity = self.event_manager.event_queue.maxsize
        queue_usage = (queue_size / queue_capacity) * 100 if queue_capacity > 0 else 0
        
        self.logger.log(
            self.log_level, 
            f"Queue: Current={queue_size}/{queue_capacity} ({queue_usage:.1f}%), "
            f"Max={self.stats['queue_stats']['max_size']}, "
            f"Avg={self.stats['queue_stats']['avg_size']:.1f}"
        )
        
        # Log processing latency
        if self.stats["processing_stats"]["latency_samples"]:
            self.logger.log(
                self.log_level,
                f"Processing latency: Avg={self.stats['processing_stats']['avg_latency']*1000:.2f}ms, "
                f"Max={self.stats['processing_stats']['max_latency']*1000:.2f}ms"
            )

    def log_event(self, event, level=None):
        """
        Log details of a specific event.
        
        Args:
            event: The event to log
            level: Optional logging level (defaults to the instance's log_level)
        """
        if level is None:
            level = self.log_level
            
        # Extract event details
        event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
        event_id = getattr(event, 'event_id', None)
        
        # Create attribute list
        attributes = []
        for attr in ['symbol', 'order_id', 'strategy_id', 'timestamp']:
            if hasattr(event, attr):
                value = getattr(event, attr)
                attributes.append(f"{attr}={value}")
        
        # Log event details
        attr_str = ", ".join(attributes) if attributes else "no attributes"
        self.logger.log(level, f"Event: {event_type} (ID: {event_id}) - {attr_str}")

    def get_stats(self, include_details=False):
        """
        Get event statistics.
        
        Args:
            include_details: Whether to include detailed statistics
            
        Returns:
            dict: Event statistics
        """
        stats = {
            "total_events": self.stats["total_events"],
            "events_by_type": {k: v for k, v in self.event_counts.items() if v > 0},
            "event_rates": {
                k: v["rate"] 
                for k, v in self.stats["event_rates"].items() 
                if v["rate"] > 0
            },
            "queue_stats": {
                "current_size": self.event_manager.event_queue.qsize(),
                "max_size": self.stats["queue_stats"]["max_size"],
                "avg_size": self.stats["queue_stats"]["avg_size"]
            },
            "processing_stats": {
                "avg_latency_ms": self.stats["processing_stats"]["avg_latency"] * 1000,
                "max_latency_ms": self.stats["processing_stats"]["max_latency"] * 1000
            },
            "running_time_seconds": time.time() - self.stats["start_time"]
        }
        
        # Include detailed stats if requested
        if include_details:
            stats["queue_stats"]["size_samples"] = self.stats["queue_stats"]["size_samples"]
            stats["processing_stats"]["latency_samples"] = [
                sample * 1000 for sample in self.stats["processing_stats"]["latency_samples"]
            ]
        
        return stats

    def get_event_count(self, event_type):
        """
        Get the count of events of a specific type.
        
        Args:
            event_type: The event type to get the count for
            
        Returns:
            int: Count of events of this type
        """
        event_type_key = event_type.value if hasattr(event_type, 'value') else str(event_type)
        return self.event_counts.get(event_type_key, 0)

    def stop(self):
        """Stop the event logger."""
        self.running = False
        if self.log_thread and self.log_thread.is_alive():
            self.log_thread.join(timeout=1.0)
        self.logger.info("EventLogger stopped") 