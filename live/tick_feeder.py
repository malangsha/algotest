"""
Tick Feeder Module for Indian Option Trading Framework

This module provides a mechanism to replay historical tick data from a file,
simulating a live market data feed for testing and development purposes.

Features:
- Reads and parses tick data from finvasia.ticks file
- Transforms raw ticks using FinVasiaFeed._convert_tick_to_market_data
- Maintains token ID to instrument definition mapping
- Dispatches market data events via FinVasiaFeed._on_market_data
- Supports configurable playback speed
- Handles error conditions gracefully
- Loops continuously through the tick data
"""

import os
import re
import time
import json
import threading
import logging
from typing import Dict, Optional, List, Any, Set
from datetime import datetime

from models.instrument import Instrument
from utils.constants import Exchange
from core.data_manager import DataManager
from live.market_data_feeds import FinvasiaFeed
from core.logging_manager import get_logger

class TickFeeder:
    """
    Reads historical tick data from a file and feeds it into the trading framework
    as if it were coming from a live market data feed.
    """
    
    def __init__(
        self, 
        tick_file_path: str,
        finvasia_feed: FinvasiaFeed,
        data_manager: DataManager,
        playback_speed: float = 1.0
    ):
        """
        Initialize the TickFeeder.
        
        Args:
            tick_file_path: Path to the finvasia.ticks file
            finvasia_feed: Instance of FinvasiaFeed to dispatch ticks to
            data_manager: DataManager instance for instrument lookups
            playback_speed: Playback speed multiplier (1.0 = real-time, 2.0 = 2x speed, etc.)
        """
        self.logger = get_logger("live.tick_feeder")
        self.tick_file_path = tick_file_path
        self.finvasia_feed = finvasia_feed
        self.data_manager = data_manager
        self.playback_speed = playback_speed
        
        # Validate tick file exists
        if not os.path.exists(tick_file_path):
            raise FileNotFoundError(f"Tick file not found: {tick_file_path}")
        
        # Internal state
        self.is_running = False
        self.feed_thread = None
        self._stop_event = threading.Event()
        
        # Token to instrument mapping
        self.token_to_instrument: Dict[str, Instrument] = {}
        
        # Statistics
        self.ticks_processed = 0
        self.ticks_skipped = 0
        self.errors_encountered = 0

        self.SYMBOL_MAPPINGS = {
            "NIFTY INDEX": "NIFTY",
            "Nifty 50":"NIFTY INDEX",
            "NIFTY": "NFITY",
            "NIFTY BANK": "BANKNIFTY",
            "MIDCPNIFTY": "MIDCAP",
            "FINNIFTY": "FINNIFTY",
            "SENSEX": "SENSEX",
            "BANKEX": "BANKEX"
        }       
        
        self.logger.info(f"TickFeeder initialized with file: {tick_file_path}, playback speed: {playback_speed}x")
    
    def start(self):
        """Start the tick feeder."""
        if self.is_running:
            self.logger.warning("TickFeeder is already running")
            return
        
        self._stop_event.clear()
        self.is_running = True
        self.feed_thread = threading.Thread(target=self._feed_loop)
        self.feed_thread.daemon = True
        self.feed_thread.start()
        self.logger.info("TickFeeder started")
    
    def stop(self):
        """Stop the tick feeder."""
        if not self.is_running:
            self.logger.warning("TickFeeder is not running")
            return
        
        self._stop_event.set()
        if self.feed_thread and self.feed_thread.is_alive():
            self.feed_thread.join(timeout=5.0)
        
        self.is_running = False
        self.logger.info("TickFeeder stopped")
        self.logger.info(f"Statistics: {self.ticks_processed} ticks processed, "
                         f"{self.ticks_skipped} skipped, {self.errors_encountered} errors")
    
    def _feed_loop(self):
        """Main loop that reads and feeds ticks from the file."""
        self.logger.info("Starting tick feed loop")
        
        while not self._stop_event.is_set():
            try:
                self._process_tick_file()
                
                # If we've reached the end of the file, loop back to the beginning
                if not self._stop_event.is_set():
                    self.logger.info("Reached end of tick file, looping back to beginning")
                    time.sleep(1.0)  # Brief pause before restarting
            except Exception as e:
                self.logger.error(f"Error in feed loop: {str(e)}", exc_info=True)
                self.errors_encountered += 1
                time.sleep(1.0)  # Pause before retrying
    
    def _process_tick_file(self):
        """Process the tick file line by line."""
        last_tick_time = None
        
        with open(self.tick_file_path, 'r') as file:
            for line_number, line in enumerate(file, 1):
                if self._stop_event.is_set():
                    break
                
                try:
                    # Parse the line to extract timestamp and tick data
                    timestamp_str, tick_data = self._parse_tick_line(line)
                    if not timestamp_str or not tick_data:
                        self.ticks_skipped += 1
                        continue
                    
                    # Parse the timestamp
                    current_tick_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                    
                    # Calculate delay between ticks for realistic timing
                    if last_tick_time:
                        # Calculate time difference in seconds
                        time_diff = (current_tick_time - last_tick_time).total_seconds()
                        # Apply playback speed
                        adjusted_delay = time_diff / self.playback_speed
                        
                        # Sleep for the adjusted delay
                        if adjusted_delay > 0:
                            time.sleep(adjusted_delay)
                    
                    # Process the tick
                    self._process_tick(tick_data)
                    
                    # Update last tick time
                    last_tick_time = current_tick_time
                    self.ticks_processed += 1
                    
                    # Periodically log progress
                    if self.ticks_processed % 1000 == 0:
                        self.logger.info(f"Processed {self.ticks_processed} ticks")
                
                except Exception as e:
                    self.logger.error(f"Error processing line {line_number}: {str(e)}", exc_info=True)
                    self.errors_encountered += 1
    
    def _parse_tick_line(self, line: str) -> tuple:
        """
        Parse a line from the tick file.
        
        Args:
            line: A line from the tick file
            
        Returns:
            tuple: (timestamp_str, tick_data_dict) or (None, None) if parsing fails
        """
        try:
            # Expected format: "2025-05-05 09:25:31,727 tick: {'t':'tk','e':'NSE','tk':'26000', ...}"
            match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) tick: (.+)', line.strip())
            if not match:
                return None, None
            
            timestamp_str = match.group(1)
            tick_data_str = match.group(2)
            
            # Parse the tick data string into a dictionary
            # The tick data is in the format of a Python dict literal
            tick_data = eval(tick_data_str)
            
            return timestamp_str, tick_data
        except Exception as e:
            self.logger.error(f"Error parsing tick line: {str(e)}", exc_info=True)
            return None, None
    
    def _process_tick(self, tick_data: Dict[str, Any]):
        """
        Process a single tick.
        
        Args:
            tick_data: Dictionary containing the tick data
        """
        try:
            # Get the token ID from the tick
            token = tick_data.get('tk')
            if not token:
                self.logger.warning("Tick missing token ID, skipping")
                self.ticks_skipped += 1
                return
            
            # Get the exchange from the tick
            exchange_str = tick_data.get('e')
            if not exchange_str:
                self.logger.warning(f"Tick for token {token} missing exchange, skipping")
                self.ticks_skipped += 1
                return
            
            # Convert exchange string to Exchange enum
            try:
                exchange = Exchange[exchange_str]
            except (KeyError, ValueError):
                self.logger.warning(f"Unknown exchange {exchange_str} for token {token}, skipping")
                self.ticks_skipped += 1
                return
            
            # Get the symbol from the tick
            symbol = tick_data.get('ts')
            if not symbol:
                self.logger.warning(f"Tick for token {token} missing symbol, skipping")
                self.ticks_skipped += 1
                return
            
            symbol = self.SYMBOL_MAPPINGS.get(symbol, symbol)
            # Get or create instrument for this token
            instrument = self._get_instrument_for_token(token, symbol, exchange)
            if not instrument:
                self.logger.warning(f"Could not get/create instrument for {exchange_str}:{symbol} (token {token}), skipping")
                self.ticks_skipped += 1
                return
            
            # Convert the tick to market data format
            market_data = self.finvasia_feed._convert_tick_to_market_data(tick_data)
            if not market_data:
                self.logger.debug(f"Tick conversion returned None for {exchange_str}:{symbol} (token {token}), skipping")
                self.ticks_skipped += 1
                return
            
            # Dispatch the market data to the feed
            self.finvasia_feed._on_market_data([tick_data])
            
        except Exception as e:
            self.logger.error(f"Error processing tick: {str(e)}", exc_info=True)
            self.errors_encountered += 1
    
    def _get_instrument_for_token(self, token: str, symbol: str, exchange: Exchange) -> Optional[Instrument]:
        """
        Get or create an Instrument object for a token.
        
        Args:
            token: Token ID
            symbol: Symbol name
            exchange: Exchange enum
            
        Returns:
            Instrument object or None if it couldn't be created
        """
        # Check if we already have this instrument
        if token in self.token_to_instrument:
            return self.token_to_instrument[token]
         
        # Try to find the instrument in the data manager
        symbol_key = f"{exchange.value}:{symbol}"
        for inst_key, instrument in self.data_manager.instruments.items():
            if inst_key == symbol_key:
                self.token_to_instrument[token] = instrument
                return instrument
        
        # If we don't have the instrument, check if it's in the FinvasiaFeed's lookup
        instrument = self.finvasia_feed.get_instrument(symbol, exchange)
        if instrument:
            self.token_to_instrument[token] = instrument
            return instrument
        
        # If we still don't have it, we need to add it to the FinvasiaFeed
        # This would typically involve creating a new Instrument object and adding it
        # For now, we'll log a warning and return None
        self.logger.warning(f"Instrument not found for {exchange.value}:{symbol} (token {token})")
        return None
    
    def set_playback_speed(self, speed: float):
        """
        Set the playback speed.
        
        Args:
            speed: Playback speed multiplier (1.0 = real-time, 2.0 = 2x speed, etc.)
        """
        if speed <= 0:
            self.logger.warning(f"Invalid playback speed: {speed}, must be positive")
            return
        
        self.playback_speed = speed
        self.logger.info(f"Playback speed set to {speed}x")
