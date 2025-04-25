import logging
import os
import threading
import time
import json
import requests
import zipfile
import csv
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from collections import defaultdict
from utils.constants import Exchange

# Constants
CACHE_DIR = Path("symbol_cache")
CACHE_FILE = CACHE_DIR / "symbol_cache.json"
CACHE_METADATA_FILE = CACHE_DIR / "cache_metadata.json"
MASTERS_URL_ROOT = 'https://api.shoonya.com/'
MASTER_FILES = [
    'NSE_symbols.txt.zip',  # NSE Equity
    'NFO_symbols.txt.zip',  # NSE F&O
    'BFO_symbols.txt.zip'   # BSE F&O
]

# Exchange codes for filtering
REQUIRED_EXCHANGES = {
    'NSE',  # NSE Equity
    'NFO',  # NSE F&O
    'BFO'   # BSE F&O
}

@dataclass
class CacheStats:
    """Statistics for symbol cache."""
    total_symbols: int = 0
    symbols_by_exchange: Dict[str, int] = None
    last_refresh_time: datetime = None
    refresh_count: int = 0
    error_count: int = 0
    cache_size_bytes: int = 0
    
    def __post_init__(self):
        if self.symbols_by_exchange is None:
            self.symbols_by_exchange = defaultdict(int)

class SymbolCache:
    """
    Singleton class for managing Finvasia symbol cache with automatic daily refresh.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize the symbol cache and start background refresh thread."""
        self.logger = logging.getLogger("SymbolCache")
        self.cache_dir = CACHE_DIR
        self.cache_file = CACHE_FILE
        self.cache_metadata_file = CACHE_METADATA_FILE
        self.symbol_cache = {}
        self.last_refresh_date = None
        self._refresh_lock = threading.Lock()
        self._is_running = True
        self.stats = CacheStats()
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing cache
        self._load_cache()
        
        # Start background refresh thread
        self._refresh_thread = threading.Thread(
            target=self._background_refresh,
            daemon=True,
            name="SymbolCacheRefresh"
        )
        self._refresh_thread.start()
        
        self.logger.info("SymbolCache initialized")
    
    def _background_refresh(self):
        """Background thread to check and refresh cache daily."""
        while self._is_running:
            try:
                current_date = date.today()
                
                # Check if we need to refresh
                if self.last_refresh_date is None or self.last_refresh_date < current_date:
                    self.logger.info("Daily cache refresh triggered")
                    self._refresh_cache()
                
                # Sleep until next day
                next_day = datetime.combine(
                    current_date + timedelta(days=1),
                    datetime.min.time()
                )
                sleep_seconds = (next_day - datetime.now()).total_seconds()
                time.sleep(sleep_seconds)
                
            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error in background refresh thread: {e}")
                time.sleep(3600)  # Sleep for an hour if error occurs
    
    def _refresh_cache(self):
        """Refresh the symbol cache by downloading and processing master files."""
        with self._refresh_lock:
            try:
                # Create temporary directory for downloads
                temp_dir = self.cache_dir / f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                temp_dir.mkdir(parents=True, exist_ok=True)
                
                try:
                    # Download and extract master files
                    extracted_files = self._download_and_extract_masters(temp_dir)
                    if not extracted_files:
                        self.logger.error("No master files were extracted")
                        return
                    
                    # Load symbols into cache
                    new_cache = self._load_symbols_to_cache(extracted_files)
                    if not new_cache:
                        self.logger.error("Failed to load symbols into cache")
                        return
                    
                    # Validate new cache
                    if not self._validate_cache(new_cache):
                        self.logger.error("Cache validation failed")
                        return
                    
                    # Save new cache
                    self._save_cache(new_cache)
                    self.symbol_cache = new_cache
                    self.last_refresh_date = date.today()
                    
                    # Update statistics
                    self._update_stats(new_cache)
                    
                    self.logger.info(f"Cache refreshed successfully with {len(new_cache)} symbols")
                    
                finally:
                    # Clean up temporary directory
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)
                        
            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error refreshing cache: {e}")
    
    def _validate_cache(self, cache: Dict[str, Dict[str, Any]]) -> bool:
        """Validate the cache data."""
        if not cache:
            return False
            
        # Check for required fields
        for key, value in cache.items():
            if not all(k in value for k in ['exch', 'token', 'tsym']):
                self.logger.warning(f"Invalid cache entry: {key}")
                return False
                
        return True
    
    def _update_stats(self, cache: Dict[str, Dict[str, Any]]):
        """Update cache statistics."""
        self.stats.total_symbols = len(cache)
        self.stats.symbols_by_exchange.clear()
        
        for key, value in cache.items():
            self.stats.symbols_by_exchange[value['exch']] += 1
            
        self.stats.last_refresh_time = datetime.now()
        self.stats.refresh_count += 1
        self.stats.cache_size_bytes = os.path.getsize(self.cache_file) if self.cache_file.exists() else 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current cache statistics."""
        return {
            'total_symbols': self.stats.total_symbols,
            'symbols_by_exchange': dict(self.stats.symbols_by_exchange),
            'last_refresh_time': self.stats.last_refresh_time.isoformat() if self.stats.last_refresh_time else None,
            'refresh_count': self.stats.refresh_count,
            'error_count': self.stats.error_count,
            'cache_size_bytes': self.stats.cache_size_bytes
        }
    
    def cleanup_cache(self, max_age_days: int = 30):
        """Clean up old cache files."""
        try:
            current_time = datetime.now()
            for file in self.cache_dir.glob('*'):
                if file.is_file():
                    file_age = current_time - datetime.fromtimestamp(file.stat().st_mtime)
                    if file_age.days > max_age_days:
                        self.logger.info(f"Removing old cache file: {file}")
                        file.unlink()
        except Exception as e:
            self.logger.error(f"Error cleaning up cache: {e}")
    
    def _download_and_extract_masters(self, target_dir: Path) -> List[Path]:
        """Download and extract master symbol files."""
        extracted_files = []
        
        for zip_file_name in MASTER_FILES:
            self.logger.info(f"Downloading {zip_file_name}...")
            url = MASTERS_URL_ROOT + zip_file_name
            zip_file_path = target_dir / zip_file_name
            
            try:
                # Download with timeout and streaming
                r = requests.get(url, allow_redirects=True, timeout=120, stream=True)
                r.raise_for_status()
                
                with open(zip_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                # Extract contents
                with zipfile.ZipFile(zip_file_path) as z:
                    for member in z.namelist():
                        if member.startswith('/') or '..' in member:
                            self.logger.warning(f"Skipping potentially unsafe path: {member}")
                            continue
                        member_path = target_dir / Path(member).name
                        z.extract(member, path=target_dir)
                        extracted_files.append(member_path)
                
            except Exception as e:
                self.logger.error(f"Error processing {zip_file_name}: {e}")
            finally:
                # Clean up zip file
                if zip_file_path.exists():
                    try:
                        os.remove(zip_file_path)
                    except Exception as e:
                        self.logger.error(f"Error removing zip file: {e}")
        
        return extracted_files
    
    def _load_symbols_to_cache(self, file_paths: List[Path]) -> Dict[str, Dict[str, Any]]:
        """Load symbols from extracted files into cache dictionary."""
        symbol_cache = {}
        
        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    
                    # Find column indices
                    col_indices = {}
                    for i, col in enumerate(header):
                        col = col.strip().lower()
                        if col in ['exchange', 'exch']:
                            col_indices['exchange'] = i
                        elif col in ['token', 'instrumenttoken']:
                            col_indices['token'] = i
                        elif col in ['tradingsymbol', 'tsym']:
                            col_indices['tradingsymbol'] = i
                    
                    # Process rows
                    for row in reader:
                        if len(row) < len(header):
                            continue
                            
                        try:
                            exch = row[col_indices['exchange']].strip().upper()
                            
                            # Skip if exchange is not in required list
                            if exch not in REQUIRED_EXCHANGES:
                                continue
                                
                            token = row[col_indices['token']].strip()
                            tsym = row[col_indices['tradingsymbol']].strip()
                            
                            if not all([exch, token, tsym]):
                                continue
                                
                            # Create cache entry
                            cache_key = f"{exch}:{tsym.upper()}"
                            symbol_cache[cache_key] = {
                                'exch': exch,
                                'token': token,
                                'tsym': tsym
                            }
                            
                        except (IndexError, ValueError) as e:
                            self.logger.warning(f"Error processing row: {e}")
                            
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")
        
        return symbol_cache
    
    def _load_cache(self):
        """Load existing cache from file."""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.symbol_cache = json.load(f)
                    
                if self.cache_metadata_file.exists():
                    with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        self.last_refresh_date = date.fromisoformat(metadata.get('last_refresh_date', ''))
                
                self.logger.info(f"Loaded cache with {len(self.symbol_cache)} symbols")
                
            else:
                self.logger.info("No existing cache found, will create new one")
                self._refresh_cache()
                
        except Exception as e:
            self.logger.error(f"Error loading cache: {e}")
            self._refresh_cache()
    
    def _save_cache(self, cache_data: Dict[str, Dict[str, Any]]):
        """Save cache to file."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False)
                
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_refresh_date': date.today().isoformat()
                }, f)
                
        except Exception as e:
            self.logger.error(f"Error saving cache: {e}")
    
    def lookup_symbol(self, symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
        """
        Look up a symbol **in the local cache only**.

        Args:
            symbol: The trading symbol (e.g., "NIFTY25APR25FUT", "RELIANCE", "NIFTY INDEX"). Case-insensitive.
            exchange: The exchange code (e.g., "NFO", "NSE"). Case-insensitive.

        Returns:
            A dictionary containing the cached scrip details if found, otherwise None.
            Includes standard keys like 'exch', 'token', 'tsym'.
        """
        if not symbol or not exchange:
            self.logger.warning("Cache lookup requires both symbol and exchange")
            return None

        if not self.symbol_cache:
             self.logger.warning("Symbol cache is not loaded. Cannot perform cache lookup.")
             return None

        # Construct the cache key using uppercase for consistency
        cache_key = f"{exchange.upper()}:{symbol.upper()}"
        self.logger.debug(f"Looking up symbol in cache: {cache_key}")

        cached_info = self.symbol_cache.get(cache_key)

        if cached_info:
            # Ensure the format matches expectations (standardized keys)
            # The load_symbols_to_cache function should already ensure this
            self.logger.info(f"Found '{symbol}' on '{exchange}' in local cache.")
            return cached_info.copy() # Return a copy
        else:
            # Log at debug level as this is expected for API fallback sometimes
            self.logger.debug(f"'{symbol}' on '{exchange}' not found in local cache.")
            return None
    
    def format_expiry_for_symbol(self, expiry_date_str: str) -> Optional[str]:
        """Format expiry date string for symbol construction."""
        if not isinstance(expiry_date_str, str):
            return None
            
        expiry_date_str = expiry_date_str.upper()
        try:
            if len(expiry_date_str) == 9:  # DDMMMYYYY
                dt = datetime.strptime(expiry_date_str, "%d%b%Y")
                return dt.strftime("%d%b%y").upper()
            elif len(expiry_date_str) == 7:  # DDMMMYY
                datetime.strptime(expiry_date_str, "%d%b%y")
                return expiry_date_str
            else:
                return None
        except ValueError:
            return None
    
    def construct_trading_symbol(
        self,
        name: str,
        expiry_date_str: str,
        instrument_type: str,
        option_type: Optional[str] = None,
        strike: Optional[Union[int, float, str]] = None
    ) -> Optional[str]:
        """Construct trading symbol based on Finvasia's naming convention."""
        name = name.upper()
        instrument_type = instrument_type.upper()
        formatted_expiry = self.format_expiry_for_symbol(expiry_date_str)
        
        if not formatted_expiry:
            return None
            
        if instrument_type == 'FUT':
            return f"{name}{formatted_expiry}F"
            
        elif instrument_type == 'OPT':
            if not option_type or strike is None:
                return None
                
            option_type = option_type.upper()
            if option_type not in ['P', 'C']:
                return None
                
            try:
                strike_float = float(strike)
                if strike_float == int(strike_float):
                    strike_str = str(int(strike_float))
                else:
                    strike_str = str(strike_float)
            except (ValueError, TypeError):
                return None
                
            return f"{name}{formatted_expiry}{option_type}{strike_str}"
            
        else:
            return None
    
    def stop(self):
        """Stop the background refresh thread."""
        self._is_running = False
        if self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5.0) 