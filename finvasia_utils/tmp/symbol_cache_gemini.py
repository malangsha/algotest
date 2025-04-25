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

# Assuming utils.constants.Exchange exists and provides exchange names
# from utils.constants import Exchange

# Constants
BASE_CACHE_DIR = Path("symbol_cache")
CACHE_FILE_TEMPLATE = "{exchange}_symbol_cache.json"
CACHE_METADATA_FILE_TEMPLATE = "{exchange}_cache_metadata.json"
EXPIRY_DATES_FILE_TEMPLATE = "{exchange}_expiry_dates.json"
MASTERS_URL_ROOT = 'https://api.shoonya.com/'

# Mapping of master files to exchanges
MASTER_FILES_MAP = {
    'NSE': 'NSE_symbols.txt.zip',  # NSE Equity
    'BSE': 'BSE_symbols.txt.zip',  # NSE Equity
    'NFO': 'NFO_symbols.txt.zip',  # NSE F&O
    'BFO': 'BFO_symbols.txt.zip'   # BSE F&O
}

# Exchange codes to process
REQUIRED_EXCHANGES = {
    'NSE',  # NSE Equity
    'BSE',  # BSE Equity
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
    Maintains exchange-specific caches and expiry dates.
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
        self.base_cache_dir = BASE_CACHE_DIR
        # Use dictionaries to hold exchange-specific data
        self.symbol_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.expiry_dates: Dict[str, Dict[str, Any]] = {}
        self.last_refresh_date: Dict[str, Optional[date]] = {} # Track last refresh per exchange
        self._refresh_lock = threading.Lock() # Still need a global lock for refresh process
        self._is_running = True
        self.stats = CacheStats() # Stats are still global

        # Ensure base cache directory exists
        self.base_cache_dir.mkdir(parents=True, exist_ok=True)

        # Load existing caches and expiry dates for all required exchanges
        for exchange in REQUIRED_EXCHANGES:
            self._load_cache(exchange)
            self._load_expiry_dates(exchange)

        # Start background refresh thread
        self._refresh_thread = threading.Thread(
            target=self._background_refresh,
            daemon=True,
            name="SymbolCacheRefresh"
        )
        self._refresh_thread.start()

        self.logger.info("SymbolCache initialized")

    def _get_cache_file_path(self, exchange: str) -> Path:
        """Get the path for the exchange-specific symbol cache file."""
        return self.base_cache_dir / CACHE_FILE_TEMPLATE.format(exchange=exchange.upper())

    def _get_cache_metadata_file_path(self, exchange: str) -> Path:
        """Get the path for the exchange-specific cache metadata file."""
        return self.base_cache_dir / CACHE_METADATA_FILE_TEMPLATE.format(exchange=exchange.upper())

    def _get_expiry_dates_file_path(self, exchange: str) -> Path:
        """Get the path for the exchange-specific expiry dates file."""
        return self.base_cache_dir / EXPIRY_DATES_FILE_TEMPLATE.format(exchange=exchange.upper())

    def _background_refresh(self):
        """Background thread to check and refresh cache daily for all exchanges."""
        while self._is_running:
            try:
                current_date = date.today()

                for exchange in REQUIRED_EXCHANGES:
                    last_refresh = self.last_refresh_date.get(exchange)

                    # Check if we need to refresh for this exchange
                    if last_refresh is None or last_refresh < current_date:
                        self.logger.info(f"Daily cache refresh triggered for {exchange}")
                        self._refresh_cache(exchange)

                # Sleep until next day
                next_day = datetime.combine(
                    current_date + timedelta(days=1),
                    datetime.min.time()
                )
                sleep_seconds = (next_day - datetime.now()).total_seconds()
                # Ensure sleep_seconds is not negative if clock changes etc.
                sleep_seconds = max(0, sleep_seconds)
                self.logger.debug(f"Sleeping for {sleep_seconds:.2f} seconds until next refresh check.")
                time.sleep(sleep_seconds)

            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error in background refresh thread: {e}")
                time.sleep(3600)  # Sleep for an hour if error occurs

    def _refresh_cache(self, exchange: str):
        """Refresh the symbol cache for a specific exchange."""
        with self._refresh_lock: # Use global lock for the entire refresh process
            try:
                self.logger.info(f"Refreshing cache for {exchange}...")
                # Create temporary directory for downloads
                temp_dir = self.base_cache_dir / f"temp_{exchange}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                temp_dir.mkdir(parents=True, exist_ok=True)

                try:
                    # Download and extract master file for the specific exchange
                    zip_file_name = MASTER_FILES_MAP.get(exchange.upper())
                    if not zip_file_name:
                         self.logger.warning(f"No master file defined for exchange {exchange}")
                         return

                    extracted_files = self._download_and_extract_masters(temp_dir, zip_file_name)
                    if not extracted_files:
                        self.logger.error(f"No master file was extracted for {exchange}")
                        return

                    # Load symbols into cache for this exchange
                    new_exchange_cache = self._load_symbols_to_cache(extracted_files, exchange)
                    if not new_exchange_cache:
                        self.logger.error(f"Failed to load symbols into cache for {exchange}")
                        return

                    # Validate new cache
                    if not self._validate_cache(new_exchange_cache):
                        self.logger.error(f"Cache validation failed for {exchange}")
                        return

                    # Save new cache for this exchange
                    self._save_cache(exchange, new_exchange_cache)
                    self.symbol_cache[exchange] = new_exchange_cache
                    self.last_refresh_date[exchange] = date.today()

                    # Update global statistics based on all caches
                    self._update_stats()

                    self.logger.info(f"Cache refreshed successfully for {exchange} with {len(new_exchange_cache)} symbols")

                finally:
                    # Clean up temporary directory
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)

            except Exception as e:
                self.stats.error_count += 1 # Global error count
                self.logger.error(f"Error refreshing cache for {exchange}: {e}")

    def _validate_cache(self, cache: Dict[str, Dict[str, Any]]) -> bool:
        """Validate the cache data structure for a single exchange."""
        if not cache:
            return False

        # Check for required fields in each entry
        for key, value in cache.items():
            if not all(k in value for k in ['exch', 'token', 'tsym']):
                self.logger.warning(f"Invalid cache entry format: {key} -> {value}")
                return False

            # Optional: Add more specific validation if needed, e.g., check data types

        return True

    def _update_stats(self):
        """Update global cache statistics based on all loaded exchange caches."""
        self.stats.total_symbols = sum(len(cache) for cache in self.symbol_cache.values())
        self.stats.symbols_by_exchange.clear()

        for exchange, cache in self.symbol_cache.items():
            self.stats.symbols_by_exchange[exchange] = len(cache)

        self.stats.last_refresh_time = datetime.now()
        self.stats.refresh_count += 1
        # Calculate total cache size across all exchange files
        total_size = 0
        for exchange in REQUIRED_EXCHANGES:
            cache_file = self._get_cache_file_path(exchange)
            if cache_file.exists():
                total_size += os.path.getsize(cache_file)
        self.stats.cache_size_bytes = total_size


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
        """Clean up old cache and metadata files."""
        try:
            current_time = datetime.now()
            # Clean up exchange-specific files
            for exchange in REQUIRED_EXCHANGES:
                cache_file = self._get_cache_file_path(exchange)
                metadata_file = self._get_cache_metadata_file_path(exchange)
                expiry_file = self._get_expiry_dates_file_path(exchange)

                for file in [cache_file, metadata_file, expiry_file]:
                    if file.exists():
                        file_age = current_time - datetime.fromtimestamp(file.stat().st_mtime)
                        if file_age.days > max_age_days:
                            self.logger.info(f"Removing old cache file: {file}")
                            file.unlink(missing_ok=True) # Use missing_ok for robustness

            # Clean up any old temp directories
            for item in self.base_cache_dir.iterdir():
                 if item.is_dir() and item.name.startswith("temp_"):
                     try:
                         # Check directory age (based on modification time)
                         dir_age = current_time - datetime.fromtimestamp(item.stat().st_mtime)
                         if dir_age.days > max_age_days: # Use same max_age_days for temp dirs
                             self.logger.info(f"Removing old temporary directory: {item}")
                             import shutil
                             shutil.rmtree(item)
                     except Exception as e:
                         self.logger.error(f"Error removing temp directory {item}: {e}")


        except Exception as e:
            self.logger.error(f"Error cleaning up cache directory: {e}")


    def _download_and_extract_masters(self, target_dir: Path, zip_file_name: str) -> List[Path]:
        """Download and extract a specific master symbol file."""
        extracted_files = []

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
                # Extract only the text file expected inside the zip
                expected_txt_name = zip_file_name.replace('.zip', '')
                if expected_txt_name in z.namelist():
                     member_path = target_dir / Path(expected_txt_name).name
                     z.extract(expected_txt_name, path=target_dir)
                     extracted_files.append(member_path)
                else:
                    self.logger.warning(f"Expected text file '{expected_txt_name}' not found in zip '{zip_file_name}'")
                    # Fallback: extract all if specific name not found (with safety check)
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

    def _load_symbols_to_cache(self, file_paths: List[Path], exchange: str) -> Dict[str, Dict[str, Any]]:
        """Load symbols from extracted files into a cache dictionary for a specific exchange."""
        exchange_symbol_cache = {}

        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.reader(f)
                    try:
                        header = next(reader)
                    except StopIteration:
                        self.logger.warning(f"File {file_path} is empty or has no header.")
                        continue

                    # Find column indices
                    col_indices = {}
                    # Normalize header names for case-insensitivity and variations
                    header_map = {col.strip().lower(): i for i, col in enumerate(header)}

                    required_cols = ['exchange', 'token', 'tradingsymbol']
                    found_cols = {}
                    for req_col in required_cols:
                        # Check common variations
                        if req_col in header_map:
                            found_cols[req_col] = header_map[req_col]
                        elif req_col == 'exchange' and 'exch' in header_map:
                            found_cols[req_col] = header_map['exch']
                        elif req_col == 'token' and 'instrumenttoken' in header_map:
                            found_cols[req_col] = header_map['instrumenttoken']
                        elif req_col == 'tradingsymbol' and 'tsym' in header_map:
                            found_cols[req_col] = header_map['tsym']

                    if len(found_cols) < len(required_cols):
                        self.logger.warning(f"Missing required columns in file {file_path}. Found: {list(header_map.keys())}")
                        continue # Skip this file if essential columns are missing

                    # Process rows
                    for row in reader:
                        if len(row) < len(header):
                            self.logger.debug(f"Skipping malformed row in {file_path}: {row}")
                            continue

                        try:
                            row_exchange = row[found_cols['exchange']].strip().upper()

                            # Only process rows for the target exchange
                            if row_exchange != exchange.upper():
                                continue

                            token = row[found_cols['token']].strip()
                            tsym = row[found_cols['tradingsymbol']].strip()

                            if not all([row_exchange, token, tsym]):
                                self.logger.debug(f"Skipping row with missing data in {file_path}: {row}")
                                continue

                            # Create cache entry key using the row's exchange and symbol
                            cache_key = f"{row_exchange}:{tsym.upper()}"
                            exchange_symbol_cache[cache_key] = {
                                'exch': row_exchange,
                                'token': token,
                                'tsym': tsym
                                # Add other relevant fields from the CSV if needed
                                # e.g., 'instname', 'insttype', 'expiry', 'strike', 'pptick', 'lotsize'
                            }

                        except (IndexError, ValueError, KeyError) as e:
                            self.logger.warning(f"Error processing row in {file_path}: {e} - Row: {row}")

            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")

        return exchange_symbol_cache

    def _load_cache(self, exchange: str):
        """Load existing cache for a specific exchange from file."""
        cache_file = self._get_cache_file_path(exchange)
        metadata_file = self._get_cache_metadata_file_path(exchange)

        try:
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.symbol_cache[exchange] = json.load(f)

                if metadata_file.exists():
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        last_refresh_str = metadata.get('last_refresh_date', '')
                        try:
                            self.last_refresh_date[exchange] = date.fromisoformat(last_refresh_str) if last_refresh_str else None
                        except ValueError:
                             self.logger.warning(f"Invalid last_refresh_date format in metadata for {exchange}: {last_refresh_str}")
                             self.last_refresh_date[exchange] = None

                self.logger.info(f"Loaded cache for {exchange} with {len(self.symbol_cache.get(exchange, {}))} symbols")

            else:
                self.logger.info(f"No existing cache found for {exchange}, will create new one")
                # Initialize with empty cache if file doesn't exist
                self.symbol_cache[exchange] = {}
                self.last_refresh_date[exchange] = None
                # Trigger refresh for this specific exchange if no cache was found
                # This will happen during the first background refresh cycle or on explicit call
                # self._refresh_cache(exchange) # Avoid calling refresh directly in load

        except Exception as e:
            self.logger.error(f"Error loading cache for {exchange}: {e}")
            # Initialize with empty cache on error
            self.symbol_cache[exchange] = {}
            self.last_refresh_date[exchange] = None
            # Consider triggering a refresh here as well, but be cautious of infinite loops
            # self._refresh_cache(exchange)


    def _save_cache(self, exchange: str, cache_data: Dict[str, Dict[str, Any]]):
        """Save cache for a specific exchange to file."""
        cache_file = self._get_cache_file_path(exchange)
        metadata_file = self._get_cache_metadata_file_path(exchange)

        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=4) # Use indent for readability

            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_refresh_date': date.today().isoformat()
                }, f, indent=4)

        except Exception as e:
            self.logger.error(f"Error saving cache for {exchange}: {e}")

    def _load_expiry_dates(self, exchange: str):
        """Load expiry dates for a specific exchange from file."""
        expiry_file = self._get_expiry_dates_file_path(exchange)
        try:
            if expiry_file.exists():
                with open(expiry_file, 'r', encoding='utf-8') as f:
                    self.expiry_dates[exchange] = json.load(f)
                self.logger.info(f"Loaded expiry dates for {exchange}")
            else:
                self.logger.info(f"No existing expiry dates file found for {exchange}")
                self.expiry_dates[exchange] = {} # Initialize with empty dict

        except Exception as e:
            self.logger.error(f"Error loading expiry dates for {exchange}: {e}")
            self.expiry_dates[exchange] = {} # Initialize with empty dict on error

    def _save_expiry_dates(self, exchange: str, expiry_data: Dict[str, Any]):
        """Save expiry dates for a specific exchange to file."""
        expiry_file = self._get_expiry_dates_file_path(exchange)
        try:
            # Ensure the directory exists before saving
            expiry_file.parent.mkdir(parents=True, exist_ok=True)
            with open(expiry_file, 'w', encoding='utf-8') as f:
                json.dump(expiry_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            self.logger.error(f"Error saving expiry dates for {exchange}: {e}")

    # Placeholder method to update expiry dates - needs implementation based on source
    def update_expiry_dates(self, exchange: str, data: Dict[str, Any]):
        """
        Update expiry dates for a specific exchange.
        This method needs to be called with actual expiry data obtained from an external source.
        """
        if exchange.upper() not in REQUIRED_EXCHANGES:
            self.logger.warning(f"Attempted to update expiry dates for unsupported exchange: {exchange}")
            return

        # Basic validation of the data structure (can be enhanced)
        if not isinstance(data, dict) or 'indices' not in data:
            self.logger.warning(f"Invalid expiry dates data structure provided for {exchange}")
            return

        self.expiry_dates[exchange.upper()] = data
        self._save_expiry_dates(exchange.upper(), data)
        self.logger.info(f"Expiry dates updated and saved for {exchange}")


    def lookup_symbol(self, symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
        """
        Look up a symbol **in the local cache** for a specific exchange.

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

        exchange_upper = exchange.upper()
        if exchange_upper not in REQUIRED_EXCHANGES:
             self.logger.warning(f"Unsupported exchange for cache lookup: {exchange}")
             return None

        exchange_cache = self.symbol_cache.get(exchange_upper)
        if not exchange_cache:
             self.logger.warning(f"Symbol cache is not loaded for {exchange}. Cannot perform cache lookup.")
             return None

        # Construct the cache key using uppercase for consistency
        cache_key = f"{exchange_upper}:{symbol.upper()}"
        self.logger.debug(f"Looking up symbol in cache: {cache_key}")

        cached_info = exchange_cache.get(cache_key)

        if cached_info:
            self.logger.info(f"Found '{symbol}' on '{exchange}' in local cache.")
            return cached_info.copy() # Return a copy
        else:
            # Log at debug level as this is expected for API fallback sometimes
            self.logger.debug(f"'{symbol}' on '{exchange}' not found in local cache.")
            return None

    # --- Expiry Date Interface Methods ---

    def get_all_expiries(self, exchange: str, index_name: str, expiry_type: str) -> List[str]:
        """
        Get all expiry dates for a given index and expiry type from the cache.

        Args:
            exchange: The exchange code (e.g., "NFO"). Case-insensitive.
            index_name: The name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            expiry_type: The type of expiry ("weekly" or "monthly"). Case-insensitive.

        Returns:
            A list of expiry date strings (YYYY-MM-DD) or an empty list if not found.
        """
        exchange_upper = exchange.upper()
        index_name_upper = index_name.upper()
        expiry_type_lower = expiry_type.lower()

        if exchange_upper not in REQUIRED_EXCHANGES:
             self.logger.warning(f"Unsupported exchange for expiry date lookup: {exchange}")
             return []

        expiry_data = self.expiry_dates.get(exchange_upper)
        if not expiry_data or 'indices' not in expiry_data:
            self.logger.warning(f"Expiry dates not loaded or invalid for {exchange}")
            return []

        indices_data = expiry_data['indices']
        index_data = indices_data.get(index_name_upper)

        if not index_data:
            self.logger.warning(f"No expiry data found for index: {index_name} on {exchange}")
            return []

        expiries = index_data.get(expiry_type_lower, [])

        # Ensure expiries is a list of strings
        if not isinstance(expiries, list) or not all(isinstance(e, str) for e in expiries):
             self.logger.warning(f"Expiry data for {index_name} ({expiry_type}) on {exchange} is not in expected list format.")
             return []

        # Optional: Sort expiries to ensure they are in chronological order
        try:
            sorted_expiries = sorted(expiries, key=lambda d: datetime.strptime(d, "%Y-%m-%d"))
            return sorted_expiries
        except ValueError:
            self.logger.warning(f"Could not sort expiry dates for {index_name} ({expiry_type}) on {exchange}. Dates might not be in YYYY-MM-DD format.")
            return expiries # Return unsorted if sorting fails


    def get_nearest_expiry(self, exchange: str, index_name: str, expiry_type: str) -> Optional[str]:
        """
        Get the nearest upcoming expiry date for a given index and expiry type.

        Args:
            exchange: The exchange code (e.g., "NFO"). Case-insensitive.
            index_name: The name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            expiry_type: The type of expiry ("weekly" or "monthly"). Case-insensitive.

        Returns:
            The nearest expiry date string (YYYY-MM-DD) or None if not found or no future expiries.
        """
        all_expiries = self.get_all_expiries(exchange, index_name, expiry_type)
        if not all_expiries:
            return None

        today = date.today()

        # Find the first expiry date that is today or in the future
        for expiry_str in all_expiries:
            try:
                expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                if expiry_date >= today:
                    return expiry_str
            except ValueError:
                self.logger.warning(f"Invalid date format in expiry data for {index_name} ({expiry_type}) on {exchange}: {expiry_str}")
                continue # Skip invalid dates

        return None # No future expiries found

    def get_expiry_by_offset(self, exchange: str, index_name: str, expiry_type: str, offset: int) -> Optional[str]:
        """
        Get an expiry date by offset (0-based index) from the list of upcoming expiries.

        Args:
            exchange: The exchange code (e.g., "NFO"). Case-insensitive.
            index_name: The name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            expiry_type: The type of expiry ("weekly" or "monthly"). Case-insensitive.
            offset: The 0-based index of the expiry date in the list of upcoming expiries.

        Returns:
            The expiry date string (YYYY-MM-DD) at the specified offset or None if the offset is out of bounds.
        """
        # Get only upcoming expiries starting from today
        all_expiries = self.get_all_expiries(exchange, index_name, expiry_type)
        if not all_expiries:
            return None

        today = date.today()
        upcoming_expiries = []

        for expiry_str in all_expiries:
            try:
                expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                if expiry_date >= today:
                    upcoming_expiries.append(expiry_str)
            except ValueError:
                self.logger.warning(f"Invalid date format in expiry data for {index_name} ({expiry_type}) on {exchange}: {expiry_str}")
                continue # Skip invalid dates


        if 0 <= offset < len(upcoming_expiries):
            return upcoming_expiries[offset]
        else:
            self.logger.warning(f"Offset {offset} is out of bounds for upcoming {expiry_type} expiries for {index_name} on {exchange}. Found {len(upcoming_expiries)} upcoming expiries.")
            return None


    def format_expiry_for_symbol(self, expiry_date_str: str) -> Optional[str]:
        """
        Format expiry date string (YYYY-MM-DD) to DDMMMYY format for symbol construction.
        Handles both YYYY-MM-DD and existing DDMMMYY/DDMMMYYYY formats.
        """
        if not isinstance(expiry_date_str, str):
            return None

        expiry_date_str = expiry_date_str.strip()

        try:
            # Try parsing YYYY-MM-DD format first
            dt = datetime.strptime(expiry_date_str, "%Y-%m-%d")
            return dt.strftime("%d%b%y").upper()
        except ValueError:
            pass # Not YYYY-MM-DD format, try others

        try:
            # Try parsing DDMMMYYYY format
            dt = datetime.strptime(expiry_date_str, "%d%b%Y")
            return dt.strftime("%d%b%y").upper()
        except ValueError:
            pass # Not DDMMMYYYY format, try others

        try:
            # Try parsing DDMMMYY format (assuming it's already correct)
            datetime.strptime(expiry_date_str, "%d%b%y")
            return expiry_date_str.upper()
        except ValueError:
            pass # Not DDMMMYY format either

        self.logger.warning(f"Could not parse expiry date string: {expiry_date_str}. Expected YYYY-MM-DD, DDMMMYYYY, or DDMMMYY.")
        return None


    def construct_trading_symbol(
        self,
        name: str,
        expiry_date_str: str, # Expecting YYYY-MM-DD format primarily now
        instrument_type: str,
        option_type: Optional[str] = None,
        strike: Optional[Union[int, float, str]] = None
    ) -> Optional[str]:
        """
        Construct trading symbol based on Finvasia's naming convention.
        Expects expiry_date_str in YYYY-MM-DD format.
        """
        name = name.upper()
        instrument_type = instrument_type.upper()
        formatted_expiry = self.format_expiry_for_symbol(expiry_date_str)

        if not formatted_expiry:
            self.logger.warning(f"Failed to format expiry date '{expiry_date_str}' for symbol construction.")
            return None

        if instrument_type == 'FUT':
            return f"{name}{formatted_expiry}F"

        elif instrument_type == 'OPT':
            if not option_type or strike is None:
                self.logger.warning("Option symbol construction requires option_type and strike.")
                return None

            option_type = option_type.upper()
            if option_type not in ['P', 'C']:
                self.logger.warning(f"Invalid option type: {option_type}. Must be 'P' or 'C'.")
                return None

            try:
                strike_float = float(strike)
                # Format strike based on whether it's an integer or has decimal places
                if strike_float == int(strike_float):
                    strike_str = str(int(strike_float))
                else:
                    # Format with a fixed number of decimal places if needed, or just use str()
                    strike_str = str(strike_float)
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid strike value: {strike}")
                return None

            return f"{name}{formatted_expiry}{option_type}{strike_str}"

        else:
            self.logger.warning(f"Unsupported instrument type for symbol construction: {instrument_type}")
            return None

    def stop(self):
        """Stop the background refresh thread."""
        self._is_running = False
        if self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5.0)
            if self._refresh_thread.is_alive():
                 self.logger.warning("Background refresh thread did not stop within timeout.")

# Example usage (for testing purposes)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Simulate creating/updating expiry dates files manually or via another process
    # In a real application, this data would likely come from an API
    sample_expiry_data_nfo = {
      "version": "1.0",
      "last_updated": datetime.now().isoformat(),
      "indices": {
        "NIFTY": {
          "weekly": [
            "2025-04-24", "2025-05-01", "2025-05-08", "2025-05-15", "2025-05-22", "2025-05-29"
          ],
          "monthly": [
            "2025-04-30", "2025-05-29", "2025-06-26", "2025-07-31", "2025-08-28"
          ]
        },
        "BANKNIFTY": {
          "weekly": [
            "2025-04-25", "2025-05-02", "2025-05-09", "2025-05-16", "2025-05-23", "2025-05-30"
          ],
          "monthly": [
            "2025-04-30", "2025-05-29", "2025-06-26", "2025-07-31", "2025-08-28"
          ]
        }
      }
    }

    sample_expiry_data_bfo = {
      "version": "1.0",
      "last_updated": datetime.now().isoformat(),
      "indices": {
        "BANKEX": {
          "weekly": [
            "2025-04-26", "2025-05-03", "2025-05-10"
          ],
          "monthly": [
            "2025-04-30", "2025-05-29"
          ]
        }
      }
    }


    # Get the singleton instance
    cache = SymbolCache()

    # Manually update expiry dates for demonstration
    cache.update_expiry_dates("NFO", sample_expiry_data_nfo)
    cache.update_expiry_dates("BFO", sample_expiry_data_bfo)


    # Give some time for potential background refresh if cache was empty
    # In a real app, you might wait for the first refresh to complete if needed immediately
    # time.sleep(5) # Optional: Give background thread a moment

    # --- Test Symbol Lookup ---
    print("\n--- Testing Symbol Lookup ---")
    # Assuming symbols like RELIANCE and NIFTY INDEX exist in the downloaded files
    # and NIFTY25APR25FUT exists in NFO F&O
    reliance_nse = cache.lookup_symbol("RELIANCE", "NSE")
    if reliance_nse:
        print(f"Found RELIANCE on NSE: {reliance_nse}")
    else:
        print("RELIANCE on NSE not found in cache.")

    nifty_fut_nfo = cache.lookup_symbol("NIFTY25APR25FUT", "NFO")
    if nifty_fut_nfo:
         print(f"Found NIFTY25APR25FUT on NFO: {nifty_fut_nfo}")
    else:
         print("NIFTY25APR25FUT on NFO not found in cache. (Might need to run refresh or ensure master file contains it)")

    invalid_symbol = cache.lookup_symbol("NONEXISTENT", "NSE")
    if not invalid_symbol:
        print("NONEXISTENT on NSE correctly not found.")

    invalid_exchange = cache.lookup_symbol("RELIANCE", "MCX")
    if not invalid_exchange:
        print("RELIANCE on MCX correctly not found (unsupported exchange).")

    # --- Test Expiry Date Interface ---
    print("\n--- Testing Expiry Date Interface (NFO:NIFTY) ---")
    nifty_weekly_expiries = cache.get_all_expiries("NFO", "NIFTY", "weekly")
    print(f"All NIFTY weekly expiries (NFO): {nifty_weekly_expiries}")

    nifty_monthly_expiries = cache.get_all_expiries("NFO", "NIFTY", "monthly")
    print(f"All NIFTY monthly expiries (NFO): {nifty_monthly_expiries}")

    nearest_nifty_weekly = cache.get_nearest_expiry("NFO", "NIFTY", "weekly")
    print(f"Nearest NIFTY weekly expiry (NFO): {nearest_nifty_weekly}")

    nearest_nifty_monthly = cache.get_nearest_expiry("NFO", "NIFTY", "monthly")
    print(f"Nearest NIFTY monthly expiry (NFO): {nearest_nifty_monthly}")

    first_nifty_weekly = cache.get_expiry_by_offset("NFO", "NIFTY", "weekly", 0)
    print(f"First upcoming NIFTY weekly expiry (NFO, offset 0): {first_nifty_weekly}")

    second_nifty_monthly = cache.get_expiry_by_offset("NFO", "NIFTY", "monthly", 1)
    print(f"Second upcoming NIFTY monthly expiry (NFO, offset 1): {second_nifty_monthly}")

    expiry_out_of_bounds = cache.get_expiry_by_offset("NFO", "NIFTY", "weekly", 99)
    print(f"NIFTY weekly expiry (NFO, offset 99): {expiry_out_of_bounds} (Expected None)")

    print("\n--- Testing Expiry Date Interface (BFO:BANKEX) ---")
    bankex_weekly_expiries = cache.get_all_expiries("BFO", "BANKEX", "weekly")
    print(f"All BANKEX weekly expiries (BFO): {bankex_weekly_expiries}")

    nearest_bankex_weekly = cache.get_nearest_expiry("BFO", "BANKEX", "weekly")
    print(f"Nearest BANKEX weekly expiry (BFO): {nearest_bankex_weekly}")

    # --- Test Symbol Construction ---
    print("\n--- Testing Symbol Construction ---")
    # Assuming nearest_nifty_weekly is 'YYYY-MM-DD' format like '2025-04-24'
    if nearest_nifty_weekly:
        nifty_fut_symbol = cache.construct_trading_symbol("NIFTY", nearest_nifty_weekly, "FUT")
        print(f"Constructed NIFTY Future symbol for {nearest_nifty_weekly}: {nifty_fut_symbol}")

        nifty_call_symbol = cache.construct_trading_symbol("NIFTY", nearest_nifty_weekly, "OPT", "C", 22000)
        print(f"Constructed NIFTY Call symbol for {nearest_nifty_weekly}, Strike 22000: {nifty_call_symbol}")

        nifty_put_symbol_float_strike = cache.construct_trading_symbol("NIFTY", nearest_nifty_weekly, "OPT", "P", 21950.50)
        print(f"Constructed NIFTY Put symbol for {nearest_nifty_weekly}, Strike 21950.50: {nifty_put_symbol_float_strike}")
    else:
        print("Cannot test symbol construction as nearest NIFTY weekly expiry was not found.")


    # Get stats
    print("\n--- Cache Statistics ---")
    print(cache.get_stats())

    # Clean up old files (optional, for testing cleanup)
    # print("\n--- Running Cache Cleanup ---")
    # cache.cleanup_cache(max_age_days=0) # Clean up files older than 0 days (i.e., all files)

    # Stop the background thread before exiting
    print("\nStopping cache background thread...")
    cache.stop()
    print("SymbolCache stopped.")


