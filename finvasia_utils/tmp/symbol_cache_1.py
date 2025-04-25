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
# from utils.constants import Exchange

# Constants
CACHE_DIR = Path("symbol_cache")
CACHE_METADATA_FILE = CACHE_DIR / "cache_metadata.json"
EXPIRY_DATES_FILE = CACHE_DIR / "expiry_dates.json"
MASTERS_URL_ROOT = 'https://api.shoonya.com/'
MASTER_FILES = [
    'NSE_symbols.txt.zip',  # NSE Equity
    'BSE_symbols.txt.zip',  # BSE Equity
    'NFO_symbols.txt.zip',  # NSE F&O
    'BFO_symbols.txt.zip',  # BSE F&O
]

# Exchange codes for filtering
REQUIRED_EXCHANGES = {
    'NSE',  # NSE Equity
    'BSE',  # BSE Equity
    'NFO',  # NSE F&O
    'BFO'   # BSE F&O
}

# Mapping of exchange to cache file
EXCHANGE_CACHE_FILES = {
    'NSE': CACHE_DIR / "nse_symbol_cache.json",
    'BSE': CACHE_DIR / "bse_symbol_cache.json",
    'NFO': CACHE_DIR / "nfo_symbol_cache.json",
    'BFO': CACHE_DIR / "bfo_symbol_cache.json"
}

# Major indices for expiry date tracking
TRACKED_INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]

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
    Exchange-specific caches and expiry date management.
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
        self.cache_metadata_file = CACHE_METADATA_FILE
        self.expiry_dates_file = EXPIRY_DATES_FILE

        # Exchange-specific caches
        self.symbol_caches = {exchange: {} for exchange in REQUIRED_EXCHANGES}
        self.expiry_dates = {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "indices": {}
        }

        self.last_refresh_date = None
        self._refresh_lock = threading.Lock()
        self._is_running = True
        self.stats = CacheStats()

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Load existing cache
        self._load_cache()
        self._load_expiry_dates()

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
                    new_caches = self._load_symbols_to_cache(extracted_files)
                    if not new_caches:
                        self.logger.error("Failed to load symbols into cache")
                        return

                    # Extract expiry dates from the cache
                    self._extract_expiry_dates(new_caches)

                    # Validate new caches
                    for exchange, cache in new_caches.items():
                        if not self._validate_cache(cache):
                            self.logger.error(f"Cache validation failed for {exchange}")
                            return

                    # Save new caches
                    self._save_caches(new_caches)
                    self._save_expiry_dates()
                    self.symbol_caches = new_caches
                    self.last_refresh_date = date.today()

                    # Update statistics
                    self._update_stats(new_caches)

                    total_symbols = sum(len(cache) for cache in new_caches.values())
                    self.logger.info(f"Cache refreshed successfully with {total_symbols} symbols across exchanges")

                finally:
                    # Clean up temporary directory
                    if temp_dir.exists():
                        import shutil
                        shutil.rmtree(temp_dir)

            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error refreshing cache: {e}")

    def _extract_expiry_dates(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
        """Extract expiry dates from derivatives data."""
        # Focus on NFO cache as it contains derivatives
        if 'NFO' not in caches:
            self.logger.warning("NFO cache not found, can't extract expiry dates")
            return

        nfo_cache = caches['NFO']

        # Reset expiry dates structure
        expiry_data = {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "indices": {}
        }

        # Extract expiry dates for tracked indices
        for index_name in TRACKED_INDICES:
            weekly_expiries = set()
            monthly_expiries = set()

            # Find all symbols related to this index
            for key, value in nfo_cache.items():
                tsym = value.get('tsym', '').upper()

                # Check if symbol starts with index name
                if not tsym.startswith(index_name):
                    continue

                # Extract expiry date from symbol
                try:
                    # Assuming format like "NIFTY25APR25C18000"
                    # Extract date part after index name (e.g., "25APR25")
                    date_part = None

                    # Try to extract date part based on standard patterns
                    if len(tsym) > len(index_name) + 7:  # Ensure there's enough chars for date
                        date_part = tsym[len(index_name):len(index_name)+7]

                    if date_part:
                        try:
                            # Parse the date
                            expiry_date = datetime.strptime(date_part, "%d%b%y").date()

                            # Classify as weekly or monthly
                            # Typically, monthly expiries are last Thursday of month
                            if expiry_date.day >= 25 or expiry_date.day <= 7:
                                monthly_expiries.add(expiry_date.isoformat())
                            else:
                                weekly_expiries.add(expiry_date.isoformat())

                        except ValueError:
                            pass  # Not a valid date format
                except Exception as e:
                    self.logger.debug(f"Error parsing expiry from {tsym}: {e}")

            # Sort the expiry dates
            sorted_weekly = sorted(list(weekly_expiries))
            sorted_monthly = sorted(list(monthly_expiries))

            # Add to indices data
            if sorted_weekly or sorted_monthly:
                expiry_data["indices"][index_name] = {
                    "weekly": sorted_weekly,
                    "monthly": sorted_monthly
                }

        # Update the class expiry dates
        self.expiry_dates = expiry_data

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

    def _update_stats(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
        """Update cache statistics."""
        self.stats.total_symbols = sum(len(cache) for cache in caches.values())
        self.stats.symbols_by_exchange.clear()

        for exchange, cache in caches.items():
            self.stats.symbols_by_exchange[exchange] = len(cache)

        self.stats.last_refresh_time = datetime.now()
        self.stats.refresh_count += 1

        # Calculate total cache size
        total_size = 0
        for file_path in EXCHANGE_CACHE_FILES.values():
            if file_path.exists():
                total_size += os.path.getsize(file_path)

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

    def _load_symbols_to_cache(self, file_paths: List[Path]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Load symbols from extracted files into exchange-specific cache dictionaries."""
        # Initialize exchange-specific caches
        caches = {exchange: {} for exchange in REQUIRED_EXCHANGES}

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
                            cache_key = f"{tsym.upper()}"  # Exchange is already part of the cache dictionary
                            caches[exch][cache_key] = {
                                'exch': exch,
                                'token': token,
                                'tsym': tsym
                            }

                        except (IndexError, ValueError) as e:
                            self.logger.warning(f"Error processing row: {e}")

            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")

        return caches

    def _load_cache(self):
        """Load existing exchange-specific caches from files."""
        try:
            # Load exchange-specific caches
            for exchange, cache_file in EXCHANGE_CACHE_FILES.items():
                if cache_file.exists():
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        self.symbol_caches[exchange] = json.load(f)

                    self.logger.info(f"Loaded {exchange} cache with {len(self.symbol_caches[exchange])} symbols")
                else:
                    self.logger.info(f"No existing cache found for {exchange}, will create new one")

            # Load metadata
            if self.cache_metadata_file.exists():
                with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    self.last_refresh_date = date.fromisoformat(metadata.get('last_refresh_date', ''))

            # Check if any cache is empty and if so, refresh
            if not any(self.symbol_caches.values()):
                self.logger.info("No caches found, will create new ones")
                self._refresh_cache()

        except Exception as e:
            self.logger.error(f"Error loading cache: {e}")
            self._refresh_cache()

    def _load_expiry_dates(self):
        """Load existing expiry dates from file."""
        try:
            if self.expiry_dates_file.exists():
                with open(self.expiry_dates_file, 'r', encoding='utf-8') as f:
                    self.expiry_dates = json.load(f)
                self.logger.info(f"Loaded expiry dates for {len(self.expiry_dates.get('indices', {}))} indices")
            else:
                self.logger.info("No existing expiry dates found, will create new one")
        except Exception as e:
            self.logger.error(f"Error loading expiry dates: {e}")

    def _save_caches(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
        """Save exchange-specific caches to files."""
        try:
            # Save exchange-specific caches
            for exchange, cache in caches.items():
                cache_file = EXCHANGE_CACHE_FILES.get(exchange)
                if cache_file:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(cache, f, ensure_ascii=False)
                    self.logger.info(f"Saved {exchange} cache with {len(cache)} symbols")

            # Save metadata
            with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_refresh_date': date.today().isoformat()
                }, f)

        except Exception as e:
            self.logger.error(f"Error saving caches: {e}")

    def _save_expiry_dates(self):
        """Save expiry dates to file."""
        try:
            with open(self.expiry_dates_file, 'w', encoding='utf-8') as f:
                json.dump(self.expiry_dates, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved expiry dates with {len(self.expiry_dates.get('indices', {}))} indices")
        except Exception as e:
            self.logger.error(f"Error saving expiry dates: {e}")

    def lookup_symbol(self, symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
        """
        Look up a symbol in the exchange-specific local cache.

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

        exchange = exchange.upper()
        if exchange not in self.symbol_caches:
            self.logger.warning(f"Unknown exchange: {exchange}")
            return None

        exchange_cache = self.symbol_caches[exchange]
        if not exchange_cache:
            self.logger.warning(f"Symbol cache for {exchange} is not loaded")
            return None

        # Use only symbol for lookup in exchange-specific cache
        cache_key = symbol.upper()
        self.logger.debug(f"Looking up {symbol} in {exchange} cache")

        cached_info = exchange_cache.get(cache_key)

        if cached_info:
            self.logger.info(f"Found '{symbol}' on '{exchange}' in local cache")
            return cached_info.copy()  # Return a copy
        else:
            self.logger.debug(f"'{symbol}' on '{exchange}' not found in local cache")
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
            elif len(expiry_date_str) == 10:  # YYYY-MM-DD
                dt = datetime.strptime(expiry_date_str, "%Y-%m-%d")
                return dt.strftime("%d%b%y").upper()
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

    # New methods for expiry date functionality

    def get_all_expiry_dates(self, index_name: str) -> Dict[str, List[str]]:
        """
        Get all expiry dates for a specific index.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY")

        Returns:
            Dictionary with "weekly" and "monthly" expiry date lists
        """
        index_name = index_name.upper()
        if 'indices' not in self.expiry_dates:
            return {"weekly": [], "monthly": []}

        indices = self.expiry_dates.get('indices', {})
        if index_name not in indices:
            self.logger.warning(f"No expiry dates found for index: {index_name}")
            return {"weekly": [], "monthly": []}

        return {
            "weekly": indices[index_name].get("weekly", []),
            "monthly": indices[index_name].get("monthly", [])
        }

    def get_nearest_expiry(self, index_name: str, expiry_type: str = "weekly") -> Optional[str]:
        """
        Get the nearest expiry date for an index.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY")
            expiry_type: Type of expiry ("weekly" or "monthly")

        Returns:
            ISO format date string of nearest expiry, or None if not found
        """
        index_name = index_name.upper()
        expiry_type = expiry_type.lower()

        if expiry_type not in ["weekly", "monthly"]:
            self.logger.error(f"Invalid expiry type: {expiry_type}")
            return None

        expiry_dates = self.get_all_expiry_dates(index_name)
        dates_list = expiry_dates.get(expiry_type, [])

        if not dates_list:
            self.logger.warning(f"No {expiry_type} expiry dates found for {index_name}")
            return None

        today = date.today().isoformat()
        # Find the first date that is greater than or equal to today
        for expiry in dates_list:
            if expiry >= today:
                return expiry

        # If all dates are in the past, return None
        return None

    def get_expiry_by_offset(self, index_name: str, offset: int = 0, expiry_type: str = "weekly") -> Optional[str]:
        """
        Get expiry date by offset from nearest expiry.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY")
            offset: Number of expiries to skip (0 = nearest, 1 = next, etc.)
            expiry_type: Type of expiry ("weekly" or "monthly")

        Returns:
            ISO format date string of expiry at given offset, or None if not found
        """
        index_name = index_name.upper()
        expiry_type = expiry_type.lower()

        if expiry_type not in ["weekly", "monthly"]:
            self.logger.error(f"Invalid expiry type: {expiry_type}")
            return None

        if offset < 0:
            self.logger.error(f"Offset must be non-negative: {offset}")
            return None

        expiry_dates = self.get_all_expiry_dates(index_name)
        dates_list = expiry_dates.get(expiry_type, [])

        if not dates_list:
            self.logger.warning(f"No {expiry_type} expiry dates found for {index_name}")
            return None

        today = date.today().isoformat()
        # Find the first expiry that is on or after today
        future_expiries = [d for d in dates_list if d >= today]

        if not future_expiries:
            self.logger.warning(f"No future {expiry_type} expiry dates found for {index_name}")
            return None

        if offset >= len(future_expiries):
            self.logger.warning(f"Offset {offset} exceeds available future expiries ({len(future_expiries)})")
            return None

        return future_expiries[offset]

    def is_expiry_data_available(self) -> bool:
        """Check if expiry data is available."""
        return 'indices' in self.expiry_dates and bool(self.expiry_dates['indices'])

    def refresh_expiry_dates(self) -> bool:
        """Force a refresh of expiry dates."""
        try:
            self._extract_expiry_dates(self.symbol_caches)
            self._save_expiry_dates()
            return True
        except Exception as e:
            self.logger.error(f"Error refreshing expiry dates: {e}")
            return False

    def stop(self):
        """Stop the background refresh thread."""
        self._is_running = False
        if self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5.0)
