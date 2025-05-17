import logging
import os
import threading
import time
import json
import requests
import zipfile
import csv
import re # Import regex for more robust symbol parsing
import shutil # Import shutil for rmtree
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field # Import field for defaultdict
from collections import defaultdict
# from utils.constants import Exchange # Assuming this is commented out or not used

# --- Constants ---
BASE_CACHE_DIR = Path("cache/finvasia")
CACHE_METADATA_FILE = BASE_CACHE_DIR / "cache_metadata.json"
EXPIRY_DATES_FILE = BASE_CACHE_DIR / "expiry_dates.json"
MASTERS_URL_ROOT = 'https://api.shoonya.com/'

# List of master files to download
MASTER_FILES = [
    'NSE_symbols.txt.zip',  # NSE Equity
    'NFO_symbols.txt.zip',  # NSE F&O
    'BFO_symbols.txt.zip',  # BSE F&O
    # Note: BSE Equity symbols are handled via the local file
]

# Define MASTER_FILES_MAP based on MASTER_FILES
# Creates a map like {'NSE': 'NSE_symbols.txt.zip', ...}
MASTER_FILES_MAP = {
    zip_file.split('_')[0]: zip_file
    for zip_file in MASTER_FILES
}


# Exchange codes to process
REQUIRED_EXCHANGES = {
    'NSE',  # NSE Equity
    'NFO',  # NSE F&O
    'BFO',  # BSE F&O
    'BSE'   # BSE Equity
}

# Mapping of exchange to cache file path template (using BASE_CACHE_DIR)
EXCHANGE_CACHE_FILE_TEMPLATE = BASE_CACHE_DIR / "{exchange}_symbol_cache.json"


# Major indices for expiry date tracking and their expected exchange
TRACKED_INDICES = {
    "NIFTY": "NFO",
    "BANKNIFTY": "NFO",
    "FINNIFTY": "NFO",
    "MIDCPNIFTY": "NFO",
    "SENSEX": "BFO", # SENSEX derivatives are on BFO
    "BANKEX": "BFO"  # BANKEX derivatives are on BFO
}

# BSE Symbol data (for the index itself, derivatives are in BFO)
BSE_SYMBOLS_DATA = [
    ["Exchange", "Token", "LotSize", "Symbol", "TradingSymbol", "Instrument", "TickSize"],
    ["BSE", "1", "1", "SENSEX", "SENSEX", "UNDIND", "0.05"],
    ["BSE", "12", "1", "BANKEX", "BANKEX", "UNDIND", "0.05"]
]

@dataclass
class CacheStats:
    """Statistics for symbol cache."""
    total_symbols: int = 0
    # Use field(default_factory=...) for mutable defaults like dicts/lists
    symbols_by_exchange: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    last_refresh_time: Optional[datetime] = None # Use Optional for clarity
    refresh_count: int = 0
    error_count: int = 0
    cache_size_bytes: int = 0

    # Add a __str__ or __repr__ for better printing
    def __str__(self):
        refresh_time_str = self.last_refresh_time.isoformat() if self.last_refresh_time else "Never"
        exchange_counts = ", ".join(f"{ex}: {count}" for ex, count in self.symbols_by_exchange.items())
        return (
            f"CacheStats(Total Symbols: {self.total_symbols}, By Exchange: [{exchange_counts}], "
            f"Last Refresh: {refresh_time_str}, Refreshes: {self.refresh_count}, "
            f"Errors: {self.error_count}, Size (bytes): {self.cache_size_bytes})"
        )

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
        self.base_cache_dir = BASE_CACHE_DIR

        # Initialize expiry_dates_file and cache_metadata_file paths
        self.expiry_dates_file = EXPIRY_DATES_FILE
        self.cache_metadata_file = CACHE_METADATA_FILE

        # Exchange-specific caches
        self.symbol_caches: Dict[str, Dict[str, Dict[str, Any]]] = {exchange: {} for exchange in REQUIRED_EXCHANGES}
        # Initialize expiry_dates with a default structure
        self.expiry_dates = {
            "version": "1.4", # Matches version used in last extraction logic
            "last_updated": datetime.now().isoformat(),
            "indices": {}
        }

        self.last_refresh_date = None # Global last refresh date for master files
        self._refresh_lock = threading.Lock()
        self._is_running = True
        self.stats = CacheStats()
        self._refresh_thread = None # Initialize refresh thread attribute

        # Ensure base cache directory exists
        self.base_cache_dir.mkdir(parents=True, exist_ok=True)

        # Create BSE_symbols.txt in the base cache directory if it doesn't exist
        self._create_bse_symbols_file()

        # Load existing caches and expiry dates
        self._load_caches() # Load all exchange caches
        self._load_expiry_dates() # Load the single expiry dates file

        # Update initial stats after loading
        self._update_stats(self.symbol_caches)

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
        # Use f-string or / operator for path construction.
        return self.base_cache_dir / f"{exchange.upper()}_symbol_cache.json"


    def _create_bse_symbols_file(self):
        """Create BSE_symbols.txt file with BSE index entries if it doesn't exist."""
        bse_symbols_file = self.base_cache_dir / "BSE_symbols.txt"
        if not bse_symbols_file.exists():
            try:
                # Create this file directly in the base cache directory
                with open(bse_symbols_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # Use the updated BSE_SYMBOLS_DATA constant
                    for row in BSE_SYMBOLS_DATA:
                        writer.writerow(row)
                self.logger.info(f"Created {bse_symbols_file.name} with BSE index entries")
            except Exception as e:
                self.logger.error(f"Error creating BSE_symbols.txt: {e}")
        else:
             self.logger.debug(f"{bse_symbols_file.name} already exists.")


    def _background_refresh(self):
        """Background thread to check and refresh cache daily."""
        # Perform initial check immediately if needed
        self._check_and_refresh()

        while self._is_running:
            try:
                # Calculate sleep time until next check (e.g., start of next day)
                now = datetime.now()
                current_date = now.date()
                # Schedule check slightly after midnight (e.g., 00:05 AM)
                next_check_time = datetime.combine(current_date + timedelta(days=1), datetime.min.time()) + timedelta(minutes=5)

                sleep_seconds = (next_check_time - now).total_seconds()
                # Ensure sleep_seconds is not negative if check time already passed
                sleep_seconds = max(1, sleep_seconds) # Sleep at least 1 second

                self.logger.debug(f"Next refresh check scheduled around {next_check_time}. Sleeping for {sleep_seconds:.2f} seconds.")
                # Use shorter sleeps and check _is_running more often for faster shutdown
                sleep_interval = 60 # Check every minute
                slept_time = 0
                while self._is_running and slept_time < sleep_seconds:
                    wait_time = min(sleep_interval, sleep_seconds - slept_time)
                    time.sleep(wait_time)
                    slept_time += wait_time
                    if not self._is_running:
                         self.logger.info("Background thread stopping during sleep.")
                         break # Exit if stopped

                if self._is_running:
                    self._check_and_refresh()

            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error in background refresh thread: {e}", exc_info=True)
                # Avoid busy-waiting on continuous errors
                if self._is_running:
                    time.sleep(3600) # Sleep for an hour if error occurs before retrying

    def _check_and_refresh(self):
         """Checks if a refresh is needed and triggers it."""
         current_date = date.today()
         # Check if we need to refresh master files (global check)
         # Refresh if never refreshed OR if last refresh was before today
         if self.last_refresh_date is None or self.last_refresh_date < current_date:
             self.logger.info(f"Daily cache refresh triggered (Last refresh: {self.last_refresh_date}, Today: {current_date})")
             self._refresh_cache() # Refresh all caches and expiries
         else:
             self.logger.debug(f"Cache is up-to-date (Last refresh: {self.last_refresh_date})")


    def _refresh_cache(self):
        """Refresh all symbol caches by downloading and processing master files."""
        if not self._refresh_lock.acquire(blocking=False):
            self.logger.warning("Refresh already in progress. Skipping.")
            return

        try:
            self.logger.info("Starting full cache refresh...")
            # Create temporary directory for downloads
            temp_dir = self.base_cache_dir / f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            temp_dir.mkdir(parents=True, exist_ok=True)

            extracted_files: List[Path] = [] # Ensure type hint
            try:
                # Download and extract master files
                extracted_files = self._download_and_extract_masters(temp_dir)

                # Check if BSE_symbols.txt exists (it should have been created on init)
                bse_symbols_file = self.base_cache_dir / "BSE_symbols.txt"
                if not bse_symbols_file.exists():
                    self.logger.warning("BSE_symbols.txt not found, cannot load BSE symbols.")

                # Check if we have any files to process (either extracted or local BSE)
                if not extracted_files and not bse_symbols_file.exists():
                    self.logger.error("No master files were extracted and BSE_symbols.txt not found. Cannot proceed.")
                    return # Exit refresh early

                # Load symbols into exchange-specific caches
                # Include the path to the BSE file if it exists
                files_to_process = extracted_files # Start with extracted files
                if bse_symbols_file.exists():
                     files_to_process.append(bse_symbols_file) # Add BSE file

                # Proceed only if there are files to process
                if not files_to_process:
                     self.logger.error("No files available to process for symbol loading.")
                     return

                new_caches = self._load_symbols_to_caches(files_to_process)
                if not new_caches: # Check if loading itself failed (returned None or empty dict)
                    self.logger.error("Failed to load symbols into caches (returned empty).")
                    # Keep existing caches if loading fails - prevent overwriting good cache with bad
                    return # Exit refresh
                # Check if *all* required exchange caches are empty (might indicate download/extract issue)
                if not any(new_caches.get(exch) for exch in MASTER_FILES_MAP):
                     self.logger.error("All downloaded symbol caches are empty. Check download/extraction.")
                     # Optionally return here to prevent overwriting existing cache
                     # return


                # Update instance caches with the newly loaded data
                self.symbol_caches = new_caches # Update instance caches
                self._save_caches(self.symbol_caches) # Save the updated caches (will skip empty ones)
                self.last_refresh_date = date.today() # Update global refresh date
                self.stats.last_refresh_time = datetime.now() # Update stats refresh time
                self.stats.refresh_count += 1
                self._save_metadata() # Save metadata including refresh date
                self.logger.info("Symbol caches updated and saved.")


                # Extract and save expiry dates from the new caches
                self._extract_expiry_dates(self.symbol_caches)
                self._save_expiry_dates()

                # Update statistics based on the newly active caches
                self._update_stats(self.symbol_caches)

                self.logger.info(f"Full cache refresh completed successfully. Stats: {self.stats}")

            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"Error during cache refresh process: {e}", exc_info=True)

            finally:
                # Clean up temporary directory
                if temp_dir.exists():
                    try:
                        shutil.rmtree(temp_dir)
                        self.logger.debug(f"Cleaned up temporary directory: {temp_dir}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up temp directory {temp_dir}: {e}")

        finally:
             self._refresh_lock.release() # Ensure lock is always released


    def _download_and_extract_masters(self, target_dir: Path) -> List[Path]:
        """Downloads and extracts master files."""
        extracted_files = []
        session = requests.Session() # Use a session for potential connection reuse

        for exchange, zip_filename in MASTER_FILES_MAP.items():
            url = MASTERS_URL_ROOT + zip_filename
            zip_path = target_dir / zip_filename
            # Correctly determine the expected TXT filename
            if zip_filename.endswith('.zip'):
                txt_filename = zip_filename[:-4] # Remove last 4 characters ('.zip')
            else:
                self.logger.error(f"Unexpected master filename format: {zip_filename}. Skipping.")
                continue

            extracted_txt_path = target_dir / txt_filename

            try:
                self.logger.info(f"Downloading {exchange} master file from {url}...")
                response = session.get(url, stream=True, timeout=60) # Added timeout
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                self.logger.info(f"Downloaded {zip_filename} successfully.")

                # Extract the zip file
                self.logger.info(f"Extracting {zip_filename}...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # Check if the expected txt filename exists in the zip archive
                    if txt_filename in zip_ref.namelist():
                        zip_ref.extract(txt_filename, target_dir)
                        if extracted_txt_path.exists():
                            extracted_files.append(extracted_txt_path)
                            self.logger.info(f"Extracted {txt_filename} successfully to {extracted_txt_path}")
                        else:
                             self.logger.error(f"Extraction seemed successful but {txt_filename} not found at {target_dir}")
                    else:
                        self.logger.error(f"Expected file '{txt_filename}' not found inside {zip_filename}. Available files: {zip_ref.namelist()}")

            except requests.exceptions.RequestException as e:
                self.stats.error_count += 1
                self.logger.error(f"Failed to download {url}: {e}")
            except zipfile.BadZipFile:
                self.stats.error_count += 1
                self.logger.error(f"Failed to extract {zip_filename}. File might be corrupted or not a zip file.")
            except Exception as e:
                self.stats.error_count += 1
                self.logger.error(f"An error occurred processing {zip_filename}: {e}", exc_info=True)
            finally:
                 # Ensure zip file is removed even if extraction fails but download succeeded
                 if zip_path.exists():
                      try:
                           os.remove(zip_path)
                           self.logger.debug(f"Removed temporary zip file: {zip_path}")
                      except OSError as e:
                           self.logger.error(f"Error removing temporary zip file {zip_path}: {e}")


        return extracted_files


    def _validate_cache(self, cache: Dict[str, Dict[str, Any]]) -> bool:
        """Perform basic validation on a loaded cache."""
        if not isinstance(cache, dict):
            self.logger.error("Cache validation failed: Cache is not a dictionary.")
            return False
        # Basic check passed
        return True


    def _get_last_day_of_month(self, year: int, month: int) -> date:
        """Get the last day of the given month."""
        if month == 12:
            return date(year, 12, 31)
        else:
            # Next month's first day minus one day
            return date(year, month + 1, 1) - timedelta(days=1)

    def _extract_expiry_dates(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
        """
        Extract expiry dates from the 'expiry' column and classify correctly
        into weekly and monthly based on the latest date within each month.
        """
        self.logger.info("Extracting and classifying expiry dates from 'expiry' column...")
        new_expiry_data = {
            "version": "1.4", # Keep version or increment if needed
            "last_updated": datetime.now().isoformat(),
            "indices": {}
        }

        # Expected date format in the 'Expiry' column
        expiry_date_format = "%d-%b-%Y" # e.g., 27-MAY-2025

        # Process expiries for each tracked index
        for index_name, expected_exchange in TRACKED_INDICES.items():
            exchange_cache = caches.get(expected_exchange)
            if not exchange_cache:
                self.logger.warning(f"Cache for {expected_exchange} not found or empty, cannot extract expiry dates for {index_name}")
                # Fallback for BSE indices if BFO cache missing
                if index_name in ["SENSEX", "BANKEX"] and expected_exchange == "BFO":
                     self._create_default_bse_expiries(index_name, new_expiry_data["indices"])
                continue

            # --- Step 1: Collect all unique valid expiry dates for this index ---
            all_valid_dates = set()
            index_symbols_count = 0
            for key, value in exchange_cache.items():
                # Check if the symbol relates to the current index
                underlying_symbol = value.get('symbol', '')
                tsym = value.get('tsym', '').upper()
                # Check if index name is in trading symbol OR underlying symbol (for derivatives)
                is_relevant = False
                # Check instrument type first for efficiency
                instrument = value.get('instrument')
                if instrument in ['OPTIDX', 'FUTIDX']:
                    # Check if index name matches the start of the underlying symbol
                    # (e.g., symbol='NIFTY' for NIFTY options/futures)
                    # Or if index name matches the start of trading symbol (less reliable but fallback)
                    if underlying_symbol.startswith(index_name) or tsym.startswith(index_name):
                         is_relevant = True

                if not is_relevant:
                    continue

                index_symbols_count += 1
                expiry_str = value.get('expiry')
                if expiry_str:
                    try:
                        expiry_date = datetime.strptime(expiry_str, expiry_date_format).date()
                        all_valid_dates.add(expiry_date) # Add date object to the set
                    except (ValueError, TypeError):
                         self.logger.debug(f"Could not parse expiry date string '{expiry_str}' using format '{expiry_date_format}' from symbol '{tsym}'")
                    except Exception as e:
                         self.logger.warning(f"Error parsing expiry string '{expiry_str}' for {tsym}: {e}")

            self.logger.info(f"Processed {index_symbols_count} potential {index_name} symbols in {expected_exchange}. Found {len(all_valid_dates)} unique expiry dates.")

            if not all_valid_dates:
                 # Fallback check only if BFO cache existed but yielded no dates
                 if index_name in ["SENSEX", "BANKEX"] and expected_exchange == "BFO":
                      self.logger.warning(f"No valid derivative expiries extracted for {index_name} from BFO 'expiry' column, creating defaults.")
                      self._create_default_bse_expiries(index_name, new_expiry_data["indices"])
                 else:
                      self.logger.warning(f"No valid expiry dates found for {index_name} in {expected_exchange} cache.")
                 continue # Skip to next index if no dates found


            # --- Step 2: Group dates by month and find the latest date (monthly expiry) ---
            monthly_expiries_dates = set()
            dates_by_month = defaultdict(list)
            for dt in all_valid_dates:
                dates_by_month[(dt.year, dt.month)].append(dt)

            for month_key, dates_in_month in dates_by_month.items():
                if dates_in_month:
                    monthly_expiry_for_month = max(dates_in_month) # Find the latest date
                    monthly_expiries_dates.add(monthly_expiry_for_month)
                    self.logger.debug(f"Identified {monthly_expiry_for_month.isoformat()} as monthly expiry for {index_name} in month {month_key}")


            # --- Step 3: Classify and store ISO strings ---
            weekly_expiries_iso = set()
            monthly_expiries_iso = set()

            for dt in all_valid_dates:
                expiry_iso = dt.isoformat()
                if dt in monthly_expiries_dates:
                    monthly_expiries_iso.add(expiry_iso)
                else:
                    weekly_expiries_iso.add(expiry_iso)

            # --- Step 4: Store sorted lists ---
            sorted_weekly = sorted(list(weekly_expiries_iso))
            sorted_monthly = sorted(list(monthly_expiries_iso))

            if sorted_weekly or sorted_monthly:
                new_expiry_data["indices"][index_name] = {
                    "weekly": sorted_weekly,
                    "monthly": sorted_monthly
                }
                self.logger.info(f"Classified {index_name} expiry dates: {len(sorted_weekly)} weekly, {len(sorted_monthly)} monthly")
            else:
                 # This case should not happen if all_valid_dates was not empty, but log just in case
                 self.logger.warning(f"No weekly or monthly expiries were classified for {index_name} despite finding valid dates.")


        # Update the class expiry dates
        self.expiry_dates = new_expiry_data


    def _create_default_bse_expiries(self, index_name: str, indices_data: Dict[str, Any]):
        """Create default expiry dates for BSE indices (SENSEX, BANKEX) based on Fridays."""
        # This is a fallback if the BFO file doesn't yield results
        weekly_expiries = set()
        monthly_expiries = set()
        expiry_weekday = 4 # Friday for BSE derivatives

        today = date.today()

        # Generate weekly expiries - all Fridays for next ~3 months (e.g., 12 weeks)
        current_date = today
        # Find next Friday (or today if it is Friday)
        days_ahead = (expiry_weekday - current_date.weekday() + 7) % 7
        current_date += timedelta(days=days_ahead)


        # Add ~12 consecutive Fridays
        for _ in range(12):
            # Check if it's the last Friday of the month
            last_day = self._get_last_day_of_month(current_date.year, current_date.month)
            last_friday = last_day
            while last_friday.weekday() != expiry_weekday:
                last_friday -= timedelta(days=1)

            expiry_iso = current_date.isoformat()
            if current_date == last_friday:
                monthly_expiries.add(expiry_iso)
            else:
                weekly_expiries.add(expiry_iso)

            current_date += timedelta(days=7) # Move to next week's Friday


        sorted_weekly = sorted(list(weekly_expiries))
        sorted_monthly = sorted(list(monthly_expiries))

        # Add to indices data only if not already present (avoid overwriting if called multiple times)
        if index_name not in indices_data:
            indices_data[index_name] = {
                "weekly": sorted_weekly,
                "monthly": sorted_monthly
            }
            self.logger.info(f"Created default {index_name} expiry dates: {len(sorted_weekly)} weekly, {len(sorted_monthly)} monthly")
        else:
             self.logger.debug(f"Default expiry data for {index_name} already exists, not overwriting.")


    def _load_symbols_to_caches(self, file_paths: List[Path]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Load symbols from extracted/local files into exchange-specific cache dictionaries."""
        # Initialize exchange-specific caches
        caches = {exchange: {} for exchange in REQUIRED_EXCHANGES}
        processed_files_count = 0

        for file_path in file_paths:
            file_exchange = None
            try:
                # Determine exchange from filename
                filename = file_path.name
                if filename == "BSE_symbols.txt":
                    file_exchange = 'BSE'
                else:
                    # Infer exchange from downloaded file name pattern (e.g., NFO_symbols.txt)
                    # Use MASTER_FILES_MAP for robust mapping
                    for exch, zip_name in MASTER_FILES_MAP.items():
                         expected_txt_name = zip_name[:-4] if zip_name.endswith('.zip') else zip_name # Remove .zip
                         if filename == expected_txt_name:
                            file_exchange = exch
                            break

                if file_exchange is None or file_exchange not in REQUIRED_EXCHANGES:
                    self.logger.warning(f"Could not determine required exchange for file: {filename}. Skipping.")
                    continue

                self.logger.info(f"Processing symbol file for {file_exchange}: {filename}")
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    reader = csv.reader(f)
                    try:
                        header = next(reader)
                        self.logger.debug(f"Header for {filename}: {header}")
                    except StopIteration:
                        self.logger.warning(f"File {filename} is empty or has no header.")
                        continue

                    # Find column indices dynamically and handle variations
                    header_map = {col.strip().lower().replace(' ', '').replace('_',''): i for i, col in enumerate(header)}

                    # Map expected keys to actual column names found
                    col_mapping = {}
                    key_variations = {
                        'exchange': ['exchange', 'exch'],
                        'token': ['token', 'instrumenttoken'],
                        'lotsize': ['lotsize', 'ls'],
                        'symbol': ['symbol', 'sym'], # Underlying symbol or name
                        'tradingsymbol': ['tradingsymbol', 'tsym'], # The actual symbol to trade
                        'instrument': ['instrument', 'instrumenttype', 'instname'], # e.g., EQ, FUT, OPT, IDX
                        'ticksize': ['ticksize', 'pp', 'ticksize'],
                        'expiry': ['expiry', 'expirydate', 'expd'], # Ensure 'expiry' is mapped
                        'optiontype': ['optiontype', 'optt', 'opttype'], # CE/PE
                        'strikeprice': ['strikeprice', 'strike', 'strkprc', 'sp']
                    }

                    found_required = True
                    required_keys = ['exchange', 'token', 'tradingsymbol'] # Minimum required
                    max_index_needed = -1 # Track the highest column index we need
                    for key, variations in key_variations.items():
                        found = False
                        for var in variations:
                            if var in header_map:
                                idx = header_map[var]
                                col_mapping[key] = idx
                                max_index_needed = max(max_index_needed, idx)
                                found = True
                                break
                        if key in required_keys and not found:
                            self.logger.error(f"Missing required column '{key}' (or variations) in file {filename}. Header: {header}")
                            found_required = False
                            break # Stop processing this file
                        # Log if expiry column specifically is missing in derivative files
                        if key == 'expiry' and not found and file_exchange in ['NFO', 'BFO']:
                             self.logger.warning(f"Column 'expiry' (or variations like 'expirydate') not found in derivative file {filename}. Expiry extraction will fail.")
                             # Allow processing to continue, but extraction will likely fail later

                    if not found_required:
                        continue # Skip this file

                    # Process rows
                    rows_processed = 0
                    rows_skipped_exchange = 0
                    rows_skipped_missing_data = 0
                    rows_skipped_malformed = 0
                    rows_added = 0
                    for i, row in enumerate(reader):
                        rows_processed += 1
                        # Check if row has enough columns based on max index needed
                        if len(row) <= max_index_needed:
                            self.logger.debug(f"Skipping malformed row {i+2} in {filename} (length {len(row)} less than required {max_index_needed + 1}): {row}")
                            rows_skipped_malformed += 1
                            continue

                        try:
                            # Extract data using the mapped indices
                            row_exch = row[col_mapping['exchange']].strip().upper()

                            # Only process rows for the determined file exchange
                            if row_exch != file_exchange:
                                rows_skipped_exchange += 1
                                continue

                            token = row[col_mapping['token']].strip()
                            tsym = row[col_mapping['tradingsymbol']].strip()

                            if not all([row_exch, token, tsym]):
                                rows_skipped_missing_data += 1
                                continue

                            # Create cache entry using the trading symbol (tsym) as the primary key
                            cache_key = tsym.upper()
                            entry = {
                                'exch': row_exch,
                                'token': token,
                                'tsym': tsym,
                                # Add other relevant fields if they exist in the file and mapping
                                'symbol': row[col_mapping['symbol']].strip() if 'symbol' in col_mapping else None,
                                'lotsize': row[col_mapping['lotsize']].strip() if 'lotsize' in col_mapping else None,
                                'instrument': row[col_mapping['instrument']].strip() if 'instrument' in col_mapping else None,
                                'ticksize': row[col_mapping['ticksize']].strip() if 'ticksize' in col_mapping else None,
                                'expiry': row[col_mapping['expiry']].strip() if 'expiry' in col_mapping else None, # Store expiry string
                                'optiontype': row[col_mapping['optiontype']].strip() if 'optiontype' in col_mapping else None,
                                'strikeprice': row[col_mapping['strikeprice']].strip() if 'strikeprice' in col_mapping else None,
                            }
                            # Clean up None values and empty strings
                            entry = {k: v for k, v in entry.items() if v is not None and v != ''}

                            caches[row_exch][cache_key] = entry
                            rows_added += 1

                        except IndexError:
                            # This should be caught by the length check above, but keep as safety net
                            self.logger.warning(f"IndexError processing row {i+2} in {filename}. Max index needed: {max_index_needed}, Row length: {len(row)}. Row: {row}")
                            rows_skipped_malformed += 1
                        except (ValueError, KeyError) as e:
                            self.logger.warning(f"Error processing row {i+2} in {filename}: {e} - Row: {row}")
                            rows_skipped_malformed += 1 # Count as malformed

                    self.logger.info(
                        f"Finished processing {filename}. Total rows read: {rows_processed}. Added: {rows_added}. "
                        f"Skipped (Wrong Exch): {rows_skipped_exchange}. Skipped (Missing Data): {rows_skipped_missing_data}. "
                        f"Skipped (Malformed): {rows_skipped_malformed}."
                    )
                    processed_files_count += 1

            except FileNotFoundError:
                 self.logger.error(f"File not found during processing: {file_path}")
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

        if processed_files_count == 0:
             self.logger.error("No symbol files were successfully processed.")
             return None # Return None to indicate failure

        return caches


    def _load_caches(self):
        """Load existing exchange-specific caches from files."""
        self.logger.info("Loading existing caches from files...")
        loaded_count = 0
        for exchange in REQUIRED_EXCHANGES:
            cache_file = self._get_cache_file_path(exchange)
            try:
                if cache_file.exists() and cache_file.stat().st_size > 0: # Check if exists and not empty
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        # Basic validation before assigning
                        loaded_data = json.load(f)
                        if self._validate_cache(loaded_data):
                             self.symbol_caches[exchange] = loaded_data
                             self.logger.info(f"Loaded {exchange} cache with {len(self.symbol_caches[exchange])} symbols from {cache_file.name}")
                             loaded_count += 1
                        else:
                             self.logger.error(f"Invalid cache data found in {cache_file.name}. Discarding.")
                             self.symbol_caches[exchange] = {} # Initialize empty
                else:
                    # Log if file missing or empty
                    if not cache_file.exists():
                         self.logger.info(f"No existing cache file found for {exchange} ({cache_file.name}). Initializing empty cache.")
                    else:
                         self.logger.info(f"Existing cache file for {exchange} is empty ({cache_file.name}). Initializing empty cache.")
                    self.symbol_caches[exchange] = {} # Ensure exchange key exists even if file is missing/empty

            except json.JSONDecodeError as e:
                 self.logger.error(f"Error decoding JSON from cache file {cache_file.name}: {e}. Initializing empty cache for {exchange}.")
                 self.symbol_caches[exchange] = {}
            except Exception as e:
                self.logger.error(f"Error loading cache file {cache_file.name}: {e}. Initializing empty cache for {exchange}.")
                self.symbol_caches[exchange] = {}

        # Load global metadata after loading caches
        self._load_metadata()

        # Check if any essential cache is empty and if a refresh isn't already running
        if not self.symbol_caches.get("NFO") and not self._refresh_lock.locked():
             self.logger.warning("NFO cache is empty after loading. Background thread will attempt refresh.")


    def _load_metadata(self):
        """Loads cache metadata like last refresh date."""
        try:
            if self.cache_metadata_file.exists() and self.cache_metadata_file.stat().st_size > 0:
                with open(self.cache_metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    last_refresh_str = metadata.get('last_refresh_date', '')
                    try:
                        self.last_refresh_date = date.fromisoformat(last_refresh_str) if last_refresh_str else None
                        self.logger.info(f"Loaded last refresh date from metadata: {self.last_refresh_date}")
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid last_refresh_date format in metadata: {last_refresh_str}. Setting to None.")
                        self.last_refresh_date = None
                    # Load other stats if saved
                    self.stats.refresh_count = metadata.get('refresh_count', 0)
                    self.stats.error_count = metadata.get('error_count', 0)
                    last_refresh_time_str = metadata.get('last_refresh_time')
                    if last_refresh_time_str:
                         try:
                              self.stats.last_refresh_time = datetime.fromisoformat(last_refresh_time_str)
                         except (ValueError, TypeError):
                              self.stats.last_refresh_time = None

            else:
                 self.logger.info("Cache metadata file not found or empty. Initializing defaults.")
                 self.last_refresh_date = None


        except json.JSONDecodeError as e:
             self.logger.error(f"Error decoding JSON from metadata file {self.cache_metadata_file.name}: {e}")
             self.last_refresh_date = None
        except Exception as e:
             self.logger.error(f"Error loading metadata file {self.cache_metadata_file.name}: {e}")
             self.last_refresh_date = None


    def _load_expiry_dates(self):
        """Load existing expiry dates from file."""
        try:
            if self.expiry_dates_file.exists() and self.expiry_dates_file.stat().st_size > 0:
                with open(self.expiry_dates_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    # Basic validation
                    if isinstance(loaded_data, dict) and 'indices' in loaded_data and 'version' in loaded_data:
                         # Check version compatibility if needed
                         # if loaded_data.get('version') == "1.4": # Check against current version
                         self.expiry_dates = loaded_data
                         self.logger.info(f"Loaded expiry dates (Version: {self.expiry_dates.get('version')}) for {len(self.expiry_dates.get('indices', {}))} indices")
                         # else:
                         #      self.logger.warning(f"Expiry date file version mismatch (found {loaded_data.get('version')}, expected 1.4). Re-initializing.")
                         #      self._initialize_default_expiry_structure()
                    else:
                         self.logger.error(f"Invalid format in expiry dates file {self.expiry_dates_file.name}. Using default structure.")
                         self._initialize_default_expiry_structure()

            else:
                if not self.expiry_dates_file.exists():
                     self.logger.info(f"No existing expiry dates file found ({self.expiry_dates_file.name}). Using default structure.")
                else:
                     self.logger.info(f"Existing expiry dates file is empty ({self.expiry_dates_file.name}). Using default structure.")
                self._initialize_default_expiry_structure()

        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON from expiry dates file {self.expiry_dates_file.name}: {e}. Using default structure.")
            self._initialize_default_expiry_structure()
        except Exception as e:
            self.logger.error(f"Error loading expiry dates file {self.expiry_dates_file.name}: {e}. Using default structure.")
            self._initialize_default_expiry_structure()

    def _initialize_default_expiry_structure(self):
         """Sets the expiry_dates attribute to a default empty structure."""
         self.expiry_dates = {
            "version": "1.4", # Match version used in extraction
            "last_updated": datetime.now().isoformat(),
            "indices": {}
         }


    def _save_caches(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
        """Save exchange-specific caches to files. Skips saving if cache is empty."""
        self.logger.info("Saving caches to files...")
        saved_count = 0
        for exchange, cache in caches.items():
            cache_file = self._get_cache_file_path(exchange)
            # Only save if cache is not empty
            if cache:
                try:
                    # Ensure directory exists before saving
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(cache, f, ensure_ascii=False, indent=2) # Use indent=2 for smaller files
                    self.logger.info(f"Saved {exchange} cache with {len(cache)} symbols to {cache_file.name}")
                    saved_count += 1
                except Exception as e:
                    self.logger.error(f"Error saving cache for {exchange} to {cache_file.name}: {e}")
            else:
                 self.logger.info(f"Skipping save for empty cache: {exchange}")
                 # Optionally delete existing empty file if desired
                 if cache_file.exists():
                      try:
                           cache_file.unlink()
                           self.logger.info(f"Deleted empty cache file: {cache_file.name}")
                      except OSError as e:
                           self.logger.error(f"Error deleting empty cache file {cache_file.name}: {e}")


        self.logger.info(f"Finished saving caches. Saved {saved_count} non-empty caches.")
        # Metadata is saved separately in _save_metadata


    def _save_metadata(self):
         """Saves cache metadata like last refresh date and stats."""
         try:
              # Ensure directory exists before saving
              self.cache_metadata_file.parent.mkdir(parents=True, exist_ok=True)
              metadata = {
                  'last_refresh_date': self.last_refresh_date.isoformat() if self.last_refresh_date else None,
                  'last_refresh_time': self.stats.last_refresh_time.isoformat() if self.stats.last_refresh_time else None,
                  'refresh_count': self.stats.refresh_count,
                  'error_count': self.stats.error_count,
                  # Add other relevant stats if needed
              }
              with open(self.cache_metadata_file, 'w', encoding='utf-8') as f:
                  json.dump(metadata, f, indent=2)
              self.logger.info(f"Saved cache metadata to {self.cache_metadata_file.name}")
         except Exception as e:
              self.logger.error(f"Error saving cache metadata: {e}")


    def _save_expiry_dates(self):
        """Save expiry dates to file."""
        try:
            # Ensure directory exists before saving
            self.expiry_dates_file.parent.mkdir(parents=True, exist_ok=True)
            # Update last updated time before saving
            self.expiry_dates["last_updated"] = datetime.now().isoformat()
            with open(self.expiry_dates_file, 'w', encoding='utf-8') as f:
                json.dump(self.expiry_dates, f, ensure_ascii=False, indent=2) # Use indent=2
            self.logger.info(f"Saved expiry dates with {len(self.expiry_dates.get('indices', {}))} indices to {self.expiry_dates_file.name}")
        except Exception as e:
            self.logger.error(f"Error saving expiry dates: {e}")


    def _update_stats(self, caches: Dict[str, Dict[str, Dict[str, Any]]]):
         """Update cache statistics based on the current caches."""
         total_symbols = 0
         symbols_by_exchange = defaultdict(int)
         total_size_bytes = 0

         # Ensure caches is a dict before iterating
         if not isinstance(caches, dict):
              self.logger.error("Cannot update stats: caches object is not a dictionary.")
              return

         for exchange, cache in caches.items():
              # Ensure cache is iterable (dict)
              if isinstance(cache, dict):
                   count = len(cache)
                   total_symbols += count
                   symbols_by_exchange[exchange] = count
                   # Estimate cache size from file size
                   try:
                       cache_file = self._get_cache_file_path(exchange)
                       if cache_file.exists():
                            total_size_bytes += cache_file.stat().st_size
                   except Exception as e:
                        self.logger.warning(f"Could not get size for cache file {exchange}: {e}")
              else:
                   self.logger.warning(f"Cache for exchange {exchange} is not a dict, cannot count symbols.")


         # Add size of metadata and expiry files
         try:
             if self.cache_metadata_file.exists():
                  total_size_bytes += self.cache_metadata_file.stat().st_size
             if self.expiry_dates_file.exists():
                  total_size_bytes += self.expiry_dates_file.stat().st_size
         except Exception as e:
              self.logger.warning(f"Could not get size for metadata/expiry files: {e}")


         # Update stats object
         self.stats.total_symbols = total_symbols
         self.stats.symbols_by_exchange = dict(symbols_by_exchange) # Convert back to regular dict
         self.stats.cache_size_bytes = total_size_bytes
         # last_refresh_time, refresh_count, error_count are updated elsewhere


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

        exchange_upper = exchange.upper()
        # Check if exchange is one we are supposed to handle
        if exchange_upper not in REQUIRED_EXCHANGES:
            self.logger.warning(f"Unsupported exchange for cache lookup: {exchange}")
            return None

        # Check if the cache for this required exchange exists and is loaded
        if exchange_upper not in self.symbol_caches:
            # This case should ideally not happen after initialization if REQUIRED_EXCHANGES is correct
            self.logger.error(f"Internal error: Cache key missing for required exchange {exchange_upper}.")
            return None

        exchange_cache = self.symbol_caches.get(exchange_upper)
        # Handle case where exchange is required but cache is empty
        if not exchange_cache: # Checks if dict is empty
            self.logger.warning(f"Symbol cache is empty for {exchange_upper}. Cannot perform cache lookup.")
            return None

        # Use uppercase symbol for lookup (cache keys are stored as uppercase)
        cache_key = symbol.upper()
        self.logger.debug(f"Looking up '{cache_key}' in '{exchange_upper}' cache")

        cached_info = exchange_cache.get(cache_key)

        if cached_info:
            self.logger.debug(f"Found '{symbol}' on '{exchange_upper}' in local cache")
            return cached_info.copy()  # Return a copy to prevent modification of cache
        else:
            self.logger.debug(f"'{symbol}' on '{exchange_upper}' not found in local cache")
            return None

    # --- Expiry Date Interface Methods ---

    def get_all_expiry_dates(self, index_name: str) -> Dict[str, List[str]]:
        """
        Get all stored expiry dates for a specific index, classified as weekly/monthly.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.

        Returns:
            Dictionary with "weekly" and "monthly" expiry date lists (YYYY-MM-DD format),
            sorted chronologically. Returns empty lists if index not found or no dates.
        """
        index_name_upper = index_name.upper()
        # Check if expiry data structure is valid
        if not isinstance(self.expiry_dates, dict) or 'indices' not in self.expiry_dates:
            self.logger.warning("Expiry dates data structure is invalid or missing 'indices' key.")
            return {"weekly": [], "monthly": []}

        indices = self.expiry_dates.get('indices', {})
        # Check if data for the specific index exists
        index_expiries = indices.get(index_name_upper)

        if not index_expiries or not isinstance(index_expiries, dict):
            self.logger.warning(f"No expiry dates found or data invalid for index: {index_name}")
            return {"weekly": [], "monthly": []}

        # Ensure weekly and monthly keys exist and are lists of strings
        weekly_list = index_expiries.get("weekly", [])
        monthly_list = index_expiries.get("monthly", [])

        # Validate and clean the lists (ensure they are lists and contain valid date strings)
        valid_weekly = sorted([d for d in weekly_list if isinstance(d, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', d)])
        valid_monthly = sorted([d for d in monthly_list if isinstance(d, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', d)])

        if len(valid_weekly) != len(weekly_list):
             self.logger.warning(f"Some invalid entries found/removed from weekly expiry list for {index_name}")
        if len(valid_monthly) != len(monthly_list):
             self.logger.warning(f"Some invalid entries found/removed from monthly expiry list for {index_name}")

        return {
            "weekly": valid_weekly,
            "monthly": valid_monthly
        }

    # --- NEW METHOD: Get combined list of all expiry dates ---
    def get_combined_expiry_dates(self, index_name: str) -> List[str]:
        """
        Get a single sorted list of all stored expiry dates (weekly and monthly)
        for a specific index.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.

        Returns:
            A single sorted list of all expiry dates (YYYY-MM-DD format),
            or an empty list if index not found or no dates.
        """
        # Call the existing method to get the classified dates
        classified_dates = self.get_all_expiry_dates(index_name)

        # Combine the lists
        combined_list = classified_dates.get("weekly", []) + classified_dates.get("monthly", [])

        # Ensure uniqueness (though classification logic should handle this) and sort
        # Use set for uniqueness then convert back to sorted list
        unique_sorted_list = sorted(list(set(combined_list)))

        return unique_sorted_list
    # --- End New Method ---


    def get_nearest_expiry(self, index_name: str, expiry_type: str = "weekly") -> Optional[str]:
        """
        Get the nearest upcoming expiry date for an index (on or after today)
        based on the pre-classified weekly/monthly lists.

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            expiry_type: Type of expiry ("weekly" or "monthly"). Case-insensitive.

        Returns:
            ISO format date string (YYYY-MM-DD) of nearest upcoming expiry, or None if not found.
        """
        index_name_upper = index_name.upper()
        expiry_type_lower = expiry_type.lower()

        if expiry_type_lower not in ["weekly", "monthly"]:
            self.logger.error(f"Invalid expiry type: {expiry_type}. Must be 'weekly' or 'monthly'.")
            return None

        # Directly access the pre-classified data
        if not isinstance(self.expiry_dates, dict) or 'indices' not in self.expiry_dates:
            self.logger.warning("Expiry dates data structure is invalid or missing 'indices' key.")
            return None
        indices = self.expiry_dates.get('indices', {})
        index_expiries = indices.get(index_name_upper)
        if not index_expiries or not isinstance(index_expiries, dict):
            self.logger.warning(f"No expiry dates found or data invalid for index: {index_name}")
            return None

        # Get the specific list (weekly or monthly)
        dates_list = index_expiries.get(expiry_type_lower, [])

        # Validate the list content
        valid_dates = sorted([d for d in dates_list if isinstance(d, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', d)])

        if not valid_dates:
            self.logger.warning(f"No valid {expiry_type_lower} expiry dates found for {index_name_upper} after validation.")
            return None

        today_str = date.today().isoformat()

        # Find the first date that is greater than or equal to today
        for expiry in valid_dates:
            if expiry >= today_str:
                return expiry

        # If all dates are in the past, return None
        self.logger.warning(f"All stored {expiry_type_lower} expiries for {index_name_upper} are in the past.")
        return None

    def get_expiry_by_offset(self, index_name: str, offset: int = 0, expiry_type: str = "weekly") -> Optional[str]:
        """
        Get expiry date by offset (0-based index) from the list of upcoming
        pre-classified weekly or monthly expiries (on or after today).

        Args:
            index_name: Name of the index (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            offset: Number of expiries to skip (0 = nearest, 1 = next, etc.)
            expiry_type: Type of expiry ("weekly" or "monthly"). Case-insensitive.

        Returns:
            ISO format date string (YYYY-MM-DD) of expiry at given offset, or None if not found or offset out of bounds.
        """
        index_name_upper = index_name.upper()
        expiry_type_lower = expiry_type.lower()

        if expiry_type_lower not in ["weekly", "monthly"]:
            self.logger.error(f"Invalid expiry type: {expiry_type}. Must be 'weekly' or 'monthly'.")
            return None

        if not isinstance(offset, int) or offset < 0:
            self.logger.error(f"Offset must be a non-negative integer: {offset}")
            return None

        # Directly access the pre-classified data
        if not isinstance(self.expiry_dates, dict) or 'indices' not in self.expiry_dates:
            self.logger.warning("Expiry dates data structure is invalid or missing 'indices' key.")
            return None
        indices = self.expiry_dates.get('indices', {})
        index_expiries = indices.get(index_name_upper)
        if not index_expiries or not isinstance(index_expiries, dict):
            self.logger.warning(f"No expiry dates found or data invalid for index: {index_name}")
            return None

        # Get the specific list (weekly or monthly)
        dates_list = index_expiries.get(expiry_type_lower, [])

        # Validate the list content
        valid_dates = sorted([d for d in dates_list if isinstance(d, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', d)])

        if not valid_dates:
            self.logger.warning(f"No valid {expiry_type_lower} expiry dates found for {index_name_upper} after validation.")
            return None

        today_str = date.today().isoformat()
        # Filter for dates on or after today
        future_expiries = [d for d in valid_dates if d >= today_str]

        if not future_expiries:
            self.logger.warning(f"No future {expiry_type_lower} expiry dates found for {index_name_upper}")
            return None

        if offset >= len(future_expiries):
            self.logger.warning(f"Offset {offset} exceeds available future expiries ({len(future_expiries)}) for {index_name_upper} ({expiry_type_lower})")
            return None

        return future_expiries[offset]

    # --- UPDATE 2: Replace with user's construct_trading_symbol and add helper ---

    def format_expiry_for_symbol(self, expiry_date_str: str) -> Optional[str]:
        """
        Formats a YYYY-MM-DD date string into DDMMMYY format for Finvasia symbols.

        Args:
            expiry_date_str: Date string in YYYY-MM-DD format.

        Returns:
            Formatted date string (e.g., "28JUL22") or None if input is invalid.
        """
        if not expiry_date_str:
            return None
        try:
            expiry_dt = datetime.strptime(expiry_date_str, "%Y-%m-%d")
            # Format as DDMMMYY (uppercase month, 2-digit year)
            formatted = expiry_dt.strftime("%d%b%y").upper()
            return formatted
        except (ValueError, TypeError):
            self.logger.error(f"Invalid expiry date format or type for symbol construction: {expiry_date_str}. Expected YYYY-MM-DD string.")
            return None

    def construct_trading_symbol(
        self,
        name: str,
        expiry_date_str: str, # Expects YYYY-MM-DD
        instrument_type: str, # FUT, OPT
        option_type: Optional[str] = None, # P, C
        strike: Optional[Union[int, float, str]] = None,
        exchange: str = ""
    ) -> Optional[str]:
        """Construct trading symbol based on Finvasia's naming convention."""
        name = name.upper()
        instrument_type = instrument_type.upper()
        # Use the helper method to get DDMMMYY format
        formatted_expiry = self.format_expiry_for_symbol(expiry_date_str)

        if not formatted_expiry:
            return None # Error logged in helper

        if instrument_type == 'FUT':
            # Example: BANKNIFTY25AUG22F
            return f"{name}{formatted_expiry}F"

        elif instrument_type == 'OPT':
            if not option_type or strike is None:
                 self.logger.error("Option type and strike price are required for OPT instrument.")
                 return None

            option_type = option_type.upper()

            # --- BFO Exchange Specific Format ---
            if exchange == 'BFO':
                # Format: NAME<YY><Month><DD><Strike><CE/PE>
                # Example: SENSEX2561073400CE (SENSEX, 2025 June 10th, Strike 73400, Call)
                try:
                    dt_obj = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                    yy = dt_obj.strftime('%y') # Year as 2 digits (e.g., 25)
                    month = str(dt_obj.month) # Month as number (e.g., 6)
                    dd = dt_obj.strftime('%d') # Day as 2 digits (e.g., 10)
                except ValueError:
                    self.logger.error(f"Invalid expiry date format for BFO: {expiry_date_str}. Expected YYYY-MM-DD.")
                    return None
                except Exception as e:
                     self.logger.error(f"Error parsing expiry date {expiry_date_str} for BFO: {e}")
                     return None

                # Validate and format option type to CE/PE
                option_type_bfo = option_type.upper()
                if option_type_bfo == 'C':
                    option_type_bfo = 'CE'
                elif option_type_bfo == 'P':
                    option_type_bfo = 'PE'
                elif option_type_bfo not in ['CE', 'PE']:
                    self.logger.error(f"Invalid option type for BFO: {option_type}. Expected P/PE or C/CE.")
                    return None

                # Validate and format strike price (BFO expects integer)
                try:
                    strike_int = int(float(strike)) # Convert potential float/str to int
                    strike_str_int = str(strike_int)
                except (ValueError, TypeError):
                    self.logger.error(f"Invalid strike price format for BFO: {strike}. Expected number convertible to integer.")
                    return None

                return f"{name}{yy}{month}{dd}{strike_str_int}{option_type_bfo}"

            # Finvasia uses P or C (not PE/CE) in the symbol examples provided
            if option_type not in ['P', 'C']:
                 # Accept PE/CE as input but convert to P/C for symbol
                 if option_type == 'PE':
                      option_type = 'P'
                 elif option_type == 'CE':
                      option_type = 'C'
                 else:
                      self.logger.error(f"Invalid option type: {option_type}. Expected P/PE or C/CE.")
                      return None

            try:
                # Format strike: remove .0 if integer, otherwise keep float/string
                strike_float = float(strike)
                if strike_float == int(strike_float):
                    strike_str = str(int(strike_float))
                else:
                    # Keep original precision if float
                    strike_str = str(strike_float)
            except (ValueError, TypeError):
                self.logger.error(f"Invalid strike price format or type: {strike}. Expected number.")
                return None

            # Example: BANKNIFTY02AUG22P8150
            return f"{name}{formatted_expiry}{option_type}{strike_str}"

        else:
            # Handle Equity Spot case based on documentation: RELIANCE-EQ
            if instrument_type == 'EQ':
                 return f"{name}-EQ" # Assuming '-' is correct, adjust if needed

            self.logger.error(f"Invalid or unhandled instrument type: {instrument_type}. Expected FUT, OPT, or EQ.")
            return None

    def is_expiry_data_available(self) -> bool:
        """Check if expiry data is available for any tracked index."""
        return isinstance(self.expiry_dates, dict) and 'indices' in self.expiry_dates and bool(self.expiry_dates['indices'])

    def refresh_expiry_dates(self) -> bool:
        """Force a refresh of expiry dates from the currently loaded symbol caches."""
        self.logger.info("Force refreshing expiry dates from current caches.")
        # Check if caches are loaded before attempting refresh
        if not self.symbol_caches or not any(cache for cache in self.symbol_caches.values() if cache): # Check if any cache is non-empty
             self.logger.error("Cannot refresh expiry dates: Symbol caches are not loaded or all are empty.")
             return False
        try:
            self._extract_expiry_dates(self.symbol_caches)
            self._save_expiry_dates()
            self.logger.info("Expiry dates force refresh completed.")
            return True
        except Exception as e:
            self.logger.error(f"Error force refreshing expiry dates: {e}", exc_info=True)
            return False

    def stop(self):
        """Stop the background refresh thread."""
        self.logger.info("Stopping background refresh thread...")
        self._is_running = False
        # Check if thread was actually started
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5.0) # Wait for up to 5 seconds
            if self._refresh_thread.is_alive():
                self.logger.warning("Background refresh thread did not stop within timeout.")
            else:
                 self.logger.info("Background refresh thread stopped successfully.")
        else:
             self.logger.info("Background refresh thread was not running or already stopped.")


# Example usage (for testing purposes)
if __name__ == "__main__":
    # Setup logging (ensure handlers are not added multiple times if run repeatedly)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_formatter)

    # Configure specific logger instead of root to avoid conflicts if used as library
    cache_logger = logging.getLogger("SymbolCache")
    if not cache_logger.hasHandlers(): # Add handler only if not already configured
        cache_logger.addHandler(log_handler)
    cache_logger.setLevel(logging.DEBUG) # Set specific logger level for detailed output

    # Get the singleton instance
    cache = SymbolCache()

    # --- Allow time for initial load/refresh if needed ---
    print("\nWaiting for initial cache refresh attempt (max 15 seconds)...")
    start_wait = time.time()
    while cache._refresh_lock.locked() and (time.time() - start_wait) < 15:
         time.sleep(0.5)
    if cache._refresh_lock.locked():
         print("Initial refresh still running after 15s, proceeding anyway...")
    else:
         print("Initial refresh attempt complete or wasn't needed.")


    # --- Test Symbol Lookup ---
    print("\n--- Testing Symbol Lookup ---")
    reliance_nse = cache.lookup_symbol("RELIANCE", "NSE")
    if reliance_nse: print(f"Found RELIANCE on NSE: {reliance_nse}")
    else: print("RELIANCE on NSE not found in cache.")

    # Use Finvasia format for lookup
    nifty_fut_nfo = cache.lookup_symbol("NIFTY25APR25F", "NFO") # DDMMMYYF format
    if nifty_fut_nfo: print(f"Found NIFTY25APR25F on NFO: {nifty_fut_nfo}")
    else: print("NIFTY25APR25F on NFO not found in cache. (Check NFO_symbols cache or master file)")

    # Example BFO symbol lookup (using Finvasia format)
    # SENSEX expiring April 26, 2025 (Friday), 74000 CE -> SENSEX26APR25C74000
    sensex_opt_bfo = cache.lookup_symbol("SENSEX26APR25C74000", "BFO")
    if sensex_opt_bfo: print(f"Found SENSEX26APR25C74000 on BFO: {sensex_opt_bfo}")
    else: print("SENSEX26APR25C74000 on BFO not found in cache. (Check BFO_symbols cache or master file)")


    sensex_bse_index = cache.lookup_symbol("SENSEX", "BSE")
    if sensex_bse_index: print(f"Found SENSEX on BSE: {sensex_bse_index}")
    else: print("SENSEX on BSE not found in cache. (Check BSE_symbols.txt processing)")

    invalid_symbol = cache.lookup_symbol("NONEXISTENTXYZ", "NSE")
    if not invalid_symbol: print("NONEXISTENTXYZ on NSE correctly not found.")

    invalid_exchange = cache.lookup_symbol("RELIANCE", "MCX") # MCX is not in REQUIRED_EXCHANGES
    if not invalid_exchange: print("RELIANCE on MCX correctly not found (unsupported exchange).")

    # --- Test Expiry Date Interface ---
    print("\n--- Testing Expiry Date Interface (NFO:NIFTY) ---")
    nifty_expiries = cache.get_all_expiry_dates("NIFTY")
    print(f"All NIFTY weekly expiries (NFO): {nifty_expiries.get('weekly', [])}")
    print(f"All NIFTY monthly expiries (NFO): {nifty_expiries.get('monthly', [])}")
    # --- Test NEW get_combined_expiry_dates ---
    nifty_combined = cache.get_combined_expiry_dates("NIFTY")
    print(f"All NIFTY combined expiries (NFO): {nifty_combined}")
    # --- End Test NEW ---
    nearest_nifty_weekly = cache.get_nearest_expiry("NIFTY", "weekly")
    print(f"Nearest NIFTY weekly expiry (NFO): {nearest_nifty_weekly}")
    nearest_nifty_monthly = cache.get_nearest_expiry("NIFTY", "monthly")
    print(f"Nearest NIFTY monthly expiry (NFO): {nearest_nifty_monthly}")
    first_nifty_weekly = cache.get_expiry_by_offset("NIFTY", 0, "weekly")
    print(f"First upcoming NIFTY weekly expiry (NFO, offset 0): {first_nifty_weekly}")
    second_nifty_monthly = cache.get_expiry_by_offset("NIFTY", 1, "monthly")
    print(f"Second upcoming NIFTY monthly expiry (NFO, offset 1): {second_nifty_monthly}")
    expiry_out_of_bounds = cache.get_expiry_by_offset("NIFTY", 99, "weekly")
    print(f"NIFTY weekly expiry (NFO, offset 99): {expiry_out_of_bounds} (Expected None if < 100 future expiries)")

    print("\n--- Testing Expiry Date Interface (BFO:SENSEX) ---")
    sensex_expiries = cache.get_all_expiry_dates("SENSEX")
    print(f"All SENSEX weekly expiries (BFO): {sensex_expiries.get('weekly', [])}")
    print(f"All SENSEX monthly expiries (BFO): {sensex_expiries.get('monthly', [])}")
    # --- Test NEW get_combined_expiry_dates ---
    sensex_combined = cache.get_combined_expiry_dates("SENSEX")
    print(f"All SENSEX combined expiries (BFO): {sensex_combined}")
    # --- End Test NEW ---
    nearest_sensex_weekly = cache.get_nearest_expiry("SENSEX", "weekly")
    print(f"Nearest SENSEX weekly expiry (BFO): {nearest_sensex_weekly}")
    nearest_sensex_monthly = cache.get_nearest_expiry("SENSEX", "monthly")
    print(f"Nearest SENSEX monthly expiry (BFO): {nearest_sensex_monthly}")
    first_sensex_weekly = cache.get_expiry_by_offset("SENSEX", 0, "weekly")
    print(f"First upcoming SENSEX weekly expiry (BFO, offset 0): {first_sensex_weekly}")

    print("\n--- Testing Expiry Date Interface (BFO:BANKEX) ---")
    bankex_expiries = cache.get_all_expiry_dates("BANKEX")
    print(f"All BANKEX weekly expiries (BFO): {bankex_expiries.get('weekly', [])}")
    print(f"All BANKEX monthly expiries (BFO): {bankex_expiries.get('monthly', [])}")
    # --- Test NEW get_combined_expiry_dates ---
    bankex_combined = cache.get_combined_expiry_dates("BANKEX")
    print(f"All BANKEX combined expiries (BFO): {bankex_combined}")
    # --- End Test NEW ---
    nearest_bankex_weekly = cache.get_nearest_expiry("BANKEX", "weekly")
    print(f"Nearest BANKEX weekly expiry (BFO): {nearest_bankex_weekly}")

    # --- Test Symbol Construction (Using new method) ---
    print("\n--- Testing Symbol Construction (Finvasia Format) ---")
    test_expiry_date_nfo = nearest_nifty_weekly
    test_expiry_date_bfo = nearest_sensex_weekly # Use weekly if available

    if test_expiry_date_nfo:
        print(f"\nUsing NIFTY expiry {test_expiry_date_nfo} for NFO construction tests...")
        nifty_fut_symbol_check = cache.construct_trading_symbol("NIFTY", test_expiry_date_nfo, "FUT")
        print(f"Constructed NIFTY Future symbol: {nifty_fut_symbol_check}") # Expect NIFTYDDMMMYYF
        nifty_call_symbol = cache.construct_trading_symbol("NIFTY", test_expiry_date_nfo, "OPT", "CE", 22500)
        print(f"Constructed NIFTY Call symbol (Strike 22500): {nifty_call_symbol}") # Expect NIFTYDDMMMYYC22500
        nifty_put_symbol = cache.construct_trading_symbol("NIFTY", test_expiry_date_nfo, "OPT", "P", 22000.5)
        print(f"Constructed NIFTY Put symbol (Strike 22000.5): {nifty_put_symbol}") # Expect NIFTYDDMMMYYP22000.5
    else:
        print("\nSkipping NFO construction tests (nearest NIFTY weekly expiry not found).")

    # Use monthly if weekly not found for BFO test
    if not test_expiry_date_bfo:
        test_expiry_date_bfo = nearest_sensex_monthly

    if test_expiry_date_bfo:
        print(f"\nUsing SENSEX expiry {test_expiry_date_bfo} for BFO construction tests...")
        sensex_fut_symbol = cache.construct_trading_symbol("SENSEX", test_expiry_date_bfo, "FUT")
        print(f"Constructed SENSEX Future symbol: {sensex_fut_symbol}") # Expect SENSEXDDMMMYYF
        sensex_call_symbol = cache.construct_trading_symbol("SENSEX", test_expiry_date_bfo, "OPT", "C", 74000, "BFO")
        print(f"Constructed SENSEX Call symbol (Strike 74000): {sensex_call_symbol}") # Expect SENSEXDDMMMYYC74000
        sensex_put_symbol = cache.construct_trading_symbol("SENSEX", test_expiry_date_bfo, "OPT", "PE", 73500, "BFO")
        print(f"Constructed SENSEX Put symbol (Strike 73500): {sensex_put_symbol}") # Expect SENSEXDDMMMYYP73500
    else:
        print("\nSkipping BFO construction tests (nearest SENSEX expiry not found).")

    # Test Equity construction
    print("\nTesting Equity Construction...")
    reliance_eq_symbol = cache.construct_trading_symbol("RELIANCE", None, "EQ") # Expiry not needed
    print(f"Constructed RELIANCE Equity symbol: {reliance_eq_symbol}") # Expect RELIANCE-EQ


    # Get stats
    print("\n--- Cache Statistics ---")
    print(cache.stats)

    # Stop the background thread before exiting
    print("\nStopping cache background thread...")
    cache.stop()
    print("SymbolCache stopped.")
