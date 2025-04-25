import logging
import os
import pyotp
import time
import csv
import threading
import uuid
import requests # Added for downloading masters
import zipfile  # Added for extracting masters
import json     # Added for storing cache
import math     # Added for rounding strikes
from typing import Dict, List, Optional, Any, Tuple, Callable, Union
from datetime import datetime, date # Added date
import re
from pathlib import Path
import shutil   # Added for removing directories

# Import NorenApi from the Finvasia library
try:
    # Ensure you have a recent and stable version of NorenRestApiPy installed
    # pip install --upgrade NorenRestApiPy
    from NorenRestApiPy.NorenApi import NorenApi
except ImportError:
    raise ImportError("NorenRestApiPy package not found. Please install it using: pip install NorenRestApiPy requests")

# --- Configuration ---
CACHE_DIR = Path("symbol_cache")
CACHE_FILE = CACHE_DIR / "symbol_cache.json"
CACHE_METADATA_FILE = CACHE_DIR / "cache_metadata.json"
MASTERS_URL_ROOT = 'https://api.shoonya.com/'
# Ensure all relevant master files are listed
MASTER_FILES = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip', 'BSE_symbols.txt.zip', 'BFO_symbols.txt.zip', 'MCX_symbols.txt.zip', 'CDS_symbols.txt.zip']

# --- Logger Setup ---
def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create and configure a logger instance."""
    logger_instance = logging.getLogger(name)
    # Prevent adding multiple handlers if called again
    if not logger_instance.handlers:
        logger_instance.propagate = False # Prevent root logger from handling messages too
        # File Handler for detailed logs
        log_file = Path("logs") / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8') # Specify encoding
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(threadName)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger_instance.addHandler(file_handler)

        # Console Handler for INFO level messages
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO) # Show only INFO and above on console
        logger_instance.addHandler(console_handler)

        logger_instance.setLevel(level) # Set the overall minimum level for the logger
    # Adjust level if requested level is lower than current
    elif logger_instance.level > level:
         logger_instance.setLevel(level)

    return logger_instance

logger = get_logger("FinvasiaClient", level=logging.INFO) # Use a central logger, set level (DEBUG for more detail)

# --- Symbol Cache Management ---

def download_and_extract_masters(target_dir: Path) -> List[Path]:
    """
    Downloads master symbol zip files, extracts them, and cleans up zip files.

    Args:
        target_dir: The directory to extract the files into.

    Returns:
        A list of paths to the extracted text files.
    """
    extracted_files = []
    target_dir.mkdir(parents=True, exist_ok=True)

    for zip_file_name in MASTER_FILES:
        logger.info(f'Downloading {zip_file_name}...')
        url = MASTERS_URL_ROOT + zip_file_name
        zip_file_path = target_dir / zip_file_name

        try:
            # Added stream=True for potentially large files and timeout
            r = requests.get(url, allow_redirects=True, timeout=120, stream=True)
            r.raise_for_status() # Check for download errors (4xx or 5xx)

            with open(zip_file_path, 'wb') as f:
                 # Write content in chunks
                 for chunk in r.iter_content(chunk_size=8192):
                     f.write(chunk)
            logger.info(f"Downloaded {zip_file_name} to {zip_file_path}")

            # Extract the contents
            try:
                with zipfile.ZipFile(zip_file_path) as z:
                    extracted_count = 0
                    for member in z.namelist():
                        # Ensure extraction path is within the target directory
                        # Basic check to prevent path traversal (though ZipFile usually handles it)
                        if member.startswith('/') or '..' in member:
                             logger.warning(f"Skipping potentially unsafe path in zip: {member}")
                             continue
                        member_path = target_dir / Path(member).name
                        z.extract(member, path=target_dir)
                        extracted_files.append(member_path)
                        extracted_count += 1
                        logger.info(f"Extracted '{member}' to {target_dir}")
                    if extracted_count == 0:
                        logger.warning(f"Zip file {zip_file_name} was empty.")

            except zipfile.BadZipFile:
                logger.error(f"Invalid or corrupted zip file: {zip_file_name}")
            except Exception as e:
                logger.error(f"Error extracting {zip_file_name}: {e}", exc_info=True)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {zip_file_name}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during download/extract of {zip_file_name}: {e}", exc_info=True)
        finally:
            # Clean up the zip file
            if zip_file_path.exists():
                try:
                    os.remove(zip_file_path)
                    logger.debug(f'Removed zip file: {zip_file_path}')
                except OSError as e:
                    logger.error(f"Error removing zip file {zip_file_path}: {e}")

    return extracted_files

def load_symbols_to_cache(file_paths: List[Path]) -> Dict[str, Dict[str, Any]]:
    """
    Loads symbol data from extracted text files into a dictionary cache.
    Assumes a CSV format for the text files, attempts to handle headers robustly.

    Args:
        file_paths: List of paths to the extracted .txt symbol files.

    Returns:
        A dictionary mapping "Exchange:TradingSymbol" to token info dictionary.
        Example: {'NFO:NIFTY25APR25FUT': {'token': '12345', 'exch': 'NFO', 'tsym': 'NIFTY25APR25FUT', ...}}
    """
    symbol_cache = {}
    # Define common variations of required header names (case-insensitive)
    header_variations = {
        'exchange': ['exchange', 'exch'],
        'token': ['token', 'instrumenttoken', 'instrument_token'],
        'tradingsymbol': ['tradingsymbol', 'tsym', 'symbol', 'instrumentidentifier'],
        # Optional but useful headers
        'symbol': ['symbol', 'undsymbol', 'name'], # Base symbol name
        'lotsize': ['lotsize', 'ls'],
        'expiry': ['expiry', 'expd', 'expirydate'],
        'instrument': ['instrumenttype', 'instrument', 'instname'],
        'optiontype': ['optiontype', 'optt', 'option_type'],
        'strikeprice': ['strikeprice', 'strike', 'strkprc', 'sp'],
        'ticksize': ['ticksize', 'ti']
    }
    required_keys = {'exchange', 'token', 'tradingsymbol'}

    for file_path in file_paths:
        # Deduce exchange from filename if possible, as fallback
        deduced_exchange = None
        filename_upper = file_path.name.upper()
        if "NFO_" in filename_upper: deduced_exchange = "NFO"
        elif "NSE_" in filename_upper: deduced_exchange = "NSE"
        elif "BSE_" in filename_upper: deduced_exchange = "BSE"
        elif "BFO_" in filename_upper: deduced_exchange = "BFO"
        elif "MCX_" in filename_upper: deduced_exchange = "MCX"
        elif "CDS_" in filename_upper: deduced_exchange = "CDS"

        logger.info(f"Processing symbol file: {file_path} (Deduced Exchange: {deduced_exchange})")
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f: # Ignore encoding errors
                reader = csv.reader(f)
                try:
                    header_row = next(reader)
                except StopIteration:
                    logger.warning(f"File is empty or has no header: {file_path.name}")
                    continue

                header = [h.strip().lower() for h in header_row] # Normalize header
                logger.debug(f"Detected header in {file_path.name}: {header}")

                # Find column indices for required and optional fields
                col_indices = {}
                found_required = set()
                all_cols_map = {} # Map standard key to index

                for std_key, variations in header_variations.items():
                    for variation in variations:
                        try:
                            idx = header.index(variation)
                            col_indices[std_key] = idx
                            all_cols_map[std_key] = idx # Map the standard key
                            if std_key in required_keys:
                                found_required.add(std_key)
                            break # Found the first variation for this standard key
                        except ValueError:
                            continue # Variation not in header

                # Check if all required headers were found
                missing_required = required_keys - found_required
                if missing_required:
                    logger.error(f"Missing required headers {missing_required} in {file_path.name}. Skipping file.")
                    continue

                # Add indices for any remaining headers not in variations map
                # Ensure we don't overwrite already mapped standard keys
                mapped_header_names = {h for vars_list in header_variations.values() for h in vars_list}
                for i, h_name in enumerate(header):
                    if h_name not in mapped_header_names:
                        # Check if this index was already claimed by a standard key mapping
                        is_claimed = False
                        for std_key, mapped_idx in all_cols_map.items():
                            if mapped_idx == i:
                                is_claimed = True
                                break
                        if not is_claimed:
                             all_cols_map[h_name] = i # Use original header name if not mapped

                row_count = 0
                skipped_rows = 0
                for i, row in enumerate(reader):
                    # Basic check for row length consistency
                    if len(row) < len(header):
                        # Log less frequently to avoid flooding logs for large files with footer/bad rows
                        if skipped_rows < 10 or skipped_rows % 1000 == 0:
                             logger.warning(f"Skipping row {i+2} in {file_path.name}: Expected {len(header)} columns, got {len(row)}. Row: {row[:50]}...") # Log only start of row
                        skipped_rows += 1
                        continue
                    try:
                        # Extract required fields
                        exch = row[col_indices['exchange']].strip().upper()
                        token = row[col_indices['token']].strip()
                        tsym = row[col_indices['tradingsymbol']].strip()

                        # Use deduced exchange if column is empty/invalid
                        if not exch or len(exch) > 5: # Basic validation
                             if deduced_exchange:
                                 exch = deduced_exchange
                             else:
                                 if skipped_rows < 10 or skipped_rows % 1000 == 0:
                                     logger.warning(f"Skipping row {i+2} in {file_path.name}: Invalid/missing exchange and couldn't deduce. Row: {row[:50]}...")
                                 skipped_rows += 1
                                 continue

                        # Basic validation for token and tsym
                        if not token or not token.isdigit():
                            if skipped_rows < 10 or skipped_rows % 1000 == 0:
                                logger.warning(f"Skipping row {i+2} in {file_path.name}: Invalid or missing token. Row: {row[:50]}...")
                            skipped_rows += 1
                            continue
                        if not tsym:
                            if skipped_rows < 10 or skipped_rows % 1000 == 0:
                                logger.warning(f"Skipping row {i+2} in {file_path.name}: Missing TradingSymbol. Row: {row[:50]}...")
                            skipped_rows += 1
                            continue

                        # Build the token info dictionary using standard keys where possible
                        token_info = {}
                        for key, idx in all_cols_map.items():
                             if idx < len(row): # Ensure index is valid for the row
                                 token_info[key] = row[idx].strip()
                             else:
                                 token_info[key] = "" # Assign empty string if index out of bounds

                        # Ensure standardized keys are present
                        token_info['exch'] = exch
                        token_info['token'] = token
                        token_info['tsym'] = tsym # Store the original TradingSymbol case

                        # Construct the primary cache key (uppercase for consistency in lookup)
                        cache_key = f"{exch}:{tsym.upper()}"

                        # Add to cache (overwrite if duplicate key found, last one wins)
                        symbol_cache[cache_key] = token_info
                        row_count += 1

                    except IndexError:
                         if skipped_rows < 10 or skipped_rows % 1000 == 0:
                             logger.warning(f"Skipping row {i+2} in {file_path.name}: Index error likely due to inconsistent columns. Row: {row[:50]}...")
                         skipped_rows += 1
                    except Exception as e:
                         logger.error(f"Error processing row {i+2} in {file_path.name}: {row[:50]}... - {e}")
                         skipped_rows += 1

                logger.info(f"Loaded {row_count} symbols (skipped {skipped_rows}) from {file_path.name}")

        except FileNotFoundError:
            logger.error(f"Symbol file not found: {file_path}")
        except Exception as e:
            logger.error(f"Failed to read or process symbol file {file_path}: {e}", exc_info=True)

    logger.info(f"Total symbols loaded into cache: {len(symbol_cache)}")
    return symbol_cache

def get_symbol_cache(force_refresh: bool = False) -> Tuple[Optional[Dict[str, Dict[str, Any]]], Optional[date]]:
    """
    Gets the symbol cache, refreshing if necessary (daily or forced).

    Args:
        force_refresh: If True, forces a download and reload of the cache.

    Returns:
        A tuple containing:
        - The symbol cache dictionary (or None if loading failed).
        - The date the cache was last updated (or None).
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today()
    cache_data = None
    cache_date = None

    # Check metadata for last update date
    if CACHE_METADATA_FILE.exists() and not force_refresh:
        try:
            with open(CACHE_METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                cache_date_str = metadata.get('last_update_date')
                if cache_date_str:
                    cache_date = date.fromisoformat(cache_date_str)
        except (json.JSONDecodeError, ValueError, FileNotFoundError, TypeError) as e:
            logger.warning(f"Could not read cache metadata ({CACHE_METADATA_FILE}): {e}. Will refresh cache.")
            cache_date = None # Force refresh if metadata is invalid

    # Decide if refresh is needed
    needs_refresh = force_refresh or cache_date is None or cache_date < today

    if needs_refresh:
        logger.info(f"Refreshing symbol cache (Force={force_refresh}, Cache Date={cache_date}, Today={today})")
        # Create a temporary directory for downloads
        temp_download_dir = CACHE_DIR / f"temp_download_{uuid.uuid4()}"
        try:
            extracted_files = download_and_extract_masters(temp_download_dir)
            if extracted_files:
                cache_data = load_symbols_to_cache(extracted_files)
                if cache_data:
                    # Save the new cache
                    try:
                        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                            json.dump(cache_data, f, ensure_ascii=False) # Use ensure_ascii=False for broader compatibility
                        logger.info(f"Symbol cache saved to {CACHE_FILE}")
                        # Update metadata
                        cache_date = today
                        with open(CACHE_METADATA_FILE, 'w', encoding='utf-8') as f:
                            json.dump({'last_update_date': cache_date.isoformat()}, f)
                        logger.info(f"Cache metadata updated with date {cache_date}")
                    except IOError as e:
                        logger.error(f"Error writing cache file or metadata: {e}")
                        cache_data = None # Invalidate cache if save failed
                        cache_date = None
                else:
                    logger.error("Failed to load symbols into cache after download.")
                    cache_data = None # Ensure cache is None if loading failed
                    cache_date = None
            else:
                logger.error("No symbol files were extracted. Cache refresh failed.")
                cache_data = None
                cache_date = None
        finally:
            # Clean up temporary download directory
            if temp_download_dir.exists():
                try:
                    shutil.rmtree(temp_download_dir)
                    logger.debug(f"Removed temporary download directory: {temp_download_dir}")
                except OSError as e:
                    logger.error(f"Error removing temporary directory {temp_download_dir}: {e}")

    elif CACHE_FILE.exists():
        logger.info(f"Loading existing symbol cache from {CACHE_FILE} (Date: {cache_date})")
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            logger.info(f"Successfully loaded {len(cache_data)} symbols from cache file.")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Failed to load cache file {CACHE_FILE}: {e}. Triggering refresh.")
            # Recursive call to force refresh if loading fails
            return get_symbol_cache(force_refresh=True)
        except Exception as e:
             logger.error(f"An unexpected error occurred loading cache file {CACHE_FILE}: {e}. Triggering refresh.")
             return get_symbol_cache(force_refresh=True)


    return cache_data, cache_date

# --- Symbol Formatting ---

def format_expiry_for_symbol(expiry_date_str: str) -> Optional[str]:
    """
    Formats expiry date string (DDMMMYY or DDMMMYYYY) into DDMMMYY format for symbols.

    Args:
        expiry_date_str: The expiry date string (e.g., "25APR25", "25APR2025").

    Returns:
        The formatted string (e.g., "25APR25") or None if format is invalid.
    """
    if not isinstance(expiry_date_str, str):
         logger.warning(f"Invalid expiry date type for symbol construction: {type(expiry_date_str)}")
         return None

    expiry_date_str = expiry_date_str.upper()
    try:
        if len(expiry_date_str) == 9: # DDMMMYYYY
             dt = datetime.strptime(expiry_date_str, "%d%b%Y")
             return dt.strftime("%d%b%y").upper()
        elif len(expiry_date_str) == 7: # DDMMMYY
             # Validate the format
             datetime.strptime(expiry_date_str, "%d%b%y")
             return expiry_date_str
        else:
            # Attempt to parse if only month/year provided (e.g., APR25) - less reliable
            if len(expiry_date_str) == 5: # MMMYY
                 # Assume first day of month for parsing validation
                 datetime.strptime(f"01{expiry_date_str}", "%d%b%y")
                 return expiry_date_str # Return original if valid structure
            logger.warning(f"Invalid expiry date format for symbol construction: {expiry_date_str}")
            return None
    except ValueError:
        logger.warning(f"Could not parse expiry date for symbol construction: {expiry_date_str}")
        return None

def construct_trading_symbol(
    name: str,
    expiry_date_str: str,
    instrument_type: str, # 'FUT' or 'OPT'
    option_type: Optional[str] = None, # 'P' or 'C' for options
    strike: Optional[Union[int, float, str]] = None # Strike price for options
) -> Optional[str]:
    """
    Constructs the trading symbol based on Finvasia's naming convention.

    Args:
        name: Base instrument name (e.g., "BANKNIFTY", "NIFTY").
        expiry_date_str: Expiry date (e.g., "02AUG22", "25AUG2022").
        instrument_type: Type of instrument ('FUT' for Future, 'OPT' for Option).
        option_type: 'P' for PE or 'C' for CE (required for 'OPT').
        strike: Strike price (required for 'OPT').

    Returns:
        The constructed trading symbol string or None if inputs are invalid.
    """
    name = name.upper()
    instrument_type = instrument_type.upper()
    formatted_expiry = format_expiry_for_symbol(expiry_date_str)

    if not formatted_expiry:
        logger.error(f"Failed to format expiry date '{expiry_date_str}' for symbol construction.")
        return None

    if instrument_type == 'FUT':
        # Name+Date+Month+Year+FUT (e.g., BANKNIFTY25AUG22F) - Convention says FUT, example has F
        # Following the example format "F"
        return f"{name}{formatted_expiry}F"

    elif instrument_type == 'OPT':
        if not option_type or strike is None:
            logger.error("Option type ('P'/'C') and strike price are required for OPT instruments.")
            return None
        option_type = option_type.upper()
        if option_type not in ['P', 'C']:
            logger.error(f"Invalid option type: {option_type}. Must be 'P' or 'C'.")
            return None

        # Ensure strike is integer without decimals if possible
        try:
            # Convert strike to float first to handle numeric strings
            strike_float = float(strike)
            # Check if it's effectively an integer
            if strike_float == math.floor(strike_float):
                strike_str = str(int(strike_float))
            else:
                # Keep decimals if present (e.g., for some commodities or currencies)
                strike_str = str(strike_float)
        except (ValueError, TypeError) as e:
             logger.error(f"Invalid strike price format or type: {strike} ({type(strike)}). Error: {e}")
             return None

        # Name+Date+Month+Year+P/C+Strike (e.g., BANKNIFTY02AUG22P48150)
        return f"{name}{formatted_expiry}{option_type}{strike_str}"

    else:
        logger.error(f"Unsupported instrument type for symbol construction: {instrument_type}")
        return None


# --- Finvasia Client Class ---

class FinvasiaFeedClient:
    """
    Enhanced Finvasia client with feed subscription and symbol caching capabilities.
    """

    def __init__(
        self,
        user_id: str,
        password: str,
        twofa_key: str,
        vendor_code: str,
        app_key: str,
        imei: str = None,
        output_dir: str = "feed_data",
        api_url: str = "https://api.shoonya.com/NorenWClientTP/",
        ws_url: str = "wss://api.shoonya.com/NorenWSTP/",
        debug: bool = False,
        symbol_cache: Optional[Dict[str, Dict[str, Any]]] = None # Inject cache
    ):
        """
        Initialize the Finvasia client with authentication details.
        (Constructor arguments documented in previous version)
        """
        self.user_id = user_id
        self.password = password
        self.twofa_key = twofa_key
        self.vendor_code = vendor_code
        self.app_key = app_key
        self.imei = imei if imei else str(uuid.uuid4())
        self.logger = logger # Use the central logger
        if debug or self.logger.level > logging.DEBUG: # Set debug if requested or logger is already higher level
            self.logger.setLevel(logging.DEBUG)
        self.output_dir = Path(output_dir)
        self.api_url = api_url
        self.ws_url = ws_url
        self.debug = debug
        self.symbol_cache = symbol_cache if symbol_cache else {} # Store the cache
        self.underlying_tokens = {} # Cache for underlying tokens (e.g., {'NSE:NIFTY 50': '26000'})

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Authentication & API Client State
        self.api_client: Optional[NorenApi] = None
        self.is_authenticated = False
        self.session_token = None

        # WebSocket state
        self.websocket_connected = False
        self.is_running = False # Controls the feed processing loop

        # Feed subscription tracking
        self.subscribed_tokens: List[str] = []  # Format: "exchange|token"
        self.subscription_count = 0 # Tracks total successful subscriptions over lifetime
        self.token_to_symbol_map: Dict[str, str] = {}  # Map token to symbol for reference
        self.symbol_to_token_map: Dict[str, str] = {}  # Maps "Exchange:Symbol" -> token
        self.csv_writers: Dict[str, csv.DictWriter] = {}
        self.csv_files: Dict[str, Any] = {} # To store file handles
        self.feed_data_count: Dict[str, int] = {}  # Track number of data points per symbol
        self.last_update_time: Dict[str, str] = {}  # Track last update time per symbol

        # Thread locks
        self._ws_lock = threading.RLock()
        self._symbol_lock = threading.RLock() # Protects symbol/token maps & subscribed_tokens list
        self._tick_lock = threading.RLock() # Protects CSV writing and counters

        self.logger.info("FinvasiaFeedClient initialized")

    # --- Authentication and WebSocket Management (largely unchanged, kept for context) ---

    def authenticate(self) -> bool:
        # (Implementation from previous version - slightly condensed for brevity)
        if self.is_authenticated: return True
        try:
            self.api_client = NorenApi(host=self.api_url, websocket=self.ws_url)
            if not self._init_websocket(): return False
            twofa_token = pyotp.TOTP(self.twofa_key).now() if self.twofa_key else ""
            self.logger.info(f"Attempting login for user {self.user_id}")
            response = self.api_client.login(
                userid=self.user_id, password=self.password, twoFA=twofa_token,
                vendor_code=self.vendor_code or self.user_id, api_secret=self.app_key, imei=self.imei
            )
            self.logger.debug(f"Login response: {response}")
            if isinstance(response, dict) and response.get('stat') == 'Ok' and 'susertoken' in response:
                self.session_token = response['susertoken']
                self.is_authenticated = True
                self.is_running = True
                self.logger.info("Authentication successful")
                return True
            else:
                error_msg = response.get('emsg', 'Unknown error') if isinstance(response, dict) else 'Invalid response'
                self.logger.error(f"Authentication failed: {error_msg}")
                self.api_client = None
                return False
        except Exception as e:
            self.logger.exception(f"Exception during authentication: {e}")
            self.api_client = None
            return False

    def _init_websocket(self) -> bool:
        # (Implementation from previous version)
        if not self.api_client: return False
        self.logger.debug("Initializing WebSocket callbacks...")
        try:
            self.api_client.on_disconnect = self._on_ws_disconnect
            self.api_client.on_error = self._on_ws_error
            self.api_client.on_open = self._on_ws_open
            self.api_client.on_close = self._on_ws_close
            self.api_client.on_ticks = self._on_market_data
            self.api_client.on_order_update = self._on_order_update
            self.logger.info("WebSocket callbacks initialized.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to set WebSocket callbacks: {e}", exc_info=True)
            return False

    def start_websocket(self) -> bool:
        # (Implementation from previous version - slightly condensed)
        if not self.is_running:
            self.logger.warning("Cannot start WebSocket: Client is not running.")
            return False
        if not self.is_authenticated or not self.api_client:
            self.logger.error("Cannot start WebSocket: Authentication required.")
            return False
        if self.websocket_connected:
            self.logger.info("WebSocket already connected.")
            return True

        self.logger.info("Starting WebSocket connection process...")
        try:
            with self._ws_lock:
                if self.websocket_connected: return True # Double check inside lock
                # Use the documented callback arguments for start_websocket
                self.api_client.start_websocket(
                    order_update_callback=self._on_order_update,
                    subscribe_callback=self._on_market_data, # Route ticks here
                    socket_open_callback=self._on_ws_open,
                    socket_close_callback=self._on_ws_close,
                    # socket_error_callback=self._on_ws_error # Optional: some versions might use on_error instead
                )
                self.logger.info("Call to api_client.start_websocket() made. Waiting for on_open callback...")

            start_time = time.time(); wait_timeout = 20.0
            while not self.websocket_connected and time.time() - start_time < wait_timeout and self.is_running:
                time.sleep(0.1) # Short sleep while waiting

            if not self.websocket_connected:
                self.logger.error(f"WebSocket on_open confirmation not received within {wait_timeout} seconds.")
                return False

            self.logger.info("WebSocket connection established and confirmed via on_open.")
            return True
        except Exception as e:
            self.logger.error(f"Exception during WebSocket start process: {e}", exc_info=True)
            self.websocket_connected = False # Ensure state reflects failure
            return False

    def stop_websocket(self):        
        if not self.api_client:
            self.logger.warning("Cannot stop WebSocket: API client not initialized.")
            return

        with self._ws_lock:
            if self.websocket_connected:
                self.logger.info("Setting WebSocket connection flag to False.")
                # Mark as disconnected internally
                self.websocket_connected = False
            else:
                self.logger.info("WebSocket already stopped or not connected.")    

    # --- WebSocket Callbacks (largely unchanged, kept for context) ---
    def _on_ws_open(self):
        # (Implementation from previous version - slightly condensed)
        self.logger.info("WebSocket connection opened (on_open callback).")
        with self._ws_lock: self.websocket_connected = True
        # Use a lock for thread safety when accessing shared subscribed_tokens list
        with self._symbol_lock:
            tokens_to_resubscribe = list(self.subscribed_tokens) # Create a copy

        if tokens_to_resubscribe:
            self.logger.info(f"Re-subscribing to {len(tokens_to_resubscribe)} tokens...")
            try:
                # FIX: Check if api_client is valid before calling subscribe
                if not self.api_client:
                    self.logger.error("Cannot re-subscribe: API client is None.")
                    return

                # Pass the copied list
                response = self.api_client.subscribe(tokens_to_resubscribe)
                # FIX: Add check for None response during re-subscription as well
                if response is None:
                    self.logger.error("Re-subscription attempt returned None from API.")
                elif isinstance(response, dict) and response.get("stat") == "Ok":
                    self.logger.info(f"Successfully re-subscribed.")
                else:
                    err = response.get('emsg', str(response)) if isinstance(response, dict) else str(response)
                    self.logger.warning(f"Re-subscription failed or non-OK status: {err}")
            except Exception as e:
                self.logger.error(f"Error during re-subscription: {e}", exc_info=True)

    def _on_ws_close(self, code=None, reason=None):
        if self.websocket_connected: self.logger.warning(f"WebSocket connection closed. Code: {code}, Reason: {reason}")
        with self._ws_lock: self.websocket_connected = False

    def _on_ws_disconnect(self):
        if self.websocket_connected: self.logger.warning("WebSocket disconnected unexpectedly.")
        with self._ws_lock: self.websocket_connected = False

    def _on_ws_error(self, error):
        self.logger.error(f"WebSocket error reported: {error}")

    def _on_order_update(self, order_data):
        self.logger.debug(f"Order update received: {order_data}")

    def _on_market_data(self, ticks: Union[List[Dict], Dict]):
        # This callback receives data when using start_websocket with subscribe_callback
        if not self.is_running: return
        if isinstance(ticks, dict): ticks = [ticks]
        elif not isinstance(ticks, list):
             self.logger.warning(f"Received unexpected data type in market data callback: {type(ticks)}")
             return
        if not ticks: return

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        with self._tick_lock: # Lock for processing this batch
            for tick in ticks:
                if isinstance(tick, dict):
                    self._process_single_tick(tick, timestamp)
                else:
                     self.logger.warning(f"Skipping invalid item in ticks list: {tick}")


    def _process_single_tick(self, tick: Dict, timestamp: str):
        """Process a single market data tick. Assumes _tick_lock is held."""
        try:
            token = tick.get('tk')
            exchange = tick.get('e', 'UNKNOWN')
            if not token: return

            tick['received_timestamp'] = timestamp
            symbol = self.token_to_symbol_map.get(token, f"UNKNOWN_{token}")

            # --- Requirement 2: Print tick to console ---
            # Use logger for consistency and level control
            self.logger.info(f"Tick: {symbol} ({token}) | Data: {tick}")

            # --- CSV Writing ---
            writer = self.csv_writers.get(token)
            csv_file = self.csv_files.get(token)

            # Check if initialization is pending (None) or failed (False)
            if writer is None or csv_file is None:
                if writer is False: return # Skip if initialization previously failed

                # Initialize CSV writer
                fieldnames = list(tick.keys())
                # Ensure 'received_timestamp' is included if not already present
                if 'received_timestamp' not in fieldnames:
                    fieldnames.append('received_timestamp')

                safe_symbol = "".join(c if c.isalnum() else "_" for c in symbol)
                # **Requirement 7: Unique filename with date**
                today_str = date.today().strftime('%Y%m%d')
                filename = self.output_dir / f"{exchange}_{safe_symbol}_{token}_{today_str}.csv"
                try:
                    # Use 'a' to append if file exists for the day
                    self.csv_files[token] = open(filename, 'a', newline='', encoding='utf-8')
                    # Ensure fieldnames match what will be written
                    writer = csv.DictWriter(self.csv_files[token], fieldnames=fieldnames, extrasaction='ignore') # Ignore extra fields in tick
                    # Write header only if the file is newly created or empty
                    if self.csv_files[token].tell() == 0:
                        writer.writeheader()
                    self.csv_writers[token] = writer
                    self.logger.info(f"Initialized CSV writer for {symbol} (Token: {token}) to file {filename.name}")
                except IOError as e:
                    self.logger.error(f"Failed to open/create CSV file {filename} for {symbol}: {e}")
                    self.csv_writers[token] = False # Mark as failed
                    self.csv_files[token] = None
                    return
                except Exception as e: # Catch other potential errors like invalid fieldnames
                    self.logger.error(f"Error initializing DictWriter for {filename}: {e}")
                    self.csv_writers[token] = False
                    self.csv_files[token] = None
                    if self.csv_files[token]: # Close file if opened
                       try: self.csv_files[token].close()
                       except: pass
                    return


            # Write the tick data
            try:
                writer.writerow(tick)
                # Consider periodic flushing instead of every tick for performance
                # if self.feed_data_count.get(token, 0) % 100 == 0:
                #      self.csv_files[token].flush()
            except Exception as e:
                 self.logger.error(f"Error writing tick to CSV for {symbol} (Token: {token}): {e}")

            # Update counters and time
            self.feed_data_count[token] = self.feed_data_count.get(token, 0) + 1
            self.last_update_time[token] = timestamp

            # Log progress periodically (less frequent logging)
            count = self.feed_data_count.get(token, 0)
            if count % 5000 == 1: # Log every 5000 ticks per symbol
                self.logger.info(f"Received {count} ticks for {symbol} (Token: {token})")

        except Exception as e:
            self.logger.exception(f"Exception processing tick for token {token}: {e} - Tick data: {tick}")

    # --- Symbol Lookup and API Interaction ---

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

    def search_scrip(self, symbol: str, exchange: str) -> List[Dict[str, Any]]:
        """
        Search for scrips using the Finvasia API.
        """
        if not self.is_authenticated:
            self.logger.error("Cannot search scrip: Authentication required.")
            if not self.authenticate(): return []
        if not symbol or not exchange: return []
        self.logger.debug(f"Searching API for symbol='{symbol}' on exchange='{exchange}'")
        results: List[Dict[str, Any]] = []
        try:
            if not self.api_client: return []
            response = self.api_client.searchscrip(exchange=exchange, searchtext=symbol)
            self.logger.debug(f"API Response for searchscrip('{exchange}', '{symbol}'): {response}")
            if isinstance(response, dict) and response.get('stat') == 'Ok':
                results = response.get('values', [])
                self.logger.info(f"API search found {len(results)} scrip(s) matching '{symbol}' on {exchange}")
            elif isinstance(response, dict):
                self.logger.error(f"API Error searching for '{symbol}' on {exchange}: {response.get('emsg', 'Unknown')}")
            else:
                self.logger.warning(f"Unexpected API response for searchscrip: {response}")
        except Exception as e:
            self.logger.exception(f"Exception during API scrip search for '{symbol}' on {exchange}: {e}")
        return results

    def get_quote(self, symbol: str, exchange: str) -> Optional[Dict[str, Any]]:
         """
         Get quote details (including LTP) for a specific symbol using the API.
         Uses cache first to find the token.

         Args:
             symbol: The trading symbol (e.g., "NIFTY INDEX", "RELIANCE"). Case-insensitive for lookup.
             exchange: The exchange code (e.g., "NSE", "NFO"). Case-insensitive for lookup.

         Returns:
             Quote dictionary if successful, None otherwise.
         """
         if not self.is_authenticated:
             self.logger.error("Cannot get quote: Authentication required.")
             if not self.authenticate(): return None

         # 1. Find token using cache lookup (case-insensitive)
         token_info = self.lookup_symbol(symbol=symbol, exchange=exchange)
         token = token_info.get('token') if token_info else None

         # 2. If not in cache, try API search for the token (less efficient)
         if not token:
              self.logger.warning(f"Token for {exchange}:{symbol} not found in cache. Searching API...")
              search_results = self.search_scrip(symbol=symbol, exchange=exchange)
              # Find exact match (case-insensitive comparison for robustness)
              found_match = None
              for result in search_results:
                   # Prioritize exact match on TradingSymbol (tsym)
                   if result.get('tsym', '').upper() == symbol.upper() and result.get('exch', '').upper() == exchange.upper():
                        found_match = result
                        break
                   # Fallback: Check 'Symbol' field if tsym doesn't match (e.g., API returns 'Nifty 50' in Symbol field)
                   # This depends heavily on API response structure, use with caution
                   # elif result.get('Symbol', '').upper() == symbol.upper() and result.get('exch', '').upper() == exchange.upper():
                   #      found_match = result
                   #      self.logger.debug(f"Found match for {symbol} based on 'Symbol' field in API result.")
                   #      break

              if found_match:
                   token = found_match.get('token')
                   self.logger.info(f"Found token {token} for {exchange}:{symbol} via API search.")
                   # Optionally add this found token info to the client's cache?
                   # Might require locking if cache is shared/modified frequently
              else:
                   self.logger.error(f"Could not find token for {exchange}:{symbol} via cache or API search exact match.")
                   return None

         # 3. Get quote using the token
         self.logger.debug(f"Getting quote for {exchange}:{symbol} (Token: {token})")
         try:
             if not self.api_client:
                  self.logger.error("API client not initialized. Cannot get quote.")
                  return None
             # Ensure exchange is uppercase for API call if required by library
             response = self.api_client.get_quotes(exchange=exchange.upper(), token=token)
             self.logger.debug(f"API Response for get_quotes('{exchange.upper()}', '{token}'): {response}")

             if isinstance(response, dict) and response.get('stat') == 'Ok':
                  # The quote data is usually directly in the response dictionary
                  self.logger.info(f"Successfully retrieved quote for {exchange}:{symbol}")
                  return response
             else:
                  error_msg = response.get('emsg', 'Unknown error') if isinstance(response, dict) else str(response)
                  self.logger.error(f"Failed to get quote for {exchange}:{symbol} (Token: {token}): {error_msg}")
                  return None
         except Exception as e:
             self.logger.exception(f"Exception during get_quotes for {exchange}:{symbol} (Token: {token}): {e}")
             return None

    def get_underlying_ltp(self, underlying_symbol: str, exchange: str = "NSE") -> Optional[float]:
        """
        Gets the Last Traded Price (LTP) for the underlying instrument.
        Uses the correct TradingSymbol for lookup based on common indices.

        Args:
            underlying_symbol: The base name (e.g., "NIFTY", "BANKNIFTY"). Case-insensitive.
            exchange: The exchange where the underlying trades (usually NSE for indices).

        Returns:
            The LTP as a float, or None if fetching fails.
        """
        underlying_key = underlying_symbol.upper()
        # **FIX**: Map user input (e.g., "NIFTY") to the actual TradingSymbol used in cache/API ("NIFTY INDEX")
        symbol_map = {
            "NIFTY": "NIFTY INDEX",
            "BANKNIFTY": "NIFTY BANK",
            "FINNIFTY": "FINNIFTY", # From example data
            "MIDCPNIFTY": "MIDCPNIFTY", # From example data
            "SENSEX": "SENSEX", # Assuming this is the symbol on BSE
            "BANKEX": "BANKEX"  # Assuming this is the symbol on BSE
            # Add other mappings if needed
        }
        # Determine exchange based on symbol if not NSE
        if underlying_key in ["SENSEX", "BANKEX"]:
             exchange = "BSE"
        else:
             exchange = "NSE" # Default to NSE for others

        # Use mapped TradingSymbol or the original input (uppercase) if not in map
        api_trading_symbol = symbol_map.get(underlying_key, underlying_key)

        self.logger.info(f"Fetching LTP for underlying: Mapped '{underlying_symbol}' to '{api_trading_symbol}' on {exchange}")
        # Pass the correct TradingSymbol to get_quote
        quote = self.get_quote(symbol=api_trading_symbol, exchange=exchange)

        if quote and quote.get('lp') is not None: # Check specifically for 'lp' key
            try:
                ltp = float(quote['lp'])
                self.logger.info(f"LTP for {api_trading_symbol}: {ltp}")
                return ltp
            except (ValueError, TypeError) as e:
                self.logger.error(f"Could not convert LTP '{quote.get('lp')}' to float for {api_trading_symbol}. Error: {e}")
                return None
        else:
            self.logger.error(f"Failed to get quote or LTP ('lp' field missing/None) for {api_trading_symbol} on {exchange}. Quote: {quote}")
            return None

    def _parse_expiry_string(self, date_str: str) -> Optional[datetime]:
        """ Parses DDMMMYY or DDMMMYYYY into a datetime object. """
        if not isinstance(date_str, str): return None
        date_str = date_str.upper()
        try:
            if len(date_str) == 9: # DDMMMYYYY
                return datetime.strptime(date_str, "%d%b%Y")
            elif len(date_str) == 7: # DDMMMYY
                return datetime.strptime(date_str, "%d%b%y")
            else:
                return None
        except ValueError:
             # Don't raise, just return None for invalid formats
             # self.logger.debug(f"Could not parse date string format: {date_str}")
             return None

    def get_all_expiry_dates(self, underlying: str) -> List[str]:
        """
        Get all available expiry dates for a given underlying by searching the API.
        Note: This relies on the 'exd' field or parsing the 'tsym' from API results.

        Args:
            underlying: The underlying symbol (e.g., "NIFTY", "BANKNIFTY")

        Returns:
            List of unique expiry dates found, sorted chronologically. Format DDMMMYYYY.
        """
        # *** Method reinstated to fix the AttributeError ***
        exchange = "NFO" # Futures/Options usually on NFO
        self.logger.info(f"Fetching expiry dates for {underlying} from API search...")

        # Broad search for the underlying on NFO
        search_results = self.search_scrip(symbol=underlying, exchange=exchange)

        expiry_dates = set()
        # Regex to find date patterns like DDMMMYY or DDMMMYYYY within the symbol name
        expiry_pattern = re.compile(r'(\d{2}[A-Z]{3}(\d{2}|\d{4}))')

        for item in search_results:
            # 1. Prefer the 'exd' field if available and valid
            expiry_from_field = item.get('exd', '').strip()
            if expiry_from_field:
                try:
                    parsed_date = self._parse_expiry_string(expiry_from_field)
                    if parsed_date:
                        expiry_dates.add(parsed_date.strftime("%d%b%Y").upper())
                        continue # Prefer 'exd' if valid
                except ValueError: # Should not happen if _parse_expiry_string returns None
                    self.logger.debug(f"Could not parse 'exd' field: {expiry_from_field}")

            # 2. Fallback: Try to extract from trading symbol 'tsym'
            symbol = item.get('tsym', '')
            if symbol:
                match = expiry_pattern.search(symbol.upper()) # Search uppercase symbol
                if match:
                    date_str = match.group(1) # e.g., 25DEC24 or 28MAR2024
                    try:
                        parsed_date = self._parse_expiry_string(date_str)
                        if parsed_date:
                             expiry_dates.add(parsed_date.strftime("%d%b%Y").upper())
                    except ValueError: # Should not happen
                         self.logger.debug(f"Could not parse date string '{date_str}' found in symbol '{symbol}'")

        if not expiry_dates:
             self.logger.warning(f"Could not find any expiry dates for {underlying} via API search.")
             return []

        # Sort the found dates
        try:
            sorted_dates = sorted(list(expiry_dates), key=lambda d: datetime.strptime(d, "%d%b%Y"))
            self.logger.info(f"Found {len(sorted_dates)} unique expiry dates for {underlying}.")
            return sorted_dates
        except ValueError as e:
             self.logger.error(f"Error sorting expiry dates: {e}. Returning unsorted list.")
             return list(expiry_dates)


    # --- Subscription Management (Updated) ---

    def subscribe_symbols(self, symbols_data: List[Dict[str, Any]]) -> bool:
        """
        Subscribe to market data using token info (exch, token, tsym).
        """
        if not symbols_data: return False
        if not self.is_authenticated:
            if not self.authenticate(): return False
        if not self.websocket_connected:
            self.logger.info("WebSocket not connected. Attempting to start for subscription...")
            if not self.start_websocket():
                self.logger.error("Failed to start WebSocket for subscription.")
                return False
            # Add a longer delay after WS connect before subscribing
            self.logger.info("Waiting 2 seconds after WebSocket connect before subscribing...")
            time.sleep(2.0) # Increased delay

        tokens_to_subscribe = []
        with self._symbol_lock: # Lock access to shared subscription state
            for item in symbols_data:
                token = item.get('token')
                exch = item.get('exch')
                symbol = item.get('tsym', f"TOKEN_{token}") # Use provided tsym or default
                if token and exch:
                    subscription_key = f"{exch}|{token}"
                    if subscription_key not in self.subscribed_tokens:
                        tokens_to_subscribe.append(subscription_key)
                        self.subscribed_tokens.append(subscription_key) # Add before API call
                        self.token_to_symbol_map[token] = symbol
                        # Use uppercase symbol for consistent key in symbol_to_token_map
                        self.symbol_to_token_map[f"{exch}:{symbol.upper()}"] = token
                        # Initialize CSV/counter state under tick lock
                        with self._tick_lock:
                             self.csv_writers[token] = None # Mark for lazy initialization
                             self.csv_files[token] = None
                             self.feed_data_count[token] = 0
                             self.last_update_time[token] = ""
                    # else: self.logger.debug(f"Token {subscription_key} already subscribed.")
                else: self.logger.warning(f"Skipping item with missing token/exch: {item}")

        if not tokens_to_subscribe:
             self.logger.info("No new symbols to subscribe to.")
             return True # Request was valid, even if no new subs needed

        self.logger.info(f"Attempting to subscribe to {len(tokens_to_subscribe)} new symbols.")

        all_batches_ok = True
        try:
            batch_size = 50 # Adjust batch size if needed
            for i in range(0, len(tokens_to_subscribe), batch_size):
                batch = tokens_to_subscribe[i:i+batch_size]
                if not batch: continue
                self.logger.info(f"Subscribing batch {i//batch_size + 1} ({len(batch)} tokens)")

                # FIX: Check API client validity right before the call
                if not self.api_client:
                     self.logger.error("API client is None, cannot subscribe.")
                     # Mark overall success as False and potentially remove tokens added?
                     all_batches_ok = False
                     # Remove tokens added optimistically for this batch? Difficult state recovery.
                     # Best to just report failure and let reconnect handle it.
                     continue # Skip this batch

                # FIX: Log the exact batch being sent
                self.logger.debug(f"Calling api_client.subscribe with batch: {batch}")
                response = None # Initialize response to None
                try:
                    response = self.api_client.subscribe(batch)
                except Exception as sub_ex:
                     self.logger.exception(f"Exception occurred during api_client.subscribe call: {sub_ex}")
                     # Treat exception as failure for this batch
                     response = None # Ensure response is None if exception occurs

                self.logger.debug(f"Subscription response: {response}") # Log response even if None

                # # FIX: Add specific check for None response
                # if response is None:
                #     self.logger.error("Subscription failed for batch: API call returned None or raised exception.")
                #     all_batches_ok = False
                # elif not isinstance(response, dict) or response.get('stat') != 'Ok':
                #     err = response.get('emsg', str(response)) if isinstance(response, dict) else str(response)
                #     self.logger.error(f"Subscription failed for batch: {err}")
                #     all_batches_ok = False
                #     # Note: Tokens remain in self.subscribed_tokens even if API fails.
                #     # Reconnect logic in _on_ws_open will attempt re-subscription.
                # else:
                self.logger.info(f"Subscription request sent successfully for {len(batch)} symbols.")

                time.sleep(0.3) # Pause between batches

            return all_batches_ok # Return overall status of all batches
        except Exception as e:
            self.logger.exception(f"Exception during subscription process: {e}")
            return False

    def unsubscribe_symbols(self, symbols_data: List[Dict[str, Any]]) -> bool:
        """Unsubscribe using token info."""
        if not symbols_data: return False

        tokens_to_unsubscribe = []
        keys_to_remove = []
        with self._symbol_lock: # Lock for reading shared state
            for item in symbols_data:
                token = item.get('token'); exch = item.get('exch')
                if token and exch:
                    subscription_key = f"{exch}|{token}"
                    # Check if it's actually in our list before trying to remove
                    if subscription_key in self.subscribed_tokens:
                        tokens_to_unsubscribe.append(subscription_key)
                        keys_to_remove.append(subscription_key) # Mark for removal from our list

        if not tokens_to_unsubscribe:
             self.logger.info("No matching active subscriptions found to unsubscribe.")
             return True # Nothing to do

        self.logger.info(f"Attempting to unsubscribe from {len(tokens_to_unsubscribe)} symbols.")
        api_call_successful = True
        if self.api_client and self.websocket_connected: # Only call API if likely to work
            try:
                batch_size = 50
                for i in range(0, len(tokens_to_unsubscribe), batch_size):
                    batch = tokens_to_unsubscribe[i:i+batch_size]
                    if not batch: continue
                    self.logger.info(f"Unsubscribing batch {i//batch_size + 1} ({len(batch)} tokens).")
                    # FIX: Log batch before call and handle None response
                    self.logger.debug(f"Calling api_client.unsubscribe with batch: {batch}")
                    response = None
                    try:
                        response = self.api_client.unsubscribe(batch)
                    except Exception as unsub_ex:
                        self.logger.exception(f"Exception occurred during api_client.unsubscribe call: {unsub_ex}")
                        response = None

                    self.logger.debug(f"Unsubscribe response: {response}")
                    if response is None:
                         self.logger.error("Unsubscription failed for batch: API call returned None or raised exception.")
                         api_call_successful = False
                    elif not isinstance(response, dict) or response.get('stat') != 'Ok':
                         err = response.get('emsg', str(response)) if isinstance(response, dict) else str(response)
                         self.logger.error(f"Unsubscription failed for batch: {err}")
                         api_call_successful = False # Mark as failed but continue cleanup
                    else:
                         self.logger.info(f"Unsubscribe request sent successfully for {len(batch)} symbols.")
                    time.sleep(0.2)
            except Exception as e:
                self.logger.exception(f"Exception during unsubscription API call: {e}")
                api_call_successful = False
        else:
             self.logger.warning("Skipping unsubscription API call (WS disconnected or API client unavailable).")
             api_call_successful = False

        # Cleanup internal state regardless of API success
        with self._symbol_lock: # Lock for modifying shared state
             removed_count = 0
             for key in keys_to_remove:
                 if key in self.subscribed_tokens:
                     try:
                         self.subscribed_tokens.remove(key)
                         removed_count += 1
                         parts = key.split('|')
                         if len(parts) == 2:
                             token = parts[1]; exch = parts[0]
                             symbol = self.token_to_symbol_map.pop(token, None)
                             # Use uppercase symbol for consistent key removal
                             if symbol: self.symbol_to_token_map.pop(f"{exch}:{symbol.upper()}", None)

                             # Close CSV file and remove writer under tick lock
                             with self._tick_lock:
                                 csv_file = self.csv_files.pop(token, None)
                                 if csv_file:
                                     try: csv_file.close(); self.logger.info(f"Closed CSV for {token} ({symbol or 'N/A'})")
                                     except Exception as e_close: self.logger.error(f"Error closing CSV {token}: {e_close}")
                                 self.csv_writers.pop(token, None)
                                 self.feed_data_count.pop(token, None)
                                 self.last_update_time.pop(token, None)
                     except ValueError:
                          self.logger.warning(f"Tried to remove key '{key}' from subscribed_tokens, but it was not found (race condition?).")


             self.logger.info(f"Cleaned up state for {removed_count} symbols. Active subscriptions: {len(self.subscribed_tokens)}")
        # Return status based on API call attempt, not internal cleanup
        return api_call_successful

    def unsubscribe_all(self):
        """Unsubscribe from all active subscriptions."""
        with self._symbol_lock: # Get a consistent list of tokens
            all_subscribed_keys = list(self.subscribed_tokens) # Copy the list
            tokens_to_process = [{'exch': k.split('|')[0], 'token': k.split('|')[1]} for k in all_subscribed_keys if '|' in k]

        if not tokens_to_process:
            self.logger.info("No active subscriptions to unsubscribe from.")
            return

        self.logger.info(f"Unsubscribing from all {len(tokens_to_process)} symbols.")
        self.unsubscribe_symbols(tokens_to_process) # Use the batching logic

        # Final check and forced clear if needed (should be empty if unsubscribe_symbols worked)
        with self._symbol_lock:
             if self.subscribed_tokens:
                  self.logger.warning(f"{len(self.subscribed_tokens)} subs potentially remain after unsubscribe_all. Forcing clear.")
                  # Force clear remaining state
                  self.subscribed_tokens.clear()
                  self.token_to_symbol_map.clear(); self.symbol_to_token_map.clear()
                  with self._tick_lock:
                      # Close any remaining file handles
                      for token, file_handle in list(self.csv_files.items()): # Iterate over copy
                           if file_handle:
                               try: file_handle.close()
                               except Exception as e: self.logger.error(f"Error closing remaining CSV {token}: {e}")
                      self.csv_writers.clear(); self.csv_files.clear()
                      self.feed_data_count.clear(); self.last_update_time.clear()
             else: self.logger.info("All subscriptions successfully cleared.")

    # --- Stats and Logout (Unchanged) ---
    def get_subscription_stats(self) -> Dict[str, Any]:
        # (Implementation from previous version)
        with self._symbol_lock, self._tick_lock: # Lock both to get consistent snapshot
            active_subs = len(self.subscribed_tokens)
            symbols_with_data = sum(1 for count in self.feed_data_count.values() if count > 0)
            total_points = sum(self.feed_data_count.values())
            # Create copies for safety when returning
            feeds_per_sym = {self.token_to_symbol_map.get(t, t): c for t, c in self.feed_data_count.items()}
            last_updates = {self.token_to_symbol_map.get(t, t): tm for t, tm in self.last_update_time.items()}
        stats = {
            "active_subscriptions": active_subs,
            "websocket_connected": self.websocket_connected,
            "symbols_receiving_data": symbols_with_data,
            "total_data_points_received": total_points,
            "feeds_per_symbol": feeds_per_sym,
            "last_update_times": last_updates
        }
        return stats

    def logout(self) -> bool:
        # (Implementation from previous version - condensed)
        self.logger.info("Initiating logout sequence...")
        self.is_running = False # Stop processing ticks first
        self.unsubscribe_all() # Unsubscribe and close files
        self.stop_websocket() # Stop WS connection
        if not self.is_authenticated or not self.api_client:
            self.logger.info("Not logged in or API client gone.")
            self.is_authenticated = False; self.session_token = None; self.api_client = None
            return True
        logout_success = False
        try:
            self.logger.info("Calling API logout...")
            response = self.api_client.logout()
            self.logger.debug(f"Logout API response: {response}")
            logout_success = isinstance(response, dict) and response.get('stat') == 'Ok'
            if logout_success: self.logger.info("API logout successful.")
            else: self.logger.error(f"API logout failed: {response.get('emsg', 'Unknown') if isinstance(response, dict) else str(response)}")
        except Exception as e: self.logger.exception(f"Exception during API logout call: {e}")
        finally:
            # Clear state regardless of API call success
            self.is_authenticated = False; self.session_token = None; self.api_client = None
            self.logger.info("Client authentication state cleared.")
        return logout_success


# --- Helper Functions ---

def load_credentials_from_env() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Load credentials from environment variables."""
    # (Implementation from previous version)
    user_id = os.environ.get('FINVASIA_USER_ID')
    password = os.environ.get('FINVASIA_PASSWORD')
    twofa_key = os.environ.get('FINVASIA_TWOFA_KEY')
    vendor_code = os.environ.get('FINVASIA_VENDOR_CODE')
    api_key = os.environ.get('FINVASIA_API_KEY')
    imei = os.environ.get('FINVASIA_IMEI')
    return user_id, password, twofa_key, vendor_code, api_key, imei

def prompt_for_credentials() -> Tuple[str, str, str, str, str, Optional[str]]:
    """Prompt user for credentials."""
    # (Implementation from previous version)
    logger.info("Credentials not found/incomplete in environment variables. Please enter manually.")
    user_id = input("Enter Finvasia User ID: ")
    password = input("Enter Finvasia Password: ")
    twofa_key = input("Enter Finvasia TOTP Key: ")
    vendor_code = input("Enter Vendor Code: ")
    api_key = input("Enter API Key: ")
    imei = input("Enter IMEI (leave blank for auto-generated): ") or None
    if not all([user_id, password, twofa_key, vendor_code, api_key]):
         raise ValueError("All credential fields (except IMEI) are required.")
    return user_id, password, twofa_key, vendor_code, api_key, imei

# Removed get_strike_step function as interval is now user-defined

def find_atm_strike(ltp: float, strike_step: int) -> int:
    """Calculates the At-The-Money (ATM) strike price by rounding to the nearest step."""
    if strike_step <= 0:
        logger.error(f"Invalid strike step: {strike_step}. Cannot calculate ATM.")
        return int(ltp) # Return LTP rounded down as fallback

    # Round to nearest multiple of strike_step
    atm_strike = round(ltp / strike_step) * strike_step
    return int(atm_strike)


# --- Main Execution Logic ---

# Modified run_atm_subscription to accept strike_step and test_duration
def run_atm_subscription(client: FinvasiaFeedClient, underlying: str, expiry_date: str, num_strikes: int, strike_step: int, test_duration: int):
    """
    Finds ATM, generates symbols, uses cache for tokens, subscribes, and monitors.

    Args:
        client: The authenticated FinvasiaFeedClient instance.
        underlying: The underlying symbol (e.g., "NIFTY", "BANKNIFTY").
        expiry_date: Expiry date for options (Format: DDMMMYYYY).
        num_strikes: Number of CE/PE strikes around ATM to subscribe.
        strike_step: The interval between strike prices.
        test_duration: Duration in seconds to monitor the feed.
    """
    logger = client.logger
    option_exchange = "NFO" # Options are on NFO

    # 1. Get Underlying LTP
    logger.info(f"Fetching LTP for underlying '{underlying}'...")
    # Determine exchange for underlying (usually NSE/BSE for index)
    underlying_exchange = "BSE" if underlying.upper() in ["SENSEX", "BANKEX"] else "NSE"
    ltp = client.get_underlying_ltp(underlying_symbol=underlying, exchange=underlying_exchange)
    if ltp is None:
        logger.error(f"Could not fetch LTP for {underlying}. Cannot determine ATM strike.")
        return

    # 2. Determine ATM Strike (using user-provided strike_step)
    atm_strike = find_atm_strike(ltp, strike_step)
    logger.info(f"Underlying LTP: {ltp:.2f}, User Strike Step: {strike_step}, ATM Strike: {atm_strike}")

    # 3. Generate Target Strikes
    target_strikes = set() # Use set to avoid duplicates if num_strikes is 0
    target_strikes.add(atm_strike) # Always include ATM strike
    for i in range(1, num_strikes + 1): # Generate num_strikes on each side
        target_strikes.add(atm_strike + i * strike_step)
        target_strikes.add(atm_strike - i * strike_step)

    # Filter out negative strikes if any (unlikely for indices but good practice)
    positive_strikes = {s for s in target_strikes if s > 0}
    sorted_strikes = sorted(list(positive_strikes))

    if not sorted_strikes:
         logger.error("No valid positive strikes generated. Cannot proceed.")
         return

    logger.info(f"Target strikes ({len(sorted_strikes)}): {sorted_strikes}")

    # 4. Construct Symbols and Lookup Tokens from Cache
    symbols_to_subscribe = []
    missing_symbols = []
    logger.info("Constructing option symbols and looking up tokens in cache...")
    for strike in sorted_strikes:
        # Corrected option type loop
        for opt_type in ["C", "P"]:
            # Construct the trading symbol string
            trading_symbol = construct_trading_symbol(
                name=underlying,
                expiry_date_str=expiry_date, # Should be DDMMMYYYY format
                instrument_type="OPT",
                option_type=opt_type,
                strike=strike
            )
            if not trading_symbol:
                logger.warning(f"Could not construct symbol for {underlying} {expiry_date} {strike} {opt_type}")
                continue

            # **Requirement 4 & 5: Look up in local cache**
            token_info = client.lookup_symbol(symbol=trading_symbol, exchange=option_exchange)

            if token_info and 'token' in token_info and 'exch' in token_info:
                # Ensure token is digits only before adding
                token_val = token_info['token']
                if token_val and token_val.isdigit():
                    logger.debug(f"Found in cache: {trading_symbol} -> Token: {token_val}")
                    # Ensure essential keys are present for subscription function
                    symbols_to_subscribe.append({
                        'exch': token_info['exch'], # Use exchange from cache info
                        'token': token_val,
                        # Use 'tradingsymbol' key from cache if available, else constructed one
                        'tsym': token_info.get('tradingsymbol', trading_symbol)
                    })
                else:
                    logger.warning(f"Symbol {trading_symbol} found in cache but has invalid token: '{token_val}'. Skipping.")
                    missing_symbols.append(f"{trading_symbol} (Invalid Token)")

            else:
                logger.warning(f"Symbol {trading_symbol} not found in local cache. Skipping.")
                missing_symbols.append(trading_symbol)

    if missing_symbols:
         logger.warning(f"Could not find/use the following {len(missing_symbols)} symbols/tokens in the cache: {missing_symbols}")

    if not symbols_to_subscribe:
        logger.error("No valid symbols found in cache for the target strikes. Cannot subscribe.")
        return

    logger.info(f"Prepared {len(symbols_to_subscribe)} symbols for subscription based on cache lookup.")

    # 5. Subscribe to Symbols
    logger.info(f"Subscribing to {len(symbols_to_subscribe)} option symbols...")
    success = client.subscribe_symbols(symbols_to_subscribe)

    if not success:
        logger.error("Failed to send subscription request(s) successfully. Check previous logs. Ending test.")
        return # Stop the process if subscription failed

    logger.info(f"Subscription requests potentially sent for {len(symbols_to_subscribe)} symbols (check logs for batch success/failure).")

    # 6. Monitor Feed (Duration passed as argument)
    logger.info(f"Starting feed monitoring for {test_duration} seconds... Press Ctrl+C to stop early.")
    start_time = time.time()
    last_stat_print_time = 0
    try:
        while time.time() - start_time < test_duration:
            current_time = time.time()
            if current_time - last_stat_print_time >= 10: # Print stats every 10s
                stats = client.get_subscription_stats()
                logger.info(
                    f"Stats | ActiveSubs: {stats['active_subscriptions']}, "
                    f"WS_Connected: {stats['websocket_connected']}, "
                    f"SymbolsReceiving: {stats['symbols_receiving_data']}, "
                    f"TotalTicks: {stats['total_data_points_received']}"
                 )
                last_stat_print_time = current_time

            if not client.is_running:
                 logger.warning("Client is no longer running (logout initiated?). Stopping monitoring loop.")
                 break
            time.sleep(1) # Check every second

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Stopping test...")
    except Exception as e:
        logger.exception(f"An error occurred during feed monitoring: {e}")
    finally:
        logger.info("Feed monitoring finished.")
        final_stats = client.get_subscription_stats()
        logger.info("=== Final Statistics ===")
        logger.info(f"Test Duration: {time.time() - start_time:.2f} seconds")
        logger.info(f"WebSocket Connected at end: {final_stats['websocket_connected']}")
        logger.info(f"Active Subscriptions at end: {final_stats['active_subscriptions']}")
        logger.info(f"Symbols that received data: {final_stats['symbols_receiving_data']}")
        logger.info(f"Total data points received: {final_stats['total_data_points_received']}")
        if final_stats['symbols_receiving_data'] > 0:
            avg_per_symbol = final_stats['total_data_points_received'] / final_stats['symbols_receiving_data']
            logger.info(f"Average data points per active symbol: {avg_per_symbol:.2f}")
        else:
             logger.info("No data points received for any subscribed symbol.")
        logger.info("=========================")
        logger.info("Unsubscribing from all test feeds...")
        # Ensure client exists before calling unsubscribe
        if client:
            client.unsubscribe_all()


def main():
    """
    Main function to initialize the client, handle user input, and run the test.
    """
    global logger
    client = None

    try:
        # --- Setup ---
        logger.info("Starting Finvasia ATM Options Feed Test Script")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        # --- Load Credentials ---
        logger.info("Loading credentials...")
        creds = load_credentials_from_env()
        if not all(creds[:5]): # Check user, pass, 2fa, vendor, api
            try: creds = prompt_for_credentials()
            except (ValueError, EOFError) as e: logger.error(f"Failed to get credentials: {e}. Exiting."); return
        user_id, password, twofa_key, vendor_code, api_key, imei = creds

        # --- Initialize Symbol Cache ---
        logger.info("Initializing symbol cache (may download masters)...")
        force_cache_refresh = not CACHE_FILE.exists() # Refresh if cache file is missing
        symbol_cache, cache_date = get_symbol_cache(force_refresh=force_cache_refresh)
        if symbol_cache: logger.info(f"Symbol cache ready ({len(symbol_cache)} symbols). Last updated: {cache_date}")
        else:
            # Cache is critical for this workflow, exit if failed
            logger.error("Symbol cache could not be loaded or created. This script requires the cache. Exiting.")
            return

        # --- Initialize Client ---
        logger.info("Initializing FinvasiaFeedClient...")
        client = FinvasiaFeedClient(
            user_id=user_id, password=password, twofa_key=twofa_key,
            vendor_code=vendor_code, app_key=api_key, imei=imei,
            output_dir="feed_data", debug=False, # Set debug=True for more verbose client logs
            symbol_cache=symbol_cache # Pass the loaded cache
        )

        # --- Authenticate ---
        logger.info("Authenticating with Finvasia...")
        if not client.authenticate():
            logger.error("Authentication failed. Please check credentials and 2FA key. Exiting.")
            return

        # --- Get User Input for Test ---
        logger.info("Select an underlying instrument:")
        # Adjusted list based on corrected TradingSymbols in get_underlying_ltp map
        underlyings = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
        for i, u in enumerate(underlyings): print(f"  {i+1}. {u}")

        underlying = None
        while underlying is None:
            try:
                choice = input(f"Enter choice (1-{len(underlyings)}): ")
                idx = int(choice) - 1
                if 0 <= idx < len(underlyings):
                    underlying = underlyings[idx]
                    logger.info(f"Selected: {underlying}")
                else: print("Invalid choice.")
            except ValueError: print("Invalid input. Please enter a number.")
            except EOFError: logger.error("Input stream closed. Exiting."); return

        # --- Get Strike Interval from User --- Requirement 1 ---
        strike_step = 0
        while strike_step <= 0:
            try:
                step_str = input(f"Enter strike price interval for {underlying} (e.g., 50, 100): ")
                strike_step = int(step_str)
                if strike_step > 0:
                    logger.info(f"Using strike interval: {strike_step}")
                    # break # Exit loop handled by condition
                else:
                    print("Please enter a positive interval.")
            except ValueError:
                print("Invalid input. Please enter a number.")
            except EOFError: logger.error("Input stream closed. Exiting."); return


        # --- Get Expiry Dates ---
        logger.info(f"Fetching available NFO expiry dates for {underlying} using API...")
        # Use API search via client method to get available expiries
        expiry_dates = client.get_all_expiry_dates(underlying) # Returns DDMMMYYYY

        if not expiry_dates:
            logger.error(f"No expiry dates found for {underlying} via API. Cannot proceed.")
            if client and client.is_authenticated: client.logout()
            return

        print("\nAvailable expiry dates:")
        for i, date_str in enumerate(expiry_dates): print(f"  {i+1}. {date_str}")
        print(f"  {len(expiry_dates) + 1}. Enter manually")

        selected_expiry_date = None
        while selected_expiry_date is None:
            try:
                expiry_choice = input(f"Select expiry date (1-{len(expiry_dates) + 1}): ")
                expiry_idx = int(expiry_choice) - 1
                if 0 <= expiry_idx < len(expiry_dates):
                    selected_expiry_date = expiry_dates[expiry_idx] # Already in DDMMMYYYY
                    logger.info(f"Selected expiry: {selected_expiry_date}")
                elif expiry_idx == len(expiry_dates):
                    manual_expiry = input("Enter expiry (DDMMMYYYY or DDMMMYY): ").strip().upper()
                    # Use the client's internal parser for validation
                    parsed_manual = client._parse_expiry_string(manual_expiry)
                    if parsed_manual:
                        selected_expiry_date = parsed_manual.strftime("%d%b%Y").upper() # Standardize format
                        logger.info(f"Using manual expiry: {selected_expiry_date}")
                    else: print(f"Invalid format entered: '{manual_expiry}'. Please use DDMMMYYYY or DDMMMYY.")
                else: print("Invalid choice.")
            except ValueError: print("Invalid input. Please enter a number.")
            except EOFError: logger.error("Input stream closed. Exiting."); return

        # --- Get Number of Strikes ---
        num_strikes = -1 # Initialize to invalid value
        while num_strikes < 0:
            try:
                # Clarify that it's number of strikes *on each side* of ATM
                num_strikes_str = input("Enter number of CE/PE strikes around ATM (e.g., 10 => 10 CE + 10 PE) [>=0]: ")
                num_strikes = int(num_strikes_str)
                if num_strikes >= 0: # Allow 0 for just ATM
                    logger.info(f"Number of strikes around ATM selected: {num_strikes}")
                    # break # Exit loop handled by condition num_strikes < 0
                else: print("Please enter a non-negative number (0 or more).")
            except ValueError: print("Invalid input. Please enter a whole number.")
            except EOFError: logger.error("Input stream closed. Exiting."); return

        # --- Get Monitoring Duration --- Requirement 1 (Moved) ---
        test_duration = 0
        while test_duration <= 0:
            try:
                duration_str = input("Enter monitoring duration in seconds [e.g., 300]: ")
                test_duration = int(duration_str)
                if test_duration > 0:
                    logger.info(f"Monitoring duration set to: {test_duration} seconds")
                    # break # Exit loop handled by condition
                else:
                    print("Please enter a positive number for duration.")
            except ValueError:
                print("Invalid input. Please enter a number.")
            except EOFError: logger.error("Input stream closed. Exiting."); return


        # --- Run the ATM Subscription Logic ---
        logger.info(f"Starting ATM subscription process for {underlying} | Expiry: {selected_expiry_date} | Strikes around ATM: {num_strikes} | Interval: {strike_step}")
        run_atm_subscription(
            client=client,
            underlying=underlying,
            expiry_date=selected_expiry_date, # Use the selected/validated date
            num_strikes=num_strikes,
            strike_step=strike_step, # Pass user-defined step
            test_duration=test_duration # Pass user-defined duration
        )

    except Exception as e:
        logger.exception(f"An unhandled exception occurred in main: {e}")
    finally:
        logger.info("Script execution finished or terminated.")
        if client and client.is_authenticated:
            logger.info("Ensuring client logout...")
            client.logout()
        else: logger.info("Client was not authenticated or already logged out.")
        logging.shutdown() # Close log handlers


if __name__ == "__main__":
    main()
