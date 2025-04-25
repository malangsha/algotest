import logging
import time
from datetime import datetime, date
from pathlib import Path

# Import the enhanced SymbolCache class
# Assuming the class is in a file called symbol_cache.py
from symbol_cache_1 import SymbolCache

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('symbol_cache_demo.log')
        ]
    )
    return logging.getLogger('SymbolCacheDriver')

def display_cache_stats(cache):
    """Display cache statistics."""
    stats = cache.get_stats()
    logger.info("=== Symbol Cache Statistics ===")
    logger.info(f"Total symbols: {stats['total_symbols']}")
    logger.info(f"Symbols by exchange: {stats['symbols_by_exchange']}")
    logger.info(f"Last refresh time: {stats['last_refresh_time']}")
    logger.info(f"Cache size: {stats['cache_size_bytes'] / (1024*1024):.2f} MB")
    logger.info(f"Refresh count: {stats['refresh_count']}")
    logger.info(f"Error count: {stats['error_count']}")
    logger.info("================================")

def test_symbol_lookup(cache):
    """Test symbol lookup functionality."""
    logger.info("=== Testing Symbol Lookup ===")

    # Test NSE equity symbol lookup
    symbols_to_test = [
        ('RELIANCE-EQ', 'NSE'),
        ('SBIN-EQ', 'NSE'),
        ('NIFTY25APR25FUT', 'NFO'),
        ('NIFTY INDEX', 'NSE'),
        ('BANKNIFTY25APR25C46000', 'NFO'),
        ('SENSEX', 'BSE')
    ]

    for symbol, exchange in symbols_to_test:
        result = cache.lookup_symbol(symbol, exchange)
        if result:
            logger.info(f"Found {symbol} on {exchange}: {result}")
        else:
            logger.info(f"Could not find {symbol} on {exchange}")

    logger.info("============================")

def test_expiry_dates(cache):
    """Test expiry date functionality."""
    logger.info("=== Testing Expiry Date Functions ===")

    # Check if expiry data is available
    if not cache.is_expiry_data_available():
        logger.warning("No expiry data available. Attempting to refresh...")
        cache.refresh_expiry_dates()
        if not cache.is_expiry_data_available():
            logger.error("Could not get expiry data.")
            return

    # Test indices
    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

    for index in indices:
        logger.info(f"\nExpiry dates for {index}:")

        # Get all expiry dates
        all_expiries = cache.get_all_expiry_dates(index)
        logger.info(f"Weekly expiries: {all_expiries['weekly']}")
        logger.info(f"Monthly expiries: {all_expiries['monthly']}")

        # Get nearest expiry dates
        nearest_weekly = cache.get_nearest_expiry(index, "weekly")
        nearest_monthly = cache.get_nearest_expiry(index, "monthly")
        logger.info(f"Nearest weekly expiry: {nearest_weekly}")
        logger.info(f"Nearest monthly expiry: {nearest_monthly}")

        # Get expiry by offset
        for i in range(3):
            weekly_offset = cache.get_expiry_by_offset(index, i, "weekly")
            logger.info(f"Weekly expiry offset {i}: {weekly_offset}")

        for i in range(2):
            monthly_offset = cache.get_expiry_by_offset(index, i, "monthly")
            logger.info(f"Monthly expiry offset {i}: {monthly_offset}")

    logger.info("===================================")

def test_symbol_construction(cache):
    """Test trading symbol construction functionality."""
    logger.info("=== Testing Symbol Construction ===")

    # Test various symbol constructions
    test_cases = [
        # (name, expiry, type, option_type, strike)
        ("NIFTY", "25APR25", "FUT", None, None),
        ("NIFTY", "2025-04-25", "FUT", None, None),  # ISO date format
        ("BANKNIFTY", "25APR25", "OPT", "C", 46000),
        ("FINNIFTY", "25APR25", "OPT", "P", 22500),
        ("NIFTY", "25APR2025", "FUT", None, None),  # Long date format
    ]

    for name, expiry, instr_type, opt_type, strike in test_cases:
        symbol = cache.construct_trading_symbol(name, expiry, instr_type, opt_type, strike)
        logger.info(f"Constructed symbol for {name} {expiry} {instr_type} {opt_type} {strike}: {symbol}")

    logger.info("================================")

def main():
    """Main function to demonstrate SymbolCache usage."""
    logger.info("Starting Symbol Cache Driver Demo")

    try:
        # Get singleton instance
        cache = SymbolCache()

        # If cache is empty, this will trigger an initial download
        # Wait for initial setup if needed
        if sum(len(cache_data) for cache_data in cache.symbol_caches.values()) == 0:
            logger.info("Initial cache setup in progress, waiting...")
            time.sleep(10)  # Give some time for initial download

        # Display cache statistics
        display_cache_stats(cache)

        # Test symbol lookup
        test_symbol_lookup(cache)

        # Test expiry dates
        test_expiry_dates(cache)

        # Test symbol construction
        test_symbol_construction(cache)

        # Example of how to force a refresh
        logger.info("Demonstrating forced refresh...")
        cache.refresh_expiry_dates()

        # Show updated cache stats
        display_cache_stats(cache)

        logger.info("Demo completed successfully!")

    except Exception as e:
        logger.error(f"Error in demo: {e}", exc_info=True)
    finally:
        # Cleanup (not strictly necessary with singleton)
        pass

if __name__ == "__main__":
    logger = setup_logging()
    main()
