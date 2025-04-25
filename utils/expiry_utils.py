"""
Utility functions for option expiry date calculations.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List
import calendar

# Cache for expiry dates to avoid recalculating
_expiry_cache = {}

def get_next_expiry(index_symbol: str, offset: int = 0, ref_date: date = None) -> date:
    """
    Get the next expiry date for an index.
    
    Args:
        index_symbol: Index symbol
        offset: 0 for current expiry, 1 for next expiry, etc.
        ref_date: Reference date, defaults to today
        
    Returns:
        date: Expiry date
    """
    if ref_date is None:
        ref_date = datetime.now().date()
        
    # Generate cache key
    cache_key = f"{index_symbol}_{offset}_{ref_date}"
    
    # Check if we have a cached result
    if cache_key in _expiry_cache:
        return _expiry_cache[cache_key]
    
    # Default to weekly Thursday expiry
    expiry_day = 3  # 0=Monday, 6=Sunday
    is_monthly = False
    
    # Configure expiry day based on index
    if index_symbol == "NIFTY" or index_symbol == "BANKNIFTY":
        expiry_day = 3  # Thursday
    elif index_symbol == "FINNIFTY":
        expiry_day = 1  # Tuesday
    else:
        # Default to Thursday for other indices
        expiry_day = 3  # Thursday
    
    # Calculate the next expiry
    expiry = ref_date
    
    # First, find the next expiry day from reference date
    days_ahead = (expiry_day - expiry.weekday()) % 7
    if days_ahead == 0:
        # If today is expiry day, check if we've passed market close
        if datetime.now().hour >= 15:  # 3 PM
            days_ahead = 7  # Move to next week
    
    expiry = expiry + timedelta(days=days_ahead)
    
    # Apply offset
    for _ in range(offset):
        expiry = expiry + timedelta(days=7)
    
    # Handle monthly expiry (last Thursday of the month)
    if is_monthly:
        # Check if this is the last occurrence of this weekday in the month
        next_week = expiry + timedelta(days=7)
        if next_week.month != expiry.month:
            # This is already the last occurrence
            pass
        else:
            # Find the last occurrence
            while True:
                test_date = expiry + timedelta(days=7)
                if test_date.month != expiry.month:
                    break
                expiry = test_date
    
    # Handle holidays (simplified - would need actual holiday calendar)
    # ...
    
    # Cache the result
    _expiry_cache[cache_key] = expiry
    
    return expiry

def get_monthly_expiry(year: int, month: int) -> date:
    """
    Get the monthly expiry date for a given year and month.
    
    Args:
        year: Year
        month: Month (1-12)
        
    Returns:
        date: Monthly expiry date
    """
    # Monthly expiry is typically the last Thursday of the month
    # Get the last day of the month
    last_day = calendar.monthrange(year, month)[1]
    last_date = date(year, month, last_day)
    
    # Find the last Thursday
    weekday = last_date.weekday()
    offset = (3 - weekday) % 7  # 3 is Thursday
    if offset > 0:
        offset -= 7  # Go back to previous Thursday
    
    return last_date + timedelta(days=offset)

def is_expiry_day(symbol: str, check_date: date = None) -> bool:
    """
    Check if a given date is an expiry day for the specified symbol.
    
    Args:
        symbol: Symbol to check
        check_date: Date to check, defaults to today
        
    Returns:
        bool: True if the date is an expiry day, False otherwise
    """
    if check_date is None:
        check_date = datetime.now().date()
    
    # Get the next expiry date from a day before check_date
    prev_day = check_date - timedelta(days=1)
    next_expiry = get_next_expiry(symbol, 0, prev_day)
    
    return next_expiry == check_date

def get_days_to_expiry(symbol: str, check_date: date = None) -> int:
    """
    Get the number of days to the next expiry for a symbol.
    
    Args:
        symbol: Symbol to check
        check_date: Date to check from, defaults to today
        
    Returns:
        int: Number of days to expiry
    """
    if check_date is None:
        check_date = datetime.now().date()
    
    next_expiry = get_next_expiry(symbol, 0, check_date)
    
    return (next_expiry - check_date).days

def clear_expiry_cache():
    """
    Clear the expiry date cache.
    """
    global _expiry_cache
    _expiry_cache = {}