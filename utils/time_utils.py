"""
Time utility functions for market timing.
"""

import logging
import warnings
from datetime import datetime, time, date, timedelta
from typing import Optional, Dict, List, Tuple, Set
import calendar

# Market open and close times
DEFAULT_MARKET_OPEN = time(9, 15, 0)   # 9:15 AM
DEFAULT_MARKET_CLOSE = time(15, 30, 0)  # 3:30 PM

# Market holidays (simplified example - would need to be updated yearly)
MARKET_HOLIDAYS_2023 = [
    date(2023, 1, 26),   # Republic Day
    date(2023, 3, 7),    # Holi
    date(2023, 4, 7),    # Good Friday
    date(2023, 4, 14),   # Dr. Ambedkar Jayanti
    date(2023, 5, 1),    # Maharashtra Day
    date(2023, 6, 28),   # Bakri Id
    date(2023, 8, 15),   # Independence Day
    date(2023, 9, 19),   # Ganesh Chaturthi
    date(2023, 10, 2),   # Gandhi Jayanti
    date(2023, 11, 14),  # Diwali
    date(2023, 11, 27),  # Guru Nanak Jayanti
    date(2023, 12, 25),  # Christmas
]

MARKET_HOLIDAYS_2024 = [
    date(2024, 1, 26),   # Republic Day
    date(2024, 3, 8),    # Maha Shivaratri
    date(2024, 3, 25),   # Holi
    date(2024, 3, 29),   # Good Friday
    date(2024, 4, 11),   # Eid-ul-Fitr
    date(2024, 4, 17),   # Ram Navami
    date(2024, 5, 1),    # Maharashtra Day
    date(2024, 6, 17),   # Bakri Id
    date(2024, 8, 15),   # Independence Day
    date(2024, 10, 2),   # Gandhi Jayanti
    date(2024, 11, 1),   # Diwali
    date(2024, 11, 15),  # Guru Nanak Jayanti
    date(2024, 12, 25),  # Christmas
]

# Adding 2025 market holidays
MARKET_HOLIDAYS_2025 = [
    date(2025, 1, 26),   # Republic Day
    date(2025, 3, 14),   # Maha Shivaratri (approximate)
    date(2025, 3, 18),   # Good Friday (approximate)
    date(2025, 4, 14),   # Dr. Ambedkar Jayanti
    date(2025, 5, 1),    # Maharashtra Day
    date(2025, 8, 15),   # Independence Day
    date(2025, 10, 2),   # Gandhi Jayanti
    date(2025, 10, 23),  # Diwali (approximate)
    date(2025, 12, 25),  # Christmas
]

# Cache for dynamically generated holidays
_generated_holiday_cache = {}

def generate_standard_holidays(year: int) -> Set[date]:
    """
    Generate a set of standard holidays for a given year.
    This is an approximation and should be updated with actual holidays when known.
    
    Args:
        year: The year to generate holidays for
        
    Returns:
        Set[date]: A set of holiday dates
    """
    holidays = set()
    
    # Fixed date holidays
    fixed_holidays = [
        (1, 26),    # Republic Day
        (4, 14),    # Dr. Ambedkar Jayanti
        (5, 1),     # Maharashtra Day
        (8, 15),    # Independence Day
        (10, 2),    # Gandhi Jayanti
        (12, 25),   # Christmas
    ]
    
    for month, day in fixed_holidays:
        holidays.add(date(year, month, day))
    
    # Last Thursday of each month (monthly expiry)
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        last_date = date(year, month, last_day)
        
        # Find the last Thursday
        weekday = last_date.weekday()
        offset = (3 - weekday) % 7  # 3 is Thursday
        if offset > 0:
            offset -= 7  # Go back to previous Thursday
        
        monthly_expiry = last_date + timedelta(days=offset)
        # Note: We don't add expiry dates as holidays, this is just for reference
    
    # Warning about using generated holidays
    logging.warning(f"Using auto-generated holidays for {year}. These are approximate and should be updated.")
    
    return holidays

def is_market_open(check_time: Optional[datetime] = None, market_open: time = DEFAULT_MARKET_OPEN, 
                  market_close: time = DEFAULT_MARKET_CLOSE) -> bool:
    """
    Check if the market is open at the given time.
    
    Args:
        check_time: Time to check (defaults to current time)
        market_open: Market opening time
        market_close: Market closing time
        
    Returns:
        bool: True if market is open, False otherwise
    """
    if check_time is None:
        check_time = datetime.now()
        
    # Check if today is a weekend
    if check_time.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
        
    # Check if today is a holiday
    today = check_time.date()
    if is_market_holiday(today):
        return False
        
    # Check if time is within market hours
    current_time = check_time.time()
    return time_in_range(market_open, current_time, market_close)

def is_market_holiday(check_date: date) -> bool:
    """
    Check if the given date is a market holiday.
    
    Args:
        check_date: Date to check
        
    Returns:
        bool: True if market is closed on this date, False otherwise
    """
    year = check_date.year
    
    # Select the appropriate holiday list based on year
    if year == 2023:
        holidays = MARKET_HOLIDAYS_2023
    elif year == 2024:
        holidays = MARKET_HOLIDAYS_2024
    elif year == 2025:
        holidays = MARKET_HOLIDAYS_2025
    else:
        # For future years, use generated holidays
        if year not in _generated_holiday_cache:
            _generated_holiday_cache[year] = generate_standard_holidays(year)
        
        holidays = _generated_holiday_cache[year]
        
        # Log a warning if we're using generated holidays
        logging.warning(f"Using generated holidays for {year}. Please update the MARKET_HOLIDAYS_{year} list with actual holidays.")
        
    return check_date in holidays

def time_in_range(start: time, check_time: time, end: time) -> bool:
    """
    Check if a time is within a given range.
    
    Args:
        start: Start time
        check_time: Time to check
        end: End time
        
    Returns:
        bool: True if time is within the range, False otherwise
    """
    if start <= end:
        return start <= check_time <= end
    else:
        # Handle overnight ranges (e.g., 22:00 - 06:00)
        return start <= check_time or check_time <= end

def get_market_sessions() -> List[Tuple[time, time]]:
    """
    Get market trading sessions.
    
    Returns:
        List[Tuple[time, time]]: List of (start, end) times for trading sessions
    """
    # Regular market session
    regular_session = (DEFAULT_MARKET_OPEN, DEFAULT_MARKET_CLOSE)
    
    # You could add pre-market, post-market or other special sessions here
    
    return [regular_session]

def get_time_to_market_open(check_time: Optional[datetime] = None) -> timedelta:
    """
    Get time remaining until market opens.
    
    Args:
        check_time: Time to check from (defaults to current time)
        
    Returns:
        timedelta: Time until market opens, or zero if market is already open
    """
    if check_time is None:
        check_time = datetime.now()
        
    if is_market_open(check_time):
        return timedelta(0)
        
    # If market is closed, calculate time to next opening
    current_date = check_time.date()
    current_time = check_time.time()
    
    # If current time is after market close, check next day
    if current_time > DEFAULT_MARKET_CLOSE:
        next_date = current_date + timedelta(days=1)
        # Skip weekends and holidays
        while next_date.weekday() >= 5 or is_market_holiday(next_date):
            next_date += timedelta(days=1)
        
        # Next opening time
        next_open = datetime.combine(next_date, DEFAULT_MARKET_OPEN)
        return next_open - check_time
    
    # If current time is before market open on the same day
    if current_time < DEFAULT_MARKET_OPEN:
        # If today is not a weekend or holiday
        if current_date.weekday() < 5 and not is_market_holiday(current_date):
            next_open = datetime.combine(current_date, DEFAULT_MARKET_OPEN)
            return next_open - check_time
    
    # Otherwise, find the next trading day
    next_date = current_date
    while True:
        next_date += timedelta(days=1)
        if next_date.weekday() < 5 and not is_market_holiday(next_date):
            break
    
    next_open = datetime.combine(next_date, DEFAULT_MARKET_OPEN)
    return next_open - check_time

def get_time_to_market_close(check_time: Optional[datetime] = None) -> timedelta:
    """
    Get time remaining until market closes.
    
    Args:
        check_time: Time to check from (defaults to current time)
        
    Returns:
        timedelta: Time until market closes, or zero if market is already closed
    """
    if check_time is None:
        check_time = datetime.now()
        
    if not is_market_open(check_time):
        return timedelta(0)
        
    # Market is open, calculate time to close
    current_date = check_time.date()
    market_close = datetime.combine(current_date, DEFAULT_MARKET_CLOSE)
    
    return max(market_close - check_time, timedelta(0))

def format_time_delta(delta: timedelta) -> str:
    """
    Format a timedelta for display.
    
    Args:
        delta: Time delta to format
        
    Returns:
        str: Formatted time string
    """
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def update_market_holidays(year: int, holidays: List[date]) -> None:
    """
    Update the market holidays for a specific year.
    This can be used to dynamically update holidays during runtime.
    
    Args:
        year: The year to update
        holidays: List of holiday dates
    """
    global _generated_holiday_cache
    _generated_holiday_cache[year] = set(holidays)
    logging.info(f"Updated market holidays for {year} with {len(holidays)} entries.")