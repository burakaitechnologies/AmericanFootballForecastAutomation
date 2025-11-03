"""NFL week calculation utility."""

from datetime import date, datetime
from typing import Optional


def get_current_nfl_week(date_obj: Optional[date] = None) -> int:
    """
    Calculate the current NFL week number based on the date.
    
    Uses Sept 4, 2025 as the season start date.
    Each NFL week runs from Thursday to Wednesday (7 days).
    
    Args:
        date_obj: Date to calculate for (defaults to today)
        
    Returns:
        Week number (minimum 1)
    """
    # Set NFL season start date
    season_start = date(2025, 9, 4)
    
    # Get today's date or use provided date
    if date_obj is None:
        today = date.today()
    else:
        if isinstance(date_obj, datetime):
            today = date_obj.date()
        else:
            today = date_obj
    
    # Calculate number of days since start
    days_since_start = (today - season_start).days
    
    # Each NFL week runs from Thursday to Wednesday (7 days)
    current_week = (days_since_start // 7) + 1
    
    # Ensure week doesn't go below 1
    current_week = max(1, current_week)
    
    return current_week


def get_nfl_week_for_date(date_obj: datetime) -> int:
    """
    Calculate NFL week for a specific date.
    
    Args:
        date_obj: Date to calculate week for
        
    Returns:
        Week number (minimum 1)
    """
    return get_current_nfl_week(date_obj)


if __name__ == "__main__":
    """Test the week calculator."""
    current_week = get_current_nfl_week()
    print(f"We are currently in NFL Week {current_week}")
    
    # Test for November 1, 2025 (should be around week 9)
    test_date = datetime(2025, 11, 1)
    test_week = get_nfl_week_for_date(test_date)
    print(f"Week for {test_date.date()}: {test_week}")

