"""Date utility functions for the NCAA scraper."""

from datetime import date, datetime, timedelta
from typing import List
import logging

logger = logging.getLogger(__name__)


def get_yesterday() -> date:
    """Get yesterday's date."""
    return date.today() - timedelta(days=1)


def format_date_for_url(target_date: date) -> str:
    """Format date for use in NCAA URLs (YYYY/MM/DD)."""
    return target_date.strftime('%Y/%m/%d')

def format_date_for_stats_ncaa_url(target_date: date) -> str:
    """Format date for stats.ncaa.org URLs (MM/DD/YYYY)."""
    return target_date.strftime('%m/%d/%Y')


def parse_date_from_url(url: str) -> date:
    """Parse date from NCAA URL."""
    try:
        # URL format: .../basketball-women/d3/2025/02/06/all-conf
        parts = url.split('/')
        year = int(parts[-4])
        month = int(parts[-3])
        day = int(parts[-2])
        return date(year, month, day)
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse date from URL {url}: {e}")
        raise ValueError(f"Invalid URL format: {url}")


def get_date_range(start_date: date, end_date: date) -> List[date]:
    """Get list of dates between start and end (inclusive)."""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def validate_date_string(date_str: str) -> bool:
    """Validate date string format (YYYY/MM/DD)."""
    try:
        datetime.strptime(date_str, '%Y/%m/%d')
        return True
    except ValueError:
        return False
