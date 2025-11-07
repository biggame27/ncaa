"""Constants and enums for the NCAA scraper."""

from enum import Enum


class ErrorType(Enum):
    """Types of errors that can occur during scraping."""
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    GAME_ERROR = "GAME_ERROR"


class Division(Enum):
    """NCAA divisions."""
    D1 = "d1"
    D2 = "d2"
    D3 = "d3"


class Gender(Enum):
    """Gender categories."""
    MEN = "men"
    WOMEN = "women"


# Google Drive API configuration
GOOGLE_DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.file']

# NCAA URL patterns
NCAA_BASE_URL = "https://stats.ncaa.org/contests/livestream_scoreboards"
NCAA_OLD_BASE_URL = "https://www.ncaa.com/scoreboard/basketball-{gender}/{division}/{date}/all-conf"  # Legacy

# Default values
DEFAULT_OUTPUT_DIR = "scraped_data"
DEFAULT_TOKEN_FILE = "token.pickle"
DEFAULT_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"

# Selenium configuration
DEFAULT_WAIT_TIMEOUT = 15
DEFAULT_SLEEP_TIME = 2
