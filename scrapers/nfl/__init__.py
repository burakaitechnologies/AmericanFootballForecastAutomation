"""Web scrapers for NFL prediction data."""

from .dimers_scraper import DimersScraper
from .oddshark_scraper import OddsSharkScraper
from .espn_scraper import ESPNScraper
from .dratings_scraper import DRatingsScraper
from .fantasynerds_scraper import FantasyNerdsScraper
from .sportsline_scraper import SportsLineScraper
from .florio_simms_scraper import FlorioSimmsScraper

__all__ = [
    "DimersScraper",
    "OddsSharkScraper",
    "ESPNScraper",
    "DRatingsScraper",
    "FantasyNerdsScraper",
    "SportsLineScraper",
    "FlorioSimmsScraper",
]

