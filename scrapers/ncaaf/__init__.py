"""NCAAF scrapers package."""

from .dimers_scraper import DimersScraper
from .oddshark_scraper import OddsSharkScraper
from .espn_scraper import ESPNScraper
from .dratings_scraper import DRatingsScraper

__all__ = [
    "DimersScraper",
    "OddsSharkScraper",
    "ESPNScraper",
    "DRatingsScraper",
]
