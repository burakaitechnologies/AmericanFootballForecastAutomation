"""Base scraper class with common functionality."""

import asyncio
import random
from abc import ABC, abstractmethod
from typing import Optional
import aiohttp
from utils.logger import get_logger


class BaseScraper(ABC):
    """Abstract base class for web scrapers."""
    
    def __init__(self, max_concurrent_requests: int = 50):
        """
        Initialize base scraper.
        
        Args:
            max_concurrent_requests: Maximum concurrent HTTP requests
        """
        self.max_concurrent_requests = max_concurrent_requests
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = get_logger(self.__class__.__name__)
        
        # Common headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def fetch_with_retry(
        self, 
        url: str, 
        max_retries: int = 3,
        delay_range: tuple[float, float] = (0.1, 0.5)
    ) -> Optional[str]:
        """
        Fetch URL with retry logic and exponential backoff.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            delay_range: Random delay range between requests (min, max)
            
        Returns:
            Page content or None if failed
        """
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    # Random delay to avoid being blocked
                    await asyncio.sleep(random.uniform(*delay_range))
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            content = await response.text()
                            self.logger.debug(f"Successfully fetched {url}")
                            return content
                        else:
                            self.logger.warning(f"HTTP {response.status} for {url}")
                
                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout for {url} (attempt {attempt + 1}/{max_retries})")
                except Exception as e:
                    self.logger.warning(f"Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(wait_time)
            
            self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
            return None
    
    @staticmethod
    def validate_team_name(team_name: str) -> bool:
        """Validate team name is non-empty string with reasonable length."""
        return isinstance(team_name, str) and len(team_name.strip()) > 2
    
    @staticmethod
    def validate_score(score: float) -> bool:
        """Validate score is numeric and in reasonable range."""
        try:
            score_val = float(score)
            return 0 <= score_val <= 100
        except (ValueError, TypeError):
            return False
    
    @abstractmethod
    async def scrape_all_games(self) -> dict:
        """
        Scrape all games from the source.
        
        Returns:
            Dictionary with 'website', 'total', and 'games' keys
        """
        pass

