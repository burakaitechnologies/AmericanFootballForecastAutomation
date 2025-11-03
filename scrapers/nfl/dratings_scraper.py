"""Scraper for DRatings.com NFL predictions (spreads)."""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from uuid import uuid4
from bs4 import BeautifulSoup

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.base_scraper import BaseScraper


class DRatingsScraper(BaseScraper):
    """Scraper for DRatings.com NFL predictions (spreads)."""
    
    def __init__(self, max_concurrent_requests: int = 10):
        """Initialize DRatings scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.dratings.com"
        self.main_urls = [
            "https://www.dratings.com/predictor/nfl-football-predictions/",
            "https://www.dratings.com/predictor/nfl-football-predictions/upcoming/2#scroll-upcoming",
            "https://www.dratings.com/predictor/nfl-football-predictions/upcoming/3#scroll-upcoming",
            "https://www.dratings.com/predictor/nfl-football-predictions/upcoming/4#scroll-upcoming",
            "https://www.dratings.com/predictor/nfl-football-predictions/upcoming/5#scroll-upcoming"
        ]
    
    async def get_game_urls(self) -> list[str]:
        """Extract game URLs from all main pages."""
        self.logger.info("Fetching game URLs from main pages...")
        all_game_urls = []
        
        for main_url in self.main_urls:
            self.logger.info(f"Fetching games from: {main_url}")
            html_content = await self.fetch_with_retry(main_url)
            
            if not html_content:
                self.logger.error(f"Failed to fetch main page: {main_url}")
                continue
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for game links with class "d--b"
            game_links = soup.find_all('a', class_='d--b', href=re.compile(r'/predictor/nfl-football-predictions/'))
            self.logger.info(f"Found {len(game_links)} game links on {main_url}")
            
            for link in game_links:
                href = link.get('href')
                if href:
                    # Convert relative URL to absolute URL
                    full_url = self.base_url + href if href.startswith('/') else href
                    
                    if full_url not in all_game_urls:
                        all_game_urls.append(full_url)
        
        self.logger.info(f"Total unique game URLs found: {len(all_game_urls)}")
        return all_game_urls
    
    async def parse_game_data(self, url: str) -> dict:
        """Parse individual game page to extract team names and spreads."""
        try:
            html_content = await self.fetch_with_retry(url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract away team name - selector: #away-form > thead > tr > th:nth-child(1) > div
            away_team_elem = soup.select_one('#away-form > thead > tr > th:nth-child(1) > div')
            away_team_full = away_team_elem.get_text(strip=True) if away_team_elem else None
            
            # Extract only mascot name (last word)
            away_team = away_team_full.split()[-1] if away_team_full and ' ' in away_team_full else away_team_full
            
            # Extract home team name - selector: #home-form > thead > tr > th:nth-child(1) > div
            home_team_elem = soup.select_one('#home-form > thead > tr > th:nth-child(1) > div')
            home_team_full = home_team_elem.get_text(strip=True) if home_team_elem else None
            
            # Extract only mascot name (last word)
            home_team = home_team_full.split()[-1] if home_team_full and ' ' in home_team_full else home_team_full
            
            # Extract away spread - selector: #away-breakdown-projection > span:nth-child(1)
            away_spread_elem = soup.select_one('#away-breakdown-projection > span:nth-child(1)')
            away_spread = None
            if away_spread_elem:
                spread_text = away_spread_elem.get_text(strip=True)
                try:
                    away_spread = float(spread_text)
                except ValueError:
                    pass
            
            # Extract home spread - selector: #home-breakdown-projection > span:nth-child(1)
            home_spread_elem = soup.select_one('#home-breakdown-projection > span:nth-child(1)')
            home_spread = None
            if home_spread_elem:
                spread_text = home_spread_elem.get_text(strip=True)
                try:
                    home_spread = float(spread_text)
                except ValueError:
                    pass
            
            # Validate data
            if away_team and home_team and away_spread is not None and home_spread is not None:
                game_data = {
                    'game_id': str(uuid4()),
                    'away_team': away_team,
                    'home_team': home_team,
                    'spread_away': away_spread,
                    'spread_home': home_spread,
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Validate using base class methods
                if (self.validate_team_name(game_data['away_team']) and
                    self.validate_team_name(game_data['home_team']) and
                    self.validate_score(game_data['spread_away']) and
                    self.validate_score(game_data['spread_home'])):
                    return game_data
                else:
                    self.logger.warning(f"Invalid game data for {url}")
            else:
                self.logger.warning(f"Incomplete data for {url}: away={away_team}, home={home_team}, away_spread={away_spread}, home_spread={home_spread}")
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error parsing game data from {url}: {e}")
            return None
    
    async def scrape_all_games(self) -> dict:
        """Main function to scrape all games."""
        start_time = time.time()
        
        # Get all game URLs
        game_urls = await self.get_game_urls()
        
        if not game_urls:
            self.logger.error("No game URLs found")
            return {"website": "dratings", "total": 0, "games": []}
        
        self.logger.info(f"Starting to scrape {len(game_urls)} games...")
        
        # Create tasks for parallel processing
        tasks = [self.parse_game_data(url) for url in game_urls]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        games = [result for result in results if result and not isinstance(result, Exception)]
        
        duration = time.time() - start_time
        
        self.logger.info(f"Successfully scraped {len(games)} games out of {len(game_urls)} URLs in {duration:.2f}s")
        
        return {
            "website": "dratings",
            "total": len(games),
            "games": games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with DRatingsScraper(max_concurrent_requests=10) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("dratings_games.json", league="nfl")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        scraper.logger.info(f"Total games scraped: {data['total']}")
        scraper.logger.info(f"Data saved to {output_file}")
        
        # Print summary
        print("\n=== SCRAPING SUMMARY ===")
        print(f"Website: {data['website']}")
        print(f"Total games: {data['total']}")
        print(f"Output file: {output_file}")
        
        if data['games']:
            print(f"\nSample game:")
            print(json.dumps(data['games'][0], indent=2))


if __name__ == "__main__":
    asyncio.run(main())

