"""Scraper for DRatings.com NCAAF predictions."""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4
from bs4 import BeautifulSoup

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.base_scraper import BaseScraper


class DRatingsScraper(BaseScraper):
    """Scraper for DRatings.com college football predictions."""
    
    def __init__(self, max_concurrent_requests: int = 10):
        """Initialize DRatings scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.dratings.com"
        self.main_urls = [
            "https://www.dratings.com/predictor/ncaa-football-predictions/#scroll-upcoming",
            "https://www.dratings.com/predictor/ncaa-football-predictions/upcoming/2#scroll-upcoming",
            "https://www.dratings.com/predictor/ncaa-football-predictions/upcoming/3#scroll-upcoming",
            "https://www.dratings.com/predictor/ncaa-football-predictions/upcoming/4#scroll-upcoming",
            "https://www.dratings.com/predictor/ncaa-football-predictions/upcoming/5#scroll-upcoming",
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
            game_links = soup.find_all('a', class_='d--b')
            self.logger.info(f"Found {len(game_links)} game links on {main_url}")
            
            for link in game_links:
                href = link.get('href')
                if href and '/predictor/ncaa-football-predictions/' in href:
                    # Convert relative URL to absolute URL
                    full_url = self.base_url + href if href.startswith('/') else href
                    
                    if full_url not in all_game_urls:
                        all_game_urls.append(full_url)
        
        self.logger.info(f"Total unique game URLs found: {len(all_game_urls)}")
        return all_game_urls
    
    async def scrape_game_page(self, game_url: str) -> dict:
        """Scrape individual game page for team names and spreads."""
        try:
            html_content = await self.fetch_with_retry(game_url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract away team name
            away_team_element = soup.select_one('#away-form > thead > tr > th:nth-child(1) > div')
            away_team = away_team_element.get_text(strip=True) if away_team_element else None
            
            # Extract home team name
            home_team_element = soup.select_one('#home-form > thead > tr > th:nth-child(1) > div')
            home_team = home_team_element.get_text(strip=True) if home_team_element else None
            
            # Extract away spread
            away_spread_element = soup.select_one('#away-breakdown-projection > span:nth-child(1)')
            away_spread = None
            if away_spread_element:
                try:
                    text = away_spread_element.get_text(strip=True)
                    # Remove any non-numeric characters except decimal point
                    text = re.sub(r'[^\d.]', '', text)
                    if text:
                        away_spread = float(text)
                except (ValueError, AttributeError) as e:
                    self.logger.debug(f"Could not parse away spread from {game_url}: {e}")
            
            # Extract home spread
            home_spread_element = soup.select_one('#home-breakdown-projection > span:nth-child(1)')
            home_spread = None
            if home_spread_element:
                try:
                    text = home_spread_element.get_text(strip=True)
                    # Remove any non-numeric characters except decimal point
                    text = re.sub(r'[^\d.]', '', text)
                    if text:
                        home_spread = float(text)
                except (ValueError, AttributeError) as e:
                    self.logger.debug(f"Could not parse home spread from {game_url}: {e}")
            
            # Validate that we have all required data
            if not all([away_team, home_team, away_spread is not None, home_spread is not None]):
                self.logger.warning(f"Missing data for game: {game_url}")
                return None
            
            game_data = {
                'game_id': str(uuid4()),
                'away_team': away_team,
                'home_team': home_team,
                'spread_away': away_spread,
                'spread_home': home_spread,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Validate
            if (self.validate_team_name(game_data['away_team']) and
                self.validate_team_name(game_data['home_team']) and
                self.validate_score(game_data['spread_away']) and
                self.validate_score(game_data['spread_home'])):
                return game_data
            else:
                self.logger.warning(f"Invalid game data for {game_url}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error parsing game page {game_url}: {e}")
            return None
    
    async def scrape_all_games(self) -> dict:
        """Main function to scrape all games."""
        start_time = time.time()
        
        # Get all game URLs
        game_urls = await self.get_game_urls()
        
        if not game_urls:
            self.logger.error("No game URLs found")
            return {"website": "dratings", "total": 0, "games": []}
        
        self.logger.info(f"Found {len(game_urls)} game URLs, now scraping individual pages...")
        
        # Scrape each game page concurrently
        tasks = [self.scrape_game_page(url) for url in game_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        all_games = [result for result in results if result and not isinstance(result, Exception)]
        
        # Remove duplicates based on team names
        unique_games = []
        seen_games = set()
        
        for game in all_games:
            game_key = (game['away_team'], game['home_team'])
            reverse_key = (game['home_team'], game['away_team'])
            
            if game_key not in seen_games and reverse_key not in seen_games:
                unique_games.append(game)
                seen_games.add(game_key)
        
        duration = time.time() - start_time
        
        self.logger.info(f"Successfully scraped {len(unique_games)} unique games from {len(game_urls)} URLs in {duration:.2f}s")
        
        return {
            "website": "dratings",
            "total": len(unique_games),
            "games": unique_games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    
    config = Config.from_env()
    
    async with DRatingsScraper(max_concurrent_requests=10) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
    # Save to JSON file in data/ncaaf/games_scraped/
    output_file = config.get_games_scraped_path("dratings_games.json", league="ncaaf")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print("\n=== SCRAPING SUMMARY ===")
    print(f"Website: {data['website']}")
    print(f"Total games: {data['total']}")
    print(f"Output file: {output_file}")
    
    if data['games']:
        print(f"\nSample game:")
        print(json.dumps(data['games'][0], indent=2))


if __name__ == "__main__":
    asyncio.run(main())

