"""Scraper for ESPN.com NFL predictions (spreads)."""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from uuid import uuid4
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.base_scraper import BaseScraper


class ESPNScraper(BaseScraper):
    """Scraper for ESPN.com NFL predictions (spreads)."""
    
    def __init__(self, max_concurrent_requests: int = 50):
        """Initialize ESPN scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.espn.com"
        self.odds_url = "https://www.espn.com/nfl/odds"
    
    async def get_game_urls(self) -> list[str]:
        """Extract game URLs from the main odds page."""
        self.logger.info("Fetching main odds page...")
        html_content = await self.fetch_with_retry(self.odds_url)
        
        if not html_content:
            self.logger.error("Failed to fetch main odds page")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        game_urls = []
        
        # Look for game links with data-game-link="true" attribute
        game_links = soup.find_all('a', {'data-game-link': 'true'})
        self.logger.info(f"Found {len(game_links)} game links with data-game-link attribute")
        
        for link in game_links:
            href = link.get('href')
            if href:
                full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                if full_url not in game_urls:
                    game_urls.append(full_url)
        
        # Try alternative patterns if no data-game-link found
        if not game_urls:
            self.logger.info("Trying alternative link patterns...")
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/nfl/game/' in href:
                    full_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    if full_url not in game_urls:
                        game_urls.append(full_url)
        
        self.logger.info(f"Found {len(game_urls)} total game URLs")
        return game_urls
    
    async def parse_game_data(self, url: str) -> dict:
        """Parse individual game page to extract team names and spread percentages."""
        try:
            html_content = await self.fetch_with_retry(url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract team names - selector: span.NzyJW.NMnSM
            team_spans = soup.find_all('span', class_=lambda x: x and 'NzyJW' in str(x) and 'NMnSM' in str(x))
            teams = []
            
            for span in team_spans:
                text = span.get_text(strip=True)
                if text and len(text) > 1:
                    # Extract mascot name (last word)
                    mascot = text.split()[-1] if ' ' in text else text
                    teams.append(mascot)
            
            if len(teams) < 2:
                self.logger.warning(f"Could not find 2 teams for {url}")
                return None
            
            away_team = teams[0]
            home_team = teams[1]
            
            # Extract spreads - selector: div.matchupPredictor__teamValue
            # Away spread: div.matchupPredictor__teamValue--b
            # Home spread: div.matchupPredictor__teamValue--a
            away_spread = None
            home_spread = None
            
            # Find away spread
            away_spread_elem = soup.select_one('div.matchupPredictor__teamValue--b div')
            if away_spread_elem:
                spread_text = away_spread_elem.get_text(strip=True)
                # Remove % symbol if present
                spread_text = spread_text.replace('%', '').strip()
                try:
                    away_spread = float(spread_text)
                except ValueError:
                    pass
            
            # Find home spread
            home_spread_elem = soup.select_one('div.matchupPredictor__teamValue--a div')
            if home_spread_elem:
                spread_text = home_spread_elem.get_text(strip=True)
                # Remove % symbol if present
                spread_text = spread_text.replace('%', '').strip()
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
            return {"website": "espn", "total": 0, "games": []}
        
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
            "website": "espn",
            "total": len(games),
            "games": games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with ESPNScraper(max_concurrent_requests=50) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("espn_games.json", league="nfl")
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

