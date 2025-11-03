"""Scraper for OddsShark.com NFL predictions."""

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


class OddsSharkScraper(BaseScraper):
    """Scraper for OddsShark.com NFL predictions."""
    
    def __init__(self, max_concurrent_requests: int = 100):
        """Initialize OddsShark scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.oddsshark.com"
        self.main_url = "https://www.oddsshark.com/nfl/odds"
    
    async def get_game_urls(self) -> list[str]:
        """Extract game URLs from the main odds page."""
        self.logger.info("Fetching main odds page...")
        content = await self.fetch_with_retry(self.main_url)
        
        if not content:
            self.logger.error("Failed to fetch main page")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        game_urls = []
        
        # Look for matchup links
        matchup_links = soup.find_all('a', class_='matchup-link', href=re.compile(r'/nfl/.*odds'))
        
        for link in matchup_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                if full_url not in game_urls:
                    game_urls.append(full_url)
        
        self.logger.info(f"Found {len(game_urls)} game URLs")
        return game_urls
    
    async def parse_game_data(self, url: str) -> dict:
        """Parse individual game page to extract team names and predicted scores."""
        try:
            html_content = await self.fetch_with_retry(url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract away team name - selector: div.gc-both-away-name
            away_team_elem = soup.find('div', class_='gc-both-away-name')
            away_team = away_team_elem.get_text(strip=True) if away_team_elem else None
            
            # Extract home team name - selector: div.gc-both-home-name
            home_team_elem = soup.find('div', class_='gc-both-home-name')
            home_team = home_team_elem.get_text(strip=True) if home_team_elem else None
            
            # Extract scores using multiple selector strategies
            away_score = None
            home_score = None
            
            # Strategy 1: Try the specific CSS selector path
            scores_container = soup.select_one('#oddsshark-scores')
            if scores_container:
                # Try finding left and right scores
                away_score_elem = scores_container.select_one('div.gc-score__num--left div.gc-score__num-wrapper')
                home_score_elem = scores_container.select_one('div.gc-score__num--right div.gc-score__num-wrapper')
                
                if away_score_elem:
                    try:
                        away_score = float(away_score_elem.get_text(strip=True))
                    except (ValueError, AttributeError):
                        pass
                
                if home_score_elem:
                    try:
                        home_score = float(home_score_elem.get_text(strip=True))
                    except (ValueError, AttributeError):
                        pass
            
            # Strategy 2: If not found, try finding all score wrappers and match by position/class
            if away_score is None or home_score is None:
                # Debug: Check if scores container exists
                if not scores_container:
                    self.logger.debug(f"#oddsshark-scores container not found for {url}")
                
                score_wrappers = soup.find_all('div', class_='gc-score__num-wrapper')
                self.logger.debug(f"Found {len(score_wrappers)} score wrappers for {url}")
                
                if len(score_wrappers) >= 2:
                    # Try to find parent containers to determine left vs right
                    for wrapper in score_wrappers:
                        parent_classes = []
                        parent = wrapper.parent
                        for _ in range(3):  # Check up to 3 levels up
                            if parent:
                                parent_classes.extend(parent.get('class', []))
                                parent = parent.parent
                        
                        score_text = wrapper.get_text(strip=True)
                        try:
                            score_val = float(score_text)
                            # Determine if this is away (left) or home (right) based on parent classes
                            if 'gc-score__num--left' in parent_classes or away_score is None:
                                if away_score is None:
                                    away_score = score_val
                            elif 'gc-score__num--right' in parent_classes or home_score is None:
                                if home_score is None:
                                    home_score = score_val
                        except (ValueError, AttributeError):
                            continue
                
                # If still not found and we have 2 scores, assume first is away, second is home
                if away_score is None and home_score is None and len(score_wrappers) >= 2:
                    try:
                        away_score = float(score_wrappers[0].get_text(strip=True))
                        home_score = float(score_wrappers[1].get_text(strip=True))
                    except (ValueError, AttributeError):
                        pass
            
            # Validate data
            if away_team and home_team and away_score is not None and home_score is not None:
                game_data = {
                    'game_id': str(uuid4()),
                    'away_team': away_team,
                    'home_team': home_team,
                    'predicted_score_away': away_score,
                    'predicted_score_home': home_score,
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Validate using base class methods
                if (self.validate_team_name(game_data['away_team']) and
                    self.validate_team_name(game_data['home_team']) and
                    self.validate_score(game_data['predicted_score_away']) and
                    self.validate_score(game_data['predicted_score_home'])):
                    return game_data
                else:
                    self.logger.warning(f"Invalid game data for {url}")
            else:
                self.logger.warning(f"Incomplete data for {url}: away={away_team}, home={home_team}, away_score={away_score}, home_score={home_score}")
            
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
            return {"website": "oddshark", "total": 0, "games": []}
        
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
            "website": "oddshark",
            "total": len(games),
            "games": games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with OddsSharkScraper(max_concurrent_requests=100) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("oddshark_games.json", league="nfl")
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

