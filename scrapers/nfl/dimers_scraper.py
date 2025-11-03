"""Scraper for Dimers.com NFL predictions."""

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


class DimersScraper(BaseScraper):
    """Scraper for Dimers.com NFL predictions."""
    
    def __init__(self, max_concurrent_requests: int = 100):
        """Initialize Dimers scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.dimers.com"
        self.schedule_url = "https://www.dimers.com/bet-hub/nfl/schedule"
    
    async def get_game_urls(self) -> list[str]:
        """Extract game URLs from the main schedule page."""
        self.logger.info("Fetching main schedule page...")
        html_content = await self.fetch_with_retry(self.schedule_url)
        
        if not html_content:
            self.logger.error("Failed to fetch main schedule page")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        game_urls = []
        
        # Look for game links - links with href="/bet-hub/nfl/schedule/"
        game_links = soup.find_all('a', href=re.compile(r'/bet-hub/nfl/schedule/\d+_\d+'))
        
        # Also try the specific selector mentioned
        if not game_links:
            game_links = soup.find_all('a', class_='game-link')
        
        self.logger.info(f"Found {len(game_links)} game links")
        
        for link in game_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                if full_url not in game_urls:
                    game_urls.append(full_url)
        
        self.logger.info(f"Found {len(game_urls)} total game URLs")
        return game_urls
    
    async def parse_game_data(self, url: str) -> dict:
        """Parse individual game page to extract team names and predicted scores."""
        try:
            html_content = await self.fetch_with_retry(url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract team names - look for spans with team names
            # Selector: body > app-root > main > app-match-page > div.main-column > app-match-page-block > div > app-match-page-header > div.main-row > div:nth-child(1) > span
            away_team = None
            home_team = None
            
            # Try to find team name spans
            team_spans = soup.find_all('span', class_=lambda x: x and 'ng' in str(x) or True)
            # Look for spans in the match page header
            header_div = soup.find('div', class_=lambda x: x and 'main-row' in str(x).lower() if x else False)
            
            if header_div:
                spans = header_div.find_all('span')
                if len(spans) >= 2:
                    away_team = spans[0].get_text(strip=True)
                    home_team = spans[-1].get_text(strip=True) if len(spans) >= 3 else spans[1].get_text(strip=True)
            
            # If not found, try alternative approach - look for score-row and adjacent spans
            if not away_team or not home_team:
                score_row = soup.find('div', class_=lambda x: x and 'score-row' in str(x).lower() if x else False)
                if score_row:
                    # Find parent div with main-row class
                    main_row = score_row.find_parent('div', class_=lambda x: x and 'main-row' in str(x).lower() if x else False)
                    if main_row:
                        spans = main_row.find_all('span')
                        if len(spans) >= 2:
                            away_team = spans[0].get_text(strip=True)
                            home_team = spans[-1].get_text(strip=True)
            
            # Extract scores
            # Selector: div.score (first is away, last is home)
            scores = []
            score_divs = soup.find_all('div', class_=lambda x: x and 'score' in str(x).lower() if x else False)
            
            for score_div in score_divs:
                score_text = score_div.get_text(strip=True)
                try:
                    score = float(score_text)
                    scores.append(score)
                except ValueError:
                    continue
            
            # Validate data
            if away_team and home_team and len(scores) >= 2:
                # Extract mascot names only (last word)
                away_mascot = away_team.split()[-1] if ' ' in away_team else away_team
                home_mascot = home_team.split()[-1] if ' ' in home_team else home_team
                
                game_data = {
                    'game_id': str(uuid4()),
                    'away_team': away_mascot,
                    'home_team': home_mascot,
                    'predicted_score_away': scores[0],
                    'predicted_score_home': scores[1],
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
                self.logger.warning(f"Incomplete data for {url}: away={away_team}, home={home_team}, scores={len(scores)}")
            
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
            return {"website": "dimers", "total": 0, "games": []}
        
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
            "website": "dimers",
            "total": len(games),
            "games": games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with DimersScraper(max_concurrent_requests=100) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("dimers_games.json", league="nfl")
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

