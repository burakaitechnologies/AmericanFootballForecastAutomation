"""Scraper for FantasyNerds.com NFL predictions."""

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


class FantasyNerdsScraper(BaseScraper):
    """Scraper for FantasyNerds.com NFL predictions."""
    
    def __init__(self, max_concurrent_requests: int = 100):
        """Initialize FantasyNerds scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.fantasynerds.com"
        self.picks_url = "https://www.fantasynerds.com/nfl/picks"
    
    async def get_game_urls(self) -> list[str]:
        """Extract game URLs from the main picks page."""
        self.logger.info("Fetching main picks page...")
        html_content = await self.fetch_with_retry(self.picks_url)
        
        if not html_content:
            self.logger.error("Failed to fetch main picks page")
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        game_urls = []
        
        # Look for game links - links with class "btn btn-primary btn-sm btn-block bold"
        # or containing href="/nfl/picks/"
        game_links = soup.find_all('a', href=re.compile(r'/nfl/picks/\d+/\d+'))
        
        # Also try the specific selector mentioned
        if not game_links:
            game_links = soup.select('a.btn.btn-primary.btn-sm.btn-block.bold[href*="/nfl/picks/"]')
        
        self.logger.info(f"Found {len(game_links)} game links")
        
        for link in game_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                if full_url not in game_urls:
                    game_urls.append(full_url)
        
        self.logger.info(f"Found {len(game_urls)} unique game URLs")
        return game_urls
    
    async def parse_game_data(self, url: str) -> dict:
        """Parse individual game page to extract team names and predicted scores."""
        try:
            html_content = await self.fetch_with_retry(url)
            
            if not html_content:
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the paragraph containing game info
            # CSS selector: #fnCanvas > div.well > div > div.col-md-4.col-xs-12 > p
            info_paragraph = soup.select_one('#fnCanvas > div.well > div > div.col-md-4.col-xs-12 > p')
            
            if not info_paragraph:
                # Try alternative selectors
                info_paragraph = soup.find('p', string=re.compile(r'Home Team:'))
                if not info_paragraph:
                    info_paragraph = soup.find('p', string=re.compile(r'Away Team:'))
            
            if not info_paragraph:
                self.logger.warning(f"Could not find game info paragraph for {url}")
                return None
            
            text = info_paragraph.get_text(separator='\n', strip=True)
            
            # Extract away team
            away_match = re.search(r'Away Team:\s*([^(]+)', text, re.IGNORECASE)
            away_team = away_match.group(1).strip() if away_match else None
            
            # Extract home team
            home_match = re.search(r'Home Team:\s*([^(]+)', text, re.IGNORECASE)
            home_team = home_match.group(1).strip() if home_match else None
            
            # Extract projected scores from the HTML
            # The format shows scores after team images: <img> 20  <img> 23
            projected_score_text = info_paragraph.get_text(separator=' ', strip=True)
            
            # Try to extract scores from the projected score section
            score_match = re.search(r'Projected Score:.*?(\d+)\s+(\d+)', projected_score_text, re.IGNORECASE)
            
            if not score_match:
                # Try to find scores in the paragraph HTML directly
                score_text = str(info_paragraph)
                # Look for pattern: number, possibly whitespace, number
                score_match = re.search(r'>\s*(\d+)\s*<.*?>\s*(\d+)\s*<', score_text)
            
            if score_match:
                score1 = float(score_match.group(1))
                score2 = float(score_match.group(2))
                
                # Need to determine which score is away and which is home
                # Based on the example: "Projected Score: <img> 20  <img> 23"
                # First number is away, second is home
                away_score = score1
                home_score = score2
            else:
                self.logger.warning(f"Could not extract scores from {url}")
                return None
            
            # Validate data
            if away_team and home_team:
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
                self.logger.warning(f"Incomplete team data for {url}: away={away_team}, home={home_team}")
            
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
            return {"website": "fantasynerds", "total": 0, "games": []}
        
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
            "website": "fantasynerds",
            "total": len(games),
            "games": games
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with FantasyNerdsScraper(max_concurrent_requests=100) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("fantasynerds_games.json", league="nfl")
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

