"""Scraper for OddsShark.com NCAAF predictions."""

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
    """Scraper for OddsShark.com college football predictions."""
    
    def __init__(self, max_concurrent_requests: int = 100):
        """Initialize OddsShark scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.oddsshark.com"
        self.main_url = "https://www.oddsshark.com/ncaaf/odds"
    
    async def get_game_urls(self) -> list[str]:
        """Extract all game URLs from the main odds page."""
        self.logger.info("Fetching main odds page...")
        content = await self.fetch_with_retry(self.main_url)
        
        if not content:
            self.logger.error("Failed to fetch main page")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        game_urls = []
        
        # Try to extract URLs from JSON-LD structured data
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if 'mainEntity' in data and 'itemListElement' in data['mainEntity']:
                    for item in data['mainEntity']['itemListElement']:
                        if 'url' in item:
                            url = item['url']
                            # Remove the #event fragment
                            if '#event' in url:
                                url = url.replace('#event', '')
                            if url not in game_urls:
                                game_urls.append(url)
                self.logger.info(f"Found {len(game_urls)} game URLs from structured data")
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.warning(f"Error parsing JSON-LD: {e}")
        
        self.logger.info(f"Found {len(game_urls)} total game URLs")
        return game_urls
    
    def parse_single_game_container(self, container) -> dict:
        """Parse a single game from a container element."""
        try:
            # Look for team names
            team_elements = container.find_all(['span', 'div', 'td'], string=re.compile(r'[A-Za-z\s]+'))
            teams = []
            
            for element in team_elements:
                text = element.get_text(strip=True)
                # Filter out non-team text
                if len(text) > 2 and not re.match(r'^\d+\.?\d*$', text) and text not in ['vs', 'at', '@']:
                    teams.append(text)
            
            # Look for predicted scores
            score_elements = container.find_all(['span', 'div', 'td'], string=re.compile(r'\d+\.\d+'))
            scores = []
            
            for element in score_elements:
                text = element.get_text(strip=True)
                try:
                    score = float(text)
                    if 0 < score < 100:  # Reasonable score range
                        scores.append(score)
                except ValueError:
                    continue
            
            # Try to match teams and scores
            if len(teams) >= 2 and len(scores) >= 2:
                return {
                    'game_id': str(uuid4()),
                    'away_team': teams[0],
                    'home_team': teams[1],
                    'predicted_score_away': scores[0],
                    'predicted_score_home': scores[1],
                    'scraped_at': datetime.now().isoformat()
                }
            
            return None
            
        except Exception:
            return None
    
    def parse_game_data(self, content: str, url: str) -> dict:
        """Parse game data from individual game page."""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # If this is the main page, parse all games
            if url == self.main_url:
                games = self.parse_games_from_main_page(content)
                return {"multiple_games": games} if games else None
            
            # Find away team
            away_team = None
            away_elements = soup.find_all(['div', 'span'], class_=re.compile(r'away.*city|gc-both-away-city'))
            for element in away_elements:
                if element.get_text(strip=True):
                    away_team = element.get_text(strip=True)
                    break
            
            # Find home team
            home_team = None
            home_elements = soup.find_all(['div', 'span'], class_=re.compile(r'home.*city|gc-both-home-city'))
            for element in home_elements:
                if element.get_text(strip=True):
                    home_team = element.get_text(strip=True)
                    break
            
            # Find predicted scores
            away_score = None
            home_score = None
            
            # Look for score containers
            score_containers = soup.find_all(['div'], class_=re.compile(r'score.*wrapper|gc-score__inner-wrapper'))
            
            for container in score_containers:
                if 'predicted' in container.get_text().lower():
                    score_nums = container.find_all(['div'], class_=re.compile(r'score.*num|gc-score__num'))
                    
                    scores = []
                    for num_element in score_nums:
                        wrapper = num_element.find(['div'], class_=re.compile(r'wrapper|gc-score__num-wrapper'))
                        if wrapper:
                            score_text = wrapper.get_text(strip=True)
                            try:
                                score = float(score_text)
                                scores.append(score)
                            except ValueError:
                                continue
                    
                    if len(scores) >= 2:
                        away_score = scores[0]
                        home_score = scores[1]
                        break
            
            # Alternative method: look for any numbers that might be scores
            if not away_score or not home_score:
                score_pattern = re.compile(r'\b\d+\.\d+\b')
                all_text = soup.get_text()
                potential_scores = score_pattern.findall(all_text)
                
                if len(potential_scores) >= 2:
                    try:
                        away_score = float(potential_scores[0])
                        home_score = float(potential_scores[1])
                    except ValueError:
                        pass
            
            # Validate we have all required data
            if away_team and home_team and away_score is not None and home_score is not None:
                game_data = {
                    'game_id': str(uuid4()),
                    'away_team': away_team,
                    'home_team': home_team,
                    'predicted_score_away': away_score,
                    'predicted_score_home': home_score,
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Validate
                if (self.validate_team_name(game_data['away_team']) and
                    self.validate_team_name(game_data['home_team']) and
                    self.validate_score(game_data['predicted_score_away']) and
                    self.validate_score(game_data['predicted_score_home'])):
                    return game_data
                else:
                    self.logger.warning(f"Invalid game data for {url}")
            else:
                self.logger.warning(f"Incomplete data for {url}")
            
            return None
                
        except Exception as e:
            self.logger.error(f"Error parsing game data from {url}: {e}")
            return None
    
    def parse_games_from_main_page(self, content: str) -> list:
        """Parse all games directly from the main odds page."""
        games = []
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for game containers
            game_containers = soup.find_all(['tr', 'div'], class_=re.compile(r'game|match|row|card'))
            
            for container in game_containers:
                game_data = self.parse_single_game_container(container)
                if game_data:
                    games.append(game_data)
            
            self.logger.info(f"Parsed {len(games)} games from main page")
            return games
            
        except Exception as e:
            self.logger.error(f"Error parsing games from main page: {e}")
            return []
    
    async def scrape_game(self, url: str) -> dict:
        """Scrape individual game data."""
        content = await self.fetch_with_retry(url)
        if not content:
            return None
        
        return self.parse_game_data(content, url)
    
    async def scrape_all_games(self) -> dict:
        """Main scraping function."""
        start_time = time.time()
        
        # First, try to parse from main page
        self.logger.info("Attempting to parse games from main page...")
        main_page_content = await self.fetch_with_retry(self.main_url)
        
        if main_page_content:
            games = self.parse_games_from_main_page(main_page_content)
            if games and len(games) > 0:
                self.logger.info(f"Successfully parsed {len(games)} games from main page")
                duration = time.time() - start_time
                self.logger.info(f"Scraping completed in {duration:.2f}s")
                return {
                    "website": "oddshark",
                    "total": len(games),
                    "games": games
                }
        
        # Fallback: try individual game URLs
        self.logger.info("Main page parsing failed, trying individual game URLs...")
        game_urls = await self.get_game_urls()
        
        if not game_urls:
            self.logger.error("No game URLs found")
            return {"website": "oddshark", "total": 0, "games": []}
        
        self.logger.info(f"Starting to scrape {len(game_urls)} games from individual URLs...")
        
        # Scrape all games in parallel
        tasks = [self.scrape_game(url) for url in game_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        games = []
        for result in results:
            if isinstance(result, dict) and result:
                # Handle multiple games from main page
                if "multiple_games" in result:
                    games.extend(result["multiple_games"])
                else:
                    games.append(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Exception during scraping: {result}")
        
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
        data = await scraper.scrape_all_games()
        
    # Save to JSON file in data/ncaaf/games_scraped/
    output_file = config.get_games_scraped_path("oddshark_games.json", league="ncaaf")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n=== SCRAPING SUMMARY ===")
    print(f"Website: {data['website']}")
    print(f"Total games: {data['total']}")
    print(f"Output file: {output_file}")
    
    if data['games']:
        print(f"\nSample game:")
        print(json.dumps(data['games'][0], indent=2))


if __name__ == "__main__":
    asyncio.run(main())

