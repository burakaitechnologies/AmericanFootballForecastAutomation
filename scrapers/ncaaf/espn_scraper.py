"""Scraper for ESPN.com NCAAF predictions."""

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
    """Scraper for ESPN.com college football predictions."""
    
    def __init__(self, max_concurrent_requests: int = 50):
        """Initialize ESPN scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.espn.com"
        self.odds_url = "https://www.espn.com/college-football/odds"
    
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
                game_urls.append(full_url)
        
        # Try alternative patterns if no data-game-link found
        if not game_urls:
            self.logger.info("Trying alternative link patterns...")
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href')
                if href and '/game/' in href:
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
            
            # Extract team names
            teams = []
            team_spans = soup.find_all('span', class_='tWudT cktOY mXfK GsdWP FMvI')
            
            for span in team_spans:
                text = span.get_text(strip=True)
                if text and len(text) > 1:
                    teams.append(text)
            
            # Alternative: Look for team name patterns if specific classes not found
            if len(teams) < 2:
                team_elements = soup.find_all(['span', 'div'], class_=re.compile(r'team|Team'))
                for element in team_elements:
                    text = element.get_text(strip=True)
                    if (text and len(text) > 2 and 
                        re.search(r'[A-Za-z]', text) and 
                        text.lower() not in ['team', 'teams', 'vs', 'at', 'odds', 'spread', 'total']):
                        if text not in teams:
                            teams.append(text)
            
            # Extract spread percentages
            spreads = []
            
            # Method 1: Look for matchupPredictor divs
            predictor_divs = soup.find_all('div', class_=re.compile(r'matchupPredictor'))
            
            for predictor_div in predictor_divs:
                # Look for direct text matches
                percentage_divs = predictor_div.find_all('div', string=re.compile(r'\d+\.\d+'))
                
                for div in percentage_divs:
                    text = div.get_text(strip=True)
                    if re.match(r'^\d+\.\d+$', text):
                        # Check if this div or its parent contains percentage indicator
                        parent_text = div.parent.get_text(strip=True) if div.parent else ''
                        suffix_div = div.find('div', class_=re.compile(r'suffix|percentage'))
                        if ('%' in parent_text or (suffix_div and '%' in suffix_div.get_text())):
                            try:
                                spread_value = float(text)
                                if 0 <= spread_value <= 100:
                                    spreads.append(spread_value)
                            except ValueError:
                                continue
                
                # Also look for spans or other elements within predictor div
                if not spreads:
                    for elem in predictor_div.find_all(['div', 'span', 'p']):
                        text = elem.get_text(strip=True)
                        match = re.search(r'(\d+\.\d+)%', text)
                        if match:
                            try:
                                spread_value = float(match.group(1))
                                if 0 <= spread_value <= 100:
                                    spreads.append(spread_value)
                            except ValueError:
                                continue
            
            # Method 2: Look for percentage patterns in text nodes
            if len(spreads) < 2:
                percentage_elements = soup.find_all(string=re.compile(r'\d+\.\d+\s*%'))
                for element in percentage_elements:
                    parent = element.parent
                    if parent:
                        text = element.strip()
                        match = re.search(r'(\d+\.\d+)', text)
                        if match:
                            try:
                                spread_value = float(match.group(1))
                                if 0 <= spread_value <= 100:
                                    spreads.append(spread_value)
                            except ValueError:
                                continue
                # Remove duplicates and limit to 2
                spreads = list(dict.fromkeys(spreads))[:2]
            
            # Method 3: Look for any numeric percentages in the document structure
            if len(spreads) < 2:
                all_text = soup.get_text()
                # Find all percentage patterns
                percentage_matches = re.findall(r'(\d+\.\d+)%', all_text)
                for match in percentage_matches:
                    try:
                        spread_value = float(match)
                        if 0 <= spread_value <= 100 and spread_value not in spreads:
                            spreads.append(spread_value)
                            if len(spreads) >= 2:
                                break
                    except ValueError:
                        continue
            
            # Validate data
            if len(teams) >= 2 and len(spreads) >= 2:
                game_data = {
                    'game_id': str(uuid4()),
                    'away_team': teams[0],
                    'home_team': teams[1],
                    'spread_away': spreads[0],
                    'spread_home': spreads[1],
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Validate
                if (self.validate_team_name(game_data['away_team']) and
                    self.validate_team_name(game_data['home_team']) and
                    self.validate_score(game_data['spread_away']) and
                    self.validate_score(game_data['spread_home'])):
                    return game_data
                else:
                    self.logger.warning(f"Invalid game data for {url}")
            else:
                self.logger.warning(f"Incomplete data for {url}: teams={len(teams)}, spreads={len(spreads)}")
            
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
        
    # Save to JSON file in data/ncaaf/games_scraped/
    output_file = config.get_games_scraped_path("espn_games.json", league="ncaaf")
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

