"""Scraper for NBC Sports Florio/Simms NFL predictions."""

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
from utils.nfl_week import get_current_nfl_week


class FlorioSimmsScraper(BaseScraper):
    """Scraper for NBC Sports Florio/Simms NFL predictions."""
    
    def __init__(self, max_concurrent_requests: int = 10):
        """Initialize Florio/Simms scraper."""
        super().__init__(max_concurrent_requests)
        self.base_url = "https://www.nbcsports.com"
    
    def get_url_for_week(self, week: int, year: int = None) -> str:
        """
        Construct URL for the given NFL week.
        
        Args:
            week: NFL week number (1-18)
            year: Year (defaults to current year)
            
        Returns:
            Full URL for the picks page
        """
        if year is None:
            year = datetime.now().year
        
        return f"https://www.nbcsports.com/nfl/profootballtalk/rumor-mill/news/pfts-week-{week}-{year}-nfl-picks-florio-vs-simms"
    
    async def parse_article_content(self, html_content: str) -> tuple[list, list]:
        """
        Parse the article content to extract Florio and Simms predictions.
        
        Args:
            html_content: HTML content of the page
            
        Returns:
            Tuple of (florio_games, simms_games) lists
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the article body
        article_body = soup.select_one('div.RichTextArticleBody.RichTextBody')
        if not article_body:
            # Try alternative selector
            article_body = soup.select_one('body > div.ArticlePage-content > div.ArticlePage-columns > div.ArticlePage-main > div.ArticlePage-articleBody > div.RichTextArticleBody.RichTextBody')
        
        if not article_body:
            self.logger.error("Could not find article body")
            return [], []
        
        florio_games = []
        simms_games = []
        
        # Get all paragraph text
        paragraphs = article_body.find_all('p')
        
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            # Check for Florio's pick
            florio_match = re.search(r"Florio['\u2019]s pick:\s*([A-Za-z0-9\s]+?)\s+(\d+),\s+([A-Za-z0-9\s]+?)\s+(\d+)", text, re.IGNORECASE)
            if florio_match:
                team1 = florio_match.group(1).strip()
                score1 = float(florio_match.group(2))
                team2 = florio_match.group(3).strip()
                score2 = float(florio_match.group(4))
                
                # Determine away/home based on context (usually first team is away if "at" is mentioned)
                # For now, we'll use the order as-is and let the matcher handle it
                # But we need to extract the matchup info from nearby paragraphs
                away_team, home_team = self._determine_away_home(team1, team2, p)
                
                if away_team and home_team:
                    florio_games.append({
                        'game_id': str(uuid4()),
                        'away_team': away_team,
                        'home_team': home_team,
                        'predicted_score_away': score1 if away_team == team1 else score2,
                        'predicted_score_home': score2 if away_team == team1 else score1,
                        'scraped_at': datetime.now().isoformat()
                    })
            
            # Check for Simms's pick
            simms_match = re.search(r"Simms['\u2019]s pick:\s*([A-Za-z0-9\s]+?)\s+(\d+),\s+([A-Za-z0-9\s]+?)\s+(\d+)", text, re.IGNORECASE)
            if simms_match:
                team1 = simms_match.group(1).strip()
                score1 = float(simms_match.group(2))
                team2 = simms_match.group(3).strip()
                score2 = float(simms_match.group(4))
                
                away_team, home_team = self._determine_away_home(team1, team2, p)
                
                if away_team and home_team:
                    simms_games.append({
                        'game_id': str(uuid4()),
                        'away_team': away_team,
                        'home_team': home_team,
                        'predicted_score_away': score1 if away_team == team1 else score2,
                        'predicted_score_home': score2 if away_team == team1 else score1,
                        'scraped_at': datetime.now().isoformat()
                    })
        
        return florio_games, simms_games
    
    def _determine_away_home(self, team1: str, team2: str, current_paragraph) -> tuple[str, str]:
        """
        Determine which team is away and which is home by looking at nearby paragraphs.
        
        Args:
            team1: First team name from pick
            team2: Second team name from pick
            current_paragraph: Current paragraph element
            
        Returns:
            Tuple of (away_team, home_team)
        """
        # Look for a previous paragraph with matchup info (e.g., "Ravens (-7.5) at Dolphins")
        # Check previous siblings
        prev_p = current_paragraph.find_previous_sibling('p')
        if prev_p:
            matchup_text = prev_p.get_text()
            # Look for "Team1 at Team2" or "Team1 (-X) at Team2" pattern
            at_match = re.search(r"([A-Za-z0-9\s]+?)\s+\([^)]+\)\s+at\s+([A-Za-z0-9\s]+)", matchup_text)
            if not at_match:
                at_match = re.search(r"([A-Za-z0-9\s]+?)\s+at\s+([A-Za-z0-9\s]+)", matchup_text)
            
            if at_match:
                away = at_match.group(1).strip()
                home = at_match.group(2).strip()
                
                # Match to our team names (handle variations)
                if self._teams_match(away, team1) and self._teams_match(home, team2):
                    return team1, team2
                elif self._teams_match(away, team2) and self._teams_match(home, team1):
                    return team2, team1
        
        # Fallback: assume first team mentioned is away (common pattern)
        return team1, team2
    
    def _teams_match(self, name1: str, name2: str) -> bool:
        """Check if two team names match (handles variations like '49ers' vs 'Niners')."""
        name1_clean = re.sub(r'[^A-Za-z0-9]', '', name1.lower())
        name2_clean = re.sub(r'[^A-Za-z0-9]', '', name2.lower())
        
        # Direct match
        if name1_clean == name2_clean:
            return True
        
        # Check if one contains the other (for partial matches)
        if name1_clean in name2_clean or name2_clean in name1_clean:
            return True
        
        # Special cases
        special_cases = {
            '49ers': ['niners', 'fortyniners'],
            'raiders': ['las vegas'],
            'commanders': ['washington'],
            'texans': ['houston'],
            'ravens': ['baltimore'],
            'dolphins': ['miami'],
            'bills': ['buffalo'],
            'patriots': ['new england', 'ne'],
            'jets': ['new york'],
            'steelers': ['pittsburgh'],
            'browns': ['cleveland'],
            'bengals': ['cincinnati'],
            'colts': ['indianapolis'],
            'jaguars': ['jacksonville'],
            'titans': ['tennessee'],
            'broncos': ['denver'],
            'chiefs': ['kansas city', 'kc'],
            'chargers': ['los angeles', 'la'],
            'cowboys': ['dallas'],
            'giants': ['new york'],
            'eagles': ['philadelphia'],
            'commanders': ['washington'],
            'bears': ['chicago'],
            'lions': ['detroit'],
            'packers': ['green bay'],
            'vikings': ['minnesota'],
            'falcons': ['atlanta'],
            'panthers': ['carolina'],
            'saints': ['new orleans'],
            'buccaneers': ['tampa bay', 'tampa'],
            'cardinals': ['arizona'],
            'rams': ['los angeles', 'la'],
            'seahawks': ['seattle'],
        }
        
        for key, variations in special_cases.items():
            if name1_clean in [key] + variations and name2_clean in [key] + variations:
                return True
        
        return False
    
    async def scrape_all_games(self) -> dict:
        """Main function to scrape all games."""
        start_time = time.time()
        
        # Get current NFL week
        week = get_current_nfl_week()
        year = datetime.now().year
        
        self.logger.info(f"Scraping NFL Week {week} predictions for {year}")
        
        # Construct URL
        url = self.get_url_for_week(week, year)
        self.logger.info(f"Fetching URL: {url}")
        
        # Fetch page
        html_content = await self.fetch_with_retry(url)
        
        if not html_content:
            self.logger.error(f"Failed to fetch page: {url}")
            return {
                "website": "florio_simms",
                "florio": {"website": "florio", "total": 0, "games": []},
                "simms": {"website": "simms", "total": 0, "games": []}
            }
        
        # Parse content
        florio_games, simms_games = await self.parse_article_content(html_content)
        
        duration = time.time() - start_time
        
        self.logger.info(f"Successfully scraped {len(florio_games)} Florio games and {len(simms_games)} Simms games in {duration:.2f}s")
        
        return {
            "website": "florio_simms",
            "florio": {
                "website": "florio",
                "total": len(florio_games),
                "games": florio_games
            },
            "simms": {
                "website": "simms",
                "total": len(simms_games),
                "games": simms_games
            }
        }


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with FlorioSimmsScraper(max_concurrent_requests=10) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save Florio predictions
        florio_file = config.get_games_scraped_path("florio_games.json", league="nfl")
        os.makedirs(os.path.dirname(florio_file), exist_ok=True)
        
        with open(florio_file, 'w', encoding='utf-8') as f:
            json.dump(data['florio'], f, indent=2, ensure_ascii=False)
        
        # Save Simms predictions
        simms_file = config.get_games_scraped_path("simms_games.json", league="nfl")
        
        with open(simms_file, 'w', encoding='utf-8') as f:
            json.dump(data['simms'], f, indent=2, ensure_ascii=False)
        
        scraper.logger.info(f"Florio games scraped: {data['florio']['total']}")
        scraper.logger.info(f"Simms games scraped: {data['simms']['total']}")
        scraper.logger.info(f"Florio data saved to {florio_file}")
        scraper.logger.info(f"Simms data saved to {simms_file}")
        
        # Print summary
        print("\n=== SCRAPING SUMMARY ===")
        print(f"Florio games: {data['florio']['total']}")
        print(f"Simms games: {data['simms']['total']}")
        print(f"Florio output: {florio_file}")
        print(f"Simms output: {simms_file}")
        
        if data['florio']['games']:
            print(f"\nSample Florio game:")
            print(json.dumps(data['florio']['games'][0], indent=2))
        
        if data['simms']['games']:
            print(f"\nSample Simms game:")
            print(json.dumps(data['simms']['games'][0], indent=2))


if __name__ == "__main__":
    asyncio.run(main())

