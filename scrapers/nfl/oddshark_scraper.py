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
        seen_urls = set()
        
        # Strategy 1: Look for matchup links with class and href pattern
        matchup_links = soup.find_all('a', class_='matchup-link', href=re.compile(r'/nfl/.*odds'))
        self.logger.debug(f"Found {len(matchup_links)} links with class='matchup-link'")
        
        for link in matchup_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                if full_url not in seen_urls:
                    game_urls.append(full_url)
                    seen_urls.add(full_url)
        
        # Strategy 2: Fallback - look for any links with href matching game URL pattern
        if len(game_urls) == 0:
            self.logger.debug("No matchup-link class found, trying fallback strategy...")
            all_nfl_links = soup.find_all('a', href=re.compile(r'/nfl/.*-odds-.*'))
            self.logger.debug(f"Found {len(all_nfl_links)} links matching /nfl/.*-odds- pattern")
            
            for link in all_nfl_links:
                href = link.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    if full_url not in seen_urls:
                        game_urls.append(full_url)
                        seen_urls.add(full_url)
        
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
            
            def is_valid_score(value):
                """Check if a value is a valid score (0-100 range, not odds)."""
                try:
                    score = float(value)
                    # Scores should be between 0 and 100 (typical NFL scores)
                    return 0 <= score <= 100
                except (ValueError, TypeError):
                    return False
            
            def extract_score_from_spans(div_element):
                """Extract score from spans, preferring score-like values over odds."""
                spans = div_element.find_all('span')
                candidate_scores = []
                
                for span in spans:
                    text = span.get_text(strip=True)
                    # Skip if it contains + or - (these are odds)
                    if '+' in text or (text.startswith('-') and len(text) > 3):
                        continue
                    # Try to parse as float
                    if is_valid_score(text):
                        candidate_scores.append(float(text))
                
                # Return the first valid score found
                return candidate_scores[0] if candidate_scores else None
            
            def extract_score_from_text(text):
                """Extract score from text, preferring score-like values (0-100)."""
                # Find all numbers in the text
                numbers = re.findall(r'\d+\.?\d*', text)
                for num_str in numbers:
                    if is_valid_score(num_str):
                        return float(num_str)
                return None
            
            # Strategy 1: Try to find scores in the predicted-score div (primary method)
            predicted_score_div = soup.find('div', class_='predicted-score')
            if predicted_score_div:
                # The predicted-score div contains multiple child divs:
                # - divs with 'desktop-only' or 'mobile-only' classes (skip these)
                # - div with class='highlighted-pick' contains away team score
                # - div without class contains home team score
                # Each score div has spans: [team_shortname, score, odds]
                children = list(predicted_score_div.children)
                score_divs = []
                for child in children:
                    if hasattr(child, 'name') and child.name == 'div':
                        classes = child.get('class', [])
                        # Skip desktop-only and mobile-only divs
                        if 'desktop-only' not in classes and 'mobile-only' not in classes:
                            score_divs.append(child)
                
                self.logger.debug(f"Found {len(score_divs)} score divs in predicted-score")
                
                if len(score_divs) >= 2:
                    # Away team is in the highlighted-pick div
                    away_div = None
                    home_div = None
                    for div in score_divs:
                        classes = div.get('class', [])
                        if 'highlighted-pick' in classes:
                            away_div = div
                        elif not classes or (len(classes) == 0):
                            home_div = div
                    
                    # If we only found one, use position
                    if away_div is None and len(score_divs) >= 1:
                        away_div = score_divs[0]
                    if home_div is None and len(score_divs) >= 2:
                        home_div = score_divs[1]
                    
                    # Extract away score from spans (prefer valid scores)
                    if away_div:
                        away_score = extract_score_from_spans(away_div)
                        # Fallback: try extracting from text if spans didn't work
                        if away_score is None:
                            text = away_div.get_text(strip=True)
                            away_score = extract_score_from_text(text)
                    
                    # Extract home score from spans (prefer valid scores)
                    if home_div:
                        home_score = extract_score_from_spans(home_div)
                        # Fallback: try extracting from text if spans didn't work
                        if home_score is None:
                            text = home_div.get_text(strip=True)
                            home_score = extract_score_from_text(text)
            
            # Strategy 2: Try the specific CSS selector path from #oddsshark-scores (fallback)
            # Note: This container is often empty (React component), but we try it anyway
            if away_score is None or home_score is None:
                scores_container = soup.select_one('#oddsshark-scores')
                if scores_container:
                    # Try finding left and right scores using user's exact selectors
                    away_score_elem = scores_container.select_one('div.gc-score__num--left div.gc-score__num-wrapper')
                    home_score_elem = scores_container.select_one('div.gc-score__num--right div.gc-score__num-wrapper')
                    
                    if away_score_elem and away_score is None:
                        score_text = away_score_elem.get_text(strip=True)
                        if is_valid_score(score_text):
                            try:
                                away_score = float(score_text)
                            except (ValueError, AttributeError):
                                pass
                    
                    if home_score_elem and home_score is None:
                        score_text = home_score_elem.get_text(strip=True)
                        if is_valid_score(score_text):
                            try:
                                home_score = float(score_text)
                            except (ValueError, AttributeError):
                                pass
            
            # Strategy 3: Try finding all score wrappers and match by position/class (fallback)
            if away_score is None or home_score is None:
                score_wrappers = soup.find_all('div', class_='gc-score__num-wrapper')
                
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
                        if not is_valid_score(score_text):
                            continue
                        
                        try:
                            score_val = float(score_text)
                            # Determine if this is away (left) or home (right) based on parent classes
                            if 'gc-score__num--left' in parent_classes and away_score is None:
                                away_score = score_val
                            elif 'gc-score__num--right' in parent_classes and home_score is None:
                                home_score = score_val
                        except (ValueError, AttributeError):
                            continue
                
                # If still not found and we have 2 valid scores, assume first is away, second is home
                if away_score is None and home_score is None and len(score_wrappers) >= 2:
                    valid_scores = []
                    for wrapper in score_wrappers:
                        score_text = wrapper.get_text(strip=True)
                        if is_valid_score(score_text):
                            try:
                                valid_scores.append(float(score_text))
                            except (ValueError, AttributeError):
                                pass
                    
                    if len(valid_scores) >= 2:
                        away_score = valid_scores[0]
                        home_score = valid_scores[1]
            
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
                validation_results = {
                    'away_team': self.validate_team_name(game_data['away_team']),
                    'home_team': self.validate_team_name(game_data['home_team']),
                    'away_score': self.validate_score(game_data['predicted_score_away']),
                    'home_score': self.validate_score(game_data['predicted_score_home'])
                }
                
                if all(validation_results.values()):
                    return game_data
                else:
                    failed_validations = [k for k, v in validation_results.items() if not v]
                    self.logger.warning(f"Invalid game data for {url}: failed validations: {failed_validations}")
                    self.logger.warning(f"  away_team='{game_data['away_team']}', home_team='{game_data['home_team']}'")
                    self.logger.warning(f"  away_score={game_data['predicted_score_away']}, home_score={game_data['predicted_score_home']}")
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


async def debug_specific_url():
    """Debug function to test a specific URL and inspect HTML structure."""
    # Test both the working Arizona-Dallas URL and a failing one
    import sys
    if len(sys.argv) > 2:
        test_url = sys.argv[2]
    else:
        test_url = "https://www.oddsshark.com/nfl/arizona-dallas-odds-november-3-2025-2397574"
    
    async with OddsSharkScraper(max_concurrent_requests=100) as scraper:
        print(f"\n=== DEBUGGING SPECIFIC URL ===")
        print(f"URL: {test_url}\n")
        
        html_content = await scraper.fetch_with_retry(test_url)
        
        if not html_content:
            print("Failed to fetch HTML content")
            return
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for scores container
        scores_container = soup.select_one('#oddsshark-scores')
        print(f"#oddsshark-scores container found: {scores_container is not None}")
        
        if scores_container:
            print(f"\nScores container HTML snippet:")
            print(str(scores_container)[:500])
            print("\n...\n")
        
        # Try the exact selectors provided by user
        print("\n=== Testing user-provided selectors ===")
        away_exact = soup.select_one('#oddsshark-scores > div > div > div > div.gc-score__num.gc-score__num--left > div.gc-score__num-wrapper')
        home_exact = soup.select_one('#oddsshark-scores > div > div > div > div.gc-score__num.gc-score__num--right > div.gc-score__num-wrapper')
        
        print(f"Away (exact selector): {away_exact}")
        if away_exact:
            print(f"  Text: {away_exact.get_text(strip=True)}")
        
        print(f"Home (exact selector): {home_exact}")
        if home_exact:
            print(f"  Text: {home_exact.get_text(strip=True)}")
        
        # Try current selectors
        print("\n=== Testing current selectors ===")
        away_current = scores_container.select_one('div.gc-score__num--left div.gc-score__num-wrapper') if scores_container else None
        home_current = scores_container.select_one('div.gc-score__num--right div.gc-score__num-wrapper') if scores_container else None
        
        print(f"Away (current selector): {away_current}")
        if away_current:
            print(f"  Text: {away_current.get_text(strip=True)}")
        
        print(f"Home (current selector): {home_current}")
        if home_current:
            print(f"  Text: {home_current.get_text(strip=True)}")
        
        # Find all score wrappers
        print("\n=== All score wrappers found ===")
        all_wrappers = soup.find_all('div', class_='gc-score__num-wrapper')
        print(f"Total wrappers: {len(all_wrappers)}")
        for i, wrapper in enumerate(all_wrappers):
            print(f"  {i+1}. Text: '{wrapper.get_text(strip=True)}'")
            # Check parent classes
            parent = wrapper.parent
            parent_classes = []
            for _ in range(5):
                if parent:
                    classes = parent.get('class', [])
                    if classes:
                        parent_classes.append(classes)
                    parent = parent.parent
            print(f"     Parent classes: {parent_classes[:3]}")
        
        # Check for JSON-LD or script tags with data
        print("\n=== Checking for JSON data in script tags ===")
        script_tags = soup.find_all('script', type='application/ld+json')
        print(f"JSON-LD scripts found: {len(script_tags)}")
        
        # Check for inline scripts with game data
        all_scripts = soup.find_all('script')
        print(f"Total script tags: {len(all_scripts)}")
        
        # Search for score-related text in scripts
        for i, script in enumerate(all_scripts[:5]):  # Check first 5 scripts
            script_text = script.string
            if script_text:
                if 'score' in script_text.lower() or '24.0' in script_text or '23.5' in script_text:
                    print(f"\nScript {i+1} contains score-related data:")
                    print(script_text[:500])
                    print("...")
        
        # Check for data attributes or other structures
        print("\n=== Checking for data attributes ===")
        all_divs = soup.find_all('div', class_=lambda x: x and 'score' in str(x).lower() if x else False)
        print(f"Divs with 'score' in class: {len(all_divs)}")
        for div in all_divs[:10]:
            print(f"  Class: {div.get('class')}, Text: '{div.get_text(strip=True)[:100]}'")
        
        # Check predicted-score div in detail
        print("\n=== Inspecting predicted-score div ===")
        predicted_score_divs = soup.find_all('div', class_='predicted-score')
        print(f"Predicted-score divs found: {len(predicted_score_divs)}")
        for i, div in enumerate(predicted_score_divs):
            print(f"\nDiv {i+1}:")
            print(f"  Full HTML: {str(div)[:300]}")
            print(f"  Text: '{div.get_text(strip=True)}'")
            print(f"  Children: {len(list(div.children))}")
            for j, child in enumerate(div.children):
                if hasattr(child, 'get_text'):
                    print(f"    Child {j}: {child.name}, Classes: {child.get('class')}, Text: '{child.get_text(strip=True)[:50]}'")
        
        # Look for all elements containing "24.0" or "23.5"
        print("\n=== Searching for score values directly ===")
        html_str = str(soup)
        if '24.0' in html_str:
            idx = html_str.find('24.0')
            print(f"Found '24.0' at position {idx}")
            print(f"Context: {html_str[max(0, idx-100):idx+100]}")
        if '23.5' in html_str:
            idx = html_str.find('23.5')
            print(f"Found '23.5' at position {idx}")
            print(f"Context: {html_str[max(0, idx-100):idx+100]}")
        
        # Test parsing
        print("\n=== Testing parse_game_data ===")
        result = await scraper.parse_game_data(test_url)
        print(f"Result: {result}")


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
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--debug":
        asyncio.run(debug_specific_url())
    elif len(sys.argv) > 1 and sys.argv[1] == "--test-url":
        # Test a specific URL: python oddshark_scraper.py --test-url <url>
        async def test_url():
            if len(sys.argv) < 3:
                print("Usage: python oddshark_scraper.py --test-url <url>")
                return
            test_url = sys.argv[2]
            async with OddsSharkScraper(max_concurrent_requests=100) as scraper:
                result = await scraper.parse_game_data(test_url)
                print(f"\nResult for {test_url}:")
                print(json.dumps(result, indent=2) if result else "None")
        asyncio.run(test_url())
    else:
        asyncio.run(main())

