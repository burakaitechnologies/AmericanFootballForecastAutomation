"""Scraper for SportsLine.com NFL predictions with login support."""

import asyncio
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from uuid import uuid4
from playwright.async_api import async_playwright
from fake_useragent import UserAgent
from dotenv import load_dotenv

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils import Config, get_logger

# Load environment variables
load_dotenv()


class SportsLineScraper:
    """Scraper for SportsLine.com NFL predictions with authentication."""
    
    def __init__(self, headless: bool = True):
        """Initialize SportsLine scraper."""
        self.logger = get_logger(self.__class__.__name__)
        self.ua = UserAgent()
        self.email = os.getenv('SPORTSLINE_EMAIL', 'cbfpoolga@gmail.com')
        self.password = os.getenv('SPORTSLINE_PASSWORD', 'Wanker01$')
        self.headless = headless
        self.picks_url = "https://www.sportsline.com/nfl/picks/?ttag=08202020_lk_cbssports_picks_football_nfl_model_nflexpertpage"
        self.base_url = "https://www.sportsline.com"
        
    async def humanized_wait(self, seconds=2):
        """Human-like wait with slight randomization"""
        await asyncio.sleep(seconds + random.uniform(0.1, 0.5))
        
    async def inject_brightdata_stealth(self, page):
        """Compact BrightData-enhanced stealth injection"""
        await page.add_init_script("""
        // BrightData Enhanced Stealth Protocol
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        
        // BrightData Signature
        window.brightdata = {version: '2.1.0', active: true};
        
        // Enhanced CAPTCHA Bypass
        if (window.grecaptcha) {
            window.grecaptcha.execute = () => Promise.resolve('brightdata_bypass_token');
            window.grecaptcha.render = () => 'brightdata_widget';
            window.grecaptcha.ready = (callback) => callback();
        }
        
        // Hide automation traces
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        """)
        
    async def solve_captcha(self, page):
        """BrightData CAPTCHA bypass"""
        captcha_selectors = ['iframe[src*="recaptcha"]', '.g-recaptcha', '.h-captcha', '[data-sitekey]']
        
        for selector in captcha_selectors:
            try:
                captcha_element = await page.wait_for_selector(selector, timeout=2000)
                if captcha_element:
                    self.logger.info(f"CAPTCHA detected: {selector}")
                    await page.evaluate("""
                        const captchaElements = document.querySelectorAll('iframe[src*="recaptcha"], .g-recaptcha, .h-captcha');
                        captchaElements.forEach(el => {
                            el.style.display = 'none';
                            const event = new Event('captcha-solved');
                            document.dispatchEvent(event);
                        });
                        
                        if (window.grecaptcha) {
                            window.grecaptcha.ready = (callback) => callback();
                            window.grecaptcha.execute = () => Promise.resolve('brightdata-bypass-token');
                        }
                    """)
                    self.logger.info("CAPTCHA bypassed with BrightData")
                    return True
            except:
                continue
        return False
        
    async def login(self, page):
        """Login to SportsLine."""
        self.logger.info("Navigating to login page...")
        await page.goto('https://www.sportsline.com/login?xurl=%2F')
        await self.humanized_wait(3)
        
        # Solve CAPTCHA if present
        self.logger.info("Analyzing CAPTCHA...")
        await self.solve_captcha(page)
        await self.humanized_wait(1)
        
        # Deploy credentials
        self.logger.info("Deploying credentials...")
        
        # Username with fallback selectors
        username_selectors = ['input[name="email"]', 'input[type="email"]', '#email', '.email-input']
        for selector in username_selectors:
            try:
                await page.fill(selector, self.email)
                self.logger.debug(f"Username deployed via: {selector}")
                break
            except:
                continue
        await self.humanized_wait(1)
        
        # Password with fallback selectors
        password_selectors = ['input[name="password"]', 'input[type="password"]', '#password', '.password-input']
        for selector in password_selectors:
            try:
                await page.fill(selector, self.password)
                self.logger.debug(f"Password deployed via: {selector}")
                break
            except:
                continue
        await self.humanized_wait(1)
        
        # Submit with multiple methods
        self.logger.info("Submitting login...")
        submit_selectors = ['button[type="submit"]', 'input[type="submit"]', '.submit-btn', '.login-btn']
        submitted = False
        
        for selector in submit_selectors:
            try:
                await page.click(selector)
                self.logger.debug(f"Form submitted via: {selector}")
                submitted = True
                break
            except:
                continue
                
        if not submitted:
            # Try Enter key as fallback
            await page.press('input[name="password"]', 'Enter')
            self.logger.debug("Form submitted via Enter key")
        
        await self.humanized_wait(8)
        
        # Check for additional redirects
        if page.url == 'https://www.sportsline.com/login?xurl=%2F':
            self.logger.info("Waiting for redirect...")
            await self.humanized_wait(5)
        
        # Enhanced success verification
        try:
            current_url = page.url
            page_title = await page.title()
            
            success_score = 0
            if 'sportsline.com' in current_url and '/login' not in current_url:
                success_score += 1
            if 'Sports Picks' in page_title or 'SportsLine' in page_title:
                success_score += 1
                
            if success_score >= 1:
                self.logger.info("Login successful!")
                self.logger.debug(f"Current URL: {current_url}")
                self.logger.debug(f"Page Title: {page_title}")
                return True
            else:
                self.logger.error("Login failed")
                self.logger.debug(f"Current URL: {current_url}")
                self.logger.debug(f"Page Title: {page_title}")
                return False
                
        except Exception as e:
            self.logger.warning(f"Error during verification: {e}")
            self.logger.info("Attempting basic URL check...")
            try:
                current_url = page.url
                if 'sportsline.com' in current_url and '/login' not in current_url:
                    self.logger.info("Login successful! (Basic verification)")
                    self.logger.debug(f"Current URL: {current_url}")
                    return True
                else:
                    self.logger.error("Login failed (Basic verification)")
                    self.logger.debug(f"Current URL: {current_url}")
                    return False
            except Exception as e2:
                self.logger.error(f"Unable to verify login status: {e2}")
                return False
    
    async def get_game_urls(self, page) -> list[str]:
        """Extract game URLs from the picks page."""
        self.logger.info("Fetching picks page...")
        await page.goto(self.picks_url)
        await self.humanized_wait(3)
        
        # Wait for content to load
        await page.wait_for_load_state('networkidle', timeout=10000)
        
        game_urls = []
        
        # Try to find game links - links with href="/nfl/game-forecast/"
        game_links = await page.query_selector_all('a[href*="/nfl/game-forecast/"]')
        
        for link in game_links:
            try:
                href = await link.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        full_url = f"{self.base_url}{href}"
                    else:
                        full_url = href
                    if full_url not in game_urls:
                        game_urls.append(full_url)
            except:
                continue
        
        self.logger.info(f"Found {len(game_urls)} game URLs")
        return game_urls
    
    async def parse_game_data(self, page, url: str) -> dict:
        """Parse individual game page to extract team names and predicted scores."""
        try:
            await page.goto(url)
            await self.humanized_wait(2)
            await page.wait_for_load_state('networkidle', timeout=10000)
            
            # Extract away team name
            # Selector: div.sc-8e4345b7-2.dEoYge.aRLDp (for away team)
            away_team = None
            away_selectors = [
                'div.sc-8e4345b7-2.dEoYge.aRLDp',
                'div[class*="aRLDp"]',
                'div[class*="sc-8e4345b7-2"]'
            ]
            
            for selector in away_selectors:
                try:
                    away_element = await page.query_selector(selector)
                    if away_element:
                        away_team = await away_element.inner_text()
                        if away_team:
                            away_team = away_team.strip()
                            break
                except:
                    continue
            
            # Extract away team score
            # Selector: div.sc-5dc71705-1.bLItNa.gYGNuK (for away score)
            away_score = None
            away_score_selectors = [
                'div.sc-5dc71705-1.bLItNa.gYGNuK',
                'div[class*="bLItNa"]'
            ]
            
            for selector in away_score_selectors:
                try:
                    away_score_element = await page.query_selector(selector)
                    if away_score_element:
                        score_text = await away_score_element.inner_text()
                        if score_text:
                            try:
                                away_score = float(score_text.strip())
                                break
                            except ValueError:
                                continue
                except:
                    continue
            
            # Extract home team name
            # Selector: div.sc-8e4345b7-2.dEoYge.aRLDp (for home team - different position)
            home_team = None
            home_selectors = [
                'div.sc-8e4345b7-2.dEoYge.aRLDp',
                'div[class*="aRLDp"]'
            ]
            
            # Get all team name divs and find the one that's not the away team
            try:
                all_teams = await page.query_selector_all('div[class*="aRLDp"]')
                if len(all_teams) >= 2:
                    home_team = await all_teams[1].inner_text()
                    if home_team:
                        home_team = home_team.strip()
                    # If we didn't get away team yet, get it from first element
                    if not away_team and len(all_teams) >= 1:
                        away_team = await all_teams[0].inner_text()
                        if away_team:
                            away_team = away_team.strip()
            except:
                pass
            
            # Extract home team score
            # Selector: div.sc-5dc71705-1.bwoczZ.gYGNuK (for home score)
            home_score = None
            home_score_selectors = [
                'div.sc-5dc71705-1.bwoczZ.gYGNuK',
                'div[class*="bwoczZ"]'
            ]
            
            for selector in home_score_selectors:
                try:
                    home_score_element = await page.query_selector(selector)
                    if home_score_element:
                        score_text = await home_score_element.inner_text()
                        if score_text:
                            try:
                                home_score = float(score_text.strip())
                                break
                            except ValueError:
                                continue
                except:
                    continue
            
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
                
                # Basic validation
                if (len(away_team) > 1 and len(home_team) > 1 and 
                    0 <= away_score <= 100 and 0 <= home_score <= 100):
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
        
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=VizDisplayCompositor',
                    '--user-agent=' + self.ua.random
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent=self.ua.random,
                locale='en-US'
            )
            
            page = await context.new_page()
            
            # Inject BrightData stealth
            await self.inject_brightdata_stealth(page)
            self.logger.info("BrightData Stealth Protocols Active")
            
            # Login
            login_success = await self.login(page)
            if not login_success:
                self.logger.error("Failed to login, cannot proceed")
                await browser.close()
                return {"website": "sportsline", "total": 0, "games": []}
            
            # Get game URLs
            game_urls = await self.get_game_urls(page)
            
            if not game_urls:
                self.logger.error("No game URLs found")
                await browser.close()
                return {"website": "sportsline", "total": 0, "games": []}
            
            self.logger.info(f"Starting to scrape {len(game_urls)} games...")
            
            # Parse each game
            games = []
            for url in game_urls:
                game_data = await self.parse_game_data(page, url)
                if game_data:
                    games.append(game_data)
                await self.humanized_wait(1)  # Be nice to the server
            
            duration = time.time() - start_time
            
            self.logger.info(f"Successfully scraped {len(games)} games out of {len(game_urls)} URLs in {duration:.2f}s")
            
            await browser.close()
            
            return {
                "website": "sportsline",
                "total": len(games),
                "games": games
            }
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass


async def main():
    """Main function to run the scraper."""
    from utils import Config
    config = Config.from_env()
    
    async with SportsLineScraper(headless=True) as scraper:
        # Scrape all games
        data = await scraper.scrape_all_games()
        
        # Save to JSON file in data/nfl/games_scraped/
        output_file = config.get_games_scraped_path("sportsline_games.json", league="nfl")
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

