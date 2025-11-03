"""Match games across multiple data sources."""

import json
import os
import sys
from typing import Dict, List, Optional

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fuzzywuzzy import fuzz
from utils import Config, get_logger

logger = get_logger(__name__)


class GameMatcher:
    """Match games across multiple prediction sources."""
    
    # Limited set of campus/location identifiers in official university names
    CAMPUS_KEYWORDS = [
        "Berkeley", "Los Angeles", "Santa Barbara", "Santa Cruz", "Riverside", "Davis", "Irvine", "Merced",
        "Boulder", "Colorado Springs",
        "Austin", "Arlington", "Dallas", "El Paso", "San Antonio",
        "Chapel Hill", "Charlotte", "Greensboro", "Wilmington", "Asheville",
        "Amherst", "Lowell", "Boston", "Dartmouth",
        "Las Vegas", "Reno",
        "Manoa", "Hilo",
        "Lafayette", "Monroe", "Shreveport",
        "Birmingham", "Huntsville",
        "Park", "College Station",
        "Storrs", "Kennesaw", "Lubbock"
    ]
    
    def __init__(self, config: Optional[Config] = None, fuzzy_threshold: int = 85):
        """
        Initialize game matcher.
        
        Args:
            config: Application configuration
            fuzzy_threshold: Minimum fuzzy match score (0-100)
        """
        self.config = config or Config.from_env()
        self.fuzzy_threshold = fuzzy_threshold
        self.sheets_data: Optional[Dict] = None
        self.dimers_data: Optional[Dict] = None
        self.oddshark_data: Optional[Dict] = None
        self.espn_data: Optional[Dict] = None
        self.dratings_data: Optional[Dict] = None
        logger.info(f"GameMatcher initialized with fuzzy_threshold={fuzzy_threshold}")
    
    def normalize_team_name(self, team_name: str) -> str:
        """
        Normalize a university name by removing campus identifiers.
        
        Args:
            team_name: Team name to normalize
            
        Returns:
            Normalized team name
        """
        if not team_name:
            return team_name
        
        name = team_name.strip()
        
        # Replace "University at" → "University of"
        if name.lower().startswith("university at "):
            name = "University of " + name[14:]
        
        # Remove everything after a comma
        if ',' in name:
            name = name.split(',')[0]
        
        # Remove everything after ' at ' (if used as delimiter)
        if ' at ' in name.lower():
            at_pos = name.lower().find(' at ')
            name = name[:at_pos]
        
        # Remove known campus identifiers
        for keyword in self.CAMPUS_KEYWORDS:
            if name.endswith(" " + keyword):
                name = name[: -len(keyword) - 1]
            elif f" {keyword} " in name:
                name = name.replace(f" {keyword} ", " ")
        
        return name.strip()
    
    def load_json_file(self, file_path: str) -> Dict:
        """Load JSON data from file."""
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                if not isinstance(data, dict):
                    logger.warning(f"Invalid data structure in {file_path}")
                    return {}
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            return {}
    
    def find_matching_game(
        self, 
        away_team: str, 
        home_team: str, 
        games_list: List[Dict]
    ) -> Optional[Dict]:
        """
        Find a matching game using normalization and fuzzy matching.
        
        Args:
            away_team: Away team name to match
            home_team: Home team name to match
            games_list: List of games to search
            
        Returns:
            Matching game dict or None
        """
        if not games_list or not isinstance(games_list, list):
            return None
        
        normalized_away = self.normalize_team_name(away_team)
        normalized_home = self.normalize_team_name(home_team)
        
        # Try exact match first
        for game in games_list:
            if not isinstance(game, dict):
                continue
            if 'away_team' not in game or 'home_team' not in game:
                logger.debug(f"Game missing team keys: {game}")
                continue
            
            try:
                game_away = self.normalize_team_name(game['away_team'])
                game_home = self.normalize_team_name(game['home_team'])
                
                if normalized_away == game_away and normalized_home == game_home:
                    return game
            except Exception as e:
                logger.debug(f"Error processing game: {e}")
                continue
        
        # Fallback: fuzzy matching
        for game in games_list:
            if not isinstance(game, dict):
                continue
            if 'away_team' not in game or 'home_team' not in game:
                continue
            
            try:
                game_away = self.normalize_team_name(game['away_team'])
                game_home = self.normalize_team_name(game['home_team'])
                
                if (fuzz.token_set_ratio(normalized_away, game_away) >= self.fuzzy_threshold and
                    fuzz.token_set_ratio(normalized_home, game_home) >= self.fuzzy_threshold):
                    return game
            except Exception as e:
                logger.debug(f"Error in fuzzy matching: {e}")
                continue
        
        logger.debug(f"No match found for {away_team} @ {home_team}")
        return None
    
    def load_all_data(self):
        """Load all JSON files from games_scraped/ and llm_university/, preferring LLM-processed versions."""
        # Load sheets data - try LLM processed version first, fallback to regular
        self.sheets_data = self.load_json_file(self.config.get_llm_university_path('sheets_games_llm.json'))
        if not self.sheets_data or not self.sheets_data.get('games'):
            self.sheets_data = self.load_json_file(self.config.get_games_scraped_path('sheets_games.json', league="ncaaf"))
        
        # Load all scraper outputs - try LLM processed versions first (from llm_university), fallback to scraped versions
        self.dimers_data = self.load_json_file(self.config.get_llm_university_path('dimers_games_llm.json'))
        if not self.dimers_data or not self.dimers_data.get('games'):
            self.dimers_data = self.load_json_file(self.config.get_games_scraped_path('dimers_games.json', league="ncaaf"))
        
        self.oddshark_data = self.load_json_file(self.config.get_llm_university_path('oddshark_games_llm.json'))
        if not self.oddshark_data or not self.oddshark_data.get('games'):
            self.oddshark_data = self.load_json_file(self.config.get_games_scraped_path('oddshark_games.json', league="ncaaf"))
        
        self.espn_data = self.load_json_file(self.config.get_llm_university_path('espn_games_llm.json'))
        if not self.espn_data or not self.espn_data.get('games'):
            self.espn_data = self.load_json_file(self.config.get_games_scraped_path('espn_games.json', league="ncaaf"))
        
        self.dratings_data = self.load_json_file(self.config.get_llm_university_path('dratings_games_llm.json'))
        if not self.dratings_data or not self.dratings_data.get('games'):
            self.dratings_data = self.load_json_file(self.config.get_games_scraped_path('dratings_games.json', league="ncaaf"))
        
        logger.info("All data files loaded")
    
    def match_games(self) -> Dict:
        """Match games across all data sources."""
        if not self.sheets_data or not isinstance(self.sheets_data, dict):
            logger.error("No sheets data available")
            return {}
        
        if 'games' not in self.sheets_data or not isinstance(self.sheets_data['games'], list):
            logger.error("Invalid sheets data structure")
            return {}
        
        results = {
            "sheets_total": len(self.sheets_data['games']),
            "dimers_matched": 0,
            "oddshark_matched": 0,
            "espn_matched": 0,
            "dratings_matched": 0,
            "matched_sheets_rows": {}
        }
        
        # Prepare source data
        sources = {
            'dimers': self.dimers_data.get('games', []) if self.dimers_data else [],
            'oddshark': self.oddshark_data.get('games', []) if self.oddshark_data else [],
            'espn': self.espn_data.get('games', []) if self.espn_data else [],
            'dratings': self.dratings_data.get('games', []) if self.dratings_data else []
        }
        
        for sheet_game in self.sheets_data['games']:
            if not isinstance(sheet_game, dict):
                logger.warning(f"Invalid sheet game format: {sheet_game}")
                continue
            
            if 'away_team' not in sheet_game or 'home_team' not in sheet_game or 'row_number' not in sheet_game:
                logger.warning(f"Sheet game missing required keys: {sheet_game}")
                continue
            
            try:
                away_team = sheet_game['away_team']
                home_team = sheet_game['home_team']
                row_number = str(sheet_game['row_number'])
                
                normalized_away = self.normalize_team_name(away_team)
                normalized_home = self.normalize_team_name(home_team)
                
                matched_game = {
                    "sheets": {
                        "away_team": normalized_away, 
                        "home_team": normalized_home
                    }
                }
                
                for source_name, games_list in sources.items():
                    match = self.find_matching_game(away_team, home_team, games_list)
                    if match:
                        try:
                            normalized_match = match.copy()
                            normalized_match['away_team'] = self.normalize_team_name(match.get('away_team', ''))
                            normalized_match['home_team'] = self.normalize_team_name(match.get('home_team', ''))
                            matched_game[source_name] = normalized_match
                            results[f"{source_name}_matched"] += 1
                        except Exception as e:
                            logger.warning(f"Error normalizing match from {source_name}: {e}")
                
                results["matched_sheets_rows"][row_number] = matched_game
            
            except Exception as e:
                logger.error(f"Error processing sheet game: {e}")
                continue
        
        logger.info(f"Matching complete: {results['dimers_matched']} Dimers, "
                   f"{results['oddshark_matched']} OddShark, "
                   f"{results['espn_matched']} ESPN, "
                   f"{results['dratings_matched']} DRatings")
        
        return results
    
    def save_results(self, results: Dict, output_file: str = 'matched_games.json'):
        """Save the matched results to JSON in data/ncaaf/."""
        try:
            output_path = self.config.get_data_path(output_file, league="ncaaf")
            
            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(results, file, indent=2, ensure_ascii=False)
            
            logger.info(f"Results saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def match_all_games(self):
        """Execute the full matching workflow (alias for run())."""
        return self.run()
    
    def run(self):
        """Execute the full matching workflow."""
        logger.info("Starting matching workflow...")
        self.load_all_data()
        results = self.match_games()
        self.save_results(results)
        
        print(f"\n=== MATCHING SUMMARY ===")
        print(f"Matched {results['sheets_total']} games:")
        print(f"  Dimers: {results['dimers_matched']}")
        print(f"  OddShark: {results['oddshark_matched']}")
        print(f"  ESPN: {results['espn_matched']}")
        print(f"  DRatings: {results['dratings_matched']}")
        
        return results


def main():
    """Main function."""
    matcher = GameMatcher()
    matcher.run()


if __name__ == "__main__":
    main()
