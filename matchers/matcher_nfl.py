"""Match NFL games across multiple data sources using simplified mascot name matching."""

import json
import os
import sys
from typing import Dict, List, Optional

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fuzzywuzzy import fuzz
from utils import Config, get_logger

logger = get_logger(__name__)


class NFLGameMatcher:
    """Match NFL games across multiple prediction sources using mascot names."""
    
    def __init__(self, config: Optional[Config] = None, fuzzy_threshold: int = 80):
        """
        Initialize NFL game matcher.
        
        Args:
            config: Application configuration
            fuzzy_threshold: Minimum fuzzy match score (0-100), lower for NFL since names are simpler
        """
        self.config = config or Config.from_env()
        self.fuzzy_threshold = fuzzy_threshold
        self.sheets_data: Optional[Dict] = None
        self.fantasynerds_data: Optional[Dict] = None
        self.sportsline_data: Optional[Dict] = None
        self.florio_data: Optional[Dict] = None
        self.simms_data: Optional[Dict] = None
        self.dimers_data: Optional[Dict] = None
        self.oddshark_data: Optional[Dict] = None
        self.espn_data: Optional[Dict] = None
        self.dratings_data: Optional[Dict] = None
        logger.info(f"NFLGameMatcher initialized with fuzzy_threshold={fuzzy_threshold}")
    
    def normalize_team_name(self, team_name: str) -> str:
        """
        Normalize an NFL team name to mascot (simplified - just lowercase and strip).
        
        Args:
            team_name: Team name to normalize (should already be mascot)
            
        Returns:
            Normalized team name (lowercase, stripped)
        """
        if not team_name:
            return team_name
        
        name = team_name.strip()
        
        # For NFL, names should already be mascots, but handle common variations
        # Remove city names if present (e.g., "Las Vegas Raiders" -> "Raiders")
        # Most scrapers should already extract just mascot, but handle edge cases
        
        # Extract last word if multiple words (in case city name is still there)
        if ' ' in name:
            # Common patterns: "New England Patriots" -> "Patriots"
            # But most should already be just "Patriots"
            parts = name.split()
            # If last word looks like a mascot (capitalized, common team name)
            if len(parts) > 1:
                last_word = parts[-1]
                # Check if last word is a common NFL team name
                nfl_teams = [
                    'Raiders', 'Ravens', 'Bills', 'Bengals', 'Browns', 'Broncos', 'Texans',
                    'Colts', 'Jaguars', 'Chiefs', 'Chargers', 'Dolphins', 'Patriots', 'Jets',
                    'Steelers', 'Titans', 'Cowboys', 'Giants', 'Eagles', 'Commanders',
                    'Bears', 'Lions', 'Packers', 'Vikings', 'Falcons', 'Panthers', 'Saints',
                    'Buccaneers', 'Cardinals', 'Rams', '49ers', 'Seahawks'
                ]
                if any(team.lower() == last_word.lower() for team in nfl_teams):
                    name = last_word
        
        return name.lower().strip()
    
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
        Find a matching game using simplified normalization and fuzzy matching.
        
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
                
                # Try both orderings (away/home and home/away)
                if (normalized_away == game_away and normalized_home == game_home):
                    return game
                # Also try reversed (in case source has them flipped)
                elif (normalized_away == game_home and normalized_home == game_away):
                    # Swap scores if reversed
                    reversed_game = game.copy()
                    reversed_game['away_team'], reversed_game['home_team'] = reversed_game['home_team'], reversed_game['away_team']
                    # Swap scores if present
                    if 'predicted_score_away' in reversed_game and 'predicted_score_home' in reversed_game:
                        reversed_game['predicted_score_away'], reversed_game['predicted_score_home'] = reversed_game['predicted_score_home'], reversed_game['predicted_score_away']
                    # Swap spreads if present
                    if 'spread_away' in reversed_game and 'spread_home' in reversed_game:
                        reversed_game['spread_away'], reversed_game['spread_home'] = reversed_game['spread_home'], reversed_game['spread_away']
                    return reversed_game
            except Exception as e:
                logger.debug(f"Error processing game: {e}")
                continue
        
        # Fallback: fuzzy matching
        best_match = None
        best_score = 0
        
        for game in games_list:
            if not isinstance(game, dict):
                continue
            if 'away_team' not in game or 'home_team' not in game:
                continue
            
            try:
                game_away = self.normalize_team_name(game['away_team'])
                game_home = self.normalize_team_name(game['home_team'])
                
                # Calculate match scores for both orderings
                score1 = (fuzz.ratio(normalized_away, game_away) + 
                         fuzz.ratio(normalized_home, game_home)) / 2
                score2 = (fuzz.ratio(normalized_away, game_home) + 
                         fuzz.ratio(normalized_home, game_away)) / 2
                
                if score1 >= self.fuzzy_threshold and score1 > best_score:
                    best_match = game
                    best_score = score1
                elif score2 >= self.fuzzy_threshold and score2 > best_score:
                    # Swap if reversed
                    reversed_game = game.copy()
                    reversed_game['away_team'], reversed_game['home_team'] = reversed_game['home_team'], reversed_game['away_team']
                    if 'predicted_score_away' in reversed_game and 'predicted_score_home' in reversed_game:
                        reversed_game['predicted_score_away'], reversed_game['predicted_score_home'] = reversed_game['predicted_score_home'], reversed_game['predicted_score_away']
                    if 'spread_away' in reversed_game and 'spread_home' in reversed_game:
                        reversed_game['spread_away'], reversed_game['spread_home'] = reversed_game['spread_home'], reversed_game['spread_away']
                    best_match = reversed_game
                    best_score = score2
            except Exception as e:
                logger.debug(f"Error in fuzzy matching: {e}")
                continue
        
        if best_match:
            return best_match
        
        logger.debug(f"No match found for {away_team} @ {home_team}")
        return None
    
    def load_all_data(self):
        """Load all JSON files from games_scraped/ and llm_mascot/, preferring LLM-processed versions."""
        # Load sheets data - try LLM processed version first, fallback to regular
        self.sheets_data = self.load_json_file(self.config.get_llm_mascot_path('sheets_games_llm.json'))
        if not self.sheets_data or not self.sheets_data.get('games'):
            self.sheets_data = self.load_json_file(self.config.get_games_scraped_path('sheets_games.json', league="nfl"))
        
        # Load all scraper outputs - try LLM processed versions first (from llm_mascot), fallback to scraped versions
        self.fantasynerds_data = self.load_json_file(self.config.get_llm_mascot_path('fantasynerds_games_llm.json'))
        if not self.fantasynerds_data or not self.fantasynerds_data.get('games'):
            self.fantasynerds_data = self.load_json_file(self.config.get_games_scraped_path('fantasynerds_games.json', league="nfl"))
        
        self.sportsline_data = self.load_json_file(self.config.get_llm_mascot_path('sportsline_games_llm.json'))
        if not self.sportsline_data or not self.sportsline_data.get('games'):
            self.sportsline_data = self.load_json_file(self.config.get_games_scraped_path('sportsline_games.json', league="nfl"))
        
        self.florio_data = self.load_json_file(self.config.get_llm_mascot_path('florio_games_llm.json'))
        if not self.florio_data or not self.florio_data.get('games'):
            self.florio_data = self.load_json_file(self.config.get_games_scraped_path('florio_games.json', league="nfl"))
        
        self.simms_data = self.load_json_file(self.config.get_llm_mascot_path('simms_games_llm.json'))
        if not self.simms_data or not self.simms_data.get('games'):
            self.simms_data = self.load_json_file(self.config.get_games_scraped_path('simms_games.json', league="nfl"))
        
        self.dimers_data = self.load_json_file(self.config.get_llm_mascot_path('dimers_games_llm.json'))
        if not self.dimers_data or not self.dimers_data.get('games'):
            self.dimers_data = self.load_json_file(self.config.get_games_scraped_path('dimers_games.json', league="nfl"))
        
        self.oddshark_data = self.load_json_file(self.config.get_llm_mascot_path('oddshark_games_llm.json'))
        if not self.oddshark_data or not self.oddshark_data.get('games'):
            self.oddshark_data = self.load_json_file(self.config.get_games_scraped_path('oddshark_games.json', league="nfl"))
        
        self.espn_data = self.load_json_file(self.config.get_llm_mascot_path('espn_games_llm.json'))
        if not self.espn_data or not self.espn_data.get('games'):
            self.espn_data = self.load_json_file(self.config.get_games_scraped_path('espn_games.json', league="nfl"))
        
        self.dratings_data = self.load_json_file(self.config.get_llm_mascot_path('dratings_games_llm.json'))
        if not self.dratings_data or not self.dratings_data.get('games'):
            self.dratings_data = self.load_json_file(self.config.get_games_scraped_path('dratings_games.json', league="nfl"))
        
        logger.info("All NFL data files loaded")
    
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
            "fantasynerds_matched": 0,
            "sportsline_matched": 0,
            "florio_matched": 0,
            "simms_matched": 0,
            "dimers_matched": 0,
            "oddshark_matched": 0,
            "espn_matched": 0,
            "dratings_matched": 0,
            "matched_sheets_rows": {}
        }
        
        # Prepare source data
        sources = {
            'fantasynerds': self.fantasynerds_data.get('games', []) if self.fantasynerds_data else [],
            'sportsline': self.sportsline_data.get('games', []) if self.sportsline_data else [],
            'florio': self.florio_data.get('games', []) if self.florio_data else [],
            'simms': self.simms_data.get('games', []) if self.simms_data else [],
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
        
        logger.info(f"Matching complete: "
                   f"FantasyNerds={results['fantasynerds_matched']}, "
                   f"SportsLine={results['sportsline_matched']}, "
                   f"Florio={results['florio_matched']}, "
                   f"Simms={results['simms_matched']}, "
                   f"Dimers={results['dimers_matched']}, "
                   f"OddShark={results['oddshark_matched']}, "
                   f"ESPN={results['espn_matched']}, "
                   f"DRatings={results['dratings_matched']}")
        
        return results
    
    def save_results(self, results: Dict, output_file: str = 'matched_games.json'):
        """Save the matched results to JSON."""
        try:
            output_path = self.config.get_data_path(output_file, league="nfl")
            
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
        logger.info("Starting NFL matching workflow...")
        self.load_all_data()
        results = self.match_games()
        self.save_results(results)
        
        print(f"\n=== NFL MATCHING SUMMARY ===")
        if results and 'sheets_total' in results:
            print(f"Matched {results['sheets_total']} games:")
            print(f"  FantasyNerds: {results.get('fantasynerds_matched', 0)}")
            print(f"  SportsLine: {results.get('sportsline_matched', 0)}")
            print(f"  Florio: {results.get('florio_matched', 0)}")
            print(f"  Simms: {results.get('simms_matched', 0)}")
            print(f"  Dimers: {results.get('dimers_matched', 0)}")
            print(f"  OddShark: {results.get('oddshark_matched', 0)}")
            print(f"  ESPN: {results.get('espn_matched', 0)}")
            print(f"  DRatings: {results.get('dratings_matched', 0)}")
        else:
            print("No sheets data available for matching")
        
        return results


def main():
    """Main function."""
    matcher = NFLGameMatcher()
    matcher.run()


if __name__ == "__main__":
    main()

