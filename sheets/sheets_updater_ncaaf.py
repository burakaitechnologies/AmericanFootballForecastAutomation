"""Update Google Sheets with predicted scores."""

import json
import math
import os
import sys
from typing import Optional, Tuple

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import Config, GoogleSheetsClient, get_logger

logger = get_logger(__name__)


class SheetsUpdater:
    """Update Google Sheets with prediction data."""
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize sheets updater.
        
        Args:
            config: Application configuration (loads from env if not provided)
        """
        self.config = config or Config.from_env()
        self.client = GoogleSheetsClient(self.config, read_only=False)
        logger.info("SheetsUpdater initialized with write access")
    
    def validate_numeric_value(self, value, field_name: str = "") -> Optional[str]:
        """
        Validate that a value is numeric and not NaN/Infinity.
        
        Args:
            value: Value to validate
            field_name: Field name for logging
            
        Returns:
            String representation of value or None if invalid
        """
        try:
            num_val = float(value)
            if math.isnan(num_val) or math.isinf(num_val):
                logger.warning(f"Invalid numeric value for {field_name}: {value} (NaN or Infinity)")
                return None
            if num_val < 0 or num_val > 100:
                logger.warning(f"Value out of reasonable range for {field_name}: {value}")
                return None
            return str(num_val)
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid numeric value for {field_name}: {value} ({e})")
            return None
    
    def update_sheets_with_predictions(
        self, 
        matched_file: Optional[str] = None,
        chatgpt_file: Optional[str] = None,
        sheet_name: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Update Google Sheets with scraped data and ChatGPT predictions.
        
        Args:
            matched_file: Path to matched_games.json (for scraped data - Dimers, OddShark, ESPN, DRatings)
            chatgpt_file: Path to chatgpt_matched.json (for predictions - columns L and M only)
            sheet_name: Sheet name (uses first sheet if not provided)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        # Default file paths (NCAAF)
        if matched_file is None:
            matched_file = self.config.get_data_path("matched_games.json", league="ncaaf")
        if chatgpt_file is None:
            chatgpt_file = self.config.get_data_path("chatgpt_matched.json", league="ncaaf")
        
        # Validate files exist
        if not os.path.exists(matched_file):
            logger.error(f"Matched games file not found: {matched_file}")
            return False, "Matched games file not found"
        
        if not os.path.exists(chatgpt_file):
            logger.error(f"ChatGPT predictions file not found: {chatgpt_file}")
            return False, "ChatGPT predictions file not found"
        
        try:
            # Load matched data (scraped sources)
            with open(matched_file, 'r', encoding='utf-8') as f:
                matched_data = json.load(f)
            
            # Load ChatGPT predictions
            with open(chatgpt_file, 'r', encoding='utf-8') as f:
                chatgpt_data = json.load(f)
            
            # Validate data structures
            if not isinstance(matched_data, dict) or 'matched_sheets_rows' not in matched_data:
                logger.error("Invalid matched_games.json structure")
                return False, "Invalid matched_games.json structure"
            
            if not isinstance(chatgpt_data, dict):
                logger.error("Invalid chatgpt_matched.json structure")
                return False, "Invalid chatgpt_matched.json structure"
            
            matched_games = matched_data.get('matched_sheets_rows', {})
            
            # Get sheet name if not provided
            if not sheet_name:
                sheet_name = self.client.get_sheet_name()
            
            # Prepare batch update data
            updates = []
            cells_attempted = 0
            cells_skipped = 0
            
            # Process all rows from matched_games for scraped data
            for row_num, game_data in matched_games.items():
                if not isinstance(game_data, dict):
                    logger.warning(f"Invalid game data for row {row_num}")
                    continue
                
                try:
                    row_number = int(row_num)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid row number: {row_num}")
                    continue
                
                # Dimers: F (away), G (home)
                if 'dimers' in game_data and isinstance(game_data['dimers'], dict):
                    # Check if either score is 0 - if so, skip entire Dimers update for this row
                    away_val = game_data['dimers'].get('predicted_score_away')
                    home_val = game_data['dimers'].get('predicted_score_home')
                    skip_dimers = False
                    try:
                        if away_val is not None and float(away_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping Dimers update (away score is 0)")
                            cells_skipped += 2
                            skip_dimers = True
                        elif home_val is not None and float(home_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping Dimers update (home score is 0)")
                            cells_skipped += 2
                            skip_dimers = True
                    except (ValueError, TypeError):
                        pass  # Not numeric, continue with normal validation
                    
                    if not skip_dimers:
                        cells_attempted += 2
                        if 'predicted_score_away' in game_data['dimers']:
                            score_away = self.validate_numeric_value(
                                game_data['dimers']['predicted_score_away'], 
                                f"Dimers score_away row {row_number}"
                            )
                            if score_away is not None:
                                updates.append({'range': f'{sheet_name}!F{row_number}', 'values': [[score_away]]})
                            else:
                                cells_skipped += 1
                        if 'predicted_score_home' in game_data['dimers']:
                            score_home = self.validate_numeric_value(
                                game_data['dimers']['predicted_score_home'], 
                                f"Dimers score_home row {row_number}"
                            )
                            if score_home is not None:
                                updates.append({'range': f'{sheet_name}!G{row_number}', 'values': [[score_home]]})
                            else:
                                cells_skipped += 1
                
                # OddShark: I (away), J (home)
                if 'oddshark' in game_data and isinstance(game_data['oddshark'], dict) and len(game_data['oddshark']) > 0:
                    # Check if either score is 0 - if so, skip entire OddShark update for this row
                    away_val = game_data['oddshark'].get('predicted_score_away')
                    home_val = game_data['oddshark'].get('predicted_score_home')
                    skip_oddshark = False
                    try:
                        if away_val is not None and float(away_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping OddShark update (away score is 0)")
                            cells_skipped += 2
                            skip_oddshark = True
                        elif home_val is not None and float(home_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping OddShark update (home score is 0)")
                            cells_skipped += 2
                            skip_oddshark = True
                    except (ValueError, TypeError):
                        pass  # Not numeric, continue with normal validation
                    
                    if not skip_oddshark:
                        cells_attempted += 2
                        if 'predicted_score_away' in game_data['oddshark']:
                            score_away = self.validate_numeric_value(
                                game_data['oddshark']['predicted_score_away'], 
                                f"OddShark score_away row {row_number}"
                            )
                            if score_away is not None:
                                updates.append({'range': f'{sheet_name}!I{row_number}', 'values': [[score_away]]})
                                logger.debug(f"Row {row_number}: Writing OddShark away score {score_away} to column I")
                            else:
                                cells_skipped += 1
                        else:
                            logger.warning(f"Row {row_number}: OddShark dict exists but missing 'predicted_score_away'")
                        if 'predicted_score_home' in game_data['oddshark']:
                            score_home = self.validate_numeric_value(
                                game_data['oddshark']['predicted_score_home'], 
                                f"OddShark score_home row {row_number}"
                            )
                            if score_home is not None:
                                updates.append({'range': f'{sheet_name}!J{row_number}', 'values': [[score_home]]})
                                logger.debug(f"Row {row_number}: Writing OddShark home score {score_home} to column J")
                            else:
                                cells_skipped += 1
                        else:
                            logger.warning(f"Row {row_number}: OddShark dict exists but missing 'predicted_score_home'")
                else:
                    if 'oddshark' in game_data:
                        logger.debug(f"Row {row_number}: OddShark data is empty or missing")
                
                # ChatGPT Predictions: L (away), M (home) - from chatgpt_matched.json
                if row_num in chatgpt_data:
                    chatgpt_game = chatgpt_data[row_num]
                    # Check if either score is 0 - if so, skip entire ChatGPT update for this row
                    away_val = chatgpt_game.get('predicted_score_away')
                    home_val = chatgpt_game.get('predicted_score_home')
                    skip_chatgpt = False
                    try:
                        if away_val is not None and float(away_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping ChatGPT update (away score is 0)")
                            cells_skipped += 2
                            skip_chatgpt = True
                        elif home_val is not None and float(home_val) == 0:
                            logger.debug(f"Row {row_number}: Skipping ChatGPT update (home score is 0)")
                            cells_skipped += 2
                            skip_chatgpt = True
                    except (ValueError, TypeError):
                        pass  # Not numeric, continue with normal validation
                    
                    if not skip_chatgpt:
                        cells_attempted += 2
                        if 'predicted_score_away' in chatgpt_game:
                            score_away = self.validate_numeric_value(
                                chatgpt_game['predicted_score_away'], 
                                f"ChatGPT score_away row {row_number}"
                            )
                            if score_away is not None:
                                updates.append({'range': f'{sheet_name}!L{row_number}', 'values': [[score_away]]})
                                logger.debug(f"Row {row_number}: Writing ChatGPT away score {score_away} to column L")
                            else:
                                cells_skipped += 1
                        else:
                            logger.warning(f"Row {row_number}: ChatGPT data exists but missing 'predicted_score_away'")
                        
                        if 'predicted_score_home' in chatgpt_game:
                            score_home = self.validate_numeric_value(
                                chatgpt_game['predicted_score_home'], 
                                f"ChatGPT score_home row {row_number}"
                            )
                            if score_home is not None:
                                updates.append({'range': f'{sheet_name}!M{row_number}', 'values': [[score_home]]})
                                logger.debug(f"Row {row_number}: Writing ChatGPT home score {score_home} to column M")
                            else:
                                cells_skipped += 1
                        else:
                            logger.warning(f"Row {row_number}: ChatGPT data exists but missing 'predicted_score_home'")
                else:
                    logger.warning(f"Row {row_number}: No ChatGPT predictions found in chatgpt_matched.json")
                
                # ESPN: O (away spread)
                if 'espn' in game_data and isinstance(game_data['espn'], dict):
                    # Check if spread is 0 - if so, skip ESPN spread update for this row
                    spread_val_raw = game_data['espn'].get('spread_away')
                    skip_espn = False
                    try:
                        if spread_val_raw is not None and float(spread_val_raw) == 0:
                            logger.debug(f"Row {row_number}: Skipping ESPN spread update (spread is 0)")
                            cells_skipped += 1
                            skip_espn = True
                    except (ValueError, TypeError):
                        pass  # Not numeric, continue with normal validation
                    
                    if not skip_espn:
                        cells_attempted += 1
                        if 'spread_away' in game_data['espn']:
                            spread_val = self.validate_numeric_value(
                                game_data['espn']['spread_away'], 
                                f"ESPN spread_away row {row_number}"
                            )
                            if spread_val is not None:
                                updates.append({'range': f'{sheet_name}!O{row_number}', 'values': [[spread_val]]})
                            else:
                                cells_skipped += 1
                
                # DRatings: Q (away spread)
                if 'dratings' in game_data and isinstance(game_data['dratings'], dict) and len(game_data['dratings']) > 0:
                    # Check if spread is 0 - if so, skip DRatings spread update for this row
                    spread_val_raw = game_data['dratings'].get('spread_away')
                    skip_dratings = False
                    try:
                        if spread_val_raw is not None and float(spread_val_raw) == 0:
                            logger.debug(f"Row {row_number}: Skipping DRatings spread update (spread is 0)")
                            cells_skipped += 1
                            skip_dratings = True
                    except (ValueError, TypeError):
                        pass  # Not numeric, continue with normal validation
                    
                    if not skip_dratings:
                        cells_attempted += 1
                        if 'spread_away' in game_data['dratings']:
                            spread_val = self.validate_numeric_value(
                                game_data['dratings']['spread_away'], 
                                f"DRatings spread_away row {row_number}"
                            )
                            if spread_val is not None:
                                updates.append({'range': f'{sheet_name}!Q{row_number}', 'values': [[spread_val]]})
                                logger.debug(f"Row {row_number}: Writing DRatings spread {spread_val} to column Q")
                            else:
                                cells_skipped += 1
                        else:
                            logger.warning(f"Row {row_number}: DRatings dict exists but missing 'spread_away'")
                else:
                    if 'dratings' in game_data:
                        logger.debug(f"Row {row_number}: DRatings data is empty or missing")
            
            # Execute batch update
            if updates:
                # Count updates by source for summary
                dimers_count = sum(1 for u in updates if '!F' in u['range'] or '!G' in u['range'])
                oddshark_count = sum(1 for u in updates if '!I' in u['range'] or '!J' in u['range'])
                llm_count = sum(1 for u in updates if '!L' in u['range'] or '!M' in u['range'])
                espn_count = sum(1 for u in updates if '!O' in u['range'])
                dratings_count = sum(1 for u in updates if '!Q' in u['range'])
                
                logger.info(f"Update summary: Dimers={dimers_count//2}, OddShark={oddshark_count//2}, ChatGPT={llm_count//2}, ESPN={espn_count}, DRatings={dratings_count}")
                logger.info(f"Preparing to update {len(updates)} cells (attempted: {cells_attempted}, skipped: {cells_skipped})")
                
                updated_cells = self.client.batch_update(updates)
                
                logger.info(f"Successfully updated {updated_cells} cells in Google Sheets")
                return True, f"Updated {updated_cells} cells (attempted: {cells_attempted}, skipped: {cells_skipped})"
            else:
                logger.warning(f"No valid data to update (attempted: {cells_attempted}, skipped: {cells_skipped})")
                return False, f"No valid data to update (attempted: {cells_attempted}, skipped: {cells_skipped})"
                
        except Exception as e:
            logger.error(f"Error updating sheets: {e}")
            return False, f"Error: {e}"


def update_sheets(config: Optional[Config] = None) -> Tuple[bool, str]:
    """
    Simple function to update sheets with predicted data.
    
    Args:
        config: Application configuration
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        updater = SheetsUpdater(config)
        success, message = updater.update_sheets_with_predictions()
        return success, message
    except Exception as e:
        return False, f"Failed to initialize sheets updater: {e}"


def main():
    """Main function to demonstrate SheetsUpdater usage."""
    try:
        print("=== SHEETS UPDATER ===")
        print("[UPDATER] Google Sheets Updater Operations")
        
        # Check if required files exist
        config = Config.from_env()
        matched_file_path = config.get_data_path("matched_games.json", league="ncaaf")
        chatgpt_file_path = config.get_data_path("chatgpt_matched.json", league="ncaaf")
        
        if os.path.exists(matched_file_path) and os.path.exists(chatgpt_file_path):
            print("\n[STEP 1] Updating Google Sheets with predictions...")
            success, message = update_sheets()
            if success:
                print(f"[SUCCESS] {message}")
            else:
                print(f"[ERROR] Update failed: {message}")
        else:
            missing = []
            if not os.path.exists(matched_file_path):
                missing.append("matched_games.json")
            if not os.path.exists(chatgpt_file_path):
                missing.append("chatgpt_matched.json")
            print(f"[WARNING] Required files not found: {', '.join(missing)}")
            print("   Run the full automation pipeline first to generate predictions")
        
        print("[SUCCESS] Updater process completed!")
        
    except Exception as e:
        logger.error(f"Error in updater process: {e}")
        print(f"\n[ERROR] Error: {e}")
        print("\nPossible issues:")
        print("- Check your internet connection")
        print("- Verify Google Sheets write permissions")
        print("- Ensure data/predicted_games.json exists and is valid")


if __name__ == "__main__":
    main()



