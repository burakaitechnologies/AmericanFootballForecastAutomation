"""Google Sheets reader for game data."""

import json
import os
import sys
from typing import Optional

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.game_models import SheetsGame, SheetsOutput
from utils import Config, GoogleSheetsClient, get_logger

logger = get_logger(__name__)


class SheetsReader:
    """Read team matchup data from Google Sheets."""
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize sheets reader.
        
        Args:
            config: Application configuration (loads from env if not provided)
        """
        self.config = config or Config.from_env()
        self.client = GoogleSheetsClient(self.config, read_only=True)
        logger.info("SheetsReader initialized")
    
    def read_games_data(self, sheet_name: Optional[str] = None) -> list[SheetsGame]:
        """
        Read team names from columns A and B starting from row 3.
        
        Args:
            sheet_name: Sheet name (uses first sheet if not provided)
            
        Returns:
            List of SheetsGame objects
        """
        # Get sheet name if not provided
        if not sheet_name:
            sheet_name = self.client.get_sheet_name()
            logger.info(f"Using sheet: {sheet_name}")
        
        # Try different range formats
        for range_format in [f'{sheet_name}!A3:B1000', 'A3:B1000', f'{sheet_name}!A:B']:
            try:
                values = self.client.read_range(range_format)
                if values:
                    logger.info(f"Successfully read {len(values)} rows")
                    break
            except Exception as e:
                logger.debug(f"Failed to read range {range_format}: {e}")
                continue
        else:
            logger.warning("No data found in any range format")
            return []
        
        # Process data starting from row 3
        games = []
        for i, row in enumerate(values, start=3):
            if len(row) >= 2 and row[0] and row[1]:  # Both teams must exist
                games.append(SheetsGame(
                    away_team=row[0].strip(),
                    home_team=row[1].strip(),
                    row_number=i
                ))
        
        logger.info(f"Successfully processed {len(games)} games")
        return games
    
    def save_games_to_file(self, output_file: Optional[str] = None) -> SheetsOutput:
        """
        Read games data and save to JSON file.
        
        Args:
            output_file: Output file path (defaults to data/ncaaf/games_scraped/sheets_games.json)
            
        Returns:
            SheetsOutput object
        """
        if output_file is None:
            output_file = self.config.get_games_scraped_path("sheets_games.json", league="ncaaf")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        games = self.read_games_data()
        output = SheetsOutput(total_games=len(games), games=games)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output.model_dump(), f, indent=2, ensure_ascii=False)
        
        logger.info(f"Games data saved to {output_file}")
        return output


def main():
    """Main function to demonstrate SheetsReader usage."""
    try:
        print("=== SHEETS READER ===")
        print("[READER] Google Sheets Reader Operations")
        
        print("\n[STEP 1] Retrieving team data from Google Sheets...")
        reader = SheetsReader()
        data = reader.save_games_to_file()
        
        print(f"[SUCCESS] Retrieved {data.total_games} games from Google Sheets")
        print(f"[OUTPUT] Output file: {reader.config.get_games_scraped_path('sheets_games.json', league='ncaaf')}")
        
        # Show sample games
        if data.games:
            print(f"\n[GAMES] Sample Games:")
            for i, game in enumerate(data.games[:5], 1):
                print(f"   {i}. Row {game.row_number}: {game.away_team} @ {game.home_team}")
            if len(data.games) > 5:
                print(f"   ... and {len(data.games) - 5} more games")
        
        print(f"\n[INFO] Sheet ID: {reader.config.sheet_id}")
        print("[SUCCESS] Reader process completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in reader process: {e}")
        print(f"\n[ERROR] Error: {e}")
        print("\nPossible issues:")
        print("- Check your internet connection")
        print("- Verify Google Sheets permissions")
        print("- Ensure the sheet contains data in columns A and B")


if __name__ == "__main__":
    main()
