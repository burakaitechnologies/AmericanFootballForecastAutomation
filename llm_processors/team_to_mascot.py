"""Convert NFL team names to standard mascot names using LLM."""

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Callable
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import Config, get_logger

logger = get_logger(__name__)


# ========== Pydantic Models ========== #

class SheetsGameConverted(BaseModel):
    """Converted sheets game with standard mascot names."""
    away_team: str = Field(description="Standard NFL mascot name of away team (e.g., 'Ravens', 'Dolphins')")
    home_team: str = Field(description="Standard NFL mascot name of home team (e.g., 'Ravens', 'Dolphins')")
    row_number: int = Field(description="Row number from original data")


class SheetsGamesOutput(BaseModel):
    """Output for converted sheets games."""
    total_games: int = Field(description="Total number of games")
    games: list[SheetsGameConverted] = Field(description="List of games with standard mascot names")


class PredictionGameConverted(BaseModel):
    """Converted prediction game with standard mascot names."""
    game_id: str = Field(description="Unique game identifier")
    away_team: str = Field(description="Standard NFL mascot name of away team")
    home_team: str = Field(description="Standard NFL mascot name of home team")
    predicted_score_away: float = Field(description="Predicted score for away team")
    predicted_score_home: float = Field(description="Predicted score for home team")
    scraped_at: str = Field(description="Timestamp when data was scraped")


class PredictionGamesOutput(BaseModel):
    """Output for converted prediction games."""
    website: str = Field(description="Website name")
    total: int = Field(description="Total number of games")
    games: list[PredictionGameConverted] = Field(description="List of games with standard mascot names")


class SpreadGameConverted(BaseModel):
    """Converted spread game with standard mascot names."""
    game_id: str = Field(description="Unique game identifier")
    away_team: str = Field(description="Standard NFL mascot name of away team")
    home_team: str = Field(description="Standard NFL mascot name of home team")
    spread_away: float = Field(description="Spread percentage for away team")
    spread_home: float = Field(description="Spread percentage for home team")
    scraped_at: str = Field(description="Timestamp when data was scraped")


class SpreadGamesOutput(BaseModel):
    """Output for converted spread games."""
    website: str = Field(description="Website name")
    total: int = Field(description="Total number of games")
    games: list[SpreadGameConverted] = Field(description="List of games with standard mascot names")


# ========== Prompts ========== #

SYSTEM_PROMPT = """You are an authoritative expert on NFL team identity.
Your sole job is to convert any NFL team name variation — including city names, full team names, typos, or abbreviations — 
into the team's standard mascot name (the official team nickname).

EXAMPLES (VARIATION → STANDARD MASCOT):

- "Miami Dolphins" → "Dolphins"
- "Baltimore" → "Ravens"
- "Baltimore Ravens" → "Ravens"
- "Chicago" → "Bears"
- "Chicago Bears" → "Bears"
- "Cincinati" → "Bengals" (fix typo)
- "Cincinnati Bengals" → "Bengals"
- "L A Chargers" → "Chargers"
- "Los Angeles Chargers" → "Chargers"
- "Tennese" → "Titans" (fix typo)
- "Tennessee Titans" → "Titans"
- "Washington Commanders" → "Commanders"
- "49ers" → "49ers" (keep as is, it's the standard)
- "San Francisco 49ers" → "49ers"
- "atlanta" → "Falcons" (lowercase city name)
- "New England" → "Patriots"
- "Buffalo Bills" → "Bills"

RULES:
1. Always return only the standard mascot name (e.g., "Ravens", "Dolphins", "Bears", "Bengals").
2. Remove city/location prefixes (e.g., "Miami ", "Baltimore ", "Chicago ").
3. Fix common typos (e.g., "Cincinati" → "Bengals", "Tennese" → "Titans").
4. Handle lowercase variations correctly.
5. For teams with numbers, keep the number (e.g., "49ers" stays "49ers").
6. Use standard NFL mascot names that are commonly recognized.
7. If a name is already just the mascot, return it as-is."""

USER_PROMPT_TEMPLATE = """Convert every NFL team name into its STANDARD MASCOT NAME.

RULES:
- Extract only the mascot name (e.g., "Dolphins", "Ravens", "Bears").
- Remove city/location prefixes (Miami, Baltimore, Chicago, etc.).
- Fix typos and misspellings.
- Handle lowercase or uppercase variations.
- For special cases like "49ers", keep the number as part of the name.
- Always return the standard, commonly recognized mascot name.

{data}

{format_instructions}

FINAL REMINDER:
Return only the standard NFL mascot name. Remove city names, fix typos, and normalize to the standard format.
Examples: "Miami Dolphins" → "Dolphins", "Baltimore" → "Ravens", "Cincinati" → "Bengals"."""


# ========== Processing Functions ========== #

def get_llm() -> ChatOpenAI:
    """Get configured LLM instance."""
    config = Config.from_env()
    return ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0,
        openai_api_key=config.openai_api_key
    )


def process_sheets_games(file_path: str) -> Dict[str, Any]:
    """Process sheets_games.json file."""
    logger.info(f"Processing {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    llm = get_llm()
    parser = PydanticOutputParser(pydantic_object=SheetsGamesOutput)
    
    user_prompt = USER_PROMPT_TEMPLATE.format(
        data=json.dumps(data, indent=2),
        format_instructions=parser.get_format_instructions()
    )
    
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_prompt)
    ]
    
    response = llm.invoke(messages)
    result = parser.parse(response.content)
    
    return result.model_dump()


def process_prediction_games(file_path: str) -> Dict[str, Any]:
    """Process prediction games files (fantasynerds, sportsline, florio, simms, dimers, oddshark)."""
    logger.info(f"Processing {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    llm = get_llm()
    parser = PydanticOutputParser(pydantic_object=PredictionGamesOutput)
    
    user_prompt = USER_PROMPT_TEMPLATE.format(
        data=json.dumps(data, indent=2),
        format_instructions=parser.get_format_instructions()
    )
    
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_prompt)
    ]
    
    response = llm.invoke(messages)
    result = parser.parse(response.content)
    
    return result.model_dump()


def process_spread_games(file_path: str) -> Dict[str, Any]:
    """Process spread games files (espn, dratings)."""
    logger.info(f"Processing {file_path}")
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    llm = get_llm()
    parser = PydanticOutputParser(pydantic_object=SpreadGamesOutput)
    
    user_prompt = USER_PROMPT_TEMPLATE.format(
        data=json.dumps(data, indent=2),
        format_instructions=parser.get_format_instructions()
    )
    
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=user_prompt)
    ]
    
    response = llm.invoke(messages)
    result = parser.parse(response.content)
    
    return result.model_dump()


def process_single_file(
    input_file: str, 
    output_file: str, 
    processor_func: Callable
) -> tuple[str, str, bool, str]:
    """Process a single file and return the result."""
    try:
        logger.info(f"Processing {input_file}...")
        
        # Process the file
        result = processor_func(input_file)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save the result
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Successfully saved {output_file}")
        return (input_file, output_file, True, "")
        
    except Exception as e:
        error_msg = f"Error processing {input_file}: {str(e)}"
        logger.error(error_msg)
        return (input_file, output_file, False, str(e))


def process_team_names_nfl(config=None):
    """Process all NFL JSON files to convert team names to mascot names."""
    if config is None:
        config = Config.from_env()
    
    files_to_process = [
        (config.get_games_scraped_path("sheets_games.json", league="nfl"), 
         config.get_llm_mascot_path("sheets_games_llm.json"), 
         process_sheets_games),
        (config.get_games_scraped_path("fantasynerds_games.json", league="nfl"), 
         config.get_llm_mascot_path("fantasynerds_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("sportsline_games.json", league="nfl"), 
         config.get_llm_mascot_path("sportsline_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("florio_games.json", league="nfl"), 
         config.get_llm_mascot_path("florio_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("simms_games.json", league="nfl"), 
         config.get_llm_mascot_path("simms_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("dimers_games.json", league="nfl"), 
         config.get_llm_mascot_path("dimers_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("oddshark_games.json", league="nfl"), 
         config.get_llm_mascot_path("oddshark_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("espn_games.json", league="nfl"), 
         config.get_llm_mascot_path("espn_games_llm.json"), 
         process_spread_games),
        (config.get_games_scraped_path("dratings_games.json", league="nfl"), 
         config.get_llm_mascot_path("dratings_games_llm.json"), 
         process_spread_games)
    ]
    
    logger.info("Starting concurrent processing of all NFL files...")
    
    # Use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_single_file, input_file, output_file, processor_func): input_file
            for input_file, output_file, processor_func in files_to_process
        }
        
        # Collect results as they complete
        results = []
        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
    
    # Print summary
    print("\n" + "="*50)
    print("NFL PROCESSING SUMMARY:")
    print("="*50)
    
    successful = sum(1 for _, _, success, _ in results if success)
    failed = len(results) - successful
    
    for input_file, output_file, success, error in results:
        if success:
            print(f"[SUCCESS] {os.path.basename(input_file)} -> {os.path.basename(output_file)}")
        else:
            print(f"[FAILED] {os.path.basename(input_file)}: {error}")
    
    print(f"\nTotal: {len(results)} files")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    logger.info("All NFL files processed!")


def main():
    """Main function to process all NFL JSON files concurrently."""
    process_team_names_nfl()


if __name__ == "__main__":
    main()

