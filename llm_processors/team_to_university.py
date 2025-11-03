"""Convert team names to official university names using LLM."""

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
    """Converted sheets game with official university names."""
    away_team: str = Field(description="Registered full university name of away team")
    home_team: str = Field(description="Registered full university name of home team")
    row_number: int = Field(description="Row number from original data")


class SheetsGamesOutput(BaseModel):
    """Output for converted sheets games."""
    total_games: int = Field(description="Total number of games")
    games: list[SheetsGameConverted] = Field(description="List of games with full university names")


class PredictionGameConverted(BaseModel):
    """Converted prediction game with official university names."""
    game_id: str = Field(description="Unique game identifier")
    away_team: str = Field(description="Registered full university name of away team")
    home_team: str = Field(description="Registered full university name of home team")
    predicted_score_away: float = Field(description="Predicted score for away team")
    predicted_score_home: float = Field(description="Predicted score for home team")
    scraped_at: str = Field(description="Timestamp when data was scraped")


class PredictionGamesOutput(BaseModel):
    """Output for converted prediction games."""
    website: str = Field(description="Website name")
    total: int = Field(description="Total number of games")
    games: list[PredictionGameConverted] = Field(description="List of games with full university names")


class SpreadGameConverted(BaseModel):
    """Converted spread game with official university names."""
    game_id: str = Field(description="Unique game identifier")
    away_team: str = Field(description="Registered full university name of away team")
    home_team: str = Field(description="Registered full university name of home team")
    spread_away: float = Field(description="Spread percentage for away team")
    spread_home: float = Field(description="Spread percentage for home team")
    scraped_at: str = Field(description="Timestamp when data was scraped")


class SpreadGamesOutput(BaseModel):
    """Output for converted spread games."""
    website: str = Field(description="Website name")
    total: int = Field(description="Total number of games")
    games: list[SpreadGameConverted] = Field(description="List of games with full university names")


# ========== Prompts ========== #

SYSTEM_PROMPT = """You are an authoritative expert on NCAA college football team identity.
Your sole job is to convert any team name — including nicknames, mascots, or abbreviations — 
into the team's officially registered university name.

EXAMPLES (MISTAKE → CORRECT FORM):

- "Cal" → "University of California, Berkeley"
- "UCLA Bruins" → "University of California, Los Angeles"
- "Texas Longhorns" → "University of Texas at Austin"
- "Colorado Buffs" → "University of Colorado Boulder"
- "Oklahoma State Cowboys" → "Oklahoma State University"
- "Florida Gators" → "University of Florida"
- "Penn St." → "Pennsylvania State University"
- "USC Trojans" → "University of Southern California"
- "Virginia Tech" → "Virginia Polytechnic Institute and State University"

RULES:
1. Write the full, registered university name exactly as it appears in official listings.
2. If "State" or "St." appears in the team name, include it.
3. If not, do not add it.
4. Do not shorten or use nicknames; always spell out the full institutional name.
5. Campus or city details may be included if part of the official registered name."""

USER_PROMPT_TEMPLATE = """Convert every NCAAF team name into its REGISTERED FULL UNIVERSITY NAME.

RULES:
- Write the exact official university name as registered (e.g., NCAA / school's legal name).
- Ignore mascots, short forms, or nicknames — replace them with the official university name.
- If the name contains "State" or "St.", keep it.
- If it doesn't, do NOT add "State" yourself.
- The output must always be the exact, correct university name — not abbreviations or fan versions.

{data}

{format_instructions}

FINAL REMINDER:
Write only the team's registered full university name as it officially exists. 
If the team name includes "State" or "St.", include it. If it doesn't, don't.
No nicknames, mascots, abbreviations, or invented forms — just the correct, full, official university name."""


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
    """Process dimers_games.json and oddshark_games.json files."""
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
    """Process espn_games.json and dratings_games.json files."""
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


def process_team_names(config=None):
    """Process all NCAAF JSON files to convert team names to university names."""
    if config is None:
        config = Config.from_env()
    league = "ncaaf"
    
    files_to_process = [
        (config.get_games_scraped_path("sheets_games.json", league=league), 
         config.get_llm_university_path("sheets_games_llm.json"), 
         process_sheets_games),
        (config.get_games_scraped_path("dimers_games.json", league=league), 
         config.get_llm_university_path("dimers_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("oddshark_games.json", league=league), 
         config.get_llm_university_path("oddshark_games_llm.json"), 
         process_prediction_games),
        (config.get_games_scraped_path("espn_games.json", league=league), 
         config.get_llm_university_path("espn_games_llm.json"), 
         process_spread_games),
        (config.get_games_scraped_path("dratings_games.json", league=league), 
         config.get_llm_university_path("dratings_games_llm.json"), 
         process_spread_games)
    ]
    
    logger.info("Starting concurrent processing of all files...")
    
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
    print("PROCESSING SUMMARY:")
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
    logger.info("All files processed!")


def main():
    """Main function to process all JSON files concurrently."""
    process_team_names()


if __name__ == "__main__":
    main()
