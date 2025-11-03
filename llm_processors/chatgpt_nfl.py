"""ChatGPT/Tavily-based NFL prediction gatherer.

Uses Tavily API to search for predicted scores from multiple sources,
then uses ChatGPT to extract and parse scores from unstructured text.
"""

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from tavily import TavilyClient
from openai import OpenAI

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import Config, get_logger

logger = get_logger(__name__)


# System prompt for ChatGPT score extraction (NFL version)
SYSTEM_PROMPT = """You are an NFL (National Football League) game prediction extractor and formatter.

Given raw web search results about a specific NFL game matchup, you must identify any predicted or expected scores mentioned in the text, average them if multiple appear, and return a clean JSON structure with the following fields:

{
  "<row_number>": {
    "away_team": "<away_team_mascot_name>",
    "home_team": "<home_team_mascot_name>",
    "predicted_score_away": <integer>,
    "predicted_score_home": <integer>
  }
}

Rules:
- Only return valid JSON.
- Use integer values.
- If no score is found, set both scores to 24 and 21 respectively (default NFL scores).
- Do not include any commentary or extra text.
- Only extract scores that look realistic (10–60 range for NFL games).
- If multiple predictions appear, calculate their mean and round to nearest integer.
- Never hallucinate or make up scores.
- Team names should be NFL team mascot names (e.g., "Ravens", "Patriots", "Chiefs")."""


# Query variations for each game matchup
QUERY_VARIATIONS = [
    "{away_team} vs {home_team} NFL predicted score",
    "{away_team} {home_team} NFL prediction",
]


def collect_raw_tavily_results(
    tavily_client: TavilyClient,
    away_team: str,
    home_team: str,
    max_results: int = 5
) -> str:
    """
    Collect all raw Tavily search results (titles + contents) for a game.
    
    Args:
        tavily_client: Tavily API client
        away_team: Away team name (mascot)
        home_team: Home team name (mascot)
        max_results: Maximum results per query
        
    Returns:
        Combined text string of all search results
    """
    all_texts = []
    
    for query_template in QUERY_VARIATIONS:
        try:
            query = query_template.format(away_team=away_team, home_team=home_team)
            logger.debug(f"Searching Tavily: {query}")
            
            response = tavily_client.search(
                query=query,
                max_results=max_results,
                search_depth="basic"
            )
            
            # Collect all titles and contents
            for result in response.get('results', []):
                title = result.get('title', '')
                content = result.get('content', '')
                if title or content:
                    all_texts.append(f"Title: {title}\nContent: {content}\n")
            
        except Exception as e:
            logger.warning(f"Error searching Tavily with query '{query}': {e}")
            continue
    
    return "\n---\n".join(all_texts)


def extract_scores_with_chatgpt(
    openai_client: OpenAI,
    raw_text: str,
    row_number: str,
    away_team: str,
    home_team: str
) -> Optional[Dict[str, int]]:
    """
    Use ChatGPT to extract predicted scores from raw Tavily search results.
    
    Args:
        openai_client: OpenAI API client
        raw_text: Raw text from Tavily search results
        row_number: Row number as string
        away_team: Away team name (mascot)
        home_team: Home team name (mascot)
        
    Returns:
        Dictionary with predicted scores or None if failed
    """
    content = None
    try:
        user_prompt = f"""Extract predicted scores for the following NFL game from the web search results below.

Game: {away_team} vs {home_team}
Row Number: {row_number}

Web Search Results:
{raw_text}

Return the JSON with row_number as the key and include away_team, home_team, predicted_score_away, and predicted_score_home."""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.0,
            max_tokens=500
        )
        
        content = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        parsed_result = json.loads(content)
        
        # Validate structure
        if not isinstance(parsed_result, dict):
            raise ValueError("ChatGPT did not return a dictionary")
        
        # Extract the result for this row number
        if row_number in parsed_result:
            result = parsed_result[row_number]
            # Ensure we have the required fields
            if 'predicted_score_away' in result and 'predicted_score_home' in result:
                # Convert to integers if they're not already
                return {
                    "predicted_score_away": int(result['predicted_score_away']),
                    "predicted_score_home": int(result['predicted_score_home'])
                }
        
        logger.warning(f"Row {row_number}: ChatGPT response missing expected structure")
        return None
        
    except json.JSONDecodeError as e:
        logger.error(f"Row {row_number}: Failed to parse ChatGPT JSON response: {e}")
        try:
            logger.debug(f"Raw response: {content[:200]}")
        except:
            pass
        return None
    except Exception as e:
        logger.error(f"Row {row_number}: Error extracting scores with ChatGPT: {e}")
        return None


def process_single_game(
    tavily_client: TavilyClient,
    openai_client: OpenAI,
    game: Dict,
    row_number: str
) -> tuple[str, Optional[Dict[str, int]]]:
    """
    Process a single game to get predicted scores using ChatGPT.
    
    Args:
        tavily_client: Tavily API client
        openai_client: OpenAI API client
        game: Game dictionary with away_team and home_team
        row_number: Sheet row number as string
        
    Returns:
        Tuple of (row_number, score_dict) or (row_number, None) if failed
    """
    away_team = game.get('away_team', '').strip()
    home_team = game.get('home_team', '').strip()
    
    if not away_team or not home_team:
        logger.warning(f"Row {row_number}: Missing team names")
        return (row_number, None)
    
    logger.info(f"Processing row {row_number}: {away_team} vs {home_team}")
    
    # Collect raw Tavily search results
    raw_results = collect_raw_tavily_results(
        tavily_client, 
        away_team, 
        home_team, 
        max_results=5
    )
    
    if not raw_results or not raw_results.strip():
        logger.warning(f"Row {row_number}: No Tavily results found for {away_team} vs {home_team}")
        return (row_number, None)
    
    # Use ChatGPT to extract scores from raw results
    scores = extract_scores_with_chatgpt(
        openai_client,
        raw_results,
        row_number,
        away_team,
        home_team
    )
    
    if scores:
        logger.info(f"Row {row_number}: Extracted scores - {scores['predicted_score_away']}-{scores['predicted_score_home']}")
        return (row_number, scores)
    else:
        logger.warning(f"Row {row_number}: Failed to extract scores with ChatGPT")
        return (row_number, None)


def run_chatgpt_nfl(config: Optional[Config] = None) -> bool:
    """
    Main function to gather predictions using Tavily and ChatGPT for NFL.
    
    Uses ChatGPT to extract scores from raw Tavily search results for each game.
    
    Args:
        config: Application configuration (loads from env if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    config = config or Config.from_env()
    
    # Load games from sheets (NFL)
    sheets_file = config.get_games_scraped_path("sheets_games.json", league="nfl")
    if not os.path.exists(sheets_file):
        logger.error(f"Sheets games file not found: {sheets_file}")
        return False
    
    with open(sheets_file, 'r', encoding='utf-8') as f:
        sheets_data = json.load(f)
    
    games = sheets_data.get('games', [])
    if not games:
        logger.error("No games found in sheets_games.json")
        return False
    
    logger.info(f"Processing {len(games)} NFL games from sheets")
    
    # Initialize Tavily client
    try:
        tavily_client = TavilyClient(api_key=config.tavily_api_key)
        logger.info("Tavily client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Tavily client: {e}")
        return False
    
    # Initialize OpenAI client for score extraction
    try:
        openai_client = OpenAI(api_key=config.openai_api_key)
        logger.info("OpenAI client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {e}")
        return False
    
    # Process games in parallel (5 at a time to avoid rate limits)
    results = {}
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks with row number tracking
        futures = {}
        for game in games:
            row_num = str(game.get('row_number', ''))
            future = executor.submit(
                process_single_game, 
                tavily_client, 
                openai_client,
                game, 
                row_num
            )
            futures[future] = row_num
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                row_num, score_dict = future.result()
                if score_dict:
                    results[row_num] = score_dict
                else:
                    logger.warning(f"Failed to get predictions for row {row_num}")
            except Exception as e:
                row_num = futures.get(future, "unknown")
                logger.error(f"Error processing game row {row_num}: {e}")
    
    # Verify all games have predictions
    expected_rows = {str(game.get('row_number', '')) for game in games}
    missing_rows = expected_rows - set(results.keys())
    
    if missing_rows:
        logger.error(f"Missing predictions for rows: {sorted(missing_rows)}")
        # Try to get scores from matched_games.json and calculate average
        matched_file = config.get_data_path("matched_games.json", league="nfl")
        matched_data = {}
        if os.path.exists(matched_file):
            try:
                with open(matched_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    matched_data = data.get('matched_sheets_rows', {})
            except Exception as e:
                logger.warning(f"Could not load matched_games.json for fallback: {e}")
        
        for row_num in missing_rows:
            collected_scores = []
            
            if row_num in matched_data:
                game_data = matched_data[row_num]
                
                # Collect scores from sources with direct scores (NFL sources)
                for source in ['fantasynerds', 'sportsline', 'florio', 'simms', 'dimers', 'oddshark']:
                    if source in game_data and isinstance(game_data[source], dict):
                        away = game_data[source].get('predicted_score_away')
                        home = game_data[source].get('predicted_score_home')
                        if away is not None and home is not None:
                            collected_scores.append({
                                "predicted_score_away": round(float(away)),
                                "predicted_score_home": round(float(home)),
                                "source": source
                            })
                
                # Convert spreads from ESPN and DRatings to scores
                # Spreads are win probabilities (0-100), convert to scores using a base total
                BASE_TOTAL_SCORE = 48  # Typical total score in NFL games
                for source in ['espn', 'dratings']:
                    if source in game_data and isinstance(game_data[source], dict):
                        spread_away = game_data[source].get('spread_away')
                        spread_home = game_data[source].get('spread_home')
                        if spread_away is not None and spread_home is not None:
                            # Convert win probabilities to scores
                            # Normalize spreads to sum to 100, then distribute base total
                            total_spread = float(spread_away) + float(spread_home)
                            if total_spread > 0:
                                away_score = (float(spread_away) / total_spread) * BASE_TOTAL_SCORE
                                home_score = (float(spread_home) / total_spread) * BASE_TOTAL_SCORE
                                collected_scores.append({
                                    "predicted_score_away": round(away_score),
                                    "predicted_score_home": round(home_score),
                                    "source": source
                                })
            
            # Calculate average if at least 3 sources available
            if len(collected_scores) >= 3:
                avg_away = round(sum(s['predicted_score_away'] for s in collected_scores) / len(collected_scores))
                avg_home = round(sum(s['predicted_score_home'] for s in collected_scores) / len(collected_scores))
                fallback_scores = {
                    "predicted_score_away": avg_away,
                    "predicted_score_home": avg_home
                }
                sources_used = [s['source'] for s in collected_scores]
                logger.warning(f"Using averaged scores from {len(collected_scores)} sources ({', '.join(sources_used)}) for row {row_num}: {avg_away}-{avg_home}")
            elif len(collected_scores) > 0:
                # Use available sources if less than 3
                avg_away = round(sum(s['predicted_score_away'] for s in collected_scores) / len(collected_scores))
                avg_home = round(sum(s['predicted_score_home'] for s in collected_scores) / len(collected_scores))
                fallback_scores = {
                    "predicted_score_away": avg_away,
                    "predicted_score_home": avg_home
                }
                sources_used = [s['source'] for s in collected_scores]
                logger.warning(f"Using averaged scores from {len(collected_scores)} source(s) ({', '.join(sources_used)}) for row {row_num}: {avg_away}-{avg_home}")
            else:
                # No sources available - don't add to results (will remain empty in sheets)
                logger.warning(f"No matched data available for row {row_num}, leaving ChatGPT predictions empty")
                continue  # Skip adding this row to results
            
            results[row_num] = fallback_scores
    
    # Sort results by row number to maintain sheets order
    sorted_results = dict(sorted(results.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0))
    
    # Save to output file (maintains order in Python 3.7+)
    output_file = config.get_data_path("chatgpt_matched.json", league="nfl")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sorted_results, f, indent=2)
    
    logger.info(f"Saved predictions to {output_file}")
    logger.info(f"Total games processed: {len(sorted_results)}")
    
    return True


if __name__ == "__main__":
    """Run the ChatGPT/Tavily prediction gatherer for NFL."""
    print("\n=== CHATGPT/TAVILY NFL PREDICTION GATHERER ===\n")
    
    try:
        success = run_chatgpt_nfl()
        if success:
            print("[SUCCESS] Predictions gathered and saved to chatgpt_matched.json")
        else:
            print("[ERROR] Failed to gather predictions")
            exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"[ERROR] {e}")
        exit(1)

