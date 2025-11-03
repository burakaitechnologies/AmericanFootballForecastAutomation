"""LangGraph orchestrator for American Football Forecast Automation workflow."""

import asyncio
import os
import sys
from typing import Dict, List, TypedDict, Annotated
import operator

from langgraph.graph import StateGraph, END

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from utils import Config, get_logger

# Import all modules needed for the workflow
from sheets.sheets_reader_ncaaf import SheetsReader
from sheets.sheets_reader_nfl import NFLSheetsReader
from sheets.sheets_updater_ncaaf import update_sheets
from sheets.sheets_updater_nfl import update_sheets_nfl as update_sheets_nfl_func
from llm_processors.team_to_university import process_team_names
from llm_processors.team_to_mascot import process_team_names_nfl
from matchers.matcher_ncaaf import GameMatcher
from matchers.matcher_nfl import NFLGameMatcher
from llm_processors.chatgpt_ncaaf import run_chatgpt_ncaaf
from llm_processors.chatgpt_nfl import run_chatgpt_nfl

logger = get_logger(__name__)


# Define the state structure
class WorkflowState(TypedDict):
    """State structure for the workflow."""
    step: str
    status: str
    errors: Annotated[List[str], operator.add]
    completed_steps: List[str]
    config: Config


# Import scraper modules
import importlib.util

def load_scraper_module(module_path: str, module_name: str):
    """Dynamically load a scraper module."""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Node functions
def read_sheets_ncaaf(state: WorkflowState) -> WorkflowState:
    """Read NCAAF sheets."""
    logger.info("=" * 60)
    logger.info("STEP 1: Reading NCAAF Sheets")
    logger.info("=" * 60)
    
    try:
        reader = SheetsReader(state.get("config"))
        reader.save_games_to_file()
        
        new_state = dict(state)
        new_state["step"] = "read_sheets_ncaaf"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["read_sheets_ncaaf"]
        logger.info("✓ NCAAF sheets read successfully")
        return new_state
    except Exception as e:
        error_msg = f"Error reading NCAAF sheets: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "read_sheets_ncaaf"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def read_sheets_nfl(state: WorkflowState) -> WorkflowState:
    """Read NFL sheets."""
    logger.info("=" * 60)
    logger.info("STEP 2: Reading NFL Sheets")
    logger.info("=" * 60)
    
    try:
        reader = NFLSheetsReader(state.get("config"))
        reader.save_games_to_file()
        
        new_state = dict(state)
        new_state["step"] = "read_sheets_nfl"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["read_sheets_nfl"]
        logger.info("✓ NFL sheets read successfully")
        return new_state
    except Exception as e:
        error_msg = f"Error reading NFL sheets: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "read_sheets_nfl"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


async def run_scraper_async(scraper_path: str, scraper_name: str):
    """Run a scraper's main function asynchronously."""
    try:
        module = load_scraper_module(scraper_path, scraper_name)
        if hasattr(module, 'main'):
            main_func = module.main
            if asyncio.iscoroutinefunction(main_func):
                await main_func()
            else:
                main_func()
            return {"scraper": scraper_name, "status": "success"}
        else:
            return {"scraper": scraper_name, "status": "error", "message": "No main function found"}
    except Exception as e:
        logger.error(f"Error running scraper {scraper_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"scraper": scraper_name, "status": "error", "message": str(e)}


def scrape_ncaaf_concurrent(state: WorkflowState) -> WorkflowState:
    """Run all NCAAF scrapers concurrently."""
    logger.info("=" * 60)
    logger.info("STEP 3: Scraping NCAAF (Concurrent)")
    logger.info("=" * 60)
    
    config = state.get("config")
    base_path = os.path.abspath(os.path.dirname(__file__))
    
    scrapers = [
        ("dimers", os.path.join(base_path, "scrapers", "ncaaf", "dimers_scraper.py")),
        ("dratings", os.path.join(base_path, "scrapers", "ncaaf", "dratings_scraper.py")),
        ("espn", os.path.join(base_path, "scrapers", "ncaaf", "espn_scraper.py")),
        ("oddshark", os.path.join(base_path, "scrapers", "ncaaf", "oddshark_scraper.py")),
    ]
    
    errors = []
    results = []
    
    async def run_all_scrapers():
        tasks = [run_scraper_async(path, name) for name, path in scrapers]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    try:
        results = asyncio.run(run_all_scrapers())
        
        for result in results:
            if isinstance(result, Exception):
                errors.append(f"Scraper error: {result}")
                logger.error(f"Scraper exception: {result}")
            elif result.get("status") == "error":
                errors.append(f"{result['scraper']}: {result.get('message', 'Unknown error')}")
                logger.error(f"Scraper {result['scraper']} failed")
            else:
                logger.info(f"✓ {result['scraper']} scraper completed")
        
        status = "completed" if len(errors) == 0 else "completed_with_errors"
        new_state = dict(state)
        new_state["step"] = "scrape_ncaaf_concurrent"
        new_state["status"] = status
        new_state["errors"] = state.get("errors", []) + errors
        new_state["completed_steps"] = state.get("completed_steps", []) + ["scrape_ncaaf_concurrent"]
        logger.info("✓ NCAAF scraping completed")
        return new_state
        
    except Exception as e:
        error_msg = f"Error in NCAAF scraping: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "scrape_ncaaf_concurrent"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def scrape_nfl_concurrent(state: WorkflowState) -> WorkflowState:
    """Run all NFL scrapers concurrently with special handling for sportsline."""
    logger.info("=" * 60)
    logger.info("STEP 4: Scraping NFL (Concurrent)")
    logger.info("=" * 60)
    
    config = state.get("config")
    base_path = os.path.abspath(os.path.dirname(__file__))
    
    scrapers = [
        ("dimers", os.path.join(base_path, "scrapers", "nfl", "dimers_scraper.py")),
        ("dratings", os.path.join(base_path, "scrapers", "nfl", "dratings_scraper.py")),
        ("espn", os.path.join(base_path, "scrapers", "nfl", "espn_scraper.py")),
        ("fantasynerds", os.path.join(base_path, "scrapers", "nfl", "fantasynerds_scraper.py")),
        ("florio_simms", os.path.join(base_path, "scrapers", "nfl", "florio_simms_scraper.py")),
        ("oddshark", os.path.join(base_path, "scrapers", "nfl", "oddshark_scraper.py")),
        ("sportsline", os.path.join(base_path, "scrapers", "nfl", "sportsline_scraper.py")),
    ]
    
    errors = []
    results = []
    
    async def run_all_scrapers():
        tasks = []
        for name, path in scrapers:
            if name == "sportsline":
                # Give sportsline more time due to login
                logger.info(f"Starting {name} scraper (may take longer due to login)...")
                task = asyncio.create_task(run_scraper_async(path, name))
                tasks.append(task)
            else:
                tasks.append(run_scraper_async(path, name))
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    try:
        results = asyncio.run(run_all_scrapers())
        
        for result in results:
            if isinstance(result, Exception):
                errors.append(f"Scraper error: {result}")
                logger.error(f"Scraper exception: {result}")
            elif result.get("status") == "error":
                errors.append(f"{result['scraper']}: {result.get('message', 'Unknown error')}")
                logger.error(f"Scraper {result['scraper']} failed")
            else:
                logger.info(f"✓ {result['scraper']} scraper completed")
        
        status = "completed" if len(errors) == 0 else "completed_with_errors"
        new_state = dict(state)
        new_state["step"] = "scrape_nfl_concurrent"
        new_state["status"] = status
        new_state["errors"] = state.get("errors", []) + errors
        new_state["completed_steps"] = state.get("completed_steps", []) + ["scrape_nfl_concurrent"]
        logger.info("✓ NFL scraping completed")
        return new_state
        
    except Exception as e:
        error_msg = f"Error in NFL scraping: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "scrape_nfl_concurrent"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def process_teams_to_university(state: WorkflowState) -> WorkflowState:
    """Process team names to university names for NCAAF."""
    logger.info("=" * 60)
    logger.info("STEP 5: Processing Teams to University (NCAAF)")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        process_team_names(config)
        
        new_state = dict(state)
        new_state["step"] = "process_teams_to_university"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["process_teams_to_university"]
        logger.info("✓ Teams to university processing completed")
        return new_state
    except Exception as e:
        error_msg = f"Error processing teams to university: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "process_teams_to_university"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def process_teams_to_mascot(state: WorkflowState) -> WorkflowState:
    """Process team names to mascot names for NFL."""
    logger.info("=" * 60)
    logger.info("STEP 6: Processing Teams to Mascot (NFL)")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        process_team_names_nfl(config)
        
        new_state = dict(state)
        new_state["step"] = "process_teams_to_mascot"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["process_teams_to_mascot"]
        logger.info("✓ Teams to mascot processing completed")
        return new_state
    except Exception as e:
        error_msg = f"Error processing teams to mascot: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "process_teams_to_mascot"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def match_ncaaf(state: WorkflowState) -> WorkflowState:
    """Match NCAAF games."""
    logger.info("=" * 60)
    logger.info("STEP 7: Matching NCAAF Games")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        matcher = GameMatcher(config)
        matcher.run()
        
        new_state = dict(state)
        new_state["step"] = "match_ncaaf"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["match_ncaaf"]
        logger.info("✓ NCAAF matching completed")
        return new_state
    except Exception as e:
        error_msg = f"Error matching NCAAF games: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "match_ncaaf"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def match_nfl(state: WorkflowState) -> WorkflowState:
    """Match NFL games."""
    logger.info("=" * 60)
    logger.info("STEP 8: Matching NFL Games")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        matcher = NFLGameMatcher(config)
        matcher.run()
        
        new_state = dict(state)
        new_state["step"] = "match_nfl"
        new_state["status"] = "completed"
        new_state["completed_steps"] = state.get("completed_steps", []) + ["match_nfl"]
        logger.info("✓ NFL matching completed")
        return new_state
    except Exception as e:
        error_msg = f"Error matching NFL games: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "match_nfl"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def chatgpt_ncaaf(state: WorkflowState) -> WorkflowState:
    """Run ChatGPT processor for NCAAF."""
    logger.info("=" * 60)
    logger.info("STEP 9: Running ChatGPT Processor (NCAAF)")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        success = run_chatgpt_ncaaf(config)
        
        if success:
            new_state = dict(state)
            new_state["step"] = "chatgpt_ncaaf"
            new_state["status"] = "completed"
            new_state["completed_steps"] = state.get("completed_steps", []) + ["chatgpt_ncaaf"]
            logger.info("✓ NCAAF ChatGPT processing completed")
            return new_state
        else:
            error_msg = "ChatGPT processing for NCAAF failed"
            logger.error(error_msg)
            new_state = dict(state)
            new_state["step"] = "chatgpt_ncaaf"
            new_state["status"] = "error"
            new_state["errors"] = state.get("errors", []) + [error_msg]
            return new_state
    except Exception as e:
        error_msg = f"Error in NCAAF ChatGPT processing: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "chatgpt_ncaaf"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def chatgpt_nfl(state: WorkflowState) -> WorkflowState:
    """Run ChatGPT processor for NFL."""
    logger.info("=" * 60)
    logger.info("STEP 10: Running ChatGPT Processor (NFL)")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        success = run_chatgpt_nfl(config)
        
        if success:
            new_state = dict(state)
            new_state["step"] = "chatgpt_nfl"
            new_state["status"] = "completed"
            new_state["completed_steps"] = state.get("completed_steps", []) + ["chatgpt_nfl"]
            logger.info("✓ NFL ChatGPT processing completed")
            return new_state
        else:
            error_msg = "ChatGPT processing for NFL failed"
            logger.error(error_msg)
            new_state = dict(state)
            new_state["step"] = "chatgpt_nfl"
            new_state["status"] = "error"
            new_state["errors"] = state.get("errors", []) + [error_msg]
            return new_state
    except Exception as e:
        error_msg = f"Error in NFL ChatGPT processing: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "chatgpt_nfl"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def update_sheets_ncaaf(state: WorkflowState) -> WorkflowState:
    """Update NCAAF sheets."""
    logger.info("=" * 60)
    logger.info("STEP 11: Updating NCAAF Sheets")
    logger.info("=" * 60)
    
    try:
        config = state.get("config")
        success, message = update_sheets(config)
        
        if success:
            new_state = dict(state)
            new_state["step"] = "update_sheets_ncaaf"
            new_state["status"] = "completed"
            new_state["completed_steps"] = state.get("completed_steps", []) + ["update_sheets_ncaaf"]
            logger.info(f"✓ NCAAF sheets updated: {message}")
            return new_state
        else:
            error_msg = f"Failed to update NCAAF sheets: {message}"
            logger.error(error_msg)
            new_state = dict(state)
            new_state["step"] = "update_sheets_ncaaf"
            new_state["status"] = "error"
            new_state["errors"] = state.get("errors", []) + [error_msg]
            return new_state
    except Exception as e:
        error_msg = f"Error updating NCAAF sheets: {e}"
        logger.error(error_msg)
        new_state = dict(state)
        new_state["step"] = "update_sheets_ncaaf"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


def update_sheets_nfl(state: WorkflowState) -> WorkflowState:
    """Update NFL sheets."""
    logger.info("=" * 60)
    logger.info("STEP 12: Updating NFL Sheets")
    logger.info("=" * 60)
    
    try:
        config = state["config"] if isinstance(state, dict) else state.get("config")
        success, message = update_sheets_nfl_func(config)
        
        if success:
            new_state = dict(state)
            new_state["step"] = "update_sheets_nfl"
            new_state["status"] = "completed"
            new_state["completed_steps"] = state.get("completed_steps", []) + ["update_sheets_nfl"]
            logger.info(f"✓ NFL sheets updated: {message}")
            return new_state
        else:
            error_msg = f"Failed to update NFL sheets: {message}"
            logger.error(error_msg)
            new_state = dict(state)
            new_state["step"] = "update_sheets_nfl"
            new_state["status"] = "error"
            new_state["errors"] = state.get("errors", []) + [error_msg]
            return new_state
    except Exception as e:
        error_msg = f"Error updating NFL sheets: {e}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        new_state = dict(state)
        new_state["step"] = "update_sheets_nfl"
        new_state["status"] = "error"
        new_state["errors"] = state.get("errors", []) + [error_msg]
        return new_state


# Build the workflow graph
def create_workflow():
    """Create and return the LangGraph workflow."""
    workflow = StateGraph(WorkflowState)
    
    # Add all nodes
    workflow.add_node("read_sheets_ncaaf", read_sheets_ncaaf)
    workflow.add_node("read_sheets_nfl", read_sheets_nfl)
    workflow.add_node("scrape_ncaaf_concurrent", scrape_ncaaf_concurrent)
    workflow.add_node("scrape_nfl_concurrent", scrape_nfl_concurrent)
    workflow.add_node("process_teams_to_university", process_teams_to_university)
    workflow.add_node("process_teams_to_mascot", process_teams_to_mascot)
    workflow.add_node("match_ncaaf", match_ncaaf)
    workflow.add_node("match_nfl", match_nfl)
    workflow.add_node("chatgpt_ncaaf", chatgpt_ncaaf)
    workflow.add_node("chatgpt_nfl", chatgpt_nfl)
    workflow.add_node("update_sheets_ncaaf", update_sheets_ncaaf)
    workflow.add_node("update_sheets_nfl", update_sheets_nfl)
    
    # Set entry point
    workflow.set_entry_point("read_sheets_ncaaf")
    
    # Define the workflow edges (sequential)
    workflow.add_edge("read_sheets_ncaaf", "read_sheets_nfl")
    workflow.add_edge("read_sheets_nfl", "scrape_ncaaf_concurrent")
    workflow.add_edge("scrape_ncaaf_concurrent", "scrape_nfl_concurrent")
    workflow.add_edge("scrape_nfl_concurrent", "process_teams_to_university")
    workflow.add_edge("process_teams_to_university", "process_teams_to_mascot")
    workflow.add_edge("process_teams_to_mascot", "match_ncaaf")
    workflow.add_edge("match_ncaaf", "match_nfl")
    workflow.add_edge("match_nfl", "chatgpt_ncaaf")
    workflow.add_edge("chatgpt_ncaaf", "chatgpt_nfl")
    workflow.add_edge("chatgpt_nfl", "update_sheets_ncaaf")
    workflow.add_edge("update_sheets_ncaaf", "update_sheets_nfl")
    workflow.add_edge("update_sheets_nfl", END)
    
    return workflow.compile()


def main():
    """Main entry point for the orchestrator."""
    print("\n" + "=" * 80)
    print("AMERICAN FOOTBALL FORECAST AUTOMATION - ORCHESTRATOR")
    print("=" * 80 + "\n")
    
    try:
        # Initialize configuration
        config = Config.from_env()
        
        # Create initial state
        initial_state = {
            "step": "start",
            "status": "initialized",
            "errors": [],
            "completed_steps": [],
            "config": config
        }
        
        # Create and run workflow
        app = create_workflow()
        final_state = app.invoke(initial_state)
        
        # Print summary
        print("\n" + "=" * 80)
        print("WORKFLOW SUMMARY")
        print("=" * 80)
        print(f"Completed steps: {len(final_state.get('completed_steps', []))}")
        print(f"Steps: {', '.join(final_state.get('completed_steps', []))}")
        
        errors = final_state.get("errors", [])
        if errors:
            print(f"\nErrors encountered: {len(errors)}")
            for error in errors:
                print(f"  - {error}")
        else:
            print("\n✓ Workflow completed successfully with no errors!")
        
        print("=" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"Fatal error in orchestrator: {e}")
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

