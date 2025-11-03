"""LLM-based processors for team name normalization and score predictions."""

from llm_processors.chatgpt_ncaaf import run_chatgpt_ncaaf
from llm_processors.chatgpt_nfl import run_chatgpt_nfl
from llm_processors.team_to_university import process_team_names
from llm_processors.team_to_mascot import process_team_names_nfl

__all__ = [
    "run_chatgpt_ncaaf",
    "run_chatgpt_nfl",
    "process_team_names",
    "process_team_names_nfl",
]

