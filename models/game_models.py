"""Pydantic models for game data structures."""

from datetime import datetime
from typing import Optional, Dict
from uuid import uuid4
from pydantic import BaseModel, Field


class Game(BaseModel):
    """Base game model with team names."""
    game_id: str = Field(default_factory=lambda: str(uuid4()))
    away_team: str = Field(description="Away team name")
    home_team: str = Field(description="Home team name")
    scraped_at: Optional[datetime] = Field(default_factory=datetime.now)


class PredictionGame(Game):
    """Game with predicted scores."""
    predicted_score_away: float = Field(description="Predicted score for away team")
    predicted_score_home: float = Field(description="Predicted score for home team")


class SpreadGame(Game):
    """Game with spread percentages."""
    spread_away: float = Field(description="Win probability percentage for away team")
    spread_home: float = Field(description="Win probability percentage for home team")


class SheetsGame(BaseModel):
    """Game from Google Sheets with row number."""
    away_team: str = Field(description="Away team name")
    home_team: str = Field(description="Home team name")
    row_number: int = Field(description="Row number in the sheet")


class ScraperOutput(BaseModel):
    """Output from web scrapers."""
    website: str = Field(description="Source website name")
    total: int = Field(description="Total number of games scraped")
    games: list = Field(description="List of games")


class SheetsOutput(BaseModel):
    """Output from sheets reader."""
    total_games: int = Field(description="Total number of games")
    games: list[SheetsGame] = Field(description="List of games from sheets")


class MatchedGame(BaseModel):
    """Matched game across multiple sources."""
    sheets: Dict = Field(description="Data from sheets")
    dimers: Optional[Dict] = None
    oddshark: Optional[Dict] = None
    espn: Optional[Dict] = None
    dratings: Optional[Dict] = None


class MatchedGamesOutput(BaseModel):
    """Output from game matcher."""
    sheets_total: int
    dimers_matched: int
    oddshark_matched: int
    espn_matched: int
    dratings_matched: int
    matched_sheets_rows: Dict[str, MatchedGame]


class LLMPredictedScore(BaseModel):
    """LLM predicted scores."""
    predicted_score_away: float = Field(description="Predicted score for away team")
    predicted_score_home: float = Field(description="Predicted score for home team")


class PredictedGame(BaseModel):
    """Game with LLM predictions and all source data."""
    llm_predicted_score: LLMPredictedScore
    sheets: Dict
    dimers: Optional[Dict] = None
    oddshark: Optional[Dict] = None
    espn: Optional[Dict] = None
    dratings: Optional[Dict] = None


class PredictedGamesOutput(BaseModel):
    """Output from predictor."""
    predicted_sheets_rows: Dict[str, PredictedGame]

