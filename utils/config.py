"""Configuration management from environment variables."""

import json
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class Config:
    """Application configuration from environment variables."""
    
    # Google Sheets
    sheet_id: str
    google_service_account_key: str
    
    # API Keys
    openai_api_key: str
    tavily_api_key: str
    
    # Optional fields (must come after required fields)
    nfl_sheet_id: Optional[str] = None
    
    # Data paths
    data_dir: str = "data"
    
    @classmethod
    def _build_google_credentials_json(cls) -> str:
        """
        Build Google service account credentials JSON from individual environment variables.
        
        Supports both new format (individual env vars) and legacy format (single JSON string).
        
        Returns:
            JSON string representation of Google service account credentials
        """
        # Try legacy format first (backward compatibility)
        legacy_key = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
        if legacy_key and legacy_key.strip().startswith("{"):
            try:
                # Validate it's valid JSON
                json.loads(legacy_key)
                return legacy_key
            except json.JSONDecodeError:
                pass  # Fall through to new format
        
        # New format: individual environment variables
        required_vars = {
            "GOOGLE_PROJECT_ID": os.getenv("GOOGLE_PROJECT_ID"),
            "GOOGLE_PRIVATE_KEY_ID": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            "GOOGLE_PRIVATE_KEY": os.getenv("GOOGLE_PRIVATE_KEY"),
            "GOOGLE_CLIENT_EMAIL": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "GOOGLE_CLIENT_ID": os.getenv("GOOGLE_CLIENT_ID"),
        }
        
        # Check if all required vars are present
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            # If we have legacy format, use it; otherwise raise error
            if legacy_key:
                return legacy_key
            raise ValueError(
                f"Missing required Google credentials: {', '.join(missing_vars)}. "
                "Either provide individual GOOGLE_* env vars or legacy GOOGLE_SERVICE_ACCOUNT_KEY."
            )
        
        # Build credentials dictionary
        credentials = {
            "type": "service_account",
            "project_id": required_vars["GOOGLE_PROJECT_ID"],
            "private_key_id": required_vars["GOOGLE_PRIVATE_KEY_ID"],
            "private_key": required_vars["GOOGLE_PRIVATE_KEY"].strip('"'),  # Remove quotes if present
            "client_email": required_vars["GOOGLE_CLIENT_EMAIL"],
            "client_id": required_vars["GOOGLE_CLIENT_ID"],
            "auth_uri": os.getenv("GOOGLE_AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": os.getenv("GOOGLE_TOKEN_URI", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": os.getenv(
                "GOOGLE_AUTH_PROVIDER_X509_CERT_URL",
                "https://www.googleapis.com/oauth2/v1/certs"
            ),
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL", ""),
            "universe_domain": os.getenv("GOOGLE_UNIVERSE_DOMAIN", "googleapis.com"),
        }
        
        # Remove empty optional fields
        credentials = {k: v for k, v in credentials.items() if v}
        
        return json.dumps(credentials)
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        sheet_id = os.getenv("SHEET_ID")
        nfl_sheet_id = os.getenv("NFL_SHEET_ID")
        openai_key = os.getenv("OPENAI_API_KEY")
        tavily_key = os.getenv("TAVILY_API_KEY")
        
        if not sheet_id:
            raise ValueError("SHEET_ID not found in environment")
        if not openai_key:
            raise ValueError("OPENAI_API_KEY not found in environment")
        if not tavily_key:
            raise ValueError("TAVILY_API_KEY not found in environment")
        
        # Build Google credentials JSON (handles both formats)
        google_key = cls._build_google_credentials_json()
        
        return cls(
            sheet_id=sheet_id,
            nfl_sheet_id=nfl_sheet_id,
            google_service_account_key=google_key,
            openai_api_key=openai_key,
            tavily_api_key=tavily_key
        )
    
    def get_data_path(self, filename: str, league: str = "ncaaf") -> str:
        """
        Get full path for a data file.
        
        Args:
            filename: Name of the file
            league: League identifier ('ncaaf' or 'nfl'), defaults to 'ncaaf'
            
        Returns:
            Full path to the data file in the league-specific subfolder
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, self.data_dir, league, filename)
    
    def get_sheet_id(self, league: str = "ncaaf") -> str:
        """
        Get the appropriate sheet ID for the specified league.
        
        Args:
            league: League identifier ('ncaaf' or 'nfl'), defaults to 'ncaaf'
            
        Returns:
            Sheet ID for the specified league
        """
        if league == "nfl":
            if not self.nfl_sheet_id:
                raise ValueError("NFL_SHEET_ID not found in environment. Please set NFL_SHEET_ID in .env file.")
            return self.nfl_sheet_id
        return self.sheet_id
    
    def get_games_scraped_path(self, filename: str, league: str = "ncaaf") -> str:
        """
        Get full path for a scraped games file in the games_scraped subdirectory.
        
        Args:
            filename: Name of the file
            league: League identifier ('ncaaf' or 'nfl'), defaults to 'ncaaf'
            
        Returns:
            Full path to the scraped games file in the league-specific games_scraped subfolder
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, self.data_dir, league, "games_scraped", filename)
    
    def get_llm_mascot_path(self, filename: str) -> str:
        """
        Get full path for an LLM-processed mascot file (NFL only).
        
        Args:
            filename: Name of the file
            
        Returns:
            Full path to the LLM mascot file in data/nfl/llm_mascot/
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, self.data_dir, "nfl", "llm_mascot", filename)
    
    def get_llm_university_path(self, filename: str) -> str:
        """
        Get full path for an LLM-processed university file (NCAAF only).
        
        Args:
            filename: Name of the file
            
        Returns:
            Full path to the LLM university file in data/ncaaf/llm_university/
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(base_path, self.data_dir, "ncaaf", "llm_university", filename)

