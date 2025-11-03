"""Google Sheets API client wrapper."""

import json
import codecs
from typing import Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.config import Config
from utils.logger import get_logger

logger = get_logger(__name__)


class GoogleSheetsClient:
    """Wrapper for Google Sheets API operations."""
    
    def __init__(self, config: Config, read_only: bool = True, sheet_id: Optional[str] = None):
        """
        Initialize Google Sheets client.
        
        Args:
            config: Application configuration
            read_only: If True, use read-only scope; otherwise use full access
            sheet_id: Optional sheet ID to override config.sheet_id
        """
        self.config = config
        self.sheet_id = sheet_id or config.sheet_id
        
        # Parse credentials
        try:
            key_decoded = codecs.decode(config.google_service_account_key, 'unicode_escape')
            credentials_info = json.loads(key_decoded)
        except (json.JSONDecodeError, UnicodeDecodeError):
            credentials_info = json.loads(config.google_service_account_key)
        
        # Set appropriate scope
        scope = (
            ['https://www.googleapis.com/auth/spreadsheets.readonly'] 
            if read_only 
            else ['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Create credentials and service
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=scope
        )
        self.service = build('sheets', 'v4', credentials=credentials)
        
        logger.info(f"Google Sheets client initialized (read_only={read_only})")
    
    def get_sheet_name(self) -> str:
        """Get the first sheet name."""
        sheets = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
        return sheets['sheets'][0]['properties']['title']
    
    def read_range(self, range_str: str) -> list:
        """
        Read values from a range.
        
        Args:
            range_str: Range string like 'Sheet1!A1:B10'
            
        Returns:
            List of rows
        """
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.sheet_id, 
            range=range_str
        ).execute()
        return result.get('values', [])
    
    def batch_update(self, updates: list[dict]) -> int:
        """
        Perform batch update.
        
        Args:
            updates: List of update dicts with 'range' and 'values' keys
            
        Returns:
            Number of cells updated
        """
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': updates
        }
        
        result = self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.sheet_id, 
            body=body
        ).execute()
        
        return result.get('totalUpdatedCells', 0)

