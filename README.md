# American Football Forecast Automation

Automated workflow for collecting and processing NCAAF and NFL game predictions from multiple sources and updating Google Sheets.

## Structure

```
AmericanFootballForecastAutomation/
├── venv/                    # Virtual environment
├── orchestrator.py         # Main orchestrator script
├── requirements.txt        # Python dependencies
├── utils/                  # Utility package
│   ├── config.py          # Configuration management
│   ├── logger.py          # Logging utilities
│   ├── base_scraper.py    # Base scraper class
│   ├── google_sheets.py   # Google Sheets integration
│   └── nfl_week.py        # NFL week calculation
├── models/                 # Data models
│   └── game_models.py     # Pydantic models
├── scrapers/              # Web scrapers
│   ├── ncaaf/             # NCAAF scrapers
│   │   ├── dimers_scraper.py
│   │   ├── dratings_scraper.py
│   │   ├── espn_scraper.py
│   │   └── oddshark_scraper.py
│   └── nfl/               # NFL scrapers
│       ├── dimers_scraper.py
│       ├── dratings_scraper.py
│       ├── espn_scraper.py
│       ├── fantasynerds_scraper.py
│       ├── florio_simms_scraper.py
│       ├── oddshark_scraper.py
│       └── sportsline_scraper.py
├── sheets/                # Google Sheets integration
│   ├── sheets_reader_ncaaf.py
│   ├── sheets_reader_nfl.py
│   ├── sheets_updater_ncaaf.py
│   └── sheets_updater_nfl.py
├── matchers/              # Game matching logic
│   ├── matcher_ncaaf.py
│   └── matcher_nfl.py
└── llm_processors/        # LLM-based processors
    ├── chatgpt_ncaaf.py
    ├── chatgpt_nfl.py
    ├── team_to_university.py
    └── team_to_mascot.py
```

## Prerequisites

- Python 3.8 or higher
- Virtual environment (recommended)
- Google Cloud Service Account with Sheets API access
- OpenAI API key
- Tavily API key
- (Optional) SportsLine credentials for NFL scraping

## Setup

1. **Create and activate virtual environment:**
   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. **Install Python dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Install Playwright browsers** (required for SportsLine scraper):
   ```powershell
   playwright install chromium
   ```

4. **Configure environment variables:**
   Create a `.env` file in the project root with the following variables:
   ```env
   # Google Sheets Configuration
   SHEET_ID=your_ncaaf_google_sheet_id
   NFL_SHEET_ID=your_nfl_google_sheet_id
   
   # API Keys
   OPENAI_API_KEY=your_openai_api_key
   TAVILY_API_KEY=your_tavily_api_key
   
   # Google Service Account (choose one method)
   # Method 1: Individual environment variables (recommended)
   GOOGLE_PROJECT_ID=your_project_id
   GOOGLE_PRIVATE_KEY_ID=your_private_key_id
   GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   GOOGLE_CLIENT_EMAIL=your_service_account_email
   GOOGLE_CLIENT_ID=your_client_id
   
   # Method 2: Legacy JSON string format
   # GOOGLE_SERVICE_ACCOUNT_KEY={"type":"service_account",...}
   
   # Optional: SportsLine credentials (for NFL SportsLine scraper)
   SPORTSLINE_EMAIL=your_sportsline_email
   SPORTSLINE_PASSWORD=your_sportsline_password
   ```

## Usage

**Run the complete workflow** (both NCAAF and NFL):
```powershell
python orchestrator.py
```

The orchestrator will execute all steps sequentially:
1. Read NCAAF and NFL games from Google Sheets
2. Scrape predictions from all configured sources
3. Normalize team names using LLM
4. Match scraped games to sheet rows
5. Generate ChatGPT predictions via Tavily
6. Update Google Sheets with all predictions

**Note:** The orchestrator currently runs both leagues. To run individual components, execute the specific modules directly.

## Workflow

The orchestrator runs the following steps in sequence:

1. **Read games from Google Sheets**
   - Reads NCAAF team matchups from columns A and B (starting row 3)
   - Reads NFL team matchups from columns A and B (starting row 3)
   - Saves to `data/{league}/games_scraped/sheets_games.json`

2. **Scrape predictions** (runs concurrently for each source)
   - NCAAF: Dimers, OddShark, ESPN, DRatings
   - NFL: Dimers, OddShark, ESPN, DRatings, FantasyNerds, SportsLine, Florio/Simms
   - Saves to `data/{league}/games_scraped/{source}_games.json`

3. **Normalize team names**
   - NCAAF: Uses LLM to convert team names to university names
   - NFL: Uses LLM to convert team names to mascot names
   - Saves to `data/{league}/llm_{university|mascot}/{source}_games_llm.json`

4. **Match games**
   - Associates scraped data with sheet rows using fuzzy string matching
   - Handles variations in team name formatting
   - Saves to `data/{league}/matched_games.json`

5. **Generate ChatGPT predictions**
   - Uses Tavily API to search for game predictions
   - Uses ChatGPT to extract scores from search results
   - Falls back to averaging available source predictions if needed
   - Saves to `data/{league}/chatgpt_matched.json`

6. **Update Google Sheets**
   - Writes all predictions to appropriate columns
   - NCAAF and NFL use different column mappings

## Data Sources

**NCAAF:**
- **Dimers.com** - Predicted scores
- **OddShark.com** - Predicted scores
- **ESPN.com** - Win probabilities (converted to scores)
- **DRatings.com** - Win probabilities (converted to scores)
- **ChatGPT/Tavily** - AI-generated predictions from web search

**NFL:**
- **Dimers.com** - Predicted scores
- **OddShark.com** - Predicted scores
- **ESPN.com** - Win probabilities (converted to scores)
- **DRatings.com** - Win probabilities (converted to scores)
- **FantasyNerds.com** - Predicted scores
- **SportsLine.com** - Predicted scores (requires login)
- **Florio/Simms (ProFootballTalk)** - Expert picks
- **ChatGPT/Tavily** - AI-generated predictions from web search

## Requirements

See `requirements.txt` for the complete list of dependencies. Key packages include:

- **Web Scraping**: `aiohttp`, `beautifulsoup4`, `playwright`
- **LLM Integration**: `openai`, `langchain-openai`, `tavily-python`
- **Workflow**: `langgraph`
- **Data Validation**: `pydantic`
- **Google Sheets**: `google-api-python-client`, `google-auth`
- **String Matching**: `fuzzywuzzy`, `python-Levenshtein`

## Notes

- Playwright browsers must be installed separately after installing requirements
- The SportsLine scraper requires login credentials in `.env`
- The orchestrator processes both leagues sequentially
- All scraped data is cached in JSON files under the `data/` directory
- Google Sheets must have appropriate permissions for the service account

