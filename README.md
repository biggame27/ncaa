# NCAA Basketball Box Score Scraper

A modular, well-structured scraper for NCAA basketball box scores with Google Drive integration.

## 🏀 What This Project Does

This scraper automatically collects NCAA basketball box score data from the official NCAA website and organizes it for analysis. It's designed to be reliable, efficient, and easy to use.

### Key Features
- 🏀 Scrapes NCAA basketball box scores (Men's & Women's, D1/D2/D3)
- 📁 Organized folder structure by year/month/gender/division
- ☁️ Automatic Google Drive upload with organized folders
- 🔄 Duplicate prevention (session and file-based)
- 📊 Batch processing and smart skipping
- 🗓️ Date-based scraping with backfill support
- 🔔 Discord notifications for errors and warnings
- 🧪 Comprehensive error handling and logging
- 🔧 Modular, extensible architecture

### What Gets Scraped

| Division | Gender | Example Date |
|----------|--------|--------------|
| D1 | Men | 2025-02-14 |
| D1 | Women | 2025-02-14 |
| D2 | Men | 2025-02-14 |
| D2 | Women | 2025-02-14 |
| D3 | Men | 2025-02-14 |
| D3 | Women | 2025-02-14 |

## 🏗️ Architecture

The scraper is organized into focused, modular components:

```
ncaa_scraper/
├── config/              # Configuration management
│   ├── settings.py      # Main configuration class
│   └── constants.py     # Constants and enums
├── models/              # Data models
│   ├── game_data.py     # Game and team data models
│   └── scraping_config.py # Scraping configuration models
├── utils/               # Utility functions
│   ├── date_utils.py    # Date handling
│   ├── url_utils.py     # URL generation and parsing
│   └── validators.py    # Input validation
├── storage/             # Storage operations
│   ├── file_manager.py  # Local file operations
│   ├── csv_handler.py   # CSV-specific operations
│   └── google_drive.py  # Google Drive integration
├── notifications/       # Notification systems
│   ├── base_notifier.py # Base notification interface
│   └── discord_notifier.py # Discord notifications
├── scrapers/            # Scraping logic
│   ├── base_scraper.py  # Base scraper class
│   ├── ncaa_scraper.py  # NCAA-specific scraper
│   └── selenium_utils.py # Selenium utilities
└── main.py              # Main entry point
```

### Key Improvements

1. **Modular Design** - Each component has a single responsibility, easy to test and extend
2. **Better Error Handling** - Centralized error handling with comprehensive logging
3. **Type Safety** - Type hints throughout for better IDE support and debugging
4. **Configuration Management** - Centralized config with environment variable support
5. **Extensibility** - Base classes for easy extension and plugin-like architecture

## ⚙️ Configuration & Setup

### Quick Start

#### Option 1: Docker (Recommended) 🐳
```bash
# Build and run - no setup needed!
docker build -t ncaa-scraper .
docker run --rm ncaa-scraper

# With custom date
docker run --rm ncaa-scraper --date 2025/01/15

# With specific divisions/genders
docker run --rm ncaa-scraper --date 2025/01/15 --divisions d3 --genders women
```

**Why Docker?** Same environment as production, no dependency conflicts, works everywhere!

#### Option 2: Local Python Development 🐍
```bash
# 1. Create virtual environment (REQUIRED - prevents dependency conflicts)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up Google Drive credentials (optional)
python migrate_credentials.py

# 4. Run with default settings (yesterday's games, all divisions/genders)
python main.py
```

**⚠️ Important:** Always use a virtual environment for local Python development to avoid dependency conflicts!

### Environment Variables
Create a `.env` file in the project root:

```env
# Google Drive OAuth (required for uploads)
GOOGLE_CLIENT_ID=your_client_id_here
GOOGLE_CLIENT_SECRET=your_client_secret_here
GOOGLE_REDIRECT_URI=http://localhost:8080/

# Google Drive settings
GOOGLE_DRIVE_FOLDER_ID=your_base_folder_id_here
UPLOAD_TO_GDRIVE=true

# Optional notifications
DISCORD_WEBHOOK_URL=your_discord_webhook_url_here

# Runtime settings
OUTPUT_DIR=scraped_data
LOG_LEVEL=INFO
SLEEP_TIME=2
WAIT_TIMEOUT=15
```

### Google Drive Setup
1. Create a Google Cloud project and enable Google Drive API
2. Create OAuth credentials (Desktop application)
3. Set environment variables in `.env`
4. Run the scraper; token saved to `token.pickle`

## 🚀 Usage

### Basic Commands
```bash
# Scrape yesterday's games (default: all divisions, all genders)
python main.py

# Scrape specific date
python main.py --date 2025/01/15

# Scrape specific division and gender
python main.py --date 2025/01/15 --divisions d3 --genders women

# Disable Google Drive upload
python main.py --no-upload-gdrive
```

### Advanced Commands
```bash
# Scrape multiple divisions
python main.py --divisions d1 d2 d3

# Scrape both genders
python main.py --genders men women

# Custom output directory
python main.py --output-dir /path/to/data

# Backfill specific dates
python main.py --backfill
```

### Docker Development
```bash
# Build the image
docker build -t ncaa-scraper .

# Basic usage (same as local Python)
docker run --rm ncaa-scraper
docker run --rm ncaa-scraper --date 2025/01/15
docker run --rm ncaa-scraper --divisions d3 --genders women

# Development with volume mounts (preserves data/logs)
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/.env:/app/.env \
  ncaa-scraper --date 2025/01/15

# Interactive debugging
docker run -it --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  ncaa-scraper /bin/bash
```

### Programmatic Usage
```python
from ncaa_scraper.config import get_config
from ncaa_scraper.scrapers import NCAAScraper
from ncaa_scraper.utils import generate_ncaa_urls
from datetime import date

# Get configuration
config = get_config()

# Create scraper
scraper = NCAAScraper(config)

# Generate URLs
urls = generate_ncaa_urls("2025/01/15")

# Scrape data
for url in urls:
    games = scraper.scrape(url)
    print(f"Scraped {len(games)} games from {url}")
```

## 🐛 Troubleshooting

### Common Issues

#### Virtual Environment Issues
1. **"Module not found" errors**: Make sure you activated your virtual environment
   ```bash
   # Check if venv is active (should show (venv) in prompt)
   which python  # Should point to venv/bin/python
   
   # If not active, activate it
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

2. **Dependency conflicts**: Always use a fresh virtual environment
   ```bash
   # Remove old venv and create new one
   rm -rf venv
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

#### General Issues
3. **Import Errors**: Make sure you're in the project root directory
4. **Selenium Issues**: Ensure Chrome browser is installed (or use Docker)
5. **Google Drive Auth**: Run `python migrate_credentials.py` to set up credentials
6. **Permission Errors**: Check file/directory permissions
7. **Rate Limiting**: Wait 15-30 minutes between runs, or use `--divisions` and `--genders` to reduce requests

### Debug Mode
```bash
# Enable debug logging
LOG_LEVEL=DEBUG python main.py

# Increase delays to avoid rate limiting
SLEEP_TIME=5 python main.py
```

## 🛠️ Development

### Running Tests
```bash
# Run all tests
python -m pytest tests/

# Run with coverage
python -m pytest --cov=ncaa_scraper tests/
```

### Adding New Features
1. **New Scraper**: Extend `BaseScraper` class
2. **New Storage**: Implement storage interface
3. **New Notifications**: Extend `BaseNotifier` class
4. **New Utilities**: Add to appropriate utility module

### Code Style
- Follow PEP 8
- Use type hints
- Add docstrings for all public methods
- Write tests for new functionality

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

MIT License

## 📊 Google Drive Features

### Default Behavior
- Google Drive upload is **enabled by default**
- Automatic duplicate detection before scraping
- Pre-checking of Google Drive before starting
- Intelligent uploads (only new/updated files)

### Duplicate Detection
The scraper prevents unnecessary uploads by:
1. Checking for existing files in the target folder
2. Comparing local vs Google Drive modification timestamps (UTC)
3. Smartly deciding to upload, update, or skip

### Example Output
```
2025-10-29 04:30:12,011 - INFO - Pre-checking Google Drive for existing files...
2025-10-29 04:30:12,015 - INFO - ✓ men d1 2025-02-14 already exists in Google Drive
2025-10-29 04:30:12,016 - INFO - ✗ women d1 2025-02-14 needs scraping
2025-10-29 04:30:12,021 - INFO - Google Drive pre-check complete: 3/6 files already exist
```

## 🔄 GitHub Actions Setup

### Workflow Options
- `/.github/workflows/ncaa-scraper.yml` (regular): faster startup
- `/.github/workflows/ncaa-scraper-docker.yml` (docker): highest reproducibility

Both run daily at 06:00 UTC. Disable `schedule` in one if you only want a single daily run.

### Required Secrets
- `GOOGLE_TOKEN_FILE_B64` – base64 of your local `token.pickle`
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`
- `GOOGLE_DRIVE_FOLDER_ID` (optional but recommended)
- `DISCORD_WEBHOOK_URL` (optional)

### Create Token (PowerShell)
```powershell
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes(".\token.pickle"))
$b64 | Set-Clipboard
```
Paste clipboard into the `GOOGLE_TOKEN_FILE_B64` secret.