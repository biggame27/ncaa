# NCAA Basketball Box Score Scraper (Refactored)

A modular, well-structured scraper for NCAA basketball box scores with Google Drive integration.

## 🚀 Quick Start

### 1. Set Up Environment
```bash
# Install dependencies
pip install -r requirements.txt

# Set up Google Drive credentials (optional)
python migrate_credentials.py
```

### 2. Run the Scraper
```bash
# Scrape yesterday's games
python main.py

# Scrape specific date
python main.py --date 2025/01/15

# Upload to Google Drive
python main.py --upload-gdrive

# Scrape multiple divisions and genders
python main.py --divisions d1 d2 d3 --genders men women
```

## 📁 New Architecture

The refactored scraper is organized into focused, modular components:

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

## ✨ Key Improvements

### 1. **Modular Design**
- Each component has a single responsibility
- Easy to test, maintain, and extend
- Clear separation of concerns

### 2. **Better Error Handling**
- Centralized error handling strategies
- Comprehensive logging throughout
- Graceful degradation on failures

### 3. **Type Safety**
- Type hints throughout the codebase
- Data models with validation
- Better IDE support and debugging

### 4. **Configuration Management**
- Centralized configuration with validation
- Environment variable support
- Easy to override settings

### 5. **Extensibility**
- Base classes for easy extension
- Plugin-like architecture for notifications
- Easy to add new scrapers or storage backends

## 🔧 Usage

### Basic Usage
```bash
# Scrape yesterday's women's D3 games
python main.py

# Scrape specific date
python main.py --date 2025/01/15

# Upload to Google Drive
python main.py --upload-gdrive
```

### Advanced Usage
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

## 📊 Features

- 🏀 Scrapes NCAA basketball box scores (Men's & Women's, D1/D2/D3)
- 📁 Organized folder structure by year/month/gender/division
- ☁️ Automatic Google Drive upload with organized folders
- 🔄 Duplicate prevention (session and file-based)
- 📊 Batch processing and smart skipping
- 🗓️ Date-based scraping with backfill support
- 🔔 Discord notifications for errors and warnings
- 🧪 Comprehensive error handling and logging
- 🔧 Modular, extensible architecture

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

## 🔄 Migration from Old Version

The refactored version is backward compatible with the original:

1. **Same CLI interface**: All original command-line arguments work
2. **Same output format**: CSV files have the same structure
3. **Same configuration**: Uses the same `.env` file format
4. **Same features**: All original functionality is preserved

### Key Differences
- **Better organization**: Code is split into logical modules
- **Improved error handling**: More robust error recovery
- **Type safety**: Better IDE support and fewer runtime errors
- **Extensibility**: Easy to add new features
- **Testability**: Each component can be tested independently

## 📝 Configuration

### Environment Variables
```bash
# Required for Google Drive
GOOGLE_CLIENT_ID=your_client_id_here
GOOGLE_CLIENT_SECRET=your_client_secret_here
GOOGLE_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob

# Optional
GOOGLE_DRIVE_FOLDER_ID=your_folder_id_here
DISCORD_WEBHOOK_URL=your_discord_webhook_url_here
OUTPUT_DIR=scraped_data
LOG_LEVEL=INFO
```

### Configuration File
You can also create a `config.yaml` file for more complex configurations:

```yaml
scraper:
  output_dir: "scraped_data"
  wait_timeout: 15
  sleep_time: 2

google_drive:
  enabled: true
  folder_id: "your_folder_id"

notifications:
  discord:
    enabled: true
    webhook_url: "your_webhook_url"
```

## 🐛 Troubleshooting

### Common Issues
1. **Import Errors**: Make sure you're in the project root directory
2. **Selenium Issues**: Ensure Chrome browser is installed
3. **Google Drive Auth**: Run `python migrate_credentials.py` to set up credentials
4. **Permission Errors**: Check file/directory permissions

### Debug Mode
```bash
# Enable debug logging
LOG_LEVEL=DEBUG python main.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

MIT License
