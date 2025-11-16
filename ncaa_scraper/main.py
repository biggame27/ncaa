"""Main entry point for the refactored NCAA scraper."""

import argparse
import logging
from datetime import date
from typing import List

from .config import get_config, Division, Gender
from .scrapers import NCAAScraper
from .scrapers.selenium_utils import SeleniumUtils
from .utils import get_yesterday, format_date_for_url, generate_ncaa_urls
from .models import ScrapingConfig, DateRange
from .config.constants import ErrorType
from .discovery import discover_games, load_game_links_mapping, get_games_for_division_gender
import time
import json

logger = logging.getLogger(__name__)


def main():
    """Main entry point for the NCAA scraper."""
    parser = argparse.ArgumentParser(description='NCAA Box Score Scraper (Refactored)')
    parser.add_argument('--date', type=str, help='Date in YYYY/MM/DD format (default: yesterday)')
    parser.add_argument('--output-dir', type=str, default='scraped_data', help='Output directory for CSV files')
    parser.add_argument('--backfill', action='store_true', help='Run backfill for specific dates')
    parser.add_argument('--upload-gdrive', action='store_true', help='Upload scraped data to Google Drive (default: enabled)')
    parser.add_argument('--no-upload-gdrive', action='store_true', help='Disable Google Drive upload')
    parser.add_argument('--gdrive-folder-id', type=str, help='Google Drive folder ID to upload to (optional)')
    parser.add_argument('--force-rescrape', action='store_true', help='Force rescrape and override existing Google Drive files')
    parser.add_argument('--divisions', nargs='+', choices=['d1', 'd2', 'd3'], default=['d1', 'd2', 'd3'], 
                       help='Divisions to scrape (default: all divisions)')
    parser.add_argument('--genders', nargs='+', choices=['men', 'women'], default=['men', 'women'], 
                       help='Genders to scrape (default: both genders)')
    parser.add_argument('--discover', action='store_true', help='Discovery mode: extract game links and identify duplicates')
    parser.add_argument('--mapping-file', type=str, help='Path to game links mapping JSON file (for single division/gender scraping)')
    parser.add_argument('--single-division', type=str, choices=['d1', 'd2', 'd3'], help='Scrape single division (requires --mapping-file)')
    parser.add_argument('--single-gender', type=str, choices=['men', 'women'], help='Scrape single gender (requires --mapping-file)')
    
    args = parser.parse_args()
    
    # Get configuration
    config = get_config()
    if not config.validate():
        return 1
    
    # Override config with command line arguments
    if args.output_dir:
        config.output_dir = args.output_dir
    if args.gdrive_folder_id:
        config.google_drive_folder_id = args.gdrive_folder_id
    if args.upload_gdrive:
        config.upload_to_gdrive = True
    if args.no_upload_gdrive:
        config.upload_to_gdrive = False
    
    # Convert division and gender strings to enums
    divisions = [Division(d) for d in args.divisions]
    genders = [Gender(g) for g in args.genders]
    
    # Create output directory
    import os
    os.makedirs(config.output_dir, exist_ok=True)
    logger.info(f"Output directory: {os.path.abspath(config.output_dir)}")
    
    # Handle discovery mode
    if args.discover:
        target_date = _parse_date(args.date) if args.date else get_yesterday()
        logger.info(f"Discovery mode: extracting game links for {target_date}")
        try:
            mapping = discover_games(target_date, "discovery/game_links_mapping.json")
            logger.info(f"Discovery completed successfully. Found {mapping['total_games']} games.")
            return 0
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            return 1
    
    # Handle single division/gender scraping (requires mapping file)
    if args.single_division and args.single_gender:
        if not args.mapping_file:
            logger.error("--mapping-file is required when using --single-division and --single-gender")
            return 1
        
        target_date = _parse_date(args.date) if args.date else get_yesterday()
        logger.info(f"Single division/gender mode: {args.single_division} {args.single_gender} for {target_date}")
        
        try:
            mapping = load_game_links_mapping(args.mapping_file)
            game_links = get_games_for_division_gender(mapping, args.single_division, args.single_gender)
            logger.info(f"Found {len(game_links)} games to scrape for {args.single_division} {args.single_gender}")
            
            # Initialize scraper
            scraper = NCAAScraper(config)
            scraper.force_rescrape = args.force_rescrape
            
            # Set duplicate mapping on scraper
            scraper.duplicate_mapping = mapping
            
            # Scrape games
            from .utils import parse_url_components
            components = parse_url_components(generate_ncaa_urls(format_date_for_url(target_date), [Division(args.single_division)], [Gender(args.single_gender)])[0])
            
            _scrape_games_from_mapping(
                scraper,
                game_links,
                target_date,
                args.single_division,
                args.single_gender,
                config.output_dir
            )
            
            logger.info("Scraping completed!")
            return 0
        except Exception as e:
            logger.error(f"Error in single division/gender scraping: {e}")
            return 1
    
    # Initialize scraper
    scraper = NCAAScraper(config)
    
    try:
        if args.backfill:
            # Backfill specific dates
            backfill_dates = [
                date(2025, 1, 12),
                date(2025, 2, 15)  # Add your desired date here
            ]
            
            for target_date in backfill_dates:
                logger.info(f"Backfilling data for {target_date}")
                scraping_config = ScrapingConfig.for_backfill(
                    [target_date], divisions, genders, config.output_dir, 
                    config.upload_to_gdrive, config.google_drive_folder_id,
                    force_rescrape=args.force_rescrape
                )
                _run_scraping_session(scraper, scraping_config)
        else:
            # Regular scraping for specified date or yesterday
            target_date = _parse_date(args.date) if args.date else get_yesterday()
            logger.info(f"Scraping data for {target_date}")
            
            scraping_config = ScrapingConfig.for_single_date(
                target_date, divisions, genders, config.output_dir,
                config.upload_to_gdrive, config.google_drive_folder_id,
                force_rescrape=args.force_rescrape
            )
            _run_scraping_session(scraper, scraping_config)
        
        logger.info("Scraping completed!")
        return 0
        
    except Exception as e:
        error_msg = f"Unexpected error in main function: {e}"
        logger.error(error_msg)
        scraper.send_notification(error_msg, ErrorType.ERROR)
        return 1


def _parse_date(date_str: str) -> date:
    """Parse date string to date object."""
    from datetime import datetime
    try:
        return datetime.strptime(date_str, '%Y/%m/%d').date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Expected YYYY/MM/DD")
        raise


def _run_scraping_session(scraper: NCAAScraper, scraping_config: ScrapingConfig):
    """Run a scraping session for the given configuration."""
    # Set force_rescrape flag on scraper instance
    scraper.force_rescrape = scraping_config.force_rescrape
    
    # Generate URLs for all dates in range
    all_urls = []
    current_date = scraping_config.date_range.start_date
    end_date = scraping_config.date_range.end_date or scraping_config.date_range.start_date
    
    from datetime import timedelta
    
    while current_date <= end_date:
        date_str = format_date_for_url(current_date)
        urls = generate_ncaa_urls(date_str, scraping_config.divisions, scraping_config.genders)
        all_urls.extend(urls)
        current_date += timedelta(days=1)
    
    # Pre-check Google Drive for existing files (if enabled and not forcing rescrape)
    if scraping_config.upload_to_gdrive and not scraping_config.force_rescrape:
        logger.info("Pre-checking Google Drive for existing files...")
        _precheck_google_drive(scraper, all_urls)
    elif scraping_config.force_rescrape:
        logger.info("Force rescrape enabled - will override existing Google Drive files")
    
    # Scrape each URL with progress logging and cleanup between URLs
    total_urls = len(all_urls)
    logger.info(f"Starting scraping session: {total_urls} URLs to process")
    
    for idx, url in enumerate(all_urls, 1):
        try:
            logger.info(f"Processing URL {idx}/{total_urls}: {url}")
            scraper.scrape(url)
            
            # Cleanup driver between URLs to prevent resource buildup
            if scraper.driver:
                try:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(2)  # Brief pause between URLs for resource cleanup
                except Exception as cleanup_error:
                    logger.warning(f"Error during driver cleanup between URLs: {cleanup_error}")
                    
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            # Ensure driver is cleaned up even on error
            if scraper.driver:
                try:
                    SeleniumUtils.safe_quit_driver(scraper.driver)
                    scraper.driver = None
                    SeleniumUtils._cleanup_driver_resources()
                except Exception:
                    pass
            continue
    
    logger.info(f"Completed scraping session: {total_urls} URLs processed")


def _scrape_games_from_mapping(
    scraper: NCAAScraper,
    game_links: List[str],
    target_date: date,
    division: str,
    gender: str,
    output_dir: str
):
    """Scrape games from a list of game links (used in single division/gender mode)."""
    from .utils import format_date_for_url
    from .storage import FileManager
    
    date_str = format_date_for_url(target_date)
    year = str(target_date.year)
    month = f"{target_date.month:02d}"
    day = f"{target_date.day:02d}"
    
    csv_path = scraper.file_manager.get_csv_path(year, month, day, gender, division)
    
    logger.info(f"Scraping {len(game_links)} games for {division} {gender}")
    
    # Initialize driver
    try:
        scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {e}")
        return
    
    try:
        scraped_count = 0
        for idx, game_link in enumerate(game_links, 1):
            try:
                logger.info(f"Scraping game {idx}/{len(game_links)}: {game_link}")
                
                # Check if duplicate and handle accordingly
                mapping = getattr(scraper, 'duplicate_mapping', {})
                game_info = mapping.get('game_links', {}).get(game_link, {})
                
                if game_info.get('is_duplicate', False):
                    primary_division = game_info.get('primary_division')
                    if primary_division != division:
                        # This is a duplicate and we're not the primary division
                        # Copy data from primary division's CSV
                        logger.info(f"Game is duplicate, copying from {primary_division} division")
                        primary_csv_path = scraper.file_manager.get_csv_path(year, month, day, gender, primary_division)
                        
                        # Try to read from primary CSV
                        existing_data = scraper.csv_handler.get_game_data_by_link(primary_csv_path, game_link)
                        if existing_data is not None and not existing_data.empty:
                            # Mark as duplicate
                            existing_data = existing_data.copy()
                            if 'DUPLICATE_ACROSS_DIVISIONS' not in existing_data.columns:
                                existing_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                            else:
                                existing_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                            
                            # Append to current division's CSV
                            if scraper.csv_handler.append_game_data(csv_path, existing_data):
                                logger.info(f"Copied duplicate game data from {primary_division}")
                                scraped_count += 1
                            continue
                        else:
                            logger.warning(f"Could not find game in {primary_division} CSV, will scrape instead")
                
                # Scrape the game normally
                game_data = scraper._scrape_single_game(
                    game_link, year, month, day, gender, division, csv_path
                )
                
                if game_data:
                    scraped_count += 1
                
                # Recreate driver every 20 games
                if idx > 0 and idx % 20 == 0:
                    logger.info(f"Recreating driver after {idx} games...")
                    try:
                        SeleniumUtils._cleanup_driver_resources()
                        SeleniumUtils.safe_quit_driver(scraper.driver)
                        scraper.driver = None
                        time.sleep(3)
                        scraper.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e:
                        logger.warning(f"Error recreating driver: {e}")
                
            except Exception as e:
                logger.error(f"Error scraping game {game_link}: {e}")
                continue
        
        logger.info(f"Scraped {scraped_count}/{len(game_links)} games successfully")
        
        # Upload to Google Drive if enabled
        if scraper.config.upload_to_gdrive and scraper.file_manager.file_exists_and_has_content(csv_path):
            logger.info(f"Uploading CSV to Google Drive: {csv_path}")
            scraper.upload_to_gdrive(csv_path, year, month, gender, division)
            
    finally:
        if scraper.driver:
            SeleniumUtils.safe_quit_driver(scraper.driver)
            scraper.driver = None
            SeleniumUtils._cleanup_driver_resources()


def _precheck_google_drive(scraper: NCAAScraper, urls: List[str]):
    """Pre-check Google Drive for existing files to provide summary."""
    try:
        from .utils import parse_url_components
        
        existing_count = 0
        total_count = len(urls)
        
        for url in urls:
            try:
                components = parse_url_components(url)
                year = components['year']
                month = components['month']
                day = components['day']
                gender = components['gender']
                division = components['division']
                
                gdrive_exists, _ = scraper.google_drive.check_file_exists_in_gdrive(
                    year, month, gender, division, day
                )
                
                if gdrive_exists:
                    existing_count += 1
                    logger.info(f"✓ {gender} {division} {year}-{month}-{day} already exists in Google Drive")
                else:
                    logger.info(f"✗ {gender} {division} {year}-{month}-{day} needs scraping")
                    
            except Exception as e:
                logger.warning(f"Error checking Google Drive for {url}: {e}")
                continue
        
        logger.info(f"Google Drive pre-check complete: {existing_count}/{total_count} files already exist")
        
    except Exception as e:
        logger.error(f"Error during Google Drive pre-check: {e}")


if __name__ == "__main__":
    exit(main())
