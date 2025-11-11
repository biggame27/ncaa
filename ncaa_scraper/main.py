"""Main entry point for the refactored NCAA scraper."""

import argparse
import logging
from datetime import date
from typing import List

from .config import get_config, Division, Gender
from .scrapers import NCAAScraper
from .utils import get_yesterday, format_date_for_url, generate_ncaa_urls
from .models import ScrapingConfig, DateRange
from .config.constants import ErrorType

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
    
    # Scrape each URL
    for url in all_urls:
        try:
            scraper.scrape(url)
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            continue


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
