"""NCAA-specific scraper implementation."""

import time
import logging
from typing import List, Optional, Set, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from io import StringIO
import pandas as pd

from .base_scraper import BaseScraper
from .selenium_utils import SeleniumUtils
from ..models import GameData, TeamData
from ..utils import parse_url_components, extract_game_id_from_url
from ..config.constants import ErrorType

logger = logging.getLogger(__name__)


class NCAAScraper(BaseScraper):
    """NCAA basketball box score scraper."""
    
    def __init__(self, config):
        super().__init__(config)
        self.driver: Optional[webdriver.Chrome] = None
    
    def scrape(self, url: str) -> List[GameData]:
        """
        Scrape NCAA box scores from a scoreboard URL.
        
        Args:
            url: NCAA scoreboard URL
        
        Returns:
            List of scraped game data
        """
        try:
            # Parse URL components
            components = parse_url_components(url)
            year = components['year']
            month = components['month']
            day = components['day']
            gender = components['gender']
            division = components['division']
            
            # Create CSV path
            csv_path = self.file_manager.get_csv_path(year, month, day, gender, division)
            
            # Check if data already exists locally
            if self.file_manager.file_exists_and_has_content(csv_path):
                self.logger.info(f"Data for {gender} {division} on {year}-{month}-{day} already exists locally, skipping...")
                return []
            
            # Check if data already exists in Google Drive (if enabled)
            if self.config.upload_to_gdrive:
                gdrive_exists, gdrive_file_id = self.google_drive.check_file_exists_in_gdrive(
                    year, month, gender, division, day
                )
                if gdrive_exists:
                    self.logger.info(f"Data for {gender} {division} on {year}-{month}-{day} already exists in Google Drive, skipping...")
                    return []
            
            self.logger.info(f"Processing: {csv_path}")
            
            # Initialize driver with retry mechanism
            try:
                self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
            except Exception as e:
                error_msg = f"Failed to initialize WebDriver: {e}"
                self.logger.error(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.ERROR,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender
                )
                return []
            
            try:
                # Load scoreboard page
                if not self._load_scoreboard_page(url, division, gender, f"{year}-{month}-{day}"):
                    return []
                
                # Get game links (pass URL for retry purposes)
                game_links = self._extract_game_links(url)
                if not game_links:
                    no_links_msg = f"No valid game links found for {url}"
                    self.logger.warning(no_links_msg)
                    self.send_notification(
                        no_links_msg,
                        ErrorType.INFO,
                        division=division,
                        date=f"{year}-{month}-{day}",
                        gender=gender
                    )
                    return []
                
                # Filter links - keep cross-division duplicates, skip same-division duplicates
                new_links = []
                cross_division_duplicates = []
                for link in game_links:
                    if link in self.visited_links:
                        # Check if it was visited in a different division
                        previous_division = self.visited_links[link]
                        if previous_division != division:
                            # Cross-division duplicate - scrape it but mark it
                            cross_division_duplicates.append(link)
                            new_links.append(link)
                        # If same division, skip it (already visited in this division)
                    else:
                        # Not visited yet
                        new_links.append(link)
                
                skipped_count = len(game_links) - len(new_links)
                
                if skipped_count > 0:
                    self.logger.info(f"Found {len(game_links)} total games, {skipped_count} already visited in same division, {len(new_links)} new games to scrape")
                else:
                    self.logger.info(f"Found {len(game_links)} games to scrape")
                
                if cross_division_duplicates:
                    self.logger.info(f"Found {len(cross_division_duplicates)} cross-division duplicate games that will be marked")
                
                # Scrape each game
                scraped_games = []
                for idx, game_link in enumerate(new_links):
                    try:
                        # Recreate driver every 20 games to prevent memory/resource buildup
                        if idx > 0 and idx % 20 == 0:
                            self.logger.info(f"Recreating driver after {idx} games to prevent resource buildup...")
                            try:
                                # Aggressive cleanup before recreating
                                SeleniumUtils._cleanup_driver_resources()
                                SeleniumUtils.safe_quit_driver(self.driver)
                            except Exception as e:
                                self.logger.warning(f"Error quitting old driver: {e}")
                            
                            # Wait a bit for processes to fully terminate
                            time.sleep(3)
                            
                            # Create new driver
                            try:
                                self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            except Exception as e:
                                self.logger.error(f"Failed to recreate driver: {e}")
                                # Try once more with longer cleanup
                                SeleniumUtils._cleanup_driver_resources()
                                time.sleep(5)
                                try:
                                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                                except Exception as e2:
                                    self.logger.error(f"Failed to recreate driver after cleanup: {e2}")
                                    return scraped_games  # Return what we have so far
                        
                        game_data = self._scrape_single_game(
                            game_link, year, month, day, gender, division, csv_path
                        )
                        if game_data:
                            scraped_games.append(game_data)
                            # Update visited_links with current division
                            self.visited_links[game_link] = division
                    except Exception as e:
                        self.logger.error(f"Error scraping game {game_link}: {e}")
                        self.send_notification(
                            f"Error scraping game: {e}",
                            ErrorType.GAME_ERROR,
                            division=division,
                            date=f"{year}-{month}-{day}",
                            gender=gender,
                            game_link=game_link
                        )
                        continue
                
                # Upload to Google Drive if enabled
                if self.config.upload_to_gdrive and self.file_manager.file_exists_and_has_content(csv_path):
                    self.logger.info(f"Uploading completed CSV for {gender} {division}: {csv_path}")
                    self.upload_to_gdrive(csv_path, year, month, gender, division)
                
                return scraped_games
                
            finally:
                if self.driver:
                    SeleniumUtils.safe_quit_driver(self.driver)
                    self.driver = None
                    
        except Exception as e:
            self.logger.error(f"Unexpected error in scrape method: {e}")
            self.send_notification(
                f"Unexpected error in scrape method: {e}",
                ErrorType.ERROR,
                division=components.get('division') if 'components' in locals() else None,
                date=f"{components.get('year')}-{components.get('month')}-{components.get('day')}" if 'components' in locals() else None,
                gender=components.get('gender') if 'components' in locals() else None
            )
            return []
    
    def _load_scoreboard_page(self, url: str, division: str, gender: str, date: str) -> bool:
        """Load the scoreboard page and check for errors."""
        try:
            self.logger.info(f"Loading scoreboard page: {url}")
            
            # Add human-like delay before loading
            SeleniumUtils.human_like_delay(1.0, 2.0)
            
            # Navigate with timeout handling and protection against read timeouts
            try:
                # Use safe_driver_operation to prevent read timeout errors
                load_success = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.get(url),
                    timeout=90,  # 90 seconds max (longer than page_load_timeout of 60s)
                    operation_name=f"load scoreboard page {url}"
                )
                
                if load_success is None:
                    # Page load hung - force stop
                    self.logger.warning(f"Page load hung for scoreboard {url}, attempting recovery...")
                    try:
                        SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.execute_script("window.stop();"),
                            timeout=5,
                            operation_name="stop page load"
                        )
                        time.sleep(1)
                    except Exception:
                        # Driver is stuck - recreate it
                        self.logger.warning("Driver unresponsive, recreating...")
                        try:
                            SeleniumUtils.safe_quit_driver(self.driver)
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            # Retry the page load once
                            load_success = SeleniumUtils.safe_driver_operation(
                                self.driver,
                                lambda: self.driver.get(url),
                                timeout=90,
                                operation_name=f"retry load scoreboard page {url}"
                            )
                            if load_success is None:
                                self.logger.error(f"Page load still hung after driver recreation for {url}")
                                return False
                            # Give page time to stabilize after recovery
                            time.sleep(3)
                        except Exception as e2:
                            self.logger.error(f"Failed to recreate driver: {e2}")
                            return False
                            
            except TimeoutException:
                self.logger.warning(f"Page load timeout for scoreboard {url}, attempting recovery...")
                try:
                    SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda: self.driver.execute_script("window.stop();"),
                        timeout=5,
                        operation_name="stop page load"
                    )
                    time.sleep(1)
                except Exception:
                    # Driver is stuck - recreate it
                    self.logger.warning("Driver unresponsive, recreating...")
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Retry the page load once
                        load_success = SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.get(url),
                            timeout=90,
                            operation_name=f"retry load scoreboard page {url}"
                        )
                        if load_success is None:
                            self.logger.error(f"Failed to load page after driver recreation: {url}")
                            return False
                        # Give page time to stabilize after recovery
                        time.sleep(3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return False
            except Exception as e:
                # Catch ANY exception during get() - driver might be frozen
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen/unresponsive during scoreboard load for {url}: {e}")
                    # Recreate driver and retry once
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Retry the page load once
                        load_success = SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.get(url),
                            timeout=90,
                            operation_name=f"retry load scoreboard page {url}"
                        )
                        if load_success is None:
                            self.logger.error(f"Failed to load page after driver recreation: {url}")
                            return False
                        # Give page time to stabilize after recovery
                        time.sleep(3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return False
                else:
                    # Re-raise other exceptions
                    raise
            
            # Add another delay after page load to ensure stability
            SeleniumUtils.human_like_delay(2.0, 4.0)
            
            # Verify driver is responsive before proceeding
            try:
                test_result = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.current_url,
                    timeout=5,
                    default_return=None,
                    operation_name="verify driver responsive"
                )
                if test_result is None:
                    self.logger.warning("Driver not responsive after page load, may need recovery")
            except Exception as e:
                self.logger.warning(f"Driver health check failed: {e}")
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, self.config.wait_timeout)
            
            # If the page explicitly shows a no-games message, treat as non-error and stop
            try:
                no_games = self.driver.find_elements(By.CLASS_NAME, "no-games-message")
                if no_games:
                    no_games_msg = f"No games found on scoreboard page: {url}"
                    self.logger.info(no_games_msg)
                    self.send_notification(
                        no_games_msg,
                        ErrorType.INFO,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
            except Exception:
                # If this probe fails, continue with normal flow
                pass
            
            try:
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "gamePod-link")))
                return True
            except TimeoutException:
                # Double-check for explicit no-games message before treating as error
                try:
                    no_games = self.driver.find_elements(By.CLASS_NAME, "no-games-message")
                    if no_games:
                        no_games_msg = f"No games found on scoreboard page: {url}"
                        self.logger.info(no_games_msg)
                        self.send_notification(
                            no_games_msg,
                            ErrorType.INFO,
                            division=division,
                            date=date,
                            gender=gender
                        )
                        return False
                except Exception:
                    pass
                # Check for specific error pages
                error_msg = SeleniumUtils.check_for_errors(self.driver)
                if error_msg:
                    self.logger.warning(f"{error_msg} for {url}")
                    self.send_notification(
                        f"{error_msg} for {url}",
                        ErrorType.WARNING,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
                
                # Check for HTTP errors
                http_error = SeleniumUtils.check_http_status(self.driver)
                if http_error:
                    self.logger.warning(f"{http_error} for {url}")
                    self.send_notification(
                        f"{http_error} for {url}",
                        ErrorType.ERROR,
                        division=division,
                        date=date,
                        gender=gender
                    )
                    return False
                
                # No games found on scoreboard page - send notification
                no_games_msg = f"No games found on scoreboard page: {url}"
                self.logger.warning(no_games_msg)
                self.send_notification(
                    no_games_msg,
                    ErrorType.INFO,
                    division=division,
                    date=date,
                    gender=gender
                )
                return False
                
        except WebDriverException as e:
            error_msg = f"Selenium WebDriver error loading scoreboard page {url}: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR,
                division=division,
                date=date,
                gender=gender
            )
            return False
        except Exception as e:
            error_msg = f"Unexpected error loading scoreboard page {url}: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR,
                division=division,
                date=date,
                gender=gender
            )
            return False
    
    def _extract_game_links(self, scoreboard_url: Optional[str] = None) -> List[str]:
        """Extract game links from the scoreboard page with retry logic."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = 2 * (attempt)  # 2s, 4s delays
                    self.logger.info(f"Retrying game link extraction (attempt {attempt + 1}/{max_retries}) after {delay}s delay")
                    time.sleep(delay)
                
                # Wrap find_elements in safe_driver_operation to prevent timeouts
                box_scores = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.find_elements(By.CLASS_NAME, "gamePod-link"),
                    timeout=30,  # Shorter timeout to fail fast
                    default_return=[],
                    operation_name="find game links"
                )
                
                if not box_scores:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"No game links found, retrying...")
                        continue
                    self.logger.warning("No game links found after all retries")
                    return []
                
                # Extract hrefs with timeout protection
                game_links = []
                for box_score in box_scores:
                    href = SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda bs=box_score: bs.get_attribute('href'),
                        timeout=10,  # Short timeout per element
                        default_return=None,
                        operation_name="get href attribute"
                    )
                    if href:
                        game_links.append(href)
                
                if game_links:
                    return game_links
                elif attempt < max_retries - 1:
                    self.logger.warning(f"No valid hrefs found, retrying...")
                    continue
                else:
                    self.logger.warning("No valid game links found after all retries")
                    return []
                    
            except Exception as e:
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen during link extraction (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        # Recreate driver and retry
                        try:
                            SeleniumUtils._cleanup_driver_resources()
                            SeleniumUtils.safe_quit_driver(self.driver)
                        except Exception as e2:
                            self.logger.warning(f"Error during cleanup: {e2}")
                        
                        time.sleep(5)
                        
                        try:
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            # Reload the scoreboard page if URL is available
                            if scoreboard_url:
                                SeleniumUtils.safe_driver_operation(
                                    self.driver,
                                    lambda url=scoreboard_url: self.driver.get(url),
                                    timeout=90,
                                    operation_name="reload scoreboard page"
                                )
                                time.sleep(2)
                            else:
                                # Try to get current URL as fallback
                                current_url = SeleniumUtils.safe_driver_operation(
                                    self.driver,
                                    lambda: self.driver.current_url,
                                    timeout=5,
                                    default_return=None,
                                    operation_name="get current URL"
                                )
                                if current_url:
                                    SeleniumUtils.safe_driver_operation(
                                        self.driver,
                                        lambda url=current_url: self.driver.get(url),
                                        timeout=90,
                                        operation_name="reload scoreboard page"
                                    )
                                    time.sleep(2)
                        except Exception as e2:
                            self.logger.error(f"Failed to recreate driver: {e2}")
                            if attempt == max_retries - 1:
                                return []
                            continue
                    else:
                        return []
                else:
                    error_msg = f"Error finding game links (attempt {attempt + 1}/{max_retries}): {e}"
                    self.logger.error(error_msg)
                    if attempt < max_retries - 1:
                        continue
                    self.send_notification(
                        error_msg,
                        ErrorType.ERROR
                    )
                    return []
        
        return []
    
    def _scrape_single_game(
        self, 
        game_link: str, 
        year: str, 
        month: str, 
        day: str, 
        gender: str, 
        division: str,
        csv_path: str
    ) -> Optional[GameData]:
        """Scrape a single game's box score data."""
        game_id = extract_game_id_from_url(game_link)
        
        # Check if game already exists in CSV
        if self.is_duplicate(game_id, csv_path):
            self.logger.info(f"Game {game_id} already exists in {csv_path}, skipping...")
            return None
        
        # Check if already visited in this session
        if game_link in self.visited_links:
            previous_division = self.visited_links[game_link]
            if previous_division == division:
                # Same division - skip
                self.logger.info(f"Game link {game_link} already visited in this session for {division}, skipping...")
                return None
            else:
                # Different division - reuse existing data instead of rescraping
                self.logger.info(f"Game link {game_link} already scraped in {previous_division}, reusing existing data for {division}")
                
                # Get the CSV path for the previous division
                previous_csv_path = self.file_manager.get_csv_path(year, month, day, gender, previous_division)
                
                # Read the game data from the previous division's CSV
                existing_game_data = self.csv_handler.get_game_data_by_link(previous_csv_path, game_link)
                
                if existing_game_data is not None and not existing_game_data.empty:
                    # Update the previous division's CSV to mark it as duplicate
                    if self.csv_handler.update_duplicate_flag(previous_csv_path, game_link, duplicate_value=True):
                        # Upload updated CSV to Google Drive if enabled
                        if self.config.upload_to_gdrive:
                            self.logger.info(f"Uploading updated CSV with duplicate flag to Google Drive: {previous_csv_path}")
                            self.upload_to_gdrive(previous_csv_path, year, month, gender, previous_division)
                    
                    # Copy the data to current division's CSV with duplicate flag set
                    existing_game_data = existing_game_data.copy()
                    
                    # Ensure DUPLICATE_ACROSS_DIVISIONS column exists and set to True
                    if 'DUPLICATE_ACROSS_DIVISIONS' not in existing_game_data.columns:
                        existing_game_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                    else:
                        existing_game_data['DUPLICATE_ACROSS_DIVISIONS'] = True
                    
                    # Append to current division's CSV
                    if self.csv_handler.append_game_data(csv_path, existing_game_data):
                        self.logger.info(f"Successfully copied game data from {previous_division} to {division} CSV")
                        # Update visited_links with current division
                        self.visited_links[game_link] = division
                        return None  # Return None since we didn't create new GameData
                    else:
                        self.logger.error(f"Failed to copy game data to {division} CSV")
                        # Fall through to scrape it anyway
                else:
                    self.logger.warning(f"Could not find existing game data in {previous_division} CSV, will scrape instead")
                    # Fall through to scrape it
        
        self.logger.info(f"Scraping: {game_link}")
        
        try:
            # Navigate to game page with timeout handling and protection against read timeouts
            try:
                # Use safe_driver_operation to prevent read timeout errors
                load_success = SeleniumUtils.safe_driver_operation(
                    self.driver,
                    lambda: self.driver.get(game_link),
                    timeout=90,  # 90 seconds max (longer than page_load_timeout of 60s)
                    operation_name=f"load game page {game_link}"
                )
                
                if load_success is None:
                    # Page load hung - force stop
                    self.logger.warning(f"Page load hung for game {game_link}")
                    try:
                        SeleniumUtils.safe_driver_operation(
                            self.driver,
                            lambda: self.driver.execute_script("window.stop();"),
                            timeout=5,
                            operation_name="stop page load"
                        )
                        time.sleep(1)
                    except Exception:
                        # Driver stuck, skip this game
                        self.logger.error(f"Driver unresponsive for {game_link}, skipping...")
                        return None
                else:
                    self.logger.info(f"Successfully navigated to: {game_link}")
                    
            except TimeoutException:
                self.logger.warning(f"Page load timeout for game {game_link}")
                try:
                    SeleniumUtils.safe_driver_operation(
                        self.driver,
                        lambda: self.driver.execute_script("window.stop();"),
                        timeout=5,
                        operation_name="stop page load"
                    )
                    time.sleep(1)
                except Exception:
                    # Driver stuck, skip this game
                    self.logger.error(f"Driver unresponsive for {game_link}, skipping...")
                    return None
            except Exception as e:
                # Catch ANY exception during get() - driver might be frozen
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen/unresponsive during page load for {game_link}: {e}")
                    # Aggressive cleanup and recreation
                    try:
                        # Kill Chrome processes first before trying to quit driver
                        SeleniumUtils._cleanup_driver_resources()
                        # Then try to quit driver (might timeout, but try anyway)
                        SeleniumUtils.safe_quit_driver(self.driver)
                    except Exception as e2:
                        self.logger.warning(f"Error during cleanup: {e2}")
                    
                    # Wait before recreating to let processes fully die
                    time.sleep(5)
                    
                    # Recreate driver
                    try:
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        # If this fails, we're in a bad state - wait longer and try once more
                        SeleniumUtils._cleanup_driver_resources()
                        time.sleep(10)
                        try:
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        except Exception as e3:
                            self.logger.error(f"Failed to recreate driver after extended wait: {e3}")
                            return None  # Give up on this game
                    return None
                else:
                    # Re-raise other exceptions
                    raise
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, self.config.wait_timeout)
            
            # Check for team selector with additional timeout protection
            try:
                team_selector = SeleniumUtils.wait_for_element(
                    self.driver, By.CLASS_NAME, "boxscore-team-selector", self.config.wait_timeout
                )
            except Exception as e:
                error_str = str(e)
                if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                    self.logger.error(f"Driver frozen during element search for {game_link}: {e}")
                    # Aggressive cleanup and recreation
                    try:
                        SeleniumUtils._cleanup_driver_resources()
                        SeleniumUtils.safe_quit_driver(self.driver)
                    except Exception as e2:
                        self.logger.warning(f"Error during cleanup: {e2}")
                    
                    time.sleep(5)
                    
                    try:
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        SeleniumUtils._cleanup_driver_resources()
                        time.sleep(10)
                        try:
                            self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        except Exception as e3:
                            self.logger.error(f"Failed to recreate driver after extended wait: {e3}")
                    return None
                else:
                    # Re-raise other exceptions (like TimeoutException from wait_for_element)
                    raise
            if not team_selector:
                error_msg = f"Box score page may not exist or is not available for {game_link}"
                self.logger.warning(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.WARNING,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender,
                    game_link=game_link
                )
                return None
            
            # Get team names
            team_names = self._extract_team_names(team_selector)
            if len(team_names) < 2:
                self.logger.warning(f"Not enough team names found for {game_link}")
                return None
            
            # Get team data
            team_one_data = self._extract_team_data(team_selector, team_names[0], team_names[1], game_id, game_link)
            if not team_one_data:
                return None
            
            # Switch to second team
            if not self._switch_to_second_team(team_selector, team_names[1]):
                return None
            
            # Get second team data
            team_two_data = self._extract_team_data(team_selector, team_names[1], team_names[0], game_id, game_link)
            if not team_two_data:
                return None
            
            # Check if this is a cross-division duplicate
            is_cross_division_duplicate = (
                game_link in self.visited_links and 
                self.visited_links[game_link] != division
            )
            
            # Create game data
            game_data = GameData(
                game_id=game_id,
                game_link=game_link,
                team_one=team_one_data,
                team_two=team_two_data,
                date=f"{year}-{month}-{day}",
                division=division,
                gender=gender,
                duplicate_across_divisions=is_cross_division_duplicate
            )
            
            # Save to CSV
            if self.save_game_data(game_data, csv_path):
                self.logger.info(f"Successfully saved game data for {game_id}")
                return game_data
            else:
                self.logger.error(f"Failed to save game data for {game_id}")
                return None
                
        except Exception as e:
            # Check if this is a frozen driver error
            error_str = str(e)
            if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                self.logger.error(f"Driver frozen/unresponsive during game scrape for {game_link}: {e}")
                # Aggressive cleanup and recreation
                try:
                    SeleniumUtils._cleanup_driver_resources()
                    SeleniumUtils.safe_quit_driver(self.driver)
                except Exception as e2:
                    self.logger.warning(f"Error during cleanup: {e2}")
                
                time.sleep(5)
                
                try:
                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                except Exception as e2:
                    self.logger.error(f"Failed to recreate driver: {e2}")
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(10)
                    try:
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                    except Exception as e3:
                        self.logger.error(f"Failed to recreate driver after extended wait: {e3}")
                return None
            else:
                # Other errors - log and notify
                error_msg = f"Error scraping game {game_link}: {e}"
                self.logger.error(error_msg)
                self.send_notification(
                    error_msg,
                    ErrorType.GAME_ERROR,
                    division=division,
                    date=f"{year}-{month}-{day}",
                    gender=gender,
                    game_link=game_link
                )
                return None
    
    def _extract_team_names(self, team_selector) -> List[str]:
        """Extract team names from the team selector."""
        try:
            child_divs = team_selector.find_elements(By.TAG_NAME, "div")
            team_names = [div.text.strip() for div in child_divs if div.text.strip()]
            return team_names
        except Exception as e:
            self.logger.error(f"Error extracting team names: {e}")
            return []
    
    def _extract_team_data(self, team_selector, team_name: str, opponent_name: str, game_id: str, game_link: str) -> Optional[TeamData]:
        """Extract data for a single team."""
        try:
            # Wait for box score table
            boxscore_table = SeleniumUtils.wait_for_element(
                self.driver, By.CLASS_NAME, 'gamecenter-tab-boxscore', self.config.wait_timeout
            )
            if not boxscore_table:
                self.logger.warning(f"Box score table not found")
                return None
            
            # Wrap these in safe_driver_operation to prevent HTTPConnectionPool timeouts
            table = SeleniumUtils.safe_driver_operation(
                self.driver,
                lambda: boxscore_table.find_element(By.TAG_NAME, 'table'),
                timeout=20,  # Shorter timeout to fail fast
                default_return=None,
                operation_name=f"find table for {team_name}"
            )
            
            if not table:
                self.logger.warning(f"Table element not found for {team_name}")
                return None
            
            outer_html = SeleniumUtils.safe_driver_operation(
                self.driver,
                lambda: table.get_attribute('outerHTML'),
                timeout=20,  # Shorter timeout to fail fast
                default_return=None,
                operation_name=f"get outerHTML for {team_name}"
            )
            
            if not outer_html:
                self.logger.warning(f"Could not get outerHTML for {team_name}")
                return None
            
            df = pd.read_html(StringIO(outer_html))[0]
            
            if df.empty:
                self.logger.warning(f"Empty box score data for team {team_name}")
                return None
            
            # Remove last 2 rows (typically totals) from individual team data
            if len(df) > 2:
                df = df.iloc[:-2]
            
            return TeamData(
                team_name=team_name,
                opponent_name=opponent_name,
                game_id=game_id,
                game_link=game_link,
                stats=df
            )
            
        except Exception as e:
            error_str = str(e)
            if "HTTPConnectionPool" in error_str or "Read timed out" in error_str:
                self.logger.error(f"Driver frozen while extracting team data for {team_name}: {e}")
            else:
                self.logger.error(f"Error extracting team data for {team_name}: {e}")
            return None
    
    def _switch_to_second_team(self, team_selector, second_team_name: str) -> bool:
        """Switch to the second team's data."""
        try:
            child_divs = team_selector.find_elements(By.TAG_NAME, "div")
            for div in child_divs:
                if div.text.strip() == second_team_name:
                    SeleniumUtils.safe_click(div)
                    time.sleep(self.config.sleep_time)  # Wait for the switch
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Error switching to second team: {e}")
            return False
