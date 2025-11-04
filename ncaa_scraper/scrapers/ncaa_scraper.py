"""NCAA-specific scraper implementation."""

import time
import logging
from typing import List, Optional, Set
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
                
                # Get game links
                game_links = self._extract_game_links()
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
                
                # Filter out already visited links
                new_links = [link for link in game_links if link not in self.visited_links]
                skipped_count = len(game_links) - len(new_links)
                
                if skipped_count > 0:
                    self.logger.info(f"Found {len(game_links)} total games, {skipped_count} already visited, {len(new_links)} new games to scrape")
                else:
                    self.logger.info(f"Found {len(game_links)} games to scrape")
                
                # Scrape each game
                scraped_games = []
                for idx, game_link in enumerate(new_links):
                    try:
                        # Recreate driver every 20 games to prevent memory/resource buildup
                        if idx > 0 and idx % 20 == 0:
                            self.logger.info(f"Recreating driver after {idx} games to prevent resource buildup...")
                            try:
                                SeleniumUtils.safe_quit_driver(self.driver)
                            except Exception as e:
                                self.logger.warning(f"Error quitting old driver: {e}")
                            
                            # Create new driver
                            try:
                                self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                            except Exception as e:
                                self.logger.error(f"Failed to recreate driver: {e}")
                                # Try once more, then give up
                                try:
                                    SeleniumUtils._cleanup_driver_resources()
                                    time.sleep(2)
                                    self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                                except Exception as e2:
                                    self.logger.error(f"Failed to recreate driver after cleanup: {e2}")
                                    return scraped_games  # Return what we have so far
                        
                        game_data = self._scrape_single_game(
                            game_link, year, month, day, gender, division, csv_path
                        )
                        if game_data:
                            scraped_games.append(game_data)
                            self.visited_links.add(game_link)
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
            
            # Navigate with timeout handling
            try:
                self.driver.get(url)
            except TimeoutException:
                self.logger.warning(f"Page load timeout for scoreboard {url}, attempting recovery...")
                try:
                    self.driver.execute_script("window.stop();")
                    time.sleep(1)
                except Exception:
                    # Driver is stuck - recreate it
                    self.logger.warning("Driver unresponsive, recreating...")
                    try:
                        SeleniumUtils.safe_quit_driver(self.driver)
                        self.driver = SeleniumUtils.create_driver(headless=True, max_retries=3)
                        # Retry the page load once
                        try:
                            self.driver.get(url)
                        except Exception as e2:
                            self.logger.error(f"Failed to load page after driver recreation: {e2}")
                            return False
                    except Exception as e2:
                        self.logger.error(f"Failed to recreate driver: {e2}")
                        return False
            
            # Add another delay after page load
            SeleniumUtils.human_like_delay(2.0, 4.0)
            
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
    
    def _extract_game_links(self) -> List[str]:
        """Extract game links from the scoreboard page."""
        try:
            box_scores = self.driver.find_elements(By.CLASS_NAME, "gamePod-link")
            game_links = [box_score.get_attribute('href') for box_score in box_scores if box_score.get_attribute('href')]
            return game_links
        except WebDriverException as e:
            error_msg = f"Selenium error finding game links: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR
            )
            return []
        except Exception as e:
            error_msg = f"Unexpected error finding game links: {e}"
            self.logger.error(error_msg)
            self.send_notification(
                error_msg,
                ErrorType.ERROR
            )
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
            self.logger.info(f"Game link {game_link} already visited in this session, skipping...")
            return None
        
        self.logger.info(f"Scraping: {game_link}")
        
        try:
            # Navigate to game page with timeout handling
            try:
                self.driver.get(game_link)
                self.logger.info(f"Successfully navigated to: {game_link}")
            except TimeoutException:
                self.logger.warning(f"Page load timeout for game {game_link}")
                try:
                    self.driver.execute_script("window.stop();")
                    time.sleep(1)
                except Exception:
                    # Driver stuck, skip this game
                    self.logger.error(f"Driver unresponsive for {game_link}, skipping...")
                    return None
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, self.config.wait_timeout)
            
            # Check for team selector
            team_selector = SeleniumUtils.wait_for_element(
                self.driver, By.CLASS_NAME, "boxscore-team-selector", self.config.wait_timeout
            )
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
            
            # Create game data
            game_data = GameData(
                game_id=game_id,
                game_link=game_link,
                team_one=team_one_data,
                team_two=team_two_data,
                date=f"{year}-{month}-{day}",
                division=division,
                gender=gender
            )
            
            # Save to CSV
            if self.save_game_data(game_data, csv_path):
                self.logger.info(f"Successfully saved game data for {game_id}")
                return game_data
            else:
                self.logger.error(f"Failed to save game data for {game_id}")
                return None
                
        except Exception as e:
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
            
            table = boxscore_table.find_element(By.TAG_NAME, 'table')
            df = pd.read_html(StringIO(table.get_attribute('outerHTML')))[0]
            
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
