"""Base scraper class for the NCAA scraper."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Set, Optional
import logging

from ..config import ScraperConfig
from ..models import GameData
from ..storage import FileManager, CSVHandler, GoogleDriveManager
from ..notifications import DiscordNotifier
from ..config.constants import ErrorType

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.visited_links: Dict[str, str] = {}  # Dict[game_link, division] to track cross-division duplicates
        
        # Initialize components
        self.file_manager = FileManager(config.output_dir)
        self.csv_handler = CSVHandler(self.file_manager)
        self.google_drive = GoogleDriveManager(config)
        self.notifier = DiscordNotifier(config.discord_webhook_url)
    
    @abstractmethod
    def scrape(self, url: str) -> List[GameData]:
        """
        Scrape data from a URL.
        
        Args:
            url: URL to scrape
        
        Returns:
            List of scraped game data
        """
        pass
    
    def is_duplicate(self, game_id: str, csv_path: str) -> bool:
        """
        Check if a game is already in the CSV file.
        
        Args:
            game_id: Game ID to check
            csv_path: Path to CSV file
        
        Returns:
            True if duplicate, False otherwise
        """
        return self.csv_handler.game_exists_in_csv(csv_path, game_id)
    
    def save_game_data(self, game_data: GameData, csv_path: str) -> bool:
        """
        Save game data to CSV file.
        
        Args:
            game_data: Game data to save
            csv_path: Path to CSV file
        
        Returns:
            True if successful, False otherwise
        """
        try:
            combined_df = game_data.to_combined_dataframe()
            return self.csv_handler.append_game_data(csv_path, combined_df)
        except Exception as e:
            self.logger.error(f"Error saving game data: {e}")
            return False
    
    def send_notification(
        self,
        message: str,
        error_type: ErrorType,
        division: Optional[str] = None,
        date: Optional[str] = None,
        gender: Optional[str] = None,
        game_link: Optional[str] = None
    ) -> bool:
        """
        Send a notification.
        
        Args:
            message: Message to send
            error_type: Type of notification
            division: Division (d1, d2, d3)
            date: Date in YYYY-MM-DD format
            gender: Gender (men, women)
            game_link: Specific game link if applicable
        
        Returns:
            True if notification sent successfully, False otherwise
        """
        return self.notifier.send_notification(
            message, error_type, division, date, gender, game_link
        )
    
    def upload_to_gdrive(self, file_path: str, year: str, month: str, gender: str, division: str) -> bool:
        """
        Upload file to Google Drive with intelligent duplicate detection.
        
        Args:
            file_path: Path to file to upload
            year: Year
            month: Month
            gender: Gender
            division: Division
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Preparing to upload {file_path} to Google Drive...")
            
            # Create folder structure
            folder_id = self.google_drive.create_folder_structure(
                year, month, gender, division, self.config.google_drive_folder_id
            )
            
            if not folder_id:
                self.logger.error("Failed to create Google Drive folder structure")
                return False
            
            # Check if file should be uploaded (intelligent duplicate detection)
            should_upload, existing_file_id = self.google_drive.should_upload_file(file_path, folder_id)
            
            if not should_upload:
                self.logger.info(f"File already exists and is up-to-date in Google Drive, skipping upload")
                return True
            
            # Upload the file
            file_id = self.google_drive.upload_file(file_path, folder_id)
            
            if file_id:
                self.logger.info(f"Successfully uploaded to Google Drive: {file_path} (ID: {file_id})")
                
                # Get upload stats for the folder
                stats = self.google_drive.get_upload_stats(folder_id)
                if stats:
                    self.logger.info(f"Google Drive folder stats: {stats['total_files']} files, "
                                   f"{stats['csv_files']} CSV files, "
                                   f"{stats['total_size'] / 1024 / 1024:.2f} MB total")
                
                return True
            else:
                self.logger.error(f"Failed to upload {file_path} to Google Drive")
                return False
                
        except Exception as e:
            self.logger.error(f"Error uploading to Google Drive: {e}")
            return False
