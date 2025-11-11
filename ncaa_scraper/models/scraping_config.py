"""Configuration models for scraping operations."""

from dataclasses import dataclass
from typing import List, Optional
from datetime import date
from ..config.constants import Division, Gender


@dataclass
class DateRange:
    """Represents a date range for scraping."""
    start_date: date
    end_date: Optional[date] = None
    
    def __post_init__(self):
        if self.end_date is None:
            self.end_date = self.start_date


@dataclass
class ScrapingConfig:
    """Configuration for a scraping operation."""
    date_range: DateRange
    divisions: List[Division]
    genders: List[Gender]
    output_dir: str
    upload_to_gdrive: bool = False
    gdrive_folder_id: Optional[str] = None
    force_rescrape: bool = False
    
    @classmethod
    def for_single_date(
        cls,
        target_date: date,
        divisions: List[Division] = None,
        genders: List[Gender] = None,
        output_dir: str = "scraped_data",
        upload_to_gdrive: bool = False,
        gdrive_folder_id: Optional[str] = None,
        force_rescrape: bool = False
    ) -> 'ScrapingConfig':
        """Create configuration for a single date."""
        if divisions is None:
            divisions = [Division.D3]  # Default to D3 only
        if genders is None:
            genders = [Gender.WOMEN]  # Default to women only
            
        return cls(
            date_range=DateRange(target_date),
            divisions=divisions,
            genders=genders,
            output_dir=output_dir,
            upload_to_gdrive=upload_to_gdrive,
            gdrive_folder_id=gdrive_folder_id,
            force_rescrape=force_rescrape
        )
    
    @classmethod
    def for_backfill(
        cls,
        dates: List[date],
        divisions: List[Division] = None,
        genders: List[Gender] = None,
        output_dir: str = "scraped_data",
        upload_to_gdrive: bool = False,
        gdrive_folder_id: Optional[str] = None,
        force_rescrape: bool = False
    ) -> 'ScrapingConfig':
        """Create configuration for backfill operation."""
        if divisions is None:
            divisions = [Division.D3]
        if genders is None:
            genders = [Gender.WOMEN]
            
        return cls(
            date_range=DateRange(dates[0], dates[-1] if len(dates) > 1 else None),
            divisions=divisions,
            genders=genders,
            output_dir=output_dir,
            upload_to_gdrive=upload_to_gdrive,
            gdrive_folder_id=gdrive_folder_id,
            force_rescrape=force_rescrape
        )
