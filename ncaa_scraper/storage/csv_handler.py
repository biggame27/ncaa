"""CSV handling utilities for the NCAA scraper."""

import os
import pandas as pd
import logging
from typing import Optional, Set

logger = logging.getLogger(__name__)


class CSVHandler:
    """Handles CSV file operations for game data."""
    
    def __init__(self, file_manager):
        self.file_manager = file_manager
    
    def game_exists_in_csv(self, csv_path: str, game_id: str) -> bool:
        """
        Check if a game already exists in the CSV file.
        
        Args:
            csv_path: Path to the CSV file
            game_id: Game ID to check for
        
        Returns:
            True if game exists, False otherwise
        """
        if not os.path.exists(csv_path):
            return False
        
        try:
            df = pd.read_csv(csv_path)
            if 'GAMEID' in df.columns:
                return game_id in df['GAMEID'].values
            return False
        except Exception as e:
            logger.warning(f"Error reading CSV file {csv_path}: {e}")
            return False
    
    def append_game_data(self, csv_path: str, game_data_df: pd.DataFrame) -> bool:
        """
        Append game data to CSV file.
        
        Args:
            csv_path: Path to the CSV file
            game_data_df: DataFrame containing game data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            file_exists = os.path.exists(csv_path)
            game_data_df.to_csv(csv_path, index=False, header=not file_exists, mode='a')
            logger.info(f"Successfully saved {len(game_data_df)} rows to: {csv_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to {csv_path}: {e}")
            return False
    
    def read_csv_safely(self, csv_path: str) -> Optional[pd.DataFrame]:
        """
        Safely read CSV file.
        
        Args:
            csv_path: Path to the CSV file
        
        Returns:
            DataFrame if successful, None otherwise
        """
        try:
            if not os.path.exists(csv_path):
                return None
            return pd.read_csv(csv_path)
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_path}: {e}")
            return None
    
    def get_existing_game_ids(self, csv_path: str) -> Set[str]:
        """
        Get set of existing game IDs from CSV file.
        
        Args:
            csv_path: Path to the CSV file
        
        Returns:
            Set of existing game IDs
        """
        df = self.read_csv_safely(csv_path)
        if df is not None and 'GAMEID' in df.columns:
            return set(df['GAMEID'].values)
        return set()
    
    def validate_csv_structure(self, csv_path: str) -> bool:
        """
        Validate that CSV has the expected structure.
        
        Args:
            csv_path: Path to the CSV file
        
        Returns:
            True if structure is valid, False otherwise
        """
        df = self.read_csv_safely(csv_path)
        if df is None:
            return False
        
        required_columns = ['GAMEID', 'TEAM', 'OPP', 'GAMELINK']
        return all(col in df.columns for col in required_columns)
    
    def get_game_data_by_link(self, csv_path: str, game_link: str) -> Optional[pd.DataFrame]:
        """
        Get all rows for a specific game link from CSV file.
        
        Args:
            csv_path: Path to the CSV file
            game_link: Game link to search for
        
        Returns:
            DataFrame with game data rows, or None if not found
        """
        df = self.read_csv_safely(csv_path)
        if df is None or 'GAMELINK' not in df.columns:
            return None
        
        game_rows = df[df['GAMELINK'] == game_link].copy()
        if game_rows.empty:
            return None
        
        return game_rows
    
    def update_duplicate_flag(self, csv_path: str, game_link: str, duplicate_value: bool = True) -> bool:
        """
        Update DUPLICATE_ACROSS_DIVISIONS flag for a game in CSV file.
        
        Args:
            csv_path: Path to the CSV file
            game_link: Game link to update
            duplicate_value: Value to set for DUPLICATE_ACROSS_DIVISIONS
        
        Returns:
            True if successful, False otherwise
        """
        try:
            df = self.read_csv_safely(csv_path)
            if df is None:
                return False
            
            if 'GAMELINK' not in df.columns:
                return False
            
            # Ensure DUPLICATE_ACROSS_DIVISIONS column exists
            if 'DUPLICATE_ACROSS_DIVISIONS' not in df.columns:
                df['DUPLICATE_ACROSS_DIVISIONS'] = False
            
            # Update rows for this game
            mask = df['GAMELINK'] == game_link
            df.loc[mask, 'DUPLICATE_ACROSS_DIVISIONS'] = duplicate_value
            
            # Save updated CSV
            df.to_csv(csv_path, index=False)
            logger.info(f"Updated DUPLICATE_ACROSS_DIVISIONS flag for game {game_link} in {csv_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating duplicate flag in {csv_path}: {e}")
            return False