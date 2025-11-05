"""Data models for game and team data."""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List
import pandas as pd


@dataclass
class TeamData:
    """Data for a single team in a game."""
    team_name: str
    opponent_name: str
    game_id: str
    game_link: str
    stats: pd.DataFrame
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        stats_dict = self.stats.to_dict('records') if not self.stats.empty else []
        return {
            'team_name': self.team_name,
            'opponent_name': self.opponent_name,
            'game_id': self.game_id,
            'game_link': self.game_link,
            'stats': stats_dict
        }


@dataclass
class GameData:
    """Complete game data including both teams."""
    game_id: str
    game_link: str
    team_one: TeamData
    team_two: TeamData
    date: str
    division: str
    gender: str
    duplicate_across_divisions: bool = False  # Flag to mark if this game was already scraped in another division
    
    def to_combined_dataframe(self) -> pd.DataFrame:
        """Convert to combined pandas DataFrame for CSV export."""
        # Create copies of team dataframes
        team_one_df = self.team_one.stats.copy()
        team_two_df = self.team_two.stats.copy()
        
        # Add metadata columns
        team_one_df['TEAM'] = self.team_one.team_name
        team_one_df['OPP'] = self.team_one.opponent_name
        team_one_df['GAMEID'] = self.game_id
        team_one_df['GAMELINK'] = self.game_link
        team_one_df['DUPLICATE_ACROSS_DIVISIONS'] = self.duplicate_across_divisions
        
        team_two_df['TEAM'] = self.team_two.team_name
        team_two_df['OPP'] = self.team_two.opponent_name
        team_two_df['GAMEID'] = self.game_id
        team_two_df['GAMELINK'] = self.game_link
        team_two_df['DUPLICATE_ACROSS_DIVISIONS'] = self.duplicate_across_divisions
        
        # Combine both teams
        combined_df = pd.concat([team_one_df, team_two_df], ignore_index=True)
        
        return combined_df
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            'game_id': self.game_id,
            'game_link': self.game_link,
            'team_one': self.team_one.to_dict(),
            'team_two': self.team_two.to_dict(),
            'date': self.date,
            'division': self.division,
            'gender': self.gender
        }
