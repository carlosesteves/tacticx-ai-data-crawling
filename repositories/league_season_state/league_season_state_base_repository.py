from abc import abstractmethod
from typing import Protocol, Any, Optional
from models.league_season_state import LeagueSeasonState


class ILeagueSeasonStateRepository(Protocol):
    def __init__(self, client):
        self.client = client
        super().__init__()

    def get_state(self, league_id: int, season_id: int) -> Optional[LeagueSeasonState]:
        """Get the state for a specific league-season combination"""
        ...
    
    def save_state(self, state: LeagueSeasonState) -> Any:
        """Save or update the state for a league-season"""
        ...
    
    def delete_state(self, league_id: int, season_id: int) -> Any:
        """Delete the state for a league-season"""
        ...
