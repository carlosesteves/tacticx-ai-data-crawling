from typing import Optional
from models.league_season_state import LeagueSeasonState
from repositories.league_season_state.league_season_state_base_repository import ILeagueSeasonStateRepository


class FakeLeagueSeasonStateRepository(ILeagueSeasonStateRepository):
    def __init__(self):
        self.states = {}  # Key: (league_id, season_id), Value: LeagueSeasonState

    def get_state(self, league_id: int, season_id: int) -> Optional[LeagueSeasonState]:
        """Get the state for a specific league-season combination"""
        return self.states.get((league_id, season_id))

    def save_state(self, state: LeagueSeasonState):
        """Save or update the state for a league-season"""
        key = (state.league_id, state.season_id)
        self.states[key] = state
        return state

    def delete_state(self, league_id: int, season_id: int):
        """Delete the state for a league-season"""
        key = (league_id, season_id)
        if key in self.states:
            del self.states[key]
        return None
