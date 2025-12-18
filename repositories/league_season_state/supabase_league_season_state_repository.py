from supabase import Client
from typing import Optional
from models.league_season_state import LeagueSeasonState
from repositories.league_season_state.league_season_state_base_repository import ILeagueSeasonStateRepository
from datetime import datetime


class SupabaseLeagueSeasonStateRepository(ILeagueSeasonStateRepository):
    def __init__(self, client: Client):
        self.client = client

    def get_state(self, league_id: int, season_id: int) -> Optional[LeagueSeasonState]:
        """Get the state for a specific league-season combination"""
        try:
            response = self.client.table("league_season_state") \
                .select("*") \
                .eq("league_id", league_id) \
                .eq("season_id", season_id) \
                .execute()
            
            if response.data and len(response.data) > 0:
                data = response.data[0]
                # Convert string datetime back to datetime objects
                if data.get('last_processed_match_date'):
                    data['last_processed_match_date'] = datetime.fromisoformat(data['last_processed_match_date'])
                if data.get('last_updated_at'):
                    data['last_updated_at'] = datetime.fromisoformat(data['last_updated_at'])
                return LeagueSeasonState(**data)
            return None
        except Exception as e:
            print(f"Error fetching state for league_id={league_id}, season_id={season_id}: {e}")
            return None

    def save_state(self, state: LeagueSeasonState):
        """Save or update the state for a league-season"""
        try:
            data = state.model_dump(mode="json")
            
            # Upsert based on league_id and season_id
            response = self.client.table("league_season_state") \
                .upsert(data, on_conflict="league_id,season_id") \
                .execute()
            
            return response.data
        except Exception as e:
            raise Exception(f"Supabase save state error: {e}")

    def delete_state(self, league_id: int, season_id: int):
        """Delete the state for a league-season"""
        try:
            response = self.client.table("league_season_state") \
                .delete() \
                .eq("league_id", league_id) \
                .eq("season_id", season_id) \
                .execute()
            return response.data
        except Exception as e:
            raise Exception(f"Supabase delete state error: {e}")
