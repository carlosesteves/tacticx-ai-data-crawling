from supabase import create_client, Client
from models.coach import Coach
from repositories.base_repository import ICoachRepository
import os

class SupabaseCoachRepository(ICoachRepository):
    def __init__(self, client):
        self.client = client

    def save(self, coach: Coach):
        try:
            data = coach.model_dump(mode="json")
            response = self.client.table("Coach").upsert(data).execute()
            return response.data  # APIResponse has .data
        except Exception as e:
            raise Exception(f"Supabase save error: {e}")
        
    def fetch_all_ids(self) -> set[int]:
        response = self.client.table("Coach").select("tm_coach_id").execute()
        return {row["tm_coach_id"] for row in response.data}