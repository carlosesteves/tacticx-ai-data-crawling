from datetime import date
from supabase import create_client, Client
from models.coach import Coach
from repositories.coach.coach_base_repository import ICoachRepository
import os

class SupabaseCoachRepository(ICoachRepository):
    def __init__(self, client: Client):
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
    
    def get_coach_id_by_name(self, name: str) -> int:
        response =  self.client.table("Coach").select("tm_coach_id").eq('name', name).execute()
        return response.data
    
    def get_coach_id_by_date(self, club_id: int, match_date: date) -> int:
        response = self.client.table("Coach_tenure") \
            .select("coach_id") \
            .eq("club_id", club_id) \
            .lte("start_date", match_date) \
            .or_(f"end_date.gte.{match_date},end_date.is.null") \
            .limit(1).execute()
        return response.data