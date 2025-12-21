from supabase import Client
from models.coach_tenure import CoachTenure
from repositories.tenure.coach_tenure_base_repository import ICoachTenureRepository

class SupabaseCoachTenureRepository(ICoachTenureRepository):
    def __init__(self, client: Client):
        self.client = client

    def save(self, tenure: CoachTenure):
        try:
            data = tenure.model_dump(mode="json")
            club_id = data["club_id"]
            
            # 1️⃣ Check if the referenced club exists
            club_exists = self.client.table("Club") \
                .select("*") \
                .eq("tm_club_id", club_id) \
                .execute()
            
            if not club_exists.data:
                # Club does not exist, skip insert
                print(f"Club with tm_club_id={club_id} does not exist, skipping insert.")
                return None
            
            # 2️⃣ Use upsert to handle the unique constraint on (coach_id, club_id, start_date)
            # This will insert if not exists, or update if exists
            response = self.client.table("Coach_tenure") \
                .upsert(data, on_conflict="coach_id,club_id,start_date") \
                .execute()
            
            return response.data

        except Exception as e:
            raise Exception(f"Supabase save error: {e}")
        
    def fetch_all_ids(self) -> set[int]:
        response = self.client.table("coach_tenure").select("tenure_id").execute()
        return [(r["tm_coach_id"], r["team_id"], r["start_date"]) for r in response]
