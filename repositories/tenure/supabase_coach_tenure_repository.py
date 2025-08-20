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
            
            # 2️⃣ Check if the tenure row already exists
            existing = self.client.table("Coach_tenure") \
                .select("*") \
                .eq("coach_id", data["coach_id"]) \
                .eq("club_id", club_id) \
                .execute()
            
            # 3️⃣ Insert only if it doesn't exist
            if not existing.data:
                response = self.client.table("Coach_tenure").insert(data).execute()
                return response.data
            else:
                # Row already exists, skip insert
                return existing.data

        except Exception as e:
            raise Exception(f"Supabase save error: {e}")
        
    def fetch_all_ids(self) -> set[int]:
        response = self.client.table("coach_tenure").select("tenure_id").execute()
        return [(r["tm_coach_id"], r["team_id"], r["start_date"]) for r in response]
