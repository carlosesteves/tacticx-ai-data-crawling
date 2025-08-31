from supabase import create_client, Client
from models.match import Match
from repositories.match.match_base_repository import IMatchRepository

class SupabaseMatchRepository(IMatchRepository):
    def __init__(self, client: Client):
        self.client = client

    def save(self, match: Match):
        try:
            data = match.model_dump(mode="json")
            response = self.client.table("Match").upsert(data).execute()
            return response.data  # APIResponse has .data
        except Exception as e:
            raise Exception(f"Supabase save error: {e}")
        
    def fetch_all_ids(self) -> set[int]:
        response = self.client.table("Match").select("tm_match_id").execute()
        return {row["tm_match_id"] for row in response.data}
    
    def fetch_ids_by_year_league(self, season_id: int, league_id: int) -> set[int]:
        response = self.client.table("Match").select("tm_match_id").eq("season_id", season_id).eq("league_id", league_id).execute()
        return {row["tm_match_id"] for row in response.data}
    

    # def does_match_exist(self, match_id: int) -> bool:
    #     response = self.client.table("Match").select("tm_match_id").eq("tm_match_id", match_id).execute()
    #     return response.data is None