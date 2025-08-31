import sys
import os
import requests
from pipelines.season_pipeline import run_season_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from utils.db_utils import fetch_league_data, get_league_seasons
from services.supabase_service import create_supabase_client

# Add project root (../) to PYTHONPATH at runtime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def create_context() -> PipelineContext:
    client = create_supabase_client()
    return PipelineContext(
        coach_repo=SupabaseCoachRepository(client=client),
        match_repo=SupabaseMatchRepository(client=client),
        tenure_repo=SupabaseCoachTenureRepository(client=client),
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )

def main():
    # Loop through all Leagues
    session = requests.session()
    for league in fetch_league_data(create_supabase_client()).itertuples():
        db_league_seasons = get_league_seasons(create_supabase_client(), league.tm_league_id) 
        db_league_seasons=db_league_seasons.drop_duplicates()

        # Loop through all seasons 
        for row in db_league_seasons.itertuples():
            # america 172
            # 2nd tier 54
            # 1st tier europe 21
            # Asia 222
            if(row.tm_league_id>226):
                print(f"\n----------------------")
                print(f"League: {row.name}, Season ID: {row.season_id}, Country: {row.country}, TM Code: {row.tm_code}")
                run_season_pipeline(league_id=row.tm_league_id, league_code=row.tm_code, season_id=row.season_id, session=session, context=create_context())        

if __name__ == "__main__":
    main()