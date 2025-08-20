from pages.league_page_matches import LeaguePageMatches
from pages.match_page import MatchPage
from pipelines.match_pipeline import run_match_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure import supabase_coach_tenure_repository
from services.supabase_service import create_supabase_client


def run_season_pipeline(league_id: int, league_code: str, season_id: int) -> list:
    err_match_ids = []
    client = create_supabase_client()
    context = PipelineContext(
        coach_repo=SupabaseCoachRepository(client=client),
        match_repo=SupabaseMatchRepository(client=client),
        tenure_repo=SupabaseCoachTenureRepository(client=client),
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )
    
    for match_id in LeaguePageMatches(league_code=league_code, season_id=season_id).get_match_ids():
        try:            
            run_match_pipeline(
                match_id=match_id,
                league_id=league_id,
                season_id=season_id,
                context=context
            )
        except Exception:
            err_match_ids.append(match_id)
            print(f"Error processing match {match_id}")

    return err_match_ids