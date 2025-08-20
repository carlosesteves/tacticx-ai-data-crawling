from pages.match_page import MatchPage
from pipelines.coach_pipeline import run_coach_pipeline
from repositories.match.match_base_repository import IMatchRepository
from repositories.pipeline_context import PipelineContext
from services.match_service import MatchService

def run_match_pipeline(match_id: int, league_id: int, season_id: int, context: PipelineContext, page: MatchPage = None):
    if match_id in context.match_cache:
        print(f"Skipping match {match_id}")
        return

    if(page is None):
        page = MatchPage(match_id=match_id)
    
    # get match data
    match = MatchService.parse(league_id, season_id, page)
    
    print(f"-----------------------")
    # handle coaches inside the same pipeline
    run_coach_pipeline(match.home_coach_id, context)
    run_coach_pipeline(match.away_coach_id, context)

    # save match
    context.match_repo.save(match)
    context.match_cache.add(match.tm_match_id)
    print(f"✅ Saved match {match.tm_match_id}")
    print(f"-----------------------")
