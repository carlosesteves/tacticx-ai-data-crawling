from requests import Session
from pages.match_page import MatchPage, MissingCoachException, MissingResultException
from pipelines.coach_pipeline import run_coach_pipeline
from repositories.match.match_base_repository import IMatchRepository
from repositories.pipeline_context import PipelineContext
from services.match_service import MatchService

def run_match_pipeline(session: Session, match_id: int, league_id: int, season_id: int, context: PipelineContext, page: MatchPage = None):
    db_match_ids = context.match_repo.fetch_all_ids()
    if int(match_id) in context.match_cache or int(match_id) in db_match_ids:
        print(f"⏭️  Skipping match={match_id}")
        return 

    if(page is None):
        page = MatchPage(match_id=match_id, session=session)
    
    # Parse match data - will raise exceptions if coach info or result is missing
    try:
        match = MatchService.parse(league_id, season_id, page)
    except MissingCoachException as e:
        print(f"❌ Match {match_id}: {e.message}")
        raise  # Re-raise to mark this match as failed
    except MissingResultException as e:
        print(f"❌ Match {match_id}: {e.message}")
        raise  # Re-raise to mark this match as failed
    
    print(f"-----------------------")
    # handle coaches inside the same pipeline
    run_coach_pipeline(session=session, coach_id=match.home_coach_id, context=context)
    run_coach_pipeline(session=session, coach_id=match.away_coach_id, context=context)

    # save match
    context.match_repo.save(match)
    context.match_cache.add(match.tm_match_id)
    print(f"✅ Saved match {match.tm_match_id}")
    print(f"-----------------------")
