from requests import Session
from supabase import create_client
from pages.coach_page import CoachPage
from repositories.pipeline_context import PipelineContext
from services.coach_service import CoachService
from repositories.coach.supabase_coach_repository import ICoachRepository


def run_coach_pipeline(session: Session, coach_id: int, context: PipelineContext, page: CoachPage = None):
    # Check if coach was already processed in this session (memory cache)
    if coach_id in context.coach_cache:
        print(f"⏭️  Coach {coach_id} already processed in this session")
        return
    
    # Check if coach exists in database
    coach_exists_in_db = int(coach_id) in context.coach_repo.fetch_all_ids()
    
    if(page is None):
        page = CoachPage(session=session, coach_id=coach_id)

    # Always save/update coach info (in case of updates)
    coach = CoachService.parse_general_info(page)
    context.coach_repo.save(coach)
    
    # Always update tenures (even for existing coaches)
    tenures = CoachService.parse_tenures(page)
    status = "Updated" if coach_exists_in_db else "Saved"
    print(f"✅ {status} coach {coach.name} ({coach.tm_coach_id}, with {len(tenures)} tenures)")
    
    for tenure in tenures:
        context.tenure_repo.save(tenure)
    
    # Add to cache to avoid reprocessing in the same session
    context.coach_cache.add(coach.tm_coach_id)


    