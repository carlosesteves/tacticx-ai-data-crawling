from supabase import create_client
from pages.coach_page import CoachPage
from repositories.pipeline_context import PipelineContext
from services.coach_service import CoachService
from repositories.coach.supabase_coach_repository import ICoachRepository


def run_coach_pipeline(coach_id: int, context: PipelineContext, page: CoachPage = None):
    if coach_id in context.coach_cache or int(coach_id) in context.coach_repo.fetch_all_ids():
        print(f"⏭️ Skipping coach {coach_id}")
        return

    if(page is None):
        page = CoachPage(coach_id)

    coach = CoachService.parse_general_info(page)
    context.coach_repo.save(coach)
    context.coach_cache.add(coach.tm_coach_id)
    
    print(f"✅ Saved coach {coach.name} ({coach.tm_coach_id}, with {len(CoachService.parse_tenures(page))} tenures)")
    for tenure in CoachService.parse_tenures(page):        
        if (tenure.coach_id, tenure.club_id) not in context.tenure_cache:            
            context.tenure_repo.save(tenure)
            context.tenure_cache.add((tenure.coach_id, tenure.club_id))

    
    