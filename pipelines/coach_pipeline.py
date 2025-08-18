from supabase import create_client
from pages.coach_page import CoachPage
from services.coach_service import CoachService
from repositories.coach_repository import ICoachRepository


def run_coach_pipeline(coach_id: int, repo: ICoachRepository, cache: set[int], page: CoachPage):
    url = "https://your-project.supabase.co"
    key = "your-anon-or-service-role-key"
    client = create_client(url, key)

    if coach_id in cache:
        print(f"Skipping coach {coach_id}, already in DB")
        return None

    coach = CoachService.parse(page)
    result = repo.save(coach)
    cache[coach.tm_coach_id] = coach  # update cache after successful insert

    print(f"✅ Saved coach {coach.name} with ID={coach.tm_coach_id}")
    return result