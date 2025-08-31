from models.coach_tenure import CoachTenure
from repositories.tenure.coach_tenure_base_repository import ICoachTenureRepository

class FakeCoachTenureRepository(ICoachTenureRepository):
    def __init__(self, initial_coach_tenures=None):
        self.coach_tenures = {}
        if initial_coach_tenures:
            for coach_tenure in initial_coach_tenures:
                self.coach_tenures[coach_tenure.tenure_id] = coach_tenure

    def save(self, coach_tenure: CoachTenure):
        self.coach_tenures[coach_tenure.tenure_id] = coach_tenure
        return coach_tenure  # mimic persistence result

    def fetch_all_ids(self) -> tuple[int, int]:
        return self.coach_tenures