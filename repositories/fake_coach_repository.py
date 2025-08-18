from typing import List
from models.coach import Coach
from repositories.base_repository import ICoachRepository

class FakeCoachRepository(ICoachRepository):
    def __init__(self, initial_coaches=None):
        self.coaches = {}
        if initial_coaches:
            for coach in initial_coaches:
                self.coaches[coach.tm_coach_id] = coach

    def save(self, coach: Coach):
        self.coaches[coach.tm_coach_id] = coach
        return coach  # mimic persistence result

    def fetch_all_ids(self) -> set[int]:
        return self.coaches