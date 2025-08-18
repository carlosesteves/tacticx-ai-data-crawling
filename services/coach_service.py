from models.coach import Coach
from pages.coach_page import CoachPage


class CoachService:
    @staticmethod
    def parse(page: CoachPage) -> Coach:
        return Coach(
            tm_coach_id=page.coach_id,
            name=page.get_coach_name(),
            dob=page.get_dob(),
            country=page.get_citizenship_country(),
            coaching_license=page.get_coaching_license()
            )