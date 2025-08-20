from models.coach import Coach
from models.coach_tenure import CoachTenure
from pages.coach_page import CoachPage


class CoachService:
    @staticmethod
    def parse_general_info(page: CoachPage) -> Coach:
        return Coach(
            tm_coach_id=page.coach_id,
            name=page.get_coach_name(),
            dob=page.get_dob(),
            country=page.get_citizenship_country(),
            coaching_license=page.get_coaching_license()
            )
    
    def parse_tenures(page: CoachPage) -> list[CoachTenure]:
        list_of_tenures = []
        for tenure in page.get_tenures():
            list_of_tenures.append(
                CoachTenure(
                    coach_id=page.get_coach_id(),
                    club_id=tenure['club_id'],
                    start_date=tenure['start_date'],
                    end_date=tenure['end_date'],
                    role=tenure['role'],
                    is_current=bool(tenure['is_current'])
                )
            )
        return list_of_tenures