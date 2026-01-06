from pages.match_page import MatchPage, MissingCoachException, MissingResultException
from models.match import Match

class MatchService:
    @staticmethod
    def parse(league_id: int, season_id: int, page: MatchPage) -> Match:
        # Validate that we have all required data before creating Match
        try:
            match_result = page.get_match_result()
        except MissingResultException as e:
            print(f"❌ Error: {e.message}")
            raise
        
        try:
            coaches_ids = page.get_coaches_ids()
            home_coach_id = coaches_ids[0]
            away_coach_id = coaches_ids[1]
        except MissingCoachException as e:
            print(f"❌ Error: {e.message}")
            raise
        
        # Create match object with validated data
        match = Match(
            tm_match_id=page.match_id,
            home_club_id=page.get_team(True),
            away_club_id=page.get_team(False),
            season_id=season_id,
            league_id=league_id,       
            home_coach_id=home_coach_id,
            away_coach_id=away_coach_id,     
            date=page.get_match_date(),
            attendance=page.get_attendance(),
            home_team_score=page.get_home_team_score(),
            away_team_score=page.get_away_team_score(),
            home_team_points=page.get_home_team_points(),
            away_team_points=page.get_away_team_points()            
        )
        return match

