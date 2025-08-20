from pages.match_page import MatchPage
from models.match import Match

class MatchService:
    @staticmethod
    def parse(league_id: int, season_id: int, page: MatchPage) -> Match:         
        match = Match(
            tm_match_id=page.match_id,
            home_club_id=page.get_team(True),
            away_club_id=page.get_team(False),
            season_id=season_id,
            league_id=league_id,       
            home_coach_id=page.get_home_coach_id(),
            away_coach_id=page.get_away_coach_id(),     
            date=page.get_match_date(),
            attendance=page.get_attendance(),
            home_team_score=page.get_home_team_score(),
            away_team_score=page.get_away_team_score(),
            home_team_points=page.get_home_team_points(),
            away_team_points=page.get_away_team_points()            
        )
        return match

