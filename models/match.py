from pydantic import BaseModel
from typing import Any, Optional
from datetime import date


class Match(BaseModel):
    tm_match_id: int
    home_club_id: int
    away_club_id: int
    season_id: int
    league_id: int
    date: date
    home_coach_id: int
    away_coach_id: int
    attendance: int
    home_team_score: int
    away_team_score: int
    home_team_points: int
    away_team_points: int
