from dataclasses import dataclass
from datetime import date

from repositories.coach.coach_base_repository import ICoachRepository
from repositories.match.match_base_repository import IMatchRepository
from repositories.tenure.coach_tenure_base_repository import ICoachTenureRepository
from repositories.league_season_state.league_season_state_base_repository import ILeagueSeasonStateRepository

@dataclass
class PipelineContext:
    coach_repo: ICoachRepository
    match_repo: IMatchRepository
    tenure_repo: ICoachTenureRepository
    state_repo: ILeagueSeasonStateRepository

    coach_cache: set[int]
    match_cache: set[int]
    tenure_cache: list[tuple[int, int, date]]