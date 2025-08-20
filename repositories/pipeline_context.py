from dataclasses import dataclass

from repositories.coach.coach_base_repository import ICoachRepository
from repositories.match.match_base_repository import IMatchRepository
from repositories.tenure.coach_tenure_base_repository import ICoachTenureRepository

@dataclass
class PipelineContext:
    coach_repo: ICoachRepository
    match_repo: IMatchRepository
    tenure_repo: ICoachTenureRepository

    coach_cache: set[int]
    match_cache: set[int]
    tenure_cache: list[tuple[int, int]]