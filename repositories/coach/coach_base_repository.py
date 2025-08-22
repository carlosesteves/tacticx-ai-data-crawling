# repositories/base_repository.py
from abc import abstractmethod
from typing import Protocol, Any
from models.coach import Coach

class ICoachRepository(Protocol):
    def __init__(self, client):
        self.client = client
        super().__init__()

    def fetch_all_ids(self) -> set[int]:
        return {}
    
    def save(self, coach: Coach) -> Any:
        ...

    def get_coach_id_by_name(self, name: str) -> int:
        ...

    def get_coach_id_by_date(self, club_id: int, match_date) -> int:
        ...