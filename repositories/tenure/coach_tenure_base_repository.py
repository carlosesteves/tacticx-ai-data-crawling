# repositories/base_repository.py
from abc import abstractmethod
from typing import Protocol, Any
from models.coach_tenure import CoachTenure

class ICoachTenureRepository(Protocol):
    def __init__(self, client):
        self.client = client
        super().__init__()

    def fetch_all_ids(self) -> tuple[int, int]:
        return {}
    
    def save(self, coach: CoachTenure) -> Any:
        ...