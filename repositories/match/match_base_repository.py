# repositories/base_repository.py
from abc import abstractmethod
from typing import Protocol, Any
from models.match import Match

class IMatchRepository(Protocol):
    def __init__(self, client):
        self.client = client
        super().__init__()

    def fetch_all_ids(self) -> set[int]:
        return {}
    
    def save(self, match: Match) -> Any:
        ...

        