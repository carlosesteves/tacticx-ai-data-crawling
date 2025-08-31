from typing import List
from models.match import Match
from repositories.match.match_base_repository import IMatchRepository

class FakeMatchRepository(IMatchRepository):
    def __init__(self, initial_matches=None):
        self.matches = {}
        if initial_matches:
            for match in initial_matches:
                self.matches[match.match_id] = match

    def save(self, match: Match):
        self.matches[match.tm_match_id] = match
        return match  # mimic persistence result

    def fetch_ids_by_year_league(self, season_id: int, league_id: int) -> set[int]:
        return set()

    def fetch_all_ids(self) -> set[int]:
        return self.matches