from typing import List
from models.match import Match
from repositories.match.fake_match_repository import IMatchRepository

class FakeMatch(IMatchRepository):
    def __init__(self, initial_matches=None):
        self.matches = {}
        if initial_matches:
            for match in initial_matches:
                self.matches[match.match_id] = match

    def save(self, match: Match):
        self.matches[match.tm_match_id] = match
        return match  # mimic persistence result

    def fetch_all_ids(self) -> set[int]:
        return self.matches