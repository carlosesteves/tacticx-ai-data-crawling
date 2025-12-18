from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class LeagueSeasonState(BaseModel):
    league_id: int
    season_id: int
    last_processed_match_date: Optional[datetime] = None
    last_processed_match_id: Optional[int] = None
    total_matches_processed: int = 0
    failed_match_ids: List[int] = []  # Track matches that failed to process
    last_updated_at: datetime
    status: str = 'in_progress'  # 'in_progress', 'completed', 'error'
