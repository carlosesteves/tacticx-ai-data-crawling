from pydantic import BaseModel
from typing import Any, Optional
from datetime import date


class CoachTenure(BaseModel):
    coach_id: int
    club_id: int
    start_date: Optional[date]
    end_date: Optional[date]
    role: str
    is_current: bool
