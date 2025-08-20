from pydantic import BaseModel
from typing import Any, Optional
from datetime import date


class Coach(BaseModel):
    tm_coach_id: int
    name: str
    dob: date
    country: str
    coaching_license: str
