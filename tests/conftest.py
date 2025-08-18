import pytest
from repositories.fake_coach_repository import FakeCoachRepository
from models.coach import Coach


@pytest.fixture
def fake_repo():
    # preload with a coach id=1
    existing_coach = Coach(tm_coach_id=1, name="Existing Coach", dob="1970-01-01", country="PT", coaching_license="UEFA")
    return FakeCoachRepository([existing_coach])


@pytest.fixture
def new_coach():
    return Coach(tm_coach_id=2, name="New Coach", dob="1980-01-01", country="ES", coaching_license="UEFA")
