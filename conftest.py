# conftest.py
import sys, os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import pytest
from repositories.fake_coach_repository import FakeCoachRepository
from repositories.coach_repository import SupabaseCoachRepository

@pytest.fixture
def coach_repo(request):
    """
    Dynamically select which repository to use in tests.
    Default: Fake repository.
    """
    repo_type = request.config.getoption("--repo")
    if repo_type == "supabase":
        return SupabaseCoachRepository()
    return FakeCoachRepository()

def pytest_addoption(parser):
    parser.addoption(
        "--repo",
        action="store",
        default="fake",
        help="Choose repository: fake | supabase"
    )

