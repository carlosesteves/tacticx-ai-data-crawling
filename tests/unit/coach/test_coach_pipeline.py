import pytest
import requests
from pages.coach_page import CoachPage
from repositories.coach.fake_coach_repository import FakeCoachRepository
from pipelines.coach_pipeline import run_coach_pipeline
from models.coach import Coach

@pytest.fixture
def coach_page_html():
    with open("tests/fixtures/coach_page_1705_test.html", encoding="utf-8") as f:
        return f.read()

@pytest.fixture
def coach_page(coach_page_html):
    return CoachPage(coach_id="1705", html_content=coach_page_html)

# def test_pipeline_skips_existing(coach_page, fake_repo):    
#     cache = fake_repo.fetch_all_ids()
#     fake_repo.save(Coach(tm_coach_id=1705, name="Existing Coach", dob="1970-01-01", country="PT", coaching_license="UEFA"))
#     # Use the coach_id from the fixture
#     print(cache)
#     result = run_coach_pipeline(session=requests.session(), coach_id=1705, fake_repo, cache, coach_page)
#     # Should skip because ID is already in cache
#     assert result is None


# def test_pipeline_inserts_new(coach_page, fake_repo):
#     cache = fake_repo.fetch_all_ids()
#     new_id = 9999  # ID not in fake repo
#     # Fake a page with a new ID
#     # coach_page.coach_id = str(new_id)
    
#     # result = run_coach_pipeline(coach_id=int(coach_page.get_coach_id()), fake_repo, cache, coach_page)
    
#     # assert new_id in cache
#     # assert new_id in fake_repo.fetch_all_ids()
#     # assert result.tm_coach_id == new_id
