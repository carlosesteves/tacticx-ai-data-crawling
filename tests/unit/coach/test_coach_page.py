import pytest
from pages.coach_page import CoachPage
from utils.page_utils import extract_attendance_from_text, extract_coach_id, extract_team_id, extract_date_from_href

# Fixture to load the HTML file
@pytest.fixture
def coach_page_html():
    with open("tests/fixtures/coach_page_1705_test.html", encoding="utf-8") as f:
        return f.read()

@pytest.fixture
def coach_page(coach_page_html):
    return CoachPage(coach_id="1705", html_content=coach_page_html)

@pytest.fixture
def empty_coach_page(coach_page_html):
    return CoachPage(coach_id="1705", html_content="<hmtl></html>")

def test_get_coach_name(coach_page):
    coach_name = coach_page.get_coach_name()
    assert coach_name == "Steve Kean"

def test_get_coaching_license(coach_page):
    coaching_license = coach_page.get_coaching_license()
    assert coaching_license == 'UEFA Pro Licence'

def test_get_coaching_license_does_not_exist(empty_coach_page):
    coaching_license = empty_coach_page.get_coaching_license()
    assert coaching_license == ""

def test_get_dob(coach_page):
    dob = coach_page.get_dob()
    assert dob == "1967-09-30"

def test_get_citizenship_country(coach_page):
    citizenship_country = coach_page.get_citizenship_country()
    assert citizenship_country == "Scotland"
    

def test_get_preferred_formation(coach_page):
    preferred_formation = coach_page.get_preferred_formation()
    assert preferred_formation == "4-2-3-1"


def test_get_coach_tenures(coach_page):
    tenures = coach_page.get_tenures()
    assert isinstance(tenures, list)
    assert len(tenures) == 16
    assert tenures[0]['club_id'] == "253"
    assert tenures[0]['start_date'] == "2025-01-24"
    assert tenures[0]['end_date'] is None
    assert tenures[0]['role'] == "Sporting Director"
    assert tenures[1]['club_id'] == "253"
    assert tenures[1]['start_date'] == "2023-05-19"
    assert tenures[1]['end_date'] == "2025-01-23"
    assert tenures[1]['role'] == "Manager"