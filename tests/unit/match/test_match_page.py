import pytest
import requests
from pages.match_page import MatchPage, MissingCoachException, MissingResultException
from utils.page_utils import extract_attendance_from_text, extract_coach_id, extract_team_id, extract_date_from_href

# Fixture to load the HTML file
@pytest.fixture
def match_page_html():
    with open("tests/fixtures/match_page_1028917_test.html", encoding="utf-8") as f:
        return f.read()

@pytest.fixture
def match_page(match_page_html):
    return MatchPage(session=requests.session(), match_id="12345", html_content=match_page_html)

def test_get_team_home(match_page):
    home_team = match_page.get_team(home=True)
    assert home_team == "148"

def test_get_team_away(match_page):
    away_team = match_page.get_team(home=False)
    assert away_team == "281"

def test_get_match_date(match_page):
    match_date = match_page.get_match_date()
    assert match_date == "2010-08-14"

def test_get_attendance(match_page):
    attendance = match_page.get_attendance()
    assert attendance == 35928

def test_get_coaches_ids(match_page):
    coaches = match_page.get_coaches_ids()
    assert coaches == ["448", "524"]  # Expected coach IDs from fixture

def test_get_match_result(match_page):
    match_result = match_page.get_match_result()
    assert match_result == "0:0"  # Expected match result from fixture


# Tests for missing data exceptions
def test_missing_coaches_raises_exception():
    """Test that missing coaches raises MissingCoachException"""
    html_no_coaches = "<html><body><div>No coaches here</div></body></html>"
    page = MatchPage(session=requests.session(), match_id=99999, html_content=html_no_coaches)
    
    with pytest.raises(MissingCoachException) as exc_info:
        page.get_coaches_ids()
    
    assert exc_info.value.match_id == 99999
    assert "Coach information missing" in str(exc_info.value)


def test_missing_result_raises_exception():
    """Test that missing result raises MissingResultException"""
    html_no_result = "<html><body><div>No result here</div></body></html>"
    page = MatchPage(session=requests.session(), match_id=88888, html_content=html_no_result)
    
    with pytest.raises(MissingResultException) as exc_info:
        page.get_match_result()
    
    assert exc_info.value.match_id == 88888
    assert "Match result is missing" in str(exc_info.value)
