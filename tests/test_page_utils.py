import pytest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup
from lxml import etree
import requests
from requests.exceptions import Timeout

from utils.page_utils import (
    convert_to_yyyy_mm_dd,
    get_soup,
    convert_bsoup_to_page,
    extract_coach_id,
    extract_team_id,
    extract_date_from_href,
    extract_attendance_from_text,
    extract_goals_from_score,
    get_points_from_score
)

# -------------------------
# get_soup tests
# -------------------------
def test_get_soup_success():
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html><body><h1>OK</h1></body></html>"
    mock_session.get.return_value = mock_response

    soup = get_soup("http://example.com", mock_session)
    assert isinstance(soup, etree._Element)
    assert b"OK" in etree.tostring(soup)

def test_get_soup_timeout_then_success():
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html><body>Retry Works</body></html>"
    mock_session.get.side_effect = [
        Timeout("Timeout 1"),
        mock_response
    ]

    soup = get_soup("http://example.com", mock_session)
    assert b"Retry Works" in etree.tostring(soup)

def test_get_soup_failure_all_retries():
    mock_session = Mock()
    mock_session.get.side_effect = Timeout("Always Timeout")
    soup = get_soup("http://example.com", mock_session)
    assert soup is None

def test_get_soup_http_error():
    mock_session = Mock()
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404")
    mock_session.get.return_value = mock_response
    result = get_soup("http://example.com", mock_session)
    assert result is None

# -------------------------
# convert_bsoup_to_page
# -------------------------
def test_convert_bsoup_to_page():
    bs = BeautifulSoup("<html><body>Test</body></html>", "html.parser")
    page = convert_bsoup_to_page(bs)
    assert isinstance(page, etree._Element)
    assert b"Test" in etree.tostring(page)

# -------------------------
# extract_coach_id
# -------------------------
@pytest.mark.parametrize("href,expected", [
    ("/profil/trainer/20947", "20947"),
    ("/trainer/12345", "12345"),
    ("/no/trainer/id", "")
])
def test_extract_coach_id(href, expected):
    assert extract_coach_id(href) == expected

# -------------------------
# extract_team_id
# -------------------------
@pytest.mark.parametrize("href,expected", [
    ("/verein/123/saison_id/2024", "123"),
    ("/verein/999/saison_id/2020", "999"),
    ("/no/team/id", "")
])
def test_extract_team_id(href, expected):
    assert extract_team_id(href) == expected

# -------------------------
# extract_date_from_href
# -------------------------
@pytest.mark.parametrize("href,expected", [
    ("/datum/2025-03-07", "2025-03-07"),
    ("/something/else", None)
])
def test_extract_date_from_href(href, expected):
    assert extract_date_from_href(href) == expected

# -------------------------
# extract_attendance_from_text
# -------------------------
@pytest.mark.parametrize("text,expected", [
    ("Attendance: 35.928", "35928"),
    ("Attendance: 1.000", "1000"),
    ("No attendance info", None)
])
def test_extract_attendance_from_text(text, expected):
    assert extract_attendance_from_text(text) == expected

def test_extract_attendance_from_text_with_commas():
    text = "Attendance: 35,928"
    assert extract_attendance_from_text(text) == "35928"

def test_extract_attendance_from_text_with_dots():
    text = "Attendance: 12.345"
    assert extract_attendance_from_text(text) == "12345"

def test_extract_attendance_from_text_no_attendance():
    text = "No attendance info here"
    assert extract_attendance_from_text(text) is None

def test_extract_attendance_from_text_empty_string():
    text = ""
    assert extract_attendance_from_text(text) is None

def test_extract_attendance_from_text_multiple_numbers():
    text = "Attendance: 5.432 and something else 123"
    assert extract_attendance_from_text(text) == "5432"

# -------------------------
# extract_goals_from_score
# -------------------------
@pytest.mark.parametrize("score,expected", [
    ("2:1", (2, 1)),
    ("0:0", (0, 0)),
    ("abc", (None, None))
])
def test_extract_goals_from_score(score, expected):
    assert extract_goals_from_score(score) == expected

# -------------------------
# get_points_from_score
# -------------------------
@pytest.mark.parametrize("score,expected", [
    ("2:1", (3, 0)),   # Home win
    ("0:1", (0, 3)),   # Away win
    ("1:1", (1, 1)),   # Draw
    ("bad", (None, None))  # Invalid
])
def test_get_points_from_score(score, expected):
    assert get_points_from_score(score) == expected

# -------------------------
# convert_to_yyyy_mm_dd
# -------------------------
def test_convert_with_parentheses():
    assert convert_to_yyyy_mm_dd("Sep 30, 1967 (57)") == "1967-09-30"
    assert convert_to_yyyy_mm_dd("Jan 1, 2000 (23)") == "2000-01-01"
    assert convert_to_yyyy_mm_dd("Mar 2, 1947 (78)") == "1947-03-02"
    assert convert_to_yyyy_mm_dd("02.03.1947 (78)") == "1947-03-02"
    

def test_convert_without_parentheses():
    assert convert_to_yyyy_mm_dd("Feb 28, 1995") == "1995-02-28"
    assert convert_to_yyyy_mm_dd("Dec 31, 2020") == "2020-12-31"

def test_convert_with_prefix_and_parentheses():
    assert convert_to_yyyy_mm_dd("22/23 (May 19, 2023)") == "2023-05-19"
    assert convert_to_yyyy_mm_dd("Season 21/22 (June 1, 2020)") == "2020-06-01"

def test_invalid_date_format():
    assert convert_to_yyyy_mm_dd("") is None
    assert convert_to_yyyy_mm_dd(None) is None
    assert convert_to_yyyy_mm_dd("No date here") is None


