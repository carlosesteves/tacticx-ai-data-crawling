import pytest
from unittest.mock import patch

from utils.page_utils import extract_attendance_from_text

@pytest.fixture
def test_extract_attendance_from_text_with_commas():
    text = "Attendance: 35,928"
    assert extract_attendance_from_text(text) == "35,928"

def test_extract_attendance_from_text_with_dots():
    text = "Attendance: 12.345"
    assert extract_attendance_from_text(text) == "12345"

def test_extract_attendance_from_text_with_spaces():
    text = "Attendance: 1 234"
    assert extract_attendance_from_text(text) is None

def test_extract_attendance_from_text_no_attendance():
    text = "No attendance info here"
    assert extract_attendance_from_text(text) is None

def test_extract_attendance_from_text_empty_string():
    text = ""
    assert extract_attendance_from_text(text) is None

def test_extract_attendance_from_text_multiple_numbers():
    text = "Attendance: 5.432 and something else 123"
    assert extract_attendance_from_text(text) == "5432"