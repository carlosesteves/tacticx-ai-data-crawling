import re
import time
from requests.exceptions import Timeout
from bs4 import BeautifulSoup
from config.constants import HEADERS
from lxml import etree


def get_soup(url, session) -> etree._Element:
    """Fetches and parses the HTML page with retry logic for timeouts."""
    retries = 3  # Maximum retries
    for attempt in range(retries):
        try:
            response = session.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()  # Handle HTTP errors
            return convert_bsoup_to_page(BeautifulSoup(response.text, 'html.parser'))
        except Timeout as e:  # Timeout error handling
            print("Timeout error fetching {}: {}. Retrying ({}/{})...".format(url, str(e), attempt + 1, retries))
            if attempt < retries - 1:
                time.sleep(1)  # Wait 1 second before retrying
            else:
                print("Failed to fetch {} after {} attempts.".format(url, retries))
                return None
        except Exception as e:  # General error handling
            print("Error fetching {}: {}. Retrying ({}/{})...".format(url, str(e), attempt + 1, retries))
            if attempt < retries - 1:
                time.sleep(1)  # Wait 1 second before retrying
            else:
                print("Failed to fetch {} after {} attempts.".format(url, retries))
                return None

def convert_bsoup_to_page(bsoup) -> etree._Element:
    """Converts BeautifulSoup object to lxml tree (Python 2.7 compatible)."""
    return etree.HTML(str(bsoup))

# extract coach id from href like "/profil/trainer/20947"
def extract_coach_id(href):
    # Extract Coach ID
    coach_id_match = re.search(r'/trainer/(\d+)', href)
    return coach_id_match.group(1) if coach_id_match else ""

def extract_team_id(href):
    # Extract Coach ID
    team_id_match = re.search(r'/verein/(\d+)/saison_id/', href)
    return team_id_match.group(1) if team_id_match else ""

# extract date from href like /aktuell/waspassiertheute/aktuell/new/datum/2025-03-07""
def extract_date_from_href(href):
    # Extract date from href
    date_match = re.search(r'/datum/(\d{4}-\d{2}-\d{2})', href)
    return date_match.group(1) if date_match else None


# Extract attendance from text like "Attendance: 35,928"
def extract_attendance_from_text(text):
    # Extract attendance from text
    attendance_match = re.search(r'Attendance:\s*([\d\.]+)', text)
    if attendance_match:
        return attendance_match.group(1).replace('.', '')
    return None

# Extract goals from score such as "2:1"
def extract_goals_from_score(score):
    # Extract goals from score
    goals_match = re.search(r'(\d+):(\d+)', score)
    if goals_match:
        return int(goals_match.group(1)), int(goals_match.group(2))
    return None, None

# Get points from match result
def get_points_from_score(score):
    # Extract points from score
    home_goals, away_goals = extract_goals_from_score(score)
    if home_goals is None or away_goals is None:
        return None, None
    if home_goals > away_goals:
        return 3, 0  # Home team wins
    elif home_goals < away_goals:
        return 0, 3  # Away team wins
    else:
        return 1, 1  # Draw
    return None, None  # Invalid score format