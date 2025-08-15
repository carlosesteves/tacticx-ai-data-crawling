import logging
import requests
from pages.league_page_matches import LeaguePageMatches
from pages.match_page import MatchPage
from pages.page import Page
from utils.db_utils import get_coaches_from_db, get_matches_from_db, insert_match_data
from utils.page_utils import extract_goals_from_score, get_points_from_score, get_soup
from utils.tm_utils import construct_tm_league_url
from supabase import Client

logger = logging.getLogger(__name__)

def extract_match_data(match_id, league_id, season_id) -> list:
    match_page = MatchPage(match_id)
    match_score = match_page.get_match_result()
    return {                        
            "tm_match_id": match_id,
            "home_club_id": match_page.get_team(home=True),
            "away_club_id": match_page.get_team(home=False),
            "season_id": season_id,
            "league_id": league_id,
            "date": match_page.get_match_date(),
            "home_coach_id": match_page.get_coaches_ids()[0],
            "away_coach_id": match_page.get_coaches_ids()[1],
            "attendance": match_page.get_attendance(),
            "home_team_score": extract_goals_from_score(match_score)[0],
            "away_team_score": extract_goals_from_score(match_score)[1],            
            "home_team_points": get_points_from_score(match_score)[0],
            "away_team_points": get_points_from_score(match_score)[1],
        }

def fech_year_match_data(supabase: Client, league_code: str, league_id: str, season_id: int):
    db_matches = get_matches_from_db(supabase, league_id, season_id) # Fetch existing matches from the database
    db_coaches = get_coaches_from_db(supabase) # Fetch existing coaches from the database    

    year_match_data = []
    match_ids = LeaguePageMatches(league_code, season_id).get_match_ids()

    error_match_ids = []
    for match_id in match_ids:
        if(match_id in db_matches):
            logger.info(f"Match ID {match_id} already exists in the database. Skipping...")
            continue               
        try:
            match_data = extract_match_data(match_id, league_id, season_id)
            logger.info(f"Extracted match data for {match_id}: {match_data}")
            print(f"Extracted match data for {match_id}: {match_data}")
            # insert_match_data(supabase, match_data)
            year_match_data.append(match_data)
        except Exception as e:
            print(f"Error extracting match data for {match_id}: {e}")
            error_match_ids.append(match_id)            
            continue
        logger.info("\n-------------------")
        logger.info(f"Fetching match data for {league_code} {season_id} - Match ID: {match_id}")       
    return year_match_data