import logging
import requests
from pages.coach_page import CoachPage
from pages.league_page_matches import LeaguePageMatches
from pages.match_page import MatchPage
from pages.page import Page
from utils.db_utils import get_clubs_from_db, get_coaches_from_db, get_matches_from_db, insert_coach_data, insert_coach_tenure_data, insert_match_data
from utils.page_utils import extract_goals_from_score, get_points_from_score, get_soup
from utils.tm_utils import construct_tm_league_url
from supabase import Client

logger = logging.getLogger(__name__)

def extract_coach_data(coach_id: str) -> dict:
    return CoachPage(coach_id).get_coach_data()

def extract_match_data(match_id, league_id, season_id) -> list:
    return MatchPage(match_id, league_id, season_id).get_match_data()    

def extract_all_coaches_data(db_coaches: list, db_clubs: list, match_data: dict, supabase: Client, processed_coach_ids: set = None) -> tuple:
    match_data['home_coach_id']
    home_coach_id = match_data['home_coach_id']
    away_coach_id = match_data['away_coach_id']
    home_coach_data = None
    away_coach_data = None
        
    if home_coach_id not in processed_coach_ids and db_coaches.empty:
        logger.info(f"Fetching coach data for Home Coach ID: {home_coach_id}")
        home_coach_data = extract_coach_data(home_coach_id)
        insert_coach_data(supabase, home_coach_data['general_info'])
        for tenure in home_coach_data['tenures']:
            tenure['club_id'] = home_coach_id
            insert_coach_tenure_data(supabase, tenure) if tenure['club_id'] not in db_clubs['tm_club_id'].values else None
    elif int(home_coach_id) not in db_coaches['tm_coach_id'].values and (processed_coach_ids is None or int(home_coach_id) not in processed_coach_ids):
        logger.info(f"Fetching coach data for Home Coach ID: {home_coach_id}")

        print(db_coaches['tm_coach_id'].values)
        print(home_coach_id)
        print(int(home_coach_id) not in db_coaches['tm_coach_id'].values)
        home_coach_data = extract_coach_data(home_coach_id)
        insert_coach_data(supabase, home_coach_data['general_info'])
        
        for tenure in home_coach_data['tenures']:
            tenure['club_id'] = home_coach_id
            print(db_clubs['tm_club_id'].values)
                        
            if int(tenure['club_id']) not in db_clubs['tm_club_id'].values:
                insert_coach_tenure_data(supabase, tenure)                      
    else:
        logger.info(f"Home Coach ID {home_coach_id} already exists in the database. Skipping...")

    if int(away_coach_id) not in  db_coaches['tm_coach_id'].values and (processed_coach_ids is None or int(away_coach_id) not in processed_coach_ids):
        logger.info(f"Fetching coach data for Away Coach ID: {away_coach_id}")
        away_coach_data = extract_coach_data(away_coach_id)
        insert_coach_data(supabase, away_coach_data['general_info'])
        for tenure in away_coach_data['tenures']:
            tenure['club_id'] = away_coach_id
            print(db_clubs['tm_club_id'].values)
            print(int(tenure['club_id']))
            print(int(tenure['club_id']) not in db_clubs['tm_club_id'].values)
            if int(tenure['club_id']) not in db_clubs['tm_club_id'].values:
                insert_coach_tenure_data(supabase, tenure)                      
    else:
        logger.info(f"Away Coach ID {away_coach_id} already exists in the database. Skipping...")
    return home_coach_data, away_coach_data

def fech_year_match_data(supabase: Client, league_code: str, league_id: str, season_id: int):
    db_matches = get_matches_from_db(supabase, league_id, season_id) # Fetch existing matches from the database
    db_coaches = get_coaches_from_db(supabase) # Fetch existing coaches from the database    
    db_clubs = get_clubs_from_db(supabase) # Fetch existing clubs from the database
    processed_coach_ids = set()
    year_match_data = []
    
    page_league_match_ids = LeaguePageMatches(league_code, season_id).get_match_ids()
    error_match_ids = []
    for match_id in page_league_match_ids:
        if(int(match_id) in db_matches):
            logger.info(f"Match ID {match_id} already exists in the database. Skipping...")
            continue               
        try:
            print(f"--------------------")
            # Extract match data 
            match_data = extract_match_data(match_id, league_id, season_id)
            year_match_data.append(match_data)
            logger.info(f"Extracted match data for {match_id}: {match_data}")
            print(f"Extracted match data for {match_id}: {match_data}")

            # Extract coaches data
            home_coach_data = extract_coach_data(match_data['home_coach_id'])
            away_coach_data = extract_coach_data(match_data['away_coach_id'])
            processed_coach_ids.add(match_data['home_coach_id'])
            processed_coach_ids.add(match_data['away_coach_id'])
            
            print(f"--------------------")
            print(f"Home Coach Data for {match_data['home_coach_id']}: \n{home_coach_data}")
            print(f"--------------------")
            print(f"Away Coach Data for {match_data['home_coach_id']}: \n{away_coach_data}")

            # Insert data into the database
            # extract_all_coaches_data(db_coaches, db_clubs, match_data, supabase, processed_coach_ids)                                                
            # insert_match_data(supabase, match_data)                                
            print(f"--------------------")
        except Exception as e:
            print(f"Error extracting match data for {match_id}: {e}")
            error_match_ids.append(match_id)            

        logger.info("\n-------------------")
        logger.info(f"Fetching match data for {league_code} {season_id} - Match ID: {match_id}")       
    return year_match_data
