import sys
import os

import requests
from supabase import create_client

from models.match import Match
from pages.coach_page import CoachPage
from pipelines.coach_pipeline import run_coach_pipeline
from pipelines.season_pipeline import run_season_matches, run_season_pipeline
from utils.db_utils import fetch_league_data, get_league_id_by_code, get_league_seasons
from utils.file_utils import write_csv
from utils.page_utils import get_points_from_score


# Add project root (../) to PYTHONPATH at runtime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import math
from datetime import datetime
from config.settings import CLUB_DATA_PATH
from services.supabase_service import create_supabase_client

from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from config.settings import SUPABASE_URL, SUPABASE_KEY

def main():
    # Loop through all Leagues
    session = requests.session()
    for league in fetch_league_data(create_supabase_client()).itertuples():
        db_league_seasons = get_league_seasons(create_supabase_client(), league.tm_league_id) 
        db_league_seasons=db_league_seasons.drop_duplicates()

        # Loop through all seasons 
        for row in db_league_seasons.itertuples():
            print(f"\n----------------------")
            print(f"League: {row.name}, Season ID: {row.season_id}, Country: {row.country}, TM Code: {row.tm_code}")
            run_season_pipeline(league_id=row.tm_league_id, league_code=row.tm_code, season_id=row.season_id, session=session)        

# def process_all_league_results():
#     # Loop through all Leagues
#     session = requests.session()
#     for league in fetch_league_data(create_supabase_client()).itertuples():
#         db_league_seasons = get_league_seasons(create_supabase_client(), league.tm_league_id) 
#         db_league_seasons=db_league_seasons.drop_duplicates()

#         # Loop through all seasons 
#         for row in db_league_seasons.itertuples():
#             print(f"\n----------------------")
#             print(f"League: {row.name}, Season ID: {row.season_id}, Country: {row.country}, TM Code: {row.tm_code}")
#             matches = run_season_matches(league_id=row.tm_league_id, league_code=row.tm_code, season_id=row.season_id, session=session)        
#             print(matches)


# def process_matches_from_file():
#     supabase_client = create_supabase_client()
#     coach_repo = SupabaseCoachRepository(supabase_client)
#     # coach_page = CoachPage(session=requests.session(), )
#     df = pd.read_csv('data/games.csv')
#     for row in df.itertuples():
#         tm_match_id = row.game_id
#         home_club_id=int(row.home_club_id)
#         away_club_id=int(row.away_club_id)
#         tm_league_id = get_league_id_by_code(supabase_client, row.competition_id)
#         season_id = row.season
#         date=row.date
#         home_coach_id=coach_repo.get_coach_id_by_name(row.home_club_manager_name)
#         away_coach_id=coach_repo.get_coach_id_by_name(row.away_club_manager_name)
#         attendance=int(row.attendance)
#         home_team_score=int(row.away_club_goals)
#         away_team_score=int(row.away_club_goals)
#         home_team_points=get_points_from_score(f"{row.home_club_goals}:{row.away_club_goals}")[0]
#         away_team_points=get_points_from_score(f"{row.home_club_goals}:{row.away_club_goals}")[1]
#         is_domestic = True if row.competition_type == 'domestic_league' else False

#         # home_coach_id = 

#         if(is_domestic):
#             match = Match(
#                 tm_match_id=tm_match_id,
#                 home_club_id=home_club_id,
#                 away_club_id=away_club_id,
#                 season_id=season_id,
#                 league_id=tm_league_id,
#                 date=date,
#                 home_coach_id=home_coach_id,
#                 away_coach_id=away_coach_id,
#                 attendance=attendance,
#                 home_team_score=home_team_score,
#                 away_team_score=away_team_score,
#                 home_team_points=home_team_points,
#                 away_team_points=away_team_points
#             )

#             print(match)

    # for row in file:
    ##    context.match_repo.save(Match(

    ##            ))
    ##    context.match_cache.add(match.tm_match_id)


    return


if __name__ == "__main__":
    main()
    # process_all_league_results()
    # process_matches_from_file()
