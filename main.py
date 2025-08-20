import sys
import os

from supabase import create_client

from pages.coach_page import CoachPage
from pipelines.coach_pipeline import run_coach_pipeline
from pipelines.season_pipeline import run_season_pipeline
from utils.db_utils import get_league_seasons
from utils.file_utils import write_csv


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
    ERR_FILE_PATH = 'csv/err_match_ids.csv'
    client = create_supabase_client()

    db_league_seasons = get_league_seasons(create_supabase_client(), 1)  # Example league_id
    db_league_seasons=db_league_seasons.drop_duplicates()

    for row in db_league_seasons.itertuples():
        print(f"\n----------------------")
        print(f"League: {row.name}, Season ID: {row.season_id}, Country: {row.country}, TM Code: {row.tm_code}")
        err_match_ids = run_season_pipeline(league_id=row.tm_league_id, league_code=row.tm_code, season_id=row.season_id)        
        write_csv(err_match_ids, ERR_FILE_PATH)            

if __name__ == "__main__":
    main()
