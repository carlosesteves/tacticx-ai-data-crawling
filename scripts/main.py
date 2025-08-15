import sys
import os

# Add project root (../) to PYTHONPATH at runtime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import math
from datetime import datetime
from config.settings import CLUB_DATA_PATH
from services.supabase_service import create_supabase_client
from utils.db_utils import (
    fetch_club_data,
    fetch_league_data,
    insert_club_data,
    insert_club_season_data,
    is_club_id_in_db,
    is_season_club_in_db,
    get_league_seasons
)
from utils.logic_utils import league_data_by_league_code
from scripts.fetch_match_data import fech_year_match_data

def main():
    supabase = create_supabase_client()

    db_league_seasons = get_league_seasons(supabase, 1)  # Example league_id
    db_league_seasons=db_league_seasons.drop_duplicates()

    for row in db_league_seasons.itertuples():
        print(f"League: {row.name}, Season ID: {row.season_id}, Country: {row.country}, TM Code: {row.tm_code}")
        league_year_match_data = fech_year_match_data(supabase, row.tm_code, row.tm_league_id, row.season_id)
        print(f"Fetched {len(league_year_match_data)} matches for league {row.name} in season {row.season_id}.")
        print("--------------------------------------")

if __name__ == "__main__":
    main()
