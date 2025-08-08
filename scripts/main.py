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
    is_season_club_in_db
)
from utils.logic_utils import league_data_by_league_code

def main():
    supabase = create_supabase_client()

    db_league_data = fetch_league_data(supabase)
    db_club_data = fetch_club_data(supabase)
    club_data = pd.read_csv(CLUB_DATA_PATH)

    error_log = []

    for row in club_data.itertuples():
        try:
            league_data_code = league_data_by_league_code(row.league_id, db_league_data)
            club_name = row.club_name
            club_id = int(row.club_id)
            club_country = league_data_code["country"].values[0]

            data = {
                "name": club_name,
                "tm_club_id": club_id,
                "country": club_country,
            }

            valuation = 0 if math.isnan(row.valuation) else float(row.valuation)

            data_club_season = {
                "league_id": int(league_data_code["tm_league_id"].values[0]),
                "club_id": club_id,
                "season_id": row.season_id,
                "valuation": valuation,
            }

            if club_id in db_club_data["tm_club_id"].values:
                print(f"Club {club_name} already exists in the database.")
            else:
                if is_club_id_in_db(club_id, supabase):
                    print(f"Club {club_name} with ID {club_id} already exists.")
                    continue

                print(f"Inserting club {club_name}...")
                club_inserted_data = pd.DataFrame(insert_club_data(supabase, data))
                db_club_data = pd.concat([db_club_data, club_inserted_data])
                print(f"Inserted club {club_name}.")

            if is_season_club_in_db(supabase, data_club_season["league_id"], data_club_season["club_id"], data_club_season["season_id"]):
                print(f"Club season already exists: {data_club_season}")
            else:
                print(f"Inserting club season data {data_club_season}...")
                insert_club_season_data(supabase, data_club_season)
                print(f"Inserted club season data.")

            print("--------------------------------------")
        except Exception as e:
            print(f"Error processing club {row.club_name} with ID {row.club_id}: {e}")
            error_log.append(row._asdict())
            continue

    if error_log:
        error_log_df = pd.DataFrame(error_log)
        error_log_df.to_csv("error_log.csv", index=False)
        print(f"Error log saved with {len(error_log)} errors.")

if __name__ == "__main__":
    main()
