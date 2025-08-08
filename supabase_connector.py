import math
import os
from supabase import create_client, Client
import pandas as pd
from datetime import datetime

SUPABASE_URL='https://owdayzmhxpsfpyshwtxc.supabase.co'
SUPABASE_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93ZGF5em1oeHBzZnB5c2h3dHhjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mzc4MDAsImV4cCI6MjA2OTMxMzgwMH0.0mzxqkGi18QJXODxiXMKH5waZGruiFsi56elHxNyPks'
CLUB_DATA_PATH = "data/data_for_db/clubs_tm.csv"


def create_supabase_client():
    """Creates a Supabase client."""
    url = SUPABASE_URL
    key = SUPABASE_KEY
    return create_client(url, key)

def fetch_club_data(client: Client):
    """Fetches club data from the Supabase database."""
    response = client.table("Club").select("*").execute()
    return pd.DataFrame(response.data)

def fetch_league_data(client: Client):
    """Fetches league data from the Supabase database."""
    response = client.table("League").select("country", "tm_code", "tm_league_id").execute()    
    return pd.DataFrame(response.data)

def league_data_by_league_code(league_code: str, league_data: pd.DataFrame):
    """Filters league data by league code."""
    return league_data[league_data["tm_code"] == league_code]

def insert_club_data(supabase: Client, data: dict):
    """Inserts club data into the Supabase database."""
    response = supabase.table("Club").insert(data).execute()
    return response.data

def insert_club_season_data(supabase: Client, data: dict):
    """Inserts club season data into the Supabase database."""
    response = supabase.table("Season").insert(data).execute()
    return response.data    
    
def is_club_id_in_db(club_id: int, supabase: Client):
    """Checks if a club ID exists in the Supabase database."""
    response = supabase.table("Club").select("tm_club_id").eq("tm_club_id", club_id).execute()
    return len(response.data) > 0   

def is_season_club_in_db(supabase: Client, league_id: int, club_id: int, season_id: str):
    """Checks if a club-season combination exists in the Supabase database."""
    response = supabase.table("Season").select("*").eq("league_id", league_id).eq("club_id", club_id).eq("season_id", season_id).execute()
    return len(response.data) > 0
    

"""Initializes the Supabase client."""
supabase: Client = create_supabase_client()

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

        # Prepare data for insertion
        data = {
            "name": club_name,
            "tm_club_id": club_id,
            "country": club_country,
        }

        valuation = 0

        if math.isnan(row.valuation):
            print(f"Valuation for club {club_name} is not available, setting valuation to 0.")
            valuation = 0        
        else:
            valuation = float(row.valuation) 

        data_club_season = {
            "league_id": int(league_data_code["tm_league_id"].values[0]),
            "club_id": club_id,
            "season_id": row.season_id,
            "valuation": valuation,
        }

        # check if the club already exists in club_data db
        if club_id in db_club_data["tm_club_id"].values:
            print(f"Club {club_name} already exists in the database.")
            # insert_club_season_data(supabase, data_club_season)
        else:
            # # Insert data into the Club table
            if is_club_id_in_db(club_id, supabase):
                print(f"Club {club_name} with ID {club_id} already exists in the database.")
                continue    
            
            print(f"Inserting club {club_name} / {data} into the database...")
            club_inserted_data = pd.DataFrame(insert_club_data(supabase, data))        
            db_club_data = pd.concat([db_club_data, club_inserted_data])
            print(f"Inserted club {club_name} into the database.")


        if(is_season_club_in_db(supabase, data_club_season["league_id"], data_club_season["club_id"], data_club_season["season_id"])):
            print(f"Club season {data_club_season} already exists in the database.")
        else:

            print(f"Inserting club season data {data_club_season} into the database...")
            insert_club_season_data(supabase, data_club_season)
            print(f"Inserted club season data {data_club_season} into the database.")
            

        print("--------------------------------------")
    except Exception as e:
        print(f"Error processing club {row.club_name} with ID {row.club_id}: {e}")
        # add to array to log errors
        error_log.append(row._asdict())        
        continue

# Save error log to a CSV file
if error_log:
    error_log_df = pd.DataFrame(error_log)
    error_log_df.to_csv("error_log.csv", index=False)
    print(f"Error log saved to error_log.csv with {len(error_log)} errors.")

