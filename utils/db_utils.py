import pandas as pd
from supabase import Client

def fetch_club_data(client: Client) -> pd.DataFrame:
    response = client.table("Club").select("*").execute()
    return pd.DataFrame(response.data)

def fetch_league_data(client: Client) -> pd.DataFrame:
    response = client.table("League").select("country", "tm_code", "tm_league_id").execute()
    return pd.DataFrame(response.data)

def insert_club_data(client: Client, data: dict):
    response = client.table("Club").insert(data).execute()
    return response.data

def insert_club_season_data(client: Client, data: dict):
    response = client.table("Season").insert(data).execute()
    return response.data

def is_club_id_in_db(club_id: int, client: Client) -> bool:
    response = client.table("Club").select("tm_club_id").eq("tm_club_id", club_id).execute()
    return len(response.data) > 0

def is_season_club_in_db(client: Client, league_id: int, club_id: int, season_id: str) -> bool:
    response = client.table("Season").select("*").eq("league_id", league_id).eq("club_id", club_id).eq("season_id", season_id).execute()
    return len(response.data) > 0

