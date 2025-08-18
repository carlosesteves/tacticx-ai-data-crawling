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

def get_league_seasons(client: Client, league_id: int) -> pd.DataFrame:
    response = (
        client.table("Season")
        .select("season_id, League(tm_league_id, name, country, tier, region, tm_code)")
        .eq("league_id", league_id)
        .execute()
    )
    df = pd.DataFrame(response.data)

    # Expand the nested League dict into separate columns
    if "League" in df.columns:
        league_df = pd.json_normalize(df["League"])
        df = pd.concat([df.drop(columns=["League"]), league_df], axis=1)

    return df.dropna(subset=["season_id", "tm_league_id", "name", "country", "tier", "region", "tm_code"])

def insert_match_data(client: Client, match_data: dict):
    response = client.table("Match").insert(match_data).execute()
    return response.data


def get_matches_from_db(client: Client, league_id: str, season_id: int) -> pd.DataFrame:
    response = (
        client.table("Match")
        .select("*")
        .eq("league_id", league_id)
        .eq("season_id", season_id)
        .execute()
    )
    return pd.DataFrame(response.data)

def get_coaches_from_db(client: Client) -> pd.DataFrame:
    response = client.table("Coach").select("*").execute()
    return pd.DataFrame(response.data)

def insert_coach_data(client: Client, coach_data: dict):
    response = client.table("Coach").insert(coach_data).execute()
    return response.data

def insert_coach_tenure_data(client: Client, tenure_data: dict):
    response = client.table("Coach_tenure").insert(tenure_data).execute()
    return response.data


def get_clubs_from_db(client: Client) -> pd.DataFrame:
    response = client.table("Club").select("*").execute()
    return pd.DataFrame(response.data)