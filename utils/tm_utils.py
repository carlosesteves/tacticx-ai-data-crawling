from config.constants import TM_BASE_URL

def construct_tm_league_url(league_code: str, season_id: int) -> str:
    """
    Constructs the Transfermarkt league URL for a given league code and season ID.
    
    Args:
        league_code (str): The code of the league (e.g., 'PO1' for Liga Portugal).
        season_id (int): The ID of the season (e.g., 2024).
        
    Returns:
        str: The constructed URL.
    """
    return f"{TM_BASE_URL}-/gesamtspielplan/wettbewerb/{league_code}/saison_id/{season_id}"