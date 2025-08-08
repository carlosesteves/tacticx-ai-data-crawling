import pandas as pd

def league_data_by_league_code(league_code: str, league_data: pd.DataFrame) -> pd.DataFrame:
    return league_data[league_data["tm_code"] == league_code]
