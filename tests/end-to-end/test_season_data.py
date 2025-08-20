from pipelines.season_pipeline import run_season_pipeline


def test_get_season_data():
    season_data = run_season_pipeline(league_code='GB1', season_id=2011)
    assert isinstance(season_data, dict)
    # assert one of the matches (including all info) is in the db
    # assert of the coaches (including info and tenures) is in the db
    # assert x coaches were created
    # assert x matches were created
    assert season_data is not None
    
