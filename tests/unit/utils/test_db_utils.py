import pandas as pd
import pytest
from unittest.mock import MagicMock

from utils.db_utils import (
    fetch_club_data,
    fetch_league_data,
    get_coaches_from_db,
    get_matches_from_db,
    insert_club_data,
    insert_club_season_data,
    insert_match_data,
    is_club_id_in_db,
    is_season_club_in_db,
    get_league_seasons,
)

@pytest.fixture
def mock_client():
    """Fixture to create a mock Supabase client."""
    return MagicMock()

def test_fetch_club_data(mock_client):
    mock_client.table.return_value.select.return_value.execute.return_value.data = [
        {"id": 1, "name": "Test FC"},
        {"id": 2, "name": "Example FC"},
    ]
    df = fetch_club_data(mock_client)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "name" in df.columns

def test_fetch_league_data(mock_client):
    mock_client.table.return_value.select.return_value.execute.return_value.data = [
        {"country": "England", "tm_code": "ENG", "tm_league_id": 1}
    ]
    df = fetch_league_data(mock_client)
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["country"] == "England"

def test_insert_club_data(mock_client):
    data = {"name": "Test FC"}
    mock_client.table.return_value.insert.return_value.execute.return_value.data = [data]
    result = insert_club_data(mock_client, data)
    assert result == [data]

def test_insert_club_season_data(mock_client):
    data = {"season_id": "2023", "club_id": 1}
    mock_client.table.return_value.insert.return_value.execute.return_value.data = [data]
    result = insert_club_season_data(mock_client, data)
    assert result == [data]

def test_is_club_id_in_db_true(mock_client):
    mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [{"tm_club_id": 1}]
    assert is_club_id_in_db(1, mock_client) is True

def test_is_club_id_in_db_false(mock_client):
    mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = []
    assert is_club_id_in_db(1, mock_client) is False

def test_is_season_club_in_db_true(mock_client):
    mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value.data = [{"id": 1}]
    assert is_season_club_in_db(mock_client, 1, 1, "2023") is True

def test_is_season_club_in_db_false(mock_client):
    mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value.data = []
    assert is_season_club_in_db(mock_client, 1, 1, "2023") is False

def test_get_league_seasons_flattened(mock_client):
    mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [
        {
            "season_id": "2023",
            "League": {
                "tm_league_id": 10,
                "name": "Premier League",
                "country": "England",
                "tier": 1,
                "region": "Europe",
                "tm_code": "EPL"
            }
        }
    ]
    df = get_league_seasons(mock_client, 1)
    assert isinstance(df, pd.DataFrame)
    assert "tm_league_id" in df.columns
    assert df.iloc[0]["name"] == "Premier League"


def test_insert_match_data(mock_client):
    mock_data = {"tm_match_id": 123}
    mock_client.table.return_value.insert.return_value.execute.return_value.data = [mock_data]

    result = insert_match_data(mock_client, mock_data)

    mock_client.table.assert_called_once_with("Match")
    mock_client.table.return_value.insert.assert_called_once_with(mock_data)
    assert result == [mock_data]


def test_get_matches_from_db(mock_client):
    mock_response_data = [
        {"tm_match_id": 123, "league_id": "PO1", "season_id": 2024}
    ]
    mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value.data = mock_response_data

    df = get_matches_from_db(mock_client, "PO1", 2024)

    mock_client.table.assert_called_once_with("Match")
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="records") == mock_response_data


def test_get_coaches_from_db(mock_client):
    mock_response_data = [
        {"coach_id": 1, "name": "José Mourinho"},
        {"coach_id": 2, "name": "Pep Guardiola"},
    ]
    mock_client.table.return_value.select.return_value.execute.return_value.data = mock_response_data

    df = get_coaches_from_db(mock_client)

    mock_client.table.assert_called_once_with("Coach")
    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="records") == mock_response_data