import pytest
from unittest.mock import patch
from scripts.fetch_match_data import fech_year_match_data

# def test_fech_year_match_data_calls_construct_and_print(capsys):
#     league_code = "PO1"
#     season_id = 2024
#     fake_url = "https://fakeurl.com"


#     results = [
#         {"league_code": league_code, "season_id": season_id, "url": fake_url}
#     ]
#     results.append(fake_url)

#     # Patch construct_tm_league_url
#     with patch("scripts.fetch_match_data", return_value=fake_url) as mock_construct:
#         result = fech_year_match_data(league_code, season_id)

#     # Verify the helper function was called correctly
#     mock_construct.assert_called_once_with(league_code, season_id)

#     # Check printed output
#     captured = capsys.readouterr()
#     assert f"Fetching match data from: {fake_url}" in captured.out

#     # Check return value
#     # assert result is 
