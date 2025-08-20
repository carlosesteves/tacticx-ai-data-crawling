import pytest
from unittest.mock import MagicMock, patch

from pipelines.match_pipeline import run_match_pipeline

@pytest.fixture
def mock_context():
    context = MagicMock()
    context.match_repo.fetch_all_ids.return_value = []
    context.match_cache = set()
    return context


def test_skip_if_in_cache(mock_context):
    mock_context.match_cache.add(123)

    run_match_pipeline(match_id=123, league_id=1, season_id=2025, context=mock_context)

    # should not save
    mock_context.match_repo.save.assert_not_called()


def test_skip_if_in_db(mock_context):
    mock_context.match_repo.fetch_all_ids.return_value = [456]

    run_match_pipeline(match_id=456, league_id=1, season_id=2025, context=mock_context)

    mock_context.match_repo.save.assert_not_called()


@patch("services.match_service.MatchService.parse")
@patch("pipelines.match_pipeline.run_match_pipeline")
def test_insert_new_match(mock_run_coach, mock_parse, mock_context):
    # mock parsed match
    mock_match = MagicMock()
    mock_match.tm_match_id = 789
    mock_match.home_coach_id = 10
    mock_match.away_coach_id = 20
    mock_parse.return_value = mock_match


    run_match_pipeline(match_id=789, league_id=1, season_id=2025, context=mock_context)

    # coaches should be processed
    mock_run_coach.assert_any_call(10, mock_context)
    mock_run_coach.assert_any_call(20, mock_context)

    # match should be saved
    mock_context.match_repo.save.assert_called_once_with(mock_match)

    # match should be added to cache
    assert 789 in mock_context.match_cache