from pipelines.season_pipeline import run_season_pipeline
from repositories.coach.fake_coach_repository import FakeCoachRepository
from repositories.match.fake_match_repository import FakeMatchRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure import fake_coach_tenure_repository
from repositories.league_season_state.fake_league_season_state_repository import FakeLeagueSeasonStateRepository


def test_get_season_data():
    #season_data = run_season_pipeline(league_code='GB1', season_id=2011)
    context = PipelineContext(
        coach_repo=FakeCoachRepository(),
        match_repo=FakeMatchRepository(),
        tenure_repo=fake_coach_tenure_repository.FakeCoachTenureRepository(),
        state_repo=FakeLeagueSeasonStateRepository(),
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )

    season_data = run_season_pipeline(league_id=1, league_code='GB1', season_id=2011, session=None, context=context)
    assert isinstance(season_data, list)
    # assert one of the matches (including all info) is in the db
    # assert of the coaches (including info and tenures) is in the db
    # assert x coaches were created
    # assert x matches were created
    assert season_data is not None
