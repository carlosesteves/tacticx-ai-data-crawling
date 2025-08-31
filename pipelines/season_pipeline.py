import traceback
from requests import Session
from pages.league_page_matches import LeaguePageMatches
from pipelines.match_pipeline import run_match_pipeline
from repositories.pipeline_context import PipelineContext

def get_remaining_match_ids(league_id: str, league_code: str, season_id: int, context: PipelineContext, session: Session) -> set[int]:
    all_match_ids = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id).get_match_ids()
    processed_match_ids = context.match_repo.fetch_ids_by_year_league(season_id=season_id, league_id=league_id)  # You need to provide league_id here if required
    return all_match_ids - processed_match_ids - context.match_cache

def run_season_pipeline(league_id: int, league_code: str, season_id: int, session: Session, context: PipelineContext) -> list:
    match_counter = 1
    err_match_ids = []    
    
    remaining_match_ids = get_remaining_match_ids(league_id, league_code, season_id, context, session)

    if not remaining_match_ids:
        print(f"✅ All matches for league_id={league_id} season_id={season_id} already processed.")

    for match_id in remaining_match_ids:
        try:         
            match_counter+=1
            print(f"💬 Processing match={match_id} {match_counter}/{len(remaining_match_ids)}")   
            run_match_pipeline(session=session, match_id=match_id, league_id=league_id, season_id=season_id, context=context)            
        except Exception as e:
            err_match_ids.append(match_id)
            print(f"❌ Error processing match {match_id}: {e}")
            traceback.print_exc()

    return err_match_ids