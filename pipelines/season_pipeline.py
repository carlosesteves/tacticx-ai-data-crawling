import traceback
from datetime import datetime
from requests import Session
from pages.league_page_matches import LeaguePageMatches
from pipelines.match_pipeline import run_match_pipeline
from repositories.pipeline_context import PipelineContext
from models.league_season_state import LeagueSeasonState

def get_remaining_match_ids(league_id: str, league_code: str, season_id: int, context: PipelineContext, session: Session) -> set[int]:
    all_match_ids = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id).get_match_ids()
    processed_match_ids = context.match_repo.fetch_ids_by_year_league(season_id=season_id, league_id=league_id)
    return all_match_ids - processed_match_ids - context.match_cache

def get_matches_with_dates(league_code: str, season_id: int, session: Session) -> list[dict]:
    """Get all matches with their dates, sorted by date"""
    league_page = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id)
    matches = league_page.get_matches()
    
    # Sort by date
    matches_list = sorted(matches, key=lambda x: x.get('date', '9999-12-31'))
    return matches_list

def update_season_state(context: PipelineContext, league_id: int, season_id: int, 
                        last_match_id: int, last_match_date: datetime, 
                        total_processed: int, failed_match_ids: list = None, 
                        status: str = 'in_progress'):
    """Update the state of the league-season in the database"""
    state = LeagueSeasonState(
        league_id=league_id,
        season_id=season_id,
        last_processed_match_date=last_match_date,
        last_processed_match_id=last_match_id,
        total_matches_processed=total_processed,
        failed_match_ids=failed_match_ids or [],
        last_updated_at=datetime.now(),
        status=status
    )
    context.state_repo.save_state(state)
    return state

def run_season_pipeline(league_id: int, league_code: str, season_id: int, session: Session, 
                        context: PipelineContext, incremental: bool = False) -> list:
    """
    Run the season pipeline to process matches for a league-season.
    
    Always determines which matches to process by comparing what's on the page
    vs what's already in the database (never uses last_processed_match_date for filtering).
    
    Args:
        league_id: The ID of the league
        league_code: The Transfermarkt code for the league (e.g., 'GB1')
        season_id: The season year (e.g., 2025)
        session: HTTP session for requests
        context: Pipeline context with repositories
        incremental: Deprecated - kept for backward compatibility but ignored
    
    Returns:
        List of match IDs that had errors during processing
    """
    err_match_ids = []
    
    # Load existing state for informational/tracking purposes only
    existing_state = context.state_repo.get_state(league_id, season_id)
    if existing_state:
        print(f"📊 Current state: {existing_state.total_matches_processed} matches processed previously")
        if existing_state.failed_match_ids:
            print(f"⚠️  Previous run had {len(existing_state.failed_match_ids)} failed matches")
    
    # Get all matches with dates, sorted chronologically
    try:
        matches_with_dates = get_matches_with_dates(league_code, season_id, session)
        print(f"🔍 Found {len(matches_with_dates)} total matches for {league_code} {season_id}")
    except Exception as e:
        print(f"❌ Error fetching matches: {e}")
        return err_match_ids
    
    # Always determine what to process based on DB diff (not using last_processed_match_date)
    # Fetch processed match IDs from database
    processed_match_ids = context.match_repo.fetch_ids_by_year_league(season_id=season_id, league_id=league_id)
    all_match_ids = [int(m.get('match_id')) for m in matches_with_dates if m.get('match_id') is not None]
    
    # Filter to only matches not yet in database
    matches_to_process = [m for m in matches_with_dates if m.get('match_id') is not None and int(m.get('match_id')) not in processed_match_ids]

    print(f"🔎 {len(processed_match_ids)} match IDs already in DB")
    print(f"🔎 {len(all_match_ids) - len(matches_to_process)} match IDs will be excluded from processing")
    print(f"🔎 {len(matches_to_process)} match IDs to process")

    if not matches_to_process:
        print(f"✅ All matches for league_id={league_id} season_id={season_id} already processed.")
        # Update state as completed
        existing_state = existing_state or context.state_repo.get_state(league_id, season_id)
        if existing_state:
            final_status = 'completed' if not existing_state.failed_match_ids else 'completed_with_errors'
            update_season_state(context, league_id, season_id, 
                              existing_state.last_processed_match_id,
                              existing_state.last_processed_match_date,
                              existing_state.total_matches_processed,
                              existing_state.failed_match_ids,
                              status=final_status)
        return err_match_ids
    
    # Process matches in chronological order
    # Track total successfully processed (informational only)
    total_processed = 0
    accumulated_errors = []
    current_date = datetime.now().date()
    
    # Track last processed match for informational/state tracking purposes only
    last_processed_id = None
    last_processed_date = None
    
    for idx, match_data in enumerate(matches_to_process, 1):
        match_id = match_data.get('match_id')
        match_date_str = match_data.get('date')
        if not match_id:
            print(f"⚠️  Skipping match without ID")
            continue
        # Check if match is in the future
        if match_date_str:
            match_date = datetime.strptime(match_date_str, '%Y-%m-%d').date()
            if match_date >= current_date:
                print(f"🛑 Stopping: Match {match_id} on {match_date_str} is in the future or today")
                print(f"✅ Processed {total_processed} matches up to {match_date_str}")
                break
        try:
            print(f"💬 Processing match={match_id} ({idx}/{len(matches_to_process)}) Date: {match_date_str}")
            run_match_pipeline(session=session, match_id=match_id, league_id=league_id, 
                             season_id=season_id, context=context)
            total_processed += 1
            # Update tracking info
            if match_date_str:
                match_datetime = datetime.strptime(match_date_str, '%Y-%m-%d')
                last_processed_id = match_id
                last_processed_date = match_datetime
        except Exception as e:
            err_match_ids.append(match_id)
            accumulated_errors.append(match_id)
            print(f"❌ Error processing match {match_id}: {e}")
            traceback.print_exc()
            # Still track this match for informational purposes
            if match_date_str:
                match_datetime = datetime.strptime(match_date_str, '%Y-%m-%d')
                last_processed_id = match_id
                last_processed_date = match_datetime

    # Update final state for tracking/informational purposes
    if total_processed > 0 or accumulated_errors:
        # Calculate total across all runs
        base_total = existing_state.total_matches_processed if existing_state else 0
        final_total = base_total + total_processed
        
        final_status = 'completed' if not accumulated_errors else 'completed_with_errors'
        update_season_state(context, league_id, season_id,
                          last_processed_id,
                          last_processed_date, 
                          final_total, 
                          accumulated_errors,
                          status=final_status)
        
        if accumulated_errors:
            print(f"⚠️  Season pipeline completed with errors: {total_processed} processed, {len(accumulated_errors)} failed")
            print(f"Failed match IDs: {accumulated_errors}")
        else:
            print(f"✅ Season pipeline completed: {total_processed} matches processed in this run")

    return err_match_ids


def update_season_pipeline(league_id: int, season_id: int, session: Session, context: PipelineContext) -> list:
    """
    Legacy function - consider using run_season_pipeline with incremental=True instead
    """
    err_match_ids = []    
    league_codes = []
    for league_code in league_codes:        
        all_match_ids = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id).get_match_ids()

        for match_id in all_match_ids:
            try:         
                print(f"💬 Updating match={match_id}")   
                run_match_pipeline(session=session, match_id=match_id, league_id=league_id, season_id=season_id, context=context)            
            except Exception as e:
                err_match_ids.append(match_id)
                print(f"❌ Error updating match {match_id}: {e}")
                traceback.print_exc()

    return err_match_ids