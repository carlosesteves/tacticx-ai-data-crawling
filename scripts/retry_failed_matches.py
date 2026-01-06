#!/usr/bin/env python3
"""
[DEPRECATED] Script to retry failed matches from a league-season.

NOTE: This script is now deprecated. The main update scripts (update_all_leagues_season.py
and update_league_season.py) now automatically detect which matches are missing from the
database by comparing what's on the Transfermarkt page vs what's in the DB. Simply re-run
the main script to process any failed or missing matches.

The failed_match_ids field in league_season_state is kept for informational/tracking
purposes only and is not used to determine which matches to process.

Usage (if you still want to use this):
    python3 scripts/retry_failed_matches.py --league GB1 --season 2025
    python3 scripts/retry_failed_matches.py --league GB1 --season 2025 --match-id 1234567

Better alternative:
    python3 scripts/update_league_season.py --league GB1 --season 2025
"""

import sys
import os

# Check Python version
if sys.version_info[0] < 3:
    print("Error: This script requires Python 3.6 or higher")
    print("Please run with: python3 scripts/retry_failed_matches.py")
    sys.exit(1)

# Add project root to PYTHONPATH first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import requests
import traceback
from datetime import datetime

from pipelines.match_pipeline import run_match_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.league_season_state.supabase_league_season_state_repository import SupabaseLeagueSeasonStateRepository
from utils.db_utils import get_league_id_by_code
from services.supabase_service import create_supabase_client


def create_context() -> PipelineContext:
    """Create a pipeline context with all necessary repositories"""
    client = create_supabase_client()
    return PipelineContext(
        coach_repo=SupabaseCoachRepository(client=client),
        match_repo=SupabaseMatchRepository(client=client),
        tenure_repo=SupabaseCoachTenureRepository(client=client),
        state_repo=SupabaseLeagueSeasonStateRepository(client=client),
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )


def retry_failed_matches(league_id: int, season_id: int, context: PipelineContext, 
                        session: requests.Session, specific_match_id: int = None):
    """
    Retry failed matches for a league-season
    
    Args:
        league_id: The league ID
        season_id: The season year
        context: Pipeline context
        session: HTTP session
        specific_match_id: Optional specific match ID to retry
    
    Returns:
        Tuple of (successful_count, still_failed_ids)
    """
    # Get the current state
    state = context.state_repo.get_state(league_id, season_id)
    
    if not state:
        print(f"❌ No state found for league_id={league_id}, season_id={season_id}")
        return 0, []
    
    failed_ids = state.failed_match_ids if state.failed_match_ids else []
    
    if not failed_ids:
        print(f"✅ No failed matches to retry for league_id={league_id}, season_id={season_id}")
        return 0, []
    
    # Filter to specific match if provided
    if specific_match_id:
        if specific_match_id in failed_ids:
            matches_to_retry = [specific_match_id]
            print(f"🔄 Retrying specific match: {specific_match_id}")
        else:
            print(f"❌ Match {specific_match_id} is not in the failed list")
            return 0, failed_ids
    else:
        matches_to_retry = failed_ids
        print(f"🔄 Found {len(matches_to_retry)} failed matches to retry")
    
    successful_retries = []
    still_failed = []
    
    for idx, match_id in enumerate(matches_to_retry, 1):
        try:
            print(f"\n💬 Retrying match {match_id} ({idx}/{len(matches_to_retry)})")
            run_match_pipeline(
                session=session,
                match_id=match_id,
                league_id=league_id,
                season_id=season_id,
                context=context
            )
            successful_retries.append(match_id)
            print(f"✅ Successfully processed match {match_id}")
            
        except Exception as e:
            still_failed.append(match_id)
            print(f"❌ Match {match_id} failed again: {e}")
            traceback.print_exc()
    
    # Update state with new failed list
    if successful_retries:
        remaining_failed = [m for m in failed_ids if m not in successful_retries]
        
        # Update the state
        from models.league_season_state import LeagueSeasonState
        updated_state = LeagueSeasonState(
            league_id=state.league_id,
            season_id=state.season_id,
            last_processed_match_date=state.last_processed_match_date,
            last_processed_match_id=state.last_processed_match_id,
            total_matches_processed=state.total_matches_processed + len(successful_retries),
            failed_match_ids=remaining_failed,
            last_updated_at=datetime.now(),
            status='completed' if not remaining_failed else 'completed_with_errors'
        )
        context.state_repo.save_state(updated_state)
    
    return len(successful_retries), still_failed


def main():
    parser = argparse.ArgumentParser(
        description='Retry failed matches from a league-season'
    )
    parser.add_argument(
        '--league', 
        required=True, 
        help='League code (e.g., GB1 for Premier League)'
    )
    parser.add_argument(
        '--season', 
        required=True, 
        type=int,
        help='Season year (e.g., 2025)'
    )
    parser.add_argument(
        '--match-id',
        type=int,
        help='Retry only a specific match ID (optional)'
    )
    
    args = parser.parse_args()
    
    league_code = args.league
    season_id = args.season
    specific_match_id = args.match_id
    
    print(f"\n{'='*60}")
    print(f"🔄 Retrying Failed Matches")
    print(f"🏆 League: {league_code}, Season: {season_id}")
    if specific_match_id:
        print(f"🎯 Specific Match: {specific_match_id}")
    print(f"{'='*60}\n")
    
    # Get league ID from code
    try:
        client = create_supabase_client()
        league_id = get_league_id_by_code(client, league_code)
        print(f"✅ League ID: {league_id}")
    except Exception as e:
        print(f"❌ Error fetching league ID for code '{league_code}': {e}")
        sys.exit(1)
    
    # Create context and session
    context = create_context()
    session = requests.session()
    session.verify = False  # Bypass SSL certificate verification
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Retry failed matches
    try:
        successful_count, still_failed = retry_failed_matches(
            league_id=league_id,
            season_id=season_id,
            context=context,
            session=session,
            specific_match_id=specific_match_id
        )
        
        print(f"\n{'='*60}")
        print(f"📊 Retry Results:")
        print(f"✅ Successfully processed: {successful_count}")
        print(f"❌ Still failed: {len(still_failed)}")
        if still_failed:
            print(f"Failed match IDs: {still_failed}")
        print(f"{'='*60}\n")
        
        if successful_count > 0:
            print(f"✅ Retry complete! {successful_count} matches now processed successfully.")
        elif still_failed:
            print(f"⚠️  All retry attempts failed. Consider investigating these matches manually.")
        
    except Exception as e:
        print(f"\n❌ Fatal error during retry: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
