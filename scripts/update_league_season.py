#!/usr/bin/env python3
"""
Script to update a specific league-season with incremental processing.

Usage:
    python3 scripts/update_league_season.py --league GB1 --season 2025
    python3 scripts/update_league_season.py --league GB1 --season 2025 --full  # Force full reprocess
"""

import sys
import os

# Check Python version
if sys.version_info[0] < 3:
    print("Error: This script requires Python 3.6 or higher")
    print("Please run with: python3 scripts/update_league_season.py")
    sys.exit(1)

# Add project root to PYTHONPATH first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import requests

from pipelines.season_pipeline import run_season_pipeline
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


def main():
    parser = argparse.ArgumentParser(
        description='Update a specific league-season with incremental processing'
    )
    parser.add_argument(
        '--league', 
        required=True, 
        help='League code (e.g., GB1 for Premier League, ES1 for La Liga)'
    )
    parser.add_argument(
        '--season', 
        required=True, 
        type=int,
        help='Season year (e.g., 2025)'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Force full reprocessing (ignore checkpoint)'
    )
    
    args = parser.parse_args()
    
    league_code = args.league
    season_id = args.season
    full_reprocess = args.full
    
    print(f"\n{'='*60}")
    print(f"🏆 Updating League: {league_code}, Season: {season_id}")
    print(f"📊 Mode: {'Full Reprocess' if full_reprocess else 'Incremental Update'}")
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
    
    # If full reprocess, delete existing state
    if full_reprocess:
        print(f"🔄 Clearing existing state for full reprocess...")
        try:
            context.state_repo.delete_state(league_id, season_id)
            print(f"✅ State cleared")
        except Exception as e:
            print(f"⚠️  Could not clear state: {e}")
    
    # Run the season pipeline with incremental mode
    try:
        err_match_ids = run_season_pipeline(
            league_id=league_id,
            league_code=league_code,
            season_id=season_id,
            session=session,
            context=context,
            incremental=not full_reprocess  # Incremental unless full reprocess requested
        )
        
        if err_match_ids:
            print(f"\n⚠️  Completed with {len(err_match_ids)} errors")
            print(f"Failed match IDs: {err_match_ids}")
        else:
            print(f"\n✅ Successfully completed update for {league_code} {season_id}")
            
    except Exception as e:
        print(f"\n❌ Fatal error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"🏁 Update complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
