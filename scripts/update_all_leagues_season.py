#!/usr/bin/env python3
"""
Script to update a specific season for all leagues in the database.

This script fetches all distinct leagues from the database and updates each one for the
specified season. It automatically detects which matches need processing by comparing what's
on the Transfermarkt page vs what's already in the database.

The script does NOT rely on last_processed_match_date or failed_match_ids for determining
what to process - those fields are kept for informational/tracking purposes only.

For each league-season, the script will:
1. Fetch all match IDs from the Transfermarkt page
2. Fetch all match IDs already in the database
3. Process the difference (matches not yet in DB)
4. Update tracking state for monitoring

Usage:
    python3 scripts/update_all_leagues_season.py --season 2025
    python3 scripts/update_all_leagues_season.py --season 2025 --full  # Force full reprocess
    python3 scripts/update_all_leagues_season.py --season 2025 --limit 5  # Test with first 5 leagues
"""

import sys
import os

# Check Python version
if sys.version_info[0] < 3:
    print("Error: This script requires Python 3.6 or higher")
    print("Please run with: python3 scripts/update_all_leagues_season.py")
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
from utils.db_utils import fetch_league_data
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
        description='Update a specific season for all leagues in the database'
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
        help='Force full reprocessing for all leagues (ignore checkpoints)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit to first N leagues (for testing)'
    )
    
    args = parser.parse_args()
    
    season_id = args.season
    full_reprocess = args.full
    limit = args.limit
    
    print(f"\n{'='*80}")
    print(f"🌍 Updating All Leagues for Season: {season_id}")
    print(f"📊 Mode: {'Full Reprocess' if full_reprocess else 'Incremental Update'}")
    if limit:
        print(f"⚠️  Limited to first {limit} leagues")
    print(f"{'='*80}\n")
    
    # Fetch all leagues from database
    try:
        client = create_supabase_client()
        leagues_df = fetch_league_data(client)
        print(f"✅ Found {len(leagues_df)} leagues in database\n")
    except Exception as e:
        print(f"❌ Error fetching leagues from database: {e}")
        sys.exit(1)
    
    # Apply limit if specified
    if limit:
        leagues_df = leagues_df.head(limit)
    
    # Create session
    session = requests.session()
    session.verify = False  # Bypass SSL certificate verification
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Track results
    successful_leagues = []
    failed_leagues = []
    total_errors = 0
    
    # Process each league
    for idx, league in enumerate(leagues_df.itertuples(), 1):
        league_id = league.tm_league_id
        league_code = league.tm_code
        country = league.country
        
        print(f"\n{'='*80}")
        print(f"[{idx}/{len(leagues_df)}] 🏆 Processing: {country} - {league_code}")
        print(f"{'='*80}")
        
        # Create fresh context for each league to avoid cache bloat
        context = create_context()
        
        # If full reprocess, delete existing state
        if full_reprocess:
            try:
                context.state_repo.delete_state(league_id, season_id)
                print(f"🔄 Cleared state for full reprocess")
            except Exception as e:
                print(f"⚠️  Could not clear state: {e}")
        
        # Run the season pipeline
        try:
            err_match_ids = run_season_pipeline(
                league_id=league_id,
                league_code=league_code,
                season_id=season_id,
                session=session,
                context=context,
                incremental=not full_reprocess
            )
            
            if err_match_ids:
                total_errors += len(err_match_ids)
                failed_leagues.append({
                    'league_code': league_code,
                    'country': country,
                    'error_count': len(err_match_ids)
                })
                print(f"⚠️  Completed with {len(err_match_ids)} match errors")
            else:
                successful_leagues.append(f"{country} - {league_code}")
                print(f"✅ Successfully completed")
                
        except Exception as e:
            failed_leagues.append({
                'league_code': league_code,
                'country': country,
                'error': str(e)
            })
            print(f"❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"📊 SUMMARY - Season {season_id} Update")
    print(f"{'='*80}")
    print(f"✅ Successfully processed: {len(successful_leagues)} leagues")
    print(f"❌ Failed or had errors: {len(failed_leagues)} leagues")
    print(f"⚠️  Total match errors: {total_errors}")
    
    if successful_leagues:
        print(f"\n✅ Successful leagues:")
        for league in successful_leagues:
            print(f"   • {league}")
    
    if failed_leagues:
        print(f"\n❌ Failed/Error leagues:")
        for league in failed_leagues:
            error_info = f" ({league.get('error_count', 0)} matches)" if 'error_count' in league else f" - {league.get('error', 'Unknown error')}"
            print(f"   • {league['country']} - {league['league_code']}{error_info}")
    
    print(f"\n{'='*80}")
    print(f"🏁 All leagues processed!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
