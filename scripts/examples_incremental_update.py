#!/usr/bin/env python3
"""
Example: How to use the incremental update feature

This script demonstrates different ways to use the new incremental update system.
"""

import sys
import os
import requests

# Add project root to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.season_pipeline import run_season_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.league_season_state.supabase_league_season_state_repository import SupabaseLeagueSeasonStateRepository
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


def example_1_incremental_update():
    """Example 1: Incremental update of a league-season"""
    print("\n" + "="*60)
    print("Example 1: Incremental Update")
    print("="*60)
    
    context = create_context()
    session = requests.session()
    
    # Update GB1 2025 incrementally - only processes new matches
    err_match_ids = run_season_pipeline(
        league_id=17,  # GB1 Premier League
        league_code='GB1',
        season_id=2025,
        session=session,
        context=context,
        incremental=True  # Only process new matches
    )
    
    print(f"Completed with {len(err_match_ids)} errors")


def example_2_full_reprocess():
    """Example 2: Full reprocess of a league-season"""
    print("\n" + "="*60)
    print("Example 2: Full Reprocess")
    print("="*60)
    
    context = create_context()
    session = requests.session()
    
    # First, clear the state
    context.state_repo.delete_state(league_id=17, season_id=2025)
    
    # Then run with incremental=False to process all matches
    err_match_ids = run_season_pipeline(
        league_id=17,
        league_code='GB1',
        season_id=2025,
        session=session,
        context=context,
        incremental=False  # Process all matches
    )
    
    print(f"Completed with {len(err_match_ids)} errors")


def example_3_check_state():
    """Example 3: Check the current state of a league-season"""
    print("\n" + "="*60)
    print("Example 3: Check State")
    print("="*60)
    
    context = create_context()
    
    # Get the current state
    state = context.state_repo.get_state(league_id=17, season_id=2025)
    
    if state:
        print(f"League ID: {state.league_id}")
        print(f"Season ID: {state.season_id}")
        print(f"Last Processed Match: {state.last_processed_match_id}")
        print(f"Last Processed Date: {state.last_processed_match_date}")
        print(f"Total Matches Processed: {state.total_matches_processed}")
        print(f"Status: {state.status}")
        print(f"Last Updated: {state.last_updated_at}")
    else:
        print("No state found for this league-season")


def example_4_multiple_leagues():
    """Example 4: Update multiple leagues"""
    print("\n" + "="*60)
    print("Example 4: Update Multiple Leagues")
    print("="*60)
    
    context = create_context()
    session = requests.session()
    
    # Define leagues to update
    leagues = [
        {'league_id': 17, 'league_code': 'GB1', 'name': 'Premier League'},
        {'league_id': 19, 'league_code': 'ES1', 'name': 'La Liga'},
        {'league_id': 20, 'league_code': 'L1', 'name': 'Bundesliga'},
    ]
    
    season_id = 2025
    
    for league in leagues:
        print(f"\n🏆 Updating {league['name']}...")
        
        err_match_ids = run_season_pipeline(
            league_id=league['league_id'],
            league_code=league['league_code'],
            season_id=season_id,
            session=session,
            context=context,
            incremental=True
        )
        
        print(f"✅ {league['name']} completed with {len(err_match_ids)} errors")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Incremental Update Examples")
    print("="*60)
    print("\nUncomment the example you want to run:\n")
    
    # Uncomment one of these to run:
    
    # example_1_incremental_update()
    # example_2_full_reprocess()
    # example_3_check_state()
    # example_4_multiple_leagues()
    
    print("\nEdit this file to uncomment the example you want to run.")
