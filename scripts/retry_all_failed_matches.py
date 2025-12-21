#!/usr/bin/env python3
"""
Script to retry all failed matches for all leagues and all seasons.

Usage:
    python scripts/retry_all_failed_matches.py
"""
import sys
import os
from datetime import datetime

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests
from pipelines.match_pipeline import run_match_pipeline
from repositories.pipeline_context import PipelineContext
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.league_season_state.supabase_league_season_state_repository import SupabaseLeagueSeasonStateRepository
from utils.db_utils import get_league_season_state
from services.supabase_service import create_supabase_client
from scripts.retry_failed_matches import retry_failed_matches

def create_context() -> PipelineContext:
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
    print(f"\n{'='*60}")
    print(f"🔄 Retrying ALL Failed Matches for ALL Leagues and Seasons")
    print(f"{'='*60}\n")

    client = create_supabase_client()
    context = create_context()
    session = requests.session()
    session.verify = False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


    # Fetch all league_season_state rows
    response = client.table("league_season_state").select("league_id, season_id").execute()
    rows = response.data if response.data else []
    for row in rows:
        league_id = row["league_id"]
        season_id = row["season_id"]
        print(f"\n--- League ID: {league_id}, Season: {season_id} ---")
        successful_count, still_failed = retry_failed_matches(
            league_id=league_id,
            season_id=season_id,
            context=context,
            session=session
        )
        print(f"✅ Successfully processed: {successful_count}")
        print(f"❌ Still failed: {len(still_failed)}")
        if still_failed:
            print(f"Failed match IDs: {still_failed}")

    print(f"\n{'='*60}")
    print(f"🎉 All retries complete!")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
