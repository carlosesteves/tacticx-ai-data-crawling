#!/usr/bin/env python3
"""
Script to update a match by match ID.

Usage:
    python scripts/update_match_by_id.py --match-id 1234567 --league GB1 --season 2025
"""
import sys
import os
import argparse
import requests
from datetime import datetime

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipelines.match_pipeline import run_match_pipeline
from repositories.pipeline_context import PipelineContext
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.league_season_state.supabase_league_season_state_repository import SupabaseLeagueSeasonStateRepository
from utils.db_utils import get_league_id_by_code
from services.supabase_service import create_supabase_client

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
    parser = argparse.ArgumentParser(description='Update a match by match ID')
    parser.add_argument('--match-id', required=True, type=int, help='Match ID to update')
    parser.add_argument('--league', required=True, help='League code (e.g., GB1)')
    parser.add_argument('--season', required=True, type=int, help='Season year (e.g., 2025)')
    args = parser.parse_args()

    match_id = args.match_id
    league_code = args.league
    season_id = args.season

    print(f"\n{'='*60}")
    print(f"🔄 Updating Match {match_id}")
    print(f"🏆 League: {league_code}, Season: {season_id}")
    print(f"{'='*60}\n")

    try:
        client = create_supabase_client()
        league_id = get_league_id_by_code(client, league_code)
        print(f"✅ League ID: {league_id}")
    except Exception as e:
        print(f"❌ Error fetching league ID for code '{league_code}': {e}")
        sys.exit(1)

    context = create_context()
    session = requests.session()
    session.verify = False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        run_match_pipeline(
            session=session,
            match_id=match_id,
            league_id=league_id,
            season_id=season_id,
            context=context
        )
        print(f"✅ Successfully updated match {match_id}")
    except Exception as e:
        print(f"❌ Failed to update match {match_id}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
