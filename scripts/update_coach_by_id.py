#!/usr/bin/env python3
"""
Script to update a specific coach by their Transfermarkt ID.

This script fetches the latest data for a coach including their basic info
and all tenures, and updates the database.

Usage:
    python3 scripts/update_coach_by_id.py --coach-id 450
    python3 scripts/update_coach_by_id.py --coach-id 450,10939,5672  # Multiple coaches
"""

import sys
import os

# Check Python version
if sys.version_info[0] < 3:
    print("Error: This script requires Python 3.6 or higher")
    print("Please run with: python3 scripts/update_coach_by_id.py")
    sys.exit(1)

# Disable SSL certificate verification BEFORE any imports
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pipelines.coach_pipeline import run_coach_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.pipeline_context import PipelineContext
from services.supabase_service import create_supabase_client


def create_context() -> PipelineContext:
    """Create a pipeline context with necessary repositories"""
    client = create_supabase_client()
    return PipelineContext(
        coach_repo=SupabaseCoachRepository(client=client),
        match_repo=None,  # Not needed for coach updates
        tenure_repo=SupabaseCoachTenureRepository(client=client),
        state_repo=None,  # Not needed for coach updates
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )


def main():
    parser = argparse.ArgumentParser(
        description='Update a specific coach or multiple coaches by their Transfermarkt ID(s)'
    )
    parser.add_argument(
        '--coach-id',
        required=True,
        type=str,
        help='Coach ID(s) from Transfermarkt (comma-separated for multiple, e.g., "450,10939,5672")'
    )
    
    args = parser.parse_args()
    
    # Parse coach IDs
    coach_ids = [cid.strip() for cid in args.coach_id.split(',')]
    
    print(f"\n{'='*80}")
    print(f"👔 Updating Coach(es): {', '.join(coach_ids)}")
    print(f"{'='*80}\n")
    
    # Create session with SSL bypass
    session = requests.session()
    session.verify = False
    
    # Create context
    context = create_context()
    
    # Track results
    successful_coaches = []
    failed_coaches = []
    
    # Process each coach
    for coach_id in coach_ids:
        print(f"\n{'='*80}")
        print(f"📋 Processing Coach ID: {coach_id}")
        print(f"{'='*80}")
        
        try:
            run_coach_pipeline(
                session=session,
                coach_id=coach_id,
                context=context
            )
            
            successful_coaches.append(coach_id)
            print(f"✅ Successfully updated coach {coach_id}")
            
        except Exception as e:
            failed_coaches.append(coach_id)
            print(f"❌ Error updating coach {coach_id}: {e}")
            import traceback
            traceback.print_exc()
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"📊 Update Summary")
    print(f"{'='*80}")
    print(f"✅ Successfully updated: {len(successful_coaches)} coach(es)")
    if successful_coaches:
        print(f"   Coach IDs: {', '.join(successful_coaches)}")
    
    if failed_coaches:
        print(f"\n❌ Failed to update: {len(failed_coaches)} coach(es)")
        print(f"   Coach IDs: {', '.join(failed_coaches)}")
    
    print(f"\n{'='*80}\n")
    
    # Exit with error code if any failures
    if failed_coaches:
        sys.exit(1)


if __name__ == "__main__":
    main()
