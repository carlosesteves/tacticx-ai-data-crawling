#!/usr/bin/env python3
"""
ETL Pipeline: Populate match_team_expectation table
Converts Match data with odds into team-level performance metrics
"""

import pandas as pd
from supabase import create_client, Client
from typing import Dict, Tuple, Optional
from datetime import datetime
import time

# Supabase configuration
SUPABASE_URL = "https://owdayzmhxpsfpyshwtxc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93ZGF5em1oeHBzZnB5c2h3dHhjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mzc4MDAsImV4cCI6MjA2OTMxMzgwMH0.0mzxqkGi18QJXODxiXMKH5waZGruiFsi56elHxNyPks"


def odds_to_probabilities(odds_home: float, odds_draw: float, odds_away: float) -> Tuple[float, float, float]:
    """
    Convert decimal odds to probabilities with overround removal.
    
    Bookmaker odds include overround (margin). We remove it to get true probabilities.
    Formula: p_i = (1/odds_i) / sum(1/odds_j)
    """
    if pd.isna(odds_home) or pd.isna(odds_draw) or pd.isna(odds_away):
        return None, None, None
    
    if odds_home <= 0 or odds_draw <= 0 or odds_away <= 0:
        return None, None, None
    
    # Convert to implied probabilities
    implied_home = 1.0 / odds_home
    implied_draw = 1.0 / odds_draw
    implied_away = 1.0 / odds_away
    
    # Remove overround (normalize to sum to 1)
    total = implied_home + implied_draw + implied_away
    
    p_home = implied_home / total
    p_draw = implied_draw / total
    p_away = implied_away / total
    
    return round(p_home, 4), round(p_draw, 4), round(p_away, 4)


def calculate_expected_points(p_win: float, p_draw: float) -> float:
    """
    Calculate expected points from win/draw probabilities.
    
    xPts = 3 * P(win) + 1 * P(draw) + 0 * P(loss)
    """
    return round(3 * p_win + 1 * p_draw, 3)


def calculate_actual_points(goals_for: int, goals_against: int) -> int:
    """Calculate actual points from match result."""
    if goals_for > goals_against:
        return 3  # Win
    elif goals_for == goals_against:
        return 1  # Draw
    else:
        return 0  # Loss


def determine_difficulty(p_win: float, p_draw: float, p_loss: float, is_home: bool) -> str:
    """
    Determine match difficulty based on win probability.
    
    For home teams:
    - high: p_win < 0.35 (underdog)
    - medium: 0.35 <= p_win <= 0.55
    - low: p_win > 0.55 (favorite)
    
    For away teams (slightly adjusted):
    - high: p_win < 0.25
    - medium: 0.25 <= p_win <= 0.45
    - low: p_win > 0.45
    """
    if pd.isna(p_win):
        return None
    
    if is_home:
        if p_win < 0.34:
            return 'high'
        elif p_win <= 0.532:
            return 'medium'
        else:
            return 'low'
    else:  # away
        if p_win < 0.205:
            return 'high'
        elif p_win <= 0.367:
            return 'medium'
        else:
            return 'low'


def fetch_matches_with_odds(supabase: Client, limit: Optional[int] = None) -> pd.DataFrame:
    """Fetch matches that have complete odds data."""
    print("Fetching matches with odds from database...")
    
    all_matches = []
    batch_size = 1000
    offset = 0
    
    while True:
        query = supabase.table('Match').select(
            'tm_match_id, date, league_id, '
            'home_club_id, away_club_id, '
            'home_team_score, away_team_score, '
            'odds_home, odds_draw, odds_away'
        )
        
        if limit:
            query = query.limit(limit)
        
        result = query.range(offset, offset + batch_size - 1).execute()
        
        if not result.data:
            break
        
        all_matches.extend(result.data)
        offset += batch_size
        
        if limit and len(all_matches) >= limit:
            all_matches = all_matches[:limit]
            break
        
        print(f"  Fetched {len(all_matches):,} matches...", end='\r')
    
    print(f"\n  ✓ Total matches fetched: {len(all_matches):,}")
    
    df = pd.DataFrame(all_matches)
    
    # Filter to matches with complete odds
    initial_count = len(df)
    df = df[df['odds_home'].notna() & df['odds_draw'].notna() & df['odds_away'].notna()]
    print(f"  ✓ Matches with complete odds: {len(df):,} (filtered out {initial_count - len(df):,})")
    
    return df


def fetch_coach_data(supabase: Client) -> pd.DataFrame:
    """Fetch coach tenure data to map coaches to matches."""
    print("Fetching coach tenure data...")
    
    all_tenures = []
    batch_size = 1000
    offset = 0
    
    while True:
        result = supabase.table('Coach_tenure').select(
            'coach_id, club_id, start_date, end_date, role'
        ).eq('role', 'Manager').range(offset, offset + batch_size - 1).execute()
        
        if not result.data:
            break
        
        all_tenures.extend(result.data)
        offset += batch_size
        print(f"  Fetched {len(all_tenures):,} tenures...", end='\r')
    
    print(f"\n  ✓ Total manager tenures: {len(all_tenures):,}")
    
    return pd.DataFrame(all_tenures)


def map_coach_to_match(team_id: int, match_date: str, coach_df: pd.DataFrame) -> Optional[int]:
    """Find the coach for a team on a specific match date."""
    if coach_df.empty or pd.isna(match_date):
        return None
    
    # Filter to this team's coaches
    team_coaches = coach_df[coach_df['club_id'] == team_id]
    
    if team_coaches.empty:
        return None
    
    # Find coach active on match date
    match_date_pd = pd.to_datetime(match_date)
    
    for _, tenure in team_coaches.iterrows():
        from_date = pd.to_datetime(tenure['start_date']) if tenure['start_date'] else pd.NaT
        until_date = pd.to_datetime(tenure['end_date']) if tenure['end_date'] else pd.Timestamp.max
        
        if pd.notna(from_date) and from_date <= match_date_pd <= until_date:
            return int(tenure['coach_id'])
    
    return None


def transform_match_to_team_expectations(match_row: pd.Series, coach_df: pd.DataFrame) -> list:
    """
    Transform one match into zero, one, or two rows (one per team with a coach) with expectations.
    """
    # Convert odds to probabilities
    p_home_win, p_draw, p_away_win = odds_to_probabilities(
        match_row['odds_home'],
        match_row['odds_draw'],
        match_row['odds_away']
    )
    
    if p_home_win is None:
        return []  # Skip if odds conversion failed
    
    # Get coach IDs for each team independently
    home_coach_id = map_coach_to_match(match_row['home_club_id'], match_row['date'], coach_df)
    away_coach_id = map_coach_to_match(match_row['away_club_id'], match_row['date'], coach_df)
    
    # Only create rows for teams that have coaches
    # (coach_id is NOT NULL constraint in database, but we can still create row for one team)
    results = []
    
    # Home team row
    if home_coach_id is not None:
        home_x_pts = calculate_expected_points(p_home_win, p_draw)
        home_actual_pts = calculate_actual_points(
            match_row['home_team_score'],
            match_row['away_team_score']
        )
        home_difficulty = determine_difficulty(p_home_win, p_draw, p_away_win, is_home=True)
        
        home_row = {
            'match_id': int(match_row['tm_match_id']),
            'match_date': match_row['date'],
            'league_id': match_row['league_id'],
            'club_id': int(match_row['home_club_id']),
            'is_home': True,
            'coach_id': home_coach_id,
            'xpts': home_x_pts,
            'actual_pts': home_actual_pts,
            'delta_pts': round(home_actual_pts - home_x_pts, 3),
            'difficulty': home_difficulty,
            'p_win': p_home_win,
            'p_draw': p_draw,
            'p_loss': p_away_win,
            'goals_for': int(match_row['home_team_score']),
            'goals_against': int(match_row['away_team_score'])
        }
        results.append(home_row)
    
    # Away team row
    if away_coach_id is not None:
        away_x_pts = calculate_expected_points(p_away_win, p_draw)
        away_actual_pts = calculate_actual_points(
            match_row['away_team_score'],
            match_row['home_team_score']
        )
        away_difficulty = determine_difficulty(p_away_win, p_draw, p_home_win, is_home=False)
        
        away_row = {
            'match_id': int(match_row['tm_match_id']),
            'match_date': match_row['date'],
            'league_id': match_row['league_id'],
            'club_id': int(match_row['away_club_id']),
            'is_home': False,
            'coach_id': away_coach_id,
            'xpts': away_x_pts,
            'actual_pts': away_actual_pts,
            'delta_pts': round(away_actual_pts - away_x_pts, 3),
            'difficulty': away_difficulty,
            'p_win': p_away_win,
            'p_draw': p_draw,
            'p_loss': p_home_win,
            'goals_for': int(match_row['away_team_score']),
            'goals_against': int(match_row['home_team_score'])
        }
        results.append(away_row)
    
    return results


def batch_upsert_expectations(supabase: Client, expectations: list, batch_size: int = 100):
    """Upsert team expectations in batches."""
    print(f"\nUpserting {len(expectations):,} team expectation records...")
    
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    for i in range(0, len(expectations), batch_size):
        batch = expectations[i:i+batch_size]
        
        try:
            supabase.table('match_team_expectation').upsert(
                batch,
                on_conflict='match_id,club_id'
            ).execute()
            
            success_count += len(batch)
            
        except Exception as e:
            error_count += len(batch)
            print(f"\n  ❌ Error upserting batch {i//batch_size + 1}: {e}")
            # Print first record of failed batch for debugging
            if batch and error_count <= 10:
                print(f"     First record in failed batch: {batch[0]}")
        
        # Progress update
        if (i + batch_size) % 1000 == 0 or (i + batch_size) >= len(expectations):
            elapsed = time.time() - start_time
            progress = min(i + batch_size, len(expectations))
            rate = success_count / elapsed if elapsed > 0 else 0
            
            print(f"  Progress: {progress:,}/{len(expectations):,} "
                  f"({progress/len(expectations)*100:.1f}%), "
                  f"Success: {success_count:,}, Errors: {error_count:,}, "
                  f"Rate: {rate:.1f}/sec", end='\r')
    
    elapsed = time.time() - start_time
    print(f"\n  ✓ Completed: {success_count:,} records in {elapsed:.1f}s")
    
    return success_count, error_count


def run_etl(limit: Optional[int] = None):
    """Main ETL pipeline."""
    print("="*80)
    print("MATCH TEAM EXPECTATION ETL PIPELINE")
    print("="*80)
    
    # Connect to Supabase
    print("\n1. Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("  ✓ Connected")
    
    # Extract: Fetch matches with odds
    print("\n2. EXTRACT: Fetching match data...")
    matches_df = fetch_matches_with_odds(supabase, limit=limit)
    
    if matches_df.empty:
        print("  ✗ No matches with odds found!")
        return
    
    # Extract: Fetch coach data
    print("\n3. EXTRACT: Fetching coach tenure data...")
    coach_df = fetch_coach_data(supabase)
    
    # Transform: Convert matches to team expectations
    print("\n4. TRANSFORM: Calculating team expectations...")
    all_expectations = []
    
    for idx, match_row in matches_df.iterrows():
        team_expectations = transform_match_to_team_expectations(match_row, coach_df)
        all_expectations.extend(team_expectations)
        
        if (idx + 1) % 1000 == 0:
            print(f"  Processed {idx + 1:,}/{len(matches_df):,} matches...", end='\r')
    
    print(f"\n  ✓ Generated {len(all_expectations):,} team expectation records "
          f"from {len(matches_df):,} matches")
    
    # Load: Upsert to database
    print("\n5. LOAD: Upserting to match_team_expectation table...")
    success, errors = batch_upsert_expectations(supabase, all_expectations)
    
    # Summary
    print("\n" + "="*80)
    print("ETL COMPLETE!")
    print("="*80)
    print(f"Matches processed: {len(matches_df):,}")
    print(f"Team expectation records created: {len(all_expectations):,}")
    print(f"Successfully loaded: {success:,}")
    print(f"Errors: {errors:,}")
    print("="*80)


if __name__ == '__main__':
    import sys
    
    # Optional: limit for testing
    limit = None
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
        print(f"Running in TEST mode with limit={limit}")
    
    run_etl(limit=limit)
