#!/usr/bin/env python3
"""
Batch update Match table with odds data from all_leagues_full_augmented.csv
Uses efficient batch processing and logs all matches/non-matches
"""

import pandas as pd
from supabase import create_client, Client
import time
from datetime import datetime

# Supabase configuration (Production)
SUPABASE_URL = "https://owdayzmhxpsfpyshwtxc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93ZGF5em1oeHBzZnB5c2h3dHhjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mzc4MDAsImV4cCI6MjA2OTMxMzgwMH0.0mzxqkGi18QJXODxiXMKH5waZGruiFsi56elHxNyPks"

def batch_update_odds():
    """Batch update Match records with odds data"""
    
    print("="*80)
    print("BATCH ODDS UPDATE WITH LOGGING")
    print("="*80)
    
    # Initialize Supabase client
    print("\n1. Connecting to Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("   ✓ Connected")
    
    # Load CSV data
    print("\n2. Loading all_leagues_full_augmented.csv...")
    csv_df = pd.read_csv('data/all_leagues_full_augmented.csv')
    print(f"   ✓ Loaded {len(csv_df):,} rows")
    
    # Filter rows with complete odds
    csv_df = csv_df[csv_df['OddsH'].notna() & csv_df['OddsD'].notna() & csv_df['OddsA'].notna()].copy()
    print(f"   ✓ {len(csv_df):,} rows have complete odds data")
    
    # Fetch all Match data from database
    print("\n3. Fetching Match data from database...")
    print("   This may take a moment...")
    
    # Fetch in batches to avoid timeouts
    all_matches = []
    batch_size = 1000
    offset = 0
    
    while True:
        result = supabase.table('Match').select(
            'tm_match_id, date, home_club_id, away_club_id, home_team_score, away_team_score, odds_home, odds_draw, odds_away'
        ).range(offset, offset + batch_size - 1).execute()
        
        if not result.data:
            break
        
        all_matches.extend(result.data)
        offset += batch_size
        print(f"   Fetched {len(all_matches):,} matches...", end='\r')
    
    print(f"\n   ✓ Fetched {len(all_matches):,} matches from database")
    
    # Convert to DataFrame
    db_df_all = pd.DataFrame(all_matches)
    
    # Create a copy for matches with odds (for filtering unmatched later)
    db_df_with_odds = db_df_all[
        db_df_all['odds_home'].notna() & 
        db_df_all['odds_draw'].notna() & 
        db_df_all['odds_away'].notna()
    ].copy()
    
    # Filter to matches that need odds data
    before_filter = len(db_df_all)
    db_df = db_df_all[
        db_df_all['odds_home'].isna() | 
        db_df_all['odds_draw'].isna() | 
        db_df_all['odds_away'].isna()
    ].copy()
    matches_with_odds = before_filter - len(db_df)
    print(f"   ✓ Filtered out {matches_with_odds:,} matches that already have complete odds")
    print(f"   ✓ {len(db_df):,} matches need odds data")
    
    if len(db_df) == 0:
        print("\n✓ All matches already have odds data! Nothing to update.")
        return
    
    # Create composite key for matching
    print("\n4. Creating match keys...")
    csv_df['match_key'] = (
        csv_df['Date'].astype(str) + '|' +
        csv_df['tm_home_team_id'].astype(str) + '|' +
        csv_df['tm_away_team_id'].astype(str) + '|' +
        csv_df['FTHG'].astype(str) + '|' +
        csv_df['FTAG'].astype(str)
    )
    
    db_df['match_key'] = (
        db_df['date'].astype(str) + '|' +
        db_df['home_club_id'].astype(str) + '|' +
        db_df['away_club_id'].astype(str) + '|' +
        db_df['home_team_score'].astype(str) + '|' +
        db_df['away_team_score'].astype(str)
    )
    
    # Create lookup dictionary
    db_lookup = db_df.set_index('match_key')['tm_match_id'].to_dict()
    print(f"   ✓ Created lookup for {len(db_lookup):,} unique match keys")
    
    # Debug: Show sample keys
    print(f"\n   Sample database keys (without odds):")
    for i, key in enumerate(list(db_lookup.keys())[:5]):
        print(f"     {i+1}. {key}")
    
    print(f"\n   Sample CSV keys:")
    for i, key in enumerate(csv_df['match_key'].head(5)):
        print(f"     {i+1}. {key}")
    
    # Match CSV rows to database
    print("\n5. Matching CSV rows to database records...")
    csv_df['tm_match_id'] = csv_df['match_key'].map(db_lookup)
    csv_df['match_status'] = csv_df['tm_match_id'].notna()
    
    matched_df = csv_df[csv_df['match_status']].copy()
    unmatched_df = csv_df[~csv_df['match_status']].copy()
    
    # Filter out CSV rows that would match DB records already with complete odds
    if len(db_df_with_odds) > 0:
        db_with_odds_keys = set(
            db_df_with_odds['date'].astype(str) + '|' +
            db_df_with_odds['home_club_id'].astype(str) + '|' +
            db_df_with_odds['away_club_id'].astype(str) + '|' +
            db_df_with_odds['home_team_score'].astype(str) + '|' +
            db_df_with_odds['away_team_score'].astype(str)
        )
        rows_matching_complete = unmatched_df['match_key'].isin(db_with_odds_keys).sum()
        unmatched_df = unmatched_df[~unmatched_df['match_key'].isin(db_with_odds_keys)]
        print(f"   ✓ Excluded {rows_matching_complete:,} CSV rows that match DB records with complete odds")
    
    print(f"   ✓ Matched: {len(matched_df):,} ({len(matched_df)/len(csv_df)*100:.1f}%)")
    print(f"   ✓ Unmatched (need attention): {len(unmatched_df):,} ({len(unmatched_df)/len(csv_df)*100:.1f}%)")
    
    # Save match results
    print("\n6. Saving match results...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save matched records
    matched_log = matched_df[[
        'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG',
        'tm_home_team_id', 'tm_away_team_id', 'tm_match_id',
        'OddsH', 'OddsD', 'OddsA'
    ]].copy()
    matched_log['status'] = 'MATCHED'
    matched_log_file = f'data/odds_update_matched_{timestamp}.csv'
    matched_log.to_csv(matched_log_file, index=False)
    print(f"   ✓ Saved matched records to {matched_log_file}")
    
    # Save unmatched records with analysis
    if len(unmatched_df) > 0:
        # Analyze unmatched reasons
        unmatched_df['reason'] = 'No matching record in database'
        
        # Check if teams exist
        db_home_teams = set(db_df['home_club_id'].unique())
        db_away_teams = set(db_df['away_club_id'].unique())
        
        unmatched_df['home_team_exists'] = unmatched_df['tm_home_team_id'].isin(db_home_teams)
        unmatched_df['away_team_exists'] = unmatched_df['tm_away_team_id'].isin(db_away_teams)
        
        # Refine reasons
        unmatched_df.loc[~unmatched_df['home_team_exists'], 'reason'] = 'Home team not in database'
        unmatched_df.loc[~unmatched_df['away_team_exists'], 'reason'] = 'Away team not in database'
        unmatched_df.loc[~unmatched_df['home_team_exists'] & ~unmatched_df['away_team_exists'], 'reason'] = 'Both teams not in database'
        
        unmatched_log = unmatched_df[[
            'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG',
            'tm_home_team_id', 'tm_away_team_id', 'reason'
        ]].copy()
        unmatched_log['status'] = 'UNMATCHED'
        unmatched_log_file = f'data/odds_update_unmatched_{timestamp}.csv'
        unmatched_log.to_csv(unmatched_log_file, index=False)
        print(f"   ✓ Saved unmatched records to {unmatched_log_file}")
        
        # Print reason breakdown
        print("\n   Unmatched breakdown:")
        reason_counts = unmatched_df['reason'].value_counts()
        for reason, count in reason_counts.items():
            print(f"     - {reason}: {count:,}")
    
    # Batch update matched records
    if len(matched_df) == 0:
        print("\n✗ No matches found to update!")
        return
    
    print(f"\n7. Updating {len(matched_df):,} matches in database...")
    print("   Processing in batches...")
    
    updated_count = 0
    error_count = 0
    batch_size = 50  # Smaller batches for stability
    
    start_time = time.time()
    
    for i in range(0, len(matched_df), batch_size):
        batch = matched_df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            try:
                supabase.table('Match').update({
                    'odds_home': float(row['OddsH']),
                    'odds_draw': float(row['OddsD']),
                    'odds_away': float(row['OddsA'])
                }).eq('tm_match_id', int(row['tm_match_id'])).execute()
                
                updated_count += 1
                
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"\n   Error updating match {row['tm_match_id']}: {e}")
        
        # Progress update
        if (i + batch_size) % 500 == 0 or (i + batch_size) >= len(matched_df):
            elapsed = time.time() - start_time
            progress = min(i + batch_size, len(matched_df))
            rate = updated_count / elapsed if elapsed > 0 else 0
            remaining = len(matched_df) - progress
            eta = remaining / rate if rate > 0 else 0
            
            print(f"   Progress: {progress:,}/{len(matched_df):,} "
                  f"({progress/len(matched_df)*100:.1f}%), "
                  f"Updated: {updated_count:,}, Errors: {error_count:,}, "
                  f"Rate: {rate:.1f}/sec, ETA: {eta/60:.1f}min")
    
    # Final summary
    elapsed = time.time() - start_time
    print("\n" + "="*80)
    print("UPDATE COMPLETE!")
    print("="*80)
    print(f"Total CSV rows processed: {len(csv_df):,}")
    print(f"Database matches already with odds: {matches_with_odds:,}")
    print(f"Matched to database: {len(matched_df):,} ({len(matched_df)/len(csv_df)*100:.1f}%)")
    print(f"Successfully updated: {updated_count:,}")
    print(f"Errors: {error_count:,}")
    print(f"Not matched: {len(unmatched_df):,} ({len(unmatched_df)/len(csv_df)*100:.1f}%)")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Average rate: {updated_count/elapsed:.1f} updates/sec")
    print(f"\nLog files:")
    print(f"  - Matched: {matched_log_file}")
    if len(unmatched_df) > 0:
        print(f"  - Unmatched: {unmatched_log_file}")
    print("="*80)

if __name__ == '__main__':
    batch_update_odds()
