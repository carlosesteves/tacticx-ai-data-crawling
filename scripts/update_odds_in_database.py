#!/usr/bin/env python3
"""
Update Match table with odds data from all_leagues_full_augmented.csv
Matches on: date, home_club_id, away_club_id, home_team_score, away_team_score
Updates: odds_home, odds_away, odds_draw
"""

import pandas as pd
from supabase import create_client, Client
import time
import sys

# Supabase configuration (Production)
SUPABASE_URL = "https://owdayzmhxpsfpyshwtxc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93ZGF5em1oeHBzZnB5c2h3dHhjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mzc4MDAsImV4cCI6MjA2OTMxMzgwMH0.0mzxqkGi18QJXODxiXMKH5waZGruiFsi56elHxNyPks"

def retry_operation(func, max_retries=3, delay=2):
    """Retry an operation with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"  Attempt {attempt + 1} failed, retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
    return None

def update_odds_in_database():
    """Update Match records with odds data from CSV"""
    
    # Initialize Supabase client
    print("Connecting to Supabase...")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        # Test connection
        test_result = supabase.table('Match').select('tm_match_id').limit(1).execute()
        print("✓ Successfully connected to Supabase")
    except Exception as e:
        print(f"✗ Failed to connect to Supabase: {e}")
        print("\nPlease check:")
        print("  1. Your internet connection")
        print("  2. Supabase URL and API key are correct")
        sys.exit(1)
    
    # Load the augmented CSV
    print("Loading all_leagues_full_augmented.csv...")
    df = pd.read_csv('data/all_leagues_full_augmented.csv')
    print(f"Loaded {len(df):,} rows")
    
    # Filter rows that have odds data
    df_with_odds = df[df['OddsH'].notna() & df['OddsD'].notna() & df['OddsA'].notna()].copy()
    print(f"\nRows with complete odds data: {len(df_with_odds):,} ({len(df_with_odds)/len(df)*100:.1f}%)")
    
    if len(df_with_odds) == 0:
        print("No rows with complete odds data. Exiting.")
        return
    
    # Statistics
    updated_count = 0
    not_found_count = 0
    error_count = 0
    batch_size = 100
    
    print(f"\nStarting to update odds data...")
    print(f"Processing in batches of {batch_size}")
    
    start_time = time.time()
    
    for idx, row in df_with_odds.iterrows():
        try:
            # Query for matching record with retry
            def query_match():
                return supabase.table('Match').select('tm_match_id').eq(
                    'date', row['Date']
                ).eq(
                    'home_club_id', int(row['tm_home_team_id'])
                ).eq(
                    'away_club_id', int(row['tm_away_team_id'])
                ).eq(
                    'home_team_score', int(row['FTHG'])
                ).eq(
                    'away_team_score', int(row['FTAG'])
                ).execute()
            
            result = retry_operation(query_match)
            
            if result and result.data and len(result.data) > 0:
                # Found matching record(s), update with odds
                match_id = result.data[0]['tm_match_id']
                
                def update_match():
                    return supabase.table('Match').update({
                        'odds_home': float(row['OddsH']),
                        'odds_draw': float(row['OddsD']),
                        'odds_away': float(row['OddsA'])
                    }).eq('tm_match_id', match_id).execute()
                
                update_result = retry_operation(update_match)
                updated_count += 1
                
                if updated_count % batch_size == 0:
                    elapsed = time.time() - start_time
                    rate = updated_count / elapsed
                    remaining = len(df_with_odds) - updated_count
                    eta = remaining / rate if rate > 0 else 0
                    print(f"Progress: {updated_count:,}/{len(df_with_odds):,} updated "
                          f"({updated_count/len(df_with_odds)*100:.1f}%), "
                          f"{not_found_count:,} not found, {error_count:,} errors, "
                          f"Rate: {rate:.1f} rows/sec, ETA: {eta/60:.1f} min")
            else:
                not_found_count += 1
                
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Only print first few errors
                print(f"\nError processing row {idx}: {e}")
                print(f"  Date: {row['Date']}, Home: {row['HomeTeam']}, Away: {row['AwayTeam']}")
    
    # Final statistics
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Update Complete!")
    print(f"{'='*60}")
    print(f"Total rows processed: {len(df_with_odds):,}")
    print(f"Successfully updated: {updated_count:,} ({updated_count/len(df_with_odds)*100:.1f}%)")
    print(f"Not found in database: {not_found_count:,} ({not_found_count/len(df_with_odds)*100:.1f}%)")
    print(f"Errors: {error_count:,}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Average rate: {updated_count/elapsed:.1f} rows/sec")

if __name__ == '__main__':
    update_odds_in_database()
