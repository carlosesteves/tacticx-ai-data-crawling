#!/usr/bin/env python3
"""
Augment all_leagues_full.csv with Transfermarkt team information from oddscheck_to_db_mapping.csv
Adds columns: tm_home_team_name, tm_home_team_id, tm_away_team_name, tm_away_team_id
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def parse_date(date_str):
    """Parse date from various formats to YYYY-MM-DD"""
    if pd.isna(date_str) or date_str == '':
        return None
    
    date_str = str(date_str).strip()
    
    # Try different date formats
    formats = [
        '%d/%m/%Y',   # 03/08/2012
        '%d/%m/%y',   # 14/08/10
        '%Y-%m-%d',   # 2012-08-03
        '%d-%m-%Y',   # 03-08-2012
        '%m/%d/%Y',   # 08/03/2012
        '%m/%d/%y',   # 08/14/10
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If no format worked, return original
    return date_str

def augment_all_leagues():
    """Add Transfermarkt team info to all_leagues_full.csv"""
    
    # Load the files
    print("Loading all_leagues_full.csv...")
    all_leagues_df = pd.read_csv('data/all_leagues_full.csv')
    print(f"Loaded {len(all_leagues_df)} rows")
    
    print("\nLoading oddscheck_to_db_mapping.csv...")
    mapping_df = pd.read_csv('data/oddscheck_to_db_mapping.csv')
    print(f"Loaded {len(mapping_df)} mapping entries")
    
    # Create mapping dictionaries
    # For teams with multiple mappings, we'll use the one with highest average confidence
    mapping_df = mapping_df.sort_values('avg_confidence', ascending=False)
    mapping_df = mapping_df.drop_duplicates(subset='oddscheck_team_name', keep='first')
    
    team_to_id = dict(zip(mapping_df['oddscheck_team_name'], mapping_df['db_club_id']))
    team_to_name = dict(zip(mapping_df['oddscheck_team_name'], mapping_df['db_club_name']))
    
    print(f"\nCreated mapping for {len(team_to_id)} unique teams")
    
    # Add new columns
    print("\nAdding Transfermarkt columns...")
    all_leagues_df['tm_home_team_id'] = all_leagues_df['HomeTeam'].map(team_to_id)
    all_leagues_df['tm_home_team_name'] = all_leagues_df['HomeTeam'].map(team_to_name)
    all_leagues_df['tm_away_team_id'] = all_leagues_df['AwayTeam'].map(team_to_id)
    all_leagues_df['tm_away_team_name'] = all_leagues_df['AwayTeam'].map(team_to_name)
    
    # Statistics before filtering
    original_count = len(all_leagues_df)
    home_matches = all_leagues_df['tm_home_team_id'].notna().sum()
    away_matches = all_leagues_df['tm_away_team_id'].notna().sum()
    both_matches = (all_leagues_df['tm_home_team_id'].notna() & all_leagues_df['tm_away_team_id'].notna()).sum()
    
    print(f"\nMatching Statistics (before filtering):")
    print(f"  Rows with home team match: {home_matches:,} ({home_matches/original_count*100:.1f}%)")
    print(f"  Rows with away team match: {away_matches:,} ({away_matches/original_count*100:.1f}%)")
    print(f"  Rows with both teams matched: {both_matches:,} ({both_matches/original_count*100:.1f}%)")
    
    # Filter: keep only rows where both teams have matches
    print("\nFiltering to keep only rows with both teams matched...")
    all_leagues_df = all_leagues_df[
        all_leagues_df['tm_home_team_id'].notna() & 
        all_leagues_df['tm_away_team_id'].notna()
    ].copy()
    print(f"Kept {len(all_leagues_df):,} rows ({len(all_leagues_df)/original_count*100:.1f}%)")
    
    # Convert IDs to int
    print("\nConverting team IDs to integers...")
    all_leagues_df['tm_home_team_id'] = all_leagues_df['tm_home_team_id'].astype(int)
    all_leagues_df['tm_away_team_id'] = all_leagues_df['tm_away_team_id'].astype(int)
    
    # Convert FTHG and FTAG to int (handle NaN values)
    print("Converting FTHG and FTAG to integers...")
    all_leagues_df['FTHG'] = pd.to_numeric(all_leagues_df['FTHG'], errors='coerce').fillna(0).astype(int)
    all_leagues_df['FTAG'] = pd.to_numeric(all_leagues_df['FTAG'], errors='coerce').fillna(0).astype(int)
    
    # Remove unwanted columns
    print("Removing columns: Div, HTHG, HTAG, HTR, season_code...")
    columns_to_drop = ['Div', 'HTHG', 'HTAG', 'HTR', 'season_code']
    existing_columns_to_drop = [col for col in columns_to_drop if col in all_leagues_df.columns]
    if existing_columns_to_drop:
        all_leagues_df = all_leagues_df.drop(columns=existing_columns_to_drop)
        print(f"Removed {len(existing_columns_to_drop)} columns: {', '.join(existing_columns_to_drop)}")
    
    # Standardize date format
    print("Standardizing date format to YYYY-MM-DD...")
    all_leagues_df['Date'] = all_leagues_df['Date'].apply(parse_date)
    standardized_dates = all_leagues_df['Date'].notna().sum()
    print(f"Standardized {standardized_dates:,} dates")
    
    # Save augmented file
    output_file = 'data/all_leagues_full_augmented.csv'
    print(f"\nSaving augmented file to {output_file}...")
    all_leagues_df.to_csv(output_file, index=False)
    print("Done!")
    
    # Show sample of augmented data
    print("\nSample of augmented data (first 5 rows with matches):")
    sample = all_leagues_df[all_leagues_df['tm_home_team_id'].notna()].head()
    print(sample[['HomeTeam', 'tm_home_team_name', 'tm_home_team_id', 'AwayTeam', 'tm_away_team_name', 'tm_away_team_id']].to_string())

if __name__ == '__main__':
    augment_all_leagues()
