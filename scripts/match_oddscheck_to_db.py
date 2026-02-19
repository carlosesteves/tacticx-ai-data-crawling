"""
Match team names from all_leagues_full.csv (oddscheck data) to database club names.

This script:
1. Extracts all unique club names from all_leagues_full.csv
2. For each match, queries the database for matches on the same date and country
3. Uses score matching and fuzzy string matching to find the correct clubs
4. Stores high-confidence matches in a mapping file
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from thefuzz import fuzz
from collections import defaultdict
import sys
import os
from supabase import create_client, Client
import csv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.football_data_league_mapping import get_league_info

# Supabase credentials
SUPABASE_URL = 'https://owdayzmhxpsfpyshwtxc.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93ZGF5em1oeHBzZnB5c2h3dHhjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM3Mzc4MDAsImV4cCI6MjA2OTMxMzgwMH0.0mzxqkGi18QJXODxiXMKH5waZGruiFsi56elHxNyPks'


def get_supabase_client():
    """Create Supabase client."""
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def normalize_team_name(name):
    """Normalize team name for better matching."""
    if pd.isna(name):
        return ""
    return str(name).strip().lower()


def parse_date(date_str):
    """Parse date from various formats in the CSV."""
    if pd.isna(date_str):
        return None
    
    # Try different date formats (including 2-digit year format)
    formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%y', '%m/%d/%y']
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except ValueError:
            continue
    
    return None


def calculate_match_confidence(odds_home, odds_away, odds_score_home, odds_score_away,
                               db_home, db_away, db_score_home, db_score_away):
    """
    Calculate confidence score for a match.
    
    Returns a score between 0 and 100, where:
    - Score match: 50 points
    - Home team fuzzy match: 25 points
    - Away team fuzzy match: 25 points
    """
    confidence = 0
    
    # Score matching (50 points)
    if (odds_score_home == db_score_home and odds_score_away == db_score_away):
        confidence += 50
    
    # Home team fuzzy matching (25 points max)
    home_ratio = fuzz.ratio(normalize_team_name(odds_home), normalize_team_name(db_home))
    confidence += (home_ratio / 100) * 25
    
    # Away team fuzzy matching (25 points max)
    away_ratio = fuzz.ratio(normalize_team_name(odds_away), normalize_team_name(db_away))
    confidence += (away_ratio / 100) * 25
    
    return confidence, home_ratio, away_ratio


def match_clubs_to_database(min_confidence=70, resume=True):
    """
    Match clubs from all_leagues_full.csv to database clubs.
    
    Args:
        min_confidence: Minimum confidence score (0-100) to accept a match
        resume: Whether to resume from last checkpoint
    """
    # Setup paths
    data_dir = Path(__file__).parent.parent / "data"
    input_file = data_dir / "all_leagues_full.csv"
    output_file = data_dir / "oddscheck_to_db_mapping.csv"
    checkpoint_file = data_dir / "oddscheck_mapping_checkpoint.txt"
    
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Initialize Supabase client
    print("Connecting to database...")
    supabase = get_supabase_client()
    
    # Cache league IDs by country to avoid repeated queries
    print("Fetching league data...")
    leagues_response = supabase.table('League').select('tm_league_id, country').execute()
    country_to_league_ids = defaultdict(list)
    for league in leagues_response.data:
        country_to_league_ids[league['country']].append(league['tm_league_id'])
    print(f"Loaded {len(leagues_response.data)} leagues from {len(country_to_league_ids)} countries")
    
    # Track mappings: {odds_team_name: {db_club_id, db_club_name, confidence, count}}
    mappings = defaultdict(lambda: {'matches': []})
    teams_with_mapping = set()  # Track teams that have any mapping (written or not)
    confident_teams = set()  # Track teams we're confident about
    
    # Check for existing output and load already mapped teams
    start_row = 0
    if resume and output_file.exists():
        print(f"Loading existing mappings from {output_file}...")
        existing_df = pd.read_csv(output_file)
        teams_with_mapping.update(existing_df['oddscheck_team_name'].unique())
        confident_teams.update(
            existing_df[existing_df['avg_confidence'] >= 85]['oddscheck_team_name'].unique()
        )
        print(f"Found {len(teams_with_mapping)} unique teams already mapped")
        print(f"Found {len(confident_teams)} confident teams (≥85% confidence)")
        
        # Load checkpoint
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                start_row = int(f.read().strip())
            print(f"Resuming from row {start_row}")
        else:
            print(f"No checkpoint found, will process all matches but skip mapped teams")
    
    # Open CSV file for appending (or writing if new)
    mode = 'a' if (resume and output_file.exists()) else 'w'
    print(f"Opening output file: {output_file} (mode: {mode})")
    csv_file = open(output_file, mode, newline='', encoding='utf-8')
    csv_writer = csv.DictWriter(csv_file, fieldnames=[
        'oddscheck_team_name', 'db_club_id', 'db_club_name', 
        'match_count', 'avg_confidence', 'avg_fuzzy_score'
    ])
    if mode == 'w':
        csv_writer.writeheader()
    csv_file.flush()
    
    # Extract all unique clubs
    all_clubs = set()
    all_clubs.update(df['HomeTeam'].dropna().unique())
    all_clubs.update(df['AwayTeam'].dropna().unique())
    print(f"Found {len(all_clubs)} unique clubs in oddscheck data")
    
    # Process each match
    total_matches = len(df)
    processed = 0
    matches_found = 0
    total_db_matches = 0
    high_confidence_matches = 0
    skipped_matches = 0
    
    print(f"\nProcessing {total_matches} matches (starting from row {start_row})...")
    
    for idx, row in df.iterrows():
        # Skip rows before checkpoint
        if idx < start_row:
            continue
            
        processed += 1
        if processed % 100 == 0:
            print(f"Processed {processed}/{total_matches - start_row} | DB queries: {total_db_matches} | High conf: {high_confidence_matches} | Skipped: {skipped_matches} | Mapped teams: {len(teams_with_mapping)}")
            # Save checkpoint
            with open(checkpoint_file, 'w') as f:
                f.write(str(idx + 1))
        
        # Parse match data
        match_date = parse_date(row['Date'])
        if not match_date:
            continue
        
        home_team = row['HomeTeam']
        away_team = row['AwayTeam']
        
        # Skip if both teams already have mappings
        if home_team in teams_with_mapping and away_team in teams_with_mapping:
            skipped_matches += 1
            continue
        league_code = row.get('league_code')
        
        # Get scores
        try:
            home_score = int(row['FTHG']) if pd.notna(row['FTHG']) else None
            away_score = int(row['FTAG']) if pd.notna(row['FTAG']) else None
        except (ValueError, TypeError):
            continue
        
        if home_score is None or away_score is None:
            continue
        
        # Get country from league code
        league_info = get_league_info(league_code) if league_code else {}
        country = league_info.get('country')
        tm_code = league_info.get('tm_code')
        
        if not country:
            continue
        
        # Query database for matches on the same date
        try:
            # Search for matches in the database  
            query = supabase.table('Match').select(
                'tm_match_id, date, home_team_score, away_team_score, home_club_id, away_club_id, league_id'
            ).eq('date', str(match_date))
            
            response = query.execute()
            
            if not response.data:
                continue
            
            # Filter by country if we have it
            filtered_matches = response.data
            if country and country in country_to_league_ids:
                league_ids = country_to_league_ids[country]
                filtered_matches = [m for m in response.data if m.get('league_id') in league_ids]
            
            if not filtered_matches:
                continue
                
            total_db_matches += len(filtered_matches)
            
            # Get club info for all clubs in these matches
            club_ids = set()
            for m in filtered_matches:
                if m.get('home_club_id'):
                    club_ids.add(m['home_club_id'])
                if m.get('away_club_id'):
                    club_ids.add(m['away_club_id'])
            
            if not club_ids:
                continue
            
            # Fetch club names
            clubs_response = supabase.table('Club').select('tm_club_id, name').in_('tm_club_id', list(club_ids)).execute()
            clubs_dict = {c['tm_club_id']: c['name'] for c in clubs_response.data}
            
            # Check each database match
            for db_match in filtered_matches:
                db_home_id = db_match.get('home_club_id')
                db_away_id = db_match.get('away_club_id')
                
                if not db_home_id or not db_away_id:
                    continue
                
                db_home_name = clubs_dict.get(db_home_id, '')
                db_away_name = clubs_dict.get(db_away_id, '')
                db_home_score = db_match.get('home_team_score')
                db_away_score = db_match.get('away_team_score')
                
                # Calculate confidence
                confidence, home_ratio, away_ratio = calculate_match_confidence(
                    home_team, away_team, home_score, away_score,
                    db_home_name, db_away_name, db_home_score, db_away_score
                )
                
                if confidence >= min_confidence:
                    high_confidence_matches += 1
                    
                    # Store home team mapping
                    mappings[home_team]['matches'].append({
                        'db_club_id': db_home_id,
                        'db_club_name': db_home_name,
                        'confidence': confidence,
                        'fuzzy_score': home_ratio,
                        'match_date': str(match_date)
                    })
                    
                    # Store away team mapping
                    mappings[away_team]['matches'].append({
                        'db_club_id': db_away_id,
                        'db_club_name': db_away_name,
                        'confidence': confidence,
                        'fuzzy_score': away_ratio,
                        'match_date': str(match_date)
                    })
                    
                    # Write to CSV if we have enough data for these teams
                    for team_name in [home_team, away_team]:
                        # Skip if already has mapping
                        if team_name in teams_with_mapping:
                            continue
                            
                        if len(mappings[team_name]['matches']) >= 3:
                            # Calculate averages
                            club_matches = defaultdict(list)
                            for match in mappings[team_name]['matches']:
                                club_id = match['db_club_id']
                                club_matches[club_id].append(match)
                            
                            # Write the most common mapping
                            for club_id, matches in club_matches.items():
                                if len(matches) >= 3:  # Only write if we have at least 3 matches
                                    avg_confidence = sum(m['confidence'] for m in matches) / len(matches)
                                    avg_fuzzy = sum(m['fuzzy_score'] for m in matches) / len(matches)
                                    db_club_name = matches[0]['db_club_name']
                                    
                                    csv_writer.writerow({
                                        'oddscheck_team_name': team_name,
                                        'db_club_id': club_id,
                                        'db_club_name': db_club_name,
                                        'match_count': len(matches),
                                        'avg_confidence': round(avg_confidence, 2),
                                        'avg_fuzzy_score': round(avg_fuzzy, 2)
                                    })
                                    csv_file.flush()
                                    teams_with_mapping.add(team_name)
                                    
                                    # Mark team as confident if avg confidence is high enough
                                    if avg_confidence >= 85:
                                        confident_teams.add(team_name)
        
        except Exception as e:
            if processed % 100 == 0:  # Only log errors periodically
                print(f"Error processing match at row {idx}: {e}")
            continue
    
    print(f"\nProcessing complete!")
    print(f"Total matches processed: {processed}")
    print(f"Matches skipped (both teams mapped): {skipped_matches}")
    print(f"Total DB matches queried: {total_db_matches}")
    print(f"High confidence matches: {high_confidence_matches}")
    print(f"Teams with mapping: {len(teams_with_mapping)}")
    print(f"Teams with confident mapping: {len(confident_teams)}")
    print(f"Found mappings for {len(mappings)} teams.")
    
    # Write remaining teams that haven't been written yet
    print(f"\nWriting remaining teams to CSV...")
    remaining_written = 0
    for odds_name, data in mappings.items():
        if not data['matches'] or odds_name in teams_with_mapping:
            continue
        
        # Group by club_id and aggregate
        club_matches = defaultdict(list)
        for match in data['matches']:
            club_id = match['db_club_id']
            club_matches[club_id].append(match)
        
        # Write any remaining mappings
        for club_id, matches in club_matches.items():
            avg_confidence = sum(m['confidence'] for m in matches) / len(matches)
            avg_fuzzy = sum(m['fuzzy_score'] for m in matches) / len(matches)
            db_club_name = matches[0]['db_club_name']
            
            csv_writer.writerow({
                'oddscheck_team_name': odds_name,
                'db_club_id': club_id,
                'db_club_name': db_club_name,
                'match_count': len(matches),
                'avg_confidence': round(avg_confidence, 2),
                'avg_fuzzy_score': round(avg_fuzzy, 2)
            })
            remaining_written += 1
            teams_with_mapping.add(odds_name)
    
    csv_file.close()
    print(f"Wrote {remaining_written} remaining teams to CSV")
    
    # Clear checkpoint file since we're done
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        print(f"Cleared checkpoint file")
    
    # Read back and print statistics
    result_df = pd.read_csv(output_file)
    
    # Print statistics
    print(f"\nStatistics:")
    print(f"Total unique clubs in oddscheck data: {len(all_clubs)}")
    print(f"Clubs matched to database: {len(result_df)}")
    print(f"Match rate: {len(result_df)/len(all_clubs)*100:.1f}%")
    print(f"\nConfidence distribution:")
    print(f"  90-100: {len(result_df[result_df['avg_confidence'] >= 90])}")
    print(f"  80-89:  {len(result_df[(result_df['avg_confidence'] >= 80) & (result_df['avg_confidence'] < 90)])}")
    print(f"  70-79:  {len(result_df[(result_df['avg_confidence'] >= 70) & (result_df['avg_confidence'] < 80)])}")
    
    print(f"\nMapping saved to: {output_file}")
    
    return result_df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Match oddscheck team names to database clubs')
    parser.add_argument('--min-confidence', type=int, default=70,
                       help='Minimum confidence score (0-100) to accept a match (default: 70)')
    parser.add_argument('--no-resume', action='store_true',
                       help='Start from beginning, ignoring checkpoint (default: resume from checkpoint)')
    
    args = parser.parse_args()
    
    match_clubs_to_database(min_confidence=args.min_confidence, resume=not args.no_resume)
