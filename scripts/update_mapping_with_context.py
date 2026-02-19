#!/usr/bin/env python3
"""
Update existing team name mapping CSV to include country and tier information.

This script reads an existing team_name_mapping file and adds country, tier,
and league context columns based on the league_code using the football_data_league_mapping.

Usage:
    python scripts/update_mapping_with_context.py \\
        --input data/team_name_mapping_auto.csv \\
        --output data/team_name_mapping_with_context.csv
"""

import argparse
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.football_data_league_mapping import get_league_info


def update_mapping_with_context(input_file: str, output_file: str):
    """
    Update team name mapping to include league context (country, tier, etc.)
    """
    print(f"📥 Reading mapping from {input_file}...")
    df = pd.read_csv(input_file)
    print(f"   Loaded {len(df)} team mappings")
    
    # Add context columns if they don't exist
    if 'tm_code' not in df.columns:
        df['tm_code'] = None
    if 'country' not in df.columns:
        df['country'] = None
    if 'tier' not in df.columns:
        df['tier'] = None
    if 'league_name' not in df.columns:
        df['league_name'] = None
    if 'database_club_country' not in df.columns:
        df['database_club_country'] = None
    
    # Update with league information
    print("🔄 Adding league context information...")
    leagues_found = set()
    leagues_not_found = set()
    
    for idx, row in df.iterrows():
        league_code = row['league_code']
        
        # Get league info from mapping
        league_info = get_league_info(league_code)
        
        if league_info['tm_code']:
            leagues_found.add(league_code)
            df.at[idx, 'tm_code'] = league_info['tm_code']
            df.at[idx, 'country'] = league_info['country']
            df.at[idx, 'tier'] = league_info['tier']
            df.at[idx, 'league_name'] = league_info['full_name']
        else:
            leagues_not_found.add(league_code)
    
    print(f"✅ Updated {len(df)} team mappings")
    print(f"   Leagues found: {len(leagues_found)}")
    if leagues_not_found:
        print(f"   ⚠️  Leagues not in mapping: {sorted(leagues_not_found)}")
        print(f"      These leagues need to be added to config/football_data_league_mapping.py")
    
    # Reorder columns for better readability
    column_order = [
        'league_code',
        'tm_code', 
        'country',
        'tier',
        'league_name',
        'football_data_name',
        'normalized_name',
        'tm_club_id',
        'database_club_name',
        'database_club_country',
        'match_score',
        'match_method',
        'confidence'
    ]
    
    # Only reorder columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in column_order]
    final_column_order = existing_columns + other_columns
    
    df = df[final_column_order]
    
    # Save updated mapping
    df.to_csv(output_file, index=False)
    print(f"\n💾 Updated mapping saved to: {output_file}")
    
    # Show statistics
    print(f"\n📊 Statistics by country:")
    if 'country' in df.columns and df['country'].notna().any():
        country_stats = df.groupby('country').agg({
            'tm_club_id': lambda x: x.notna().sum(),
            'football_data_name': 'count'
        }).rename(columns={'tm_club_id': 'mapped', 'football_data_name': 'total'})
        country_stats['percentage'] = (country_stats['mapped'] / country_stats['total'] * 100).round(1)
        country_stats = country_stats.sort_values('total', ascending=False)
        print(country_stats.head(15).to_string())
    
    print(f"\n📊 Statistics by tier:")
    if 'tier' in df.columns and df['tier'].notna().any():
        tier_stats = df.groupby('tier').agg({
            'tm_club_id': lambda x: x.notna().sum(),
            'football_data_name': 'count'
        }).rename(columns={'tm_club_id': 'mapped', 'football_data_name': 'total'})
        tier_stats['percentage'] = (tier_stats['mapped'] / tier_stats['total'] * 100).round(1)
        tier_stats = tier_stats.sort_values('tier')
        print(tier_stats.to_string())
    
    # Show samples of unmapped teams by country
    print(f"\n⚠️  Sample of unmapped teams:")
    unmapped = df[df['tm_club_id'].isna()]
    if len(unmapped) > 0:
        for country in unmapped['country'].unique()[:5]:
            if pd.notna(country):
                country_unmapped = unmapped[unmapped['country'] == country]
                print(f"\n   {country} (Tier {country_unmapped['tier'].iloc[0]}):")
                for _, row in country_unmapped.head(3).iterrows():
                    print(f"      • {row['football_data_name']}")


def main():
    parser = argparse.ArgumentParser(
        description='Update team name mapping with league context (country, tier)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--input',
        default='data/team_name_mapping_auto.csv',
        help='Input mapping CSV file'
    )
    
    parser.add_argument(
        '--output',
        default='data/team_name_mapping_with_context.csv',
        help='Output mapping CSV file with context'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"❌ Error: Input file not found: {args.input}")
        sys.exit(1)
    
    update_mapping_with_context(args.input, args.output)
    
    print(f"\n💡 Next steps:")
    print(f"   1. Review unmapped teams in {args.output}")
    print(f"   2. For teams not matched, search manually in your database")
    print(f"   3. Update tm_club_id for unmatched teams")
    print(f"   4. Use {args.output} for matching football data to database")


if __name__ == "__main__":
    main()
