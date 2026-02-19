"""
Script to standardize dates in unique_clubs_reference_newest.csv
Handles DD/MM/YYYY, DD/MM/YY, and YYYY-MM-DD formats
"""

import pandas as pd
from datetime import datetime

def parse_and_format_date(date_str):
    """Convert various date formats to YYYY-MM-DD."""
    if pd.isna(date_str) or date_str == '':
        return ''
    
    date_str = str(date_str).strip()
    
    # Already in correct format (YYYY-MM-DD)
    if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
        try:
            # Validate it's a proper date
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except:
            pass
    
    # Try DD/MM/YYYY format (4-digit year)
    try:
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d')
    except:
        pass
    
    # Try DD/MM/YY format (2-digit year)
    try:
        dt = datetime.strptime(date_str, '%d/%m/%y')
        return dt.strftime('%Y-%m-%d')
    except:
        pass
    
    # Return as-is if cannot parse
    print(f"Warning: Could not parse date: {date_str}")
    return date_str

def main():
    input_path = '/Users/ESTE04/src/tacticx-ai-data-crawling/data/unique_clubs_reference_newest.csv'
    
    print("Reading unique_clubs_reference_newest.csv...")
    df = pd.read_csv(input_path)
    
    print(f"Total rows: {len(df)}")
    print(f"\nOriginal date format sample:")
    print(df['Date'].head(5))
    
    # Standardize date format
    df['Date'] = df['Date'].apply(parse_and_format_date)
    
    print(f"\nStandardized date format sample:")
    print(df['Date'].head(5))
    
    # Save updated CSV
    df.to_csv(input_path, index=False)
    
    print(f"\n✅ Updated file with standardized dates: {input_path}")

if __name__ == '__main__':
    main()
