# Incremental League Season Updates

This document describes the new incremental update feature for league seasons, which allows you to efficiently update a specific league-season combination (e.g., GB1 2025) while tracking progress and resuming from checkpoints.

## Overview

The system now supports:
- ✅ **Incremental Processing**: Only process new matches since the last run
- ✅ **State Tracking**: Track the last processed match date and game week
- ✅ **Resume Capability**: Automatically resume from the last checkpoint
- ✅ **Full Reprocessing**: Option to force full reprocessing if needed

## Database Setup

### 1. Create the State Tracking Table

Run this SQL in your Supabase SQL Editor:

```sql
-- Create league_season_state table for tracking incremental updates
CREATE TABLE IF NOT EXISTS league_season_state (
    id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    last_processed_match_date TIMESTAMP,
    last_processed_match_id INTEGER,
    total_matches_processed INTEGER DEFAULT 0,
    last_updated_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'in_progress',
    
    -- Ensure unique combination of league and season
    UNIQUE(league_id, season_id)
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_league_season_state_league_season 
    ON league_season_state(league_id, season_id);

CREATE INDEX IF NOT EXISTS idx_league_season_state_status 
    ON league_season_state(status);
```

Or run the helper script:
```bash
python scripts/create_league_season_state_table.py
```

## Usage

### Update a Specific League-Season

```bash
# Update GB1 (Premier League) for 2025 season (incremental)
python scripts/update_league_season.py --league GB1 --season 2025

# Force full reprocessing (ignores checkpoint)
python scripts/update_league_season.py --league GB1 --season 2025 --full

# Update other leagues
python scripts/update_league_season.py --league ES1 --season 2024  # La Liga
python scripts/update_league_season.py --league L1 --season 2025   # Bundesliga
python scripts/update_league_season.py --league IT1 --season 2025  # Serie A
python scripts/update_league_season.py --league FR1 --season 2025  # Ligue 1
```

### How It Works

1. **First Run**: Processes all matches chronologically, saving state after each match
2. **Subsequent Runs**: Checks the last processed match date and only processes newer matches
3. **Progress Tracking**: Updates state in database showing:
   - Last processed match ID
   - Last processed match date
   - Total matches processed
   - Status (in_progress, completed, error)

### Example Output

```
============================================================
🏆 Updating League: GB1, Season: 2025
📊 Mode: Incremental Update
============================================================

✅ League ID: 17
📊 Resuming from checkpoint: 150 matches processed
📅 Last processed match: 1234567 on 2025-11-15 00:00:00
🔍 Found 380 total matches for GB1 2025
🔄 Incremental mode: 15 new matches to process

💬 Processing match=1234568 (1/15) Date: 2025-11-22
✅ Saved match 1234568
💬 Processing match=1234569 (2/15) Date: 2025-11-23
✅ Saved match 1234569
...

✅ Season pipeline completed: 165 total matches processed

============================================================
🏁 Update complete!
============================================================
```

## Architecture

### New Components

1. **`models/league_season_state.py`**
   - Pydantic model for state tracking

2. **`repositories/league_season_state/`**
   - `league_season_state_base_repository.py` - Interface
   - `supabase_league_season_state_repository.py` - Supabase implementation
   - `fake_league_season_state_repository.py` - Testing implementation

3. **`pipelines/season_pipeline.py` (Updated)**
   - New `incremental` parameter for `run_season_pipeline()`
   - `get_matches_with_dates()` - Gets matches sorted by date
   - `update_season_state()` - Saves checkpoint state

4. **`scripts/update_league_season.py`**
   - CLI tool for updating specific league-seasons

5. **`utils/db_utils.py` (Extended)**
   - Helper functions for state management

### State Model

```python
class LeagueSeasonState(BaseModel):
    league_id: int
    season_id: int
    last_processed_match_date: Optional[datetime]
    last_processed_match_id: Optional[int]
    total_matches_processed: int
    last_updated_at: datetime
    status: str  # 'in_progress', 'completed', 'error'
```

## Integration with Existing Code

### Updated `PipelineContext`

```python
@dataclass
class PipelineContext:
    coach_repo: ICoachRepository
    match_repo: IMatchRepository
    tenure_repo: ICoachTenureRepository
    state_repo: ILeagueSeasonStateRepository  # NEW
    
    coach_cache: set[int]
    match_cache: set[int]
    tenure_cache: list[tuple[int, int, date]]
```

### Updated `run_season_pipeline()`

```python
def run_season_pipeline(
    league_id: int, 
    league_code: str, 
    season_id: int, 
    session: Session, 
    context: PipelineContext, 
    incremental: bool = False  # NEW parameter
) -> list:
    # Implementation handles incremental vs full processing
```

## Use Cases

### 1. Weekly Updates During Season

Update the current season weekly to get new match results:

```bash
# Run every Monday to get weekend's matches
python scripts/update_league_season.py --league GB1 --season 2025
```

### 2. Error Recovery

If processing fails mid-season, simply re-run to resume:

```bash
# Automatically resumes from last checkpoint
python scripts/update_league_season.py --league GB1 --season 2025
```

### 3. Data Correction

Force full reprocessing if data needs correction:

```bash
# Clears state and reprocesses everything
python scripts/update_league_season.py --league GB1 --season 2025 --full
```

### 4. Multiple Leagues

Update multiple leagues easily:

```bash
# Update all top 5 European leagues for current season
python scripts/update_league_season.py --league GB1 --season 2025
python scripts/update_league_season.py --league ES1 --season 2025
python scripts/update_league_season.py --league L1 --season 2025
python scripts/update_league_season.py --league IT1 --season 2025
python scripts/update_league_season.py --league FR1 --season 2025
```

## Monitoring

### Check State in Database

```sql
-- View all league-season states
SELECT * FROM league_season_state
ORDER BY last_updated_at DESC;

-- Check specific league-season
SELECT * FROM league_season_state
WHERE league_id = 17 AND season_id = 2025;

-- View completed seasons
SELECT * FROM league_season_state
WHERE status = 'completed';

-- View in-progress seasons
SELECT * FROM league_season_state
WHERE status = 'in_progress'
ORDER BY total_matches_processed DESC;
```

## Benefits

1. **Efficiency**: Only processes new matches, saving time and API calls
2. **Reliability**: Automatic checkpointing means you can resume after failures
3. **Flexibility**: Easy to update specific leagues without reprocessing everything
4. **Tracking**: Clear visibility into what's been processed
5. **Data Freshness**: Keep current season data up-to-date throughout the season

## Common League Codes

- `GB1` - Premier League (England)
- `ES1` - La Liga (Spain)
- `L1` - Bundesliga (Germany)
- `IT1` - Serie A (Italy)
- `FR1` - Ligue 1 (France)
- `PO1` - Primeira Liga (Portugal)
- `NL1` - Eredivisie (Netherlands)
- `TR1` - Süper Lig (Turkey)

## Troubleshooting

### "No matches found"
- Verify the league code is correct
- Check if the season exists in your database

### "League ID not found"
- Ensure the league exists in your League table
- Check the `tm_code` matches the `--league` parameter

### State not updating
- Verify the `league_season_state` table exists
- Check database permissions
- Look for errors in the console output

## Future Enhancements

Possible improvements:
- Game week tracking (when available in source data)
- Parallel processing for multiple matches
- Notification system for errors
- Dashboard for monitoring all league-season states
- Automated scheduling with cron jobs
