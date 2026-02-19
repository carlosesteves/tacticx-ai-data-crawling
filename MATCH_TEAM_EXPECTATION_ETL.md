# Match Team Expectation ETL

Pre-computed table for analyzing team performance vs expectations using betting odds.

## Overview

Creates a `match_team_expectation` table with **one row per team per match** containing:
- Expected points (xPts) calculated from betting odds
- Actual points from match result
- Performance delta (actual - expected)
- Match difficulty classification
- Win/Draw/Loss probabilities
- Goals and coach information

## Setup

### 1. Create the Database Table

Run the SQL migration to create the table:

```sql
psql -h your-db-host -U your-user -d your-db < scripts/create_match_team_expectation_table.sql
```

Or execute the SQL in your Supabase SQL editor.

### 2. Run the ETL Pipeline

Full run (all matches with odds):
```bash
source .venv/bin/activate
python scripts/populate_match_team_expectation.py
```

Test run (limited matches):
```bash
python scripts/populate_match_team_expectation.py 100
```

## Key Calculations

### 1. Odds to Probabilities

Bookmaker odds include overround (profit margin). We remove it:

```python
p_home = (1/odds_home) / (1/odds_home + 1/odds_draw + 1/odds_away)
p_draw = (1/odds_draw) / (1/odds_home + 1/odds_draw + 1/odds_away)
p_away = (1/odds_away) / (1/odds_home + 1/odds_draw + 1/odds_away)
```

### 2. Expected Points (xPts)

```python
xPts = 3 × P(win) + 1 × P(draw) + 0 × P(loss)
```

Range: 0 to 3 points

### 3. Performance Delta

```python
delta_pts = actual_pts - xPts
```

- **Positive**: Overperformance (better than expected)
- **Negative**: Underperformance (worse than expected)
- **~0**: Performance as expected

### 4. Match Difficulty

Based on win probability:

**Home teams:**
- `high`: P(win) < 0.35 (underdog)
- `medium`: 0.35 ≤ P(win) ≤ 0.55
- `low`: P(win) > 0.55 (favorite)

**Away teams:**
- `high`: P(win) < 0.25
- `medium`: 0.25 ≤ P(win) ≤ 0.45
- `low`: P(win) > 0.45

## Example Queries

### Coach Performance vs Expectations
```sql
SELECT 
    coach_id,
    COUNT(*) as matches,
    AVG(delta_pts) as avg_overperformance,
    SUM(actual_pts) as total_points,
    SUM(x_pts) as expected_points
FROM match_team_expectation
WHERE coach_id IS NOT NULL
GROUP BY coach_id
ORDER BY avg_overperformance DESC;
```

### Team Performance by Difficulty
```sql
SELECT 
    team_id,
    difficulty,
    COUNT(*) as matches,
    AVG(delta_pts) as avg_delta,
    AVG(actual_pts) as avg_points,
    AVG(x_pts) as avg_expected
FROM match_team_expectation
GROUP BY team_id, difficulty
ORDER BY team_id, difficulty;
```

### Recent Form vs Expectations
```sql
SELECT 
    team_id,
    date,
    is_home,
    x_pts,
    actual_pts,
    delta_pts,
    difficulty
FROM match_team_expectation
WHERE team_id = 123
ORDER BY date DESC
LIMIT 10;
```

## Data Flow

```
Match table (with odds)
    ↓
[EXTRACT] Fetch matches + coach tenures
    ↓
[TRANSFORM] 
  - Convert odds → probabilities
  - Calculate xPts
  - Calculate actual points
  - Determine difficulty
  - Create 2 rows per match (home + away)
    ↓
[LOAD] Upsert to match_team_expectation
    ↓
match_team_expectation table
```

## Incremental Updates

The ETL uses upsert with conflict resolution on `(match_id, team_id)`, so you can:

1. **Run full refresh**: Reprocesses all matches
2. **Run incrementally**: Add new matches only by filtering in the EXTRACT phase

Example for incremental:
```python
# Modify fetch_matches_with_odds() to add date filter
query = supabase.table('Match').select(...).gte('date', '2025-01-01')
```

## Schema

```sql
match_team_expectation (
    match_id BIGINT,
    team_id BIGINT,
    date DATE,
    league_code VARCHAR(10),
    is_home BOOLEAN,
    coach_id BIGINT,
    x_pts NUMERIC(5,3),      -- 0.000 to 3.000
    actual_pts INTEGER,       -- 0, 1, or 3
    delta_pts NUMERIC(5,3),   -- actual - expected
    difficulty ENUM,          -- high, medium, low
    p_win NUMERIC(5,4),       -- 0.0000 to 1.0000
    p_draw NUMERIC(5,4),
    p_loss NUMERIC(5,4),
    goals_for INTEGER,
    goals_against INTEGER,
    
    UNIQUE(match_id, team_id)
)
```
