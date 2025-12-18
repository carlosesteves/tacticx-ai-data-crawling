"""
SQL Migration Script for League Season State Tracking

This script should be run in your Supabase SQL Editor to create the 
league_season_state table for tracking incremental updates.
"""

CREATE_TABLE_SQL = """
-- Create league_season_state table for tracking incremental updates
CREATE TABLE IF NOT EXISTS league_season_state (
    id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    last_processed_match_date TIMESTAMP,
    last_processed_match_id INTEGER,
    total_matches_processed INTEGER DEFAULT 0,
    failed_match_ids INTEGER[] DEFAULT '{}',
    last_updated_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(30) DEFAULT 'in_progress',
    
    -- Ensure unique combination of league and season
    UNIQUE(league_id, season_id)
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_league_season_state_league_season 
    ON league_season_state(league_id, season_id);

CREATE INDEX IF NOT EXISTS idx_league_season_state_status 
    ON league_season_state(status);

-- Add comments for documentation
COMMENT ON TABLE league_season_state IS 'Tracks the processing state of each league-season combination for incremental updates';
COMMENT ON COLUMN league_season_state.league_id IS 'Foreign key to League table';
COMMENT ON COLUMN league_season_state.season_id IS 'The season year (e.g., 2025)';
COMMENT ON COLUMN league_season_state.last_processed_match_date IS 'Date of the last successfully processed match';
COMMENT ON COLUMN league_season_state.last_processed_match_id IS 'ID of the last successfully processed match';
COMMENT ON COLUMN league_season_state.total_matches_processed IS 'Total count of matches processed for this league-season';
COMMENT ON COLUMN league_season_state.failed_match_ids IS 'Array of match IDs that failed to process';
COMMENT ON COLUMN league_season_state.last_updated_at IS 'Timestamp of last state update';
COMMENT ON COLUMN league_season_state.status IS 'Status: in_progress, completed, completed_with_errors, or error';
"""

if __name__ == "__main__":
    print("Copy and paste the following SQL into your Supabase SQL Editor:\n")
    print(CREATE_TABLE_SQL)
