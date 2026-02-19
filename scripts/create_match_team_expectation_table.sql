-- Create match_team_expectation table for pre-computed performance metrics
-- One row per team per match

CREATE TYPE difficulty_level AS ENUM ('high', 'medium', 'low');

CREATE TABLE IF NOT EXISTS match_team_expectation (
    id BIGSERIAL PRIMARY KEY,
    
    -- Match and team identifiers
    match_id BIGINT NOT NULL,
    date DATE NOT NULL,
    league_code VARCHAR(10),
    team_id BIGINT NOT NULL,
    is_home BOOLEAN NOT NULL,
    coach_id BIGINT,
    
    -- Expected vs actual points
    x_pts NUMERIC(5, 3) NOT NULL,  -- Expected points (0-3)
    actual_pts INTEGER NOT NULL,    -- Actual points (0, 1, or 3)
    delta_pts NUMERIC(5, 3) NOT NULL,  -- actual_pts - x_pts
    
    -- Match difficulty
    difficulty difficulty_level,
    
    -- Win/Draw/Loss probabilities
    p_win NUMERIC(5, 4) NOT NULL,
    p_draw NUMERIC(5, 4) NOT NULL,
    p_loss NUMERIC(5, 4) NOT NULL,
    
    -- Goals
    goals_for INTEGER NOT NULL,
    goals_against INTEGER NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT match_team_expectation_unique UNIQUE (match_id, team_id),
    CONSTRAINT match_team_expectation_match_fk FOREIGN KEY (match_id) REFERENCES "Match"(tm_match_id) ON DELETE CASCADE,
    CONSTRAINT match_team_expectation_team_fk FOREIGN KEY (team_id) REFERENCES "Club"(tm_club_id) ON DELETE CASCADE,
    CONSTRAINT match_team_expectation_coach_fk FOREIGN KEY (coach_id) REFERENCES "Coach"(tm_coach_id) ON DELETE SET NULL,
    
    -- Validation constraints
    CONSTRAINT x_pts_range CHECK (x_pts >= 0 AND x_pts <= 3),
    CONSTRAINT actual_pts_valid CHECK (actual_pts IN (0, 1, 3)),
    CONSTRAINT probabilities_sum CHECK (ABS(p_win + p_draw + p_loss - 1.0) < 0.01),
    CONSTRAINT probabilities_valid CHECK (
        p_win >= 0 AND p_win <= 1 AND
        p_draw >= 0 AND p_draw <= 1 AND
        p_loss >= 0 AND p_loss <= 1
    ),
    CONSTRAINT goals_non_negative CHECK (goals_for >= 0 AND goals_against >= 0)
);

-- Indexes for query performance
CREATE INDEX idx_match_team_expectation_match ON match_team_expectation(match_id);
CREATE INDEX idx_match_team_expectation_team ON match_team_expectation(team_id);
CREATE INDEX idx_match_team_expectation_coach ON match_team_expectation(coach_id);
CREATE INDEX idx_match_team_expectation_date ON match_team_expectation(date);
CREATE INDEX idx_match_team_expectation_league ON match_team_expectation(league_code);
CREATE INDEX idx_match_team_expectation_team_date ON match_team_expectation(team_id, date);
CREATE INDEX idx_match_team_expectation_coach_date ON match_team_expectation(coach_id, date) WHERE coach_id IS NOT NULL;

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_match_team_expectation_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER match_team_expectation_updated_at
    BEFORE UPDATE ON match_team_expectation
    FOR EACH ROW
    EXECUTE FUNCTION update_match_team_expectation_updated_at();

-- Grant permissions (adjust as needed for your setup)
GRANT SELECT, INSERT, UPDATE, DELETE ON match_team_expectation TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE match_team_expectation_id_seq TO anon, authenticated;

COMMENT ON TABLE match_team_expectation IS 'Pre-computed team performance metrics vs expectations from betting odds';
COMMENT ON COLUMN match_team_expectation.x_pts IS 'Expected points calculated from betting odds probabilities';
COMMENT ON COLUMN match_team_expectation.delta_pts IS 'Performance delta: actual_pts - x_pts (positive = overperformance)';
COMMENT ON COLUMN match_team_expectation.difficulty IS 'Match difficulty based on opponent strength/odds';
