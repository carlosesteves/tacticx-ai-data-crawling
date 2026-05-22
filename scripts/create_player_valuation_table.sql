-- Stores point-in-time player market valuations
-- This table is populated by scripts/scrape_coach_player_valuation_history.py

CREATE TABLE IF NOT EXISTS "Player_valuation_history" (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL,
    club_id BIGINT,
    valuation_date DATE NOT NULL,
    market_value_eur BIGINT NOT NULL,
    currency VARCHAR(8) DEFAULT 'EUR',
    age INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT player_valuation_player_fk
        FOREIGN KEY (player_id) REFERENCES "Player"(player_id) ON DELETE CASCADE,
    CONSTRAINT player_valuation_club_fk
        FOREIGN KEY (club_id) REFERENCES "Club"(tm_club_id) ON DELETE SET NULL,
    CONSTRAINT player_valuation_unique
        UNIQUE (player_id, valuation_date, club_id),
    CONSTRAINT player_valuation_value_non_negative
        CHECK (market_value_eur >= 0)
);

CREATE INDEX IF NOT EXISTS idx_player_valuation_player
    ON "Player_valuation_history" (player_id);

CREATE INDEX IF NOT EXISTS idx_player_valuation_date
    ON "Player_valuation_history" (valuation_date DESC);

CREATE INDEX IF NOT EXISTS idx_player_valuation_player_date
    ON "Player_valuation_history" (player_id, valuation_date DESC);

GRANT SELECT, INSERT, UPDATE, DELETE ON "Player_valuation_history" TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE "Player_valuation_history_id_seq" TO anon, authenticated;
