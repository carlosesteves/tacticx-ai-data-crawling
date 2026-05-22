-- Stores player participation per match, including starters, bench players,
-- and substitution timings.

CREATE TABLE IF NOT EXISTS "Player_match" (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    club_id BIGINT,
    match_side VARCHAR(8),
    squad_role VARCHAR(16),
    is_starter BOOLEAN NOT NULL DEFAULT FALSE,
    appeared BOOLEAN NOT NULL DEFAULT FALSE,
    shirt_number INTEGER,
    is_captain BOOLEAN NOT NULL DEFAULT FALSE,
    position_id INTEGER,
    position_name VARCHAR(64),
    came_on_minute INTEGER,
    came_on_added_time INTEGER,
    went_off_minute INTEGER,
    went_off_added_time INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT player_match_unique UNIQUE (match_id, player_id),
    CONSTRAINT player_match_match_fk FOREIGN KEY (match_id) REFERENCES "Match"(tm_match_id) ON DELETE CASCADE,
    CONSTRAINT player_match_player_fk FOREIGN KEY (player_id) REFERENCES "Player"(player_id) ON DELETE CASCADE,
    CONSTRAINT player_match_club_fk FOREIGN KEY (club_id) REFERENCES "Club"(tm_club_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_player_match_match ON "Player_match" (match_id);
CREATE INDEX IF NOT EXISTS idx_player_match_player ON "Player_match" (player_id);
CREATE INDEX IF NOT EXISTS idx_player_match_club ON "Player_match" (club_id);
CREATE INDEX IF NOT EXISTS idx_player_match_appeared ON "Player_match" (appeared);

GRANT SELECT, INSERT, UPDATE, DELETE ON "Player_match" TO anon, authenticated;
GRANT USAGE, SELECT ON SEQUENCE "Player_match_id_seq" TO anon, authenticated;
