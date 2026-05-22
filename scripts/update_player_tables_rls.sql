-- Enable writes from anon/authenticated roles for Player, Player_tenure, Player_valuation_history, Player_match.
-- Apply only if this aligns with your security model.

ALTER TABLE IF EXISTS "Player" ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Player_tenure" ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Player_valuation_history" ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "Player_match" ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS player_select_all ON "Player";
DROP POLICY IF EXISTS player_insert_all ON "Player";
DROP POLICY IF EXISTS player_update_all ON "Player";

CREATE POLICY player_select_all ON "Player"
    FOR SELECT TO anon, authenticated
    USING (true);

CREATE POLICY player_insert_all ON "Player"
    FOR INSERT TO anon, authenticated
    WITH CHECK (true);

CREATE POLICY player_update_all ON "Player"
    FOR UPDATE TO anon, authenticated
    USING (true)
    WITH CHECK (true);

DROP POLICY IF EXISTS player_tenure_select_all ON "Player_tenure";
DROP POLICY IF EXISTS player_tenure_insert_all ON "Player_tenure";
DROP POLICY IF EXISTS player_tenure_update_all ON "Player_tenure";

CREATE POLICY player_tenure_select_all ON "Player_tenure"
    FOR SELECT TO anon, authenticated
    USING (true);

CREATE POLICY player_tenure_insert_all ON "Player_tenure"
    FOR INSERT TO anon, authenticated
    WITH CHECK (true);

CREATE POLICY player_tenure_update_all ON "Player_tenure"
    FOR UPDATE TO anon, authenticated
    USING (true)
    WITH CHECK (true);

DROP POLICY IF EXISTS player_valuation_select_all ON "Player_valuation_history";
DROP POLICY IF EXISTS player_valuation_insert_all ON "Player_valuation_history";
DROP POLICY IF EXISTS player_valuation_update_all ON "Player_valuation_history";

CREATE POLICY player_valuation_select_all ON "Player_valuation_history"
    FOR SELECT TO anon, authenticated
    USING (true);

CREATE POLICY player_valuation_insert_all ON "Player_valuation_history"
    FOR INSERT TO anon, authenticated
    WITH CHECK (true);

CREATE POLICY player_valuation_update_all ON "Player_valuation_history"
    FOR UPDATE TO anon, authenticated
    USING (true)
    WITH CHECK (true);

DROP POLICY IF EXISTS player_match_select_all ON "Player_match";
DROP POLICY IF EXISTS player_match_insert_all ON "Player_match";
DROP POLICY IF EXISTS player_match_update_all ON "Player_match";

CREATE POLICY player_match_select_all ON "Player_match"
    FOR SELECT TO anon, authenticated
    USING (true);

CREATE POLICY player_match_insert_all ON "Player_match"
    FOR INSERT TO anon, authenticated
    WITH CHECK (true);

CREATE POLICY player_match_update_all ON "Player_match"
    FOR UPDATE TO anon, authenticated
    USING (true)
    WITH CHECK (true);
