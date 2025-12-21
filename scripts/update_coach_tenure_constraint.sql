-- Step 1: Drop the existing primary key constraint
ALTER TABLE "Coach_tenure" 
DROP CONSTRAINT IF EXISTS "Coach_tenure_pkey";

-- Step 2: Drop the existing unique constraint if it exists
ALTER TABLE "Coach_tenure" 
DROP CONSTRAINT IF EXISTS "coach_tenure_coach_club_unique";

-- Step 2b: Drop the new unique constraint if it already exists
ALTER TABLE "Coach_tenure" 
DROP CONSTRAINT IF EXISTS "coach_tenure_coach_club_start_unique";

-- Step 3: Add an auto-incrementing ID column as the new primary key
-- (Only if it doesn't already exist)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'Coach_tenure' AND column_name = 'id'
    ) THEN
        ALTER TABLE "Coach_tenure" 
        ADD COLUMN id SERIAL PRIMARY KEY;
    END IF;
END $$;

-- Step 4: Add a unique constraint on (coach_id, club_id, start_date)
-- This allows multiple tenures for the same coach at the same club (different stints)
ALTER TABLE "Coach_tenure" 
ADD CONSTRAINT "coach_tenure_coach_club_start_unique" 
UNIQUE (coach_id, club_id, start_date);
