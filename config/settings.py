import os
from dotenv import load_dotenv

# Load .env from the repo root (two levels up from this file: config/ -> repo root)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
CLUB_DATA_PATH = os.environ.get('CLUB_DATA_PATH', 'data/data_for_db/clubs_tm.csv')

ODDS_API_URL = {
    'base_url': os.environ.get('ODDS_API_URL', 'https://api.the-odds-api.com/v4/sports'),
}