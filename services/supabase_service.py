from supabase import create_client, Client
from config.settings import SUPABASE_URL, SUPABASE_KEY

def create_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)
