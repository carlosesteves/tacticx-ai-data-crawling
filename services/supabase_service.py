from supabase import create_client, Client
from config.settings import SUPABASE_URL, SUPABASE_KEY
import ssl

def create_supabase_client() -> Client:
    # Disable SSL verification to bypass certificate errors
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Monkey patch SSL context to disable verification
    ssl._create_default_https_context = ssl._create_unverified_context
    
    return create_client(SUPABASE_URL, SUPABASE_KEY)
