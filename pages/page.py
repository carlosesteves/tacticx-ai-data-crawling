
import os
import random, time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['PYTHONHTTPSVERIFY'] = '0'

import requests
from utils.page_utils import get_soup
from lxml import html  # Assuming you are using xpath

BASE_URL = "http://www.transfermarkt.com"

class Page:
    def __init__(self, session):
        self.url = f"{BASE_URL}-"
        self.page = None
        self.fetch_page()
        self.session = session

    def fetch_page(self):
        retries = 3
        sleep_time = 3
        for attempt in range(retries):
            page = get_soup(self.url, self.session)
            if page is not None:
                self.page = page
                return
            else:
                print(f"[fetch_page] Attempt {attempt+1}/{retries} failed for {self.url}. Retrying in {sleep_time} seconds...")
                if attempt < retries - 1:
                    time.sleep(sleep_time)
        raise Exception(f"Failed to fetch page for {self.url} after {retries} attempts.")
    
    def load_html(self, html_content: str):
        """
        Load HTML directly (for testing or pre-fetched content).
        """
        # Parse with lxml.html for xpath support
        self.page = html.fromstring(html_content)