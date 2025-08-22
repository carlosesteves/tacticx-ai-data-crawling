import random, time

import requests
from utils.page_utils import get_soup
from lxml import html  # Assuming you are using xpath

BASE_URL = "https://www.transfermarkt.com"

class Page:
    def __init__(self, session):
        self.url = f"{BASE_URL}-"
        self.page = None
        self.fetch_page()
        self.session = session

    def fetch_page(self):
        page = get_soup(self.url, self.session)
        if page is not None:
            self.page = page
        else:
            raise Exception(f"Failed to fetch page for {self.url}")
    
    def load_html(self, html_content: str):
        """
        Load HTML directly (for testing or pre-fetched content).
        """
        # Parse with lxml.html for xpath support
        self.page = html.fromstring(html_content)