import requests
from utils.page_utils import get_soup


BASE_URL = "https://www.transfermarkt.com"

class Page:
    def __init__(self):
        self.url = f"{BASE_URL}-"
        self.page = None
        self.fetch_page()

    def fetch_page(self):
        page = get_soup(self.url, requests.Session())
        if page is not None:
            self.page = page
        else:
            raise Exception(f"Failed to fetch page for {self.url}")
