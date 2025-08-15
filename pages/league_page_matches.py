from bs4 import BeautifulSoup
import requests

from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import get_soup
from utils.tm_utils import construct_tm_league_url

class LeaguePageMatches(Page):
    def __init__(self, league_code: str, season_id: int):
        self.url = f"{TM_BASE_URL}-/gesamtspielplan/wettbewerb/{league_code}/saison_id/{season_id}"
        self.page = None
        self.fetch_page()
        
    def get_match_ids(self):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        match_links = self.page.xpath('//a[contains(@class, "ergebnis-link")]//@id')
        if not match_links:
            print("No match links found on the page.")
            return []
        return match_links