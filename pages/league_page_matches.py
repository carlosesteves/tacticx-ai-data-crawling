from lxml import html
from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import get_soup

class LeaguePageMatches(Page):
    def __init__(self, league_code: str, season_id: int, html_content: str = None):
        """
        If html_content is provided, it will be parsed instead of fetching the page.
        """
        self.url = f"{TM_BASE_URL}-/gesamtspielplan/wettbewerb/{league_code}/saison_id/{season_id}"
        self.page = None

        if html_content:
            # Parse directly from provided HTML
            self.page = html.fromstring(html_content)
        else:
            self.fetch_page()

    def get_match_ids(self):
        if self.page is None:
            self.fetch_page()
        match_links = self.page.xpath('//a[contains(@class, "ergebnis-link")]//@id')
        print(f"Found {len(match_links)} matches in {self.url}")
        if not match_links:
            print("No match links found on the page.")
            return []
        return match_links
