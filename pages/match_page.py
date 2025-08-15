from bs4 import BeautifulSoup
from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import (
    extract_attendance_from_text,
    extract_coach_id,
    extract_date_from_href,
    extract_team_id,
    get_soup
)

class MatchPage(Page):
    def __init__(self, match_id: str, html_content: str = None):
        """
        If html_content is provided, it will be used instead of fetching from the network.
        """
        self.url = f"{TM_BASE_URL}-/index/spielbericht/{match_id}"
        self.page = None

        if html_content:
            self.load_html(html_content)
        else:
            self.fetch_page()

    def get_team(self, home: bool = True):
        if self.page is None:
            self.fetch_page()
        selector = (
            '//div[contains(@class, "sb-heim")]//a[contains(@class, "sb-vereinslink")]/@href'
            if home else
            '//div[contains(@class, "sb-gast")]//a[contains(@class, "sb-vereinslink")]/@href'
        )
        team_links = self.page.xpath(selector)
        if not team_links:
            return []
        return extract_team_id(team_links[0])

    def get_match_date(self):
        if self.page is None:
            self.fetch_page()
        date_links = self.page.xpath('//div[contains(@class, "sb-spieldaten")]//a/@href')
        if not date_links or len(date_links) < 2:
            return None
        return extract_date_from_href(date_links[1])

    def get_attendance(self):
        if self.page is None:
            self.fetch_page()
        attendance_text = self.page.xpath('//*[contains(text(), "Attendance")]//text()')
        if not attendance_text:
            return None
        return extract_attendance_from_text(attendance_text[0].strip())

    def get_coaches_ids(self):
        if self.page is None:
            self.fetch_page()
        coach_links = self.page.xpath('//td[contains(@class, "bench-table__td")]//a/@href')
        if not coach_links or len(coach_links) < 2:
            return None
        return [extract_coach_id(coach_links[0]), extract_coach_id(coach_links[1])]

    def get_match_result(self):
        if self.page is None:
            self.fetch_page()
        result_text = self.page.xpath('//*[contains(@class, "sb-endstand")][1]//text()')
        if not result_text:
            return None
        return result_text[0].strip()
