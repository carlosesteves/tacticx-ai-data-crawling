from bs4 import BeautifulSoup
from requests import Session
from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import (
    extract_attendance_from_text,
    extract_coach_id,
    extract_date_from_href,
    extract_goals_from_score,
    extract_team_id,
    get_points_from_score,
    get_soup
)

class MatchPage(Page):
    def __init__(self, session: Session, match_id: int, html_content: str = None):
        """
        If html_content is provided, it will be used instead of fetching from the network.
        """
        self.url = f"{TM_BASE_URL}-/index/spielbericht/{match_id}"
        self.page = None
        self.match_id = match_id
        self.session = session

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
        date_links = self.page.xpath('//a[contains(@href, "datum")]/@href')
        if not date_links:
            return None
        return extract_date_from_href(date_links[0])

    def get_attendance(self):
        if self.page is None:
            self.fetch_page()
        attendance_text = self.page.xpath('//*[contains(text(), "Attendance")]//text()')
        if not attendance_text:
            return None
        return int(extract_attendance_from_text(attendance_text[0].strip()))

    def get_home_coach_id(self):
        return int(self.get_coaches_ids()[0])

    def get_away_coach_id(self):
        return int(self.get_coaches_ids()[1])

    def get_coaches_ids(self):
        if self.page is None:
            self.fetch_page()
        coach_links = self.page.xpath('//a[contains(@href, "/profil/trainer/")]/@href')
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
    
    def get_home_team_score(self):
        return extract_goals_from_score(self.get_match_result())[0]
    
    def get_away_team_score(self):
        return extract_goals_from_score(self.get_match_result())[1]
    
    def get_home_team_points(self):
        return get_points_from_score(self.get_match_result())[0]
    
    def get_away_team_points(self):
        return get_points_from_score(self.get_match_result())[1]

    
    def get_match_data(self):
        match_score = self.get_match_result()
        return {                        
                "tm_match_id": self.match_id,
                "home_club_id": self.get_team(home=True),
                "away_club_id": self.get_team(home=False),
                "date": self.get_match_date(),
                "home_coach_id": self.get_coaches_ids()[0],
                "away_coach_id": self.get_coaches_ids()[1],
                "attendance": self.get_attendance(),
                "home_team_score": extract_goals_from_score(match_score)[0],
                "away_team_score": extract_goals_from_score(match_score)[1],            
                "home_team_points": get_points_from_score(match_score)[0],
                "away_team_points": get_points_from_score(match_score)[1],
            }