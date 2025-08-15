from bs4 import BeautifulSoup
import requests

from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import extract_attendance_from_text, extract_coach_id, extract_date_from_href, extract_team_id, get_soup
from utils.tm_utils import construct_tm_league_url

class MatchPage(Page):
    def __init__(self, match_id: str):
        self.url = f"{TM_BASE_URL}-/index/spielbericht/{match_id}"
        self.page = None
        self.fetch_page()
        
    def get_results(self):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        results = self.page.xpath('//div[contains(@class, "match-info")]//span[@class="score"]')
        if not results:
            print("No match results found on the page.")
            return []
        return [result.text for result in results]
    
    def get_team(self, home: bool = True):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        team_selector = ''
        if(home):
            team_selector = '//div[contains(@class, "sb-heim")]//a[contains(@class, "sb-vereinslink")]/@href'
        else:
            team_selector = '//div[contains(@class, "sb-gast")]//a[contains(@class, "sb-vereinslink")]/@href'
        home_team_id = self.page.xpath(team_selector)

        if not home_team_id:
            print("No team names found on the page.")
            return []
        home_team_id = extract_team_id(home_team_id[0])
        
        if not home_team_id:
            print("No home team ID found on the page.")
            return []
        # Return the first home team ID found
        return home_team_id

    def get_match_date(self):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        date_selector = '//div[contains(@class, "sb-spieldaten")]//a/@href'  # Adjust the selector as needed
        # Extract match date from the href attribute

        match_date = self.page.xpath(date_selector)    

        print
        if not match_date:
            print("No match date found on the page.")
            return None
        return extract_date_from_href(match_date[1]) if match_date else None   
    

    def get_attendance(self):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        attendance_selector = '//*[contains(text(), "Attendance")]//text()'
        attendance = self.page.xpath(attendance_selector)

        if not attendance:
            print("No attendance found on the page.")
            return None
        return extract_attendance_from_text(attendance[0].strip())
    

    def get_coaches_ids(self):
        if self.page is None:
            self.fetch_page()
        
        coach_selector = '//td[contains(@class, "bench-table__td")]//a/@href'
        
        coach_id = self.page.xpath(coach_selector)

        if not coach_id:
            print("No coach ID found on the page.")
            return None
        return [extract_coach_id(coach_id[0]), extract_coach_id(coach_id[1])] 
    
    def get_match_result(self):
        if self.page is None:
            self.fetch_page()
        # Example XPath/CSS selector parsing
        result_selector = '//*[contains(@class, "sb-endstand")][1]//text()'
        result = self.page.xpath(result_selector)

        if not result:
            print("No match result found on the page.")
            return None
        return result[0].strip() if result else None