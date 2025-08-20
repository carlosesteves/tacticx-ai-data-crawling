from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import (
    convert_to_yyyy_mm_dd,
    extract_team_id
)

class CoachPage(Page):
    def __init__(self, coach_id: str, html_content: str = None):
        """
        If html_content is provided, it will be used instead of fetching from the network.
        """
        self.url = f"{TM_BASE_URL}-/profil/trainer/{coach_id}"
        self.page = None
        self.coach_id = coach_id
        if html_content:
            self.load_html(html_content)
        else:
            self.fetch_page()

    def get_coach_id(self):
        return self.coach_id

    def _get_td_text_by_th(self, label: str):
        """
        Generic helper to extract the <td> text for a given <th> label using XPath.
        """
        if self.page is None:
            self.fetch_page()

        # Find the <tr> where <th> contains the label
        row = self.page.xpath(f'//table[contains(@class, "auflistung")]//tr[contains(string(th), "{label}")]/td/text()')
        
        if not row:
            return None
        
        if len(row) > 1:
            return row[1].strip()  # Return second <td> text if exists

        # Return text content
        return row[0].strip()

    def get_coach_name(self):
        row = self.page.xpath('//h1[contains(@class, "data-header__headline-wrapper")]//text()')
        if not row:
            return None
        
        return f"{row[0].strip()} {row[1].strip()}" if len(row) > 1 else row[0].strip()

    def get_coaching_license(self):
        coaching_licenses_list = self.page.xpath('//li[contains(.,"Coaching Licence")]//*[@class="data-header__content"]//text()')
        if len(coaching_licenses_list) == 0:
            return ''
        else:
            return coaching_licenses_list[0].strip()

    def get_dob(self):
        dob_text = self._get_td_text_by_th("Date of birth").strip()
        if dob_text:
            return convert_to_yyyy_mm_dd(dob_text)
        return None

    def get_citizenship_country(self):
        td_text = self._get_td_text_by_th("Citizenship")
        if td_text:
            return td_text.strip()
        return None

    def get_preferred_formation(self):
        return self._get_td_text_by_th("Preferred formation")
    
    def get_tenures(self):
        tenures = []
        if self.page is None:
            self.fetch_page()
        row_tenures = self.page.xpath('//div[contains(@class, "box") and .//h2[contains(normalize-space(.), "History")]]//table[contains(@class, "items")]//tr')
        role = None
        team_id = None
        start_date = None
        end_date = None

        for row in row_tenures:
            cols = row.xpath('.//td')
            if(len(cols) > 3):
                team_id = extract_team_id(cols[1].xpath('.//@href')[0])
                role = cols[1].xpath('.//text()')[1]
                start_date = convert_to_yyyy_mm_dd(cols[2].xpath('.//text()')[0].strip())
                end_date = convert_to_yyyy_mm_dd(cols[3].xpath('.//text()')[0].strip())            

                tenures.append({
                    'club_id': team_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'role': role,
                    'tm_coach_id': self.coach_id,
                    'is_current': end_date is None
                })
        
        return tenures