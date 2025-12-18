from datetime import datetime
from lxml import html
from requests import Session
from config.constants import TM_BASE_URL
from pages.page import Page
from utils.page_utils import clean_text, extract_club_id, extract_match_id, extract_team_id, get_soup, parse_result

class LeaguePageMatches(Page):
    def __init__(self, session: Session, league_code: str, season_id: int, html_content: str = None):
        """
        If html_content is provided, it will be parsed instead of fetching the page.
        """
        self.url = f"{TM_BASE_URL}-/gesamtspielplan/wettbewerb/{league_code}/saison_id/{season_id}"
        self.page = None
        self.session = session

        if html_content:
            # Parse directly from provided HTML
            self.page = html.fromstring(html_content)
        else:
            self.fetch_page()

    def get_match_ids(self) -> set[int]:
        if self.page is None:
            self.fetch_page()
        match_links = self.page.xpath('//a[contains(@class, "ergebnis-link")]//@id')
        print(f"Found {len(match_links)} matches in {self.url}")
        if not match_links:
            print("No match links found on the page.")
            return set()
        return {int(link) for link in match_links if link.isdigit()}


    def get_matches(self):
        matches = []  # Changed from set() to list

           # Loop through all tables on the page
        tables = self.page.xpath('//table')
        for table in tables:
            rows = table.xpath('.//tbody/tr')

            current_date = None
            current_time = None

            for row in rows:
                # Header rows (carry forward date/time)
                if "bg_blau_20" in row.xpath('./@class'):
                    date_text = row.xpath('.//a/text()')
                    if date_text:
                        # Try multiple date formats (dd/mm/yy or mm/dd/yy)
                        date_str = clean_text(date_text[0])
                        parsed_date = None
                        for fmt in ["%d/%m/%y", "%m/%d/%y"]:
                            try:
                                parsed_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            current_date = parsed_date.strftime("%Y-%m-%d")

                    # possible time in header
                    time_text = clean_text("".join(row.xpath('.//text()')))
                    for token in time_text.split():
                        if "AM" in token or "PM" in token:
                            current_time = token
                    continue

                # Match rows
                date   = current_date
                time   = clean_text(row.xpath('normalize-space(./td[2])')) or current_time
                result = clean_text(row.xpath('normalize-space(./td[5])'))

                home_href = row.xpath('./td[3]/a/@href')
                home_id = extract_club_id(home_href[0]) if home_href else None

                away_href = row.xpath('./td[7]/a/@href')
                away_id = extract_club_id(away_href[0]) if away_href else None

                # Match report link (for match_id)
                match_href = row.xpath('./td[5]/a/@href')
                match_id = extract_match_id(match_href[0]) if match_href else None

                home_goals, away_goals = parse_result(result)

                if home_id and away_id:
                    matches.append({
                        "match_id": match_id,
                        "date": date,
                        "home_club_id": home_id,
                        "away_club_id": away_id,
                        "home_goals": home_goals,
                        "away_goals": away_goals
                    })
        return matches
