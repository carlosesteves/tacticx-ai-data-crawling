import pathlib
from pages.league_page_matches import LeaguePageMatches

FIXTURES = pathlib.Path(__file__).parent / "fixtures"

def load_fixture(filename):
    return (FIXTURES / filename).read_text(encoding="utf-8")


def test_get_match_ids_from_fixture():
    html_content = load_fixture("league_year_matches_GB1_2011_page_test.html")
    page = LeaguePageMatches("GB1", 2024, html_content=html_content)

    match_ids = page.get_match_ids()

    assert isinstance(match_ids, list)
    assert all(isinstance(m, str) for m in match_ids)
    assert len(match_ids) == 380
    assert "1131726" in match_ids


def test_get_match_ids_empty_fixture():
    empty_html = "<html></html>"
    page = LeaguePageMatches("GB1", 2024, html_content=empty_html)

    match_ids = page.get_match_ids()

    assert match_ids == []
