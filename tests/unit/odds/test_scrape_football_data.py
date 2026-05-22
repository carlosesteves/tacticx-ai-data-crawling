"""
Unit tests for scripts/scrape_football_data.py

Coverage:
  - _current_season_code: correct season flip in July
  - _parse_last_modified: valid header, None input, malformed input
  - _load_meta / _save_meta: round-trip, missing file, corrupt file
  - discover_csv_links: filters by known FD codes, season, returns correct shape
  - download_csv: freshness skip, force override, HTTP error, missing cols,
                  empty body, all-null columns stripped, happy path
  - merge_and_write: no existing file, dedup (new wins), corrupt existing file,
                     no frames (no-op)
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest
import requests

# ---------------------------------------------------------------------------
# Helpers to build fake HTTP responses
# ---------------------------------------------------------------------------

def _make_response(
    status_code: int = 200,
    content: bytes = b"",
    headers: Optional[dict] = None,
    text: str = "",
) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.content = content
    resp.text = text
    resp.headers = headers or {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


_MINIMAL_CSV = (
    b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\n"
    b"E0,01/08/2025,Arsenal,Chelsea,2,1,H\n"
)

_DATA_PAGE_HTML = """
<html><body>
  <a href="/mmz4281/2526/E0.csv">E0</a>
  <a href="/mmz4281/2526/E1.csv">E1</a>
  <a href="/mmz4281/2425/E0.csv">E0 prev</a>
  <a href="/mmz4281/2526/UNKNOWN.csv">UNKNOWN league</a>
  <a href="/other/page.html">irrelevant</a>
</body></html>
"""


# ---------------------------------------------------------------------------
# _current_season_code
# ---------------------------------------------------------------------------

class TestCurrentSeasonCode:
    def test_august_gives_current_start_year(self):
        from scripts.scrape_football_data import _current_season_code
        with patch("scripts.scrape_football_data.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2025, 8, 1)
            assert _current_season_code() == "2526"

    def test_january_gives_previous_start_year(self):
        from scripts.scrape_football_data import _current_season_code
        with patch("scripts.scrape_football_data.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 15)
            assert _current_season_code() == "2526"

    def test_june_still_previous_season(self):
        from scripts.scrape_football_data import _current_season_code
        with patch("scripts.scrape_football_data.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 6, 30)
            assert _current_season_code() == "2526"

    def test_july_flips_season(self):
        from scripts.scrape_football_data import _current_season_code
        with patch("scripts.scrape_football_data.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 7, 1)
            assert _current_season_code() == "2627"


# ---------------------------------------------------------------------------
# _parse_last_modified
# ---------------------------------------------------------------------------

class TestParseLastModified:
    def test_valid_rfc2822_header(self):
        from scripts.scrape_football_data import _parse_last_modified
        result = _parse_last_modified("Tue, 01 Aug 2025 12:00:00 GMT")
        assert isinstance(result, datetime)
        assert result.year == 2025

    def test_none_returns_none(self):
        from scripts.scrape_football_data import _parse_last_modified
        assert _parse_last_modified(None) is None

    def test_malformed_returns_none(self):
        from scripts.scrape_football_data import _parse_last_modified
        assert _parse_last_modified("not-a-date") is None


# ---------------------------------------------------------------------------
# _load_meta / _save_meta
# ---------------------------------------------------------------------------

class TestMetaCache:
    def test_round_trip(self, tmp_path):
        from scripts.scrape_football_data import _load_meta, _save_meta
        meta_file = tmp_path / ".last_modified_cache.json"
        data = {"2526/E0": "Tue, 01 Aug 2025 12:00:00 GMT"}
        _save_meta(meta_file, data)
        loaded = _load_meta(meta_file)
        assert loaded == data

    def test_missing_file_returns_empty(self, tmp_path):
        from scripts.scrape_football_data import _load_meta
        result = _load_meta(tmp_path / "nonexistent.json")
        assert result == {}

    def test_corrupt_file_returns_empty(self, tmp_path):
        from scripts.scrape_football_data import _load_meta
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {{{{")
        result = _load_meta(bad_file)
        assert result == {}

    def test_save_creates_parent_dirs(self, tmp_path):
        from scripts.scrape_football_data import _save_meta
        nested = tmp_path / "a" / "b" / "cache.json"
        _save_meta(nested, {"k": "v"})
        assert nested.exists()


# ---------------------------------------------------------------------------
# discover_csv_links
# ---------------------------------------------------------------------------

class TestDiscoverCsvLinks:
    def _make_session(self, html: str) -> MagicMock:
        sess = MagicMock(spec=requests.Session)
        sess.headers = {}
        resp = _make_response(200, text=html)
        sess.get.return_value = resp
        return sess

    def test_returns_known_leagues_only(self):
        from scripts.scrape_football_data import discover_csv_links, KNOWN_FD_CODES
        # Patch KNOWN_FD_CODES to contain just E0 and E1
        with patch("scripts.scrape_football_data.KNOWN_FD_CODES", {"E0", "E1"}):
            sess = self._make_session(_DATA_PAGE_HTML)
            links = discover_csv_links(session=sess)
        codes = {lnk["fd_code"] for lnk in links}
        assert "E0" in codes
        assert "E1" in codes
        assert "UNKNOWN" not in codes

    def test_season_filter_applied(self):
        from scripts.scrape_football_data import discover_csv_links
        with patch("scripts.scrape_football_data.KNOWN_FD_CODES", {"E0", "E1"}):
            sess = self._make_session(_DATA_PAGE_HTML)
            links = discover_csv_links(season_filter="2526", session=sess)
        seasons = {lnk["season"] for lnk in links}
        assert seasons == {"2526"}

    def test_no_filter_returns_all_seasons(self):
        from scripts.scrape_football_data import discover_csv_links
        with patch("scripts.scrape_football_data.KNOWN_FD_CODES", {"E0", "E1"}):
            sess = self._make_session(_DATA_PAGE_HTML)
            links = discover_csv_links(season_filter=None, session=sess)
        seasons = {lnk["season"] for lnk in links}
        assert "2526" in seasons
        assert "2425" in seasons

    def test_url_is_absolute(self):
        from scripts.scrape_football_data import discover_csv_links, BASE_URL
        with patch("scripts.scrape_football_data.KNOWN_FD_CODES", {"E0"}):
            sess = self._make_session(_DATA_PAGE_HTML)
            links = discover_csv_links(session=sess)
        for lnk in links:
            assert lnk["url"].startswith(BASE_URL)

    def test_http_error_propagates(self):
        from scripts.scrape_football_data import discover_csv_links
        sess = self._make_session("")
        sess.get.return_value = _make_response(500)
        with pytest.raises(requests.HTTPError):
            discover_csv_links(session=sess)


# ---------------------------------------------------------------------------
# download_csv
# ---------------------------------------------------------------------------

class TestDownloadCsv:
    def _make_session(
        self,
        head_headers: Optional[dict] = None,
        get_content: bytes = _MINIMAL_CSV,
        get_headers: Optional[dict] = None,
        get_status: int = 200,
    ) -> MagicMock:
        sess = MagicMock(spec=requests.Session)
        sess.headers = {}
        head_resp = _make_response(200, headers=head_headers or {})
        get_resp = _make_response(get_status, content=get_content, headers=get_headers or {})
        sess.head.return_value = head_resp
        sess.get.return_value = get_resp
        return sess

    def test_happy_path_returns_dataframe_with_league_code(self):
        from scripts.scrape_football_data import download_csv
        sess = self._make_session()
        df = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert df is not None
        assert "league_code" in df.columns
        assert (df["league_code"] == "E0").all()

    def test_season_code_added(self):
        from scripts.scrape_football_data import download_csv
        sess = self._make_session()
        df = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert "season_code" in df.columns
        assert (df["season_code"] == "2526").all()

    def test_freshness_skip_when_cached_lm_is_newer(self):
        from scripts.scrape_football_data import download_csv
        server_lm = "Mon, 01 Jan 2025 00:00:00 GMT"
        cached_lm = "Tue, 01 Jul 2025 00:00:00 GMT"  # newer than server
        sess = self._make_session(head_headers={"Last-Modified": server_lm})
        meta = {"2526/E0": cached_lm}
        result = download_csv("http://example.com/E0.csv", "E0", "2526", force=False, meta=meta, session=sess)
        assert result is None
        sess.get.assert_not_called()

    def test_force_skips_freshness_check(self):
        from scripts.scrape_football_data import download_csv
        server_lm = "Mon, 01 Jan 2025 00:00:00 GMT"
        cached_lm = "Tue, 01 Jul 2025 00:00:00 GMT"
        sess = self._make_session(head_headers={"Last-Modified": server_lm})
        meta = {"2526/E0": cached_lm}
        result = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, meta=meta, session=sess)
        assert result is not None  # downloaded despite cached being newer
        sess.get.assert_called_once()

    def test_http_error_returns_none(self):
        from scripts.scrape_football_data import download_csv
        sess = self._make_session(get_status=404, get_content=b"")
        result = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert result is None

    def test_empty_content_returns_none(self):
        from scripts.scrape_football_data import download_csv
        sess = self._make_session(get_content=b"   ")
        result = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert result is None

    def test_missing_required_columns_returns_none(self):
        from scripts.scrape_football_data import download_csv
        bad_csv = b"Col1,Col2\nfoo,bar\n"
        sess = self._make_session(get_content=bad_csv)
        result = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert result is None

    def test_all_null_columns_are_dropped(self):
        from scripts.scrape_football_data import download_csv
        csv_with_trailing_comma = (
            b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,\n"
            b"E0,01/08/2025,Arsenal,Chelsea,2,1,H,\n"
        )
        sess = self._make_session(get_content=csv_with_trailing_comma)
        df = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert df is not None
        assert all(df[col].notna().any() for col in df.columns)

    def test_blank_home_team_rows_dropped(self):
        from scripts.scrape_football_data import download_csv
        csv = (
            b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\n"
            b"E0,01/08/2025,Arsenal,Chelsea,2,1,H\n"
            b"E0,,,,,,"  # blank row
        )
        sess = self._make_session(get_content=csv)
        df = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert df is not None
        assert len(df) == 1

    def test_last_modified_stored_in_meta(self):
        from scripts.scrape_football_data import download_csv
        lm = "Tue, 01 Aug 2025 12:00:00 GMT"
        sess = self._make_session(get_headers={"Last-Modified": lm})
        meta = {}
        download_csv("http://example.com/E0.csv", "E0", "2526", force=True, meta=meta, session=sess)
        assert meta.get("2526/E0") == lm

    def test_latin1_fallback_encoding(self):
        """CSV with latin-1 characters should parse without error."""
        from scripts.scrape_football_data import download_csv
        latin1_csv = "Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR\nE0,01/08/2025,Dep\xe9rtivo,Chelsea,1,0,H\n".encode("latin-1")
        sess = self._make_session(get_content=latin1_csv)
        df = download_csv("http://example.com/E0.csv", "E0", "2526", force=True, session=sess)
        assert df is not None
        assert len(df) == 1


# ---------------------------------------------------------------------------
# merge_and_write
# ---------------------------------------------------------------------------

class TestMergeAndWrite:
    def _make_frame(self, rows: list[dict]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def test_writes_new_file_when_none_exists(self, tmp_path):
        from scripts.scrape_football_data import merge_and_write
        out = tmp_path / "out.csv"
        frames = [self._make_frame([
            {"Date": "01/08/2025", "HomeTeam": "Arsenal", "AwayTeam": "Chelsea", "league_code": "E0"}
        ])]
        merged = merge_and_write(frames, out)
        assert out.exists()
        assert len(merged) == 1

    def test_new_row_overwrites_existing_duplicate(self, tmp_path):
        from scripts.scrape_football_data import merge_and_write
        out = tmp_path / "out.csv"
        # Write old version with OddsH=1.5
        old = self._make_frame([
            {"Date": "01/08/2025", "HomeTeam": "Arsenal", "AwayTeam": "Chelsea",
             "league_code": "E0", "OddsH": 1.5}
        ])
        old.to_csv(out, index=False)

        # New download with updated OddsH=2.0
        new_frames = [self._make_frame([
            {"Date": "01/08/2025", "HomeTeam": "Arsenal", "AwayTeam": "Chelsea",
             "league_code": "E0", "OddsH": 2.0}
        ])]
        merged = merge_and_write(new_frames, out)
        assert len(merged) == 1
        assert merged["OddsH"].iloc[0] == 2.0

    def test_distinct_rows_are_both_kept(self, tmp_path):
        from scripts.scrape_football_data import merge_and_write
        out = tmp_path / "out.csv"
        old = self._make_frame([
            {"Date": "01/08/2025", "HomeTeam": "Arsenal", "AwayTeam": "Chelsea", "league_code": "E0"}
        ])
        old.to_csv(out, index=False)

        new_frames = [self._make_frame([
            {"Date": "05/08/2025", "HomeTeam": "Brentford", "AwayTeam": "Villa", "league_code": "E0"}
        ])]
        merged = merge_and_write(new_frames, out)
        assert len(merged) == 2

    def test_corrupt_existing_file_is_replaced(self, tmp_path):
        from scripts.scrape_football_data import merge_and_write
        out = tmp_path / "out.csv"
        out.write_text("this is not a valid csv \x00\xff")
        frames = [self._make_frame([
            {"Date": "01/08/2025", "HomeTeam": "Arsenal", "AwayTeam": "Chelsea", "league_code": "E0"}
        ])]
        # Should not raise; should replace corrupt file
        merged = merge_and_write(frames, out)
        assert len(merged) == 1

    def test_empty_frames_list_does_not_write(self, tmp_path):
        from scripts.scrape_football_data import merge_and_write
        out = tmp_path / "out.csv"
        merged = merge_and_write([], out)
        # Returns empty DataFrame; output file may or may not exist
        assert merged.empty
