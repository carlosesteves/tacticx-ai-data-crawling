#!/usr/bin/env python3
"""
Scrape football-data.co.uk and download CSV files for all known leagues.

This script replaces the manual Step 1 of the odds pipeline:
  - Fetches https://www.football-data.co.uk/data.php
  - Discovers all CSV links matching known league codes
  - Downloads only new/updated files (via Last-Modified header comparison)
  - Adds a `league_code` column to each CSV
  - Concatenates everything into data/all_leagues_full.csv

Usage:
    python scripts/scrape_football_data.py                    # current season only
    python scripts/scrape_football_data.py --season 2526      # specific season
    python scripts/scrape_football_data.py --all-seasons      # full backfill
    python scripts/scrape_football_data.py --force            # skip freshness check
"""

import argparse
import io
import re
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.football_data_league_mapping import FOOTBALL_DATA_TO_TM_LEAGUE_MAP

BASE_URL = "https://www.football-data.co.uk"
DATA_PAGE = f"{BASE_URL}/data.php"

# Request headers that mimic a browser to avoid 403s
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
}

# CSV link pattern: /mmz4281/<season>/<fd_code>.csv
CSV_LINK_RE = re.compile(r"/mmz4281/(\d{4})/([A-Z0-9]+)\.csv", re.IGNORECASE)

# Where data lives relative to repo root
DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw_football_data"   # per-league per-season CSVs cached here
OUTPUT_FILE = DATA_DIR / "all_leagues_full.csv"
META_FILE = RAW_DIR / ".last_modified_cache.json"  # stores Last-Modified timestamps

KNOWN_FD_CODES = set(FOOTBALL_DATA_TO_TM_LEAGUE_MAP.keys())


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _current_season_code() -> str:
    """
    Return the current football season code in football-data.co.uk format.

    Seasons run Aug–May, so:
      - Aug 2025 → May 2026  →  '2526'
      - Jan 2026 (spring half) →  '2526'
    """
    now = datetime.now()
    # Season flips in July
    if now.month >= 7:
        start = now.year
    else:
        start = now.year - 1
    end = start + 1
    return f"{str(start)[2:]}{str(end)[2:]}"


def _parse_last_modified(header_value: Optional[str]) -> Optional[datetime]:
    """Parse an HTTP Last-Modified header into a timezone-aware datetime."""
    if not header_value:
        return None
    try:
        return parsedate_to_datetime(header_value)
    except Exception:
        return None


def _load_meta(meta_file: Path) -> dict:
    """Load the Last-Modified cache from disk."""
    import json
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text())
        except Exception:
            pass
    return {}


def _save_meta(meta_file: Path, meta: dict) -> None:
    """Persist the Last-Modified cache to disk."""
    import json
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    meta_file.write_text(json.dumps(meta, indent=2))


# ---------------------------------------------------------------------------
# Core discovery
# ---------------------------------------------------------------------------

def discover_csv_links(
    season_filter: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> list[dict]:
    """
    Fetch the football-data.co.uk data page and return all CSV links
    that match known league codes.

    Args:
        season_filter: If given (e.g. '2526'), only return links for that season.
        session:       Optional requests.Session to use.

    Returns:
        List of dicts with keys: season, fd_code, url
    """
    sess = session or requests.Session()
    sess.headers.update(HEADERS)

    resp = sess.get(DATA_PAGE, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        m = CSV_LINK_RE.search(href)
        if not m:
            continue

        season, fd_code = m.group(1), m.group(2).upper()

        if fd_code not in KNOWN_FD_CODES:
            continue

        if season_filter and season != season_filter:
            continue

        url = BASE_URL + href if href.startswith("/") else href
        results.append({"season": season, "fd_code": fd_code, "url": url})

    return results


# ---------------------------------------------------------------------------
# Download a single CSV
# ---------------------------------------------------------------------------

def download_csv(
    url: str,
    fd_code: str,
    season: str,
    force: bool = False,
    meta: Optional[dict] = None,
    session: Optional[requests.Session] = None,
) -> Optional[pd.DataFrame]:
    """
    Download a single CSV from football-data.co.uk.

    Uses conditional GET (Last-Modified check) to skip files that haven't
    changed since the last download, unless `force=True`.

    Args:
        url:     Full CSV URL.
        fd_code: Football-data league code (e.g. 'E0').
        season:  Season string (e.g. '2526').
        force:   Skip freshness check and always download.
        meta:    Dict mapping cache_key → last-modified string (mutated in place).
        session: Optional requests.Session.

    Returns:
        DataFrame with `league_code` column added, or None if skipped/empty.
    """
    if meta is None:
        meta = {}

    sess = session or requests.Session()
    sess.headers.update(HEADERS)

    cache_key = f"{season}/{fd_code}"

    # --- freshness check via HEAD ---
    if not force:
        try:
            head_resp = sess.head(url, timeout=15, allow_redirects=True)
            server_lm = _parse_last_modified(
                head_resp.headers.get("Last-Modified")
            )
            cached_lm_str = meta.get(cache_key)
            if server_lm and cached_lm_str:
                cached_lm = parsedate_to_datetime(cached_lm_str)
                if cached_lm >= server_lm:
                    return None  # nothing new
        except Exception:
            pass  # network hiccup — fall through to full download

    # --- full download ---
    try:
        get_resp = sess.get(url, timeout=60)
        get_resp.raise_for_status()
    except requests.HTTPError as exc:
        print(f"  [WARN] HTTP {exc.response.status_code} for {url} — skipping")
        return None

    # Update Last-Modified cache
    lm_header = get_resp.headers.get("Last-Modified")
    if lm_header:
        meta[cache_key] = lm_header

    content = get_resp.content
    if not content.strip():
        return None

    # Parse CSV — football-data files sometimes have trailing commas / blank cols
    try:
        df = pd.read_csv(
            io.BytesIO(content),
            encoding="utf-8",
            on_bad_lines="skip",
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            io.BytesIO(content),
            encoding="latin-1",
            on_bad_lines="skip",
        )

    if df.empty:
        return None

    # Drop entirely-empty columns (trailing commas artefact)
    df = df.dropna(axis=1, how="all")

    # Ensure required columns exist
    required = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
    missing = required - set(df.columns)
    if missing:
        print(f"  [WARN] {fd_code}/{season}: missing columns {missing} — skipping")
        return None

    # Drop rows with no date or team names (footer rows / blank separators)
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam"])
    df = df[df["HomeTeam"].str.strip().ne("")]

    if df.empty:
        return None

    df["league_code"] = fd_code
    df["season_code"] = season
    return df


# ---------------------------------------------------------------------------
# Merge & write
# ---------------------------------------------------------------------------

def merge_and_write(
    frames: list[pd.DataFrame],
    output_file: Path,
    existing_file: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Concatenate downloaded frames with any pre-existing data and write to disk.

    New rows (identified by Date + HomeTeam + AwayTeam + league_code) replace
    existing ones so re-running is idempotent.

    Args:
        frames:        List of DataFrames from this run.
        output_file:   Destination CSV path.
        existing_file: Path to the file that may already contain historic data.
                       Defaults to output_file.

    Returns:
        The merged DataFrame that was written.
    """
    if existing_file is None:
        existing_file = output_file

    new_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if existing_file.exists() and not new_df.empty:
        try:
            old_df = pd.read_csv(existing_file, low_memory=False)
            # Concatenate then deduplicate, keeping the newer (last) row per key
            combined = pd.concat([old_df, new_df], ignore_index=True)
            dedup_cols = ["Date", "HomeTeam", "AwayTeam", "league_code"]
            existing_dedup = [c for c in dedup_cols if c in combined.columns]
            combined = combined.drop_duplicates(subset=existing_dedup, keep="last")
            merged = combined
        except Exception as exc:
            print(f"[WARN] Could not read existing file ({exc}); replacing it.")
            merged = new_df
    else:
        merged = new_df

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_file, index=False)
    return merged


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(
    season: Optional[str] = None,
    all_seasons: bool = False,
    force: bool = False,
    output_file: Optional[Path] = None,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """
    Orchestrate the full scrape → download → merge cycle.

    Args:
        season:      Specific season code (e.g. '2526'). Defaults to current.
        all_seasons: Download all seasons found on the page (ignores `season`).
        force:       Ignore Last-Modified cache; always re-download.
        output_file: Override the default output path.
        session:     Optional requests.Session (useful in tests for mocking).

    Returns:
        The merged DataFrame written to disk.
    """
    out = output_file or OUTPUT_FILE
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if all_seasons:
        season_filter = None
        print("Discovering all seasons from football-data.co.uk...")
    else:
        season_filter = season or _current_season_code()
        print(f"Season filter: {season_filter}")

    # Discover links
    print(f"Fetching {DATA_PAGE} ...")
    links = discover_csv_links(season_filter=season_filter, session=session)

    if not links:
        print("No matching CSV links found. Nothing to download.")
        return pd.DataFrame()

    seasons_found = sorted({lnk["season"] for lnk in links})
    print(
        f"Found {len(links)} CSV links across {len(seasons_found)} season(s): "
        f"{', '.join(seasons_found)}"
    )

    # Load freshness cache
    meta = _load_meta(META_FILE)

    frames: list[pd.DataFrame] = []
    downloaded = 0
    skipped = 0
    failed = 0

    sess = session or requests.Session()
    sess.headers.update(HEADERS)

    for lnk in links:
        fd_code = lnk["fd_code"]
        s = lnk["season"]
        url = lnk["url"]
        label = f"{fd_code}/{s}"

        time.sleep(0.3)  # polite crawl delay

        df = download_csv(
            url=url,
            fd_code=fd_code,
            season=s,
            force=force,
            meta=meta,
            session=sess,
        )

        if df is None:
            skipped += 1
            print(f"  [SKIP] {label} — unchanged or empty")
        elif df.empty:
            failed += 1
            print(f"  [FAIL] {label} — parsed empty")
        else:
            downloaded += 1
            print(f"  [OK]   {label} — {len(df)} rows")
            frames.append(df)

    # Persist freshness cache
    _save_meta(META_FILE, meta)

    print(
        f"\nDownloads: {downloaded} new, {skipped} skipped (unchanged), "
        f"{failed} failed"
    )

    if not frames:
        print("No new data downloaded. Output file not changed.")
        return pd.DataFrame()

    print(f"\nMerging into {out} ...")
    merged = merge_and_write(frames, out)
    print(f"Output: {len(merged):,} total rows in {out}")
    return merged


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download odds CSVs from football-data.co.uk"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--season",
        metavar="SSSS",
        help="Season code to download (e.g. 2526). Defaults to current season.",
    )
    group.add_argument(
        "--all-seasons",
        action="store_true",
        help="Download ALL seasons found on the page (full backfill).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore Last-Modified cache; always re-download every file.",
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        help=f"Override output CSV path (default: {OUTPUT_FILE})",
    )

    args = parser.parse_args()

    result = run(
        season=args.season,
        all_seasons=args.all_seasons,
        force=args.force,
        output_file=Path(args.output) if args.output else None,
    )

    if result.empty:
        sys.exit(0)

    print("\nDone.")
