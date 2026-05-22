#!/usr/bin/env python3
"""
Odds pipeline orchestrator — runs all 5 steps end-to-end.

Steps:
  1. scrape_football_data   — download CSVs from football-data.co.uk
  2. match_oddscheck_to_db  — fuzzy-match team names → DB club_id
  3. augment_all_leagues    — join mapping back onto the full CSV
  4. update_odds_batch      — bulk-update Match.odds_home/draw/away in DB
  5. populate_match_team_expectation — compute xPts / delta_pts, write MTE table

Any step can be skipped with the corresponding --skip-* flag, which is useful
when you've already done part of the work manually or want to re-run from a
specific point.

Usage:
    python scripts/update_odds_pipeline.py                    # current season, all steps
    python scripts/update_odds_pipeline.py --season 2526      # specific season
    python scripts/update_odds_pipeline.py --skip-scrape      # skip step 1 (CSV already fresh)
    python scripts/update_odds_pipeline.py --skip-matching    # skip step 2 (mapping already current)
    python scripts/update_odds_pipeline.py --skip-augment     # skip step 3
    python scripts/update_odds_pipeline.py --skip-odds-update # skip step 4
    python scripts/update_odds_pipeline.py --skip-mte         # skip step 5
    python scripts/update_odds_pipeline.py --force-scrape     # force re-download even if unchanged
    python scripts/update_odds_pipeline.py --dry-run          # steps 1-3 only (no DB writes)
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / "data"
SCRIPTS_DIR = Path(__file__).parent

# Required intermediate files
ALL_LEAGUES_FULL = DATA_DIR / "all_leagues_full.csv"
MAPPING_FILE = DATA_DIR / "oddscheck_to_db_mapping.csv"
AUGMENTED_FILE = DATA_DIR / "all_leagues_full_augmented.csv"


# ---------------------------------------------------------------------------
# Step runners
# ---------------------------------------------------------------------------

def _separator(title: str) -> None:
    width = 72
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def _check_file(path: Path, label: str) -> bool:
    """Return True if a required file exists and is non-empty."""
    if not path.exists():
        print(f"[ERROR] Required file not found: {path}")
        print(f"        ({label} must be run first)")
        return False
    if path.stat().st_size == 0:
        print(f"[ERROR] Required file is empty: {path}")
        return False
    return True


def step1_scrape(
    season: Optional[str],
    force: bool,
    session=None,
) -> bool:
    """
    Download CSVs from football-data.co.uk and merge into all_leagues_full.csv.

    Returns True on success.
    """
    _separator("STEP 1 / 5 — Download odds CSVs from football-data.co.uk")

    # Import here so the module is testable in isolation
    from scripts.scrape_football_data import run as scrape_run

    try:
        df = scrape_run(
            season=season,
            all_seasons=(season is None),
            force=force,
            session=session,
        )
        if df.empty and not ALL_LEAGUES_FULL.exists():
            print("[ERROR] Scrape returned no data and no existing file found.")
            return False
        print(f"[OK] Step 1 complete — {len(df):,} rows in {ALL_LEAGUES_FULL}")
        return True
    except Exception as exc:
        print(f"[ERROR] Step 1 failed: {exc}")
        return False


def step2_match_oddscheck(
    min_confidence: int = 70,
    no_resume: bool = False,
) -> bool:
    """
    Fuzzy-match team names from all_leagues_full.csv to DB club IDs.
    Appends new matches to oddscheck_to_db_mapping.csv (precious file — never overwritten).

    Returns True on success.
    """
    _separator("STEP 2 / 5 — Fuzzy-match team names → DB club IDs")

    if not _check_file(ALL_LEAGUES_FULL, "Step 1 (scrape)"):
        return False

    from scripts.match_oddscheck_to_db import match_clubs_to_database

    try:
        result_df = match_clubs_to_database(
            min_confidence=min_confidence,
            resume=not no_resume,
        )
        if result_df is None or result_df.empty:
            print("[WARN] Step 2 produced no mapping rows. Check DB connectivity.")
            return False
        print(
            f"[OK] Step 2 complete — {len(result_df):,} club mappings in {MAPPING_FILE}"
        )
        return True
    except Exception as exc:
        print(f"[ERROR] Step 2 failed: {exc}")
        return False


def step3_augment() -> bool:
    """
    Join the mapping back onto all_leagues_full.csv to add TM club IDs.
    Outputs all_leagues_full_augmented.csv.

    Returns True on success.
    """
    _separator("STEP 3 / 5 — Augment CSV with TM club IDs")

    if not _check_file(ALL_LEAGUES_FULL, "Step 1 (scrape)"):
        return False
    if not _check_file(MAPPING_FILE, "Step 2 (match_oddscheck_to_db)"):
        return False

    # augment_all_leagues uses hardcoded relative paths — run via subprocess
    # so it picks up the correct working directory (repo root).
    try:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "augment_all_leagues_with_mapping.py")],
            cwd=str(SCRIPTS_DIR.parent),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"[ERROR] Step 3 failed (exit {result.returncode}):")
            print(result.stderr[-2000:] if result.stderr else "(no stderr)")
            return False

        if not _check_file(AUGMENTED_FILE, "Step 3 output"):
            return False

        print(f"[OK] Step 3 complete — augmented file at {AUGMENTED_FILE}")
        return True
    except Exception as exc:
        print(f"[ERROR] Step 3 failed: {exc}")
        return False


def step4_update_odds() -> bool:
    """
    Batch-update Match.odds_home / odds_draw / odds_away in the database.

    Returns True on success.
    """
    _separator("STEP 4 / 5 — Batch update odds in the database")

    if not _check_file(AUGMENTED_FILE, "Step 3 (augment)"):
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "update_odds_batch.py")],
            cwd=str(SCRIPTS_DIR.parent),
            capture_output=True,
            text=True,
        )
        # Print output so the user sees progress notes
        if result.stdout:
            print(result.stdout[-4000:])
        if result.returncode != 0:
            print(f"[ERROR] Step 4 failed (exit {result.returncode}):")
            print(result.stderr[-2000:] if result.stderr else "(no stderr)")
            return False

        print("[OK] Step 4 complete")
        return True
    except Exception as exc:
        print(f"[ERROR] Step 4 failed: {exc}")
        return False


def step5_populate_mte() -> bool:
    """
    Compute xPts / delta_pts and upsert into match_team_expectation.

    Returns True on success.
    """
    _separator("STEP 5 / 5 — Populate match_team_expectation table")

    try:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "populate_match_team_expectation.py")],
            cwd=str(SCRIPTS_DIR.parent),
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout[-4000:])
        if result.returncode != 0:
            print(f"[ERROR] Step 5 failed (exit {result.returncode}):")
            print(result.stderr[-2000:] if result.stderr else "(no stderr)")
            return False

        print("[OK] Step 5 complete")
        return True
    except Exception as exc:
        print(f"[ERROR] Step 5 failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    season: Optional[str] = None,
    force_scrape: bool = False,
    skip_scrape: bool = False,
    skip_matching: bool = False,
    skip_augment: bool = False,
    skip_odds_update: bool = False,
    skip_mte: bool = False,
    dry_run: bool = False,
    min_confidence: int = 70,
    no_resume: bool = False,
) -> bool:
    """
    Run the full odds pipeline.

    Args:
        season:           Season code to scrape (e.g. '2526'). None = current season.
        force_scrape:     Pass --force to step 1 (ignore Last-Modified cache).
        skip_scrape:      Skip step 1.
        skip_matching:    Skip step 2 (fuzzy matching).
        skip_augment:     Skip step 3 (augment CSV).
        skip_odds_update: Skip step 4 (DB odds update).
        skip_mte:         Skip step 5 (match_team_expectation).
        dry_run:          Run steps 1–3 only; skip any DB writes (steps 4–5).
        min_confidence:   Minimum fuzzy-match confidence for step 2.
        no_resume:        Do not resume step 2 from checkpoint.

    Returns:
        True if all executed steps succeeded, False otherwise.
    """
    start = time.time()
    results: dict[str, bool] = {}

    print("\n" + "=" * 72)
    print("  ODDS PIPELINE — football-data.co.uk → Supabase")
    print("=" * 72)
    if dry_run:
        print("  [DRY RUN] Steps 4 and 5 will be skipped (no DB writes).")

    # Step 1
    if not skip_scrape:
        ok = step1_scrape(season=season, force=force_scrape)
        results["step1_scrape"] = ok
        if not ok:
            _print_summary(results, start)
            return False
    else:
        print("\n[SKIP] Step 1 — scrape (--skip-scrape)")
        results["step1_scrape"] = None

    # Step 2
    if not skip_matching:
        ok = step2_match_oddscheck(
            min_confidence=min_confidence,
            no_resume=no_resume,
        )
        results["step2_matching"] = ok
        if not ok:
            _print_summary(results, start)
            return False
    else:
        print("\n[SKIP] Step 2 — matching (--skip-matching)")
        results["step2_matching"] = None

    # Step 3
    if not skip_augment:
        ok = step3_augment()
        results["step3_augment"] = ok
        if not ok:
            _print_summary(results, start)
            return False
    else:
        print("\n[SKIP] Step 3 — augment (--skip-augment)")
        results["step3_augment"] = None

    # Steps 4–5 are skipped in dry-run mode
    if dry_run:
        skip_odds_update = True
        skip_mte = True

    # Step 4
    if not skip_odds_update:
        ok = step4_update_odds()
        results["step4_odds_update"] = ok
        if not ok:
            _print_summary(results, start)
            return False
    else:
        print("\n[SKIP] Step 4 — odds DB update (--skip-odds-update or --dry-run)")
        results["step4_odds_update"] = None

    # Step 5
    if not skip_mte:
        ok = step5_populate_mte()
        results["step5_mte"] = ok
        if not ok:
            _print_summary(results, start)
            return False
    else:
        print("\n[SKIP] Step 5 — match_team_expectation (--skip-mte or --dry-run)")
        results["step5_mte"] = None

    _print_summary(results, start)
    return True


def _print_summary(results: dict, start: float) -> None:
    elapsed = time.time() - start
    print("\n" + "=" * 72)
    print("  PIPELINE SUMMARY")
    print("=" * 72)
    labels = {
        "step1_scrape": "Step 1 — scrape CSVs",
        "step2_matching": "Step 2 — fuzzy match",
        "step3_augment": "Step 3 — augment",
        "step4_odds_update": "Step 4 — odds DB update",
        "step5_mte": "Step 5 — match_team_expectation",
    }
    for key, label in labels.items():
        status = results.get(key)
        if status is None:
            tag = "SKIP"
        elif status:
            tag = "OK  "
        else:
            tag = "FAIL"
        print(f"  [{tag}] {label}")
    print(f"\n  Total time: {elapsed/60:.1f} min")
    print("=" * 72)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full odds pipeline from football-data.co.uk to Supabase."
    )
    parser.add_argument(
        "--season",
        metavar="SSSS",
        help="Season code (e.g. 2526). Defaults to current season.",
    )
    parser.add_argument(
        "--force-scrape",
        action="store_true",
        help="Ignore Last-Modified cache; always re-download CSV files.",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip step 1 (CSV already up-to-date).",
    )
    parser.add_argument(
        "--skip-matching",
        action="store_true",
        help="Skip step 2 (team-name → club_id mapping already current).",
    )
    parser.add_argument(
        "--skip-augment",
        action="store_true",
        help="Skip step 3 (augmented CSV already exists).",
    )
    parser.add_argument(
        "--skip-odds-update",
        action="store_true",
        help="Skip step 4 (DB odds update).",
    )
    parser.add_argument(
        "--skip-mte",
        action="store_true",
        help="Skip step 5 (match_team_expectation ETL).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run steps 1-3 only; skip any database writes.",
    )
    parser.add_argument(
        "--min-confidence",
        type=int,
        default=70,
        metavar="N",
        help="Minimum fuzzy-match confidence for step 2 (default: 70).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume step 2 from checkpoint; restart from scratch.",
    )

    args = parser.parse_args()

    success = run_pipeline(
        season=args.season,
        force_scrape=args.force_scrape,
        skip_scrape=args.skip_scrape,
        skip_matching=args.skip_matching,
        skip_augment=args.skip_augment,
        skip_odds_update=args.skip_odds_update,
        skip_mte=args.skip_mte,
        dry_run=args.dry_run,
        min_confidence=args.min_confidence,
        no_resume=args.no_resume,
    )

    sys.exit(0 if success else 1)
