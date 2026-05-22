"""
Unit tests for scripts/update_odds_pipeline.py

Coverage:
  - _check_file: missing file, empty file, valid file
  - _print_summary: smoke test (doesn't raise)
  - step1_scrape: success, scrape returns empty + no existing file, exception
  - step2_match_oddscheck: success, empty result, exception, missing prerequisite
  - step3_augment: success, subprocess failure, missing prerequisite
  - step4_update_odds: success, subprocess failure, missing prerequisite
  - step5_populate_mte: success, subprocess failure, exception
  - run_pipeline:
      - all steps skipped → True
      - dry_run forces step 4+5 skip
      - step1 failure → fast-fail + returns False
      - step2 failure → fast-fail
      - step3 failure → fast-fail
      - step4 failure → fast-fail
      - step5 failure → fast-fail
      - full success → True, correct result keys
      - skip_scrape does not call step1
      - skip_matching does not call step2
"""

import time
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import subprocess

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_completed_process(returncode: int = 0, stdout: str = "", stderr: str = "") -> MagicMock:
    proc = MagicMock(spec=subprocess.CompletedProcess)
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


# ---------------------------------------------------------------------------
# _check_file
# ---------------------------------------------------------------------------

class TestCheckFile:
    def test_missing_file_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import _check_file
        result = _check_file(tmp_path / "nonexistent.csv", "some step")
        assert result is False
        out = capsys.readouterr().out
        assert "not found" in out

    def test_empty_file_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import _check_file
        f = tmp_path / "empty.csv"
        f.write_text("")
        result = _check_file(f, "some step")
        assert result is False
        out = capsys.readouterr().out
        assert "empty" in out

    def test_valid_file_returns_true(self, tmp_path):
        from scripts.update_odds_pipeline import _check_file
        f = tmp_path / "valid.csv"
        f.write_text("col1,col2\nfoo,bar\n")
        assert _check_file(f, "some step") is True


# ---------------------------------------------------------------------------
# _print_summary
# ---------------------------------------------------------------------------

class TestPrintSummary:
    def test_does_not_raise_with_all_states(self, capsys):
        from scripts.update_odds_pipeline import _print_summary
        results = {
            "step1_scrape": True,
            "step2_matching": False,
            "step3_augment": None,
            "step4_odds_update": True,
            "step5_mte": None,
        }
        _print_summary(results, time.time() - 5)
        out = capsys.readouterr().out
        assert "OK" in out
        assert "FAIL" in out
        assert "SKIP" in out


# ---------------------------------------------------------------------------
# step1_scrape
# ---------------------------------------------------------------------------

class TestStep1Scrape:
    def test_success_returns_true(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step1_scrape
        import pandas as pd

        fake_df = pd.DataFrame({"Date": ["01/08/2025"], "HomeTeam": ["A"], "AwayTeam": ["B"]})
        all_csv = tmp_path / "all.csv"
        all_csv.write_text("col\nval\n")

        # step1_scrape does `from scripts.scrape_football_data import run` lazily;
        # patch at the module level so the import resolves to our fake.
        with patch("scripts.scrape_football_data.run", return_value=fake_df), \
             patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_csv):
            result = step1_scrape(season="2526", force=False)

        assert result is True

    def test_scrape_returns_empty_and_no_existing_file_is_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step1_scrape
        import pandas as pd

        with patch("scripts.scrape_football_data.run", return_value=pd.DataFrame()), \
             patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", tmp_path / "missing.csv"):
            result = step1_scrape(season="2526", force=False)

        assert result is False

    def test_exception_in_scrape_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step1_scrape

        with patch("scripts.scrape_football_data.run", side_effect=RuntimeError("boom")):
            result = step1_scrape(season="2526", force=False)

        assert result is False
        out = capsys.readouterr().out
        assert "ERROR" in out


# ---------------------------------------------------------------------------
# step2_match_oddscheck
# ---------------------------------------------------------------------------

class TestStep2MatchOddscheck:
    def test_returns_false_when_prerequisite_missing(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step2_match_oddscheck
        with patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", tmp_path / "missing.csv"):
            result = step2_match_oddscheck()
        assert result is False

    def test_success_returns_true(self, tmp_path):
        from scripts.update_odds_pipeline import step2_match_oddscheck
        import pandas as pd

        fake_mapping = pd.DataFrame({"club_name": ["Arsenal"], "db_club_id": [1]})
        all_leagues = tmp_path / "all.csv"
        all_leagues.write_text("col\nval\n")

        # Inject a fake scripts.match_oddscheck_to_db into sys.modules so the
        # lazy `from scripts.match_oddscheck_to_db import ...` never loads the
        # real file (which requires thefuzz / supabase).
        fake_mod = types.ModuleType("scripts.match_oddscheck_to_db")
        fake_mod.match_clubs_to_database = MagicMock(return_value=fake_mapping)
        with patch.dict(sys.modules, {"scripts.match_oddscheck_to_db": fake_mod}), \
             patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_leagues):
            result = step2_match_oddscheck()

        assert result is True

    def test_empty_result_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step2_match_oddscheck
        import pandas as pd

        all_leagues = tmp_path / "all.csv"
        all_leagues.write_text("col\nval\n")

        fake_mod = types.ModuleType("scripts.match_oddscheck_to_db")
        fake_mod.match_clubs_to_database = MagicMock(return_value=pd.DataFrame())
        with patch.dict(sys.modules, {"scripts.match_oddscheck_to_db": fake_mod}), \
             patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_leagues):
            result = step2_match_oddscheck()

        assert result is False

    def test_exception_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step2_match_oddscheck

        all_leagues = tmp_path / "all.csv"
        all_leagues.write_text("col\nval\n")

        fake_mod = types.ModuleType("scripts.match_oddscheck_to_db")
        fake_mod.match_clubs_to_database = MagicMock(side_effect=Exception("db down"))
        with patch.dict(sys.modules, {"scripts.match_oddscheck_to_db": fake_mod}), \
             patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_leagues):
            result = step2_match_oddscheck()

        assert result is False


# ---------------------------------------------------------------------------
# step3_augment
# ---------------------------------------------------------------------------

class TestStep3Augment:
    def test_returns_false_when_all_leagues_missing(self, tmp_path):
        from scripts.update_odds_pipeline import step3_augment
        with patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", tmp_path / "missing.csv"), \
             patch("scripts.update_odds_pipeline.MAPPING_FILE", tmp_path / "mapping.csv"):
            result = step3_augment()
        assert result is False

    def test_returns_false_when_mapping_missing(self, tmp_path):
        from scripts.update_odds_pipeline import step3_augment
        all_f = tmp_path / "all.csv"
        all_f.write_text("col\nval\n")
        with patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_f), \
             patch("scripts.update_odds_pipeline.MAPPING_FILE", tmp_path / "missing_mapping.csv"):
            result = step3_augment()
        assert result is False

    def test_subprocess_failure_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step3_augment
        all_f = tmp_path / "all.csv"
        mapping_f = tmp_path / "mapping.csv"
        all_f.write_text("col\nval\n")
        mapping_f.write_text("col\nval\n")

        with patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_f), \
             patch("scripts.update_odds_pipeline.MAPPING_FILE", mapping_f), \
             patch("subprocess.run", return_value=_make_completed_process(returncode=1, stderr="fail")):
            result = step3_augment()

        assert result is False

    def test_subprocess_success_returns_true(self, tmp_path):
        from scripts.update_odds_pipeline import step3_augment
        all_f = tmp_path / "all.csv"
        mapping_f = tmp_path / "mapping.csv"
        aug_f = tmp_path / "augmented.csv"
        all_f.write_text("col\nval\n")
        mapping_f.write_text("col\nval\n")
        aug_f.write_text("col\nval\n")

        with patch("scripts.update_odds_pipeline.ALL_LEAGUES_FULL", all_f), \
             patch("scripts.update_odds_pipeline.MAPPING_FILE", mapping_f), \
             patch("scripts.update_odds_pipeline.AUGMENTED_FILE", aug_f), \
             patch("subprocess.run", return_value=_make_completed_process(returncode=0)):
            result = step3_augment()

        assert result is True


# ---------------------------------------------------------------------------
# step4_update_odds
# ---------------------------------------------------------------------------

class TestStep4UpdateOdds:
    def test_returns_false_when_augmented_missing(self, tmp_path):
        from scripts.update_odds_pipeline import step4_update_odds
        with patch("scripts.update_odds_pipeline.AUGMENTED_FILE", tmp_path / "missing.csv"):
            result = step4_update_odds()
        assert result is False

    def test_subprocess_failure_returns_false(self, tmp_path, capsys):
        from scripts.update_odds_pipeline import step4_update_odds
        aug_f = tmp_path / "aug.csv"
        aug_f.write_text("col\nval\n")
        with patch("scripts.update_odds_pipeline.AUGMENTED_FILE", aug_f), \
             patch("subprocess.run", return_value=_make_completed_process(returncode=1)):
            result = step4_update_odds()
        assert result is False

    def test_subprocess_success_returns_true(self, tmp_path):
        from scripts.update_odds_pipeline import step4_update_odds
        aug_f = tmp_path / "aug.csv"
        aug_f.write_text("col\nval\n")
        with patch("scripts.update_odds_pipeline.AUGMENTED_FILE", aug_f), \
             patch("subprocess.run", return_value=_make_completed_process(returncode=0)):
            result = step4_update_odds()
        assert result is True


# ---------------------------------------------------------------------------
# step5_populate_mte
# ---------------------------------------------------------------------------

class TestStep5PopulateMte:
    def test_subprocess_failure_returns_false(self, capsys):
        from scripts.update_odds_pipeline import step5_populate_mte
        with patch("subprocess.run", return_value=_make_completed_process(returncode=1)):
            result = step5_populate_mte()
        assert result is False

    def test_subprocess_success_returns_true(self):
        from scripts.update_odds_pipeline import step5_populate_mte
        with patch("subprocess.run", return_value=_make_completed_process(returncode=0)):
            result = step5_populate_mte()
        assert result is True

    def test_exception_returns_false(self, capsys):
        from scripts.update_odds_pipeline import step5_populate_mte
        with patch("subprocess.run", side_effect=OSError("binary not found")):
            result = step5_populate_mte()
        assert result is False


# ---------------------------------------------------------------------------
# run_pipeline — orchestration logic
# ---------------------------------------------------------------------------

class TestRunPipeline:
    """Tests for the top-level run_pipeline orchestrator."""

    def _all_skip(self):
        """Kwargs that skip every step."""
        return dict(
            skip_scrape=True,
            skip_matching=True,
            skip_augment=True,
            skip_odds_update=True,
            skip_mte=True,
        )

    def test_all_steps_skipped_returns_true(self):
        from scripts.update_odds_pipeline import run_pipeline
        result = run_pipeline(**self._all_skip())
        assert result is True

    def test_dry_run_forces_steps_4_and_5_skipped(self):
        from scripts.update_odds_pipeline import run_pipeline

        calls = []

        def fake_step3():
            calls.append("step3")
            return True

        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds") as mock4, \
             patch("scripts.update_odds_pipeline.step5_populate_mte") as mock5:
            result = run_pipeline(dry_run=True)

        assert result is True
        mock4.assert_not_called()
        mock5.assert_not_called()

    def test_step1_failure_fast_fails(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=False), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck") as mock2:
            result = run_pipeline()
        assert result is False
        mock2.assert_not_called()

    def test_step2_failure_fast_fails(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=False), \
             patch("scripts.update_odds_pipeline.step3_augment") as mock3:
            result = run_pipeline()
        assert result is False
        mock3.assert_not_called()

    def test_step3_failure_fast_fails(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=False), \
             patch("scripts.update_odds_pipeline.step4_update_odds") as mock4:
            result = run_pipeline()
        assert result is False
        mock4.assert_not_called()

    def test_step4_failure_fast_fails(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=False), \
             patch("scripts.update_odds_pipeline.step5_populate_mte") as mock5:
            result = run_pipeline()
        assert result is False
        mock5.assert_not_called()

    def test_step5_failure_returns_false(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=True), \
             patch("scripts.update_odds_pipeline.step5_populate_mte", return_value=False):
            result = run_pipeline()
        assert result is False

    def test_all_steps_success_returns_true(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=True), \
             patch("scripts.update_odds_pipeline.step5_populate_mte", return_value=True):
            result = run_pipeline()
        assert result is True

    def test_skip_scrape_does_not_call_step1(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape") as mock1, \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True), \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=True), \
             patch("scripts.update_odds_pipeline.step5_populate_mte", return_value=True):
            run_pipeline(skip_scrape=True)
        mock1.assert_not_called()

    def test_skip_matching_does_not_call_step2(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck") as mock2, \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=True), \
             patch("scripts.update_odds_pipeline.step5_populate_mte", return_value=True):
            run_pipeline(skip_matching=True)
        mock2.assert_not_called()

    def test_min_confidence_forwarded_to_step2(self):
        from scripts.update_odds_pipeline import run_pipeline
        with patch("scripts.update_odds_pipeline.step1_scrape", return_value=True), \
             patch("scripts.update_odds_pipeline.step2_match_oddscheck", return_value=True) as mock2, \
             patch("scripts.update_odds_pipeline.step3_augment", return_value=True), \
             patch("scripts.update_odds_pipeline.step4_update_odds", return_value=True), \
             patch("scripts.update_odds_pipeline.step5_populate_mte", return_value=True):
            run_pipeline(min_confidence=85)
        mock2.assert_called_once_with(min_confidence=85, no_resume=False)
