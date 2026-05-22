#!/usr/bin/env python3
"""
Scrape player valuation history for coach-related matches using TM API JSON endpoints.

Implemented entry points:
- scrape_by_match_id(match_id, ...): scrape all players (or one team) for a single match
- scrape_by_coach(coach_name/coach_id, ...): pull all match_ids for the coach, then scrape each match

Examples:
    python scripts/scrape_coach_player_valuation_history.py --match-id 4435336
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Ruben Amorim"
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Jose Mourinho"
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Ruben Amorim,Jose Mourinho"
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Jose Mourinho" --include-club-baseline
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Jose Mourinho" --include-club-baseline --baseline-window-years 3
    python scripts/scrape_coach_player_valuation_history.py --coach-name "Jose Mourinho" --skip-processed
    python scripts/scrape_coach_player_valuation_history.py --coach-id 781 --include-club-baseline --skip-processed
"""

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.constants import HEADERS
from services.supabase_service import create_supabase_client
from utils.db_utils import get_seasons_for_club

TM_API_BASE = "https://tmapi-alpha.transfermarkt.technology"
DEFAULT_OUTPUT_DIR = "data/coach_player_valuation_history"
DEFAULT_COACH_NAMES = ["Ruben Amorim", "Jose Mourinho"]


def _normalize_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip().lower()


def _slugify(value: str) -> str:
    text = _normalize_name(value)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "coach"


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class TMApiClient:
    def __init__(self, timeout: int = 40, retries: int = 3, retry_sleep_seconds: float = 1.5):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.timeout = timeout
        self.retries = retries
        self.retry_sleep_seconds = retry_sleep_seconds

        self.club_name_cache: Dict[int, str] = {}
        self.player_profile_cache: Dict[int, Dict[str, Any]] = {}
        self.player_market_cache: Dict[int, List[Dict[str, Any]]] = {}

    def fetch_json(self, endpoint: str) -> Dict[str, Any]:
        url = f"{TM_API_BASE}{endpoint}"
        last_error: Optional[Exception] = None

        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if not payload.get("success", False):
                    raise RuntimeError(f"TM API returned success=false for {endpoint}: {payload.get('message')}")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(self.retry_sleep_seconds * attempt)
                else:
                    raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc

        raise RuntimeError(f"Unreachable code in fetch_json for {url}: {last_error}")

    def get_club_name(self, club_id: Optional[int]) -> Optional[str]:
        if club_id is None or club_id <= 0:
            return None
        if club_id in self.club_name_cache:
            return self.club_name_cache[club_id]

        payload = self.fetch_json(f"/club/{club_id}")
        data = payload.get("data", {})
        name = data.get("name") or data.get("baseDetails", {}).get("shortName") or str(club_id)
        self.club_name_cache[club_id] = name
        return name

    def get_player_profile(self, player_id: int) -> Dict[str, Any]:
        if player_id in self.player_profile_cache:
            return self.player_profile_cache[player_id]

        payload = self.fetch_json(f"/player/{player_id}")
        data = payload.get("data", {})

        transfer_history: List[Dict[str, Any]] = []
        for assignment in data.get("clubAssignments") or []:
            club_id = _to_int(assignment.get("clubId"))
            transfer_history.append(
                {
                    "club_id": club_id,
                    "club_name": self.get_club_name(club_id),
                    "type": assignment.get("type"),
                    "start": assignment.get("start"),
                    "debut": assignment.get("debut"),
                    "shirt_number": assignment.get("shirtNumber"),
                    "is_captain": assignment.get("isCaptain"),
                    "source": "club_assignments",
                }
            )

        # TM player profile often has only current assignment. Reconstruct broader club tenure
        # history from valuation timeline so Player_tenure includes other clubs in the career.
        inferred_tenures = self._infer_tenures_from_market_history(player_id)
        transfer_history = self._merge_tenure_histories(transfer_history, inferred_tenures)

        profile = {
            "player_id": player_id,
            "name": data.get("name"),
            "dob": data.get("lifeDates", {}).get("dateOfBirth"),
            "nationality": data.get("nationalityDetails", {}).get("nationalities", {}).get("nationalityId"),
            "position": data.get("attributes", {}).get("position", {}).get("name"),
            "transfer_history": transfer_history,
        }
        self.player_profile_cache[player_id] = profile
        return profile

    def _infer_tenures_from_market_history(self, player_id: int) -> List[Dict[str, Any]]:
        history = self.get_player_market_value_history(player_id)
        if not history:
            return []

        points = [h for h in history if h.get("club_id") is not None and h.get("date")]
        points.sort(key=lambda h: h.get("date"))
        if not points:
            return []

        tenures: List[Dict[str, Any]] = []
        current = {
            "club_id": points[0].get("club_id"),
            "club_name": points[0].get("club_name"),
            "start": points[0].get("date"),
            "end": points[0].get("date"),
            "type": "historical",
            "debut": None,
            "shirt_number": None,
            "is_captain": False,
            "source": "market_value_history",
        }

        for point in points[1:]:
            point_club_id = point.get("club_id")
            point_date = point.get("date")

            if point_club_id == current.get("club_id"):
                current["end"] = point_date
            else:
                tenures.append(current)
                current = {
                    "club_id": point_club_id,
                    "club_name": point.get("club_name"),
                    "start": point_date,
                    "end": point_date,
                    "type": "historical",
                    "debut": None,
                    "shirt_number": None,
                    "is_captain": False,
                    "source": "market_value_history",
                }

        tenures.append(current)
        return tenures

    def _merge_tenure_histories(
        self,
        primary: List[Dict[str, Any]],
        secondary: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen = set()

        # Prefer clubAssignments for the same (club_id, start) pair and append inferred extras.
        for row in primary + secondary:
            key = (_to_int(row.get("club_id")), row.get("start"))
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

        merged.sort(key=lambda r: (r.get("start") or "", r.get("club_name") or ""))
        return merged

    def get_player_market_value_history(self, player_id: int) -> List[Dict[str, Any]]:
        if player_id in self.player_market_cache:
            return self.player_market_cache[player_id]

        payload = self.fetch_json(f"/player/{player_id}/market-value-history")
        history = payload.get("data", {}).get("history") or []

        mapped: List[Dict[str, Any]] = []
        for item in history:
            market_value = item.get("marketValue") or {}
            club_id = _to_int(item.get("clubId"))
            mapped.append(
                {
                    "date": market_value.get("determined"),
                    "value": market_value.get("value"),
                    "currency": market_value.get("currency"),
                    "club_id": club_id,
                    "club_name": self.get_club_name(club_id),
                    "age": item.get("age"),
                }
            )

        self.player_market_cache[player_id] = mapped
        return mapped


class PlayerDatabaseWriter:
    def __init__(self, db_client: Any):
        self.client = db_client
        self.can_write_player = True
        self.can_write_tenure = True
        self.can_write_valuation = True
        self.can_write_player_match = True

        self.seen_players: Set[int] = set()
        self.seen_tenures: Set[Tuple[int, Optional[int], Optional[str]]] = set()
        self.seen_valuations: Set[Tuple[int, Optional[str], Optional[int], Optional[int]]] = set()
        self.seen_player_matches: Set[Tuple[int, int]] = set()
        self.club_exists_cache: Dict[int, bool] = {}

    def _disable_table(self, table_key: str, reason: str) -> None:
        attr = f"can_write_{table_key}"
        if getattr(self, attr, False):
            setattr(self, attr, False)
            print(f"DB writes disabled for {table_key}: {reason}")

    def _is_rls_error(self, err: Exception) -> bool:
        return "row-level security" in str(err).lower()

    def _resolve_valid_club_id(self, club_id: Optional[int]) -> Optional[int]:
        if club_id is None:
            return None

        if club_id in self.club_exists_cache:
            return club_id if self.club_exists_cache[club_id] else None

        try:
            exists = bool(
                self.client.table("Club").select("tm_club_id").eq("tm_club_id", club_id).limit(1).execute().data
            )
        except Exception:
            exists = False

        self.club_exists_cache[club_id] = exists
        return club_id if exists else None

    def upsert_player(self, player: Dict[str, Any]) -> None:
        if not self.can_write_player:
            return

        player_id = _to_int(player.get("player_id"))
        if player_id is None or player_id in self.seen_players:
            return

        payload = {
            "player_id": player_id,
            "name": player.get("name"),
            "dob": player.get("dob"),
            "nationality": str(player.get("nationality")) if player.get("nationality") is not None else None,
            "position": player.get("position"),
        }

        try:
            self.client.table("Player").upsert(payload, on_conflict="player_id").execute()
            self.seen_players.add(player_id)
        except Exception as exc:
            if self._is_rls_error(exc):
                self._disable_table("player", "RLS policy denied writes to Player. Apply SQL policies or use service role key.")
                return
            raise

    def upsert_player_tenures(self, player_id: int, transfer_history: List[Dict[str, Any]]) -> None:
        if not self.can_write_tenure:
            return

        for tenure in transfer_history:
            club_id = self._resolve_valid_club_id(_to_int(tenure.get("club_id")))
            start_date = tenure.get("start") or tenure.get("debut")
            inferred_end_date = tenure.get("end")
            is_current_tenure = tenure.get("type") == "current"
            dedupe_key = (player_id, club_id, start_date)
            if dedupe_key in self.seen_tenures:
                continue

            row = {
                "player_id": player_id,
                "club_id": club_id,
                "start_date": start_date,
                "end_date": None if is_current_tenure else inferred_end_date,
                "is_current_tenure": is_current_tenure,
            }

            try:
                existing_query = (
                    self.client.table("Player_tenure")
                    .select("id")
                    .eq("player_id", player_id)
                )
                if start_date is None:
                    existing_query = existing_query.is_("start_date", "null")
                else:
                    existing_query = existing_query.eq("start_date", start_date)

                if club_id is None:
                    existing_query = existing_query.is_("club_id", "null")
                else:
                    existing_query = existing_query.eq("club_id", club_id)

                existing = existing_query.limit(1).execute().data

                # Fallback: if legacy row exists with same player/start_date but different/null club_id,
                # update it instead of inserting a duplicate.
                if not existing and start_date is not None:
                    existing = (
                        self.client.table("Player_tenure")
                        .select("id")
                        .eq("player_id", player_id)
                        .eq("start_date", start_date)
                        .limit(1)
                        .execute()
                    ).data

                if existing:
                    tenure_id = existing[0].get("id")
                    self.client.table("Player_tenure").update(row).eq("id", tenure_id).execute()
                else:
                    self.client.table("Player_tenure").insert(row).execute()

                self.seen_tenures.add(dedupe_key)
            except Exception as exc:
                if self._is_rls_error(exc):
                    self._disable_table("tenure", "RLS policy denied writes to Player_tenure. Apply SQL policies or use service role key.")
                    return
                raise

    def refresh_player_match_rows(self, match_id: int, club_id: Optional[int] = None) -> None:
        if not self.can_write_player_match:
            return

        try:
            query = self.client.table("Player_match").delete().eq("match_id", match_id)
            if club_id is None:
                query.execute()
            else:
                query.eq("club_id", club_id).execute()

            # Allow reinsertion in the same run for this match after refresh.
            self.seen_player_matches = {
                key for key in self.seen_player_matches if key[0] != match_id
            }
        except Exception as exc:
            err_text = str(exc)
            if "Could not find the table 'public.Player_match'" in err_text:
                self._disable_table("player_match", "Table Player_match not found. Create it with scripts/create_player_match_table.sql.")
                return
            if self._is_rls_error(exc):
                self._disable_table("player_match", "RLS policy denied deletes on Player_match. Apply SQL policies or use service role key.")
                return
            raise

    def upsert_player_valuations(self, player_id: int, valuation_history: List[Dict[str, Any]]) -> None:
        if not self.can_write_valuation:
            return

        for valuation in valuation_history:
            valuation_date = valuation.get("date")
            club_id = self._resolve_valid_club_id(_to_int(valuation.get("club_id")))
            value = _to_int(valuation.get("value"))
            if valuation_date is None or value is None:
                continue
            dedupe_key = (player_id, valuation_date, club_id, value)
            if dedupe_key in self.seen_valuations:
                continue

            payload = {
                "player_id": player_id,
                "club_id": club_id,
                "valuation_date": valuation_date,
                "market_value_eur": value,
                "currency": valuation.get("currency"),
                "age": _to_int(valuation.get("age")),
            }

            try:
                self.client.table("Player_valuation_history").upsert(
                    payload,
                    on_conflict="player_id,valuation_date,club_id",
                ).execute()
                self.seen_valuations.add(dedupe_key)
            except Exception as exc:
                err_text = str(exc)
                if "Could not find the table 'public.Player_valuation_history'" in err_text:
                    self._disable_table(
                        "valuation",
                        "Table Player_valuation_history not found. Create it with scripts/create_player_valuation_table.sql.",
                    )
                    return
                if self._is_rls_error(exc):
                    self._disable_table(
                        "valuation",
                        "RLS policy denied writes to Player_valuation_history. Apply SQL policies or use service role key.",
                    )
                    return
                raise

    def persist_player_bundle(self, player_row: Dict[str, Any]) -> None:
        player_id = _to_int(player_row.get("player_id"))
        if player_id is None:
            return

        self.upsert_player(player_row)
        self.upsert_player_tenures(player_id, player_row.get("transfer_history") or [])
        self.upsert_player_valuations(player_id, player_row.get("valuation_history") or [])

    def upsert_player_match(self, player_row: Dict[str, Any]) -> None:
        if not self.can_write_player_match:
            return

        player_id = _to_int(player_row.get("player_id"))
        match_id = _to_int(player_row.get("match_id"))
        if player_id is None or match_id is None:
            return

        dedupe_key = (match_id, player_id)
        if dedupe_key in self.seen_player_matches:
            return

        payload = {
            "match_id": match_id,
            "player_id": player_id,
            "club_id": self._resolve_valid_club_id(_to_int(player_row.get("match_team_id"))),
            "match_side": player_row.get("match_side"),
            "squad_role": player_row.get("squad_role"),
            "is_starter": bool(player_row.get("is_starter")),
            "appeared": bool(player_row.get("appeared")),
            "shirt_number": _to_int(player_row.get("shirt_number")),
            "is_captain": bool(player_row.get("is_captain")),
            "position_id": _to_int(player_row.get("position_id")),
            "position_name": player_row.get("position_name"),
            "came_on_minute": _to_int(player_row.get("came_on_minute")),
            "came_on_added_time": _to_int(player_row.get("came_on_added_time")),
            "went_off_minute": _to_int(player_row.get("went_off_minute")),
            "went_off_added_time": _to_int(player_row.get("went_off_added_time")),
        }

        try:
            self.client.table("Player_match").upsert(
                payload,
                on_conflict="match_id,player_id",
            ).execute()
            self.seen_player_matches.add(dedupe_key)
        except Exception as exc:
            err_text = str(exc)
            if "Could not find the table 'public.Player_match'" in err_text:
                self._disable_table("player_match", "Table Player_match not found. Create it with scripts/create_player_match_table.sql.")
                return
            if self._is_rls_error(exc):
                self._disable_table("player_match", "RLS policy denied writes to Player_match. Apply SQL policies or use service role key.")
                return
            raise


def resolve_coach_by_name(client: Any, coach_name: str) -> Dict[str, Any]:
    query = client.table("Coach").select("tm_coach_id,name").ilike("name", f"%{coach_name}%").limit(50).execute()
    candidates = query.data or []

    # Fallback for accent differences: search by tokens (e.g., "Jose Mourinho" -> "mourinho").
    if not candidates:
        token_candidates: Dict[int, Dict[str, Any]] = {}
        for token in [t for t in coach_name.split() if len(t) >= 3]:
            token_query = client.table("Coach").select("tm_coach_id,name").ilike("name", f"%{token}%").limit(50).execute()
            for row in token_query.data or []:
                coach_id = _to_int(row.get("tm_coach_id"))
                if coach_id is not None:
                    token_candidates[coach_id] = row
        candidates = list(token_candidates.values())

    if not candidates:
        raise ValueError(f"No coach found for name '{coach_name}'")

    target = _normalize_name(coach_name)

    # Prefer exact normalized match, else first candidate.
    for row in candidates:
        if _normalize_name(row["name"]) == target:
            return row

    return candidates[0]


def get_match_rows_for_coach(
    client: Any,
    coach_id: int,
    limit_matches: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page_size: int = 1000,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        response = (
            client.table("Match")
            .select("tm_match_id,date,home_coach_id,away_coach_id,home_club_id,away_club_id")
            .or_(f"home_coach_id.eq.{coach_id},away_coach_id.eq.{coach_id}")
            .gte("date", start_date or "1900-01-01")
            .lte("date", end_date or "9999-12-31")
            .order("date", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break

        rows.extend(batch)

        if len(batch) < page_size:
            break

        if limit_matches is not None and len(rows) >= limit_matches:
            break

        offset += page_size

    if limit_matches is not None:
        return rows[:limit_matches]
    return rows


def get_coached_clubs(client: Any, coach_id: int) -> Dict[int, List[Dict[str, Optional[str]]]]:
    response = (
        client.table("Coach_tenure")
        .select("club_id,start_date,end_date")
        .eq("coach_id", coach_id)
        .order("start_date", desc=False)
        .execute()
    )
    rows = response.data or []

    clubs: Dict[int, List[Dict[str, Optional[str]]]] = {}
    for row in rows:
        club_id = _to_int(row.get("club_id"))
        if club_id is None or club_id <= 0:
            continue
        clubs.setdefault(club_id, []).append(
            {
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
            }
        )
    return clubs


def get_match_rows_for_club(
    client: Any,
    club_id: int,
    limit_matches: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page_size: int = 1000,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        response = (
            client.table("Match")
            .select("tm_match_id,date,home_coach_id,away_coach_id,home_club_id,away_club_id")
            .or_(f"home_club_id.eq.{club_id},away_club_id.eq.{club_id}")
            .gte("date", start_date or "1900-01-01")
            .lte("date", end_date or "9999-12-31")
            .order("date", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        if not batch:
            break

        rows.extend(batch)

        if len(batch) < page_size:
            break

        if limit_matches is not None and len(rows) >= limit_matches:
            break

        offset += page_size

    if limit_matches is not None:
        return rows[:limit_matches]
    return rows


def _extract_match_players(
    game_data: Dict[str, Any],
    tm_client: TMApiClient,
    team_filter_club_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    player_rows: List[Dict[str, Any]] = []

    for side_key, side_label in (("homeClub", "home"), ("awayClub", "away")):
        club_data = game_data.get(side_key) or {}
        club_id = _to_int(club_data.get("clubId"))

        if team_filter_club_id is not None and club_id != team_filter_club_id:
            continue

        club_name = tm_client.get_club_name(club_id)

        lineup = club_data.get("lineup") or {}
        starter_players = lineup.get("players") or []
        substitute_players = lineup.get("substitutes") or []
        substitution_actions = club_data.get("actions", {}).get("substitutes") or []

        incoming_index: Dict[int, Dict[str, Any]] = {}
        outgoing_index: Dict[int, Dict[str, Any]] = {}
        for action in substitution_actions:
            outgoing_id = _to_int(action.get("activePlayerId"))
            incoming_id = _to_int(action.get("passivePlayerId"))
            if outgoing_id is not None:
                outgoing_index[outgoing_id] = action
            if incoming_id is not None:
                incoming_index[incoming_id] = action

        seen_ids = set()

        for squad_role, players in (("starter", starter_players), ("substitute", substitute_players)):
            for player in players:
                player_id = _to_int(player.get("id"))
                if player_id is None or player_id in seen_ids:
                    continue
                seen_ids.add(player_id)

                incoming_action = incoming_index.get(player_id, {})
                outgoing_action = outgoing_index.get(player_id, {})
                is_starter = squad_role == "starter"
                appeared = is_starter or bool(incoming_action)
                position = player.get("position") or {}

                player_rows.append(
                    {
                        "player_id": player_id,
                        "match_team_id": club_id,
                        "match_team": club_name,
                        "match_side": side_label,
                        "squad_role": squad_role,
                        "is_starter": is_starter,
                        "appeared": appeared,
                        "shirt_number": player.get("shirtNumber"),
                        "is_captain": player.get("isCaptain"),
                        "position_id": player.get("positionId"),
                        "position_name": position.get("name"),
                        "came_on_minute": incoming_action.get("minute"),
                        "came_on_added_time": incoming_action.get("addedTime"),
                        "went_off_minute": outgoing_action.get("minute"),
                        "went_off_added_time": outgoing_action.get("addedTime"),
                    }
                )

    return player_rows


def _compute_baseline_windows(
    coached_clubs: Dict[int, List[Dict[str, Optional[str]]]],
    window_years: int = 5,
) -> List[Dict[str, Any]]:
    """For each (club_id, tenure) pair, compute the 5-year pre-arrival date window.

    Returns a list of dicts:
        {
            "club_id": int,
            "tenure_start": str (YYYY-MM-DD),
            "window_start": date,
            "window_end": date,
        }
    Tenures without a start_date are skipped.
    """
    windows: List[Dict[str, Any]] = []
    for club_id, periods in coached_clubs.items():
        for period in periods:
            tenure_start_str = period.get("start_date")
            if not tenure_start_str:
                continue
            try:
                tenure_start = date.fromisoformat(tenure_start_str)
            except (ValueError, TypeError):
                continue

            window_end = tenure_start - timedelta(days=1)
            # Subtract window_years years without introducing a dateutil dependency.
            # Handle leap-year edge (Feb 29 → Feb 28 in non-leap target year).
            try:
                window_start = window_end.replace(year=window_end.year - window_years)
            except ValueError:
                window_start = window_end.replace(
                    year=window_end.year - window_years, day=28
                )

            windows.append(
                {
                    "club_id": club_id,
                    "tenure_start": tenure_start_str,
                    "window_start": window_start,
                    "window_end": window_end,
                }
            )
    return windows


def fetch_baseline_match_ids_for_club(
    db_client: Any,
    club_id: int,
    window_start: date,
    window_end: date,
    http_session: requests.Session,
    seasons_cache: Optional[Dict[Tuple[int, int, int], List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """Discover all match IDs for a club within [window_start, window_end].

    Strategy:
    1. Query the Season table for (league_code, season_id) pairs covering the
       window years (result cached in seasons_cache to avoid repeat DB calls
       when a coach has multiple tenures at the same club).
    2. For each pair, hit the Transfermarkt league fixture page to get all
       match IDs and dates for that season.
    3. Filter to matches where the club appears as home or away, and where the
       match date falls within the window.

    Returns a list of dicts: {match_id, date, home_club_id, away_club_id,
    league_id, season_id, tm_code}
    """
    # Season IDs are start-years, e.g. 2019 = 2019/20 season.
    # A window from 2015-06-01 to 2020-05-31 spans seasons 2015 through 2019.
    start_year = window_start.year
    end_year = window_end.year

    cache_key = (club_id, start_year, end_year)
    if seasons_cache is not None and cache_key in seasons_cache:
        seasons = seasons_cache[cache_key]
    else:
        seasons = get_seasons_for_club(
            db_client, club_id, start_year=start_year, end_year=end_year
        )
        if seasons_cache is not None:
            seasons_cache[cache_key] = seasons

    if not seasons:
        print(
            f"  [baseline] No Season rows found for club {club_id} "
            f"between {start_year} and {end_year}"
        )
        return []

    window_start_str = window_start.isoformat()
    window_end_str = window_end.isoformat()

    seen_match_ids: Set[int] = set()
    result: List[Dict[str, Any]] = []

    for season_row in seasons:
        league_code = season_row.get("tm_code")
        season_id = season_row.get("season_id")
        league_id = season_row.get("league_id")

        if not league_code or season_id is None:
            continue

        try:
            from pages.league_page_matches import LeaguePageMatches

            league_page = LeaguePageMatches(
                session=http_session,
                league_code=league_code,
                season_id=int(season_id),
            )
            matches = league_page.get_matches()
        except Exception as exc:
            print(
                f"  [baseline] Failed to fetch fixtures for league={league_code} "
                f"season={season_id}: {exc}"
            )
            continue

        for m in matches:
            m_id = m.get("match_id")
            m_date = m.get("date")
            home_id = m.get("home_club_id")
            away_id = m.get("away_club_id")

            if m_id is None or m_date is None:
                continue

            # Filter to matches within the window
            if not (window_start_str <= m_date <= window_end_str):
                continue

            # Filter to matches involving this club
            try:
                home_id_int = int(home_id) if home_id is not None else None
                away_id_int = int(away_id) if away_id is not None else None
            except (TypeError, ValueError):
                continue

            if club_id != home_id_int and club_id != away_id_int:
                continue

            m_id_int = int(m_id)
            if m_id_int in seen_match_ids:
                continue

            seen_match_ids.add(m_id_int)
            result.append(
                {
                    "match_id": m_id_int,
                    "date": m_date,
                    "home_club_id": home_id_int,
                    "away_club_id": away_id_int,
                    "league_id": league_id,
                    "season_id": season_id,
                    "tm_code": league_code,
                }
            )

    result.sort(key=lambda r: r.get("date") or "")
    return result


def ensure_matches_in_db(
    db_client: Any,
    http_session: requests.Session,
    match_metas: List[Dict[str, Any]],
    existing_match_ids: Optional[Set[int]] = None,
) -> Set[int]:
    """Ensure each match exists in the Match table.

    For matches already present, this is a no-op. For missing ones, the
    Transfermarkt match report page is scraped and a minimal Match row is
    inserted.

    Args:
        existing_match_ids: Optional pre-loaded set of match IDs already in the
            DB. Pass this in when calling across multiple windows to avoid
            re-fetching the full Match table on every call. The set is mutated
            in-place as new matches are inserted, so callers accumulate state
            across successive calls automatically.

    Returns the set of match_ids that are now confirmed in the DB (either
    pre-existing or newly inserted). Matches that fail to scrape are omitted.
    """
    from pages.match_page import MatchPage, MissingCoachException, MissingResultException
    from services.match_service import MatchService

    if not match_metas:
        return set()

    # Fetch all match IDs currently in the DB only if not provided by caller.
    if existing_match_ids is None:
        existing_response = db_client.table("Match").select("tm_match_id").execute()
        existing_match_ids = {
            int(r["tm_match_id"]) for r in (existing_response.data or []) if r.get("tm_match_id")
        }

    confirmed: Set[int] = set()

    for meta in match_metas:
        match_id = meta.get("match_id")
        if match_id is None:
            continue

        if match_id in existing_match_ids:
            confirmed.add(match_id)
            continue

        league_id = meta.get("league_id")
        season_id = meta.get("season_id")

        if league_id is None or season_id is None:
            print(
                f"  [baseline] Skipping match {match_id}: missing league_id or season_id"
            )
            continue

        try:
            page = MatchPage(match_id=match_id, session=http_session)
            match = MatchService.parse(league_id, int(season_id), page)
            db_client.table("Match").upsert(
                match.model_dump(mode="json"), on_conflict="tm_match_id"
            ).execute()
            # Mutate the shared set so subsequent windows skip this match too.
            existing_match_ids.add(match_id)
            confirmed.add(match_id)
            print(f"  [baseline] Inserted missing match {match_id} into DB")
        except (MissingCoachException, MissingResultException) as exc:
            print(f"  [baseline] Skipping match {match_id}: {exc}")
        except Exception as exc:
            print(f"  [baseline] Failed to insert match {match_id}: {exc}")

    return confirmed


def scrape_by_match_id(
    match_id: int,
    tm_client: Optional[TMApiClient] = None,
    team_filter_club_id: Optional[int] = None,
    db_writer: Optional[PlayerDatabaseWriter] = None,
    processed_match_ids: Optional[Set[int]] = None,
) -> Dict[str, Any]:
    """Scrape and persist player data for a single match.

    Args:
        processed_match_ids: If provided, the match is skipped (returning an
            empty stub) when its match_id is already present in this set,
            indicating it was fully processed in a prior run. The set is NOT
            mutated here; callers update it after a successful scrape.
    """
    tm_client = tm_client or TMApiClient()

    # Skip if already fully processed in a previous run.
    if processed_match_ids is not None and match_id in processed_match_ids:
        print(f"  [skip] match {match_id} already processed — skipping TM API call")
        return {
            "match_id": match_id,
            "skipped": True,
            "players": [],
            "player_count": 0,
        }

    game_payload = tm_client.fetch_json(f"/game/{match_id}")
    game_data = game_payload.get("data", {})
    game_date = game_data.get("baseDetails", {}).get("date", {}).get("dateTimeUTC")

    # Derive human-readable team names for progress output
    home_club_data = game_data.get("homeClub") or {}
    away_club_data = game_data.get("awayClub") or {}
    home_name = home_club_data.get("clubName") or f"club_{home_club_data.get('clubId', '?')}"
    away_name = away_club_data.get("clubName") or f"club_{away_club_data.get('clubId', '?')}"
    match_date_short = (game_date or "")[:10]
    print(f"  [match {match_id}] {home_name} vs {away_name}  ({match_date_short})")

    match_players = _extract_match_players(
        game_data=game_data,
        tm_client=tm_client,
        team_filter_club_id=team_filter_club_id,
    )

    total_players = len(match_players)
    print(f"  [match {match_id}] {total_players} players to process")

    if db_writer is not None:
        db_writer.refresh_player_match_rows(match_id=match_id, club_id=team_filter_club_id)

    enriched_players: List[Dict[str, Any]] = []

    for p_idx, base_player in enumerate(match_players, start=1):
        player_id = base_player["player_id"]
        profile = tm_client.get_player_profile(player_id)
        valuation_history = tm_client.get_player_market_value_history(player_id)

        player_name = profile.get("name") or f"player_{player_id}"
        team_label = base_player.get("match_team") or str(base_player.get("match_team_id", "?"))
        role_label = base_player.get("squad_role", "")
        val_count = len(valuation_history)
        print(
            f"    [{p_idx}/{total_players}] {player_name} (id={player_id}, {team_label}, "
            f"{role_label}, {val_count} valuations)"
        )

        enriched_players.append(
            {
                "game_player": f"{match_id}-{player_id}",
                "match_id": match_id,
                "match_date": game_date,
                "match_team": base_player["match_team"],
                "match_team_id": base_player["match_team_id"],
                "match_side": base_player["match_side"],
                "squad_role": base_player["squad_role"],
                "is_starter": base_player["is_starter"],
                "appeared": base_player["appeared"],
                "shirt_number": base_player["shirt_number"],
                "is_captain": base_player["is_captain"],
                "position_id": base_player["position_id"],
                "position_name": base_player["position_name"],
                "came_on_minute": base_player["came_on_minute"],
                "came_on_added_time": base_player["came_on_added_time"],
                "went_off_minute": base_player["went_off_minute"],
                "went_off_added_time": base_player["went_off_added_time"],
                "player_id": player_id,
                "name": profile.get("name"),
                "dob": profile.get("dob"),
                "nationality": profile.get("nationality"),
                "position": profile.get("position"),
                "transfer_history": profile.get("transfer_history"),
                "valuation_history": valuation_history,
            }
        )

        if db_writer is not None:
            db_writer.persist_player_bundle(enriched_players[-1])
            db_writer.upsert_player_match(enriched_players[-1])

    print(f"  [match {match_id}] done — {len(enriched_players)} players scraped")
    return {
        "match_id": match_id,
        "match_date": game_date,
        "players": enriched_players,
        "player_count": len(enriched_players),
    }


def scrape_by_coach(
    coach_name: Optional[str] = None,
    coach_id: Optional[int] = None,
    limit_matches: Optional[int] = None,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    tm_client: Optional[TMApiClient] = None,
    persist_to_db: bool = True,
    start_date: str = "2010-01-01",
    end_date: Optional[str] = None,
    include_club_baseline: bool = False,
    baseline_window_years: int = 5,
    skip_processed: bool = False,
) -> Dict[str, Any]:
    if coach_name is None and coach_id is None:
        raise ValueError("Either coach_name or coach_id must be provided")

    db_client = create_supabase_client()
    tm_client = tm_client or TMApiClient()
    db_writer = PlayerDatabaseWriter(db_client) if persist_to_db else None

    resolved_name = coach_name
    resolved_id = coach_id

    if resolved_id is None:
        row = resolve_coach_by_name(db_client, coach_name or "")
        resolved_id = _to_int(row.get("tm_coach_id"))
        resolved_name = row.get("name")
    elif resolved_name is None:
        row = db_client.table("Coach").select("name").eq("tm_coach_id", resolved_id).limit(1).execute().data
        if row:
            resolved_name = row[0].get("name")
        else:
            resolved_name = f"coach_{resolved_id}"

    if resolved_id is None:
        raise ValueError(f"Unable to resolve coach id from input: {coach_name}")

    # Fetch the set of match_ids already fully processed (have rows in Player_match).
    # Done once here and shared across both the coach match loop and the baseline loop.
    processed_match_ids: Optional[Set[int]] = None
    if skip_processed:
        print("[skip] Fetching already-processed match IDs from Player_match...")
        _pm_resp = db_client.table("Player_match").select("match_id").execute()
        processed_match_ids = {
            int(r["match_id"]) for r in (_pm_resp.data or []) if r.get("match_id")
        }
        print(f"[skip] {len(processed_match_ids)} match IDs already processed — will be skipped")

    match_rows = get_match_rows_for_coach(
        client=db_client,
        coach_id=resolved_id,
        limit_matches=limit_matches,
        start_date=start_date,
        end_date=end_date,
    )

    by_match_id: Dict[str, Any] = {}
    match_ids: List[int] = []
    by_team: Dict[str, Dict[str, Any]] = {}
    team_match_cache: Dict[Tuple[int, Optional[int]], Dict[str, Any]] = {}

    for idx, row in enumerate(match_rows, start=1):
        match_id = _to_int(row.get("tm_match_id"))
        if match_id is None:
            continue

        home_coach_id = _to_int(row.get("home_coach_id"))
        away_coach_id = _to_int(row.get("away_coach_id"))
        home_club_id = _to_int(row.get("home_club_id"))
        away_club_id = _to_int(row.get("away_club_id"))

        coached_club_id: Optional[int] = None
        if home_coach_id == resolved_id:
            coached_club_id = home_club_id
        elif away_coach_id == resolved_id:
            coached_club_id = away_club_id

        if coached_club_id is not None and coached_club_id <= 0:
            coached_club_id = None

        match_date = row.get("date", "unknown date")
        print(f"\n[{idx}/{len(match_rows)}] Match {match_id}  ({match_date})  coach={resolved_name}")

        match_data = scrape_by_match_id(
            match_id=match_id,
            tm_client=tm_client,
            team_filter_club_id=coached_club_id,
            db_writer=db_writer,
            processed_match_ids=processed_match_ids,
        )
        # After a successful (non-skipped) scrape, mark as processed so the
        # baseline loop won't re-scrape the same match in this session.
        if processed_match_ids is not None and not match_data.get("skipped"):
            processed_match_ids.add(match_id)
        team_match_cache[(match_id, coached_club_id)] = match_data

        by_match_id[str(match_id)] = match_data
        match_ids.append(match_id)

        team_key = str(coached_club_id) if coached_club_id is not None else "unknown"
        coached_club_name = None
        if coached_club_id is not None and coached_club_id > 0:
            try:
                coached_club_name = tm_client.get_club_name(coached_club_id)
            except Exception:
                coached_club_name = None
        if team_key not in by_team:
            by_team[team_key] = {
                "team_id": coached_club_id,
                "team_name": coached_club_name,
                "match_ids": [],
                "matches": {},
            }
        by_team[team_key]["match_ids"].append(match_id)
        by_team[team_key]["matches"][str(match_id)] = match_data

    club_baseline: Dict[str, Dict[str, Any]] = {}
    if include_club_baseline:
        coached_clubs = get_coached_clubs(db_client, resolved_id)
        windows = _compute_baseline_windows(coached_clubs, window_years=baseline_window_years)

        if not windows:
            print("[baseline] No tenure windows found — skipping club baseline scrape")
        else:
            # One shared HTTP session for all league fixture page scrapes
            baseline_http_session = requests.Session()
            baseline_http_session.headers.update(HEADERS)

            # --- Session-level caches (shared across all windows) ---

            # Full set of match IDs in the DB, fetched once and mutated in-place
            # by ensure_matches_in_db as new rows are inserted.
            print("[baseline] Fetching existing Match IDs from DB (once)...")
            _existing_response = db_client.table("Match").select("tm_match_id").execute()
            shared_existing_match_ids: Set[int] = {
                int(r["tm_match_id"])
                for r in (_existing_response.data or [])
                if r.get("tm_match_id")
            }
            print(f"[baseline] {len(shared_existing_match_ids)} match IDs loaded from DB")

            # confirmed_ids accumulated across all windows — avoids re-confirming
            # the same match_id when a coach has multiple tenures at the same club.
            shared_confirmed_ids: Set[int] = set()

            # club_rows_cache: (club_id, window_start_iso, window_end_iso) -> rows
            # Avoids re-querying the DB for clubs/windows already fetched.
            club_rows_cache: Dict[Tuple[int, str, str], List[Dict[str, Any]]] = {}

            # seasons_cache: (club_id, start_year, end_year) -> season rows
            # Avoids re-querying Season table for the same club/year range.
            seasons_cache: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = {}

            for window in windows:
                club_id = window["club_id"]
                tenure_start_str = window["tenure_start"]
                window_start: date = window["window_start"]
                window_end: date = window["window_end"]
                window_start_iso = window_start.isoformat()
                window_end_iso = window_end.isoformat()

                print(
                    f"[baseline] club={club_id} tenure_start={tenure_start_str} "
                    f"window={window_start_iso} → {window_end_iso}"
                )

                try:
                    club_name = tm_client.get_club_name(club_id)
                except Exception:
                    club_name = None

                # 1. Discover match IDs for this club in the baseline window,
                #    passing the seasons_cache so Season DB calls are not repeated.
                baseline_metas = fetch_baseline_match_ids_for_club(
                    db_client=db_client,
                    club_id=club_id,
                    window_start=window_start,
                    window_end=window_end,
                    http_session=baseline_http_session,
                    seasons_cache=seasons_cache,
                )
                print(
                    f"  [baseline] Found {len(baseline_metas)} candidate matches "
                    f"for club {club_id} in window"
                )

                # 2. Ensure all candidate matches exist in the Match table.
                #    shared_existing_match_ids is mutated in-place as rows are added.
                new_confirmed = ensure_matches_in_db(
                    db_client=db_client,
                    http_session=baseline_http_session,
                    match_metas=baseline_metas,
                    existing_match_ids=shared_existing_match_ids,
                )
                shared_confirmed_ids |= new_confirmed

                # 3. Fetch confirmed baseline rows from DB, using cache to avoid
                #    re-querying the same (club_id, window) on a second tenure.
                cache_key = (club_id, window_start_iso, window_end_iso)
                if cache_key not in club_rows_cache:
                    club_rows_cache[cache_key] = get_match_rows_for_club(
                        client=db_client,
                        club_id=club_id,
                        start_date=window_start_iso,
                        end_date=window_end_iso,
                    )
                db_baseline_rows = club_rows_cache[cache_key]

                # Keep only matches where this coach was NOT the coach (pre-arrival)
                pre_arrival_rows = [
                    r for r in db_baseline_rows
                    if _to_int(r.get("home_coach_id")) != resolved_id
                    and _to_int(r.get("away_coach_id")) != resolved_id
                ]
                print(
                    f"  [baseline] {len(pre_arrival_rows)} pre-arrival matches after "
                    f"filtering out coach's own matches"
                )

                # 4. Scrape player data for each pre-arrival baseline match.
                #    team_match_cache prevents duplicate TM API /game calls.
                #    shared_confirmed_ids prevents re-processing matches confirmed
                #    in earlier windows.
                baseline_match_ids: List[int] = []
                baseline_matches: Dict[str, Any] = {}

                for b_idx, row in enumerate(pre_arrival_rows, start=1):
                    match_id = _to_int(row.get("tm_match_id"))
                    if match_id is None:
                        continue
                    if match_id not in shared_confirmed_ids:
                        continue

                    match_date = row.get("date", "unknown date")
                    print(
                        f"\n  [baseline {b_idx}/{len(pre_arrival_rows)}] "
                        f"Match {match_id}  ({match_date})  club={club_name or club_id}"
                    )

                    cached = team_match_cache.get((match_id, club_id))
                    if cached is None:
                        try:
                            cached = scrape_by_match_id(
                                match_id=match_id,
                                tm_client=tm_client,
                                team_filter_club_id=club_id,
                                db_writer=db_writer,
                                processed_match_ids=processed_match_ids,
                            )
                            # Mark as processed after a successful (non-skipped) scrape.
                            if processed_match_ids is not None and not cached.get("skipped"):
                                processed_match_ids.add(match_id)
                            team_match_cache[(match_id, club_id)] = cached
                        except Exception as exc:
                            print(
                                f"  [baseline] Failed to scrape match {match_id}: {exc}"
                            )
                            continue

                    baseline_match_ids.append(match_id)
                    baseline_matches[str(match_id)] = {
                        "is_baseline": True,
                        "baseline_tenure_start": tenure_start_str,
                        "coach_in_match": (
                            _to_int(row.get("home_coach_id")) == resolved_id
                            or _to_int(row.get("away_coach_id")) == resolved_id
                        ),
                        "data": cached,
                    }

                # 5. Accumulate into the per-club structure, grouped by tenure
                club_key = str(club_id)
                if club_key not in club_baseline:
                    club_baseline[club_key] = {
                        "team_id": club_id,
                        "team_name": club_name,
                        "tenures": [],
                    }

                club_baseline[club_key]["tenures"].append(
                    {
                        "tenure_start": tenure_start_str,
                        "baseline_window": {
                            "from": window_start.isoformat(),
                            "to": window_end.isoformat(),
                        },
                        "match_ids": baseline_match_ids,
                        "matches": baseline_matches,
                        "total_matches": len(baseline_match_ids),
                    }
                )

    result = {
        "coach": {
            "coach_id": resolved_id,
            "name": resolved_name,
        },
        "filters": {
            "start_date": start_date,
            "end_date": end_date,
            "baseline_window_years": baseline_window_years if include_club_baseline else None,
        },
        "match_ids": match_ids,
        "matches": by_match_id,
        "teams": by_team,
        "club_baseline": club_baseline,
        "total_matches": len(match_ids),
    }

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    slug = _slugify(resolved_name or str(resolved_id))
    coach_file = output_path / f"coach_{resolved_id}_{slug}.json"
    coach_file.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Saved coach output to: {coach_file}")
    return result


def parse_coach_names(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [name.strip() for name in raw.split(",") if name.strip()]


def parse_coach_ids(raw: Optional[str]) -> List[int]:
    if not raw:
        return []
    ids: List[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        ids.append(int(token))
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape player valuation history by match_id or by coach (pulling all match_ids)."
    )
    parser.add_argument("--match-id", type=int, help="Single match_id to scrape")
    parser.add_argument(
        "--coach-name",
        type=str,
        help="Coach name(s), comma-separated. Example: 'Ruben Amorim,Jose Mourinho'",
    )
    parser.add_argument(
        "--coach-id",
        type=str,
        help="Coach id(s), comma-separated. Example: '65202,781'",
    )
    parser.add_argument(
        "--limit-matches",
        type=int,
        default=None,
        help="Optional limit for number of matches per coach.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2010-01-01",
        help="Lower bound match date inclusive (YYYY-MM-DD). Default: 2010-01-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=date.today().isoformat(),
        help="Upper bound match date inclusive (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Disable DB persistence (JSON output only).",
    )
    parser.add_argument(
        "--include-club-baseline",
        action="store_true",
        help="Also crawl all pre-arrival matches in a rolling window for every club this coach has coached.",
    )
    parser.add_argument(
        "--skip-processed",
        action="store_true",
        help=(
            "Skip matches that already have rows in Player_match (i.e. were fully "
            "scraped in a prior run). Enables safe resumption without double-processing."
        ),
    )
    parser.add_argument(
        "--baseline-window-years",
        type=int,
        default=5,
        help="Number of years to look back before each coach tenure start date for the club baseline (default: 5).",
    )

    args = parser.parse_args()

    tm_client = TMApiClient()
    persist_to_db = not args.no_db

    if args.match_id:
        db_writer = PlayerDatabaseWriter(create_supabase_client()) if persist_to_db else None
        single_processed: Optional[Set[int]] = None
        if args.skip_processed:
            _pm = db_writer.client if db_writer else create_supabase_client()
            _resp = _pm.table("Player_match").select("match_id").execute()
            single_processed = {int(r["match_id"]) for r in (_resp.data or []) if r.get("match_id")}
            print(f"[skip] {len(single_processed)} match IDs already processed")
        result = scrape_by_match_id(
            match_id=args.match_id,
            tm_client=tm_client,
            db_writer=db_writer,
            processed_match_ids=single_processed,
        )

        output_path = Path(args.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        match_file = output_path / f"match_{args.match_id}.json"
        match_file.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved match output to: {match_file}")
        return

    coach_names = parse_coach_names(args.coach_name)
    coach_ids = parse_coach_ids(args.coach_id)

    if not coach_names and not coach_ids:
        coach_names = DEFAULT_COACH_NAMES

    for coach_name in coach_names:
        try:
            scrape_by_coach(
                coach_name=coach_name,
                coach_id=None,
                limit_matches=args.limit_matches,
                output_dir=args.output_dir,
                tm_client=tm_client,
                persist_to_db=persist_to_db,
                start_date=args.start_date,
                end_date=args.end_date,
                include_club_baseline=args.include_club_baseline,
                baseline_window_years=args.baseline_window_years,
                skip_processed=args.skip_processed,
            )
        except Exception as exc:
            print(f"Failed scraping coach '{coach_name}': {exc}")

    for coach_id in coach_ids:
        try:
            scrape_by_coach(
                coach_name=None,
                coach_id=coach_id,
                limit_matches=args.limit_matches,
                output_dir=args.output_dir,
                tm_client=tm_client,
                persist_to_db=persist_to_db,
                start_date=args.start_date,
                end_date=args.end_date,
                include_club_baseline=args.include_club_baseline,
                baseline_window_years=args.baseline_window_years,
                skip_processed=args.skip_processed,
            )
        except Exception as exc:
            print(f"Failed scraping coach_id '{coach_id}': {exc}")


if __name__ == "__main__":
    main()
