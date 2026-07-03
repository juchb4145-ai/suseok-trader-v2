from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import parse_timestamp, utc_now
from storage.event_store import get_gateway_status_values

from services.config import Settings, load_settings

_ROOT_CAUSE_KEYS = (
    "NO_OBSERVED_MEMBERS",
    "LOW_FRESH_COVERAGE",
    "TICK_MISSING",
    "TICK_STALE",
    "BAR_MISSING",
    "VWAP_MISSING",
)


def build_theme_data_wait_diagnostics(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest_rows = _latest_snapshot_rows(connection)
    active_member_rows = _active_member_tick_rows(connection)
    bar_codes = _codes_with_bar(connection, interval_sec=60)
    vwap_codes = _codes_with_vwap(connection)
    registered_codes = set(_registered_realtime_codes(connection))

    snapshot_by_theme = {str(row["theme_id"]): row for row in latest_rows}
    state_quality_distribution: Counter[str] = Counter()
    data_wait_reason_counts: Counter[str] = Counter()
    snapshot_reason_counts: Counter[str] = Counter()
    root_cause_counts: Counter[str] = Counter({key: 0 for key in _ROOT_CAUSE_KEYS})
    theme_members: dict[str, list[sqlite3.Row]] = defaultdict(list)

    for row in latest_rows:
        state = str(row["state"] or "UNKNOWN").upper()
        quality = str(row["quality_status"] or "UNKNOWN").upper()
        state_quality_distribution[f"{state}|{quality}"] += 1
        reasons = _json_array(row["reason_codes_json"])
        snapshot_reason_counts.update(reasons)
        if state == "DATA_WAIT":
            data_wait_reason_counts.update(reasons)

    for row in active_member_rows:
        theme_members[str(row["theme_id"])].append(row)

    theme_metrics = []
    for theme_id, members in sorted(
        theme_members.items(),
        key=lambda item: (str(item[1][0]["theme_name"] or ""), item[0]),
    ):
        snapshot = snapshot_by_theme.get(theme_id)
        active_count = len(members)
        tick_count = 0
        fresh_10 = fresh_30 = fresh_60 = 0
        registered_count = 0
        decomposition = Counter({key: 0 for key in _ROOT_CAUSE_KEYS})
        for member in members:
            code = str(member["code"])
            if code in registered_codes:
                registered_count += 1
            tick_age = _age_seconds(member["event_ts"])
            if member["event_ts"] is None:
                decomposition["TICK_MISSING"] += 1
            else:
                tick_count += 1
                if tick_age is None or tick_age > resolved_settings.market_data_tick_stale_sec:
                    decomposition["TICK_STALE"] += 1
                if tick_age is not None and tick_age <= 10:
                    fresh_10 += 1
                if tick_age is not None and tick_age <= 30:
                    fresh_30 += 1
                if tick_age is not None and tick_age <= 60:
                    fresh_60 += 1
            if code not in bar_codes:
                decomposition["BAR_MISSING"] += 1
            if code not in vwap_codes:
                decomposition["VWAP_MISSING"] += 1

        snapshot_reasons = _json_array(snapshot["reason_codes_json"]) if snapshot else []
        if "NO_OBSERVED_MEMBERS" in snapshot_reasons:
            decomposition["NO_OBSERVED_MEMBERS"] += 1
        if "LOW_FRESH_COVERAGE" in snapshot_reasons:
            decomposition["LOW_FRESH_COVERAGE"] += 1
        root_cause_counts.update(decomposition)
        required_fresh = math.ceil(
            active_count * resolved_settings.theme_min_fresh_coverage_ratio
        )
        max_per_theme = resolved_settings.realtime_subscription_max_per_theme
        impossible = required_fresh > max_per_theme
        theme_metrics.append(
            {
                "theme_id": theme_id,
                "theme_name": str(members[0]["theme_name"] or theme_id),
                "state": snapshot["state"] if snapshot else None,
                "quality_status": snapshot["quality_status"] if snapshot else None,
                "reason_codes": snapshot_reasons,
                "active_member_count": _snapshot_int(
                    snapshot,
                    "active_member_count",
                    active_count,
                ),
                "observed_member_count": _snapshot_int(snapshot, "observed_member_count", 0),
                "fresh_member_count": _snapshot_int(snapshot, "fresh_member_count", 0),
                "fresh_coverage_ratio": _snapshot_float(
                    snapshot,
                    "fresh_coverage_ratio",
                    _ratio(fresh_60, active_count),
                ),
                "tick_coverage": {
                    "member_count": active_count,
                    "tick_count": tick_count,
                    "coverage_ratio": _ratio(tick_count, active_count),
                    "registered_count": registered_count,
                },
                "fresh_coverage_windows": {
                    "10s": {"count": fresh_10, "ratio": _ratio(fresh_10, active_count)},
                    "30s": {"count": fresh_30, "ratio": _ratio(fresh_30, active_count)},
                    "60s": {"count": fresh_60, "ratio": _ratio(fresh_60, active_count)},
                },
                "subscription_capacity": {
                    "max_total": resolved_settings.realtime_subscription_max_total,
                    "max_per_theme": max_per_theme,
                    "required_fresh_for_reference_threshold": required_fresh,
                    "coverage_impossible": impossible,
                    "reason_codes": (
                        ["REFERENCE_COVERAGE_IMPOSSIBLE_BY_SUBSCRIPTION_LIMIT"]
                        if impossible
                        else []
                    ),
                },
                "root_cause_breakdown": dict(decomposition),
            }
        )

    total_required_fresh = sum(
        item["subscription_capacity"]["required_fresh_for_reference_threshold"]
        for item in theme_metrics
    )
    coverage_impossible_theme_count = sum(
        1 for item in theme_metrics if item["subscription_capacity"]["coverage_impossible"]
    )
    top_data_wait = [
        item
        for item in theme_metrics
        if str(item.get("state") or "").upper() == "DATA_WAIT"
    ]
    top_data_wait.sort(
        key=lambda item: (
            -int(item["subscription_capacity"]["coverage_impossible"]),
            -int(item["active_member_count"]),
            item["theme_name"],
            item["theme_id"],
        )
    )

    return {
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "snapshot_count": len(latest_rows),
        "state_quality_distribution": dict(state_quality_distribution),
        "snapshot_reason_counts": dict(snapshot_reason_counts),
        "data_wait_reason_counts": dict(data_wait_reason_counts),
        "root_cause_summary": dict(root_cause_counts),
        "theme_member_tick_coverage": {
            "active_member_rows": len(active_member_rows),
            "member_tick_count": sum(
                1 for row in active_member_rows if row["event_ts"] is not None
            ),
            "coverage_ratio": _ratio(
                sum(1 for row in active_member_rows if row["event_ts"] is not None),
                len(active_member_rows),
            ),
        },
        "fresh_window_summary": _fresh_window_summary(active_member_rows),
        "subscription_capacity": {
            "max_total": resolved_settings.realtime_subscription_max_total,
            "max_per_theme": resolved_settings.realtime_subscription_max_per_theme,
            "registered_count": len(registered_codes),
            "total_required_fresh_for_reference_threshold": total_required_fresh,
            "total_coverage_impossible": (
                total_required_fresh > resolved_settings.realtime_subscription_max_total
            ),
            "coverage_impossible_theme_count": coverage_impossible_theme_count,
        },
        "themes": theme_metrics[: _bounded_limit(limit)],
        "top_data_wait_themes": top_data_wait[: min(_bounded_limit(limit), 20)],
    }


def build_naver_leading_theme_overlap_report(
    connection: sqlite3.Connection,
    *,
    limit: int = 50,
) -> dict[str, Any]:
    leading_rows = connection.execute(
        """
        SELECT theme_id, theme_name, leading_code, state, calculated_at
        FROM theme_latest_snapshots
        WHERE state = 'LEADING'
        ORDER BY flow_score DESC, total_trade_value DESC, theme_name ASC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    reports = []
    overlap_ratios = []
    for row in leading_rows:
        system_codes = _active_theme_codes(connection, row["theme_id"], source_type=None)
        naver_codes = _active_theme_codes(
            connection,
            row["theme_id"],
            source_type="NAVER_REFERENCE",
        )
        if not naver_codes:
            naver_codes = _naver_codes_by_theme_name(connection, row["theme_name"])
        overlap = sorted(system_codes & naver_codes)
        denominator = len(system_codes | naver_codes)
        overlap_ratio = _ratio(len(overlap), denominator)
        overlap_ratios.append(overlap_ratio)
        reports.append(
            {
                "theme_id": row["theme_id"],
                "theme_name": row["theme_name"],
                "state": row["state"],
                "calculated_at": row["calculated_at"],
                "system_member_count": len(system_codes),
                "naver_reference_member_count": len(naver_codes),
                "overlap_count": len(overlap),
                "overlap_ratio": overlap_ratio,
                "overlap_codes": overlap[:20],
                "reason_codes": [] if naver_codes else ["NAVER_REFERENCE_THEME_MISSING"],
            }
        )
    return {
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "leading_theme_count": len(leading_rows),
        "avg_overlap_ratio": (
            sum(overlap_ratios) / len(overlap_ratios) if overlap_ratios else 0.0
        ),
        "themes": reports,
    }


def _active_theme_codes(
    connection: sqlite3.Connection,
    theme_id: str,
    *,
    source_type: str | None,
) -> set[str]:
    clauses = ["theme_id = ?", "active = 1"]
    params: list[Any] = [theme_id]
    if source_type is not None:
        clauses.append("source_type = ?")
        params.append(source_type)
    rows = connection.execute(
        f"""
        SELECT code
        FROM theme_members
        WHERE {" AND ".join(clauses)}
        """,
        tuple(params),
    ).fetchall()
    return {str(row["code"]) for row in rows}


def _naver_codes_by_theme_name(connection: sqlite3.Connection, theme_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT m.code
        FROM theme_members AS m
        JOIN themes AS t ON t.theme_id = m.theme_id
        WHERE t.theme_name = ?
            AND m.source_type = 'NAVER_REFERENCE'
            AND m.active = 1
        """,
        (theme_name,),
    ).fetchall()
    return {str(row["code"]) for row in rows}


def _latest_snapshot_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            s.theme_id,
            s.theme_name,
            s.state,
            s.quality_status,
            s.reason_codes_json,
            s.active_member_count,
            s.observed_member_count,
            s.fresh_member_count,
            s.fresh_coverage_ratio
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshots AS s ON s.snapshot_id = l.snapshot_id
        ORDER BY s.theme_name ASC, s.theme_id ASC
        """
    ).fetchall()


def _active_member_tick_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            t.theme_id,
            t.theme_name,
            m.code,
            mt.event_ts,
            mt.quality_status
        FROM theme_members AS m
        JOIN themes AS t ON t.theme_id = m.theme_id
        LEFT JOIN market_ticks_latest AS mt ON mt.code = m.code AND mt.exchange = 'KRX'
        WHERE t.active = 1 AND m.active = 1
        ORDER BY t.theme_name ASC, t.theme_id ASC, m.code ASC
        """
    ).fetchall()


def _codes_with_bar(connection: sqlite3.Connection, *, interval_sec: int) -> set[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT code
        FROM market_minute_bars
        WHERE exchange = 'KRX' AND interval_sec = ?
        """,
        (interval_sec,),
    ).fetchall()
    return {str(row["code"]) for row in rows}


def _codes_with_vwap(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        """
        SELECT DISTINCT code
        FROM market_minute_bars
        WHERE exchange = 'KRX' AND vwap IS NOT NULL
        """
    ).fetchall()
    return {str(row["code"]) for row in rows}


def _registered_realtime_codes(connection: sqlite3.Connection) -> list[str]:
    value = get_gateway_status_values(connection).get("realtime_registered_codes")
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError:
            loaded = stripped.split(",")
    else:
        loaded = value
    if not isinstance(loaded, list | tuple | set):
        loaded = [loaded]
    return [str(item).strip() for item in loaded if str(item).strip()]


def _fresh_window_summary(rows: Sequence[sqlite3.Row]) -> dict[str, dict[str, float | int]]:
    counts = {"10s": 0, "30s": 0, "60s": 0}
    for row in rows:
        age = _age_seconds(row["event_ts"])
        if age is None:
            continue
        if age <= 10:
            counts["10s"] += 1
        if age <= 30:
            counts["30s"] += 1
        if age <= 60:
            counts["60s"] += 1
    total = len(rows)
    return {
        key: {"count": value, "ratio": _ratio(value, total)}
        for key, value in counts.items()
    }


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = parse_timestamp(value, "timestamp")
    except ValueError:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _json_array(value: object) -> list[str]:
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]


def _snapshot_int(row: Mapping[str, Any] | None, key: str, default: int) -> int:
    if row is None:
        return default
    try:
        return int(row[key])
    except (KeyError, TypeError, ValueError):
        return default


def _snapshot_float(row: Mapping[str, Any] | None, key: str, default: float) -> float:
    if row is None:
        return default
    try:
        return float(row[key])
    except (KeyError, TypeError, ValueError):
        return default


def _ratio(numerator: int | float, denominator: int | float) -> float:
    denominator_float = float(denominator)
    if denominator_float <= 0:
        return 0.0
    return float(numerator) / denominator_float


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
