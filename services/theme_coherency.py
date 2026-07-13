from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now

from services.config import Settings, load_settings
from services.theme_leadership import (
    ThemeLeadershipRebuildResult,
    rebuild_theme_leadership,
)
from services.theme_service import list_top_theme_snapshots_for_dashboard

DB_TOP_SOURCE = "THEME_LATEST_SNAPSHOT"
FLOW_LEADERSHIP_SOURCE = "THEME_FLOW_SNAPSHOT"


def build_theme_coherency_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    leadership_result: ThemeLeadershipRebuildResult | None = None,
    db_top_rows: Sequence[Mapping[str, Any]] | None = None,
    limit: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    observed_at = now or utc_now()
    bounded_limit = min(max(int(limit), 1), 100)
    db_rows = list(db_top_rows) if db_top_rows is not None else (
        list_top_theme_snapshots_for_dashboard(connection, limit=bounded_limit)
    )
    leadership = leadership_result or rebuild_theme_leadership(
        connection,
        write_candidate_sources=False,
        settings=resolved_settings,
    )
    db_items = [
        _standardize_db_item(row, observed_at=observed_at)
        for row in db_rows[:bounded_limit]
    ]
    leadership_items = [
        _standardize_leadership_item(
            snapshot.to_dict(include_members=False),
            observed_at=observed_at,
            watchset_selection_source=leadership.watchset_selection_source,
        )
        for snapshot in leadership.snapshots[:bounded_limit]
    ]

    current_by_theme = _latest_pointer_rows(
        connection,
        [str(item["theme_id"]) for item in leadership_items],
    )
    latest_theme_count = _latest_theme_count(connection)
    snapshot_mismatch_count = 0
    missing_snapshot_count = 0
    source_mismatch_count = 0
    failure_reasons: list[str] = []
    warning_reasons: list[str] = []

    for item in leadership_items:
        current = current_by_theme.get(str(item["theme_id"]))
        if item["source"] != FLOW_LEADERSHIP_SOURCE:
            if current is not None:
                source_mismatch_count += 1
            continue
        if current is None:
            missing_snapshot_count += 1
            failure_reasons.append("THEME_FLOW_SNAPSHOT_POINTER_MISSING")
            continue
        if item.get("snapshot_id") != current.get("snapshot_id"):
            snapshot_mismatch_count += 1
            failure_reasons.append("THEME_FLOW_SNAPSHOT_POINTER_MISMATCH")
        if _wire_timestamp(item.get("calculated_at")) != _wire_timestamp(
            current.get("calculated_at")
        ):
            snapshot_mismatch_count += 1
            failure_reasons.append("THEME_FLOW_SNAPSHOT_CALCULATED_AT_MISMATCH")

    comparison_count = min(len(db_items), len(leadership_items))
    db_theme_ids = {
        str(item["theme_id"]) for item in db_items[:comparison_count]
    }
    leadership_theme_ids = {
        str(item["theme_id"]) for item in leadership_items[:comparison_count]
    }
    top_set_mismatch_count = len(db_theme_ids ^ leadership_theme_ids)
    overlap_count = len(db_theme_ids & leadership_theme_ids)
    if source_mismatch_count:
        warning_reasons.append("THEME_LEADERSHIP_SOURCE_DIFFERS_FROM_DB_TOP")
    if top_set_mismatch_count and db_items and leadership_items:
        warning_reasons.append("THEME_TOP_SET_MISMATCH")
    if not db_items and latest_theme_count:
        warning_reasons.append("THEME_DB_TOP_TRADABLE_EMPTY")
    if not leadership_items and latest_theme_count:
        warning_reasons.append("THEME_LEADERSHIP_EMPTY")
    if latest_theme_count == 0 and not leadership_items:
        warning_reasons.append("THEME_COHERENCY_DATA_EMPTY")

    stale_count = sum(
        1
        for item in (*db_items, *leadership_items)
        if item.get("data_age_sec") is None
        or float(item["data_age_sec"]) > resolved_settings.theme_snapshot_stale_sec
    )
    if stale_count:
        warning_reasons.append("THEME_SNAPSHOT_STALE")

    reason_codes = _dedupe([*failure_reasons, *warning_reasons])
    status = "FAIL" if failure_reasons else "WARN" if warning_reasons else "PASS"
    return {
        "status": status,
        "generated_at": datetime_to_wire(observed_at),
        "source": "THEME_DASHBOARD_COHERENCY",
        "db_top": _source_summary(
            db_items,
            source=DB_TOP_SOURCE,
            watchset_selection_source=None,
        ),
        "leadership": _source_summary(
            leadership_items,
            source=_aggregate_source(leadership_items),
            watchset_selection_source=leadership.watchset_selection_source,
        ),
        "db_top_items": db_items,
        "leadership_items": leadership_items,
        "latest_theme_count": latest_theme_count,
        "db_top_count": len(db_items),
        "leadership_top_count": len(leadership_items),
        "top_comparison_count": comparison_count,
        "overlap_count": overlap_count,
        "top_set_mismatch_count": top_set_mismatch_count,
        "source_mismatch_count": source_mismatch_count,
        "snapshot_mismatch_count": snapshot_mismatch_count,
        "missing_snapshot_count": missing_snapshot_count,
        "stale_count": stale_count,
        "reason_codes": reason_codes,
        "snapshot_stale_sec": resolved_settings.theme_snapshot_stale_sec,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def standardize_db_theme_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    observed_at = now or utc_now()
    return [_standardize_db_item(row, observed_at=observed_at) for row in rows]


def standardize_leadership_rows(
    result: ThemeLeadershipRebuildResult,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    observed_at = now or utc_now()
    return [
        _standardize_leadership_item(
            snapshot.to_dict(include_members=False),
            observed_at=observed_at,
            watchset_selection_source=result.watchset_selection_source,
        )
        for snapshot in result.snapshots
    ]


def _standardize_db_item(
    row: Mapping[str, Any],
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    item = dict(row)
    item["source"] = DB_TOP_SOURCE
    item["data_age_sec"] = _age_seconds(item.get("calculated_at"), now=observed_at)
    item["age_sec"] = item["data_age_sec"]
    item["watchset_selection_source"] = None
    return item


def _standardize_leadership_item(
    row: Mapping[str, Any],
    *,
    observed_at: datetime,
    watchset_selection_source: str | None,
) -> dict[str, Any]:
    item = dict(row)
    item["data_age_sec"] = _age_seconds(item.get("calculated_at"), now=observed_at)
    item["age_sec"] = item["data_age_sec"]
    item["watchset_selection_source"] = watchset_selection_source
    return item


def _source_summary(
    items: Sequence[Mapping[str, Any]],
    *,
    source: str,
    watchset_selection_source: str | None,
) -> dict[str, Any]:
    snapshot_ids = _dedupe(
        [str(item["snapshot_id"]) for item in items if item.get("snapshot_id")]
    )
    calculated_values = [
        str(item["calculated_at"]) for item in items if item.get("calculated_at")
    ]
    ages = [float(item["data_age_sec"]) for item in items if item.get("data_age_sec") is not None]
    return {
        "source": source,
        "snapshot_id": snapshot_ids[0] if len(snapshot_ids) == 1 else None,
        "snapshot_ids": snapshot_ids,
        "calculated_at": min(calculated_values) if calculated_values else None,
        "latest_calculated_at": max(calculated_values) if calculated_values else None,
        "data_age_sec": max(ages) if ages else None,
        "watchset_selection_source": watchset_selection_source,
        "theme_count": len(items),
    }


def _aggregate_source(items: Sequence[Mapping[str, Any]]) -> str:
    sources = _dedupe([str(item.get("source") or "UNKNOWN") for item in items])
    if not sources:
        return "NONE"
    if len(sources) == 1:
        return sources[0]
    return "MIXED"


def _latest_pointer_rows(
    connection: sqlite3.Connection,
    theme_ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    normalized = _dedupe([theme_id for theme_id in theme_ids if theme_id])
    if not normalized:
        return {}
    placeholders = ", ".join("?" for _ in normalized)
    rows = connection.execute(
        f"""
        SELECT theme_id, snapshot_id, calculated_at
        FROM theme_latest_snapshots
        WHERE theme_id IN ({placeholders})
        """,
        tuple(normalized),
    ).fetchall()
    return {str(row["theme_id"]): dict(row) for row in rows}


def _latest_theme_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM theme_latest_snapshots"
    ).fetchone()
    return int(row["count"] if row is not None else 0)


def _age_seconds(value: Any, *, now: datetime) -> float | None:
    if value in (None, ""):
        return None
    try:
        observed_at = parse_timestamp(value, "calculated_at")
    except (TypeError, ValueError):
        return None
    return round(max((now - observed_at).total_seconds(), 0.0), 3)


def _wire_timestamp(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime_to_wire(parse_timestamp(value, "calculated_at"))
    except (TypeError, ValueError):
        return None


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))
