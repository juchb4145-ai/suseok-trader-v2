from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any

from domain.broker.utils import BrokerValidationError, parse_timestamp, utc_now, validate_stock_code
from storage.event_store import get_gateway_status_values

from services.config import Settings, load_settings
from services.theme_leadership.models import ThemeUniverseMember


class ThemeUniverseBuilder:
    """Builds an active theme universe from v2 Theme Service membership tables."""

    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def build(
        self,
        connection: sqlite3.Connection,
        *,
        active_only: bool = True,
    ) -> list[ThemeUniverseMember]:
        where = "WHERE t.active = 1 AND m.active = 1" if active_only else ""
        rows = connection.execute(
            f"""
            SELECT
                t.theme_id,
                t.theme_name,
                m.code,
                m.name,
                m.weight,
                m.source_type,
                m.source_name,
                m.active,
                m.metadata_json
            FROM theme_members AS m
            JOIN themes AS t ON t.theme_id = m.theme_id
            {where}
            ORDER BY t.theme_name ASC, m.weight DESC, m.code ASC
            """
        ).fetchall()
        context = _observable_context(connection, settings=self.settings)
        return [_row_to_member(row, context=context) for row in rows]


def _row_to_member(row: sqlite3.Row, *, context: Mapping[str, Any]) -> ThemeUniverseMember:
    metadata: dict[str, Any]
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except json.JSONDecodeError:
        metadata = {"metadata_decode_error": True}
    metadata = {
        **metadata,
        "observable_universe": _observable_metadata(str(row["code"]), metadata, context),
    }
    return ThemeUniverseMember(
        theme_id=row["theme_id"],
        theme_name=row["theme_name"],
        code=row["code"],
        name=row["name"],
        weight=float(row["weight"] or 0.0),
        source_type=row["source_type"],
        source_name=row["source_name"],
        active=bool(row["active"]),
        metadata=metadata,
    )


def _observable_context(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    return {
        "registered_codes": set(_registered_realtime_codes(connection)),
        "condition_codes": set(_recent_condition_codes(connection, settings=settings)),
        "condition_fusion_codes": set(
            _recent_condition_fusion_codes(connection, settings=settings)
        ),
        "anchor_codes": set(settings.realtime_subscription_anchor_codes),
        "tick_rows": _latest_tick_rows(connection),
        "naver_top_rank": max(settings.realtime_subscription_max_per_theme, 1),
    }


def _observable_metadata(
    code: str,
    member_metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    priority = 0
    tick_rows = context.get("tick_rows") if isinstance(context.get("tick_rows"), Mapping) else {}
    tick = tick_rows.get(code) if isinstance(tick_rows, Mapping) else None
    if code in context.get("registered_codes", set()):
        reasons.append("CURRENT_REALTIME_REGISTERED")
        priority = max(priority, 1000)
    if code in context.get("condition_codes", set()):
        reasons.append("RECENT_CONDITION_ENTER")
        priority = max(priority, 900)
    if code in context.get("condition_fusion_codes", set()):
        reasons.append("RECENT_CONDITION_FUSION")
        priority = max(priority, 880)
    if code in context.get("anchor_codes", set()):
        reasons.append("ANCHOR_CODE")
        priority = max(priority, 800)
    if tick is not None:
        reasons.append("MARKET_TICK_PRESENT")
        priority = max(priority, 700)
        age = _age_seconds(tick.get("event_ts"))
        if age is not None:
            reasons.append("RECENT_MARKET_TICK")
        if _float_or_none(tick.get("cumulative_trade_value")) is not None:
            reasons.append("TRADE_VALUE_OBSERVED")
        if _float_or_none(tick.get("change_rate")) is not None:
            reasons.append("CHANGE_RATE_OBSERVED")
    naver_rank = _naver_rank(member_metadata)
    if naver_rank is not None and naver_rank <= int(context.get("naver_top_rank") or 0):
        reasons.append("NAVER_METADATA_TOP_RANK")
        priority = max(priority, 600 - naver_rank)
    return {
        "observable": bool(reasons),
        "priority": priority,
        "reason_codes": _dedupe(reasons),
    }


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
    if isinstance(loaded, str) or not isinstance(loaded, Iterable):
        loaded = [loaded]
    return _normalize_codes(loaded)


def _recent_condition_codes(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT code, event_ts
        FROM market_condition_latest
        WHERE action = ?
        """,
        (settings.candidate_condition_action_enter,),
    ).fetchall()
    return [
        row["code"]
        for row in rows
        if _safe_code(row["code"]) is not None
        and _within_stale_window(row["event_ts"], settings.realtime_subscription_stale_sec)
    ]


def _recent_condition_fusion_codes(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT code, latest_hit_at, active_roles_json
        FROM candidate_condition_fusion
        """
    ).fetchall()
    codes = []
    for row in rows:
        roles = _json_array(row["active_roles_json"])
        if not roles:
            continue
        code = _safe_code(row["code"])
        if code is None:
            continue
        if _within_stale_window(row["latest_hit_at"], settings.realtime_subscription_stale_sec):
            codes.append(code)
    return codes


def _latest_tick_rows(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT code, event_ts, cumulative_trade_value, change_rate
        FROM market_ticks_latest
        """
    ).fetchall()
    return {
        str(row["code"]): {
            "event_ts": row["event_ts"],
            "cumulative_trade_value": row["cumulative_trade_value"],
            "change_rate": row["change_rate"],
        }
        for row in rows
    }


def _within_stale_window(value: object, stale_sec: int) -> bool:
    age = _age_seconds(value)
    return age is not None and age <= stale_sec


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = parse_timestamp(value, "timestamp")
    except (BrokerValidationError, ValueError):
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _naver_rank(metadata: Mapping[str, Any]) -> int | None:
    candidates = [
        metadata.get("rank"),
        metadata.get("naver_member_rank"),
    ]
    raw = metadata.get("raw")
    if isinstance(raw, Mapping):
        candidates.extend([raw.get("rank"), raw.get("naver_member_rank")])
    for value in candidates:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return None


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


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_code(value: object) -> str | None:
    try:
        return validate_stock_code(value)
    except (BrokerValidationError, ValueError):
        return None


def _normalize_codes(values: Iterable[object]) -> list[str]:
    codes = []
    for value in values:
        code = _safe_code(value)
        if code is not None:
            codes.append(code)
    return _dedupe(codes)


def _dedupe(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result
