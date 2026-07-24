from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from domain.broker.condition_profiles import ConditionRole, parse_condition_role
from domain.broker.utils import (
    datetime_to_wire,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from storage.gateway_command_store import canonical_json

from services.config import Settings, candidate_timezone, load_settings
from services.market_data_service import get_market_data_readiness
from services.runtime.evaluation_run_guard import (
    assert_runtime_execution_fence,
    immediate_transaction,
)

CONDITION_SENSOR_REASON = "CONDITION_SENSOR_EVIDENCE"
CONDITION_NOT_BUY_SIGNAL_REASON = "MARKET_SENSOR_NOT_BUY_SIGNAL"
DISCOVERY_ONLY_REASON = "DISCOVERY_OBSERVATION_ONLY"
DISCOVERY_PROMOTION_PENDING_REASON = "DISCOVERY_PROMOTION_PENDING"
RISK_BLOCK_REASON = "RISK_BLOCKED_BY_CONDITION"
FUSION_PRIORITY_REASON = "CONDITION_FUSION_PRIORITY_READY"
_FUSION_WRITE_CHUNK_SIZE = 10

_ROLE_WEIGHTS = {
    ConditionRole.DISCOVERY.value: 1.0,
    ConditionRole.THEME_SPREAD.value: 2.0,
    ConditionRole.MANUAL.value: 3.0,
    ConditionRole.PULLBACK.value: 4.0,
    ConditionRole.BREAKOUT.value: 4.0,
    ConditionRole.LEADER.value: 5.0,
    ConditionRole.RISK_BLOCK.value: 0.0,
}
_DEFAULT_TTL_SEC = {
    ConditionRole.DISCOVERY.value: 180,
    ConditionRole.THEME_SPREAD.value: 300,
    ConditionRole.MANUAL.value: 600,
    ConditionRole.PULLBACK.value: 600,
    ConditionRole.BREAKOUT.value: 600,
    ConditionRole.LEADER.value: 600,
    ConditionRole.RISK_BLOCK.value: 1800,
}


@dataclass(frozen=True, kw_only=True)
class ConditionFusionRebuildResult:
    processed_event_count: int = 0
    fused_code_count: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_event_count": self.processed_event_count,
            "fused_code_count": self.fused_code_count,
        }


def rebuild_condition_fusion(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
) -> ConditionFusionRebuildResult:
    resolved_settings = settings or load_settings()
    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    start_at, end_at = condition_signal_trade_date_bounds(
        target_trade_date,
        resolved_settings,
    )
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_signals
        WHERE event_ts >= ?
            AND event_ts < ?
        ORDER BY event_ts ASC, id ASC
        """,
        (start_at, end_at),
    ).fetchall()
    events_by_code: dict[str, list[_ConditionProfileEvent]] = {}
    processed = 0
    for row in rows:
        if _trade_date_for_timestamp(row["event_ts"], resolved_settings) != target_trade_date:
            continue
        metadata = _metadata(row)
        if not is_profile_condition_metadata(metadata):
            continue
        event = _profile_event_from_row(row, metadata)
        events_by_code.setdefault(event.code, []).append(event)
        processed += 1

    now = utc_now()
    updated_at = datetime_to_wire(now)
    fusions: list[dict[str, Any]] = []
    for code, events in sorted(events_by_code.items()):
        fusions.append(_fuse_code_events(code, events, now=now, updated_at=updated_at))
    for index in range(0, len(fusions), _FUSION_WRITE_CHUNK_SIZE):
        with immediate_transaction(connection):
            for fusion in fusions[index : index + _FUSION_WRITE_CHUNK_SIZE]:
                _upsert_fusion(connection, target_trade_date, fusion)
            assert_runtime_execution_fence(connection)
    return ConditionFusionRebuildResult(
        processed_event_count=processed,
        fused_code_count=len(fusions),
    )


def rebuild_condition_fusion_for_code(
    connection: sqlite3.Connection,
    code: str,
    trade_date: str | None = None,
    *,
    settings: Settings | None = None,
    commit: bool = True,
) -> ConditionFusionRebuildResult:
    resolved_settings = settings or load_settings()
    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    normalized_code = validate_stock_code(code)
    start_at, end_at = _trade_date_bounds(target_trade_date, resolved_settings)
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_signals
        WHERE code = ?
            AND event_ts >= ?
            AND event_ts < ?
        ORDER BY event_ts ASC, id ASC
        """,
        (normalized_code, start_at, end_at),
    ).fetchall()

    events: list[_ConditionProfileEvent] = []
    for row in rows:
        metadata = _metadata(row)
        if not is_profile_condition_metadata(metadata):
            continue
        events.append(_profile_event_from_row(row, metadata))

    if not events:
        return ConditionFusionRebuildResult(processed_event_count=0, fused_code_count=0)

    now = utc_now()
    updated_at = datetime_to_wire(now)
    fusion = _fuse_code_events(normalized_code, events, now=now, updated_at=updated_at)
    if commit:
        with immediate_transaction(connection):
            _upsert_fusion(connection, target_trade_date, fusion)
            assert_runtime_execution_fence(connection)
    else:
        _upsert_fusion(connection, target_trade_date, fusion)
    return ConditionFusionRebuildResult(
        processed_event_count=len(events),
        fused_code_count=1,
    )


def list_condition_fusion(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    settings: Settings | None = None,
    registered_codes: Iterable[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    resolved_settings = settings or load_settings()
    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    rows = connection.execute(
        """
        SELECT *
        FROM candidate_condition_fusion
        WHERE trade_date = ?
        ORDER BY priority_score DESC, latest_hit_at DESC, code ASC
        LIMIT ?
        """,
        (target_trade_date, _bounded_limit(limit)),
    ).fetchall()
    registered = {validate_stock_code(code) for code in (registered_codes or [])}
    items: list[dict[str, Any]] = []
    for row in rows:
        item = _fusion_row_to_dict(row)
        readiness = get_market_data_readiness(
            connection,
            item["code"],
            settings=resolved_settings,
        )
        item["market_readiness_status"] = readiness["quality_status"]
        item["market_data_readiness"] = readiness
        item["subscribed"] = bool(item["subscribed"] or item["code"] in registered)
        items.append(item)
    return items


def get_condition_profile_metrics(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    settings: Settings | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    resolved_settings = settings or load_settings()
    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    try:
        return _get_condition_profile_metrics_sql(
            connection,
            target_trade_date,
            settings=resolved_settings,
            limit=limit,
        )
    except sqlite3.OperationalError:
        return _get_condition_profile_metrics_python(
            connection,
            target_trade_date,
            settings=resolved_settings,
            limit=limit,
        )


def _get_condition_profile_metrics_python(
    connection: sqlite3.Connection,
    target_trade_date: str,
    *,
    settings: Settings,
    limit: int,
) -> list[dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    start_at, end_at = condition_signal_trade_date_bounds(
        target_trade_date,
        settings,
    )
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_signals
        WHERE event_ts >= ?
            AND event_ts < ?
        ORDER BY event_ts DESC, id DESC
        """,
        (start_at, end_at),
    ).fetchall()
    for row in rows:
        if _trade_date_for_timestamp(row["event_ts"], settings) != target_trade_date:
            continue
        metadata = _metadata(row)
        if not is_profile_condition_metadata(metadata):
            continue
        profile = _profile_payload(metadata)
        profile_id = str(profile.get("profile_id") or row["condition_id"])
        item = metrics.setdefault(
            profile_id,
            {
                "profile_id": profile_id,
                "condition_name": row["condition_name"],
                "condition_index": profile.get("condition_index"),
                "role": str(profile.get("role") or "DISCOVERY").upper(),
                "hit_count": 0,
                "enter_count": 0,
                "exit_count": 0,
                "subscribed_count": 0,
                "skipped_count": 0,
                "skip_reason_summary": Counter(),
                "latest_event_at": row["event_ts"],
                "enabled": profile.get("enabled", True),
                "price_subscribe_policy": profile.get("price_subscribe_policy"),
                "ttl_sec": profile.get("ttl_sec"),
                "priority": profile.get("priority"),
            },
        )
        item["hit_count"] += 1
        action = str(row["action"] or "").upper()
        if action == "ENTER":
            item["enter_count"] += 1
        elif action == "EXIT":
            item["exit_count"] += 1
        admission = _admission_payload(metadata)
        if admission.get("subscribed") is True:
            item["subscribed_count"] += 1
        elif admission:
            item["skipped_count"] += 1
            for reason in admission.get("reason_codes", ()):
                item["skip_reason_summary"][str(reason).upper()] += 1

    ordered = sorted(
        metrics.values(),
        key=lambda item: (-int(item["hit_count"]), str(item["profile_id"])),
    )[: _bounded_limit(limit)]
    for item in ordered:
        item["skip_reason_summary"] = dict(item["skip_reason_summary"])
    return ordered


def _get_condition_profile_metrics_sql(
    connection: sqlite3.Connection,
    target_trade_date: str,
    *,
    settings: Settings,
    limit: int,
) -> list[dict[str, Any]]:
    start_at, end_at = _trade_date_bounds(target_trade_date, settings)
    rows = connection.execute(
        """
        WITH profile_signals AS (
            SELECT
                COALESCE(
                    NULLIF(json_extract(metadata_json, '$.condition_profile.profile_id'), ''),
                    NULLIF(json_extract(metadata_json, '$.condition_profile_id'), ''),
                    condition_id
                ) AS profile_id,
                COALESCE(
                    NULLIF(json_extract(metadata_json, '$.condition_profile.condition_name'), ''),
                    condition_name
                ) AS profile_condition_name,
                json_extract(metadata_json, '$.condition_profile.condition_index')
                    AS condition_index,
                COALESCE(
                    NULLIF(json_extract(metadata_json, '$.condition_profile.role'), ''),
                    NULLIF(json_extract(metadata_json, '$.condition_role'), ''),
                    'DISCOVERY'
                ) AS role,
                json_extract(metadata_json, '$.condition_profile.enabled') AS enabled,
                json_extract(metadata_json, '$.condition_profile.price_subscribe_policy')
                    AS price_subscribe_policy,
                json_extract(metadata_json, '$.condition_profile.ttl_sec') AS ttl_sec,
                json_extract(metadata_json, '$.condition_profile.priority') AS priority,
                COALESCE(
                    json_extract(metadata_json, '$.condition_admission.subscribed'),
                    json_extract(metadata_json, '$.admission.subscribed')
                ) AS subscribed,
                CASE
                    WHEN json_type(metadata_json, '$.condition_admission') IS NOT NULL
                        OR json_type(metadata_json, '$.admission') IS NOT NULL
                    THEN 1
                    ELSE 0
                END AS admission_present,
                action,
                event_ts
            FROM market_condition_signals
            WHERE event_ts >= ?
                AND event_ts < ?
                AND (
                    json_extract(metadata_json, '$.sensor_evidence') = 1
                    OR json_extract(metadata_json, '$.condition_role') IS NOT NULL
                    OR json_extract(metadata_json, '$.condition_profile_id') IS NOT NULL
                    OR json_extract(metadata_json, '$.condition_profile.role') IS NOT NULL
                    OR json_extract(metadata_json, '$.condition_profile.profile_id') IS NOT NULL
                )
        )
        SELECT
            profile_id,
            MAX(profile_condition_name) AS condition_name,
            MAX(condition_index) AS condition_index,
            UPPER(MAX(role)) AS role,
            COUNT(*) AS hit_count,
            SUM(CASE WHEN UPPER(action) = 'ENTER' THEN 1 ELSE 0 END) AS enter_count,
            SUM(CASE WHEN UPPER(action) = 'EXIT' THEN 1 ELSE 0 END) AS exit_count,
            SUM(CASE WHEN subscribed = 1 THEN 1 ELSE 0 END) AS subscribed_count,
            SUM(
                CASE
                    WHEN admission_present = 1 AND COALESCE(subscribed, 0) != 1 THEN 1
                    ELSE 0
                END
            ) AS skipped_count,
            MAX(event_ts) AS latest_event_at,
            MAX(enabled) AS enabled,
            MAX(price_subscribe_policy) AS price_subscribe_policy,
            MAX(ttl_sec) AS ttl_sec,
            MAX(priority) AS priority
        FROM profile_signals
        GROUP BY profile_id
        ORDER BY hit_count DESC, profile_id ASC
        LIMIT ?
        """,
        (start_at, end_at, _bounded_limit(limit)),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        profile_id = str(row["profile_id"])
        items.append(
            {
                "profile_id": profile_id,
                "condition_name": row["condition_name"],
                "condition_index": row["condition_index"],
                "role": str(row["role"] or "DISCOVERY").upper(),
                "hit_count": int(row["hit_count"] or 0),
                "enter_count": int(row["enter_count"] or 0),
                "exit_count": int(row["exit_count"] or 0),
                "subscribed_count": int(row["subscribed_count"] or 0),
                "skipped_count": int(row["skipped_count"] or 0),
                "skip_reason_summary": {},
                "latest_event_at": row["latest_event_at"],
                "enabled": True if row["enabled"] is None else bool(row["enabled"]),
                "price_subscribe_policy": row["price_subscribe_policy"],
                "ttl_sec": row["ttl_sec"],
                "priority": row["priority"],
            }
        )
    summaries = _condition_profile_skip_reason_summaries_sql(
        connection,
        target_trade_date,
        settings=settings,
        profile_ids=[item["profile_id"] for item in items],
    )
    for item in items:
        item["skip_reason_summary"] = summaries.get(item["profile_id"], {})
    return items


def _condition_profile_skip_reason_summaries_sql(
    connection: sqlite3.Connection,
    target_trade_date: str,
    *,
    settings: Settings,
    profile_ids: Sequence[str],
) -> dict[str, dict[str, int]]:
    if not profile_ids:
        return {}
    start_at, end_at = _trade_date_bounds(target_trade_date, settings)
    placeholders = ",".join("?" for _ in profile_ids)
    rows = connection.execute(
        f"""
        WITH profile_signals AS (
            SELECT
                COALESCE(
                    NULLIF(json_extract(metadata_json, '$.condition_profile.profile_id'), ''),
                    NULLIF(json_extract(metadata_json, '$.condition_profile_id'), ''),
                    condition_id
                ) AS profile_id,
                metadata_json,
                COALESCE(
                    json_extract(metadata_json, '$.condition_admission.subscribed'),
                    json_extract(metadata_json, '$.admission.subscribed')
                ) AS subscribed
            FROM market_condition_signals
            WHERE event_ts >= ?
                AND event_ts < ?
                AND (
                    json_type(metadata_json, '$.condition_admission') IS NOT NULL
                    OR json_type(metadata_json, '$.admission') IS NOT NULL
                )
        ),
        reasons AS (
            SELECT profile_id, UPPER(CAST(value AS TEXT)) AS reason
            FROM profile_signals, json_each(metadata_json, '$.condition_admission.reason_codes')
            WHERE profile_id IN ({placeholders})
                AND COALESCE(subscribed, 0) != 1
            UNION ALL
            SELECT profile_id, UPPER(CAST(value AS TEXT)) AS reason
            FROM profile_signals, json_each(metadata_json, '$.admission.reason_codes')
            WHERE profile_id IN ({placeholders})
                AND COALESCE(subscribed, 0) != 1
        )
        SELECT profile_id, reason, COUNT(*) AS count
        FROM reasons
        WHERE reason IS NOT NULL AND reason != ''
        GROUP BY profile_id, reason
        """,
        (start_at, end_at, *profile_ids, *profile_ids),
    ).fetchall()
    summaries: dict[str, dict[str, int]] = {}
    for row in rows:
        summaries.setdefault(str(row["profile_id"]), {})[str(row["reason"])] = int(
            row["count"] or 0
        )
    return summaries


def is_profile_condition_metadata(metadata: Mapping[str, Any]) -> bool:
    profile = _profile_payload(metadata)
    return bool(
        metadata.get("sensor_evidence") is True
        or metadata.get("condition_role")
        or metadata.get("condition_profile_id")
        or profile.get("role")
        or profile.get("profile_id")
    )


def _profile_event_from_row(
    row: sqlite3.Row,
    metadata: Mapping[str, Any],
) -> _ConditionProfileEvent:
    profile = _profile_payload(metadata)
    role = parse_condition_role(
        metadata.get("condition_role") or profile.get("role") or ConditionRole.DISCOVERY.value
    )
    return _ConditionProfileEvent(
        event_id=row["event_id"],
        condition_id=row["condition_id"],
        condition_name=row["condition_name"],
        code=validate_stock_code(row["code"]),
        name=row["name"],
        action=str(row["action"]).upper(),
        role=role.value,
        profile_id=str(
            metadata.get("condition_profile_id")
            or profile.get("profile_id")
            or row["condition_id"]
        ),
        priority=int(profile.get("priority") or metadata.get("condition_priority") or 0),
        ttl_sec=int(profile.get("ttl_sec") or _DEFAULT_TTL_SEC[role.value]),
        event_ts=row["event_ts"],
        received_at=row["received_at"],
        metadata=dict(metadata),
        subscribed=bool(_admission_payload(metadata).get("subscribed") is True),
    )


def _fuse_code_events(
    code: str,
    events: Sequence[_ConditionProfileEvent],
    *,
    now,
    updated_at: str,
) -> dict[str, Any]:
    latest_by_profile: dict[str, _ConditionProfileEvent] = {}
    enter_events = [event for event in events if event.action == "ENTER"]
    for event in events:
        latest_by_profile[event.profile_id] = event
    active_events = [
        event
        for event in latest_by_profile.values()
        if event.action == "ENTER" and not event.is_expired(now)
    ]
    active_roles = sorted({event.role for event in active_events})
    condition_names = sorted({event.condition_name for event in enter_events})
    latest_event = max(events, key=lambda event: event.event_ts)
    risk_blocked = ConditionRole.RISK_BLOCK.value in active_roles
    positive_roles = [role for role in active_roles if role != ConditionRole.RISK_BLOCK.value]
    positive_score = sum(_ROLE_WEIGHTS.get(role, 0.0) for role in positive_roles)
    role_priority = sum(
        max(event.priority, 0)
        for event in active_events
        if event.role != ConditionRole.RISK_BLOCK.value
    )
    priority_score = positive_score * 10.0 + float(role_priority)
    reasons = [CONDITION_SENSOR_REASON]
    role_set = set(active_roles)
    discovery_only = role_set == {ConditionRole.DISCOVERY.value}
    if discovery_only:
        reasons.append(DISCOVERY_ONLY_REASON)
        reasons.append(DISCOVERY_PROMOTION_PENDING_REASON)
        priority_score = min(priority_score, 25.0)
    if ConditionRole.LEADER.value in role_set and ConditionRole.PULLBACK.value in role_set:
        reasons.append("LEADER_PULLBACK_FUSION_PRIORITY")
        priority_score += 500.0
    if ConditionRole.LEADER.value in role_set and ConditionRole.BREAKOUT.value in role_set:
        reasons.append("LEADER_BREAKOUT_FUSION_PRIORITY")
        priority_score += 500.0
    if risk_blocked:
        reasons.append(RISK_BLOCK_REASON)
        reasons.append(CONDITION_NOT_BUY_SIGNAL_REASON)
        priority_score = 0.0
    if active_roles and not discovery_only and not risk_blocked:
        reasons.append(FUSION_PRIORITY_REASON)
    if not active_roles:
        reasons.append("CONDITION_FUSION_NO_ACTIVE_ROLE")
        reasons.append(CONDITION_NOT_BUY_SIGNAL_REASON)
        priority_score = 0.0

    not_buy_signal = bool(risk_blocked or not active_roles)
    candidate_promotion_allowed = bool(active_roles and not risk_blocked)
    return {
        "code": validate_stock_code(code),
        "name": latest_event.name,
        "active_roles": active_roles,
        "hit_count": len(enter_events),
        "latest_hit_at": max((event.event_ts for event in enter_events), default=None),
        "condition_names": condition_names,
        "positive_score": positive_score,
        "risk_blocked": risk_blocked,
        "priority_score": priority_score,
        "reason_codes": _dedupe(reasons),
        "subscribed": any(event.subscribed for event in active_events),
        "latest_event_id": latest_event.event_id,
        "updated_at": updated_at,
        "metadata": {
            "sensor_evidence": True,
            "not_buy_signal": not_buy_signal,
            "candidate_promotion_allowed": candidate_promotion_allowed,
            "discovery_only": discovery_only,
            "discovery_promotion_pending": discovery_only,
            "active_profiles": [event.to_payload() for event in active_events],
            "latest_event": latest_event.to_payload(),
        },
    }


def _upsert_fusion(
    connection: sqlite3.Connection,
    trade_date: str,
    fusion: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_condition_fusion (
            trade_date,
            code,
            name,
            active_roles_json,
            hit_count,
            latest_hit_at,
            condition_names_json,
            positive_score,
            risk_blocked,
            priority_score,
            reason_codes_json,
            subscribed,
            market_readiness_status,
            latest_event_id,
            metadata_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date, code) DO UPDATE SET
            name = excluded.name,
            active_roles_json = excluded.active_roles_json,
            hit_count = excluded.hit_count,
            latest_hit_at = excluded.latest_hit_at,
            condition_names_json = excluded.condition_names_json,
            positive_score = excluded.positive_score,
            risk_blocked = excluded.risk_blocked,
            priority_score = excluded.priority_score,
            reason_codes_json = excluded.reason_codes_json,
            subscribed = excluded.subscribed,
            market_readiness_status = excluded.market_readiness_status,
            latest_event_id = excluded.latest_event_id,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            trade_date,
            fusion["code"],
            fusion["name"],
            _json_dumps(fusion["active_roles"]),
            int(fusion["hit_count"]),
            fusion["latest_hit_at"],
            _json_dumps(fusion["condition_names"]),
            float(fusion["positive_score"]),
            1 if fusion["risk_blocked"] else 0,
            float(fusion["priority_score"]),
            _json_dumps(fusion["reason_codes"]),
            1 if fusion["subscribed"] else 0,
            None,
            fusion["latest_event_id"],
            canonical_json(fusion["metadata"]),
            fusion["updated_at"],
        ),
    )


@dataclass(frozen=True)
class _ConditionProfileEvent:
    event_id: str
    condition_id: str
    condition_name: str
    code: str
    name: str
    action: str
    role: str
    profile_id: str
    priority: int
    ttl_sec: int
    event_ts: str
    received_at: str
    metadata: Mapping[str, Any]
    subscribed: bool

    def is_expired(self, now) -> bool:
        if self.ttl_sec <= 0:
            return False
        return (now - parse_timestamp(self.event_ts, "event_ts")).total_seconds() > self.ttl_sec

    def to_payload(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "condition_id": self.condition_id,
            "condition_name": self.condition_name,
            "code": self.code,
            "name": self.name,
            "action": self.action,
            "role": self.role,
            "profile_id": self.profile_id,
            "priority": self.priority,
            "ttl_sec": self.ttl_sec,
            "event_ts": self.event_ts,
            "received_at": self.received_at,
            "subscribed": self.subscribed,
        }


def _metadata(row: sqlite3.Row) -> dict[str, Any]:
    try:
        loaded = json.loads(row["metadata_json"])
    except (TypeError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _profile_payload(metadata: Mapping[str, Any]) -> dict[str, Any]:
    profile = metadata.get("condition_profile")
    return dict(profile) if isinstance(profile, Mapping) else {}


def _admission_payload(metadata: Mapping[str, Any]) -> dict[str, Any]:
    admission = metadata.get("condition_admission") or metadata.get("admission")
    return dict(admission) if isinstance(admission, Mapping) else {}


def _fusion_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["active_roles"] = json.loads(data.pop("active_roles_json"))
    data["condition_names"] = json.loads(data.pop("condition_names_json"))
    data["risk_blocked"] = bool(data["risk_blocked"])
    data["subscribed"] = bool(data["subscribed"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _resolve_trade_date(trade_date: str | None, settings: Settings) -> str:
    if trade_date:
        return str(trade_date)
    return (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def condition_signal_trade_date_bounds(
    trade_date: str,
    settings: Settings,
) -> tuple[str, str]:
    """Return the UTC wire interval for one configured-market trade date."""
    trade_day = date.fromisoformat(trade_date)
    timezone = candidate_timezone(settings.candidate_trade_date_timezone)
    start = datetime.combine(trade_day, time.min, tzinfo=timezone)
    end = start + timedelta(days=1)
    return _fixed_precision_wire(start), _fixed_precision_wire(end)


def _fixed_precision_wire(value: datetime) -> str:
    """Return a UTC wire timestamp with a stable TEXT-sortable precision."""
    return parse_timestamp(value, "timestamp").strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _trade_date_bounds(trade_date: str, settings: Settings) -> tuple[str, str]:
    return condition_signal_trade_date_bounds(trade_date, settings)


def _trade_date_for_timestamp(value: str, settings: Settings) -> str:
    return (
        parse_timestamp(value, "timestamp")
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _dedupe(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip().upper()
        if text and text not in result:
            result.append(text)
    return result


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
