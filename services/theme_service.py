from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.theme.models import (
    ThemeMembership,
    ThemeMemberSnapshot,
    ThemeSnapshot,
    ThemeSourceType,
    row_to_dict,
)
from domain.theme.quality import ThemeSnapshotQuality
from domain.theme.state import ThemeMemberRole, ThemeState
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_data_service import get_latest_tick, get_market_data_readiness


@dataclass(frozen=True, kw_only=True)
class ThemeImportResult:
    batch_id: str
    source_type: ThemeSourceType
    source_name: str | None
    theme_count: int
    member_count: int
    status: str
    error_message: str | None = None
    payload_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "source_type": self.source_type.value,
            "source_name": self.source_name,
            "theme_count": self.theme_count,
            "member_count": self.member_count,
            "status": self.status,
            "error_message": self.error_message,
            "payload_hash": self.payload_hash,
        }


@dataclass(frozen=True, kw_only=True)
class ThemeSnapshotRebuildResult:
    processed_theme_count: int
    snapshot_count: int
    error_count: int

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_theme_count": self.processed_theme_count,
            "snapshot_count": self.snapshot_count,
            "error_count": self.error_count,
        }


def upsert_theme(
    connection: sqlite3.Connection,
    *,
    theme_id: str,
    theme_name: str,
    source_type: ThemeSourceType | str,
    source_name: str | None = None,
    active: bool = True,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    normalized_theme_name = require_non_empty_str(theme_name, "theme_name")
    normalized_source_type = _parse_source_type(source_type)
    normalized_source_name = _optional_source_name(source_name)
    metadata_json = canonical_json(metadata or {})
    active_int = 1 if active else 0
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO themes (
            theme_id,
            theme_name,
            source_type,
            source_name,
            active,
            metadata_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(theme_id) DO UPDATE SET
            theme_name = excluded.theme_name,
            source_type = excluded.source_type,
            source_name = excluded.source_name,
            active = excluded.active,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            normalized_theme_id,
            normalized_theme_name,
            normalized_source_type.value,
            normalized_source_name,
            active_int,
            metadata_json,
            now,
            now,
        ),
    )
    return get_theme(connection, normalized_theme_id) or {
        "theme_id": normalized_theme_id,
        "theme_name": normalized_theme_name,
        "source_type": normalized_source_type.value,
        "source_name": normalized_source_name,
        "active": active,
        "metadata": metadata or {},
    }


def upsert_theme_member(
    connection: sqlite3.Connection,
    *,
    theme_id: str,
    theme_name: str,
    code: str,
    name: str,
    source_type: ThemeSourceType | str,
    source_name: str | None = None,
    active: bool = True,
    weight: float = 1.0,
    metadata: Mapping[str, Any] | None = None,
) -> ThemeMembership:
    membership = ThemeMembership(
        theme_id=theme_id,
        theme_name=theme_name,
        code=code,
        name=name,
        source_type=_parse_source_type(source_type),
        source_name=_optional_source_name(source_name),
        active=active,
        weight=weight,
        metadata=metadata or {},
    )
    metadata_json = canonical_json(membership.metadata)
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO theme_members (
            theme_id,
            code,
            name,
            source_type,
            source_name,
            active,
            weight,
            metadata_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(theme_id, code) DO UPDATE SET
            name = excluded.name,
            source_type = excluded.source_type,
            source_name = excluded.source_name,
            active = excluded.active,
            weight = excluded.weight,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            membership.theme_id,
            membership.code,
            membership.name,
            membership.source_type.value,
            membership.source_name,
            1 if membership.active else 0,
            membership.weight,
            metadata_json,
            now,
            now,
        ),
    )
    return membership


def import_theme_memberships(
    connection: sqlite3.Connection,
    payload: Mapping[str, Any],
    *,
    source_type: ThemeSourceType | str | None = None,
    source_name: str | None = None,
    replace: bool = False,
) -> ThemeImportResult:
    normalized_payload = normalize_payload(payload)
    batch_id = new_message_id("theme_import")
    payload_json = canonical_json(normalized_payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    resolved_source_type = _parse_source_type(source_type or normalized_payload.get("source_type"))
    resolved_source_name = _optional_source_name(
        source_name if source_name is not None else normalized_payload.get("source_name")
    )
    themes = _payload_themes(normalized_payload)
    member_count = sum(len(_theme_members(theme)) for theme in themes)

    try:
        connection.execute("BEGIN IMMEDIATE")
        for theme in themes:
            theme_id = require_non_empty_str(theme.get("theme_id"), "theme_id")
            theme_name = require_non_empty_str(theme.get("theme_name"), "theme_name")
            theme_metadata = _metadata(theme)
            upsert_theme(
                connection,
                theme_id=theme_id,
                theme_name=theme_name,
                source_type=resolved_source_type,
                source_name=resolved_source_name,
                active=bool(theme.get("active", True)),
                metadata=theme_metadata,
            )
            if replace:
                _deactivate_theme_members_for_source(
                    connection,
                    theme_id=theme_id,
                    source_type=resolved_source_type,
                    source_name=resolved_source_name,
                )
            for member in _theme_members(theme):
                upsert_theme_member(
                    connection,
                    theme_id=theme_id,
                    theme_name=theme_name,
                    code=member.get("code"),
                    name=member.get("name"),
                    source_type=resolved_source_type,
                    source_name=resolved_source_name,
                    active=bool(member.get("active", True)),
                    weight=float(member.get("weight", 1.0)),
                    metadata=_metadata(member),
                )
        _record_import_batch(
            connection,
            batch_id=batch_id,
            source_type=resolved_source_type,
            source_name=resolved_source_name,
            theme_count=len(themes),
            member_count=member_count,
            status="SUCCESS",
            error_message=None,
            payload_hash=payload_hash,
        )
        connection.commit()
    except Exception as exc:
        connection.rollback()
        _record_import_batch(
            connection,
            batch_id=batch_id,
            source_type=resolved_source_type,
            source_name=resolved_source_name,
            theme_count=len(themes),
            member_count=member_count,
            status="ERROR",
            error_message=str(exc),
            payload_hash=payload_hash,
        )
        connection.commit()
        raise

    return ThemeImportResult(
        batch_id=batch_id,
        source_type=resolved_source_type,
        source_name=resolved_source_name,
        theme_count=len(themes),
        member_count=member_count,
        status="SUCCESS",
        payload_hash=payload_hash,
    )


def record_theme_import_batch(
    connection: sqlite3.Connection,
    *,
    source_type: ThemeSourceType | str,
    source_name: str | None,
    theme_count: int,
    member_count: int,
    status: str,
    error_message: str | None = None,
    payload_hash: str | None = None,
    batch_id: str | None = None,
) -> ThemeImportResult:
    resolved_source_type = _parse_source_type(source_type)
    resolved_source_name = _optional_source_name(source_name)
    resolved_batch_id = batch_id or new_message_id("theme_import")
    resolved_status = require_non_empty_str(status, "status").upper()
    normalized_theme_count = max(int(theme_count), 0)
    normalized_member_count = max(int(member_count), 0)
    _record_import_batch(
        connection,
        batch_id=resolved_batch_id,
        source_type=resolved_source_type,
        source_name=resolved_source_name,
        theme_count=normalized_theme_count,
        member_count=normalized_member_count,
        status=resolved_status,
        error_message=error_message,
        payload_hash=payload_hash,
    )
    return ThemeImportResult(
        batch_id=resolved_batch_id,
        source_type=resolved_source_type,
        source_name=resolved_source_name,
        theme_count=normalized_theme_count,
        member_count=normalized_member_count,
        status=resolved_status,
        error_message=error_message,
        payload_hash=payload_hash,
    )


def record_theme_import_error(
    connection: sqlite3.Connection,
    *,
    source_type: ThemeSourceType | str,
    source_name: str | None,
    stage: str,
    error_message: str,
    batch_id: str | None = None,
    theme_id: str | None = None,
    theme_name: str | None = None,
    code: str | None = None,
    source_url: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    normalized_code = validate_stock_code(code) if code is not None else None
    connection.execute(
        """
        INSERT INTO theme_import_errors (
            batch_id,
            source_type,
            source_name,
            stage,
            theme_id,
            theme_name,
            code,
            source_url,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _optional_non_empty(batch_id, "batch_id"),
            _parse_source_type(source_type).value,
            _optional_source_name(source_name),
            require_non_empty_str(stage, "stage"),
            _optional_non_empty(theme_id, "theme_id"),
            _optional_non_empty(theme_name, "theme_name"),
            normalized_code,
            _optional_non_empty(source_url, "source_url"),
            require_non_empty_str(error_message, "error_message"),
            canonical_json(payload or {}),
        ),
    )


def list_themes(
    connection: sqlite3.Connection,
    *,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    where = "WHERE active = 1" if active_only else ""
    rows = connection.execute(
        f"""
        SELECT *
        FROM themes
        {where}
        ORDER BY theme_name ASC, theme_id ASC
        """
    ).fetchall()
    return [_theme_row_to_dict(row) for row in rows]


def get_theme(connection: sqlite3.Connection, theme_id: str) -> dict[str, Any] | None:
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    row = connection.execute(
        """
        SELECT *
        FROM themes
        WHERE theme_id = ?
        """,
        (normalized_theme_id,),
    ).fetchone()
    return None if row is None else _theme_row_to_dict(row)


def list_theme_members(connection: sqlite3.Connection, theme_id: str) -> list[dict[str, Any]]:
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    rows = connection.execute(
        """
        SELECT
            m.*,
            t.theme_name
        FROM theme_members AS m
        JOIN themes AS t ON t.theme_id = m.theme_id
        WHERE m.theme_id = ?
        ORDER BY m.active DESC, m.weight DESC, m.code ASC
        """,
        (normalized_theme_id,),
    ).fetchall()
    return [_theme_member_row_to_dict(row) for row in rows]


def list_themes_for_code(connection: sqlite3.Connection, code: str) -> list[dict[str, Any]]:
    normalized_code = validate_stock_code(code)
    rows = connection.execute(
        """
        SELECT
            t.*,
            m.name AS member_name,
            m.active AS member_active,
            m.weight AS member_weight
        FROM theme_members AS m
        JOIN themes AS t ON t.theme_id = m.theme_id
        WHERE m.code = ?
        ORDER BY t.active DESC, t.theme_name ASC
        """,
        (normalized_code,),
    ).fetchall()
    themes = []
    for row in rows:
        item = _theme_row_to_dict(row)
        item["member_name"] = row["member_name"]
        item["member_active"] = bool(row["member_active"])
        item["member_weight"] = float(row["member_weight"])
        themes.append(item)
    return themes


def calculate_theme_snapshot(
    connection: sqlite3.Connection,
    theme_id: str,
    *,
    calculated_at: datetime | str | None = None,
    settings: Settings | None = None,
) -> ThemeSnapshot:
    resolved_settings = settings or load_settings()
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    theme = get_theme(connection, normalized_theme_id)
    if theme is None:
        raise ValueError(f"theme not found: {normalized_theme_id}")

    calculated_at_wire = _timestamp_wire(calculated_at)
    snapshot_id = _snapshot_id(normalized_theme_id, calculated_at_wire)
    members = _active_theme_members_for_snapshot(
        connection,
        normalized_theme_id,
        limit=resolved_settings.theme_snapshot_max_members,
    )
    member_count = _count_theme_members(connection, normalized_theme_id)
    member_snapshots: list[ThemeMemberSnapshot] = []
    member_scores: dict[str, float] = {}
    member_error_count = 0

    for member in members:
        try:
            member_snapshot, score = _calculate_member_snapshot(
                connection,
                theme=theme,
                member=member,
                calculated_at=calculated_at_wire,
                settings=resolved_settings,
            )
            member_snapshots.append(member_snapshot)
            member_scores[member_snapshot.code] = score
        except Exception as exc:
            member_error_count += 1
            _record_projection_error(
                connection,
                theme_id=normalized_theme_id,
                code=member["code"] if "code" in member.keys() else None,
                error_message=str(exc),
                payload={"member": row_to_dict(member)},
            )

    observed_members = [member for member in member_snapshots if member.price is not None]
    fresh_members = [
        member
        for member in member_snapshots
        if member.price is not None and member.readiness_status == "FRESH"
    ]
    scan_observed_members = [
        member for member in member_snapshots if member.observation_source == "MARKET_SCAN"
    ]
    realtime_observed_members = [
        member for member in member_snapshots if member.observation_source == "REALTIME_TICK"
    ]
    rising_members = [
        member
        for member in observed_members
        if member.change_rate is not None and member.change_rate > 0
    ]
    active_member_count = len(members)
    fresh_coverage_ratio = _ratio(len(fresh_members), active_member_count)
    scan_coverage_ratio = _ratio(len(scan_observed_members), active_member_count)
    realtime_coverage_ratio = _ratio(len(realtime_observed_members), active_member_count)
    rising_ratio = _ratio(len(rising_members), max(len(observed_members), 1))
    observed_change_rates = [
        member.change_rate for member in observed_members if member.change_rate is not None
    ]
    avg_change_rate = (
        sum(observed_change_rates) / len(observed_change_rates) if observed_change_rates else 0.0
    )
    max_change_rate = max(observed_change_rates) if observed_change_rates else 0.0
    total_trade_value = sum(
        member.cumulative_trade_value or 0.0 for member in observed_members
    )
    trade_value_delta_1m = sum(member.trade_value_delta_1m for member in member_snapshots)
    trade_value_delta_3m = sum(member.trade_value_delta_3m for member in member_snapshots)
    trade_value_delta_5m = sum(member.trade_value_delta_5m for member in member_snapshots)
    flow_metrics = _theme_flow_metrics(
        member_snapshots,
        active_member_count=active_member_count,
        rising_ratio=rising_ratio,
        top_n=resolved_settings.market_scan_top_n,
    )

    leader = _select_leader(fresh_members, member_scores)
    co_leader_codes = _select_co_leaders(
        fresh_members,
        member_scores,
        leader_code=leader.code if leader else None,
        score_ratio=resolved_settings.theme_co_leader_score_ratio,
    )
    follower_codes = _select_followers(
        fresh_members,
        leader_code=leader.code if leader else None,
        co_leader_codes=co_leader_codes,
    )
    member_snapshots = _with_member_roles(
        member_snapshots,
        leader_code=leader.code if leader else None,
        co_leader_codes=co_leader_codes,
        follower_codes=follower_codes,
    )
    state, quality_status, reason_codes = _classify_theme_snapshot(
        active_member_count=active_member_count,
        observed_member_count=len(observed_members),
        fresh_coverage_ratio=fresh_coverage_ratio,
        scan_coverage_ratio=scan_coverage_ratio,
        rising_ratio=rising_ratio,
        leading_member=leader,
        total_trade_value=total_trade_value,
        settings=resolved_settings,
    )
    if member_error_count:
        reason_codes.append("MEMBER_CALCULATION_ERROR")

    snapshot = ThemeSnapshot(
        snapshot_id=snapshot_id,
        theme_id=normalized_theme_id,
        theme_name=theme["theme_name"],
        calculated_at=calculated_at_wire,
        member_count=member_count,
        active_member_count=active_member_count,
        observed_member_count=len(observed_members),
        fresh_member_count=len(fresh_members),
        fresh_coverage_ratio=fresh_coverage_ratio,
        scan_coverage_ratio=scan_coverage_ratio,
        realtime_coverage_ratio=realtime_coverage_ratio,
        rising_member_count=len(rising_members),
        rising_ratio=rising_ratio,
        avg_change_rate=avg_change_rate,
        max_change_rate=max_change_rate,
        total_trade_value=total_trade_value,
        trade_value_delta_1m=trade_value_delta_1m,
        trade_value_delta_3m=trade_value_delta_3m,
        trade_value_delta_5m=trade_value_delta_5m,
        flow_trade_value_delta=flow_metrics["flow_trade_value_delta"],
        flow_rank_inflow_count=int(flow_metrics["flow_rank_inflow_count"]),
        flow_score=flow_metrics["flow_score"],
        leading_code=leader.code if leader else None,
        leading_name=leader.name if leader else None,
        co_leader_codes=co_leader_codes,
        follower_codes=follower_codes,
        state=state,
        quality_status=quality_status,
        reason_codes=reason_codes,
        members=member_snapshots,
        metadata={
            "observation_only": True,
            "observe_only": True,
            "no_order_side_effects": True,
            "member_error_count": member_error_count,
            "score_formula": "change_rate*100 + delta1m/1e6 + delta3m/3e6 "
            "+ cumulative_trade_value/1e9 + execution_strength/100, stale members half score",
            "flow_score_formula": flow_metrics["flow_score_formula"],
        },
    )
    _save_theme_snapshot(connection, snapshot)
    connection.commit()
    return snapshot


def calculate_all_theme_snapshots(
    connection: sqlite3.Connection,
    *,
    calculated_at: datetime | str | None = None,
    active_only: bool = True,
    settings: Settings | None = None,
) -> ThemeSnapshotRebuildResult:
    resolved_settings = settings or load_settings()
    themes = list_themes(connection, active_only=active_only)
    processed_theme_count = snapshot_count = error_count = 0
    for theme in themes:
        processed_theme_count += 1
        try:
            snapshot = calculate_theme_snapshot(
                connection,
                theme["theme_id"],
                calculated_at=calculated_at,
                settings=resolved_settings,
            )
            snapshot_count += 1
            error_count += int(snapshot.metadata.get("member_error_count", 0))
        except Exception as exc:
            error_count += 1
            _record_projection_error(
                connection,
                theme_id=theme["theme_id"],
                code=None,
                error_message=str(exc),
                payload={"theme": theme},
            )
            connection.commit()
    return ThemeSnapshotRebuildResult(
        processed_theme_count=processed_theme_count,
        snapshot_count=snapshot_count,
        error_count=error_count,
    )


def get_theme_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    return {
        "enabled": resolved_settings.theme_service_enabled,
        "theme_count": _count_rows(connection, "themes"),
        "active_theme_count": _count_rows(connection, "themes", where="active = 1"),
        "member_count": _count_rows(connection, "theme_members"),
        "latest_snapshot_count": _count_rows(connection, "theme_latest_snapshots"),
        "import_error_count": _count_rows(connection, "theme_import_errors"),
        "projection_error_count": _count_rows(connection, "theme_projection_errors"),
        "min_fresh_coverage_ratio": resolved_settings.theme_min_fresh_coverage_ratio,
        "leading_rising_ratio": resolved_settings.theme_leading_rising_ratio,
        "spreading_rising_ratio": resolved_settings.theme_spreading_rising_ratio,
        "min_active_members": resolved_settings.theme_min_active_members,
        "snapshot_max_members": resolved_settings.theme_snapshot_max_members,
        "import_allow_replace": resolved_settings.theme_import_allow_replace,
    }


def list_latest_theme_snapshots(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    state: ThemeState | str | None = None,
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit)
    params: list[Any] = []
    where = ""
    if state is not None:
        normalized_state = parse_str_enum(state, ThemeState, "state")
        where = "WHERE l.state = ?"
        params.append(normalized_state.value)
    params.append(bounded_limit)
    rows = connection.execute(
        f"""
        SELECT s.*
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshots AS s ON s.snapshot_id = l.snapshot_id
        {where}
        ORDER BY l.calculated_at DESC, l.theme_name ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_snapshot_row_to_dict(row) for row in rows]


def list_top_theme_snapshots_for_dashboard(
    connection: sqlite3.Connection,
    *,
    states: Sequence[ThemeState | str] = (ThemeState.LEADING, ThemeState.SPREADING),
    limit: int = 10,
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit)
    normalized_states = _dedupe_state_values(
        parse_str_enum(state, ThemeState, "state").value for state in states
    )
    if not normalized_states:
        return []

    placeholders = ", ".join("?" for _ in normalized_states)
    rows = connection.execute(
        f"""
        SELECT s.*
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshots AS s ON s.snapshot_id = l.snapshot_id
        WHERE l.state IN ({placeholders})
        ORDER BY
            CASE l.state
                WHEN 'LEADING' THEN 0
                WHEN 'SPREADING' THEN 1
                ELSE 9
            END,
            l.flow_score DESC,
            l.total_trade_value DESC,
            l.trade_value_delta_3m DESC,
            l.trade_value_delta_1m DESC,
            l.rising_ratio DESC,
            l.fresh_coverage_ratio DESC,
            l.calculated_at DESC,
            l.theme_name ASC
        LIMIT ?
        """,
        (*normalized_states, bounded_limit),
    ).fetchall()
    return [_snapshot_row_to_dict(row) for row in rows]


def get_latest_theme_snapshot(
    connection: sqlite3.Connection,
    theme_id: str,
    *,
    include_members: bool = False,
) -> dict[str, Any] | None:
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    row = connection.execute(
        """
        SELECT s.*
        FROM theme_latest_snapshots AS l
        JOIN theme_snapshots AS s ON s.snapshot_id = l.snapshot_id
        WHERE l.theme_id = ?
        """,
        (normalized_theme_id,),
    ).fetchone()
    if row is None:
        return None
    snapshot = _snapshot_row_to_dict(row)
    if include_members:
        snapshot["members"] = list_theme_snapshot_members(connection, snapshot["snapshot_id"])
    return snapshot


def list_theme_snapshots(
    connection: sqlite3.Connection,
    theme_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_theme_id = require_non_empty_str(theme_id, "theme_id")
    rows = connection.execute(
        """
        SELECT *
        FROM theme_snapshots
        WHERE theme_id = ?
        ORDER BY calculated_at DESC
        LIMIT ?
        """,
        (normalized_theme_id, _bounded_limit(limit)),
    ).fetchall()
    return [_snapshot_row_to_dict(row) for row in rows]


def list_theme_snapshot_members(
    connection: sqlite3.Connection,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    normalized_snapshot_id = require_non_empty_str(snapshot_id, "snapshot_id")
    rows = connection.execute(
        """
        SELECT *
        FROM theme_snapshot_members
        WHERE snapshot_id = ?
        ORDER BY
            CASE member_role
                WHEN 'LEADER_CANDIDATE' THEN 0
                WHEN 'CO_LEADER_CANDIDATE' THEN 1
                WHEN 'FOLLOWER_CANDIDATE' THEN 2
                WHEN 'LAGGARD' THEN 3
                WHEN 'STALE' THEN 4
                ELSE 5
            END,
            trade_value_delta_1m DESC,
            code ASC
        """,
        (normalized_snapshot_id,),
    ).fetchall()
    return [_snapshot_member_row_to_dict(row) for row in rows]


def list_theme_projection_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM theme_projection_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_projection_error_row_to_dict(row) for row in rows]


def list_theme_import_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM theme_import_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_import_error_row_to_dict(row) for row in rows]


def _calculate_member_snapshot(
    connection: sqlite3.Connection,
    *,
    theme: Mapping[str, Any],
    member: sqlite3.Row,
    calculated_at: str,
    settings: Settings,
) -> tuple[ThemeMemberSnapshot, float]:
    latest_tick = get_latest_tick(connection, member["code"])
    readiness = get_market_data_readiness(connection, member["code"], settings=settings)
    latest_scan = _latest_market_scan(connection, member["code"])
    bar_1m = _latest_bar(connection, member["code"], 60)
    bar_3m = _latest_bar(connection, member["code"], 180)
    bar_5m = _latest_bar(connection, member["code"], 300)
    vwap = _first_not_none(
        bar_1m["vwap"] if bar_1m is not None else None,
        bar_3m["vwap"] if bar_3m is not None else None,
        bar_5m["vwap"] if bar_5m is not None else None,
    )
    observation_source = "UNKNOWN"
    if latest_tick is not None:
        observation_source = "REALTIME_TICK"
    elif latest_scan is not None:
        observation_source = "MARKET_SCAN"
    price = _first_not_none(
        latest_tick["price"] if latest_tick is not None else None,
        latest_scan["price"] if latest_scan is not None else None,
    )
    change_rate = _first_not_none(
        latest_tick["change_rate"] if latest_tick is not None else None,
        latest_scan["change_rate"] if latest_scan is not None else None,
    )
    cumulative_trade_value = _first_not_none(
        latest_tick["cumulative_trade_value"] if latest_tick is not None else None,
        latest_scan["trade_value"] if latest_scan is not None else None,
    )
    scan_trade_value_delta, scan_previous = _scan_trade_value_delta(
        connection,
        latest_scan,
    )
    above_vwap = bool(price is not None and vwap is not None and float(price) >= float(vwap))
    snapshot = ThemeMemberSnapshot(
        theme_id=theme["theme_id"],
        theme_name=theme["theme_name"],
        code=member["code"],
        name=member["name"],
        price=price,
        change_rate=change_rate,
        cumulative_trade_value=cumulative_trade_value,
        volume_delta_1m=int(bar_1m["volume_delta"]) if bar_1m is not None else 0,
        trade_value_delta_1m=(
            float(bar_1m["trade_value_delta"])
            if bar_1m is not None
            else scan_trade_value_delta
        ),
        trade_value_delta_3m=(
            float(bar_3m["trade_value_delta"]) if bar_3m is not None else 0.0
        ),
        trade_value_delta_5m=(
            float(bar_5m["trade_value_delta"]) if bar_5m is not None else 0.0
        ),
        execution_strength=latest_tick["execution_strength"] if latest_tick is not None else None,
        vwap=vwap,
        above_vwap=above_vwap,
        readiness_status=(
            readiness["quality_status"]
            if latest_tick is not None
            else "FRESH"
            if latest_scan is not None
            else readiness["quality_status"]
        ),
        member_role=_base_member_role(latest_tick, readiness, latest_scan=latest_scan),
        observation_source=observation_source,
        tick_age_sec=(
            readiness["tick_age_sec"]
            if latest_tick is not None
            else _age_seconds(latest_scan["scanned_at"])
            if latest_scan is not None
            else readiness["tick_age_sec"]
        ),
        event_ts=(
            latest_tick["event_ts"]
            if latest_tick is not None
            else latest_scan["scanned_at"]
            if latest_scan is not None
            else None
        ),
        calculated_at=calculated_at,
        metadata={
            "condition_latest": _condition_observations(connection, member["code"]),
            "observation_source": observation_source,
            "market_scan": _market_scan_metadata(latest_scan, scan_previous),
        },
    )
    return snapshot, _member_score(snapshot)


def _save_theme_snapshot(connection: sqlite3.Connection, snapshot: ThemeSnapshot) -> None:
    data = snapshot.to_dict(include_members=False)
    connection.execute(
        "DELETE FROM theme_snapshot_members WHERE snapshot_id = ?",
        (snapshot.snapshot_id,),
    )
    connection.execute(
        """
        INSERT INTO theme_snapshots (
            snapshot_id,
            theme_id,
            theme_name,
            calculated_at,
            member_count,
            active_member_count,
            observed_member_count,
            fresh_member_count,
            fresh_coverage_ratio,
            scan_coverage_ratio,
            realtime_coverage_ratio,
            rising_member_count,
            rising_ratio,
            avg_change_rate,
            max_change_rate,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m,
            flow_trade_value_delta,
            flow_rank_inflow_count,
            flow_score,
            leading_code,
            leading_name,
            co_leader_codes_json,
            follower_codes_json,
            state,
            quality_status,
            reason_codes_json,
            metadata_json
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(snapshot_id) DO UPDATE SET
            theme_name = excluded.theme_name,
            calculated_at = excluded.calculated_at,
            member_count = excluded.member_count,
            active_member_count = excluded.active_member_count,
            observed_member_count = excluded.observed_member_count,
            fresh_member_count = excluded.fresh_member_count,
            fresh_coverage_ratio = excluded.fresh_coverage_ratio,
            scan_coverage_ratio = excluded.scan_coverage_ratio,
            realtime_coverage_ratio = excluded.realtime_coverage_ratio,
            rising_member_count = excluded.rising_member_count,
            rising_ratio = excluded.rising_ratio,
            avg_change_rate = excluded.avg_change_rate,
            max_change_rate = excluded.max_change_rate,
            total_trade_value = excluded.total_trade_value,
            trade_value_delta_1m = excluded.trade_value_delta_1m,
            trade_value_delta_3m = excluded.trade_value_delta_3m,
            trade_value_delta_5m = excluded.trade_value_delta_5m,
            flow_trade_value_delta = excluded.flow_trade_value_delta,
            flow_rank_inflow_count = excluded.flow_rank_inflow_count,
            flow_score = excluded.flow_score,
            leading_code = excluded.leading_code,
            leading_name = excluded.leading_name,
            co_leader_codes_json = excluded.co_leader_codes_json,
            follower_codes_json = excluded.follower_codes_json,
            state = excluded.state,
            quality_status = excluded.quality_status,
            reason_codes_json = excluded.reason_codes_json,
            metadata_json = excluded.metadata_json
        """,
        (
            data["snapshot_id"],
            data["theme_id"],
            data["theme_name"],
            data["calculated_at"],
            data["member_count"],
            data["active_member_count"],
            data["observed_member_count"],
            data["fresh_member_count"],
            data["fresh_coverage_ratio"],
            data["scan_coverage_ratio"],
            data["realtime_coverage_ratio"],
            data["rising_member_count"],
            data["rising_ratio"],
            data["avg_change_rate"],
            data["max_change_rate"],
            data["total_trade_value"],
            data["trade_value_delta_1m"],
            data["trade_value_delta_3m"],
            data["trade_value_delta_5m"],
            data["flow_trade_value_delta"],
            data["flow_rank_inflow_count"],
            data["flow_score"],
            data["leading_code"],
            data["leading_name"],
            _json_dumps(data["co_leader_codes"]),
            _json_dumps(data["follower_codes"]),
            data["state"],
            data["quality_status"],
            _json_dumps(data["reason_codes"]),
            canonical_json(data["metadata"]),
        ),
    )
    for member in snapshot.members:
        _insert_snapshot_member(connection, snapshot.snapshot_id, member)
    connection.execute(
        """
        INSERT INTO theme_latest_snapshots (
            theme_id,
            snapshot_id,
            theme_name,
            calculated_at,
            state,
            quality_status,
            leading_code,
            leading_name,
            fresh_coverage_ratio,
            scan_coverage_ratio,
            realtime_coverage_ratio,
            rising_ratio,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m,
            flow_trade_value_delta,
            flow_rank_inflow_count,
            flow_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(theme_id) DO UPDATE SET
            snapshot_id = excluded.snapshot_id,
            theme_name = excluded.theme_name,
            calculated_at = excluded.calculated_at,
            state = excluded.state,
            quality_status = excluded.quality_status,
            leading_code = excluded.leading_code,
            leading_name = excluded.leading_name,
            fresh_coverage_ratio = excluded.fresh_coverage_ratio,
            scan_coverage_ratio = excluded.scan_coverage_ratio,
            realtime_coverage_ratio = excluded.realtime_coverage_ratio,
            rising_ratio = excluded.rising_ratio,
            total_trade_value = excluded.total_trade_value,
            trade_value_delta_1m = excluded.trade_value_delta_1m,
            trade_value_delta_3m = excluded.trade_value_delta_3m,
            trade_value_delta_5m = excluded.trade_value_delta_5m,
            flow_trade_value_delta = excluded.flow_trade_value_delta,
            flow_rank_inflow_count = excluded.flow_rank_inflow_count,
            flow_score = excluded.flow_score
        """,
        (
            data["theme_id"],
            data["snapshot_id"],
            data["theme_name"],
            data["calculated_at"],
            data["state"],
            data["quality_status"],
            data["leading_code"],
            data["leading_name"],
            data["fresh_coverage_ratio"],
            data["scan_coverage_ratio"],
            data["realtime_coverage_ratio"],
            data["rising_ratio"],
            data["total_trade_value"],
            data["trade_value_delta_1m"],
            data["trade_value_delta_3m"],
            data["trade_value_delta_5m"],
            data["flow_trade_value_delta"],
            data["flow_rank_inflow_count"],
            data["flow_score"],
        ),
    )


def _insert_snapshot_member(
    connection: sqlite3.Connection,
    snapshot_id: str,
    member: ThemeMemberSnapshot,
) -> None:
    data = member.to_dict()
    connection.execute(
        """
        INSERT INTO theme_snapshot_members (
            snapshot_id,
            theme_id,
            code,
            name,
            price,
            change_rate,
            cumulative_trade_value,
            volume_delta_1m,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m,
            execution_strength,
            vwap,
            above_vwap,
            readiness_status,
            member_role,
            observation_source,
            tick_age_sec,
            event_ts,
            calculated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            data["theme_id"],
            data["code"],
            data["name"],
            data["price"],
            data["change_rate"],
            data["cumulative_trade_value"],
            data["volume_delta_1m"],
            data["trade_value_delta_1m"],
            data["trade_value_delta_3m"],
            data["trade_value_delta_5m"],
            data["execution_strength"],
            data["vwap"],
            1 if data["above_vwap"] else 0,
            data["readiness_status"],
            data["member_role"],
            data["observation_source"],
            data["tick_age_sec"],
            data["event_ts"],
            data["calculated_at"],
            canonical_json(data["metadata"]),
        ),
    )


def _classify_theme_snapshot(
    *,
    active_member_count: int,
    observed_member_count: int,
    fresh_coverage_ratio: float,
    scan_coverage_ratio: float,
    rising_ratio: float,
    leading_member: ThemeMemberSnapshot | None,
    total_trade_value: float,
    settings: Settings,
) -> tuple[ThemeState, ThemeSnapshotQuality, list[str]]:
    if active_member_count < settings.theme_min_active_members:
        return (
            ThemeState.DATA_WAIT,
            ThemeSnapshotQuality.MISSING_MEMBERSHIP,
            ["INSUFFICIENT_MEMBERSHIP"],
        )
    if observed_member_count == 0:
        return ThemeState.DATA_WAIT, ThemeSnapshotQuality.DATA_WAIT, ["NO_OBSERVED_MEMBERS"]
    coverage_for_state = (
        scan_coverage_ratio if settings.market_scan_enabled else fresh_coverage_ratio
    )
    if coverage_for_state < settings.theme_min_fresh_coverage_ratio:
        return ThemeState.DATA_WAIT, ThemeSnapshotQuality.PARTIAL, ["LOW_FRESH_COVERAGE"]

    reasons: list[str] = []
    if total_trade_value < settings.theme_min_total_trade_value:
        reasons.append("LOW_TOTAL_TRADE_VALUE")
    leader_change_rate = leading_member.change_rate if leading_member is not None else None
    leader_delta_1m = leading_member.trade_value_delta_1m if leading_member is not None else None
    if (
        rising_ratio >= settings.theme_leading_rising_ratio
        and leading_member is not None
        and leader_change_rate is not None
        and leader_change_rate >= settings.theme_leader_min_change_rate
        and leader_delta_1m is not None
        and leader_delta_1m >= settings.theme_leader_min_trade_value_delta_1m
        and total_trade_value >= settings.theme_min_total_trade_value
    ):
        return ThemeState.LEADING, ThemeSnapshotQuality.FRESH, reasons
    if rising_ratio >= settings.theme_spreading_rising_ratio:
        return ThemeState.SPREADING, ThemeSnapshotQuality.FRESH, reasons
    return ThemeState.WATCH, ThemeSnapshotQuality.FRESH, reasons


def _with_member_roles(
    members: list[ThemeMemberSnapshot],
    *,
    leader_code: str | None,
    co_leader_codes: Sequence[str],
    follower_codes: Sequence[str],
) -> list[ThemeMemberSnapshot]:
    co_leader_set = set(co_leader_codes)
    follower_set = set(follower_codes)
    resolved = []
    for member in members:
        role = member.member_role
        if member.code == leader_code:
            role = ThemeMemberRole.LEADER_CANDIDATE
        elif member.code in co_leader_set:
            role = ThemeMemberRole.CO_LEADER_CANDIDATE
        elif member.code in follower_set:
            role = ThemeMemberRole.FOLLOWER_CANDIDATE
        elif member.price is not None and member.readiness_status == "FRESH":
            role = ThemeMemberRole.LAGGARD
        resolved.append(
            ThemeMemberSnapshot(
                theme_id=member.theme_id,
                theme_name=member.theme_name,
                code=member.code,
                name=member.name,
                price=member.price,
                change_rate=member.change_rate,
                cumulative_trade_value=member.cumulative_trade_value,
                volume_delta_1m=member.volume_delta_1m,
                trade_value_delta_1m=member.trade_value_delta_1m,
                trade_value_delta_3m=member.trade_value_delta_3m,
                trade_value_delta_5m=member.trade_value_delta_5m,
                execution_strength=member.execution_strength,
                vwap=member.vwap,
                above_vwap=member.above_vwap,
                readiness_status=member.readiness_status,
                member_role=role,
                observation_source=member.observation_source,
                tick_age_sec=member.tick_age_sec,
                event_ts=member.event_ts,
                calculated_at=member.calculated_at,
                metadata=member.metadata,
            )
        )
    return resolved


def _base_member_role(
    latest_tick: Mapping[str, Any] | None,
    readiness: Mapping[str, Any],
    *,
    latest_scan: Mapping[str, Any] | None = None,
) -> ThemeMemberRole:
    if latest_tick is None:
        return ThemeMemberRole.LAGGARD if latest_scan is not None else ThemeMemberRole.UNKNOWN
    if readiness["quality_status"] != "FRESH":
        return ThemeMemberRole.STALE
    return ThemeMemberRole.LAGGARD


def _select_leader(
    members: Sequence[ThemeMemberSnapshot],
    scores: Mapping[str, float],
) -> ThemeMemberSnapshot | None:
    if not members:
        return None
    return max(members, key=lambda member: (scores.get(member.code, -1.0), member.code))


def _select_co_leaders(
    members: Sequence[ThemeMemberSnapshot],
    scores: Mapping[str, float],
    *,
    leader_code: str | None,
    score_ratio: float,
) -> list[str]:
    if leader_code is None:
        return []
    leader_score = scores.get(leader_code, 0.0)
    if leader_score <= 0:
        return []
    threshold = leader_score * score_ratio
    candidates = [
        member
        for member in members
        if member.code != leader_code
        and scores.get(member.code, 0.0) >= threshold
        and member.change_rate is not None
        and member.change_rate > 0
    ]
    candidates.sort(key=lambda member: (-scores.get(member.code, 0.0), member.code))
    return [member.code for member in candidates]


def _select_followers(
    members: Sequence[ThemeMemberSnapshot],
    *,
    leader_code: str | None,
    co_leader_codes: Sequence[str],
) -> list[str]:
    excluded = {leader_code, *co_leader_codes}
    return [
        member.code
        for member in members
        if member.code not in excluded
        and member.change_rate is not None
        and member.change_rate > 0
    ]


def _member_score(member: ThemeMemberSnapshot) -> float:
    if member.price is None:
        return -1.0
    score = (
        (member.change_rate or 0.0) * 100.0
        + member.trade_value_delta_1m / 1_000_000.0
        + member.trade_value_delta_3m / 3_000_000.0
        + (member.cumulative_trade_value or 0.0) / 1_000_000_000.0
        + (member.execution_strength or 0.0) / 100.0
    )
    if member.readiness_status != "FRESH":
        score *= 0.5
    return score


def _active_theme_members_for_snapshot(
    connection: sqlite3.Connection,
    theme_id: str,
    *,
    limit: int,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM theme_members
        WHERE theme_id = ? AND active = 1
        ORDER BY weight DESC, code ASC
        LIMIT ?
        """,
        (theme_id, limit),
    ).fetchall()


def _count_theme_members(connection: sqlite3.Connection, theme_id: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM theme_members
        WHERE theme_id = ?
        """,
        (theme_id,),
    ).fetchone()
    return int(row["count"])


def _latest_bar(
    connection: sqlite3.Connection,
    code: str,
    interval_sec: int,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (code, interval_sec),
    ).fetchone()


def _latest_market_scan(connection: sqlite3.Connection, code: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM market_scan_latest
        WHERE code = ?
        """,
        (validate_stock_code(code),),
    ).fetchone()
    if row is None:
        return None
    data = row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _previous_market_scan(
    connection: sqlite3.Connection,
    latest_scan: Mapping[str, Any],
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM market_scan_snapshots
        WHERE code = ? AND scanned_at < ?
        ORDER BY scanned_at DESC, rank ASC
        LIMIT 1
        """,
        (latest_scan["code"], latest_scan["scanned_at"]),
    ).fetchone()
    if row is None:
        return None
    data = row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _scan_trade_value_delta(
    connection: sqlite3.Connection,
    latest_scan: Mapping[str, Any] | None,
) -> tuple[float, Mapping[str, Any] | None]:
    if latest_scan is None or latest_scan.get("trade_value") is None:
        return 0.0, None
    previous = _previous_market_scan(connection, latest_scan)
    if previous is None or previous.get("trade_value") is None:
        return 0.0, previous
    return max(float(latest_scan["trade_value"]) - float(previous["trade_value"]), 0.0), previous


def _market_scan_metadata(
    latest_scan: Mapping[str, Any] | None,
    previous_scan: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if latest_scan is None:
        return None
    current = {
        "scan_id": latest_scan.get("scan_id"),
        "scan_type": latest_scan.get("scan_type"),
        "market": latest_scan.get("market"),
        "rank": latest_scan.get("rank"),
        "scanned_at": latest_scan.get("scanned_at"),
        "parser_status": _nested_metadata_value(latest_scan, "parser_status"),
    }
    previous = None
    if previous_scan is not None:
        previous = {
            "scan_id": previous_scan.get("scan_id"),
            "rank": previous_scan.get("rank"),
            "trade_value": previous_scan.get("trade_value"),
            "scanned_at": previous_scan.get("scanned_at"),
        }
    return {
        "current": current,
        "previous": previous,
    }


def _theme_flow_metrics(
    members: Sequence[ThemeMemberSnapshot],
    *,
    active_member_count: int,
    rising_ratio: float,
    top_n: int,
) -> dict[str, Any]:
    deltas: list[float] = []
    inflow_count = 0
    for member in members:
        metadata = (
            member.metadata.get("market_scan") if isinstance(member.metadata, Mapping) else None
        )
        if not isinstance(metadata, Mapping):
            deltas.append(0.0)
            continue
        current = metadata.get("current")
        previous = metadata.get("previous")
        current_rank = _metadata_rank(current)
        previous_rank = _metadata_rank(previous)
        trade_delta = max(float(member.trade_value_delta_1m or 0.0), 0.0)
        deltas.append(trade_delta)
        if current_rank is not None and current_rank <= top_n and (
            previous_rank is None or previous_rank > top_n
        ):
            inflow_count += 1

    flow_delta = sum(deltas)
    denominator = max(active_member_count, 1)
    mean_log_delta = sum(math.log1p(delta) for delta in deltas) / denominator
    normalized_delta_score = min(mean_log_delta / math.log1p(1_000_000_000), 1.0) * 45.0
    inflow_score = _ratio(inflow_count, denominator) * 35.0
    breadth_score = max(min(rising_ratio, 1.0), 0.0) * 20.0
    concentration = _ratio(max(deltas) if deltas else 0.0, flow_delta)
    concentration_penalty = max(concentration - 0.7, 0.0) * 40.0
    flow_score = max(
        normalized_delta_score + inflow_score + breadth_score - concentration_penalty,
        0.0,
    )
    return {
        "flow_trade_value_delta": flow_delta,
        "flow_rank_inflow_count": inflow_count,
        "flow_score": round(flow_score, 6),
        "flow_score_formula": (
            "min(mean(log1p(member_delta))/log1p(1e9),1)*45 + "
            "rank_inflow_ratio*35 + rising_ratio*20 - concentration_penalty"
        ),
        "flow_score_components": {
            "normalized_delta_score": round(normalized_delta_score, 6),
            "inflow_score": round(inflow_score, 6),
            "breadth_score": round(breadth_score, 6),
            "concentration_penalty": round(concentration_penalty, 6),
        },
    }


def _metadata_rank(value: object) -> int | None:
    if not isinstance(value, Mapping):
        return None
    try:
        rank = int(value.get("rank"))
    except (TypeError, ValueError):
        return None
    return rank if rank > 0 else None


def _nested_metadata_value(item: Mapping[str, Any], key: str) -> Any:
    metadata = item.get("metadata")
    return metadata.get(key) if isinstance(metadata, Mapping) else None


def _condition_observations(connection: sqlite3.Connection, code: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_latest
        WHERE code = ?
        ORDER BY event_ts DESC, condition_id ASC
        """,
        (code,),
    ).fetchall()
    observations = []
    for row in rows:
        item = row_to_dict(row)
        item["metadata"] = json.loads(item.pop("metadata_json"))
        observations.append(item)
    return observations


def _record_projection_error(
    connection: sqlite3.Connection,
    *,
    theme_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO theme_projection_errors (
            theme_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            theme_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            canonical_json(payload),
        ),
    )


def _record_import_batch(
    connection: sqlite3.Connection,
    *,
    batch_id: str,
    source_type: ThemeSourceType,
    source_name: str | None,
    theme_count: int,
    member_count: int,
    status: str,
    error_message: str | None,
    payload_hash: str | None,
) -> None:
    connection.execute(
        """
        INSERT INTO theme_import_batches (
            batch_id,
            source_type,
            source_name,
            theme_count,
            member_count,
            status,
            error_message,
            payload_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            source_type.value,
            source_name,
            theme_count,
            member_count,
            status,
            error_message,
            payload_hash,
        ),
    )


def _deactivate_theme_members_for_source(
    connection: sqlite3.Connection,
    *,
    theme_id: str,
    source_type: ThemeSourceType,
    source_name: str | None,
) -> None:
    connection.execute(
        """
        UPDATE theme_members
        SET active = 0,
            updated_at = ?
        WHERE theme_id = ?
            AND source_type = ?
            AND COALESCE(source_name, '') = COALESCE(?, '')
        """,
        (datetime_to_wire(utc_now()), theme_id, source_type.value, source_name),
    )


def _payload_themes(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    themes = payload.get("themes")
    if not isinstance(themes, list):
        raise ValueError("themes must be a list")
    normalized: list[Mapping[str, Any]] = []
    for theme in themes:
        if not isinstance(theme, Mapping):
            raise ValueError("each theme must be an object")
        normalized.append(theme)
    return normalized


def _theme_members(theme: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    members = theme.get("members")
    if not isinstance(members, list):
        raise ValueError("theme members must be a list")
    normalized: list[Mapping[str, Any]] = []
    for member in members:
        if not isinstance(member, Mapping):
            raise ValueError("each member must be an object")
        normalized.append(member)
    return normalized


def _metadata(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata must be an object")
    return metadata


def _parse_source_type(value: ThemeSourceType | str | object) -> ThemeSourceType:
    return parse_str_enum(value, ThemeSourceType, "source_type")


def _optional_source_name(value: object) -> str | None:
    if value is None:
        return None
    return require_non_empty_str(value, "source_name")


def _optional_non_empty(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return require_non_empty_str(value, field_name)


def _timestamp_wire(value: datetime | str | None) -> str:
    return datetime_to_wire(parse_timestamp(value or utc_now(), "calculated_at"))


def _snapshot_id(theme_id: str, calculated_at: str) -> str:
    digest = hashlib.sha256(f"{theme_id}|{calculated_at}".encode()).hexdigest()[:16]
    return f"theme_snapshot_{theme_id}_{digest}"


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except Exception:
        return None


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _theme_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["active"] = bool(data["active"])
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _theme_member_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["active"] = bool(data["active"])
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["co_leader_codes"] = json.loads(data.pop("co_leader_codes_json"))
    data["follower_codes"] = json.loads(data.pop("follower_codes_json"))
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _snapshot_member_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["above_vwap"] = bool(data["above_vwap"])
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _projection_error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _import_error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = row_to_dict(row)
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    where: str | None = None,
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _dedupe_state_values(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
