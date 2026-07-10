from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_index_service import (
    get_latest_market_index_tick,
    get_market_index_readiness,
    normalize_index_code,
)
from services.market_reference_service import get_market_for_code

REGIME_TARGET_MARKET = "__MARKET__"
MARKET_REGIME_STATUSES: frozenset[str] = frozenset(
    {"RISK_ON", "NEUTRAL", "WEAK", "RISK_OFF", "DATA_WAIT"}
)


def rebuild_market_regime_snapshot(
    connection: sqlite3.Connection,
    code: str | None = None,
    *,
    settings: Settings | None = None,
    source_event_id: str | None = None,
    source_projection: str | None = None,
    generated_by: str | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    target_code = REGIME_TARGET_MARKET if code is None else validate_stock_code(code)
    source_lineage = {
        key: value
        for key, value in {
            "source_event_id": source_event_id,
            "source_projection": source_projection,
            "generated_by": generated_by,
        }.items()
        if value
    }
    market, primary_index, secondary_index = _resolve_market_indexes(connection, target_code)
    snapshot_at = datetime_to_wire(utc_now())

    if not resolved_settings.market_regime_enabled:
        snapshot = _snapshot_payload(
            target_code=target_code,
            market=market,
            primary_index_code=primary_index,
            secondary_index_code=secondary_index,
            regime_status="DATA_WAIT",
            quality_status="DEGRADED",
            reason_codes=["MARKET_REGIME_DISABLED"],
            evidence={"enabled": False, **source_lineage},
            snapshot_at=snapshot_at,
        )
        _insert_snapshot(connection, snapshot)
        connection.commit()
        return snapshot

    primary = _index_context(connection, primary_index, settings=resolved_settings)
    secondary = (
        _index_context(connection, secondary_index, settings=resolved_settings)
        if secondary_index is not None
        else None
    )
    regime_status, quality_status, reason_codes = _classify_regime(
        market=market,
        primary=primary,
        secondary=secondary,
        settings=resolved_settings,
    )
    evidence = {
        "enabled": True,
        "primary_index": primary,
        "secondary_index": secondary,
        "thresholds": {
            "risk_on_return_5m": resolved_settings.market_regime_risk_on_return_5m,
            "weak_drawdown_15m": resolved_settings.market_regime_weak_drawdown_15m,
            "risk_off_return_5m": resolved_settings.market_regime_risk_off_return_5m,
            "risk_off_drawdown_15m": resolved_settings.market_regime_risk_off_drawdown_15m,
            "secondary_risk_off_return_5m": (
                resolved_settings.market_regime_secondary_risk_off_return_5m
            ),
        },
        "observe_only": True,
        **source_lineage,
    }
    snapshot = _snapshot_payload(
        target_code=target_code,
        market=market,
        primary_index_code=primary_index,
        secondary_index_code=secondary_index,
        regime_status=regime_status,
        quality_status=quality_status,
        primary_return_5m=primary.get("return_5m"),
        primary_drawdown_15m=primary.get("drawdown_15m"),
        secondary_return_5m=None if secondary is None else secondary.get("return_5m"),
        secondary_drawdown_15m=None if secondary is None else secondary.get("drawdown_15m"),
        reason_codes=reason_codes,
        evidence=evidence,
        snapshot_at=snapshot_at,
    )
    _insert_snapshot(connection, snapshot)
    connection.commit()
    return snapshot


def get_latest_market_regime(connection: sqlite3.Connection) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM market_regime_snapshots
        WHERE target_code = ?
        ORDER BY snapshot_at DESC, created_at DESC
        LIMIT 1
        """,
        (REGIME_TARGET_MARKET,),
    ).fetchone()
    if row is None:
        row = connection.execute(
            """
            SELECT *
            FROM market_regime_snapshots
            ORDER BY snapshot_at DESC, created_at DESC
            LIMIT 1
            """
        ).fetchone()
    return None if row is None else _snapshot_row_to_dict(row)


def should_rebuild_market_regime_snapshot(
    connection: sqlite3.Connection,
    *,
    min_interval_sec: float = 5.0,
) -> bool:
    latest = get_latest_market_regime(connection)
    if latest is None:
        return True
    snapshot_at = latest.get("snapshot_at")
    if not snapshot_at:
        return True
    try:
        latest_at = parse_timestamp(str(snapshot_at), "market_regime_snapshot_at")
    except ValueError:
        return True
    return utc_now() - latest_at >= timedelta(seconds=max(float(min_interval_sec), 0.0))


def get_market_regime_for_code(
    connection: sqlite3.Connection,
    code: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    return rebuild_market_regime_snapshot(connection, code, settings=settings)


def get_market_regime_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest = get_latest_market_regime(connection)
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_regime_snapshots
        """
    ).fetchone()
    return {
        "enabled": resolved_settings.market_regime_enabled,
        "snapshot_count": int(row["count"] if row else 0),
        "latest": latest,
        "stale_sec": resolved_settings.market_index_stale_sec,
        "observe_only": True,
    }


def _resolve_market_indexes(
    connection: sqlite3.Connection,
    target_code: str,
) -> tuple[str | None, str, str | None]:
    if target_code == REGIME_TARGET_MARKET:
        return None, "KOSPI", "KOSDAQ"
    membership = get_market_for_code(connection, target_code)
    if membership is None:
        return None, "UNKNOWN", None
    market = str(membership["market"]).upper()
    if market == "KOSDAQ":
        return market, "KOSDAQ", "KOSPI"
    if market == "KOSPI":
        return market, "KOSPI", "KOSDAQ"
    return market, "UNKNOWN", None


def _index_context(
    connection: sqlite3.Connection,
    index_code: str,
    *,
    settings: Settings,
) -> dict[str, Any]:
    if index_code == "UNKNOWN":
        return {
            "index_code": "UNKNOWN",
            "quality_status": "DEGRADED",
            "has_latest_tick": False,
            "reason_codes": ["MARKET_MEMBERSHIP_UNKNOWN"],
            "return_5m": None,
            "drawdown_15m": None,
        }
    normalized_code = normalize_index_code(index_code)
    latest = get_latest_market_index_tick(connection, normalized_code)
    readiness = get_market_index_readiness(
        connection,
        normalized_code,
        settings=settings,
    )
    if latest is None:
        return {
            "index_code": normalized_code,
            "quality_status": readiness["quality_status"],
            "has_latest_tick": False,
            "reason_codes": readiness["reason_codes"],
            "return_5m": None,
            "drawdown_15m": None,
        }
    latest_ts = parse_timestamp(latest["event_ts"], "event_ts")
    samples_15m = _sample_prices(
        connection,
        normalized_code,
        start_at=datetime_to_wire(latest_ts - timedelta(minutes=15)),
        end_at=datetime_to_wire(latest_ts),
    )
    samples_5m = [
        item
        for item in samples_15m
        if parse_timestamp(item["event_ts"], "event_ts") >= latest_ts - timedelta(minutes=5)
    ]
    latest_price = float(latest["price"])
    baseline_5m = float(samples_5m[0]["price"]) if samples_5m else latest_price
    high_15m = max((float(item["price"]) for item in samples_15m), default=latest_price)
    return_5m = _pct(latest_price - baseline_5m, baseline_5m)
    drawdown_15m = _pct(latest_price - high_15m, high_15m)
    return {
        "index_code": normalized_code,
        "index_name": latest["index_name"],
        "price": latest_price,
        "change_rate": latest["change_rate"],
        "quality_status": readiness["quality_status"],
        "has_latest_tick": readiness["has_latest_tick"],
        "tick_age_sec": readiness["tick_age_sec"],
        "reason_codes": readiness["reason_codes"],
        "return_5m": return_5m,
        "drawdown_15m": drawdown_15m,
        "sample_count_15m": len(samples_15m),
    }


def _classify_regime(
    *,
    market: str | None,
    primary: Mapping[str, Any],
    secondary: Mapping[str, Any] | None,
    settings: Settings,
) -> tuple[str, str, list[str]]:
    reasons: list[str] = []
    primary_quality = str(primary.get("quality_status") or "MISSING").upper()
    secondary_quality = (
        str(secondary.get("quality_status") or "MISSING").upper()
        if secondary is not None
        else "MISSING"
    )
    if market is None and primary.get("index_code") == "UNKNOWN":
        return "DATA_WAIT", "DEGRADED", ["MARKET_MEMBERSHIP_UNKNOWN"]
    if primary_quality != "FRESH":
        if primary_quality in {"STALE", "DEGRADED"}:
            reasons.append("MARKET_INDEX_STALE")
        else:
            reasons.append("MARKET_INDEX_MISSING")
        return "DATA_WAIT", primary_quality, reasons
    quality_status = "FRESH"
    if secondary is not None and secondary_quality != "FRESH":
        quality_status = secondary_quality if secondary_quality != "MISSING" else "DEGRADED"
        reasons.append("SECONDARY_INDEX_DATA_WAIT")
        if secondary_quality in {"STALE", "DEGRADED"}:
            reasons.append("MARKET_INDEX_STALE")

    primary_return = _number(primary.get("return_5m"))
    primary_drawdown = _number(primary.get("drawdown_15m"))
    secondary_return = _number(secondary.get("return_5m")) if secondary is not None else None

    if (
        primary_return is not None
        and primary_return <= settings.market_regime_risk_off_return_5m
    ) or (
        primary_drawdown is not None
        and primary_drawdown <= settings.market_regime_risk_off_drawdown_15m
    ):
        return "RISK_OFF", quality_status, [*reasons, "PRIMARY_INDEX_RISK_OFF"]
    if (
        secondary_quality == "FRESH"
        and secondary_return is not None
        and secondary_return <= settings.market_regime_secondary_risk_off_return_5m
    ):
        return "RISK_OFF", quality_status, [*reasons, "SECONDARY_INDEX_RISK_OFF"]
    if (
        primary_drawdown is not None
        and primary_drawdown <= settings.market_regime_weak_drawdown_15m
    ):
        return "WEAK", quality_status, [*reasons, "MARKET_REGIME_WEAK"]
    if (
        primary_return is not None
        and primary_return >= settings.market_regime_risk_on_return_5m
    ):
        return "RISK_ON", quality_status, [*reasons, "MARKET_REGIME_ALIGNED"]
    return "NEUTRAL", quality_status, [*reasons, "MARKET_REGIME_NEUTRAL"]


def _snapshot_payload(
    *,
    target_code: str,
    market: str | None,
    primary_index_code: str,
    secondary_index_code: str | None,
    regime_status: str,
    quality_status: str,
    reason_codes: list[str],
    evidence: Mapping[str, Any],
    snapshot_at: str,
    primary_return_5m: float | None = None,
    primary_drawdown_15m: float | None = None,
    secondary_return_5m: float | None = None,
    secondary_drawdown_15m: float | None = None,
) -> dict[str, Any]:
    return {
        "snapshot_id": new_message_id("market_regime"),
        "target_code": target_code,
        "market": market,
        "primary_index_code": primary_index_code,
        "secondary_index_code": secondary_index_code,
        "regime_status": regime_status,
        "quality_status": quality_status,
        "primary_return_5m": primary_return_5m,
        "primary_drawdown_15m": primary_drawdown_15m,
        "secondary_return_5m": secondary_return_5m,
        "secondary_drawdown_15m": secondary_drawdown_15m,
        "reason_codes": _merge_reasons(reason_codes),
        "evidence_json": dict(evidence),
        "snapshot_at": snapshot_at,
    }


def _insert_snapshot(connection: sqlite3.Connection, snapshot: Mapping[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO market_regime_snapshots (
            snapshot_id,
            source_event_id,
            source_projection,
            generated_by,
            target_code,
            market,
            primary_index_code,
            secondary_index_code,
            regime_status,
            quality_status,
            primary_return_5m,
            primary_drawdown_15m,
            secondary_return_5m,
            secondary_drawdown_15m,
            reason_codes_json,
            evidence_json,
            snapshot_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_id"],
            snapshot.get("evidence_json", {}).get("source_event_id"),
            snapshot.get("evidence_json", {}).get("source_projection"),
            snapshot.get("evidence_json", {}).get("generated_by"),
            snapshot["target_code"],
            snapshot.get("market"),
            snapshot["primary_index_code"],
            snapshot.get("secondary_index_code"),
            snapshot["regime_status"],
            snapshot["quality_status"],
            snapshot.get("primary_return_5m"),
            snapshot.get("primary_drawdown_15m"),
            snapshot.get("secondary_return_5m"),
            snapshot.get("secondary_drawdown_15m"),
            _json_dumps(snapshot.get("reason_codes", [])),
            canonical_json(snapshot.get("evidence_json", {})),
            snapshot["snapshot_at"],
        ),
    )


def _sample_prices(
    connection: sqlite3.Connection,
    index_code: str,
    *,
    start_at: str,
    end_at: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT event_ts, price
        FROM market_index_tick_samples
        WHERE index_code = ?
            AND event_ts >= ?
            AND event_ts <= ?
        ORDER BY event_ts ASC, event_id ASC
        """,
        (index_code, start_at, end_at),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["evidence_json"] = json.loads(data.pop("evidence_json"))
    return data


def _pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator * 100.0


def _number(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
