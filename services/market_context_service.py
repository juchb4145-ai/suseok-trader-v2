from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from domain.broker.utils import (
    MARKET_TIMEZONE,
    datetime_to_wire,
    market_today,
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
)
from services.market_reference_service import get_market_for_code
from services.market_regime_service import (
    evaluate_market_regime_for_market,
    rebuild_market_regime_snapshot,
)

MARKET_CONTEXT_MARKETS: tuple[str, ...] = ("KOSPI", "KOSDAQ")


def rebuild_market_context_snapshots(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    source_event_id: str | None = None,
    source_projection: str | None = None,
    generated_by: str = "market_context_service",
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    normalized_generated_by = str(generated_by or "market_context_service").strip()
    watermark = _market_index_watermark(connection, settings=resolved_settings)
    watermark_json = canonical_json(watermark)
    watermark_identity = _watermark_identity(watermark)
    watermark_hash = hashlib.sha256(
        canonical_json(watermark_identity).encode("utf-8")
    ).hexdigest()
    trade_date = _watermark_trade_date(watermark)
    existing = {
        market: _get_market_context_by_watermark(
            connection,
            market=market,
            source_watermark_hash=watermark_hash,
        )
        for market in MARKET_CONTEXT_MARKETS
    }
    linked_regime = _linked_regime_snapshot(connection, source_event_id)
    requires_regime_refresh = linked_regime is None and (
        source_event_id is not None or any(item is None for item in existing.values())
    )
    if requires_regime_refresh:
        linked_regime = rebuild_market_regime_snapshot(
            connection,
            settings=resolved_settings,
            source_event_id=source_event_id,
            source_projection=source_projection,
            generated_by=normalized_generated_by,
        )
    elif linked_regime is None:
        linked_regime = _latest_global_regime_snapshot(connection)

    created_count, snapshots = _persist_market_context_snapshots(
        connection,
        settings=resolved_settings,
        existing=existing,
        watermark=watermark,
        watermark_identity=watermark_identity,
        watermark_hash=watermark_hash,
        watermark_json=watermark_json,
        trade_date=trade_date,
        linked_regime=linked_regime,
        source_event_id=source_event_id,
        source_projection=source_projection,
        generated_by=normalized_generated_by,
    )
    return {
        "status": "APPLIED" if created_count else "APPLIED_BY_VERIFY",
        "created_count": created_count,
        "verified_count": len(snapshots) - created_count,
        "source_watermark_hash": watermark_hash,
        "source_watermark": watermark,
        "source_watermark_identity": watermark_identity,
        "trade_date": trade_date,
        "global_regime": linked_regime,
        "snapshots": snapshots,
        "generated_by": normalized_generated_by,
        "no_trading_side_effects": True,
    }


def should_rebuild_market_context_snapshots(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    min_interval_sec: float = 5.0,
) -> bool:
    resolved_settings = settings or load_settings()
    latest = {
        market: get_latest_market_context(connection, market)
        for market in MARKET_CONTEXT_MARKETS
    }
    if any(item is None for item in latest.values()):
        return True
    latest_items = [item for item in latest.values() if item is not None]
    latest_hashes = {str(item["source_watermark_hash"]) for item in latest_items}
    if len(latest_hashes) != 1:
        return True

    current_watermark = _market_index_watermark(
        connection,
        settings=resolved_settings,
    )
    current_hash = hashlib.sha256(
        canonical_json(_watermark_identity(current_watermark)).encode("utf-8")
    ).hexdigest()
    if current_hash in latest_hashes:
        return False

    current_data_ready = all(
        bool(_mapping(current_watermark.get(market)).get("data_usable"))
        for market in MARKET_CONTEXT_MARKETS
    )
    current_parser_ready = all(
        str(
            _mapping(current_watermark.get(market)).get("parser_status") or ""
        ).upper()
        == "VERIFIED"
        for market in MARKET_CONTEXT_MARKETS
    )
    latest_data_ready = all(
        bool(item.get("trading_data_usable")) for item in latest_items
    )
    latest_parser_ready = all(
        str(item.get("parser_confidence_status") or "").upper() == "VERIFIED"
        for item in latest_items
    )
    if (current_data_ready and not latest_data_ready) or (
        current_parser_ready and not latest_parser_ready
    ):
        return True

    latest_times = []
    for item in latest_items:
        try:
            latest_times.append(parse_timestamp(str(item["snapshot_at"]), "snapshot_at"))
        except (KeyError, ValueError):
            return True
    if not latest_times:
        return True
    return utc_now() - max(latest_times) >= timedelta(
        seconds=max(float(min_interval_sec), 0.0)
    )


def _persist_market_context_snapshots(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    existing: Mapping[str, dict[str, Any] | None],
    watermark: Mapping[str, Any],
    watermark_identity: Mapping[str, Any],
    watermark_hash: str,
    watermark_json: str,
    trade_date: str,
    linked_regime: Mapping[str, Any] | None,
    source_event_id: str | None,
    source_projection: str | None,
    generated_by: str,
) -> tuple[int, list[dict[str, Any]]]:
    created_count = 0
    snapshots: list[dict[str, Any]] = []
    snapshot_at = datetime_to_wire(utc_now())
    try:
        for market in MARKET_CONTEXT_MARKETS:
            snapshot = existing[market]
            if snapshot is None:
                regime = evaluate_market_regime_for_market(
                    connection,
                    market,
                    settings=settings,
                )
                parser_confidence_status = _parser_confidence_status(
                    watermark,
                    primary_market=market,
                )
                primary = _mapping(watermark.get(market))
                data_quality_status = str(
                    regime.get("quality_status")
                    or primary.get("quality_status")
                    or "MISSING"
                ).upper()
                secondary = _mapping(
                    watermark.get("KOSDAQ" if market == "KOSPI" else "KOSPI")
                )
                trading_data_usable = bool(
                    primary.get("data_usable")
                    and secondary.get("data_usable")
                    and data_quality_status == "FRESH"
                    and str(regime.get("regime_status") or "") != "DATA_WAIT"
                )
                evidence = {
                    "source_event_id": source_event_id,
                    "source_projection": source_projection,
                    "source_watermark_identity": watermark_identity,
                    "source_regime_snapshot_id": (
                        None
                        if linked_regime is None
                        else linked_regime.get("snapshot_id")
                    ),
                    "primary_index": primary,
                    "secondary_index": secondary,
                    "parser_confidence_separate_from_data_quality": True,
                    "trading_eligible": bool(
                        trading_data_usable and parser_confidence_status == "VERIFIED"
                    ),
                    "observe_only": True,
                    "no_trading_side_effects": True,
                }
                snapshot_id = new_message_id("market_context")
                connection.execute(
                    """
                    INSERT INTO market_context_snapshots (
                        snapshot_id, trade_date, market, source_watermark_hash,
                        source_watermark_json, source_regime_snapshot_id, source_event_id,
                        regime_status, quality_status, parser_confidence_status,
                        data_quality_status, trading_data_usable, market_regime_json,
                        evidence_json, snapshot_at, generated_by
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(market, source_watermark_hash) DO NOTHING
                    """,
                    (
                        snapshot_id,
                        trade_date,
                        market,
                        watermark_hash,
                        watermark_json,
                        None
                        if linked_regime is None
                        else linked_regime.get("snapshot_id"),
                        source_event_id,
                        str(regime.get("regime_status") or "DATA_WAIT"),
                        str(regime.get("quality_status") or "MISSING"),
                        parser_confidence_status,
                        data_quality_status,
                        int(trading_data_usable),
                        canonical_json(regime),
                        canonical_json(evidence),
                        snapshot_at,
                        generated_by,
                    ),
                )
                snapshot = _get_market_context_by_watermark(
                    connection,
                    market=market,
                    source_watermark_hash=watermark_hash,
                )
                if snapshot is None:
                    raise RuntimeError(
                        f"market context snapshot missing after insert: {market}"
                    )
                if snapshot["snapshot_id"] == snapshot_id:
                    created_count += 1
            _upsert_market_context_latest(connection, snapshot)
            snapshots.append(snapshot)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return created_count, snapshots


def get_latest_market_context(
    connection: sqlite3.Connection,
    market: str,
) -> dict[str, Any] | None:
    normalized_market = _normalize_market(market)
    row = connection.execute(
        """
        SELECT snapshot.*
        FROM market_context_latest AS latest
        JOIN market_context_snapshots AS snapshot
          ON snapshot.snapshot_id = latest.snapshot_id
        WHERE latest.market = ?
        LIMIT 1
        """,
        (normalized_market,),
    ).fetchone()
    return None if row is None else _snapshot_row_to_dict(row)


def get_market_context_for_code(
    connection: sqlite3.Connection,
    code: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    normalized_code = validate_stock_code(code)
    membership = get_market_for_code(connection, normalized_code)
    if membership is None:
        return _missing_context(
            code=normalized_code,
            market=None,
            reason_code="MARKET_MEMBERSHIP_UNKNOWN",
        )
    market = str(membership.get("market") or "").upper()
    if market not in MARKET_CONTEXT_MARKETS:
        return _missing_context(
            code=normalized_code,
            market=market or None,
            reason_code="MARKET_CONTEXT_MARKET_UNSUPPORTED",
        )
    snapshot = get_latest_market_context(connection, market)
    if snapshot is None:
        return _missing_context(
            code=normalized_code,
            market=market,
            reason_code="MARKET_CONTEXT_SNAPSHOT_MISSING",
        )
    age_sec = _snapshot_age_sec(snapshot.get("snapshot_at"))
    stale = bool(
        age_sec is None
        or age_sec > resolved_settings.market_context_snapshot_stale_sec
    )
    regime = dict(_mapping(snapshot.get("market_regime")))
    reason_codes = [str(value) for value in regime.get("reason_codes") or []]
    if stale:
        regime["regime_status"] = "DATA_WAIT"
        regime["quality_status"] = "STALE"
        regime["reason_codes"] = list(
            dict.fromkeys([*reason_codes, "MARKET_CONTEXT_SNAPSHOT_STALE"])
        )
    parser_confidence_status = str(
        snapshot.get("parser_confidence_status") or "MISSING"
    ).upper()
    trading_data_usable = bool(snapshot.get("trading_data_usable")) and not stale
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "trade_date": snapshot["trade_date"],
        "code": normalized_code,
        "market": market,
        "source_watermark_hash": snapshot["source_watermark_hash"],
        "source_watermark": snapshot["source_watermark"],
        "source_regime_snapshot_id": snapshot.get("source_regime_snapshot_id"),
        "parser_confidence_status": parser_confidence_status,
        "data_quality_status": (
            "STALE" if stale else snapshot.get("data_quality_status")
        ),
        "trading_data_usable": trading_data_usable,
        "trading_eligible": bool(
            trading_data_usable and parser_confidence_status == "VERIFIED"
        ),
        "data_age_sec": age_sec,
        "generated_by": snapshot["generated_by"],
        "snapshot_at": snapshot["snapshot_at"],
        "market_regime": regime,
        "reason_codes": list(regime.get("reason_codes") or []),
        "read_only": True,
    }


def get_market_context_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest = {
        market: get_latest_market_context(connection, market)
        for market in MARKET_CONTEXT_MARKETS
    }
    watermark_hashes = {
        str(item.get("source_watermark_hash"))
        for item in latest.values()
        if item is not None
    }
    regime_snapshot_ids = {
        item.get("source_regime_snapshot_id")
        for item in latest.values()
        if item is not None
    }
    latest_regime_coherent = bool(
        len(regime_snapshot_ids) == 1 and None not in regime_snapshot_ids
    )
    regime_reference_missing_count = sum(
        1
        for snapshot_id in regime_snapshot_ids
        if snapshot_id is None
        or connection.execute(
            "SELECT 1 FROM market_regime_snapshots WHERE snapshot_id = ? LIMIT 1",
            (snapshot_id,),
        ).fetchone()
        is None
    )
    stale_markets: list[str] = []
    parser_unverified_markets: list[str] = []
    data_unusable_markets: list[str] = []
    for market, item in latest.items():
        if item is None:
            stale_markets.append(market)
            parser_unverified_markets.append(market)
            data_unusable_markets.append(market)
            continue
        age_sec = _snapshot_age_sec(item.get("snapshot_at"))
        item["data_age_sec"] = age_sec
        item["stale"] = bool(
            age_sec is None
            or age_sec > resolved_settings.market_context_snapshot_stale_sec
        )
        if item["stale"]:
            stale_markets.append(market)
        if str(item.get("parser_confidence_status") or "") != "VERIFIED":
            parser_unverified_markets.append(market)
        if not bool(item.get("trading_data_usable")):
            data_unusable_markets.append(market)
    candidate_refs = connection.execute(
        """
        SELECT
            COUNT(*) AS context_count,
            SUM(CASE WHEN candidate.market_context_snapshot_id IS NOT NULL THEN 1 ELSE 0 END)
                AS referenced_count,
            SUM(CASE WHEN candidate.market_context_snapshot_id IS NULL THEN 1 ELSE 0 END)
                AS unreferenced_count,
            SUM(CASE
                WHEN candidate.market_context_snapshot_id IS NOT NULL
                    AND snapshot.snapshot_id IS NULL
                THEN 1 ELSE 0 END)
                AS missing_snapshot_count
        FROM candidate_context_latest AS candidate
        LEFT JOIN market_context_snapshots AS snapshot
          ON snapshot.snapshot_id = candidate.market_context_snapshot_id
        """
    ).fetchone()
    return {
        "status": (
            "PASS"
            if len(watermark_hashes) == 1
            and latest_regime_coherent
            and not regime_reference_missing_count
            and all(item is not None for item in latest.values())
            and not stale_markets
            and not parser_unverified_markets
            and not data_unusable_markets
            and not int(candidate_refs["unreferenced_count"] or 0)
            and not int(candidate_refs["missing_snapshot_count"] or 0)
            else "WARN"
        ),
        "snapshot_count": _count_rows(connection, "market_context_snapshots"),
        "latest": latest,
        "latest_watermark_coherent": len(watermark_hashes) == 1,
        "latest_regime_coherent": latest_regime_coherent,
        "regime_reference_missing_count": regime_reference_missing_count,
        "stale_markets": stale_markets,
        "parser_unverified_markets": parser_unverified_markets,
        "data_unusable_markets": data_unusable_markets,
        "candidate_context_count": int(candidate_refs["context_count"] or 0),
        "candidate_reference_count": int(candidate_refs["referenced_count"] or 0),
        "candidate_unreferenced_count": int(
            candidate_refs["unreferenced_count"] or 0
        ),
        "candidate_missing_snapshot_count": int(
            candidate_refs["missing_snapshot_count"] or 0
        ),
        "stale_sec": resolved_settings.market_context_snapshot_stale_sec,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _market_index_watermark(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    watermark: dict[str, Any] = {}
    for market in MARKET_CONTEXT_MARKETS:
        latest = get_latest_market_index_tick(connection, market)
        readiness = get_market_index_readiness(connection, market, settings=settings)
        event_rowid = None
        if latest is not None and latest.get("event_id"):
            row = connection.execute(
                "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
                (latest["event_id"],),
            ).fetchone()
            event_rowid = None if row is None else int(row["event_rowid"])
        watermark[market] = {
            "event_id": None if latest is None else latest.get("event_id"),
            "event_rowid": event_rowid,
            "event_ts": None if latest is None else latest.get("event_ts"),
            "received_at": None if latest is None else latest.get("received_at"),
            "parser_status": None if latest is None else latest.get("parser_status"),
            "parser_verified": bool(readiness.get("parser_verified")),
            "data_source": None if latest is None else latest.get("data_source"),
            "data_usable": bool(readiness.get("data_usable")),
            "quality_status": readiness.get("quality_status"),
            "tick_age_sec": readiness.get("tick_age_sec"),
            "reason_codes": readiness.get("reason_codes") or [],
        }
    return watermark


def _watermark_identity(watermark: Mapping[str, Any]) -> dict[str, Any]:
    stable_keys = (
        "event_id",
        "event_rowid",
        "event_ts",
        "received_at",
        "parser_status",
        "data_source",
    )
    return {
        market: {
            key: _mapping(watermark.get(market)).get(key)
            for key in stable_keys
        }
        for market in MARKET_CONTEXT_MARKETS
    }


def _parser_confidence_status(
    watermark: Mapping[str, Any],
    *,
    primary_market: str,
) -> str:
    secondary_market = "KOSDAQ" if primary_market == "KOSPI" else "KOSPI"
    statuses = [
        str(_mapping(watermark.get(market)).get("parser_status") or "MISSING").upper()
        for market in (primary_market, secondary_market)
    ]
    if all(status == "VERIFIED" for status in statuses):
        return "VERIFIED"
    if all(status == "MISSING" for status in statuses):
        return "MISSING"
    if "VERIFIED" in statuses:
        return "MIXED"
    return "UNVERIFIED"


def _watermark_trade_date(watermark: Mapping[str, Any]) -> str:
    timestamps = []
    for value in watermark.values():
        event_ts = _mapping(value).get("event_ts")
        if event_ts in (None, ""):
            continue
        try:
            timestamps.append(parse_timestamp(str(event_ts), "event_ts"))
        except ValueError:
            continue
    if not timestamps:
        return market_today()
    return max(timestamps).astimezone(MARKET_TIMEZONE).date().isoformat()


def _get_market_context_by_watermark(
    connection: sqlite3.Connection,
    *,
    market: str,
    source_watermark_hash: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT * FROM market_context_snapshots
        WHERE market = ? AND source_watermark_hash = ?
        LIMIT 1
        """,
        (_normalize_market(market), source_watermark_hash),
    ).fetchone()
    return None if row is None else _snapshot_row_to_dict(row)


def _upsert_market_context_latest(
    connection: sqlite3.Connection,
    snapshot: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO market_context_latest (
            market, snapshot_id, trade_date, source_watermark_hash,
            snapshot_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(market) DO UPDATE SET
            snapshot_id = excluded.snapshot_id,
            trade_date = excluded.trade_date,
            source_watermark_hash = excluded.source_watermark_hash,
            snapshot_at = excluded.snapshot_at,
            updated_at = excluded.updated_at
        WHERE excluded.snapshot_at >= market_context_latest.snapshot_at
        """,
        (
            snapshot["market"],
            snapshot["snapshot_id"],
            snapshot["trade_date"],
            snapshot["source_watermark_hash"],
            snapshot["snapshot_at"],
            datetime_to_wire(utc_now()),
        ),
    )


def _linked_regime_snapshot(
    connection: sqlite3.Connection,
    source_event_id: str | None,
) -> dict[str, Any] | None:
    if not source_event_id:
        return None
    row = connection.execute(
        """
        SELECT * FROM market_regime_snapshots
        WHERE source_event_id = ?
        ORDER BY snapshot_at DESC, created_at DESC
        LIMIT 1
        """,
        (source_event_id,),
    ).fetchone()
    return None if row is None else _regime_row_to_dict(row)


def _latest_global_regime_snapshot(
    connection: sqlite3.Connection,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT * FROM market_regime_snapshots
        WHERE target_code = '__MARKET__'
        ORDER BY snapshot_at DESC, created_at DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else _regime_row_to_dict(row)


def _missing_context(
    *,
    code: str,
    market: str | None,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "snapshot_id": None,
        "trade_date": None,
        "code": code,
        "market": market,
        "source_watermark_hash": None,
        "source_watermark": {},
        "source_regime_snapshot_id": None,
        "parser_confidence_status": "MISSING",
        "data_quality_status": "MISSING",
        "trading_data_usable": False,
        "trading_eligible": False,
        "data_age_sec": None,
        "generated_by": None,
        "snapshot_at": None,
        "market_regime": {
            "target_code": code,
            "market": market,
            "primary_index_code": "UNKNOWN" if market is None else market,
            "secondary_index_code": None,
            "regime_status": "DATA_WAIT",
            "quality_status": "MISSING",
            "reason_codes": [reason_code],
            "evidence_json": {"observe_only": True},
        },
        "reason_codes": [reason_code],
        "read_only": True,
    }


def _snapshot_age_sec(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        snapshot_at = parse_timestamp(str(value), "snapshot_at")
    except ValueError:
        return None
    return max((utc_now() - snapshot_at).total_seconds(), 0.0)


def _snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["source_watermark"] = json.loads(data.pop("source_watermark_json"))
    data["market_regime"] = json.loads(data.pop("market_regime_json"))
    data["evidence"] = json.loads(data.pop("evidence_json"))
    data["trading_data_usable"] = bool(data["trading_data_usable"])
    return data


def _regime_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["evidence_json"] = json.loads(data.pop("evidence_json"))
    return data


def _normalize_market(value: object) -> str:
    market = str(value or "").strip().upper()
    if market not in MARKET_CONTEXT_MARKETS:
        raise ValueError("market must be KOSPI or KOSDAQ")
    return market


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
