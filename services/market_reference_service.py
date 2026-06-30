from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now, validate_stock_code
from storage.gateway_command_store import canonical_json

MARKET_SYMBOL_EVENT_TYPES: frozenset[str] = frozenset({"market_symbols"})
SUPPORTED_MARKETS: frozenset[str] = frozenset({"KOSPI", "KOSDAQ"})


@dataclass(frozen=True, kw_only=True)
class MarketReferenceProcessResult:
    event_id: str
    event_type: str
    status: str
    applied_count: int = 0
    ignored_count: int = 0
    error_count: int = 0
    error_message: str | None = None


def process_market_symbols_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> MarketReferenceProcessResult:
    event_type = event.event_type.strip().lower()
    if event_type not in MARKET_SYMBOL_EVENT_TYPES:
        return MarketReferenceProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )
    if _projection_exists(connection, event.event_id):
        return MarketReferenceProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="DUPLICATE",
            ignored_count=1,
        )

    try:
        symbols = _extract_memberships(event.payload)
        if not symbols:
            return MarketReferenceProcessResult(
                event_id=event.event_id,
                event_type=event_type,
                status="IGNORED",
                ignored_count=1,
            )
        event_ts, received_at = _event_store_times(connection, event)
        now = datetime_to_wire(utc_now())
        connection.execute("BEGIN IMMEDIATE")
        for symbol in symbols:
            connection.execute(
                """
                INSERT INTO market_symbol_memberships (
                    code,
                    market,
                    name,
                    event_id,
                    event_ts,
                    received_at,
                    source,
                    metadata_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    market = excluded.market,
                    name = excluded.name,
                    event_id = excluded.event_id,
                    event_ts = excluded.event_ts,
                    received_at = excluded.received_at,
                    source = excluded.source,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    symbol["code"],
                    symbol["market"],
                    symbol.get("name"),
                    event.event_id,
                    event_ts,
                    received_at,
                    event.source,
                    canonical_json(symbol.get("metadata", {})),
                    now,
                ),
            )
        connection.commit()
    except Exception as exc:
        connection.rollback()
        return MarketReferenceProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=1,
            error_message=str(exc),
        )

    return MarketReferenceProcessResult(
        event_id=event.event_id,
        event_type=event_type,
        status="APPLIED",
        applied_count=len(symbols),
    )


def get_market_for_code(connection: sqlite3.Connection, code: str) -> dict[str, Any] | None:
    normalized_code = validate_stock_code(code)
    row = connection.execute(
        """
        SELECT *
        FROM market_symbol_memberships
        WHERE code = ?
        """,
        (normalized_code,),
    ).fetchone()
    return None if row is None else _membership_row_to_dict(row)


def list_market_symbol_memberships(
    connection: sqlite3.Connection,
    *,
    market: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if market is not None:
        clauses.append("market = ?")
        params.append(_normalize_market(market))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM market_symbol_memberships
        {where_sql}
        ORDER BY market ASC, code ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_membership_row_to_dict(row) for row in rows]


def _extract_memberships(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    markets_payload = payload.get("markets")
    if isinstance(markets_payload, Mapping):
        for market, symbols in markets_payload.items():
            items.extend(_symbols_for_market(market, symbols))
    for market in SUPPORTED_MARKETS:
        if market in payload:
            items.extend(_symbols_for_market(market, payload[market]))
    symbols_payload = payload.get("symbols")
    if isinstance(symbols_payload, Sequence) and not isinstance(symbols_payload, (str, bytes)):
        for item in symbols_payload:
            if not isinstance(item, Mapping):
                continue
            market = item.get("market") or item.get("market_code")
            if market is None:
                continue
            items.append(_normalize_symbol(market, item))

    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        deduped[item["code"]] = item
    return list(deduped.values())


def _symbols_for_market(market: object, symbols: object) -> list[dict[str, Any]]:
    normalized_market = _normalize_market(market)
    if isinstance(symbols, Mapping):
        iterable: Sequence[Any] = [
            {"code": code, **(value if isinstance(value, Mapping) else {"name": value})}
            for code, value in symbols.items()
        ]
    elif isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes)):
        iterable = symbols
    else:
        return []
    result = []
    for item in iterable:
        result.append(_normalize_symbol(normalized_market, item))
    return result


def _normalize_symbol(market: object, item: object) -> dict[str, Any]:
    normalized_market = _normalize_market(market)
    if isinstance(item, str):
        code = item
        name = None
        metadata: Mapping[str, Any] = {}
    elif isinstance(item, Mapping):
        code = item.get("code") or item.get("stock_code") or item.get("종목코드")
        name_value = item.get("name") or item.get("stock_name") or item.get("종목명")
        name = None if name_value is None else str(name_value).strip() or None
        metadata_value = item.get("metadata")
        metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    else:
        raise ValueError("market symbol item must be a code string or object")
    return {
        "code": validate_stock_code(code),
        "market": normalized_market,
        "name": name,
        "metadata": dict(metadata),
    }


def _normalize_market(value: object) -> str:
    market = str(value).strip().upper()
    if market not in SUPPORTED_MARKETS:
        allowed = ", ".join(sorted(SUPPORTED_MARKETS))
        raise ValueError(f"market must be one of: {allowed}")
    return market


def _projection_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM market_symbol_memberships
        WHERE event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return row is not None


def _event_store_times(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> tuple[str, str]:
    row = connection.execute(
        """
        SELECT event_ts, received_at
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    if row is not None:
        return row["event_ts"], row["received_at"]
    return datetime_to_wire(event.ts), datetime_to_wire(utc_now())


def _membership_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 1000)
