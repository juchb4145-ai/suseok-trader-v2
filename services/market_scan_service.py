from __future__ import annotations

import json
import math
import re
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_payload,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from storage.gateway_command_store import EnqueueCommandResult, canonical_json, enqueue_command

from services.config import Settings, load_settings

MARKET_SCAN_SOURCE = "market_scan_service"
SCAN_TYPE_TRADE_VALUE = "TRADE_VALUE"
SCAN_TYPE_CHANGE_RATE = "CHANGE_RATE"
SUPPORTED_SCAN_TYPES = (SCAN_TYPE_TRADE_VALUE, SCAN_TYPE_CHANGE_RATE)
SCAN_EVENT_TYPES = frozenset({"tr_response"})

QueueCommand = Callable[[GatewayCommand], Any]


@dataclass(frozen=True, kw_only=True)
class MarketScanCommandPlan:
    scan_type: str
    market: str
    tr_code: str
    request_id: str
    command_id: str
    queued: bool
    accepted: bool | None = None
    status: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_type": self.scan_type,
            "market": self.market,
            "tr_code": self.tr_code,
            "request_id": self.request_id,
            "command_id": self.command_id,
            "queued": self.queued,
            "accepted": self.accepted,
            "status": self.status,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, kw_only=True)
class MarketScanRunResult:
    status: str
    run_id: str
    planned_count: int
    command_count: int
    command_results: Sequence[MarketScanCommandPlan] = field(default_factory=tuple)
    observe_only: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "planned_count": self.planned_count,
            "command_count": self.command_count,
            "command_results": [item.to_dict() for item in self.command_results],
            "observe_only": self.observe_only,
            "no_order_side_effects": self.no_order_side_effects,
        }


@dataclass(frozen=True, kw_only=True)
class MarketScanProcessResult:
    event_id: str
    event_type: str
    status: str
    applied_count: int = 0
    ignored_count: int = 0
    error_count: int = 0
    scan_type: str | None = None
    market: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "status": self.status,
            "applied_count": self.applied_count,
            "ignored_count": self.ignored_count,
            "error_count": self.error_count,
            "scan_type": self.scan_type,
            "market": self.market,
            "error_message": self.error_message,
        }


def run_market_scan_once(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    queue_commands: bool | QueueCommand = True,
) -> MarketScanRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("market_scan_run")
    if not resolved_settings.market_scan_enabled:
        return MarketScanRunResult(
            status="DISABLED",
            run_id=run_id,
            planned_count=0,
            command_count=0,
        )

    plans: list[MarketScanCommandPlan] = []
    command_count = 0
    for market in resolved_settings.market_scan_markets:
        for scan_type in SUPPORTED_SCAN_TYPES:
            command = _build_scan_command(
                run_id=run_id,
                scan_type=scan_type,
                market=market,
                settings=resolved_settings,
            )
            if not queue_commands:
                plans.append(
                    MarketScanCommandPlan(
                        scan_type=scan_type,
                        market=market,
                        tr_code=str(command.payload["tr_code"]),
                        request_id=str(command.payload["request_id"]),
                        command_id=command.command_id,
                        queued=False,
                    )
                )
                continue

            enqueue_result = _queue_command(connection, command, queue_commands)
            accepted = _result_accepted(enqueue_result)
            command_count += int(accepted)
            plans.append(
                MarketScanCommandPlan(
                    scan_type=scan_type,
                    market=market,
                    tr_code=str(command.payload["tr_code"]),
                    request_id=str(command.payload["request_id"]),
                    command_id=command.command_id,
                    queued=True,
                    accepted=accepted,
                    status=_result_status(enqueue_result),
                    error_message=_result_error(enqueue_result),
                )
            )

    status = "QUEUED" if command_count else "PLAN_ONLY"
    return MarketScanRunResult(
        status=status,
        run_id=run_id,
        planned_count=len(plans),
        command_count=command_count,
        command_results=tuple(plans),
    )


def process_market_scan_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> MarketScanProcessResult:
    resolved_settings = settings or load_settings()
    event_type = event.event_type.strip().lower()
    if event_type not in SCAN_EVENT_TYPES:
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )

    try:
        response = BrokerTrResponse.from_dict(event.payload)
    except Exception as exc:
        _record_scan_error(
            connection,
            event=event,
            reason_code="TR_RESPONSE_PARSE_FAILED",
            error_message=str(exc),
            payload=event.payload,
        )
        connection.commit()
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=1,
            error_message=str(exc),
        )

    context = _scan_context(response, settings=resolved_settings)
    if context is None:
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )
    scan_type, market = context

    if _event_already_projected(connection, event.event_id, response.request_id):
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="DUPLICATE",
            ignored_count=1,
            scan_type=scan_type,
            market=market,
        )

    if not response.success:
        _record_scan_error(
            connection,
            event=event,
            response=response,
            scan_type=scan_type,
            market=market,
            reason_code="TR_RESPONSE_FAILED",
            error_message=response.message or "TR_RESPONSE_FAILED",
            payload=event.payload,
        )
        connection.commit()
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=1,
            scan_type=scan_type,
            market=market,
            error_message=response.message or "TR_RESPONSE_FAILED",
        )

    scanned_at, _received_at = _event_store_times(connection, event)
    scan_id = _scan_id(response, scan_type=scan_type, market=market, scanned_at=scanned_at)
    applied_count = 0
    error_count = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        for fallback_rank, row in enumerate(response.rows, start=1):
            try:
                item = _parse_scan_row(
                    row,
                    scan_id=scan_id,
                    scan_type=scan_type,
                    market=market,
                    fallback_rank=fallback_rank,
                    scanned_at=scanned_at,
                    source=event.source,
                    response=response,
                    settings=resolved_settings,
                )
            except _RowParseError as exc:
                error_count += 1
                _record_scan_error(
                    connection,
                    event=event,
                    response=response,
                    scan_type=scan_type,
                    market=market,
                    reason_code=exc.reason_code,
                    error_message=str(exc),
                    payload=row,
                )
                continue
            _insert_scan_snapshot(connection, item)
            _upsert_scan_latest(connection, item)
            applied_count += 1
        connection.commit()
    except Exception as exc:
        connection.rollback()
        _record_scan_error(
            connection,
            event=event,
            response=response,
            scan_type=scan_type,
            market=market,
            reason_code="MARKET_SCAN_PROJECTION_FAILED",
            error_message=str(exc),
            payload=event.payload,
        )
        connection.commit()
        return MarketScanProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=error_count + 1,
            scan_type=scan_type,
            market=market,
            error_message=str(exc),
        )

    status = "APPLIED"
    if applied_count == 0 and error_count:
        status = "ERROR"
    elif error_count:
        status = "PARTIAL"
    return MarketScanProcessResult(
        event_id=event.event_id,
        event_type=event_type,
        status=status,
        applied_count=applied_count,
        error_count=error_count,
        scan_type=scan_type,
        market=market,
    )


def get_latest_market_scan(
    connection: sqlite3.Connection,
    code: str,
) -> dict[str, Any] | None:
    normalized_code = validate_stock_code(code)
    row = connection.execute(
        """
        SELECT *
        FROM market_scan_latest
        WHERE code = ?
        """,
        (normalized_code,),
    ).fetchone()
    return None if row is None else _scan_row_to_dict(row)


def list_latest_market_scan(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_scan_latest
        ORDER BY scanned_at DESC, rank ASC, code ASC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_scan_row_to_dict(row) for row in rows]


def list_market_scan_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_scan_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_error_row_to_dict(row) for row in rows]


def get_market_scan_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest_row = connection.execute(
        """
        SELECT scanned_at
        FROM market_scan_latest
        ORDER BY scanned_at DESC
        LIMIT 1
        """
    ).fetchone()
    latest_scanned_at = None if latest_row is None else str(latest_row["scanned_at"])
    return {
        "enabled": resolved_settings.market_scan_enabled,
        "interval_sec": resolved_settings.market_scan_interval_sec,
        "top_n": resolved_settings.market_scan_top_n,
        "tr_codes": dict(resolved_settings.market_scan_tr_codes),
        "markets": list(resolved_settings.market_scan_markets),
        "latest_count": _count_rows(connection, "market_scan_latest"),
        "snapshot_count": _count_rows(connection, "market_scan_snapshots"),
        "error_count": _count_rows(connection, "market_scan_errors"),
        "latest_scanned_at": latest_scanned_at,
        "latest_age_sec": _age_seconds(latest_scanned_at),
        "observe_only": True,
        "no_order_side_effects": True,
    }


def _build_scan_command(
    *,
    run_id: str,
    scan_type: str,
    market: str,
    settings: Settings,
) -> GatewayCommand:
    normalized_scan_type = require_non_empty_str(scan_type, "scan_type").upper()
    normalized_market = require_non_empty_str(market, "market").upper()
    tr_code = settings.market_scan_tr_codes[normalized_scan_type]
    request_id = f"market_scan:{normalized_scan_type}:{normalized_market}:{run_id}"
    payload = {
        "request_id": request_id,
        "tr_code": tr_code,
        "request_name": f"market_scan_{normalized_scan_type.lower()}_{normalized_market.lower()}",
        "screen_no": settings.market_scan_screen_no,
        "params": {
            "시장구분": settings.market_scan_market_codes[normalized_market],
            "market": normalized_market,
            "scan_type": normalized_scan_type,
            "top_n": settings.market_scan_top_n,
        },
        "metadata": {
            "observe_only": True,
            "no_order_side_effects": True,
            "scan_type": normalized_scan_type,
            "market": normalized_market,
            "parser_status": settings.market_scan_parser_status,
            "source": MARKET_SCAN_SOURCE,
        },
    }
    return GatewayCommand(
        command_type="request_tr",
        source=MARKET_SCAN_SOURCE,
        payload=payload,
        idempotency_key=f"{MARKET_SCAN_SOURCE}:{normalized_scan_type}:{normalized_market}:{run_id}",
    )


def _queue_command(
    connection: sqlite3.Connection,
    command: GatewayCommand,
    queue_commands: bool | QueueCommand,
) -> Any:
    if callable(queue_commands):
        return queue_commands(command)
    return enqueue_command(
        connection,
        command,
        expires_at=utc_now() + timedelta(seconds=120),
    )


def _result_accepted(result: Any) -> bool:
    if isinstance(result, EnqueueCommandResult):
        return result.accepted
    if isinstance(result, Mapping):
        return bool(result.get("accepted", True))
    return True


def _result_status(result: Any) -> str | None:
    if isinstance(result, EnqueueCommandResult):
        return result.status.value
    if isinstance(result, Mapping):
        value = result.get("status")
        return None if value is None else str(value)
    return "QUEUED"


def _result_error(result: Any) -> str | None:
    if isinstance(result, EnqueueCommandResult):
        return result.error_message
    if isinstance(result, Mapping):
        value = result.get("error_message")
        return None if value is None else str(value)
    return None


def _scan_context(
    response: BrokerTrResponse,
    *,
    settings: Settings,
) -> tuple[str, str] | None:
    tr_codes = {value.upper(): key for key, value in settings.market_scan_tr_codes.items()}
    scan_type = tr_codes.get(response.tr_code.upper())
    market: str | None = None

    request_parts = response.request_id.split(":")
    if len(request_parts) >= 4 and request_parts[0] == "market_scan":
        scan_type = request_parts[1].upper()
        market = request_parts[2].upper()

    name_parts = response.request_name.lower().split("_")
    if response.request_name.lower().startswith("market_scan_") and len(name_parts) >= 4:
        if scan_type is None:
            scan_type = "_".join(name_parts[2:-1]).upper()
        market = market or name_parts[-1].upper()

    first_row = response.rows[0] if response.rows else {}
    if market is None:
        value = first_row.get("market") or first_row.get("시장")
        market = str(value).strip().upper() if value else None
    if scan_type is None:
        value = first_row.get("scan_type") or first_row.get("스캔유형")
        scan_type = str(value).strip().upper() if value else None

    if scan_type not in SUPPORTED_SCAN_TYPES or market not in settings.market_scan_markets:
        return None
    return scan_type, market


class _RowParseError(ValueError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _parse_scan_row(
    row: Mapping[str, Any],
    *,
    scan_id: str,
    scan_type: str,
    market: str,
    fallback_rank: int,
    scanned_at: str,
    source: str,
    response: BrokerTrResponse,
    settings: Settings,
) -> dict[str, Any]:
    normalized = normalize_payload(row)
    try:
        code = validate_stock_code(_first_value(normalized, "code", "stock_code", "종목코드"))
    except Exception as exc:
        raise _RowParseError("MARKET_SCAN_CODE_PARSE_FAILED", str(exc)) from exc
    name = _string_or_default(
        _first_value(normalized, "name", "stock_name", "종목명", "종목명 ".strip()),
        code,
    )
    rank = _int_or_default(_first_value(normalized, "rank", "순위"), fallback_rank)
    price = _float_or_none(
        _first_value(normalized, "price", "current_price", "현재가"),
        absolute=True,
    )
    change_rate = _float_or_none(
        _first_value(normalized, "change_rate", "change_rate_pct", "등락률", "전일대비등락률")
    )
    trade_value = _float_or_none(
        _first_value(
            normalized,
            "trade_value",
            "turnover",
            "trade_value_krw",
            "거래대금",
            "누적거래대금",
        ),
        min_value=0.0,
    )
    volume = _int_or_none(
        _first_value(normalized, "volume", "cum_volume", "거래량"),
        min_value=0,
    )
    metadata = {
        "raw": normalized,
        "request_id": response.request_id,
        "tr_code": response.tr_code,
        "request_name": response.request_name,
        "parser_status": settings.market_scan_parser_status,
        "observe_only": True,
        "no_order_side_effects": True,
    }
    return {
        "scan_id": scan_id,
        "scan_type": scan_type,
        "market": market,
        "code": code,
        "name": name,
        "rank": rank,
        "price": price,
        "change_rate": change_rate,
        "trade_value": trade_value,
        "volume": volume,
        "scanned_at": scanned_at,
        "source": source,
        "metadata": metadata,
    }


def _insert_scan_snapshot(connection: sqlite3.Connection, item: Mapping[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO market_scan_snapshots (
            scan_id,
            scan_type,
            market,
            code,
            name,
            rank,
            price,
            change_rate,
            trade_value,
            volume,
            scanned_at,
            source,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scan_id, scan_type, market, code) DO UPDATE SET
            name = excluded.name,
            rank = excluded.rank,
            price = excluded.price,
            change_rate = excluded.change_rate,
            trade_value = excluded.trade_value,
            volume = excluded.volume,
            scanned_at = excluded.scanned_at,
            source = excluded.source,
            metadata_json = excluded.metadata_json
        """,
        (
            item["scan_id"],
            item["scan_type"],
            item["market"],
            item["code"],
            item["name"],
            item["rank"],
            item["price"],
            item["change_rate"],
            item["trade_value"],
            item["volume"],
            item["scanned_at"],
            item["source"],
            canonical_json(item["metadata"]),
        ),
    )


def _upsert_scan_latest(connection: sqlite3.Connection, item: Mapping[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO market_scan_latest (
            code,
            scan_id,
            scan_type,
            market,
            name,
            rank,
            price,
            change_rate,
            trade_value,
            volume,
            scanned_at,
            source,
            metadata_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            scan_id = excluded.scan_id,
            scan_type = excluded.scan_type,
            market = excluded.market,
            name = excluded.name,
            rank = excluded.rank,
            price = excluded.price,
            change_rate = excluded.change_rate,
            trade_value = excluded.trade_value,
            volume = excluded.volume,
            scanned_at = excluded.scanned_at,
            source = excluded.source,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            item["code"],
            item["scan_id"],
            item["scan_type"],
            item["market"],
            item["name"],
            item["rank"],
            item["price"],
            item["change_rate"],
            item["trade_value"],
            item["volume"],
            item["scanned_at"],
            item["source"],
            canonical_json(item["metadata"]),
            datetime_to_wire(utc_now()),
        ),
    )


def _record_scan_error(
    connection: sqlite3.Connection,
    *,
    event: GatewayEvent,
    reason_code: str,
    error_message: str,
    payload: Mapping[str, Any],
    response: BrokerTrResponse | None = None,
    scan_type: str | None = None,
    market: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO market_scan_errors (
            event_id,
            request_id,
            tr_code,
            scan_type,
            market,
            reason_code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            response.request_id if response is not None else None,
            response.tr_code if response is not None else None,
            scan_type,
            market,
            reason_code.upper(),
            error_message,
            canonical_json(payload),
        ),
    )


def _event_already_projected(
    connection: sqlite3.Connection,
    event_id: str,
    request_id: str,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM market_scan_snapshots
        WHERE json_extract(metadata_json, '$.request_id') = ?
        LIMIT 1
        """,
        (request_id,),
    ).fetchone()
    if row is not None:
        return True
    error = connection.execute(
        """
        SELECT 1
        FROM market_scan_errors
        WHERE event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return error is not None


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


def _scan_id(
    response: BrokerTrResponse,
    *,
    scan_type: str,
    market: str,
    scanned_at: str,
) -> str:
    base = f"{response.request_id}:{response.tr_code}:{scan_type}:{market}:{scanned_at}"
    return f"market_scan_{abs(hash(base)):x}"


def _first_value(row: Mapping[str, Any], *keys: str) -> Any:
    normalized_keys = {str(key).lower(): key for key in row.keys()}
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
        original_key = normalized_keys.get(key.lower())
        if original_key is not None and row[original_key] not in (None, ""):
            return row[original_key]
    return None


def _string_or_default(value: Any, default: str) -> str:
    text = str(value or "").strip()
    return text or default


def _int_or_default(value: Any, default: int) -> int:
    parsed = _int_or_none(value)
    return int(default) if parsed is None else parsed


def _int_or_none(value: Any, *, min_value: int | None = None) -> int | None:
    number = _float_or_none(value)
    if number is None or not math.isfinite(number):
        return None
    parsed = int(number)
    if min_value is not None:
        parsed = max(parsed, min_value)
    return parsed


def _float_or_none(
    value: Any,
    *,
    absolute: bool = False,
    min_value: float | None = None,
) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        parsed = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        text = text.replace(",", "").replace("%", "")
        text = re.sub(r"[^0-9+\-.]", "", text)
        if text in {"", "+", "-", ".", "+.", "-."}:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
    if absolute:
        parsed = abs(parsed)
    if min_value is not None:
        parsed = max(parsed, min_value)
    return parsed


def _scan_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["metadata"] = json.loads(data.pop("metadata_json"))
    data["age_sec"] = _age_seconds(data["scanned_at"])
    return data


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except Exception:
        return None


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"] if row else 0)


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
