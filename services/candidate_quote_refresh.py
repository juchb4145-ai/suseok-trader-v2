from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.utils import (
    datetime_to_wire,
    normalize_value,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from storage.gateway_command_store import EnqueueCommandResult, enqueue_command

from services.config import Settings, candidate_timezone, load_settings

CANDIDATE_QUOTE_REFRESH_SOURCE = "candidate_quote_refresh"
CANDIDATE_QUOTE_REFRESH_REQUEST_PREFIX = f"{CANDIDATE_QUOTE_REFRESH_SOURCE}:"
CANDIDATE_QUOTE_REFRESH_TR_CODE = "OPT10001"
CANDIDATE_QUOTE_REFRESH_REQUEST_NAME = "candidate_quote_refresh_opt10001"
CANDIDATE_QUOTE_REFRESH_SCREEN_NO = "8701"
CANDIDATE_QUOTE_REFRESH_TTL_SEC = 120
CANDIDATE_QUOTE_REFRESH_MAX_COMMANDS_PER_RUN = 5
CANDIDATE_QUOTE_REFRESH_FIELDS: tuple[str, ...] = (
    "종목코드",
    "종목명",
    "현재가",
    "등락율",
    "등락률",
    "거래량",
    "거래대금",
    "시가",
    "고가",
    "저가",
)
_REFRESH_STATES = (
    CandidateState.CONTEXT_READY.value,
    CandidateState.WATCHING.value,
    CandidateState.DATA_WAIT.value,
    CandidateState.HYDRATING.value,
)
_STATE_PRIORITY = {
    CandidateState.CONTEXT_READY.value: 0,
    CandidateState.WATCHING.value: 1,
    CandidateState.DATA_WAIT.value: 2,
    CandidateState.HYDRATING.value: 3,
}


@dataclass(frozen=True, kw_only=True)
class CandidateQuoteRefreshResult:
    status: str
    trade_date: str
    candidate_count: int = 0
    stale_candidate_count: int = 0
    command_count: int = 0
    command_results: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    candidates: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    queue_commands: bool = True
    observe_only: bool = True
    no_order_side_effects: bool = True
    live_real_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "trade_date": self.trade_date,
            "candidate_count": self.candidate_count,
            "stale_candidate_count": self.stale_candidate_count,
            "command_count": self.command_count,
            "command_results": normalize_value(list(self.command_results)),
            "candidates": normalize_value(list(self.candidates)),
            "queue_commands": self.queue_commands,
            "observe_only": True,
            "not_order_intent": True,
            "no_order_side_effects": True,
            "live_real_allowed": False,
            "real_order_allowed": False,
        }


def run_candidate_quote_refresh_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    settings: Settings | None = None,
    queue_commands: bool = True,
    limit: int | None = None,
) -> CandidateQuoteRefreshResult:
    resolved_settings = settings or load_settings()
    resolved_trade_date = trade_date or _trade_date(resolved_settings)
    rows = _candidate_rows_needing_refresh(
        connection,
        trade_date=resolved_trade_date,
        settings=resolved_settings,
    )
    bounded_limit = _bounded_limit(limit or CANDIDATE_QUOTE_REFRESH_MAX_COMMANDS_PER_RUN)
    selected = rows[:bounded_limit]
    if not selected:
        return CandidateQuoteRefreshResult(
            status="NOOP",
            trade_date=resolved_trade_date,
            candidate_count=0,
            stale_candidate_count=0,
            queue_commands=bool(queue_commands),
        )
    if not queue_commands:
        return CandidateQuoteRefreshResult(
            status="PLAN_ONLY",
            trade_date=resolved_trade_date,
            candidate_count=len(selected),
            stale_candidate_count=len(rows),
            candidates=tuple(selected),
            queue_commands=False,
        )

    command_results: list[dict[str, Any]] = []
    command_count = 0
    for item in selected:
        command = _build_quote_refresh_command(item, trade_date=resolved_trade_date)
        result = enqueue_command(
            connection,
            command,
            expires_at=utc_now() + timedelta(seconds=CANDIDATE_QUOTE_REFRESH_TTL_SEC),
        )
        command_results.append(_command_result_dict(result, item=item))
        if result.accepted:
            command_count += 1

    return CandidateQuoteRefreshResult(
        status="QUEUED" if command_count else "NOOP",
        trade_date=resolved_trade_date,
        candidate_count=len(selected),
        stale_candidate_count=len(rows),
        command_count=command_count,
        command_results=tuple(command_results),
        candidates=tuple(selected),
        queue_commands=True,
    )


def is_candidate_quote_refresh_payload(payload: Mapping[str, Any]) -> bool:
    request_id = str(payload.get("request_id") or "")
    if request_id.startswith(CANDIDATE_QUOTE_REFRESH_REQUEST_PREFIX):
        return True
    metadata = payload.get("metadata")
    return (
        isinstance(metadata, Mapping)
        and metadata.get("source") == CANDIDATE_QUOTE_REFRESH_SOURCE
    )


def candidate_quote_refresh_codes_from_payload(payload: Mapping[str, Any]) -> list[str]:
    if not is_candidate_quote_refresh_payload(payload):
        return []
    codes: list[str] = []
    for row in _rows_from_payload(payload):
        code = _row_code(row)
        if code and code not in codes:
            codes.append(code)
    return codes


def candidate_quote_refresh_tick_payloads_from_tr_response(
    payload: Mapping[str, Any],
    *,
    event_ts: str | datetime,
) -> list[dict[str, Any]]:
    if not is_candidate_quote_refresh_payload(payload):
        return []
    ticks: list[dict[str, Any]] = []
    for row in _rows_from_payload(payload):
        tick = _tick_payload_from_row(row, event_ts=event_ts)
        if tick is not None:
            ticks.append(tick)
    return ticks


def _candidate_rows_needing_refresh(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    settings: Settings,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in _REFRESH_STATES)
    rows = connection.execute(
        f"""
        SELECT
            c.candidate_instance_id,
            c.trade_date,
            c.code,
            c.name,
            c.state,
            c.last_seen_at,
            mt.event_ts AS tick_event_ts,
            mt.updated_at AS tick_updated_at,
            mt.quality_status
        FROM candidates AS c
        LEFT JOIN market_ticks_latest AS mt
            ON mt.code = c.code AND mt.exchange = 'KRX'
        WHERE c.trade_date = ?
            AND c.state IN ({placeholders})
        ORDER BY
            CASE c.state
                WHEN 'CONTEXT_READY' THEN 0
                WHEN 'WATCHING' THEN 1
                WHEN 'DATA_WAIT' THEN 2
                WHEN 'HYDRATING' THEN 3
                ELSE 9
            END ASC,
            c.last_seen_at DESC,
            c.candidate_instance_id ASC
        """,
        (trade_date, *_REFRESH_STATES),
    ).fetchall()
    threshold_sec = max(int(settings.entry_timing_stale_max_seconds), 1)
    now = utc_now()
    result: list[dict[str, Any]] = []
    for row in rows:
        age_sec = _age_seconds(row["tick_event_ts"], now=now)
        missing = row["tick_event_ts"] is None
        stale = missing or age_sec is None or age_sec > threshold_sec
        if not stale:
            continue
        reason = "TICK_MISSING" if missing else "TICK_STALE"
        result.append(
            {
                "candidate_instance_id": row["candidate_instance_id"],
                "trade_date": row["trade_date"],
                "code": validate_stock_code(row["code"]),
                "name": str(row["name"] or row["code"]),
                "state": str(row["state"]),
                "state_priority": _STATE_PRIORITY.get(str(row["state"]), 9),
                "last_seen_at": row["last_seen_at"],
                "tick_event_ts": row["tick_event_ts"],
                "tick_updated_at": row["tick_updated_at"],
                "tick_age_sec": age_sec,
                "quality_status": row["quality_status"] or "MISSING",
                "reason_codes": [reason, "CANDIDATE_QUOTE_REFRESH"],
            }
        )
    return result


def _build_quote_refresh_command(
    item: Mapping[str, Any],
    *,
    trade_date: str,
) -> GatewayCommand:
    code = validate_stock_code(item["code"])
    bucket = int(utc_now().timestamp() // 60)
    payload = {
        "request_id": f"{CANDIDATE_QUOTE_REFRESH_REQUEST_PREFIX}{trade_date}:{code}:{bucket}",
        "tr_code": CANDIDATE_QUOTE_REFRESH_TR_CODE,
        "request_name": CANDIDATE_QUOTE_REFRESH_REQUEST_NAME,
        "screen_no": CANDIDATE_QUOTE_REFRESH_SCREEN_NO,
        "fields": list(CANDIDATE_QUOTE_REFRESH_FIELDS),
        "params": {
            "종목코드": code,
            "code": code,
        },
        "metadata": {
            "source": CANDIDATE_QUOTE_REFRESH_SOURCE,
            "trade_date": trade_date,
            "candidate_instance_id": item.get("candidate_instance_id"),
            "candidate_state": item.get("state"),
            "observe_only": True,
            "not_order_signal": True,
            "no_order_side_effects": True,
            "reason_codes": list(item.get("reason_codes") or []),
        },
        "observe_only": True,
        "not_order_signal": True,
        "no_order_side_effects": True,
    }
    return GatewayCommand(
        command_type="request_tr",
        source=CANDIDATE_QUOTE_REFRESH_SOURCE,
        payload=payload,
        idempotency_key=f"{CANDIDATE_QUOTE_REFRESH_SOURCE}:{trade_date}:{code}:{bucket}",
    )


def _command_result_dict(
    result: EnqueueCommandResult,
    *,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "code": item.get("code"),
        "candidate_instance_id": item.get("candidate_instance_id"),
        "accepted": result.accepted,
        "command_id": result.command_id,
        "status": result.status.value,
        "duplicate": result.duplicate,
        "error_message": result.error_message,
        "payload_hash": result.payload_hash,
    }


def _rows_from_payload(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, Mapping)]
    row = payload.get("row")
    return [row] if isinstance(row, Mapping) else []


def _tick_payload_from_row(
    row: Mapping[str, Any],
    *,
    event_ts: str | datetime,
) -> dict[str, Any] | None:
    code = _row_code(row)
    if not code:
        return None
    price = _row_int(row, "현재가", "price", "current_price")
    if price <= 0:
        return None
    name = _row_text(row, "종목명", "name", "stock_name") or code
    volume = _row_int(row, "거래량", "volume", "accumulated_volume")
    trade_value = _row_int(row, "거래대금", "trade_value", "accumulated_trade_value")
    day_high = _row_int(row, "고가", "high", "day_high") or price
    day_low = _row_int(row, "저가", "low", "day_low") or price
    return {
        "code": code,
        "name": name,
        "price": price,
        "change_rate": _row_float(row, "등락율", "등락률", "change_rate"),
        "volume": max(volume, 0),
        "trade_value": max(trade_value, 0),
        "execution_strength": _row_float(row, "체결강도", "execution_strength") or 100.0,
        "best_bid": _row_int(row, "매수호가", "best_bid") or price,
        "best_ask": _row_int(row, "매도호가", "best_ask") or price,
        "spread_ticks": _row_int(row, "spread_ticks") or 0,
        "day_high": max(day_high, day_low, price),
        "day_low": min(day_high, day_low, price),
        "trade_time": datetime_to_wire(parse_timestamp(event_ts, "event_ts")),
        "ts": datetime_to_wire(parse_timestamp(event_ts, "event_ts")),
        "metadata": {
            "source": CANDIDATE_QUOTE_REFRESH_SOURCE,
            "snapshot_only": True,
            "reason_codes": ["CANDIDATE_QUOTE_REFRESH_TR_RESPONSE"],
        },
    }


def _row_code(row: Mapping[str, Any]) -> str | None:
    raw = _row_text(row, "종목코드", "code", "stock_code")
    if not raw:
        return None
    normalized = raw.strip()
    if normalized.startswith("A") and len(normalized) >= 7:
        normalized = normalized[1:]
    try:
        return validate_stock_code(normalized)
    except Exception:
        return None


def _row_text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _row_int(row: Mapping[str, Any], *keys: str) -> int:
    text = _row_text(row, *keys)
    if not text:
        return 0
    normalized = (
        text.replace(",", "")
        .replace("%", "")
        .replace("+", "")
        .strip()
    )
    try:
        return abs(int(float(normalized)))
    except ValueError:
        return 0


def _row_float(row: Mapping[str, Any], *keys: str) -> float:
    text = _row_text(row, *keys)
    if not text:
        return 0.0
    normalized = (
        text.replace(",", "")
        .replace("%", "")
        .replace("+", "")
        .strip()
    )
    try:
        return float(normalized)
    except ValueError:
        return 0.0


def _age_seconds(value: object, *, now: datetime) -> float | None:
    if value is None:
        return None
    try:
        parsed = parse_timestamp(value, "tick_event_ts")
    except (TypeError, ValueError):
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _trade_date(settings: Settings) -> str:
    market_tz = candidate_timezone(settings.candidate_trade_date_timezone)
    return utc_now().astimezone(market_tz).date().isoformat()


def _bounded_limit(value: int) -> int:
    return max(min(int(value), 50), 1)
