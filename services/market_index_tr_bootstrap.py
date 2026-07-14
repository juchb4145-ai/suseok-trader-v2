from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import EnqueueCommandResult, enqueue_command

from services.config import Settings, TradingMode, candidate_timezone, load_settings

MARKET_INDEX_TR_BOOTSTRAP_SOURCE = "KIWOOM_TR_BOOTSTRAP_MARKET_INDEX"
MARKET_INDEX_TR_BOOTSTRAP_REQUEST_PREFIX = "market_index_tr_bootstrap:"
MARKET_INDEX_TR_BOOTSTRAP_REQUEST_NAME_PREFIX = "market_index_tr_bootstrap_"
MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION = "v1"
MARKET_INDEX_TR_BOOTSTRAP_FIELDS: tuple[str, ...] = (
    "현재가",
    "전일대비",
    "등락률",
)
MARKET_INDEX_TR_BOOTSTRAP_ROW_MODE = "single"
MARKET_INDEX_TR_BOOTSTRAP_OUTPUT_RECORD_NAME = "업종현재가"
_INDEX_NAMES = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ"}


@dataclass(frozen=True, kw_only=True)
class MarketIndexTrBootstrapInspection:
    recognized: bool
    valid: bool
    index_code: str | None = None
    parser_status: str = "UNKNOWN"
    response: BrokerTrResponse | None = None
    tick: BrokerMarketIndexTick | None = None
    reason_codes: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "recognized": self.recognized,
            "valid": self.valid,
            "index_code": self.index_code,
            "parser_status": self.parser_status,
            "reason_codes": list(self.reason_codes),
            "tick": None if self.tick is None else self.tick.to_dict(),
        }


@dataclass(frozen=True, kw_only=True)
class MarketIndexTrBootstrapCommandPlan:
    index_code: str
    request_id: str
    tr_code: str
    command_id: str
    queued: bool
    accepted: bool | None = None
    status: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index_code": self.index_code,
            "request_id": self.request_id,
            "tr_code": self.tr_code,
            "command_id": self.command_id,
            "queued": self.queued,
            "accepted": self.accepted,
            "status": self.status,
            "error_message": self.error_message,
        }


@dataclass(frozen=True, kw_only=True)
class MarketIndexTrBootstrapRunResult:
    status: str
    run_id: str
    planned_count: int
    command_count: int
    command_results: Sequence[MarketIndexTrBootstrapCommandPlan] = field(
        default_factory=tuple
    )
    reason_codes: Sequence[str] = field(default_factory=tuple)
    observe_only: bool = True
    no_order_side_effects: bool = True
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "planned_count": self.planned_count,
            "command_count": self.command_count,
            "command_results": [item.to_dict() for item in self.command_results],
            "reason_codes": list(self.reason_codes),
            "observe_only": self.observe_only,
            "no_order_side_effects": self.no_order_side_effects,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def inspect_market_index_tr_bootstrap_event(
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> MarketIndexTrBootstrapInspection:
    resolved_settings = settings or load_settings()
    if event.event_type.strip().lower() != "tr_response":
        return MarketIndexTrBootstrapInspection(recognized=False, valid=False)
    try:
        response = BrokerTrResponse.from_dict(event.payload)
    except Exception as exc:
        if not _payload_has_bootstrap_marker(event.payload):
            return MarketIndexTrBootstrapInspection(recognized=False, valid=False)
        return MarketIndexTrBootstrapInspection(
            recognized=True,
            valid=False,
            reason_codes=("MARKET_INDEX_TR_BOOTSTRAP_RESPONSE_INVALID", str(exc)),
        )

    metadata = dict(response.metadata)
    source = str(
        metadata.get("projection_source") or metadata.get("source") or ""
    ).strip().upper()
    recognized = bool(
        response.request_id.lower().startswith(MARKET_INDEX_TR_BOOTSTRAP_REQUEST_PREFIX)
        or response.request_name.lower().startswith(
            MARKET_INDEX_TR_BOOTSTRAP_REQUEST_NAME_PREFIX
        )
        or source == MARKET_INDEX_TR_BOOTSTRAP_SOURCE
    )
    if not recognized:
        return MarketIndexTrBootstrapInspection(recognized=False, valid=False)

    reasons: list[str] = []
    index_code = _request_index_code(response.request_id)
    metadata_index_code = str(metadata.get("index_code") or "").strip().upper()
    parser_status = str(
        metadata.get("parser_status")
        or resolved_settings.market_index_tr_bootstrap_parser_status
    ).strip().upper()
    if source != MARKET_INDEX_TR_BOOTSTRAP_SOURCE:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_SOURCE_INVALID")
    if (
        str(metadata.get("tr_bootstrap_contract_version") or "").strip().lower()
        != MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION
    ):
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_INVALID")
    if response.tr_code.upper() != resolved_settings.market_index_tr_bootstrap_tr_code:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_TR_CODE_MISMATCH")
    if index_code not in resolved_settings.market_index_tr_bootstrap_index_codes:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_INDEX_CODE_INVALID")
    if metadata_index_code != index_code:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_INDEX_LINEAGE_MISMATCH")
    if not response.success:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_RESPONSE_FAILED")
    if len(response.rows) != 1:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_ROW_COUNT_INVALID")
    if not parser_status:
        reasons.append("MARKET_INDEX_TR_BOOTSTRAP_PARSER_STATUS_MISSING")
    if reasons:
        return MarketIndexTrBootstrapInspection(
            recognized=True,
            valid=False,
            index_code=index_code,
            parser_status=parser_status or "UNKNOWN",
            response=response,
            reason_codes=tuple(reasons),
        )

    try:
        tick = _tick_from_response(
            event,
            response,
            index_code=str(index_code),
            parser_status=parser_status,
            timezone_name=resolved_settings.candidate_trade_date_timezone,
        )
    except (TypeError, ValueError) as exc:
        return MarketIndexTrBootstrapInspection(
            recognized=True,
            valid=False,
            index_code=index_code,
            parser_status=parser_status,
            response=response,
            reason_codes=("MARKET_INDEX_TR_BOOTSTRAP_ROW_PARSE_FAILED", str(exc)),
        )
    return MarketIndexTrBootstrapInspection(
        recognized=True,
        valid=True,
        index_code=index_code,
        parser_status=parser_status,
        response=response,
        tick=tick,
    )


def is_market_index_tr_bootstrap_event(
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> bool:
    return inspect_market_index_tr_bootstrap_event(
        event,
        settings=settings,
    ).recognized


def run_market_index_tr_bootstrap_once(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    queue_commands: bool = False,
    trade_date: str | None = None,
) -> MarketIndexTrBootstrapRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("market_index_tr_bootstrap")
    if not resolved_settings.market_index_tr_bootstrap_enabled:
        return MarketIndexTrBootstrapRunResult(
            status="DISABLED",
            run_id=run_id,
            planned_count=0,
            command_count=0,
            reason_codes=("MARKET_INDEX_TR_BOOTSTRAP_DISABLED",),
        )
    if (
        resolved_settings.trading_mode is not TradingMode.OBSERVE
        or resolved_settings.trading_allow_live_sim
        or resolved_settings.trading_allow_live_real
    ):
        return MarketIndexTrBootstrapRunResult(
            status="BLOCKED_SAFETY",
            run_id=run_id,
            planned_count=0,
            command_count=0,
            reason_codes=("MARKET_INDEX_TR_BOOTSTRAP_OBSERVE_ONLY",),
        )

    resolved_trade_date = trade_date or datetime.now(
        candidate_timezone(resolved_settings.candidate_trade_date_timezone)
    ).date().isoformat()
    request_bucket = int(utc_now().timestamp() // 60)
    plans: list[MarketIndexTrBootstrapCommandPlan] = []
    command_count = 0
    for index_code in resolved_settings.market_index_tr_bootstrap_index_codes:
        command = _build_command(
            run_id=run_id,
            trade_date=resolved_trade_date,
            request_bucket=request_bucket,
            index_code=index_code,
            settings=resolved_settings,
        )
        if not queue_commands:
            plans.append(
                MarketIndexTrBootstrapCommandPlan(
                    index_code=index_code,
                    request_id=str(command.payload["request_id"]),
                    tr_code=str(command.payload["tr_code"]),
                    command_id=command.command_id,
                    queued=False,
                )
            )
            continue
        result = enqueue_command(
            connection,
            command,
            expires_at=utc_now() + timedelta(seconds=120),
        )
        command_count += int(result.accepted)
        plans.append(_command_plan(command, index_code=index_code, result=result))

    return MarketIndexTrBootstrapRunResult(
        status=(
            "QUEUED"
            if command_count
            else "DUPLICATE"
            if queue_commands
            else "PLAN_ONLY"
        ),
        run_id=run_id,
        planned_count=len(plans),
        command_count=command_count,
        command_results=tuple(plans),
        reason_codes=(
            ("MARKET_INDEX_TR_BOOTSTRAP_KOA_STUDIO_PENDING",)
            if resolved_settings.market_index_tr_bootstrap_parser_status != "VERIFIED"
            else ()
        ),
    )


def get_market_index_tr_bootstrap_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    parser_status = resolved_settings.market_index_tr_bootstrap_parser_status
    command_count = _count_bootstrap_commands(connection)
    event_count = _count_bootstrap_events(connection)
    sample_count = _count_bootstrap_samples(connection)
    if not resolved_settings.market_index_tr_bootstrap_enabled:
        status = "DISABLED"
        reason_codes = ["MARKET_INDEX_TR_BOOTSTRAP_DISABLED"]
    elif parser_status != "VERIFIED":
        status = "READY_KOA_PENDING"
        reason_codes = ["MARKET_INDEX_TR_BOOTSTRAP_KOA_STUDIO_PENDING"]
    else:
        status = "READY"
        reason_codes = []
    return {
        "status": status,
        "enabled": resolved_settings.market_index_tr_bootstrap_enabled,
        "adapter_status": "IMPLEMENTED",
        "tr_code": resolved_settings.market_index_tr_bootstrap_tr_code,
        "screen_no": resolved_settings.market_index_tr_bootstrap_screen_no,
        "index_codes": list(
            resolved_settings.market_index_tr_bootstrap_index_codes
        ),
        "market_types": dict(
            resolved_settings.market_index_tr_bootstrap_market_types
        ),
        "industry_codes": dict(
            resolved_settings.market_index_tr_bootstrap_industry_codes
        ),
        "parser_status": parser_status,
        "parser_verified": parser_status == "VERIFIED",
        "koa_studio_confirmation_required": parser_status != "VERIFIED",
        "command_count": command_count,
        "event_count": event_count,
        "sample_count": sample_count,
        "reason_codes": reason_codes,
        "observe_only": True,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "nxt_is_not_valid_market_index_evidence": True,
    }


def _build_command(
    *,
    run_id: str,
    trade_date: str,
    request_bucket: int,
    index_code: str,
    settings: Settings,
) -> GatewayCommand:
    request_id = (
        f"{MARKET_INDEX_TR_BOOTSTRAP_REQUEST_PREFIX}{index_code}:"
        f"{trade_date}:{request_bucket}"
    )
    metadata = {
        "source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "projection_source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "tr_bootstrap_contract_version": MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION,
        "adapter_status": "IMPLEMENTED",
        "index_code": index_code,
        "trade_date": trade_date,
        "run_id": run_id,
        "request_bucket": request_bucket,
        "parser_status": settings.market_index_tr_bootstrap_parser_status,
        "tr_row_mode": MARKET_INDEX_TR_BOOTSTRAP_ROW_MODE,
        "tr_output_record_name": MARKET_INDEX_TR_BOOTSTRAP_OUTPUT_RECORD_NAME,
        "koa_studio_confirmation_required": (
            settings.market_index_tr_bootstrap_parser_status != "VERIFIED"
        ),
        "market_type": settings.market_index_tr_bootstrap_market_types[index_code],
        "industry_code": settings.market_index_tr_bootstrap_industry_codes[
            index_code
        ],
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    payload = {
        "request_id": request_id,
        "tr_code": settings.market_index_tr_bootstrap_tr_code,
        "request_name": (
            f"{MARKET_INDEX_TR_BOOTSTRAP_REQUEST_NAME_PREFIX}{index_code.lower()}"
        ),
        "screen_no": settings.market_index_tr_bootstrap_screen_no,
        "fields": list(MARKET_INDEX_TR_BOOTSTRAP_FIELDS),
        "row_mode": MARKET_INDEX_TR_BOOTSTRAP_ROW_MODE,
        "output_record_name": MARKET_INDEX_TR_BOOTSTRAP_OUTPUT_RECORD_NAME,
        "params": {
            "시장구분": settings.market_index_tr_bootstrap_market_types[index_code],
            "업종코드": settings.market_index_tr_bootstrap_industry_codes[index_code],
        },
        "metadata": metadata,
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    return GatewayCommand(
        command_type="request_tr",
        source=MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        payload=payload,
        idempotency_key=(
            f"{MARKET_INDEX_TR_BOOTSTRAP_SOURCE}:{trade_date}:"
            f"{request_bucket}:{index_code}"
        ),
    )


def _tick_from_response(
    event: GatewayEvent,
    response: BrokerTrResponse,
    *,
    index_code: str,
    parser_status: str,
    timezone_name: str,
) -> BrokerMarketIndexTick:
    row = response.rows[0]
    price = _required_number(
        row,
        ("현재가", "업종현재가", "price", "current_price"),
        abs_value=True,
    )
    change_value = _required_number(
        row,
        ("전일대비", "change_value", "change"),
        abs_value=False,
    )
    change_rate = _required_number(
        row,
        ("등락률", "등락율", "change_rate", "change_rate_pct"),
        abs_value=False,
    )
    trade_time_raw = _first_value(row, ("체결시간", "trade_time", "time"))
    trade_time = _bootstrap_trade_time(
        trade_time_raw,
        fallback=response.ts,
        timezone_name=timezone_name,
    )
    metadata = {
        **dict(response.metadata),
        "source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "projection_source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "parser_status": parser_status,
        "parent_event_id": event.event_id,
        "parent_event_type": event.event_type.strip().lower(),
        "request_id": response.request_id,
        "request_name": response.request_name,
        "tr_code": response.tr_code,
        "trade_time_source": "TR_ROW" if trade_time_raw else "TR_RESPONSE_TS",
        "raw_field_names": sorted(str(key) for key in row),
        "nxt_is_not_valid_market_index_evidence": True,
    }
    return BrokerMarketIndexTick(
        index_code=index_code,
        index_name=_INDEX_NAMES[index_code],
        price=price,
        change_rate=change_rate,
        change_value=change_value,
        trade_time=trade_time,
        ts=event.ts,
        metadata=metadata,
    )


def _payload_has_bootstrap_marker(payload: Mapping[str, Any]) -> bool:
    request_id = str(payload.get("request_id") or "").strip().lower()
    request_name = str(payload.get("request_name") or "").strip().lower()
    metadata = payload.get("metadata")
    source = ""
    if isinstance(metadata, Mapping):
        source = str(
            metadata.get("projection_source") or metadata.get("source") or ""
        ).strip().upper()
    return bool(
        request_id.startswith(MARKET_INDEX_TR_BOOTSTRAP_REQUEST_PREFIX)
        or request_name.startswith(MARKET_INDEX_TR_BOOTSTRAP_REQUEST_NAME_PREFIX)
        or source == MARKET_INDEX_TR_BOOTSTRAP_SOURCE
    )


def _request_index_code(request_id: str) -> str | None:
    parts = str(request_id or "").strip().split(":")
    if len(parts) < 3 or f"{parts[0]}:" != MARKET_INDEX_TR_BOOTSTRAP_REQUEST_PREFIX:
        return None
    return parts[1].upper()


def _required_number(
    row: Mapping[str, Any],
    keys: Sequence[str],
    *,
    abs_value: bool,
) -> float:
    raw = _first_value(row, keys)
    if raw in (None, ""):
        raise ValueError(f"required TR field missing: {keys[0]}")
    text = str(raw).strip().replace(",", "").replace("%", "")
    value = float(text)
    return abs(value) if abs_value else value


def _first_value(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _bootstrap_trade_time(
    value: Any,
    *,
    fallback: datetime,
    timezone_name: str,
) -> datetime:
    text = str(value or "").strip()
    if text.isdigit() and 1 <= len(text) <= 6:
        base = parse_timestamp(fallback, "tr_response_ts")
        local = base.astimezone(candidate_timezone(timezone_name))
        padded = text.zfill(6)
        local = local.replace(
            hour=int(padded[:2]),
            minute=int(padded[2:4]),
            second=int(padded[4:6]),
            microsecond=0,
        )
        return local.astimezone(base.tzinfo)
    return parse_timestamp(fallback, "tr_response_ts")


def _command_plan(
    command: GatewayCommand,
    *,
    index_code: str,
    result: EnqueueCommandResult,
) -> MarketIndexTrBootstrapCommandPlan:
    return MarketIndexTrBootstrapCommandPlan(
        index_code=index_code,
        request_id=str(command.payload["request_id"]),
        tr_code=str(command.payload["tr_code"]),
        command_id=command.command_id,
        queued=True,
        accepted=result.accepted,
        status=result.status.value,
        error_message=result.error_message,
    )


def _count_bootstrap_commands(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE command_type = 'request_tr' AND source = ?
        """,
        (MARKET_INDEX_TR_BOOTSTRAP_SOURCE,),
    ).fetchone()
    return int(row["count"] if row is not None else 0)


def _count_bootstrap_events(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_events
        WHERE event_type = 'tr_response'
          AND lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
              LIKE 'market_index_tr_bootstrap:%'
        """
    ).fetchone()
    return int(row["count"] if row is not None else 0)


def _count_bootstrap_samples(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_index_tick_samples
        WHERE json_extract(metadata_json, '$.projection_source') = ?
        """,
        (MARKET_INDEX_TR_BOOTSTRAP_SOURCE,),
    ).fetchone()
    return int(row["count"] if row is not None else 0)


def bootstrap_event_from_row(row: Mapping[str, Any]) -> GatewayEvent:
    payload = row.get("payload_json")
    if isinstance(payload, str):
        payload = json.loads(payload)
    return GatewayEvent(
        event_id=str(row["event_id"]),
        event_type=str(row["event_type"]),
        source=str(row.get("source") or "gateway_event_store"),
        ts=parse_timestamp(row["event_ts"], "event_ts"),
        command_id=row.get("command_id"),
        idempotency_key=row.get("idempotency_key"),
        payload=dict(payload) if isinstance(payload, Mapping) else {},
    )
