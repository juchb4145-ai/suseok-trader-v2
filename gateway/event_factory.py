from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import normalize_payload, utc_now


def make_heartbeat_event(
    *,
    source: str = "mock_gateway",
    status: str = "ok",
    sequence: int | None = None,
) -> GatewayEvent:
    payload: dict[str, Any] = {"status": status}
    if sequence is not None:
        payload["sequence"] = sequence
    return GatewayEvent(event_type="heartbeat", source=source, payload=payload)


def make_price_tick_event(
    *,
    source: str = "mock_gateway",
    code: str = "005930",
    name: str = "삼성전자",
    price: int = 70000,
    change_rate: float = 0.0,
    volume: int = 1000,
    trade_value: int | None = None,
    execution_strength: float = 100.0,
    best_bid: int | None = None,
    best_ask: int | None = None,
    spread_ticks: int = 1,
    day_high: int | None = None,
    day_low: int | None = None,
) -> GatewayEvent:
    tick = BrokerPriceTick(
        code=code,
        name=name,
        price=price,
        change_rate=change_rate,
        volume=volume,
        trade_value=trade_value if trade_value is not None else price * volume,
        execution_strength=execution_strength,
        best_bid=best_bid if best_bid is not None else max(price - 100, 1),
        best_ask=best_ask if best_ask is not None else price,
        spread_ticks=spread_ticks,
        day_high=day_high if day_high is not None else price + 500,
        day_low=day_low if day_low is not None else max(price - 500, 1),
        trade_time=utc_now(),
    )
    return GatewayEvent(event_type="price_tick", source=source, payload=tick.to_dict())


def make_market_index_tick_event(
    *,
    source: str = "mock_gateway",
    index_code: str = "KOSPI",
    index_name: str | None = None,
    price: float = 2800.0,
    change_rate: float = 0.0,
    change_value: float = 0.0,
    metadata: Mapping[str, Any] | None = None,
) -> GatewayEvent:
    tick = BrokerMarketIndexTick(
        index_code=index_code,
        index_name=index_name or index_code,
        price=price,
        change_rate=change_rate,
        change_value=change_value,
        trade_time=utc_now(),
        metadata=metadata or {"mock": True},
    )
    return GatewayEvent(
        event_type="market_index_tick",
        source=source,
        payload=tick.to_dict(),
    )


def make_condition_event(
    *,
    source: str = "mock_gateway",
    condition_id: str = "mock_condition_1",
    condition_name: str = "Mock Condition",
    code: str = "005930",
    name: str = "삼성전자",
    action: str = "ENTER",
    price: int = 70000,
    metadata: Mapping[str, Any] | None = None,
) -> GatewayEvent:
    condition_event = BrokerConditionEvent(
        condition_id=condition_id,
        condition_name=condition_name,
        code=code,
        name=name,
        action=action,
        price=price,
        metadata=metadata or {"mock": True},
    )
    return GatewayEvent(
        event_type="condition_event",
        source=source,
        payload=condition_event.to_dict(),
    )


def make_condition_load_result_event(
    *,
    source: str = "mock_gateway",
    conditions: Sequence[Mapping[str, Any]] | None = None,
) -> GatewayEvent:
    payload = {
        "success": True,
        "conditions": [
            normalize_payload(condition)
            for condition in (
                conditions
                or (
                    {
                        "condition_id": "mock_condition_1",
                        "condition_name": "Mock Condition",
                    },
                )
            )
        ],
    }
    return GatewayEvent(event_type="condition_load_result", source=source, payload=payload)


def make_tr_response_event(
    *,
    request_id: str,
    tr_code: str,
    request_name: str,
    rows: list[dict[str, Any]],
    source: str = "mock_gateway",
    message: str = "mock tr response",
    metadata: Mapping[str, Any] | None = None,
) -> GatewayEvent:
    response = BrokerTrResponse(
        request_id=request_id,
        tr_code=tr_code,
        request_name=request_name,
        success=True,
        rows=rows,
        message=message,
        metadata=normalize_payload(metadata or {}),
    )
    return GatewayEvent(event_type="tr_response", source=source, payload=response.to_dict())


def make_command_started_event(
    command: GatewayCommand,
    *,
    source: str = "mock_gateway",
) -> GatewayEvent:
    return GatewayEvent(
        event_type="command_started",
        source=source,
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload=_command_payload(command, status="started"),
    )


def make_command_ack_event(
    command: GatewayCommand,
    *,
    source: str = "mock_gateway",
    message: str = "mock command acknowledged",
    details: Mapping[str, Any] | None = None,
) -> GatewayEvent:
    payload = _command_payload(command, status="acked")
    payload["message"] = message
    if details is not None:
        payload["details"] = normalize_payload(details)
    return GatewayEvent(
        event_type="command_ack",
        source=source,
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload=payload,
    )


def make_command_failed_event(
    command: GatewayCommand,
    error_message: str,
    *,
    source: str = "mock_gateway",
) -> GatewayEvent:
    payload = _command_payload(command, status="failed")
    payload["error_message"] = error_message
    return GatewayEvent(
        event_type="command_failed",
        source=source,
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload=payload,
    )


def _command_payload(command: GatewayCommand, *, status: str) -> dict[str, Any]:
    return {
        "command_id": command.command_id,
        "command_type": command.command_type,
        "status": status,
    }
