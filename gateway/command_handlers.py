from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import normalize_payload

from gateway.event_factory import (
    make_command_ack_event,
    make_command_failed_event,
    make_command_started_event,
    make_condition_event,
    make_condition_load_result_event,
    make_tr_response_event,
)

ALLOWED_COMMAND_TYPES: frozenset[str] = frozenset(
    {
        "heartbeat_request",
        "request_tr",
        "register_realtime",
        "remove_realtime",
        "load_conditions",
        "send_condition",
        "stop_condition",
    }
)
FORBIDDEN_COMMAND_TYPES: frozenset[str] = frozenset(
    {
        "send_order",
        "submit_order",
        "cancel_order",
        "modify_order",
        "enqueue_order",
        "order_intent",
        "gateway_order",
        "live_order",
    }
)


class GatewayCommandHandler:
    def __init__(
        self,
        *,
        source: str = "mock_gateway",
        default_code: str = "005930",
        default_name: str = "삼성전자",
    ) -> None:
        self.source = source
        self.default_code = default_code
        self.default_name = default_name
        self.subscriptions: set[str] = set()

    def handle(self, command: GatewayCommand) -> list[GatewayEvent]:
        command_type = command.command_type.strip().lower()
        if _is_forbidden_order_command(command_type):
            return [
                make_command_failed_event(
                    command,
                    f"order command disabled: {command.command_type}",
                    source=self.source,
                )
            ]
        if command_type not in ALLOWED_COMMAND_TYPES:
            return [
                make_command_failed_event(
                    command,
                    f"unsupported gateway command: {command.command_type}",
                    source=self.source,
                )
            ]

        events = [make_command_started_event(command, source=self.source)]
        try:
            events.extend(self._handle_allowed(command_type, command))
        except Exception as exc:
            events.append(make_command_failed_event(command, str(exc), source=self.source))
        return events

    def _handle_allowed(self, command_type: str, command: GatewayCommand) -> list[GatewayEvent]:
        if command_type == "heartbeat_request":
            return [make_command_ack_event(command, source=self.source, message="heartbeat ok")]
        if command_type == "request_tr":
            return self._handle_request_tr(command)
        if command_type == "register_realtime":
            return self._handle_register_realtime(command)
        if command_type == "remove_realtime":
            return self._handle_remove_realtime(command)
        if command_type == "load_conditions":
            return [
                make_condition_load_result_event(source=self.source),
                make_command_ack_event(command, source=self.source, message="conditions loaded"),
            ]
        if command_type == "send_condition":
            return self._handle_send_condition(command)
        if command_type == "stop_condition":
            return [
                make_command_ack_event(
                    command,
                    source=self.source,
                    message="condition stopped",
                )
            ]
        return [
            make_command_failed_event(
                command,
                f"unsupported gateway command: {command.command_type}",
                source=self.source,
            )
        ]

    def _handle_request_tr(self, command: GatewayCommand) -> list[GatewayEvent]:
        payload = command.payload
        request_id = _string_value(payload, "request_id", command.command_id)
        tr_code = _string_value(payload, "tr_code", "OPT10001")
        request_name = _string_value(payload, "request_name", "mock_tr_request")
        rows = payload.get("rows")
        if not isinstance(rows, list):
            rows = [
                {
                    "code": _string_value(payload, "code", self.default_code),
                    "name": _string_value(payload, "name", self.default_name),
                    "price": 70000,
                    "source": "mock",
                }
            ]
        response_event = make_tr_response_event(
            request_id=request_id,
            tr_code=tr_code,
            request_name=request_name,
            rows=[normalize_payload(row) for row in rows],
            source=self.source,
        )
        ack_event = make_command_ack_event(command, source=self.source, message="tr response sent")
        return [response_event, ack_event]

    def _handle_register_realtime(self, command: GatewayCommand) -> list[GatewayEvent]:
        codes = _extract_codes(command.payload, default_code=self.default_code)
        self.subscriptions.update(codes)
        return [
            make_command_ack_event(
                command,
                source=self.source,
                message="realtime registered",
                details={"codes": sorted(codes), "subscriptions": sorted(self.subscriptions)},
            )
        ]

    def _handle_remove_realtime(self, command: GatewayCommand) -> list[GatewayEvent]:
        codes = _extract_codes(command.payload, default_code=self.default_code)
        for code in codes:
            self.subscriptions.discard(code)
        return [
            make_command_ack_event(
                command,
                source=self.source,
                message="realtime removed",
                details={"codes": sorted(codes), "subscriptions": sorted(self.subscriptions)},
            )
        ]

    def _handle_send_condition(self, command: GatewayCommand) -> list[GatewayEvent]:
        payload = command.payload
        condition_event = make_condition_event(
            source=self.source,
            condition_id=_string_value(payload, "condition_id", "mock_condition_1"),
            condition_name=_string_value(payload, "condition_name", "Mock Condition"),
            code=_string_value(payload, "code", self.default_code),
            name=_string_value(payload, "name", self.default_name),
            action=_string_value(payload, "action", "ENTER"),
            price=int(payload.get("price", 70000)),
            metadata={"mock": True, "command_id": command.command_id},
        )
        ack_event = make_command_ack_event(command, source=self.source, message="condition sent")
        return [condition_event, ack_event]


def _is_forbidden_order_command(command_type: str) -> bool:
    return command_type in FORBIDDEN_COMMAND_TYPES or "order" in command_type


def _extract_codes(payload: Mapping[str, Any], *, default_code: str) -> set[str]:
    raw_codes = payload.get("codes", payload.get("code", default_code))
    if isinstance(raw_codes, str):
        return {raw_codes}
    if isinstance(raw_codes, Iterable):
        return {str(code) for code in raw_codes}
    return {default_code}


def _string_value(payload: Mapping[str, Any], key: str, default: str) -> str:
    value = payload.get(key, default)
    if value is None:
        return default
    return str(value)
