from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from domain.broker.account_snapshot import mask_account_id
from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, normalize_payload, utc_now

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
        "broker_snapshot_request",
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
        if command_type == "send_order":
            return self._handle_live_sim_send_order(command)
        if command_type == "cancel_order":
            return self._handle_live_sim_cancel_order(command)
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
        if command_type == "broker_snapshot_request":
            return self._handle_broker_snapshot_request(command)
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

    def _handle_live_sim_send_order(self, command: GatewayCommand) -> list[GatewayEvent]:
        error_message = _validate_live_sim_order_payload(command)
        if error_message is not None:
            return [make_command_failed_event(command, error_message, source=self.source)]

        broker_order_no = f"MOCKSIM-{command.command_id[-12:]}"
        events = [
            make_command_started_event(command, source=self.source),
            _make_mock_order_pre_ack_event(command, source=self.source),
        ]
        events.append(
            make_command_ack_event(
                command,
                source=self.source,
                message="mock LIVE_SIM order accepted",
                details={
                    "accepted": True,
                    "broker_order_no": broker_order_no,
                    "broker_result_code": "0",
                    "live_sim_only": True,
                    "live_real_allowed": False,
                },
            )
        )
        metadata = _mapping_value(command.payload, "metadata")
        if (
            command.payload.get("mock_emit_execution") is True
            or metadata.get("mock_emit_execution") is True
        ):
            events.append(
                GatewayEvent(
                    event_type="execution_event",
                    source=self.source,
                    command_id=command.command_id,
                    idempotency_key=command.idempotency_key,
                    payload={
                        "execution_id": f"exec-{command.command_id}",
                        "broker_order_id": broker_order_no,
                        "broker_order_no": broker_order_no,
                        "client_order_id": command.command_id,
                        "account_id": command.payload["account_id"],
                        "code": command.payload["code"],
                        "side": command.payload["side"],
                        "quantity": int(command.payload["quantity"]),
                        "price": int(
                            command.payload.get("price") or command.payload["limit_price"]
                        ),
                        "remaining_quantity": 0,
                        "executed_at": datetime_to_wire(utc_now()),
                        "metadata": {
                            "live_sim_only": True,
                            "live_real_allowed": False,
                            "broker_env": command.payload["broker_env"],
                            "account_mode": command.payload["account_mode"],
                            "server_mode": command.payload["server_mode"],
                            "live_sim_intent_id": metadata["live_sim_intent_id"],
                            "gateway_command_id": command.command_id,
                        },
                    },
                )
            )
        return events

    def _handle_live_sim_cancel_order(self, command: GatewayCommand) -> list[GatewayEvent]:
        error_message = _validate_live_sim_cancel_payload(command)
        if error_message is not None:
            return [make_command_failed_event(command, error_message, source=self.source)]

        events = [
            make_command_started_event(command, source=self.source),
            _make_mock_order_pre_ack_event(command, source=self.source),
        ]
        events.append(
            make_command_ack_event(
                command,
                source=self.source,
                message="mock LIVE_SIM cancel accepted",
                details={
                    "accepted": True,
                    "broker_result_code": "0",
                    "original_order_no": command.payload.get("original_order_no"),
                    "live_sim_only": True,
                    "live_real_allowed": False,
                },
            )
        )
        return events

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
            metadata=_mapping_value(payload, "metadata"),
        )
        ack_event = make_command_ack_event(command, source=self.source, message="tr response sent")
        return [response_event, ack_event]

    def _handle_broker_snapshot_request(
        self,
        command: GatewayCommand,
    ) -> list[GatewayEvent]:
        payload = command.payload
        simulation_values = {"SIM", "SIMULATION", "MOCK", "PAPER"}
        if any(
            str(payload.get(key) or "").strip().upper() not in simulation_values
            for key in ("account_mode", "broker_env", "server_mode")
        ):
            return [
                make_command_failed_event(
                    command,
                    "broker_snapshot_request requires simulation-like modes",
                    source=self.source,
                )
            ]
        account_id = str(payload.get("account_id") or "").strip()
        snapshot = _mapping_value(payload, "mock_snapshot")
        snapshot_payload = {
            "snapshot_id": _string_value(payload, "snapshot_id", command.command_id),
            "snapshot_status": "COMPLETE",
            "complete": True,
            "fresh": True,
            "account_id_masked": mask_account_id(account_id),
            "trade_date": _string_value(payload, "trade_date", ""),
            "snapshot_at": datetime_to_wire(utc_now()),
            "open_orders": list(snapshot.get("open_orders") or []),
            "executions": list(snapshot.get("executions") or []),
            "positions": list(snapshot.get("positions") or []),
            "page_lineage": list(snapshot.get("page_lineage") or []),
            "requested_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
            "completed_sections": ["OPEN_ORDERS", "EXECUTIONS", "POSITIONS"],
            "errors": [],
            "source": "mock_gateway_read_only",
            "mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "automatic_local_repair": False,
        }
        return [
            GatewayEvent(
                event_type="account_snapshot",
                source=self.source,
                command_id=command.command_id,
                idempotency_key=command.idempotency_key,
                payload=normalize_payload(snapshot_payload),
            ),
            make_command_ack_event(
                command,
                source=self.source,
                message="mock LIVE_SIM broker snapshot complete",
                details={"snapshot_status": "COMPLETE", "live_real_allowed": False},
            ),
        ]

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


def _make_mock_order_pre_ack_event(
    command: GatewayCommand,
    *,
    source: str,
) -> GatewayEvent:
    payload = command.payload
    return GatewayEvent(
        event_type="order_pre_ack",
        source=source,
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload={
            "command_id": command.command_id,
            "command_type": command.command_type,
            "idempotency_key": command.idempotency_key,
            "status": "PRE_ACK",
            "broker_env": payload.get("broker_env", "MOCK"),
            "account_id": payload.get("account_id"),
            "code": payload.get("code"),
            "side": payload.get("side"),
            "quantity": payload.get("quantity"),
            "price": payload.get("price", payload.get("limit_price")),
            "order_type": payload.get("order_type"),
            "original_order_no": payload.get("original_order_no"),
            "metadata": normalize_payload(_mapping_value(payload, "metadata")),
            "mock": True,
        },
    )


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


def _validate_live_sim_order_payload(command: GatewayCommand) -> str | None:
    payload = command.payload
    metadata = _mapping_value(payload, "metadata")
    if command.source.strip().lower() != "live_sim":
        return "mock order command disabled except live_sim source"
    if not command.idempotency_key:
        return "mock send_order requires idempotency_key"
    if str(payload.get("mode", "")).upper() != "LIVE_SIM":
        return "mock send_order requires mode=LIVE_SIM"
    if metadata.get("live_sim_only") is not True:
        return "mock send_order requires metadata.live_sim_only=true"
    if metadata.get("live_real_allowed") is not False:
        return "mock send_order requires metadata.live_real_allowed=false"
    if not metadata.get("live_sim_intent_id"):
        return "mock send_order requires live_sim_intent_id"
    side = str(payload.get("side", "")).upper()
    if side == "SELL":
        if payload.get("close_only") is not True or metadata.get("close_only") is not True:
            return "mock SELL requires close_only=true"
        if not metadata.get("position_id") or not metadata.get("exit_intent_id"):
            return "mock SELL requires position_id and exit_intent_id"
        if str(payload.get("order_type", "")).upper() == "MARKET":
            return "mock SELL market order disabled"
    elif side != "BUY":
        return "mock send_order allows BUY or close-only SELL only"
    for field_name in ("account_mode", "broker_env", "server_mode"):
        if not _is_simulation_like(payload.get(field_name)):
            return f"mock send_order requires simulation-like {field_name}"
    return None


def _validate_live_sim_cancel_payload(command: GatewayCommand) -> str | None:
    payload = command.payload
    metadata = _mapping_value(payload, "metadata")
    if command.source.strip().lower() != "live_sim":
        return "mock cancel_order requires live_sim source"
    if not command.idempotency_key:
        return "mock cancel_order requires idempotency_key"
    if str(payload.get("mode", "")).upper() != "LIVE_SIM":
        return "mock cancel_order requires mode=LIVE_SIM"
    if payload.get("live_sim_only") is not True or metadata.get("live_sim_only") is not True:
        return "mock cancel_order requires live_sim_only=true"
    if payload.get("live_real_allowed") is not False:
        return "mock cancel_order requires live_real_allowed=false"
    if metadata.get("live_real_allowed") is not False:
        return "mock cancel_order requires metadata.live_real_allowed=false"
    if str(payload.get("side", "")).upper() not in {"BUY_CANCEL", "CANCEL_BUY"}:
        return "mock cancel_order requires BUY cancel side"
    if not payload.get("original_order_no"):
        return "mock cancel_order requires original_order_no"
    if not metadata.get("cancel_intent_id"):
        return "mock cancel_order requires cancel_intent_id"
    for field_name in ("account_mode", "broker_env", "server_mode"):
        if not _is_simulation_like(payload.get(field_name)):
            return f"mock cancel_order requires simulation-like {field_name}"
    return None


def _mapping_value(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _is_simulation_like(value: object) -> bool:
    return str(value or "").strip().upper() in {
        "SIMULATION",
        "MOCK",
        "PAPER",
        "MOCK_TRADING",
        "LIVE_SIM",
    }
