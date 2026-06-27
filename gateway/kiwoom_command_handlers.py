from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import normalize_payload
from storage.gateway_command_store import validate_command_type_allowed

from gateway.event_factory import (
    make_command_ack_event,
    make_command_failed_event,
    make_command_started_event,
)
from gateway.kiwoom_client import (
    KiwoomOrderRequest,
    KiwoomOrderResult,
    broker_env_from_server_gubun,
    normalize_code,
)
from gateway.kiwoom_tr import KiwoomTrRunner

OrderAckCallback = Callable[[GatewayCommand, KiwoomOrderRequest, KiwoomOrderResult], None]

ALLOWED_KIWOOM_COMMAND_TYPES = {
    "heartbeat_request",
    "request_tr",
    "register_realtime",
    "remove_realtime",
    "load_conditions",
    "send_condition",
    "stop_condition",
    "send_order",
}
FORBIDDEN_KIWOOM_COMMAND_TYPES = {
    "cancel_order",
    "modify_order",
    "submit_order",
    "enqueue_order",
    "order_intent",
    "gateway_order",
    "live_order",
}


class KiwoomGatewayCommandHandler:
    def __init__(
        self,
        client: Any,
        *,
        source: str = "kiwoom_gateway",
        tr_runner: KiwoomTrRunner | None = None,
        on_order_ack: OrderAckCallback | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.source = source
        self.tr_runner = tr_runner or KiwoomTrRunner(client)
        self.on_order_ack = on_order_ack
        self.clock = clock
        self.subscriptions: set[str] = set()
        self._last_call_at: dict[str, float] = {}
        self._min_interval_sec = {
            "request_tr": 1.0,
            "load_conditions": 1.0,
            "send_condition": 0.2,
            "send_order": 0.2,
        }

    def handle(self, command: GatewayCommand) -> list[GatewayEvent]:
        command_type = command.command_type.strip().lower()
        if command_type in FORBIDDEN_KIWOOM_COMMAND_TYPES or (
            "order" in command_type and command_type != "send_order"
        ):
            return [
                make_command_failed_event(
                    command,
                    f"{command.command_type} is disabled in the real Kiwoom Gateway adapter",
                    source=self.source,
                )
            ]
        if command_type not in ALLOWED_KIWOOM_COMMAND_TYPES:
            return [
                make_command_failed_event(
                    command,
                    f"unsupported Kiwoom gateway command: {command.command_type}",
                    source=self.source,
                )
            ]
        if command_type == "send_order":
            return self._handle_send_order(command)

        events = [make_command_started_event(command, source=self.source)]
        wait_time = self._rate_limit_wait_time(command_type)
        if wait_time > 0:
            events.append(self._rate_limited_event(command, wait_time))
            events.append(
                make_command_failed_event(
                    command,
                    f"Kiwoom command rate limited: wait {wait_time:.3f}s",
                    source=self.source,
                )
            )
            return events
        try:
            events.extend(self._handle_non_order(command_type, command))
            self._record_rate_limit(command_type)
        except Exception as exc:
            events.append(make_command_failed_event(command, str(exc), source=self.source))
        return events

    def _handle_non_order(self, command_type: str, command: GatewayCommand) -> list[GatewayEvent]:
        if command_type == "heartbeat_request":
            return [make_command_ack_event(command, source=self.source, message="heartbeat ok")]
        if command_type == "request_tr":
            return self._handle_request_tr(command)
        if command_type == "register_realtime":
            return self._handle_register_realtime(command)
        if command_type == "remove_realtime":
            return self._handle_remove_realtime(command)
        if command_type == "load_conditions":
            result_code = int(self.client.load_conditions() or 0)
            if result_code <= 0:
                return [
                    make_command_failed_event(
                        command,
                        f"condition load request failed: {result_code}",
                        source=self.source,
                    )
                ]
            return [
                make_command_ack_event(
                    command,
                    source=self.source,
                    message="condition load requested",
                    details={"broker_result_code": str(result_code)},
                )
            ]
        if command_type == "send_condition":
            return self._handle_send_condition(command)
        if command_type == "stop_condition":
            payload = command.payload
            self.client.stop_condition(
                str(payload.get("screen_no") or "7600"),
                str(payload.get("condition_name") or ""),
                int(payload.get("condition_index") or payload.get("condition_id") or 0),
            )
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
                f"unsupported Kiwoom gateway command: {command.command_type}",
                source=self.source,
            )
        ]

    def _handle_request_tr(self, command: GatewayCommand) -> list[GatewayEvent]:
        payload = command.payload
        request_id = _string_value(payload, "request_id", command.command_id)
        tr_code = _string_value(payload, "tr_code", "")
        request_name = _string_value(
            payload,
            "request_name",
            _string_value(payload, "rq_name", tr_code or "KIWOOM_TR"),
        )
        params = _mapping_value(payload, "params")
        if not params:
            params = _mapping_value(payload, "inputs")
        fields = _string_list(payload.get("fields"))
        result = self.tr_runner.request(
            request_id=request_id,
            tr_code=tr_code,
            request_name=request_name,
            params=dict(params),
            continuation_key=payload.get("continuation_key"),
            fields=fields or None,
            screen_no=_string_value(payload, "screen_no", "8700"),
        )
        if not result.success:
            return [
                make_command_failed_event(
                    command,
                    ";".join(result.errors) or "TR_REQUEST_FAILED",
                    source=self.source,
                )
            ]
        response = BrokerTrResponse(
            request_id=request_id,
            tr_code=tr_code,
            request_name=request_name,
            success=True,
            rows=[normalize_payload(row) for row in result.rows],
            message="kiwoom tr response",
            continuation_key=result.continuation_key,
        )
        return [
            GatewayEvent(
                event_type="tr_response",
                source=self.source,
                command_id=command.command_id,
                idempotency_key=command.idempotency_key,
                payload=response.to_dict(),
            ),
            make_command_ack_event(
                command,
                source=self.source,
                message="tr response sent",
                details={"row_count": len(result.rows), "warnings": result.warnings},
            ),
        ]

    def _handle_register_realtime(self, command: GatewayCommand) -> list[GatewayEvent]:
        payload = command.payload
        codes = _extract_codes(payload)
        self.client.register_realtime(codes, screen_no=payload.get("screen_no"))
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
        payload = command.payload
        codes = _extract_codes(payload)
        self.client.remove_realtime(codes, screen_no=payload.get("screen_no"))
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
        result_code = int(
            self.client.send_condition(
                str(payload.get("screen_no") or "7600"),
                str(payload.get("condition_name") or ""),
                int(payload.get("condition_index") or payload.get("condition_id") or 0),
                realtime=_bool_value(payload.get("realtime"), True),
                search_type=_optional_int(payload.get("search_type")),
            )
            or 0
        )
        if result_code != 1:
            return [
                make_command_failed_event(
                    command,
                    f"condition send failed: {result_code}",
                    source=self.source,
                )
            ]
        return [
            make_command_ack_event(
                command,
                source=self.source,
                message="condition sent",
                details={"broker_result_code": str(result_code)},
            )
        ]

    def _handle_send_order(self, command: GatewayCommand) -> list[GatewayEvent]:
        safety_error = validate_command_type_allowed("send_order", command=command)
        if safety_error is not None:
            return [make_command_failed_event(command, safety_error, source=self.source)]
        actual_broker_env = self._actual_broker_env()
        if actual_broker_env != "SIMULATION":
            return [
                make_command_failed_event(
                    command,
                    f"Kiwoom send_order requires simulation server, got {actual_broker_env}",
                    source=self.source,
                )
            ]
        wait_time = self._rate_limit_wait_time("send_order")
        if wait_time > 0:
            return [
                make_command_started_event(command, source=self.source),
                self._rate_limited_event(command, wait_time),
                make_command_failed_event(
                    command,
                    f"Kiwoom send_order rate limited: wait {wait_time:.3f}s",
                    source=self.source,
                ),
            ]

        payload = command.payload
        request = KiwoomOrderRequest(
            account=_string_value(payload, "account_id", _string_value(payload, "account", "")),
            code=normalize_code(payload.get("code")),
            quantity=int(payload.get("quantity") or 0),
            price=_order_price(payload),
            side="BUY",
            tag=_string_value(payload, "tag", command.command_id),
            order_type=_kiwoom_order_type(payload),
            hoga=_kiwoom_hoga(payload),
            command_id=command.command_id,
            idempotency_key=command.idempotency_key or "",
            metadata=_mapping_value(payload, "metadata"),
        )
        events = [make_command_started_event(command, source=self.source)]
        try:
            result = self.client.send_order(request)
            self._record_rate_limit("send_order")
        except Exception as exc:
            events.append(make_command_failed_event(command, str(exc), source=self.source))
            return events

        if result.ok and self.on_order_ack is not None:
            self.on_order_ack(command, request, result)
        details = {
            "accepted": bool(result.ok),
            "broker_result_code": str(result.code),
            "broker_message": result.message,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_env": actual_broker_env,
            "account_mode": actual_broker_env,
            "server_mode": actual_broker_env,
        }
        if result.order_no:
            details["broker_order_no"] = result.order_no
            details["broker_order_id"] = result.order_no
        if result.ok:
            events.append(
                make_command_ack_event(
                    command,
                    source=self.source,
                    message="Kiwoom LIVE_SIM order accepted",
                    details=details,
                )
            )
        else:
            events.append(
                make_command_failed_event(
                    command,
                    f"Kiwoom SendOrder rejected: {result.message}",
                    source=self.source,
                )
            )
        return events

    def _actual_broker_env(self) -> str:
        getter = getattr(self.client, "get_server_gubun", None)
        if not callable(getter):
            return "UNKNOWN"
        try:
            return broker_env_from_server_gubun(getter())
        except Exception:
            return "UNKNOWN"

    def _rate_limit_wait_time(self, command_type: str) -> float:
        interval = self._min_interval_sec.get(command_type, 0.0)
        if interval <= 0:
            return 0.0
        last_at = self._last_call_at.get(command_type)
        if last_at is None:
            return 0.0
        return max(0.0, interval - (self.clock() - last_at))

    def _record_rate_limit(self, command_type: str) -> None:
        self._last_call_at[command_type] = self.clock()

    def _rate_limited_event(self, command: GatewayCommand, wait_time: float) -> GatewayEvent:
        return GatewayEvent(
            event_type="rate_limited",
            source=self.source,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={
                "command_id": command.command_id,
                "command_type": command.command_type,
                "wait_time_sec": round(wait_time, 3),
                "status": "rate_limited",
            },
        )


def _extract_codes(payload: Mapping[str, Any]) -> set[str]:
    raw_codes = payload.get("codes", payload.get("code", []))
    if isinstance(raw_codes, str):
        candidates = raw_codes.replace(",", ";").split(";")
    elif isinstance(raw_codes, Iterable):
        candidates = [str(code) for code in raw_codes]
    else:
        candidates = []
    return {normalize_code(code) for code in candidates if str(code).strip()}


def _string_value(payload: Mapping[str, Any], key: str, default: str) -> str:
    value = payload.get(key, default)
    if value is None:
        return default
    return str(value)


def _mapping_value(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.replace(",", ";").split(";") if item.strip()]
    if isinstance(value, Iterable):
        return [str(item) for item in value if str(item).strip()]
    return []


def _bool_value(value: object, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _optional_int(value: object) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _order_price(payload: Mapping[str, Any]) -> int:
    order_type = str(payload.get("order_type") or "").strip().upper()
    if order_type == "MARKET":
        return 0
    return int(payload.get("price") or payload.get("limit_price") or 0)


def _kiwoom_hoga(payload: Mapping[str, Any]) -> str:
    explicit = str(payload.get("hoga") or "").strip()
    if explicit:
        return explicit
    if str(payload.get("order_type") or "").strip().upper() == "MARKET":
        return "03"
    return "00"


def _kiwoom_order_type(payload: Mapping[str, Any]) -> int:
    side = str(payload.get("side") or "").strip().upper()
    if side != "BUY":
        raise ValueError("Kiwoom LIVE_SIM adapter allows BUY only")
    return 1
