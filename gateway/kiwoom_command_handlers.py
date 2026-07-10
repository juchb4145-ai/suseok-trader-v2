from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import normalize_payload, parse_timestamp, utc_now
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
    normalize_order_exchange,
)
from gateway.kiwoom_tr import KiwoomTrRunner
from gateway.order_pre_ack_journal import OrderPreAckJournal, make_order_pre_ack_event

OrderAckCallback = Callable[[GatewayCommand, KiwoomOrderRequest, KiwoomOrderResult], None]
AsyncEventsCallback = Callable[[list[GatewayEvent]], None]
DurableOrderPreAckCallback = Callable[[GatewayEvent], Mapping[str, Any]]

ALLOWED_KIWOOM_COMMAND_TYPES = {
    "heartbeat_request",
    "request_tr",
    "register_realtime",
    "remove_realtime",
    "load_conditions",
    "send_condition",
    "stop_condition",
    "send_order",
    "cancel_order",
}
FORBIDDEN_KIWOOM_COMMAND_TYPES = {
    "modify_order",
    "submit_order",
    "enqueue_order",
    "order_intent",
    "gateway_order",
    "live_order",
}


@dataclass(frozen=True, kw_only=True)
class RateLimitDecision:
    allowed: bool
    wait_time_sec: float = 0.0
    reason: str = ""


class KiwoomRateLimitGovernor:
    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        global_per_second: float = 5.0,
        hourly_limit: float = 900.0,
        min_interval_sec: Mapping[str, float] | None = None,
        type_weights: Mapping[str, float] | None = None,
    ) -> None:
        self.clock = clock
        self.global_per_second = max(float(global_per_second), 0.1)
        self.hourly_limit = max(float(hourly_limit), 1.0)
        self.min_interval_sec = {
            "request_tr": 1.0,
            "load_conditions": 1.0,
            "send_condition": 0.2,
            "send_order": 0.2,
            "cancel_order": 0.2,
            **dict(min_interval_sec or {}),
        }
        self.type_weights = {
            "heartbeat_request": 0.0,
            "register_realtime": 1.0,
            "remove_realtime": 1.0,
            "request_tr": 1.0,
            "load_conditions": 1.0,
            "send_condition": 1.0,
            "stop_condition": 1.0,
            "send_order": 1.0,
            "cancel_order": 1.0,
            **dict(type_weights or {}),
        }
        self._last_call_at: dict[str, float] = {}
        self._second_calls: deque[tuple[float, float]] = deque()
        self._hourly_calls: deque[tuple[float, float]] = deque()

    def check(self, command_type: str) -> RateLimitDecision:
        normalized = command_type.strip().lower()
        weight = max(float(self.type_weights.get(normalized, 1.0)), 0.0)
        if weight <= 0:
            return RateLimitDecision(allowed=True)

        now = self.clock()
        self._trim(now)
        waits: list[tuple[float, str]] = []
        interval = max(float(self.min_interval_sec.get(normalized, 0.0)), 0.0)
        last_at = self._last_call_at.get(normalized)
        if interval > 0 and last_at is not None:
            wait = interval - (now - last_at)
            if wait > 0:
                waits.append((wait, "type_min_interval"))

        second_used = sum(call_weight for _, call_weight in self._second_calls)
        if second_used + weight > self.global_per_second and self._second_calls:
            waits.append((self._second_calls[0][0] + 1.0 - now, "global_per_second"))

        hourly_used = sum(call_weight for _, call_weight in self._hourly_calls)
        if hourly_used + weight > self.hourly_limit and self._hourly_calls:
            waits.append((self._hourly_calls[0][0] + 3600.0 - now, "hourly_rolling_limit"))

        waits = [(max(wait, 0.001), reason) for wait, reason in waits if wait > 0]
        if not waits:
            return RateLimitDecision(allowed=True)
        wait_time, reason = max(waits, key=lambda item: item[0])
        return RateLimitDecision(allowed=False, wait_time_sec=wait_time, reason=reason)

    def record(self, command_type: str) -> None:
        normalized = command_type.strip().lower()
        weight = max(float(self.type_weights.get(normalized, 1.0)), 0.0)
        if weight <= 0:
            return
        now = self.clock()
        self._trim(now)
        self._last_call_at[normalized] = now
        self._second_calls.append((now, weight))
        self._hourly_calls.append((now, weight))

    def _trim(self, now: float) -> None:
        while self._second_calls and self._second_calls[0][0] <= now - 1.0:
            self._second_calls.popleft()
        while self._hourly_calls and self._hourly_calls[0][0] <= now - 3600.0:
            self._hourly_calls.popleft()


class KiwoomGatewayCommandHandler:
    def __init__(
        self,
        client: Any,
        *,
        source: str = "kiwoom_gateway",
        tr_runner: KiwoomTrRunner | None = None,
        on_order_ack: OrderAckCallback | None = None,
        on_async_events: AsyncEventsCallback | None = None,
        on_durable_order_pre_ack: DurableOrderPreAckCallback | None = None,
        order_journal: OrderPreAckJournal | None = None,
        rate_limit_governor: KiwoomRateLimitGovernor | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.source = source
        self.tr_runner = tr_runner or KiwoomTrRunner(client)
        self.on_order_ack = on_order_ack
        self.on_async_events = on_async_events
        self.on_durable_order_pre_ack = on_durable_order_pre_ack
        self.order_journal = order_journal
        self.clock = clock
        self.subscriptions: set[str] = set()
        self.rate_limit_governor = rate_limit_governor or KiwoomRateLimitGovernor(clock=clock)

    def handle(self, command: GatewayCommand) -> list[GatewayEvent]:
        command_type = command.command_type.strip().lower()
        if command_type in FORBIDDEN_KIWOOM_COMMAND_TYPES or (
            "order" in command_type and command_type not in {"send_order", "cancel_order"}
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
        if command_type in {"send_order", "cancel_order"} and _command_expired(command):
            return [
                make_command_failed_event(
                    command,
                    "EXPIRED_BEFORE_EXECUTION",
                    source=self.source,
                )
            ]
        if command_type == "send_order":
            return self._handle_send_order(command)
        if command_type == "cancel_order":
            return self._handle_cancel_order(command)

        events = [make_command_started_event(command, source=self.source)]
        decision = self.rate_limit_governor.check(command_type)
        if not decision.allowed:
            events.append(self._rate_limited_event(command, decision))
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
        common_kwargs = {
            "request_id": request_id,
            "tr_code": tr_code,
            "request_name": request_name,
            "params": dict(params),
            "continuation_key": payload.get("continuation_key"),
            "fields": fields or None,
            "screen_no": _string_value(payload, "screen_no", "8700"),
        }
        if self.on_async_events is not None:
            completed_results: list[Any] = []
            submitting = True

            def on_complete(result) -> None:
                if submitting:
                    completed_results.append(result)
                    return
                self.on_async_events(self._tr_result_events(command, result, request_id))

            submission = self.tr_runner.submit(**common_kwargs, on_complete=on_complete)
            submitting = False
            if not submission.accepted:
                return [
                    make_command_failed_event(
                        command,
                        ";".join(submission.result.errors) or "TR_REQUEST_FAILED",
                        source=self.source,
                    )
                ]
            if completed_results:
                return self._tr_result_events(command, completed_results[-1], request_id)
            return []

        result = self.tr_runner.request(**common_kwargs)
        return self._tr_result_events(command, result, request_id)

    def _tr_result_events(
        self,
        command: GatewayCommand,
        result,
        request_id: str,
    ) -> list[GatewayEvent]:
        payload = command.payload
        tr_code = _string_value(payload, "tr_code", "")
        request_name = _string_value(
            payload,
            "request_name",
            _string_value(payload, "rq_name", tr_code or "KIWOOM_TR"),
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
            metadata=normalize_payload(_mapping_value(payload, "metadata")),
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
        decision = self.rate_limit_governor.check("send_order")
        if not decision.allowed:
            events: list[GatewayEvent] = []
            self._emit_before_blocking_call(
                make_command_started_event(command, source=self.source),
                events,
            )
            events.append(self._rate_limited_event(command, decision))
            return events

        payload = command.payload
        request = KiwoomOrderRequest(
            account=_string_value(payload, "account_id", _string_value(payload, "account", "")),
            code=normalize_code(payload.get("code")),
            quantity=int(payload.get("quantity") or 0),
            price=_order_price(payload),
            side=_string_value(payload, "side", "BUY").upper(),
            tag=_string_value(payload, "tag", command.command_id),
            order_type=_kiwoom_order_type(payload),
            hoga=_kiwoom_hoga(payload),
            command_id=command.command_id,
            idempotency_key=command.idempotency_key or "",
            order_exchange=normalize_order_exchange(payload.get("order_exchange", "KRX")),
            metadata=_mapping_value(payload, "metadata"),
        )
        events: list[GatewayEvent] = []
        self._emit_before_blocking_call(
            make_command_started_event(command, source=self.source),
            events,
        )
        durable_pre_ack_error = self._record_durable_pre_ack(
            command,
            request,
            broker_env=actual_broker_env,
        )
        if durable_pre_ack_error is not None:
            if self.order_journal is not None:
                self.order_journal.mark_failed(command, durable_pre_ack_error)
            events.append(
                make_command_failed_event(
                    command,
                    durable_pre_ack_error,
                    source=self.source,
                )
            )
            return events
        try:
            result = self.client.send_order(request)
            self._record_rate_limit("send_order")
            if self.order_journal is not None:
                self.order_journal.mark_broker_result(command, result)
        except Exception as exc:
            if self.order_journal is not None:
                self.order_journal.mark_unconfirmed(command, str(exc))
            events.append(self._order_broker_unconfirmed_event(command, exc))
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
            "order_exchange": request.order_exchange,
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

    def _handle_cancel_order(self, command: GatewayCommand) -> list[GatewayEvent]:
        safety_error = validate_command_type_allowed("cancel_order", command=command)
        if safety_error is not None:
            return [make_command_failed_event(command, safety_error, source=self.source)]
        actual_broker_env = self._actual_broker_env()
        if actual_broker_env != "SIMULATION":
            return [
                make_command_failed_event(
                    command,
                    f"Kiwoom cancel_order requires simulation server, got {actual_broker_env}",
                    source=self.source,
                )
            ]
        decision = self.rate_limit_governor.check("cancel_order")
        if not decision.allowed:
            events: list[GatewayEvent] = []
            self._emit_before_blocking_call(
                make_command_started_event(command, source=self.source),
                events,
            )
            events.append(self._rate_limited_event(command, decision))
            return events

        payload = command.payload
        request = KiwoomOrderRequest(
            account=_string_value(payload, "account_id", _string_value(payload, "account", "")),
            code=normalize_code(payload.get("code")),
            quantity=int(payload.get("cancel_quantity") or payload.get("quantity") or 0),
            price=0,
            side="BUY_CANCEL",
            tag=_string_value(payload, "tag", command.command_id),
            order_type=_kiwoom_order_type(payload),
            hoga=_kiwoom_hoga(payload),
            original_order_no=_string_value(payload, "original_order_no", ""),
            command_id=command.command_id,
            idempotency_key=command.idempotency_key or "",
            order_exchange=normalize_order_exchange(payload.get("order_exchange", "KRX")),
            metadata=_mapping_value(payload, "metadata"),
        )
        events: list[GatewayEvent] = []
        self._emit_before_blocking_call(
            make_command_started_event(command, source=self.source),
            events,
        )
        durable_pre_ack_error = self._record_durable_pre_ack(
            command,
            request,
            broker_env=actual_broker_env,
        )
        if durable_pre_ack_error is not None:
            if self.order_journal is not None:
                self.order_journal.mark_failed(command, durable_pre_ack_error)
            events.append(
                make_command_failed_event(
                    command,
                    durable_pre_ack_error,
                    source=self.source,
                )
            )
            return events
        try:
            result = self.client.send_order(request)
            self._record_rate_limit("cancel_order")
            if self.order_journal is not None:
                self.order_journal.mark_broker_result(command, result)
        except Exception as exc:
            if self.order_journal is not None:
                self.order_journal.mark_unconfirmed(command, str(exc))
            events.append(self._order_broker_unconfirmed_event(command, exc))
            return events

        details = {
            "accepted": bool(result.ok),
            "broker_result_code": str(result.code),
            "broker_message": result.message,
            "original_order_no": request.original_order_no,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_env": actual_broker_env,
            "account_mode": actual_broker_env,
            "server_mode": actual_broker_env,
            "order_exchange": request.order_exchange,
        }
        if result.ok:
            events.append(
                make_command_ack_event(
                    command,
                    source=self.source,
                    message="Kiwoom LIVE_SIM cancel accepted",
                    details=details,
                )
            )
        else:
            events.append(
                make_command_failed_event(
                    command,
                    f"Kiwoom cancel_order rejected: {result.message}",
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
        decision = self.rate_limit_governor.check(command_type)
        return 0.0 if decision.allowed else decision.wait_time_sec

    def _record_rate_limit(self, command_type: str) -> None:
        self.rate_limit_governor.record(command_type)

    def _record_durable_pre_ack(
        self,
        command: GatewayCommand,
        request: KiwoomOrderRequest,
        *,
        broker_env: str,
    ) -> str | None:
        event = (
            self.order_journal.record_pre_ack(
                command,
                request,
                broker_env=broker_env,
                source=self.source,
            )
            if self.order_journal is not None
            else make_order_pre_ack_event(
                command,
                request,
                broker_env=broker_env,
                source=self.source,
            )
        )
        if self.on_durable_order_pre_ack is None:
            return "DURABLE_DB_PRE_ACK_FAILED: callback is not configured"
        try:
            response = self.on_durable_order_pre_ack(event)
        except Exception as exc:
            return f"DURABLE_DB_PRE_ACK_FAILED: {exc}"
        if not isinstance(response, Mapping):
            return "DURABLE_DB_PRE_ACK_FAILED: Core returned an invalid response"
        if response.get("accepted") is not True:
            return "DURABLE_DB_PRE_ACK_FAILED: Core did not accept pre-ack event"
        if response.get("durable_pre_ack_recorded") is not True:
            return "DURABLE_DB_PRE_ACK_FAILED: Core did not confirm durable pre-ack"
        return None

    def _order_broker_unconfirmed_event(
        self,
        command: GatewayCommand,
        exc: Exception,
    ) -> GatewayEvent:
        return GatewayEvent(
            event_type="order_broker_unconfirmed",
            source=self.source,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={
                "command_id": command.command_id,
                "command_type": command.command_type,
                "idempotency_key": command.idempotency_key,
                "status": "UNCONFIRMED",
                "broker_call_attempted": True,
                "broker_acceptance_unknown": True,
                "error_message": str(exc),
                "live_sim_only": True,
                "live_real_allowed": False,
            },
        )

    def _emit_before_blocking_call(
        self,
        event: GatewayEvent,
        returned_events: list[GatewayEvent],
    ) -> None:
        if self.on_async_events is not None:
            self.on_async_events([event])
            return
        returned_events.append(event)

    def _rate_limited_event(
        self,
        command: GatewayCommand,
        decision: RateLimitDecision,
    ) -> GatewayEvent:
        return GatewayEvent(
            event_type="rate_limited",
            source=self.source,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={
                "command_id": command.command_id,
                "command_type": command.command_type,
                "wait_time_sec": round(decision.wait_time_sec, 3),
                "next_available_in_sec": round(decision.wait_time_sec, 3),
                "reason": decision.reason,
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


def _command_expired(command: GatewayCommand) -> bool:
    value = (
        command.payload.get("_gateway_command_expires_at")
        or command.payload.get("gateway_command_expires_at")
        or command.payload.get("expires_at")
    )
    if not value:
        return False
    try:
        return parse_timestamp(value, "expires_at") <= utc_now()
    except (TypeError, ValueError):
        return False


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
    if side == "BUY":
        return 1
    if side == "SELL":
        return 2
    if side in {"BUY_CANCEL", "CANCEL_BUY"}:
        return 3
    raise ValueError("Kiwoom LIVE_SIM adapter allows BUY, close-only SELL, or BUY_CANCEL")
