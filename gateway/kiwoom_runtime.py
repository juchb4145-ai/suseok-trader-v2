from __future__ import annotations

import os
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.condition_profiles import (
    ConditionProfile,
    ConditionSessionProfile,
    condition_profiles_from_config,
    current_condition_session_profile,
    normalize_condition_profile_payload,
)
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now

from gateway.condition_admission import (
    ConditionAdmissionDecision,
    GatewayConditionAdmissionController,
)
from gateway.core_client import CoreClient
from gateway.core_io_worker import CoreIoWorker
from gateway.event_factory import make_command_failed_event
from gateway.kiwoom_client import (
    ConditionInfo,
    KiwoomChejanParseResult,
    KiwoomOrderRequest,
    KiwoomOrderResult,
    broker_env_from_server_gubun,
    condition_event_payload,
    is_market_index_real_type,
    normalize_code,
    normalize_market_index_code,
    normalize_realtime_exchange,
    realtime_code_for_exchange,
)
from gateway.kiwoom_command_handlers import KiwoomGatewayCommandHandler
from gateway.transport import GatewayTransportError


@dataclass(frozen=True, kw_only=True)
class KiwoomGatewayRuntimeConfig:
    source: str = "kiwoom_gateway"
    command_limit: int = 20
    command_wait_sec: float = 0.0
    condition_name: str | None = None
    condition_index: int | None = None
    condition_realtime: bool = True
    condition_profiles: tuple[ConditionProfile, ...] = ()
    condition_send_interval_sec: float = 0.25
    realtime_codes: tuple[str, ...] = ()
    realtime_exchange: str = "KRX"
    observe_only: bool = True
    account: str = ""
    realtime_recover_stale_sec: float = 45.0
    realtime_recover_interval_sec: float = 300.0
    realtime_callback_timeout_sec: float = 15.0
    market_index_enabled: bool = False
    market_index_realtime_enabled: bool = False
    market_index_tr_bootstrap_enabled: bool = False
    market_index_codes: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    market_index_screen_no: str = "5700"
    market_index_poll_sec: float = 60.0
    condition_load_timeout_sec: float = 10.0
    condition_load_max_retry: int = 1
    core_io_enabled: bool = True
    command_polling_enabled: bool = True
    event_posting_enabled: bool = True
    core_io_worker_enabled: bool = False


@dataclass(frozen=True, kw_only=True)
class PendingOrderRecord:
    command_id: str
    idempotency_key: str
    account_id: str
    code: str
    side: str
    live_sim_intent_id: str
    account_mode: str
    broker_env: str
    server_mode: str


class PendingOrderRegistry:
    def __init__(self) -> None:
        self._by_signature: dict[tuple[str, str, str], PendingOrderRecord] = {}
        self._by_broker_order_no: dict[str, PendingOrderRecord] = {}

    def record_ack(
        self,
        command: GatewayCommand,
        request: KiwoomOrderRequest,
        result: KiwoomOrderResult,
    ) -> None:
        metadata = dict(request.metadata)
        record = PendingOrderRecord(
            command_id=command.command_id,
            idempotency_key=command.idempotency_key or request.idempotency_key,
            account_id=request.account,
            code=normalize_code(request.code),
            side=request.side.upper(),
            live_sim_intent_id=str(
                metadata.get("live_sim_intent_id")
                or command.payload.get("live_sim_intent_id")
                or ""
            ),
            account_mode=str(command.payload.get("account_mode") or "SIMULATION").upper(),
            broker_env=str(command.payload.get("broker_env") or "SIMULATION").upper(),
            server_mode=str(command.payload.get("server_mode") or "SIMULATION").upper(),
        )
        self._by_signature[self._signature(record.account_id, record.code, record.side)] = record
        if result.order_no:
            self._by_broker_order_no[str(result.order_no)] = record

    def enrich_chejan_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        enriched = dict(payload)
        record = self._match(enriched)
        if record is None:
            return enriched
        broker_order_no = str(
            enriched.get("broker_order_no") or enriched.get("broker_order_id") or ""
        )
        if broker_order_no:
            self._by_broker_order_no[broker_order_no] = record
        enriched["command_id"] = record.command_id
        enriched["idempotency_key"] = record.idempotency_key
        enriched["live_sim_intent_id"] = record.live_sim_intent_id
        return enriched

    def enrich_execution_payload(
        self,
        payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, str]:
        enriched = dict(payload)
        record = self._match(enriched)
        command_id = ""
        idempotency_key = ""
        if record is not None:
            command_id = record.command_id
            idempotency_key = record.idempotency_key
            metadata = dict(enriched.get("metadata") or {})
            metadata.update(
                {
                    "gateway_command_id": record.command_id,
                    "live_sim_intent_id": record.live_sim_intent_id,
                    "idempotency_key": record.idempotency_key,
                    "account_mode": record.account_mode,
                    "broker_env": record.broker_env,
                    "server_mode": record.server_mode,
                    "live_sim_only": True,
                    "live_real_allowed": False,
                }
            )
            enriched["metadata"] = metadata
        return enriched, command_id, idempotency_key

    def _match(self, payload: Mapping[str, Any]) -> PendingOrderRecord | None:
        broker_order_no = str(
            payload.get("broker_order_no") or payload.get("broker_order_id") or ""
        )
        if broker_order_no and broker_order_no in self._by_broker_order_no:
            return self._by_broker_order_no[broker_order_no]
        account = str(payload.get("account_id") or payload.get("account") or "")
        code = str(payload.get("code") or "")
        side = str(payload.get("side") or "").upper()
        if not account or not code or not side:
            return None
        return self._by_signature.get(self._signature(account, normalize_code(code), side))

    @staticmethod
    def _signature(account: str, code: str, side: str) -> tuple[str, str, str]:
        return (str(account), normalize_code(code), str(side).upper())


class KiwoomGatewayRuntime:
    def __init__(
        self,
        *,
        client: Any,
        core_client: CoreClient,
        config: KiwoomGatewayRuntimeConfig | None = None,
        schedule_delayed: Callable[[float, Callable[[], None]], None] | None = None,
    ) -> None:
        self.client = client
        self.core_client = core_client
        self.schedule_delayed = schedule_delayed
        self.config = config or KiwoomGatewayRuntimeConfig()
        self.pending_orders = PendingOrderRegistry()
        self.command_handler = KiwoomGatewayCommandHandler(
            client,
            source=self.config.source,
            on_order_ack=self.pending_orders.record_ack,
        )
        self._event_queue: deque[GatewayEvent] = deque()
        self._heartbeat_sequence = 0
        self._posted_count = 0
        self._polled_count = 0
        self._handled_command_count = 0
        self._last_error = ""
        self._login_in_progress = False
        self._login_requested = False
        self._login_result_code: int | None = None
        self._login_error = ""
        self._login_started_at = ""
        self._login_finished_at = ""
        self._login_threaded = False
        self._login_connect_state_fallback_skipped = False
        self._latest_comm_connect_call_at: datetime | None = None
        self._latest_comm_connect_result_at: datetime | None = None
        self._latest_comm_connect_result_code: int | None = None
        self._latest_on_event_connect_timeout_at: datetime | None = None
        self._condition_load_state = "IDLE"
        self._condition_load_requested_at: datetime | None = None
        self._condition_load_retry_count = 0
        self._condition_load_timeout_count = 0
        self._condition_load_call_in_progress = False
        self._latest_condition_ver_callback_at: datetime | None = None
        self._latest_condition_ver_result: dict[str, Any] = {}
        self._condition_profiles: tuple[ConditionProfile, ...] = condition_profiles_from_config(
            profiles=self.config.condition_profiles,
            legacy_condition_name=self.config.condition_name,
            legacy_condition_index=self.config.condition_index,
            legacy_realtime=self.config.condition_realtime,
        )
        self._condition_profile_by_screen: dict[str, ConditionProfile] = {}
        self._condition_profile_by_key: dict[tuple[str, int], ConditionProfile] = {}
        self._condition_send_results: list[dict[str, Any]] = []
        self._condition_reason_codes: list[str] = []
        self._condition_send_queue: deque[ConditionProfile] = deque()
        self._last_condition_send_at = 0.0
        self._condition_admission = GatewayConditionAdmissionController()
        self._registered_realtime_codes: set[str] = set()
        self._last_price_tick_at: datetime | None = None
        self._last_quote_at: datetime | None = None
        self._last_realtime_callback_at: datetime | None = None
        self._last_realtime_registration_at: datetime | None = None
        self._last_realtime_recover_at: datetime | None = None
        self._realtime_recover_count = 0
        self._realtime_recover_error = ""
        self._quote_event_count = 0
        self._realtime_callback_count = 0
        self._parsed_price_tick_count = 0
        self._realtime_parse_error_count = 0
        self._latest_realtime_parse_error: dict[str, Any] = {}
        self._real_type_counts: dict[str, int] = {}
        self._realtime_callback_real_type_counts: dict[str, int] = {}
        self._last_realtime_registration_result: dict[str, Any] = {}
        self._realtime_registration_requested_count = 0
        self._realtime_registration_success_count = 0
        self._realtime_registration_dedupe_count = 0
        self._market_index_registered_codes: set[str] = set()
        self._market_index_callback_count = 0
        self._parsed_market_index_tick_count = 0
        self._market_index_parse_error_count = 0
        self._latest_market_index_tick_at: datetime | None = None
        self._latest_market_index_parse_error: dict[str, Any] = {}
        self._latest_market_index_registration_result: dict[str, Any] = {}
        self._raw_callback_counts: dict[str, int] = {}
        self._latest_callback_at_by_method: dict[str, str] = {}
        self._active_x_thread_audit: deque[dict[str, Any]] = deque(maxlen=50)
        self._core_worker: CoreIoWorker | None = None
        self._local_event_count = 0
        self._latest_local_event: dict[str, Any] = {}

    def emit(self, event_type: str, payload: Mapping[str, Any], **kwargs: Any) -> None:
        self.emit_event(
            GatewayEvent(
                event_type=event_type,
                source=self.config.source,
                payload=dict(payload),
                **kwargs,
            )
        )

    def emit_event(self, event: GatewayEvent) -> None:
        self._event_queue.append(event)

    def emit_heartbeat(self) -> None:
        self._heartbeat_sequence += 1
        self._finish_pending_login_if_connected()
        self._recover_realtime_if_stalled()
        payload = self.heartbeat_payload()
        payload["sequence"] = self._heartbeat_sequence
        self.emit("heartbeat", payload)

    def start_core_io_worker(self) -> None:
        if not self.config.core_io_enabled or not self.config.core_io_worker_enabled:
            return
        if self._core_worker is None:
            self._core_worker = CoreIoWorker(
                core_client=self.core_client,
                command_limit=self.config.command_limit,
                command_wait_sec=self.config.command_wait_sec,
                command_polling_enabled=self.config.command_polling_enabled,
                event_posting_enabled=self.config.event_posting_enabled,
            )
        self._core_worker.start()

    def stop_core_io_worker(self) -> None:
        if self._core_worker is not None:
            self._core_worker.stop()

    def sync_core_io_status(self) -> None:
        if self._core_worker is None:
            return
        snapshot = self._core_worker.snapshot()
        self._posted_count = snapshot.posted_count
        self._polled_count = snapshot.poll_count
        if snapshot.last_error:
            self._last_error = snapshot.last_error

    def drain_core_io_worker(self, *, limit: int = 100) -> None:
        if self._core_worker is None:
            return
        commands = self._core_worker.drain_commands(limit=limit)
        self.sync_core_io_status()
        if commands:
            self.handle_commands(commands)
            self.flush_events()

    def flush_events(self, *, limit: int = 200) -> None:
        if not self.config.core_io_enabled or not self.config.event_posting_enabled:
            self._drain_events_locally(limit=limit)
            return
        if self._core_worker is not None:
            drained = 0
            while self._event_queue and drained < max(int(limit), 1):
                self._core_worker.enqueue_event(self._event_queue.popleft())
                drained += 1
            self.sync_core_io_status()
            return
        drained = 0
        while self._event_queue and drained < max(int(limit), 1):
            event = self._event_queue[0]
            try:
                self.core_client.post_event(event)
            except GatewayTransportError as exc:
                self._last_error = str(exc)
                break
            except Exception as exc:
                self._last_error = str(exc)
                break
            self._event_queue.popleft()
            self._posted_count += 1
            drained += 1

    def poll_and_handle_commands(self) -> None:
        if not self.config.core_io_enabled or not self.config.command_polling_enabled:
            return
        try:
            commands = self.core_client.poll_commands(
                limit=self.config.command_limit,
                wait_sec=self.config.command_wait_sec,
            )
        except GatewayTransportError as exc:
            self._last_error = str(exc)
            return
        except Exception as exc:
            self._last_error = str(exc)
            return
        self._polled_count += 1
        self.handle_commands(commands)
        self.flush_events()

    def handle_commands(self, commands: Iterable[GatewayCommand]) -> None:
        for command in commands:
            self._handled_command_count += 1
            try:
                events = self.command_handler.handle(command)
                self._track_realtime_command(command, events)
                for event in events:
                    self.emit_event(event)
            except Exception as exc:
                self._last_error = str(exc)
                self.emit_event(
                    make_command_failed_event(
                        command,
                        str(exc),
                        source=self.config.source,
                    )
                )
                self.emit(
                    "gateway_error",
                    {
                        "message": "COMMAND_HANDLE_FAILED",
                        "command_id": command.command_id,
                        "command_type": command.command_type,
                        "error": str(exc),
                    },
                )

    def _drain_events_locally(self, *, limit: int = 200) -> None:
        drained = 0
        while self._event_queue and drained < max(int(limit), 1):
            event = self._event_queue.popleft()
            self._local_event_count += 1
            self._latest_local_event = event.to_dict()
            drained += 1

    def heartbeat_payload(self) -> dict[str, Any]:
        self.sync_core_io_status()
        worker_snapshot = self._core_worker.snapshot() if self._core_worker is not None else None
        worker_event_queue_size = (
            worker_snapshot.event_queue_size if worker_snapshot is not None else 0
        )
        worker_command_queue_size = (
            worker_snapshot.command_queue_size if worker_snapshot is not None else 0
        )
        worker_last_error = worker_snapshot.last_error if worker_snapshot is not None else ""
        worker_coalesced_count = (
            worker_snapshot.coalesced_count if worker_snapshot is not None else 0
        )
        worker_coalesce_after_size = (
            worker_snapshot.coalesce_after_size if worker_snapshot is not None else 0
        )
        worker_dropped_count = worker_snapshot.dropped_count if worker_snapshot is not None else 0
        worker_max_buffer_size = (
            worker_snapshot.max_buffer_size if worker_snapshot is not None else 0
        )
        logged_in = self.kiwoom_logged_in()
        accounts = self._accounts() if logged_in else []
        account = self.config.account or os.getenv("TRADING_ACCOUNT", "").strip()
        if not account and accounts:
            account = accounts[0]
        server_gubun = self._server_gubun() if logged_in else ""
        broker_env = broker_env_from_server_gubun(server_gubun) if logged_in else "UNKNOWN"
        orderable = bool(logged_in and account and broker_env == "SIMULATION")
        return {
            "status": "ok",
            "kiwoom_logged_in": logged_in,
            "orderable": orderable,
            "mode": os.getenv("TRADING_MODE", "OBSERVE"),
            "account": account,
            "accounts": [_mask_account(item) for item in accounts],
            "broker_name": "KIWOOM",
            "broker_env": broker_env,
            "server_mode": broker_env,
            "account_mode": broker_env,
            "server_gubun": server_gubun,
            "last_error": self._last_error or worker_last_error,
            "reconnect_count": 0,
            "rate_limit": {"adapter": "kiwoom", "local": True},
            "command_queue_size": worker_command_queue_size,
            "event_queue_size": len(self._event_queue) + worker_event_queue_size,
            "core_io_enabled": self.config.core_io_enabled,
            "command_polling_enabled": self.config.command_polling_enabled,
            "event_posting_enabled": self.config.event_posting_enabled,
            "core_io_worker_enabled": self.config.core_io_worker_enabled,
            "core_io_worker_running": (
                bool(worker_snapshot.running) if worker_snapshot is not None else False
            ),
            "core_io_worker_thread_id": (
                worker_snapshot.thread_id if worker_snapshot is not None else None
            ),
            "core_io_worker_event_queue_size": worker_event_queue_size,
            "core_io_worker_command_queue_size": worker_command_queue_size,
            "core_io_worker_last_error": worker_last_error,
            "core_io_worker_coalesced_event_count": worker_coalesced_count,
            "core_io_worker_coalesce_after_size": worker_coalesce_after_size,
            "core_io_worker_dropped_event_count": worker_dropped_count,
            "core_io_worker_max_buffer_size": worker_max_buffer_size,
            "local_event_count": self._local_event_count,
            "latest_local_event": dict(self._latest_local_event),
            "login_in_progress": self._login_in_progress,
            "login_requested": self._login_requested,
            "login_result_code": self._login_result_code,
            "login_error": self._login_error,
            "login_started_at": self._login_started_at,
            "login_finished_at": self._login_finished_at,
            "login_threaded": self._login_threaded,
            "comm_connect_state": self._comm_connect_state(),
            "latest_comm_connect_call_at": _datetime_or_empty(self._latest_comm_connect_call_at),
            "latest_comm_connect_result_at": _datetime_or_empty(
                self._latest_comm_connect_result_at
            ),
            "latest_comm_connect_result_code": self._latest_comm_connect_result_code,
            "latest_on_event_connect_timeout_at": _datetime_or_empty(
                self._latest_on_event_connect_timeout_at
            ),
            "login_block_reason_codes": self._login_block_reason_codes(),
            "condition_load_state": self._condition_load_state,
            "condition_load_requested_at": _datetime_or_empty(self._condition_load_requested_at),
            "condition_load_retry_count": self._condition_load_retry_count,
            "condition_load_timeout_count": self._condition_load_timeout_count,
            "condition_reason_codes": list(self._condition_reason_codes),
            "condition_session_profile": self._condition_session_profile().value,
            "condition_profiles": [
                normalize_condition_profile_payload(profile) for profile in self._condition_profiles
            ],
            "condition_profile_screen_map": {
                screen_no: profile.profile_id
                for screen_no, profile in sorted(self._condition_profile_by_screen.items())
            },
            "condition_send_results": list(self._condition_send_results),
            "condition_profile_metrics": self._condition_admission.metrics(),
            "adaptive_realtime_budget": self._condition_admission.latest_budget(),
            "condition_callback_health": self._condition_callback_health(),
            "latest_condition_ver_callback_at": _datetime_or_empty(
                self._latest_condition_ver_callback_at
            ),
            "latest_condition_ver_result": dict(self._latest_condition_ver_result),
            "registered_realtime_code_count": len(self._registered_realtime_codes),
            "realtime_registered_codes": sorted(self._registered_realtime_codes),
            "realtime_exchange": self._realtime_exchange(),
            "realtime_registered_kiwoom_codes": [
                realtime_code_for_exchange(code, self._realtime_exchange())
                for code in sorted(self._registered_realtime_codes)
            ],
            "latest_price_tick_at": _datetime_or_empty(self._last_price_tick_at),
            "latest_quote_at": _datetime_or_empty(self._last_quote_at),
            "latest_realtime_callback_at": _datetime_or_empty(self._last_realtime_callback_at),
            "quote_event_count": self._quote_event_count,
            "raw_realtime_callback_count": self._realtime_callback_count,
            "realtime_callback_count": self._realtime_callback_count,
            "parsed_price_tick_count": self._parsed_price_tick_count,
            "realtime_parse_error_count": self._realtime_parse_error_count,
            "latest_realtime_parse_error": dict(self._latest_realtime_parse_error),
            "realtime_real_type_counts": dict(sorted(self._real_type_counts.items())),
            "realtime_callback_real_type_counts": dict(
                sorted(self._realtime_callback_real_type_counts.items())
            ),
            "latest_realtime_registration_at": _datetime_or_empty(
                self._last_realtime_registration_at
            ),
            "latest_realtime_registration_result": dict(self._last_realtime_registration_result),
            "realtime_registration_requested_count": (self._realtime_registration_requested_count),
            "realtime_registration_success_count": self._realtime_registration_success_count,
            "realtime_registration_dedupe_count": self._realtime_registration_dedupe_count,
            "market_index_enabled": bool(self.config.market_index_enabled),
            "market_index_realtime_enabled": bool(self.config.market_index_realtime_enabled),
            "market_index_tr_bootstrap_enabled": bool(
                self.config.market_index_tr_bootstrap_enabled
            ),
            "market_index_codes": [
                normalize_market_index_code(code) for code in self.config.market_index_codes
            ],
            "market_index_screen_no": str(self.config.market_index_screen_no or "5700"),
            "market_index_poll_sec": float(self.config.market_index_poll_sec),
            "market_index_registered_codes": sorted(self._market_index_registered_codes),
            "market_index_callback_count": self._market_index_callback_count,
            "parsed_market_index_tick_count": self._parsed_market_index_tick_count,
            "market_index_parse_error_count": self._market_index_parse_error_count,
            "latest_market_index_tick_at": _datetime_or_empty(
                self._latest_market_index_tick_at
            ),
            "latest_market_index_parse_error": dict(self._latest_market_index_parse_error),
            "latest_market_index_registration_result": dict(
                self._latest_market_index_registration_result
            ),
            "market_index_adapter_health": self._market_index_adapter_health(),
            "realtime_subscription_health": self._realtime_subscription_health(),
            "realtime_recover_count": self._realtime_recover_count,
            "realtime_recover_error": self._realtime_recover_error,
            "raw_callback_counts": dict(sorted(self._raw_callback_counts.items())),
            "latest_callback_at_by_method": dict(
                sorted(self._latest_callback_at_by_method.items())
            ),
            "active_x_thread_audit": list(self._active_x_thread_audit),
            "latest_active_x_thread_audit": (
                dict(self._active_x_thread_audit[-1]) if self._active_x_thread_audit else {}
            ),
            "queued_event_count": len(self._event_queue) + worker_event_queue_size,
            "posted_event_count": self._posted_count,
            "poll_count": self._polled_count,
            "handled_command_count": self._handled_command_count,
            "observe_only": self.config.observe_only,
            "live_real_allowed": False,
        }

    def isolation_status_payload(self) -> dict[str, Any]:
        payload = self.heartbeat_payload()
        return {
            "qt_main_thread_id": self._latest_qt_thread_id_for(
                "QApplication.event_loop",
                "QApplication.exec",
            ),
            "active_x_thread_id": self._latest_qt_thread_id_for(
                "QAxWidget.create",
                "SetRealReg",
                "GetConditionLoad",
                "CommConnect",
            ),
            "login_requested_at": payload["login_started_at"],
            "on_event_connect_count": payload["raw_callback_counts"].get("OnEventConnect", 0),
            "condition_load_requested_at": payload["condition_load_requested_at"],
            "condition_ver_callback_count": payload["raw_callback_counts"].get(
                "OnReceiveConditionVer",
                0,
            ),
            "realtime_registration_success_count": payload["realtime_registration_success_count"],
            "raw_realtime_callback_count": payload["raw_realtime_callback_count"],
            "parsed_price_tick_count": payload["parsed_price_tick_count"],
            "realtime_parse_error_count": payload["realtime_parse_error_count"],
            "market_index_enabled": payload["market_index_enabled"],
            "market_index_registered_codes": payload["market_index_registered_codes"],
            "market_index_callback_count": payload["market_index_callback_count"],
            "parsed_market_index_tick_count": payload["parsed_market_index_tick_count"],
            "market_index_parse_error_count": payload["market_index_parse_error_count"],
            "latest_market_index_tick_at": payload["latest_market_index_tick_at"],
            "latest_market_index_parse_error": payload["latest_market_index_parse_error"],
            "latest_condition_ver_callback_at": payload["latest_condition_ver_callback_at"],
            "latest_realtime_callback_at": payload["latest_realtime_callback_at"],
            "latest_price_tick_at": payload["latest_price_tick_at"],
            "condition_load_state": payload["condition_load_state"],
            "condition_callback_health": payload["condition_callback_health"],
            "realtime_subscription_health": payload["realtime_subscription_health"],
            "core_io_enabled": payload["core_io_enabled"],
            "command_polling_enabled": payload["command_polling_enabled"],
            "event_posting_enabled": payload["event_posting_enabled"],
            "queued_event_count": payload["queued_event_count"],
            "local_event_count": payload["local_event_count"],
            "last_error": payload["last_error"],
        }

    def request_login_started(self, *, threaded: bool) -> None:
        self._login_in_progress = True
        self._login_requested = True
        self._login_result_code = None
        self._login_error = ""
        self._login_started_at = datetime_to_wire(utc_now())
        self._login_finished_at = ""
        self._login_threaded = bool(threaded)

    def request_login_failed(self, exc: Exception) -> None:
        self._login_in_progress = False
        self._login_error = str(exc)
        self._login_finished_at = datetime_to_wire(utc_now())
        self.emit("gateway_error", {"message": f"KIWOOM_LOGIN_REQUEST_FAILED:{exc}"})

    def on_connected(self, ok: bool, code: int, message: str) -> None:
        self._login_in_progress = False
        self._login_finished_at = datetime_to_wire(utc_now())
        self._login_result_code = int(code)
        self._login_error = "" if ok else str(message or code)
        self.emit(
            "login_status",
            {
                "logged_in": bool(ok),
                "code": int(code),
                "message": str(message or ""),
                **self._broker_mode_payload(logged_in=bool(ok)),
            },
        )
        self.emit("orderability", self.heartbeat_payload())
        if ok:
            self._register_initial_realtime()
            self._register_initial_market_index_realtime()
            self._load_conditions()
            self._emit_market_symbols()

    def on_condition_load_result(self, success: bool, message: str = "") -> None:
        self._condition_load_state = "LOADED" if success else "FAILED"
        self._condition_load_requested_at = None
        if not success:
            self._add_condition_reason("CONDITION_LOAD_FAILED")
        self.emit(
            "condition_load_result",
            {
                "success": bool(success),
                "message": str(message or ""),
                "conditions": [condition.to_dict() for condition in self._conditions()],
                "reason_codes": [] if success else ["CONDITION_LOAD_FAILED"],
            },
        )

    def on_condition_loaded(self, conditions: Iterable[ConditionInfo]) -> None:
        loaded = [condition.to_dict() for condition in list(conditions or [])]
        self._condition_load_state = "LOADED"
        self._condition_load_requested_at = None
        self.emit("condition_loaded", {"conditions": loaded})
        self._send_configured_conditions(loaded)

    def on_condition_event(
        self,
        *,
        code: str,
        event_type: str,
        condition_name: str,
        condition_index: int,
        source: str,
        screen_no: str | None = None,
        batch_allowed: bool = False,
        planned_batch_count: int = 0,
    ) -> ConditionAdmissionDecision | None:
        normalized_code = normalize_code(code)
        profile = self._profile_for_condition_event(
            condition_name=condition_name,
            condition_index=condition_index,
            screen_no=screen_no,
        )
        profile_metadata = self._condition_profile_metadata(
            profile=profile,
            callback_source=source,
            screen_no=screen_no,
        )
        payload = condition_event_payload(
            code=normalized_code,
            event_type=event_type,
            condition_name=condition_name,
            condition_index=condition_index,
            name=self._code_name(normalized_code),
            metadata=profile_metadata,
        )
        decision: ConditionAdmissionDecision | None = None
        if profile is not None:
            decision = self._condition_admission.decide(
                profile=profile,
                action=str(payload["action"]),
                source=source,
                registered_realtime_count=len(self._registered_realtime_codes),
                runtime_metrics=self._runtime_realtime_metrics(),
                session_profile=self._condition_session_profile(),
                batch_allowed=batch_allowed,
                planned_batch_count=planned_batch_count,
            )
            payload["metadata"]["condition_admission"] = decision.to_dict()
        self.emit("condition_event", payload)
        if decision is not None and decision.register_immediate:
            self.register_realtime_codes([normalized_code])
        return decision

    def on_condition_tr_received(
        self,
        *,
        screen_no: str,
        code_list: str,
        condition_name: str,
        condition_index: int,
        next_flag: str = "",
    ) -> None:
        del next_flag
        batch_codes: list[str] = []
        for code in str(code_list or "").split(";"):
            if not code.strip():
                continue
            normalized_code = normalize_code(code)
            decision = self.on_condition_event(
                code=normalized_code,
                event_type="I",
                condition_name=str(condition_name or ""),
                condition_index=int(condition_index or -1),
                source="tr_condition",
                screen_no=str(screen_no or ""),
                batch_allowed=True,
                planned_batch_count=len(batch_codes),
            )
            if decision is not None and decision.register_batch:
                batch_codes.append(normalized_code)
        if batch_codes:
            self.register_realtime_codes(batch_codes)

    def on_price_tick(self, payload: Mapping[str, Any]) -> None:
        self._last_price_tick_at = utc_now()
        self._parsed_price_tick_count += 1
        self._observe_real_type(payload)
        self.emit("price_tick", dict(payload))

    def on_market_index_tick(self, payload: Mapping[str, Any]) -> None:
        self._latest_market_index_tick_at = utc_now()
        self._parsed_market_index_tick_count += 1
        index_code = str(payload.get("index_code") or "").strip().upper()
        if index_code:
            try:
                self._market_index_registered_codes.add(normalize_market_index_code(index_code))
            except ValueError:
                pass
        self._observe_real_type(payload)
        self.emit("market_index_tick", dict(payload))

    def on_quote(self, payload: Mapping[str, Any]) -> None:
        self._last_quote_at = utc_now()
        self._quote_event_count += 1
        self._observe_real_type(payload)
        self.emit("quote_tick", dict(payload))

    def on_realtime_data(
        self,
        *,
        code: str,
        real_type: str,
        real_data_present: bool = False,
    ) -> None:
        self._last_realtime_callback_at = utc_now()
        self._realtime_callback_count += 1
        normalized_real_type = str(real_type or "").strip() or "UNKNOWN"
        self._realtime_callback_real_type_counts[normalized_real_type] = (
            self._realtime_callback_real_type_counts.get(normalized_real_type, 0) + 1
        )
        if _is_market_index_runtime_callback(code=code, real_type=real_type):
            self._market_index_callback_count += 1

    def on_realtime_registration_result(self, payload: Mapping[str, Any]) -> None:
        normalized = dict(payload)
        if str(normalized.get("registration_type") or "") == "market_index":
            self._latest_market_index_registration_result = normalized
            if normalized.get("success") is True:
                for index_code in normalized.get("index_codes") or normalized.get("codes") or []:
                    self._market_index_registered_codes.add(
                        normalize_market_index_code(index_code)
                    )
            self.emit(
                "gateway_log",
                {
                    "message": "market index realtime registration result",
                    **normalized,
                },
            )
            return
        self._last_realtime_registration_result = normalized
        self._realtime_registration_requested_count += 1
        if normalized.get("success") is True:
            self._realtime_registration_success_count += 1
        self.emit(
            "gateway_log",
            {
                "message": "realtime registration result",
                **normalized,
            },
        )

    def on_realtime_parse_error(self, payload: Mapping[str, Any]) -> None:
        if _is_market_index_parse_error(payload):
            self._market_index_parse_error_count += 1
            self._latest_market_index_parse_error = dict(payload)
            self.emit(
                "gateway_error",
                {
                    "message": "INDEX_PARSE_ERROR",
                    **dict(payload),
                },
            )
            return
        self._realtime_parse_error_count += 1
        self._latest_realtime_parse_error = dict(payload)
        self.emit(
            "gateway_error",
            {
                "message": "REALTIME_PARSE_ERROR",
                **dict(payload),
            },
        )

    def on_active_x_thread_audit(self, payload: Mapping[str, Any]) -> None:
        audit = dict(payload)
        self._active_x_thread_audit.append(audit)
        method = str(audit.get("method") or "")
        phase = str(audit.get("phase") or "")
        if method == "CommConnect" and phase == "CALL":
            self._latest_comm_connect_call_at = utc_now()
            self._latest_comm_connect_result_at = None
            self._latest_comm_connect_result_code = None
            self._latest_on_event_connect_timeout_at = None
        elif method == "CommConnect" and phase == "RESULT":
            self._latest_comm_connect_result_at = utc_now()
            try:
                self._latest_comm_connect_result_code = int(audit.get("result_code"))
            except (TypeError, ValueError):
                self._latest_comm_connect_result_code = None
        elif method == "OnEventConnect" and phase == "TIMEOUT":
            self._latest_on_event_connect_timeout_at = utc_now()
        if phase == "CALLBACK" and method:
            self._raw_callback_counts[method] = self._raw_callback_counts.get(method, 0) + 1
            timestamp = str(audit.get("timestamp") or datetime_to_wire(utc_now()))
            self._latest_callback_at_by_method[method] = timestamp
            if method == "OnReceiveConditionVer":
                self._latest_condition_ver_callback_at = utc_now()
                self._latest_condition_ver_result = {
                    "result": audit.get("result"),
                    "message": audit.get("message"),
                    "timestamp": timestamp,
                }
        if phase in {"CALL", "RESULT"} and method in {
            "CommConnect",
            "GetConditionLoad",
            "SendCondition",
            "SetRealReg",
            "QAxWidget.create",
            "QAxWidget.setControl",
            "QAxWidget.signal_connect",
            "QAxWidget.create_constructor_fallback",
            "QApplication.exec",
        }:
            self.emit(
                "gateway_log",
                {
                    "message": "active_x_thread_audit",
                    **audit,
                },
            )

    def register_realtime_codes(self, codes: Iterable[str], *, force: bool = False) -> None:
        normalized_codes = _ordered_unique_codes(codes)
        if not normalized_codes:
            return
        skipped_already_registered_count = 0
        if not force:
            fresh_codes: list[str] = []
            for code in normalized_codes:
                if code in self._registered_realtime_codes:
                    skipped_already_registered_count += 1
                    continue
                fresh_codes.append(code)
            normalized_codes = fresh_codes
        if skipped_already_registered_count:
            self._realtime_registration_dedupe_count += skipped_already_registered_count
        if not normalized_codes:
            self.emit(
                "gateway_log",
                {
                    "message": "realtime registration skipped already registered",
                    "skipped_already_registered_count": skipped_already_registered_count,
                    "registered_realtime_code_count": len(self._registered_realtime_codes),
                },
            )
            return
        exchange = self._realtime_exchange()
        kiwoom_codes = [realtime_code_for_exchange(code, exchange) for code in normalized_codes]
        self.client.register_realtime(kiwoom_codes)
        self._registered_realtime_codes.update(normalized_codes)
        self._last_realtime_registration_at = utc_now()
        if skipped_already_registered_count:
            self.emit(
                "gateway_log",
                {
                    "message": "realtime registration deduped already registered",
                    "registered_code_count": len(normalized_codes),
                    "skipped_already_registered_count": skipped_already_registered_count,
                    "registered_realtime_code_count": len(self._registered_realtime_codes),
                },
            )

    def _track_realtime_command(
        self,
        command: GatewayCommand,
        events: Iterable[GatewayEvent],
    ) -> None:
        if not any(event.event_type == "command_ack" for event in events):
            return
        command_type = command.command_type.strip().lower()
        if command_type not in {"register_realtime", "remove_realtime"}:
            return
        codes = _command_codes(command.payload)
        if command_type == "register_realtime":
            self._registered_realtime_codes.update(codes)
            self._last_realtime_registration_at = utc_now()
            return
        for code in codes:
            self._registered_realtime_codes.discard(code)
        if not self._registered_realtime_codes:
            self._last_realtime_registration_at = None

    def _recover_realtime_if_stalled(self) -> None:
        codes = sorted(self._registered_realtime_codes)
        if not codes or not self.kiwoom_logged_in():
            return
        stale_sec = float(self.config.realtime_recover_stale_sec)
        if stale_sec <= 0:
            return
        now = utc_now()
        baseline = _latest_datetime(self._last_price_tick_at, self._last_realtime_registration_at)
        if baseline is None or (now - baseline).total_seconds() < stale_sec:
            return
        interval_sec = max(float(self.config.realtime_recover_interval_sec), 1.0)
        if (
            self._last_realtime_recover_at is not None
            and (now - self._last_realtime_recover_at).total_seconds() < interval_sec
        ):
            return
        self._last_realtime_recover_at = now
        try:
            remove_all = getattr(self.client, "remove_all_realtime", None)
            if callable(remove_all):
                remove_all()
            self.register_realtime_codes(codes, force=True)
        except Exception as exc:
            self._last_error = str(exc)
            self._realtime_recover_error = str(exc)
            self.emit(
                "gateway_error",
                {
                    "message": "REALTIME_RECOVER_FAILED",
                    "registered_realtime_code_count": len(codes),
                    "error": str(exc),
                },
            )
            return
        self._last_realtime_registration_at = now
        self._realtime_recover_count += 1
        self._realtime_recover_error = ""
        self.emit(
            "gateway_log",
            {
                "message": "realtime registration reset after stale price tick",
                "registered_realtime_code_count": len(codes),
                "latest_price_tick_at": _datetime_or_empty(self._last_price_tick_at),
                "latest_quote_at": _datetime_or_empty(self._last_quote_at),
                "latest_realtime_callback_at": _datetime_or_empty(self._last_realtime_callback_at),
                "quote_event_count": self._quote_event_count,
                "realtime_callback_count": self._realtime_callback_count,
                "realtime_real_type_counts": dict(sorted(self._real_type_counts.items())),
                "realtime_callback_real_type_counts": dict(
                    sorted(self._realtime_callback_real_type_counts.items())
                ),
            },
        )

    def on_chejan_result(self, result: KiwoomChejanParseResult) -> None:
        chejan_payload = self.pending_orders.enrich_chejan_payload(result.to_event_payload())
        command_id = str(chejan_payload.get("command_id") or "") or None
        idempotency_key = str(chejan_payload.get("idempotency_key") or "") or None
        self.emit(
            result.gateway_event_type,
            chejan_payload,
            command_id=command_id,
            idempotency_key=idempotency_key,
        )
        if result.execution_payload is not None:
            payload, exec_command_id, exec_key = self.pending_orders.enrich_execution_payload(
                result.execution_payload
            )
            self.emit(
                "execution_event",
                payload,
                command_id=exec_command_id or command_id,
                idempotency_key=exec_key or idempotency_key,
            )

    def kiwoom_logged_in(self) -> bool:
        ocx = getattr(self.client, "ocx", None)
        dynamic_call = getattr(ocx, "dynamicCall", None)
        if callable(dynamic_call):
            try:
                return int(dynamic_call("GetConnectState()") or 0) == 1
            except Exception:
                return False
        return bool(self._accounts())

    def _finish_pending_login_if_connected(self) -> None:
        if not self._login_in_progress or self._login_result_code is not None:
            return
        if bool(getattr(self.client, "login_waits_for_event_loop", False)):
            if not self._login_connect_state_fallback_skipped:
                self._login_connect_state_fallback_skipped = True
                self.emit(
                    "gateway_log",
                    {
                        "message": "LOGIN_CONNECT_STATE_FALLBACK_SKIPPED_EVENT_LOOP_CLIENT",
                        "reason_codes": ["ACTIVE_X_CALLBACK_REQUIRED"],
                    },
                )
            return
        if self._login_ready_for_fallback():
            self.on_connected(True, 0, "already connected")

    def _comm_connect_state(self) -> str:
        if self._latest_on_event_connect_timeout_at is not None:
            if self._latest_comm_connect_result_at is None:
                return "EVENT_TIMEOUT_NO_COMM_CONNECT_RESULT"
            return "EVENT_TIMEOUT_AFTER_COMM_CONNECT_RESULT"
        if self._raw_callback_counts.get("OnEventConnect", 0) > 0:
            return "EVENT_CALLBACK_RECEIVED"
        if self._latest_comm_connect_call_at is None:
            return "NOT_REQUESTED"
        if self._latest_comm_connect_result_at is None:
            return "CALLED_WAITING_RESULT"
        return "RETURNED_WAITING_EVENT"

    def _login_block_reason_codes(self) -> list[str]:
        state = self._comm_connect_state()
        reason_codes: list[str] = []
        if state == "EVENT_TIMEOUT_NO_COMM_CONNECT_RESULT":
            reason_codes.extend(
                [
                    "COMM_CONNECT_NO_RETURN",
                    "ON_EVENT_CONNECT_TIMEOUT",
                    "KIWOOM_LOGIN_DIALOG_OR_VERSION_SUSPECTED",
                    "ACTIVE_X_LOGIN_BLOCKED",
                ]
            )
        elif state == "EVENT_TIMEOUT_AFTER_COMM_CONNECT_RESULT":
            reason_codes.extend(
                [
                    "ON_EVENT_CONNECT_TIMEOUT",
                    "ACTIVE_X_CALLBACK_SUSPECTED",
                ]
            )
        if self._login_connect_state_fallback_skipped:
            reason_codes.append("CONNECT_STATE_FALLBACK_SKIPPED")
        return reason_codes

    def _login_ready_for_fallback(self) -> bool:
        if not self.kiwoom_logged_in():
            return False
        return bool(self._server_gubun())

    def close(self) -> None:
        self.stop_core_io_worker()
        close = getattr(self.core_client, "close", None)
        if callable(close):
            close()

    def _load_conditions(self) -> None:
        self._condition_load_state = "LOADING"
        self._condition_load_requested_at = utc_now()
        self._condition_load_call_in_progress = True
        try:
            result = int(self.client.load_conditions() or 0)
        except Exception as exc:
            self._condition_load_state = "FAILED"
            self._condition_load_requested_at = None
            self._condition_load_call_in_progress = False
            self._add_condition_reason("CONDITION_LOAD_EXCEPTION")
            self.emit("gateway_error", {"message": f"CONDITION_LOAD_FAILED:{exc}"})
            return
        self._condition_load_call_in_progress = False
        if result <= 0:
            self._condition_load_state = "FAILED"
            self._condition_load_requested_at = None
            self._add_condition_reason("CONDITION_LOAD_FAILED")
        if result <= 0:
            self.emit(
                "condition_load_result",
                {
                    "success": False,
                    "message": "GetConditionLoad failed",
                    "reason_codes": ["CONDITION_LOAD_FAILED"],
                },
            )

    def check_condition_load_timeout(self) -> None:
        if self._condition_load_call_in_progress:
            return
        if self._condition_load_state != "LOADING" or self._condition_load_requested_at is None:
            return
        timeout_sec = max(float(self.config.condition_load_timeout_sec), 1.0)
        now = utc_now()
        if (now - self._condition_load_requested_at).total_seconds() < timeout_sec:
            return
        self._condition_load_timeout_count += 1
        reason_codes = [
            "CONDITION_VER_CALLBACK_TIMEOUT",
            "CONDITION_LOAD_TIMEOUT",
            self._callback_suspect_reason_code(),
        ]
        for reason in reason_codes:
            self._add_condition_reason(reason)
        if self._login_threaded:
            reason_codes.append("POSSIBLE_THREADING_ISSUE")
        if self._condition_load_retry_count < max(int(self.config.condition_load_max_retry), 0):
            self._condition_load_retry_count += 1
            self.emit(
                "gateway_log",
                {
                    "message": "CONDITION_VER_CALLBACK_TIMEOUT_RETRYING",
                    "reason_codes": reason_codes,
                    "condition_load_retry_count": self._condition_load_retry_count,
                    "condition_load_timeout_sec": timeout_sec,
                },
            )
            self._load_conditions()
            return
        self._condition_load_state = "CALLBACK_TIMEOUT"
        self._condition_load_requested_at = None
        self.emit(
            "gateway_error",
            {
                "message": "CONDITION_VER_CALLBACK_TIMEOUT",
                "reason_codes": reason_codes,
                "condition_load_timeout_count": self._condition_load_timeout_count,
                "condition_load_retry_count": self._condition_load_retry_count,
                "condition_load_timeout_sec": timeout_sec,
            },
        )

    def _send_configured_conditions(self, conditions: list[dict[str, Any]]) -> None:
        profiles = self._resolve_condition_profiles(conditions)
        self._condition_profiles = tuple(profile for profile in profiles if profile.enabled)
        if not self._condition_profiles:
            return
        self._condition_send_queue.clear()
        self._condition_send_queue.extend(self._condition_profiles)
        self._process_condition_send_queue()

    def _process_condition_send_queue(self) -> None:
        # Runs on the Qt main thread; pacing between sends must never block the
        # event loop, so a wait is rescheduled through schedule_delayed instead
        # of time.sleep (which starves ActiveX callbacks).
        while self._condition_send_queue:
            profile = self._condition_send_queue[0]
            if profile.condition_index is None:
                self._condition_send_queue.popleft()
                self._add_condition_reason("CONDITION_PROFILE_INDEX_UNRESOLVED")
                self._record_condition_send_result(
                    profile,
                    result_code=-1,
                    success=False,
                    reason_codes=["CONDITION_PROFILE_INDEX_UNRESOLVED"],
                )
                continue
            interval = max(float(self.config.condition_send_interval_sec), 0.0)
            elapsed = time.monotonic() - self._last_condition_send_at
            if self._last_condition_send_at and elapsed < interval:
                self._schedule_delayed(interval - elapsed, self._process_condition_send_queue)
                return
            self._condition_send_queue.popleft()
            self._send_condition_profile(profile)

    def _send_condition_profile(self, profile: ConditionProfile) -> None:
        screen_no = profile.screen_no or "7600"
        resolved = profile.with_resolution(screen_no=screen_no)
        self._register_condition_profile_mapping(resolved)
        try:
            result = int(
                self.client.send_condition(
                    screen_no,
                    resolved.condition_name,
                    int(resolved.condition_index),
                    realtime=resolved.realtime_search,
                )
                or 0
            )
        except Exception as exc:
            self._add_condition_reason("CONDITION_SEND_EXCEPTION")
            self._record_condition_send_result(
                resolved,
                result_code=-1,
                success=False,
                reason_codes=["CONDITION_SEND_EXCEPTION"],
                error=str(exc),
            )
            self.emit(
                "gateway_error",
                {
                    "message": f"CONFIGURED_CONDITION_SEND_FAILED:{exc}",
                    "condition_profile": resolved.to_dict(),
                    "reason_codes": ["CONDITION_SEND_EXCEPTION"],
                },
            )
            return
        self._last_condition_send_at = time.monotonic()
        success = result == 1
        reason_codes = [] if success else ["CONDITION_SEND_FAILED"]
        if not success:
            self._add_condition_reason("CONDITION_SEND_FAILED")
        self._record_condition_send_result(
            resolved,
            result_code=result,
            success=success,
            reason_codes=reason_codes,
        )
        self.emit(
            "gateway_log",
            {
                "message": "configured condition profile send requested",
                "condition_profile": resolved.to_dict(),
                "condition_name": resolved.condition_name,
                "condition_index": resolved.condition_index,
                "screen_no": screen_no,
                "result_code": result,
                "success": success,
                "reason_codes": reason_codes,
            },
        )

    def _schedule_delayed(self, delay_sec: float, callback: Callable[[], None]) -> None:
        if self.schedule_delayed is not None:
            self.schedule_delayed(delay_sec, callback)
            return
        try:
            from PyQt5.QtCore import QTimer

            QTimer.singleShot(max(int(delay_sec * 1000), 0), callback)
        except Exception:
            # No Qt available (mock/test runtime without an injected scheduler):
            # run inline rather than blocking or dropping the queued sends.
            callback()

    def _resolve_condition_profiles(
        self,
        conditions: list[dict[str, Any]],
    ) -> tuple[ConditionProfile, ...]:
        resolved: list[ConditionProfile] = []
        for offset, profile in enumerate(self._condition_profiles):
            condition_name = profile.condition_name
            condition_index = profile.condition_index
            if condition_index is None and condition_name:
                for condition in conditions:
                    if str(condition.get("name") or "") == condition_name:
                        condition_index = int(condition.get("index") or 0)
                        break
            if condition_name and condition_index is None:
                self._add_condition_reason("CONDITION_PROFILE_NAME_NOT_FOUND")
            if not condition_name and condition_index is not None:
                for condition in conditions:
                    if int(condition.get("index") or -1) == condition_index:
                        condition_name = str(condition.get("name") or "")
                        break
            screen_no = profile.screen_no or f"{7600 + offset:04d}"
            resolved.append(
                profile.with_resolution(
                    condition_name=condition_name,
                    condition_index=condition_index,
                    screen_no=screen_no,
                )
            )
        return tuple(resolved)

    def _register_condition_profile_mapping(self, profile: ConditionProfile) -> None:
        if profile.screen_no:
            self._condition_profile_by_screen[profile.screen_no] = profile
        if profile.condition_index is not None:
            self._condition_profile_by_key[
                (profile.condition_name, int(profile.condition_index))
            ] = profile

    def _profile_for_condition_event(
        self,
        *,
        condition_name: str,
        condition_index: int,
        screen_no: str | None = None,
    ) -> ConditionProfile | None:
        screen = str(screen_no or "").strip()
        if screen and screen in self._condition_profile_by_screen:
            return self._condition_profile_by_screen[screen]
        key = (str(condition_name or "").strip(), int(condition_index))
        if key in self._condition_profile_by_key:
            return self._condition_profile_by_key[key]
        for profile in self._condition_profiles:
            if profile.condition_index is not None and int(profile.condition_index) == int(
                condition_index
            ):
                return profile
            if profile.condition_name == key[0]:
                return profile
        return None

    def _condition_profile_metadata(
        self,
        *,
        profile: ConditionProfile | None,
        callback_source: str,
        screen_no: str | None,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "source_event": callback_source,
            "callback_source": callback_source,
            "screen_no": str(screen_no or ""),
        }
        if profile is None:
            return metadata
        session = self._condition_session_profile()
        metadata.update(
            {
                "sensor_evidence": True,
                "not_buy_signal": True,
                "condition_profile_id": profile.profile_id,
                "condition_role": profile.role.value,
                "condition_priority": profile.priority,
                "condition_ttl_sec": profile.ttl_sec,
                "condition_session_profile": session.value,
                "regular_session_seed_only": (session is ConditionSessionProfile.PREOPEN_NXT),
                "condition_profile": normalize_condition_profile_payload(profile),
            }
        )
        return metadata

    def _record_condition_send_result(
        self,
        profile: ConditionProfile,
        *,
        result_code: int,
        success: bool,
        reason_codes: list[str],
        error: str = "",
    ) -> None:
        self._condition_send_results.append(
            {
                "profile_id": profile.profile_id,
                "condition_name": profile.condition_name,
                "condition_index": profile.condition_index,
                "screen_no": profile.screen_no,
                "role": profile.role.value,
                "result_code": int(result_code),
                "success": bool(success),
                "reason_codes": list(reason_codes),
                "error": error,
                "sent_at": datetime_to_wire(utc_now()),
            }
        )

    def _add_condition_reason(self, reason_code: str) -> None:
        reason = str(reason_code or "").strip().upper()
        if reason and reason not in self._condition_reason_codes:
            self._condition_reason_codes.append(reason)

    def _register_initial_realtime(self) -> None:
        if self.config.realtime_codes:
            self.register_realtime_codes(self.config.realtime_codes)

    def _register_initial_market_index_realtime(self) -> None:
        if not self.config.market_index_enabled:
            return
        if not self.config.market_index_realtime_enabled:
            self.emit(
                "gateway_log",
                {
                    "message": "market index realtime disabled",
                    "market_index_enabled": True,
                    "market_index_realtime_enabled": False,
                    "market_index_tr_bootstrap_enabled": bool(
                        self.config.market_index_tr_bootstrap_enabled
                    ),
                },
            )
            return
        codes = _ordered_unique_market_index_runtime_codes(self.config.market_index_codes)
        if not codes:
            return
        register = getattr(self.client, "register_market_index_realtime", None)
        if not callable(register):
            self.emit(
                "gateway_error",
                {
                    "message": "MARKET_INDEX_REALTIME_REGISTER_UNSUPPORTED",
                    "reason_codes": ["MARKET_INDEX_CLIENT_METHOD_MISSING"],
                },
            )
            return
        try:
            register(codes, screen_no=str(self.config.market_index_screen_no or "5700"))
        except Exception as exc:
            self._last_error = str(exc)
            self._latest_market_index_registration_result = {
                "success": False,
                "reason_codes": ["INDEX_REGISTER_ERROR"],
                "error": str(exc),
                "index_codes": list(codes),
            }
            self.emit(
                "gateway_error",
                {
                    "message": f"MARKET_INDEX_REALTIME_REGISTER_FAILED:{exc}",
                    "reason_codes": ["INDEX_REGISTER_ERROR"],
                    "index_codes": list(codes),
                },
            )
            return
        self._market_index_registered_codes.update(codes)

    def _emit_market_symbols(self) -> None:
        markets = []
        for market_code, market_name in (("0", "KOSPI"), ("10", "KOSDAQ")):
            try:
                codes = [
                    normalize_code(code)
                    for code in self.client.get_code_list_by_market(market_code)
                ]
            except Exception as exc:
                self.emit(
                    "gateway_error",
                    {"message": f"MARKET_SYMBOLS_LOAD_FAILED:{market_name}:{exc}"},
                )
                continue
            if codes:
                markets.append(
                    {"market_code": market_code, "market": market_name, "symbols": codes}
                )
        if markets:
            self.emit("market_symbols", {"source": "kiwoom_code_list", "markets": markets})

    def _broker_mode_payload(self, *, logged_in: bool) -> dict[str, str]:
        if not logged_in:
            return {
                "broker_name": "KIWOOM",
                "broker_env": "UNKNOWN",
                "server_mode": "UNKNOWN",
                "account_mode": "UNKNOWN",
                "server_gubun": "",
            }
        server_gubun = self._server_gubun()
        broker_env = broker_env_from_server_gubun(server_gubun)
        return {
            "broker_name": "KIWOOM",
            "broker_env": broker_env,
            "server_mode": broker_env,
            "account_mode": broker_env,
            "server_gubun": server_gubun,
        }

    def _accounts(self) -> list[str]:
        try:
            return list(self.client.get_accounts() or [])
        except Exception:
            return []

    def _conditions(self) -> list[ConditionInfo]:
        try:
            return list(self.client.condition_name_list() or [])
        except Exception:
            return []

    def _server_gubun(self) -> str:
        getter = getattr(self.client, "get_server_gubun", None)
        if not callable(getter):
            return ""
        try:
            return str(getter() or "").strip()
        except Exception:
            return ""

    def _code_name(self, code: str) -> str:
        try:
            return str(self.client.get_code_name(normalize_code(code)) or "")
        except Exception:
            return normalize_code(code)

    def _observe_real_type(self, payload: Mapping[str, Any]) -> None:
        metadata = payload.get("metadata")
        if not isinstance(metadata, Mapping):
            return
        real_type = str(metadata.get("real_type") or "").strip() or "UNKNOWN"
        self._real_type_counts[real_type] = self._real_type_counts.get(real_type, 0) + 1

    def _condition_callback_health(self) -> str:
        if self._latest_condition_ver_callback_at is not None:
            return "CALLBACK_ACTIVE"
        if self._condition_load_state == "LOADING":
            return "LOADING_WAITING_CALLBACK"
        if self._condition_load_state == "CALLBACK_TIMEOUT":
            return self._callback_suspect_reason_code()
        return self._condition_load_state

    def _callback_suspect_reason_code(self) -> str:
        if not self.config.core_io_enabled:
            return "ACTIVE_X_CALLBACK_SUSPECTED"
        if self._core_io_on_main_thread_observed():
            return "CORE_IO_BLOCKING_SUSPECTED"
        return "ACTIVE_X_CALLBACK_SUSPECTED"

    def _core_io_on_main_thread_observed(self) -> bool:
        if self.config.core_io_worker_enabled or self._core_worker is not None:
            return False
        if not self.config.core_io_enabled:
            return False
        return self._posted_count > 0 or self._polled_count > 0

    def _realtime_subscription_health(self) -> str:
        if self._realtime_parse_error_count and not self._parsed_price_tick_count:
            return "PARSE_ERROR"
        if not self._registered_realtime_codes and self._realtime_registration_requested_count <= 0:
            return "NOT_REQUESTED"
        if self._realtime_callback_count > 0:
            return "CALLBACK_ACTIVE"
        if self._last_realtime_registration_at is None:
            return "REGISTERED_WAITING_CALLBACK"
        age_sec = (utc_now() - self._last_realtime_registration_at).total_seconds()
        timeout_sec = max(float(self.config.realtime_callback_timeout_sec), 1.0)
        if age_sec >= timeout_sec:
            if not self.config.core_io_enabled:
                return "ACTIVE_X_CALLBACK_SUSPECTED"
            if self._core_io_on_main_thread_observed():
                return "CORE_IO_BLOCKING_SUSPECTED"
            return "CALLBACK_TIMEOUT"
        return "REGISTERED_WAITING_CALLBACK"

    def _market_index_adapter_health(self) -> str:
        if not self.config.market_index_enabled:
            return "DISABLED"
        if (
            self.config.market_index_tr_bootstrap_enabled
            and not self.config.market_index_realtime_enabled
        ):
            return "TR_BOOTSTRAP_NOT_IMPLEMENTED"
        if not self.config.market_index_realtime_enabled:
            return "REALTIME_DISABLED"
        if self._market_index_parse_error_count and not self._parsed_market_index_tick_count:
            return "PARSE_ERROR"
        if self._parsed_market_index_tick_count > 0:
            return "CALLBACK_ACTIVE"
        if self._market_index_callback_count > 0:
            return "CALLBACK_ACTIVE_WAITING_VALID_TICK"
        if self._market_index_registered_codes:
            return "REGISTERED_WAITING_CALLBACK"
        return "ENABLED_NOT_REGISTERED"

    def _realtime_exchange(self) -> str:
        return normalize_realtime_exchange(self.config.realtime_exchange)

    def _condition_session_profile(self) -> ConditionSessionProfile:
        return current_condition_session_profile()

    def _runtime_realtime_metrics(self) -> dict[str, Any]:
        return {
            "registered_realtime_code_count": len(self._registered_realtime_codes),
            "parsed_price_tick_count": self._parsed_price_tick_count,
            "realtime_parse_error_count": self._realtime_parse_error_count,
            "realtime_callback_count": self._realtime_callback_count,
            "realtime_subscription_health": self._realtime_subscription_health(),
        }

    def _latest_qt_thread_id_for(self, *methods: str) -> str:
        method_set = {str(method) for method in methods}
        for audit in reversed(self._active_x_thread_audit):
            if str(audit.get("method") or "") in method_set:
                return str(audit.get("qt_thread_id") or "")
        return ""


def wire_kiwoom_signals(client: Any, runtime: KiwoomGatewayRuntime) -> None:
    client.active_x_thread_audit.connect(
        lambda payload: runtime.on_active_x_thread_audit(dict(payload))
    )
    drain_audit = getattr(client, "drain_thread_audit_events", None)
    if callable(drain_audit):
        for payload in drain_audit():
            runtime.on_active_x_thread_audit(dict(payload))
    client.connected.connect(
        lambda ok, code, message: runtime.on_connected(bool(ok), int(code), str(message or ""))
    )
    client.price_tick_received.connect(lambda payload: runtime.on_price_tick(dict(payload)))
    client.market_index_tick_received.connect(
        lambda payload: runtime.on_market_index_tick(dict(payload))
    )
    client.quote_received.connect(lambda payload: runtime.on_quote(dict(payload)))
    client.realtime_data_received.connect(
        lambda code, real_type, real_data_present=False: runtime.on_realtime_data(
            code=str(code or ""),
            real_type=str(real_type or ""),
            real_data_present=bool(real_data_present),
        )
    )
    client.realtime_parse_error.connect(
        lambda payload: runtime.on_realtime_parse_error(dict(payload))
    )
    client.realtime_registration_result.connect(
        lambda payload: runtime.on_realtime_registration_result(dict(payload))
    )
    client.message_received.connect(
        lambda message: runtime.emit("gateway_log", {"message": str(message or "")})
    )
    client.condition_load_result.connect(
        lambda success, message="": runtime.on_condition_load_result(
            bool(success), str(message or "")
        )
    )
    client.condition_loaded.connect(lambda conditions: runtime.on_condition_loaded(conditions))
    client.condition_real_received.connect(
        lambda code, event_type, condition_name, condition_index: runtime.on_condition_event(
            code=str(code or ""),
            event_type=str(event_type or ""),
            condition_name=str(condition_name or ""),
            condition_index=int(condition_index or -1),
            source="real_condition",
        )
    )

    def handle_condition_tr(
        screen_no: str,
        code_list: str,
        condition_name: str,
        condition_index: int,
        next_flag: str,
    ) -> None:
        runtime.on_condition_tr_received(
            screen_no=str(screen_no or ""),
            code_list=str(code_list or ""),
            condition_name=str(condition_name or ""),
            condition_index=int(condition_index or -1),
            next_flag=str(next_flag or ""),
        )

    client.condition_tr_received.connect(handle_condition_tr)
    client.chejan_event_received.connect(lambda result: runtime.on_chejan_result(result))


def _mask_account(value: str) -> str:
    text = str(value or "")
    if len(text) <= 4:
        return "*" * len(text)
    return f"{'*' * (len(text) - 4)}{text[-4:]}"


def _command_codes(payload: Mapping[str, Any]) -> list[str]:
    raw_codes = payload.get("codes", payload.get("code", []))
    if isinstance(raw_codes, str):
        candidates = raw_codes.replace(",", ";").split(";")
    elif isinstance(raw_codes, Iterable):
        candidates = [str(code) for code in raw_codes]
    else:
        candidates = []
    return [normalize_code(code) for code in candidates if str(code).strip()]


def _ordered_unique_codes(codes: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if not str(code or "").strip():
            continue
        normalized = normalize_code(code)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _ordered_unique_market_index_runtime_codes(codes: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if not str(code or "").strip():
            continue
        normalized = normalize_market_index_code(code)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _is_market_index_runtime_callback(*, code: str, real_type: str) -> bool:
    try:
        normalize_market_index_code(code)
        return True
    except ValueError:
        return is_market_index_real_type(real_type)


def _is_market_index_parse_error(payload: Mapping[str, Any]) -> bool:
    if payload.get("market_index") is True:
        return True
    if str(payload.get("asset_type") or "") == "market_index":
        return True
    reason = str(payload.get("reason") or "").upper()
    if reason == "INDEX_PARSE_ERROR":
        return True
    reason_codes = payload.get("reason_codes")
    if isinstance(reason_codes, Iterable) and not isinstance(reason_codes, (str, bytes)):
        return "INDEX_PARSE_ERROR" in {str(item).upper() for item in reason_codes}
    return False


def _latest_datetime(*values: datetime | None) -> datetime | None:
    candidates = [value for value in values if value is not None]
    if not candidates:
        return None
    return max(candidates)


def _datetime_or_empty(value: datetime | None) -> str:
    if value is None:
        return ""
    return datetime_to_wire(value)
