from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent


@dataclass(frozen=True)
class CoreIoWorkerSnapshot:
    running: bool
    event_queue_size: int
    command_queue_size: int
    posted_count: int
    poll_count: int
    coalesced_count: int
    dropped_count: int
    last_error: str
    latest_poll_at: float | None
    latest_post_at: float | None
    thread_id: int | None
    coalesce_after_size: int
    max_buffer_size: int


class CoreIoWorker:
    """Move blocking Core HTTP work off the Kiwoom/Qt main thread."""

    def __init__(
        self,
        *,
        core_client: Any,
        command_limit: int,
        command_wait_sec: float,
        command_polling_enabled: bool = True,
        event_posting_enabled: bool = True,
        retry_sleep_sec: float = 0.2,
        coalesce_after_size: int = 50,
        command_poll_interval_sec: float = 0.2,
        max_buffer_size: int = 10_000,
    ) -> None:
        self._core_client = core_client
        self._command_limit = max(int(command_limit), 1)
        self._command_wait_sec = max(float(command_wait_sec), 0.0)
        self._command_polling_enabled = bool(command_polling_enabled)
        self._event_posting_enabled = bool(event_posting_enabled)
        self._retry_sleep_sec = max(float(retry_sleep_sec), 0.05)
        self._coalesce_after_size = max(int(coalesce_after_size), 1)
        self._command_poll_interval_sec = max(float(command_poll_interval_sec), 0.05)
        self._max_buffer_size = max(int(max_buffer_size), 1)
        self._events: deque[GatewayEvent] = deque()
        self._commands: Queue[GatewayCommand] = Queue()
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._posted_count = 0
        self._poll_count = 0
        self._coalesced_count = 0
        self._dropped_count = 0
        self._last_error = ""
        self._latest_poll_at: float | None = None
        self._latest_post_at: float | None = None
        self._thread_id: int | None = None
        self._next_post_retry_at = 0.0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="kiwoom-core-io-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout_sec: float = 2.0) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=max(float(timeout_sec), 0.1))

    def enqueue_event(self, event: GatewayEvent) -> None:
        if not self._event_posting_enabled:
            return
        with self._condition:
            if _is_priority_event(event):
                self._enqueue_priority_event_locked(event)
                self._enforce_buffer_limit_locked()
                self._condition.notify()
                return
            if len(self._events) >= self._coalesce_after_size:
                if self._coalesce_event_locked(event):
                    self._condition.notify()
                    return
            self._events.append(event)
            self._enforce_buffer_limit_locked()
            self._condition.notify()

    def _enforce_buffer_limit_locked(self) -> None:
        # Bound RAM growth while Core is unreachable (the gateway is a 32-bit
        # process). Drop oldest low-value events first; order-critical events
        # are never dropped even if that lets the buffer exceed the cap.
        while len(self._events) > self._max_buffer_size:
            drop_index = None
            for index, queued_event in enumerate(self._events):
                if _is_droppable_event(queued_event):
                    drop_index = index
                    break
            if drop_index is None:
                for index, queued_event in enumerate(self._events):
                    if not _is_drop_protected_event(queued_event):
                        drop_index = index
                        break
            if drop_index is None:
                return
            del self._events[drop_index]
            self._dropped_count += 1

    def drain_commands(self, *, limit: int = 100) -> list[GatewayCommand]:
        commands: list[GatewayCommand] = []
        for _ in range(max(int(limit), 1)):
            try:
                commands.append(self._commands.get_nowait())
            except Empty:
                break
        return commands

    def snapshot(self) -> CoreIoWorkerSnapshot:
        with self._condition:
            event_queue_size = len(self._events)
            posted_count = self._posted_count
            poll_count = self._poll_count
            coalesced_count = self._coalesced_count
            dropped_count = self._dropped_count
            last_error = self._last_error
            latest_poll_at = self._latest_poll_at
            latest_post_at = self._latest_post_at
            thread_id = self._thread_id
        return CoreIoWorkerSnapshot(
            running=self._thread is not None and self._thread.is_alive(),
            event_queue_size=event_queue_size,
            command_queue_size=self._commands.qsize(),
            posted_count=posted_count,
            poll_count=poll_count,
            coalesced_count=coalesced_count,
            dropped_count=dropped_count,
            last_error=last_error,
            latest_poll_at=latest_poll_at,
            latest_post_at=latest_post_at,
            thread_id=thread_id,
            coalesce_after_size=self._coalesce_after_size,
            max_buffer_size=self._max_buffer_size,
        )

    def _coalesce_event_locked(self, event: GatewayEvent) -> bool:
        key = _coalescing_key(event)
        if key is None:
            return False
        for index, queued_event in enumerate(self._events):
            if _coalescing_key(queued_event) == key:
                self._events[index] = event
                self._coalesced_count += 1
                return True
        return False

    def _run(self) -> None:
        with self._condition:
            self._thread_id = threading.get_ident()
        while not self._stop_event.is_set():
            posted = self._post_next_event()
            if posted:
                continue
            if self._command_polling_enabled:
                self._poll_commands()
                self._wait_for_work(timeout_sec=self._command_poll_interval_sec)
                continue
            self._wait_for_work(timeout_sec=0.1)

    def _post_next_event(self) -> bool:
        if not self._event_posting_enabled:
            return False
        now = time.monotonic()
        if now < self._next_post_retry_at:
            return False
        with self._condition:
            event = self._events[0] if self._events else None
        if event is None:
            return False
        try:
            self._core_client.post_event(event)
        except Exception as exc:
            with self._condition:
                self._last_error = str(exc)
            self._next_post_retry_at = time.monotonic() + self._retry_sleep_sec
            return False
        with self._condition:
            if self._events and self._events[0] is event:
                self._events.popleft()
            else:
                try:
                    self._events.remove(event)
                except ValueError:
                    pass
            self._posted_count += 1
            self._latest_post_at = time.monotonic()
            self._last_error = ""
        return True

    def _enqueue_priority_event_locked(self, event: GatewayEvent) -> None:
        key = _coalescing_key(event)
        if key is not None:
            for index, queued_event in enumerate(self._events):
                if _coalescing_key(queued_event) == key:
                    del self._events[index]
                    self._coalesced_count += 1
                    break
        self._events.appendleft(event)

    def _poll_commands(self) -> None:
        try:
            commands = self._core_client.poll_commands(
                limit=self._command_limit,
                wait_sec=self._command_wait_sec,
            )
        except Exception as exc:
            with self._condition:
                self._last_error = str(exc)
            self._stop_event.wait(self._retry_sleep_sec)
            return
        with self._condition:
            self._poll_count += 1
            self._latest_poll_at = time.monotonic()
            self._last_error = ""
        for command in commands:
            self._commands.put(command)

    def _wait_for_work(self, *, timeout_sec: float) -> None:
        with self._condition:
            if self._events:
                return
            self._condition.wait(timeout=max(float(timeout_sec), 0.01))


def _coalescing_key(event: GatewayEvent) -> tuple[str, str] | None:
    event_type = str(event.event_type or "").strip().lower()
    payload = event.payload
    if event_type == "heartbeat":
        return (event_type, str(event.source or ""))
    if event_type in {"price_tick", "quote_tick"}:
        code = str(payload.get("code") or "").strip()
        return (event_type, code) if code else None
    if event_type == "market_index_tick":
        index_code = str(payload.get("index_code") or payload.get("code") or "").strip()
        return (event_type, index_code) if index_code else None
    return None


def _is_priority_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() == "heartbeat"


_DROPPABLE_EVENT_TYPES = {
    "heartbeat",
    "price_tick",
    "quote_tick",
    "market_index_tick",
    "gateway_log",
}

_DROP_PROTECTED_EVENT_TYPES = {
    "command_ack",
    "command_failed",
    "execution_event",
}


def _is_droppable_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in _DROPPABLE_EVENT_TYPES


def _is_drop_protected_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in _DROP_PROTECTED_EVENT_TYPES
