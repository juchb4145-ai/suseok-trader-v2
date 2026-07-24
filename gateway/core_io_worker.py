from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import utc_now

_MAX_POST_RETRY_SLEEP_SEC = 5.0
_CORE_FAST_BATCH_EVENT_TYPES = frozenset({"price_tick", "condition_event"})


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
    latest_poll_error_at: float | None
    latest_post_at: float | None
    thread_id: int | None
    coalesce_after_size: int
    max_buffer_size: int
    consecutive_poll_error_count: int
    batch_size: int
    batch_post_count: int
    latest_batch_size: int
    rejected_event_count: int
    market_event_queue_size: int
    durable_event_queue_size: int
    oldest_event_age_sec: float | None
    oldest_market_event_age_sec: float | None
    consecutive_post_error_count: int = 0


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
        event_batch_size: int = 100,
        market_batch_share: int = 50,
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
        self._event_batch_size = min(max(int(event_batch_size), 1), 200)
        self._market_batch_share = min(
            max(int(market_batch_share), 1),
            self._event_batch_size,
        )
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
        self._latest_poll_error_at: float | None = None
        self._latest_post_at: float | None = None
        self._thread_id: int | None = None
        self._next_post_retry_at = 0.0
        self._next_command_poll_at = 0.0
        self._consecutive_poll_error_count = 0
        self._batch_post_count = 0
        self._latest_batch_size = 0
        self._last_successful_batch_fast: bool | None = None
        self._rejected_event_count = 0
        self._consecutive_post_error_count = 0

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
            if _is_command_critical_event(event):
                self._enqueue_command_critical_event_locked(event)
                self._enforce_buffer_limit_locked()
                self._condition.notify()
                return
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
            latest_poll_error_at = self._latest_poll_error_at
            latest_post_at = self._latest_post_at
            thread_id = self._thread_id
            consecutive_poll_error_count = self._consecutive_poll_error_count
            batch_post_count = self._batch_post_count
            latest_batch_size = self._latest_batch_size
            rejected_event_count = self._rejected_event_count
            consecutive_post_error_count = self._consecutive_post_error_count
            market_events = [
                event for event in self._events if _is_market_refresh_event(event)
            ]
            durable_event_queue_size = sum(
                not _is_droppable_event(event) for event in self._events
            )
            oldest_event_age_sec = _oldest_event_age_sec(self._events)
            oldest_market_event_age_sec = _oldest_event_age_sec(market_events)
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
            latest_poll_error_at=latest_poll_error_at,
            latest_post_at=latest_post_at,
            thread_id=thread_id,
            coalesce_after_size=self._coalesce_after_size,
            max_buffer_size=self._max_buffer_size,
            consecutive_poll_error_count=consecutive_poll_error_count,
            batch_size=self._event_batch_size,
            batch_post_count=batch_post_count,
            latest_batch_size=latest_batch_size,
            rejected_event_count=rejected_event_count,
            market_event_queue_size=len(market_events),
            durable_event_queue_size=durable_event_queue_size,
            oldest_event_age_sec=oldest_event_age_sec,
            oldest_market_event_age_sec=oldest_market_event_age_sec,
            consecutive_post_error_count=consecutive_post_error_count,
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
            if self._command_polling_enabled and time.monotonic() >= self._next_command_poll_at:
                self._poll_commands()
                self._next_command_poll_at = time.monotonic() + self._command_poll_interval_sec
                continue
            post_events = getattr(self._core_client, "post_events", None)
            posted = (
                self._post_next_event_batch(post_events)
                if callable(post_events)
                else self._post_next_event()
            )
            if posted:
                continue
            self._wait_for_work(timeout_sec=0.1)

    def _post_next_event_batch(self, post_events: Any) -> bool:
        now = time.monotonic()
        if now < self._next_post_retry_at:
            return False
        with self._condition:
            events = self._select_event_batch_locked()
        if not events:
            return False
        try:
            response = post_events(events)
        except Exception as exc:
            with self._condition:
                self._last_error = str(exc)
                self._consecutive_post_error_count += 1
                retry_delay_sec = self._post_retry_delay_sec_locked()
            self._next_post_retry_at = time.monotonic() + retry_delay_sec
            return False
        rejected_count = (
            int(response.get("failed_count") or 0)
            if isinstance(response, dict)
            else 0
        )
        with self._condition:
            for event in events:
                self._remove_event_by_identity_locked(event)
            self._posted_count += len(events)
            self._batch_post_count += 1
            self._latest_batch_size = len(events)
            self._last_successful_batch_fast = _is_core_fast_batch_event(events[0])
            self._rejected_event_count += rejected_count
            self._consecutive_post_error_count = 0
            self._latest_post_at = time.monotonic()
            self._last_error = (
                f"Core rejected {rejected_count} event(s) from latest batch"
                if rejected_count
                else ""
            )
        return True

    def _select_event_batch_locked(self) -> list[GatewayEvent]:
        limit = min(self._event_batch_size, len(self._events))
        if limit <= 0:
            return []
        target_fast = self._select_batch_fast_group_locked()
        selected: list[GatewayEvent] = []
        selected_ids: set[int] = set()

        def add_matching(predicate: Any, *, item_limit: int) -> None:
            if item_limit <= 0:
                return
            for event in self._events:
                if len(selected) >= limit or item_limit <= 0:
                    break
                if (
                    id(event) in selected_ids
                    or _is_core_fast_batch_event(event) is not target_fast
                    or not predicate(event)
                ):
                    continue
                selected.append(event)
                selected_ids.add(id(event))
                item_limit -= 1

        add_matching(_is_command_critical_event, item_limit=limit)
        add_matching(
            lambda event: _is_priority_event(event)
            and not _is_command_critical_event(event),
            item_limit=min(16, limit - len(selected)),
        )
        add_matching(
            _is_market_refresh_event,
            item_limit=min(self._market_batch_share, limit - len(selected)),
        )
        add_matching(lambda _: True, item_limit=limit - len(selected))
        return selected

    def _select_batch_fast_group_locked(self) -> bool:
        fast_available = any(_is_core_fast_batch_event(event) for event in self._events)
        non_fast_available = any(
            not _is_core_fast_batch_event(event) for event in self._events
        )
        if not fast_available:
            return False
        if not non_fast_available:
            return True

        command_critical = next(
            (event for event in self._events if _is_command_critical_event(event)),
            None,
        )
        if command_critical is not None:
            return _is_core_fast_batch_event(command_critical)
        if self._last_successful_batch_fast is not None:
            return not self._last_successful_batch_fast

        preferred = next(
            (event for event in self._events if _is_priority_event(event)),
            None,
        )
        if preferred is None:
            preferred = next(
                (event for event in self._events if _is_market_refresh_event(event)),
                self._events[0],
            )
        return _is_core_fast_batch_event(preferred)

    def _remove_event_by_identity_locked(self, event: GatewayEvent) -> None:
        for index, queued_event in enumerate(self._events):
            if queued_event is event:
                del self._events[index]
                return

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
                self._consecutive_post_error_count += 1
                retry_delay_sec = self._post_retry_delay_sec_locked()
            self._next_post_retry_at = time.monotonic() + retry_delay_sec
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
            self._consecutive_post_error_count = 0
            self._latest_post_at = time.monotonic()
            self._last_error = ""
        return True

    def _post_retry_delay_sec_locked(self) -> float:
        exponent = min(max(self._consecutive_post_error_count - 1, 0), 10)
        return min(
            self._retry_sleep_sec * (2**exponent),
            _MAX_POST_RETRY_SLEEP_SEC,
        )

    def _enqueue_priority_event_locked(self, event: GatewayEvent) -> None:
        key = _coalescing_key(event)
        if key is not None:
            for index, queued_event in enumerate(self._events):
                if _coalescing_key(queued_event) == key:
                    del self._events[index]
                    self._coalesced_count += 1
                    break
        self._insert_after_command_critical_locked(event)

    def _enqueue_command_critical_event_locked(self, event: GatewayEvent) -> None:
        self._insert_after_command_critical_locked(event)

    def _insert_after_command_critical_locked(self, event: GatewayEvent) -> None:
        insert_at = 0
        for index, queued_event in enumerate(self._events):
            if _is_command_critical_event(queued_event):
                insert_at = index + 1
        self._events.insert(insert_at, event)

    def _poll_commands(self) -> None:
        try:
            commands = self._core_client.poll_commands(
                limit=self._command_limit,
                wait_sec=self._command_wait_sec,
            )
        except Exception as exc:
            with self._condition:
                self._last_error = str(exc)
                self._latest_poll_error_at = time.monotonic()
                self._consecutive_poll_error_count += 1
            self._stop_event.wait(self._retry_sleep_sec)
            return
        with self._condition:
            self._poll_count += 1
            self._latest_poll_at = time.monotonic()
            self._consecutive_poll_error_count = 0
            self._last_error = ""
        for command in commands:
            self._commands.put(command)

    def _wait_for_work(self, *, timeout_sec: float) -> None:
        with self._condition:
            now = time.monotonic()
            deadlines: list[float] = []
            if self._events and self._next_post_retry_at <= now:
                return
            if self._events:
                deadlines.append(self._next_post_retry_at)
            if self._command_polling_enabled:
                if self._next_command_poll_at <= now:
                    return
                deadlines.append(self._next_command_poll_at)
            wait_sec = (
                min(deadlines) - now
                if deadlines
                else max(float(timeout_sec), 0.01)
            )
            self._condition.wait(timeout=max(wait_sec, 0.01))


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


def _is_core_fast_batch_event(event: GatewayEvent) -> bool:
    return (
        str(event.event_type or "").strip().lower()
        in _CORE_FAST_BATCH_EVENT_TYPES
    )


def _is_priority_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in {
        "heartbeat",
        "market_index_tick",
        "tr_response",
    }


def _is_market_refresh_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in {
        "price_tick",
        "quote_tick",
        "market_index_tick",
    }


def _oldest_event_age_sec(events: Any) -> float | None:
    event_list = list(events)
    if not event_list:
        return None
    now = utc_now()
    return max(
        max((now - event.ts).total_seconds(), 0.0)
        for event in event_list
    )


_COMMAND_CRITICAL_EVENT_TYPES = {
    "command_started",
    "command_ack",
    "command_failed",
    "rate_limited",
    "execution_event",
    "order_pre_ack",
    "order_broker_unconfirmed",
    "kiwoom_order_chejan",
    "kiwoom_balance_chejan",
    "kiwoom_special_chejan",
}


_DROPPABLE_EVENT_TYPES = {
    "heartbeat",
    "price_tick",
    "quote_tick",
    "market_index_tick",
    "gateway_log",
}

_DROP_PROTECTED_EVENT_TYPES = {
    "command_started",
    "command_ack",
    "command_failed",
    "rate_limited",
    "tr_response",
    "execution_event",
    "order_pre_ack",
    "order_broker_unconfirmed",
    "kiwoom_order_chejan",
    "kiwoom_balance_chejan",
    "kiwoom_special_chejan",
}


def _is_command_critical_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in _COMMAND_CRITICAL_EVENT_TYPES


def _is_droppable_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in _DROPPABLE_EVENT_TYPES


def _is_drop_protected_event(event: GatewayEvent) -> bool:
    return str(event.event_type or "").strip().lower() in _DROP_PROTECTED_EVENT_TYPES
