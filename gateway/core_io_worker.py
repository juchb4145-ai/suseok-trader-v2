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
    last_error: str
    latest_poll_at: float | None
    latest_post_at: float | None
    thread_id: int | None


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
    ) -> None:
        self._core_client = core_client
        self._command_limit = max(int(command_limit), 1)
        self._command_wait_sec = max(float(command_wait_sec), 0.0)
        self._command_polling_enabled = bool(command_polling_enabled)
        self._event_posting_enabled = bool(event_posting_enabled)
        self._retry_sleep_sec = max(float(retry_sleep_sec), 0.05)
        self._events: deque[GatewayEvent] = deque()
        self._commands: Queue[GatewayCommand] = Queue()
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._posted_count = 0
        self._poll_count = 0
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
            self._events.append(event)
            self._condition.notify()

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
            last_error=last_error,
            latest_poll_at=latest_poll_at,
            latest_post_at=latest_post_at,
            thread_id=thread_id,
        )

    def _run(self) -> None:
        with self._condition:
            self._thread_id = threading.get_ident()
        while not self._stop_event.is_set():
            posted = self._post_next_event()
            if posted:
                continue
            if self._command_polling_enabled:
                self._poll_commands()
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
