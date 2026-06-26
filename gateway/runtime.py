from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now

from gateway.command_handlers import GatewayCommandHandler
from gateway.core_client import CoreClient
from gateway.event_factory import make_heartbeat_event
from gateway.settings import GatewaySettings
from gateway.transport import GatewayTransportError

ActivityCallback = Callable[[str, Mapping[str, Any]], None]


class GatewayRuntime:
    def __init__(
        self,
        *,
        settings: GatewaySettings,
        client: CoreClient | None = None,
        command_handler: GatewayCommandHandler | None = None,
        on_activity: ActivityCallback | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or CoreClient(
            core_url=settings.core_url,
            token=settings.core_token,
            timeout_sec=settings.event_timeout_sec,
        )
        self.command_handler = command_handler or GatewayCommandHandler(source=settings.source)
        self.on_activity = on_activity
        self._event_queue: deque[GatewayEvent] = deque()
        self._stop_requested = False
        self._running = False
        self._heartbeat_sequence = 0
        self._emitted_count = 0
        self._posted_count = 0
        self._polled_count = 0
        self._handled_command_count = 0
        self._last_error: str | None = None
        self._last_event_posted_at: str | None = None
        self._last_heartbeat_sent_at: str | None = None
        self._last_command_poll_at: str | None = None

    def start_once(self) -> dict[str, Any]:
        self.emit_heartbeat()
        self.flush_events()
        self.poll_and_handle_commands()
        return self.snapshot()

    def run_forever(self, *, max_iterations: int | None = None) -> dict[str, Any]:
        self._running = True
        iteration = 0
        next_heartbeat_at = 0.0
        try:
            while not self._stop_requested:
                now = time.monotonic()
                if now >= next_heartbeat_at:
                    self.emit_heartbeat()
                    next_heartbeat_at = now + self.settings.heartbeat_interval_sec
                self.flush_events()
                self.poll_and_handle_commands()

                iteration += 1
                if max_iterations is not None and iteration >= max_iterations:
                    break
                time.sleep(self.settings.poll_interval_sec)
        finally:
            self._running = False
            self.client.close()
        return self.snapshot()

    def stop(self) -> None:
        self._stop_requested = True

    def emit_event(self, event: GatewayEvent) -> None:
        self._event_queue.append(event)
        self._emitted_count += 1

    def emit_heartbeat(self) -> None:
        self._heartbeat_sequence += 1
        self.emit_event(
            make_heartbeat_event(
                source=self.settings.source,
                sequence=self._heartbeat_sequence,
            )
        )
        self._last_heartbeat_sent_at = datetime_to_wire(utc_now())

    def flush_events(self) -> None:
        while self._event_queue:
            event = self._event_queue[0]
            try:
                result = self.client.post_event(event)
            except GatewayTransportError as exc:
                self._record_error(str(exc))
                self._record_activity(
                    "event_post_failed",
                    {
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "error": str(exc),
                    },
                )
                break
            self._event_queue.popleft()
            self._posted_count += 1
            self._last_event_posted_at = datetime_to_wire(utc_now())
            self._record_activity(
                "event_posted",
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "result": result,
                },
            )

    def poll_and_handle_commands(self) -> list[GatewayEvent]:
        try:
            commands = self.client.poll_commands(
                limit=self.settings.command_limit,
                wait_sec=self.settings.command_wait_sec,
            )
        except GatewayTransportError as exc:
            self._record_error(str(exc))
            self._record_activity("command_poll_failed", {"error": str(exc)})
            return []

        self._polled_count += 1
        self._last_command_poll_at = datetime_to_wire(utc_now())
        self._record_activity("commands_polled", {"count": len(commands)})

        emitted_events: list[GatewayEvent] = []
        for command in commands:
            events = self.command_handler.handle(command)
            self._handled_command_count += 1
            emitted_events.extend(events)
            for event in events:
                self.emit_event(event)
            self._record_activity(
                "command_handled",
                {
                    "command_id": command.command_id,
                    "command_type": command.command_type,
                    "event_count": len(events),
                },
            )
        self.flush_events()
        return emitted_events

    def snapshot(self) -> dict[str, Any]:
        return {
            "source": self.settings.source,
            "running": self._running,
            "stop_requested": self._stop_requested,
            "queued_event_count": len(self._event_queue),
            "emitted_event_count": self._emitted_count,
            "posted_event_count": self._posted_count,
            "poll_count": self._polled_count,
            "handled_command_count": self._handled_command_count,
            "subscriptions": sorted(self.command_handler.subscriptions),
            "last_error": self._last_error,
            "last_heartbeat_sent_at": self._last_heartbeat_sent_at,
            "last_event_posted_at": self._last_event_posted_at,
            "last_command_poll_at": self._last_command_poll_at,
        }

    def _record_error(self, error_message: str) -> None:
        self._last_error = error_message

    def _record_activity(self, event_name: str, payload: Mapping[str, Any]) -> None:
        if self.on_activity is not None:
            self.on_activity(event_name, payload)

