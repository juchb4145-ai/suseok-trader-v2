from __future__ import annotations

import time
from typing import Any

from gateway.core_client import CoreClient
from gateway.event_factory import make_condition_event, make_price_tick_event
from gateway.runtime import ActivityCallback, GatewayRuntime
from gateway.settings import GatewaySettings


class MockGatewayRuntime(GatewayRuntime):
    def __init__(
        self,
        *,
        settings: GatewaySettings,
        client: CoreClient | None = None,
        code: str = "005930",
        name: str = "삼성전자",
        emit_condition_events: bool = True,
        on_activity: ActivityCallback | None = None,
    ) -> None:
        super().__init__(settings=settings, client=client, on_activity=on_activity)
        self.code = code
        self.name = name
        self.emit_condition_events = emit_condition_events
        self._tick_sequence = 0
        self._condition_sent = False

    def start_once(self) -> dict[str, Any]:
        self.emit_heartbeat()
        self.emit_event(self._make_next_price_tick())
        if self.emit_condition_events:
            self.emit_event(self._make_condition_event())
            self._condition_sent = True
        self.flush_events()
        self.poll_and_handle_commands()
        return self.snapshot()

    def run_forever(self, *, max_iterations: int | None = None) -> dict[str, Any]:
        self._running = True
        iteration = 0
        next_heartbeat_at = 0.0
        next_tick_at = 0.0
        try:
            while not self._stop_requested:
                now = time.monotonic()
                if now >= next_heartbeat_at:
                    self.emit_heartbeat()
                    next_heartbeat_at = now + self.settings.heartbeat_interval_sec
                if now >= next_tick_at:
                    self.emit_event(self._make_next_price_tick())
                    next_tick_at = now + self.settings.mock_price_tick_interval_sec
                if self.emit_condition_events and not self._condition_sent:
                    self.emit_event(self._make_condition_event())
                    self._condition_sent = True

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

    def _make_next_price_tick(self):
        self._tick_sequence += 1
        return make_price_tick_event(
            source=self.settings.source,
            code=self.code,
            name=self.name,
            price=70000 + (self._tick_sequence - 1) * 10,
            volume=1000 + self._tick_sequence,
        )

    def _make_condition_event(self):
        return make_condition_event(
            source=self.settings.source,
            code=self.code,
            name=self.name,
            action="ENTER",
            price=70000,
        )

