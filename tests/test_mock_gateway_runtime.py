from __future__ import annotations

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from gateway.mock_runtime import MockGatewayRuntime
from gateway.settings import GatewaySettings
from gateway.transport import GatewayTransportError


class FakeCoreClient:
    def __init__(
        self,
        *,
        commands: list[GatewayCommand] | None = None,
        fail_post: bool = False,
    ) -> None:
        self.commands = commands or []
        self.fail_post = fail_post
        self.events: list[GatewayEvent] = []
        self.closed = False

    def post_event(self, event: GatewayEvent) -> dict[str, object]:
        if self.fail_post:
            raise GatewayTransportError("Core is unavailable")
        self.events.append(event)
        return {"accepted": True, "event_id": event.event_id}

    def poll_commands(self, *, limit: int = 20, wait_sec: float = 0) -> list[GatewayCommand]:
        commands = self.commands
        self.commands = []
        return commands

    def close(self) -> None:
        self.closed = True


def fast_settings() -> GatewaySettings:
    return GatewaySettings(
        poll_interval_sec=0.0,
        heartbeat_interval_sec=0.0,
        command_wait_sec=0.0,
        mock_price_tick_interval_sec=0.0,
    )


def test_once_flow_posts_heartbeat_price_tick_and_condition_without_execution_event() -> None:
    fake_client = FakeCoreClient()
    runtime = MockGatewayRuntime(settings=fast_settings(), client=fake_client)

    snapshot = runtime.start_once()

    event_types = [event.event_type for event in fake_client.events]
    assert event_types[:3] == ["heartbeat", "price_tick", "condition_event"]
    assert "execution_event" not in event_types
    assert snapshot["posted_event_count"] == 3


def test_runtime_polls_and_handles_allowed_command() -> None:
    command = GatewayCommand(
        command_id="cmd_runtime_tr",
        command_type="request_tr",
        source="core",
        payload={"request_id": "tr_runtime", "tr_code": "OPT10001"},
    )
    fake_client = FakeCoreClient(commands=[command])
    runtime = MockGatewayRuntime(settings=fast_settings(), client=fake_client)

    snapshot = runtime.start_once()

    event_types = [event.event_type for event in fake_client.events]
    assert "command_started" in event_types
    assert "tr_response" in event_types
    assert "command_ack" in event_types
    assert snapshot["handled_command_count"] == 1


def test_runtime_records_transport_error_without_crashing() -> None:
    runtime = MockGatewayRuntime(settings=fast_settings(), client=FakeCoreClient(fail_post=True))

    snapshot = runtime.start_once()

    assert "Core is unavailable" in snapshot["last_error"]
    assert snapshot["queued_event_count"] >= 1


def test_runtime_can_stop_gracefully() -> None:
    fake_client = FakeCoreClient()
    runtime = MockGatewayRuntime(settings=fast_settings(), client=fake_client)

    runtime.stop()
    snapshot = runtime.run_forever(max_iterations=1)

    assert snapshot["stop_requested"] is True
    assert snapshot["running"] is False
    assert fake_client.closed is True

