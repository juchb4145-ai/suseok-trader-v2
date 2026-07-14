from __future__ import annotations

from domain.broker.commands import GatewayCommand
from gateway.command_handlers import GatewayCommandHandler


def make_command(command_type: str, payload: dict[str, object] | None = None) -> GatewayCommand:
    return GatewayCommand(
        command_id=f"cmd_{command_type}",
        command_type=command_type,
        source="core",
        payload=payload or {},
    )


def test_heartbeat_request_emits_started_and_ack() -> None:
    events = GatewayCommandHandler().handle(make_command("heartbeat_request"))

    assert [event.event_type for event in events] == ["command_started", "command_ack"]


def test_request_tr_emits_started_tr_response_and_ack() -> None:
    events = GatewayCommandHandler().handle(
        make_command(
            "request_tr",
            {
                "request_id": "tr_cmd",
                "tr_code": "OPT10001",
                "request_name": "stock_basic",
                "code": "005930",
                "metadata": {"projection_source": "test_tr_metadata"},
            },
        )
    )

    assert [event.event_type for event in events] == [
        "command_started",
        "tr_response",
        "command_ack",
    ]
    assert events[1].payload["request_id"] == "tr_cmd"
    assert events[1].payload["metadata"]["projection_source"] == "test_tr_metadata"


def test_register_realtime_adds_code_to_subscription_set() -> None:
    handler = GatewayCommandHandler()

    events = handler.handle(make_command("register_realtime", {"code": "000660"}))

    assert "000660" in handler.subscriptions
    assert [event.event_type for event in events] == ["command_started", "command_ack"]


def test_remove_realtime_discards_code_from_subscription_set() -> None:
    handler = GatewayCommandHandler()
    handler.handle(make_command("register_realtime", {"code": "000660"}))

    events = handler.handle(make_command("remove_realtime", {"code": "000660"}))

    assert "000660" not in handler.subscriptions
    assert [event.event_type for event in events] == ["command_started", "command_ack"]


def test_load_conditions_emits_condition_load_result_and_ack() -> None:
    events = GatewayCommandHandler().handle(make_command("load_conditions"))

    assert [event.event_type for event in events] == [
        "command_started",
        "condition_load_result",
        "command_ack",
    ]


def test_send_condition_emits_condition_event_and_ack() -> None:
    events = GatewayCommandHandler().handle(
        make_command("send_condition", {"code": "005930", "action": "ENTER"})
    )

    assert [event.event_type for event in events] == [
        "command_started",
        "condition_event",
        "command_ack",
    ]


def test_stop_condition_emits_started_and_ack() -> None:
    events = GatewayCommandHandler().handle(make_command("stop_condition"))

    assert [event.event_type for event in events] == ["command_started", "command_ack"]


def test_forbidden_order_command_emits_failed_without_side_effect() -> None:
    handler = GatewayCommandHandler()

    events = handler.handle(make_command("send_order", {"code": "005930"}))

    assert [event.event_type for event in events] == ["command_failed"]
    assert "order command disabled" in events[0].payload["error_message"]
    assert handler.subscriptions == set()

