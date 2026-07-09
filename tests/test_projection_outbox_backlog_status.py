from __future__ import annotations

from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime.projection_outbox_backlog import (
    build_projection_outbox_backlog_status,
)
from storage.sqlite import initialize_database


def test_backlog_empty_is_pass_when_reconcile_pass(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-empty.sqlite3")
    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "PASS"
    assert status.pr11_condition_event_cutover_ready is True
    assert status.total_pending_count == 0
    assert status.no_trading_side_effects is True


def test_old_pending_backlog_warns_without_recent_pressure(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-old.sqlite3")
    old_at = datetime_to_wire(utc_now() - timedelta(hours=2))
    for index in range(3):
        _insert_outbox(connection, f"old_{index}", created_at=old_at)

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(
            projection_outbox_backlog_warn_pending_count=2,
            projection_outbox_backlog_fail_pending_count=100,
        ),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "WARN"
    assert status.recent_pending_count == 0
    assert "OUTBOX_BACKLOG_DRAIN_RECOMMENDED" in status.reason_codes
    assert "RUN_PROJECTION_OUTBOX_BACKLOG_DRAIN" in status.operator_actions


def test_recent_pending_backlog_fails(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-recent.sqlite3")
    for index in range(3):
        _insert_outbox(connection, f"recent_{index}")

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(projection_outbox_backlog_recent_fail_count=2),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "FAIL"
    assert "RECENT_OUTBOX_BACKLOG" in status.reason_codes


def test_condition_event_pending_blocks_pr11_ready(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-condition.sqlite3")
    old_at = datetime_to_wire(utc_now() - timedelta(hours=1))
    for index in range(3):
        _insert_outbox(
            connection,
            f"condition_{index}",
            event_type="condition_event",
            created_at=old_at,
        )

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(
            projection_outbox_backlog_condition_event_ready_max_pending=2,
            projection_outbox_backlog_recent_fail_count=100,
        ),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "FAIL"
    assert status.pr11_condition_event_cutover_ready is False
    assert "CONDITION_EVENT_OUTBOX_BACKLOG" in status.reason_codes


def test_error_and_dead_letter_backlog_fails(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-error.sqlite3")
    _insert_outbox(connection, "error_evt", status="ERROR")
    _insert_outbox(connection, "dead_evt", status="DEAD_LETTER")

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "FAIL"
    assert "PROJECTION_OUTBOX_ERROR" in status.reason_codes
    assert "PROJECTION_OUTBOX_DEAD_LETTER" in status.reason_codes


def test_stale_processing_backlog_fails(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-stale.sqlite3")
    old_at = datetime_to_wire(utc_now() - timedelta(minutes=10))
    _insert_outbox(
        connection,
        "stale_evt",
        status="PROCESSING",
        created_at=old_at,
        locked_at=old_at,
    )

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(projection_outbox_backlog_stale_processing_sec=120),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.readiness_status == "FAIL"
    assert status.stale_processing_count == 1
    assert "STALE_OUTBOX_PROCESSING" in status.reason_codes


def _insert_outbox(
    connection,
    event_id: str,
    *,
    projection_name: str = "market_data",
    event_type: str = "price_tick",
    status: str = "PENDING",
    created_at: str | None = None,
    locked_at: str | None = None,
) -> None:
    wire = created_at or datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id,
            projection_name,
            event_id,
            event_type,
            status,
            created_at,
            updated_at,
            locked_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}')
        """,
        (
            f"{projection_name}:{event_id}",
            projection_name,
            event_id,
            event_type,
            status,
            wire,
            wire,
            locked_at,
        ),
    )
    connection.commit()


def _reconcile(status: str) -> dict:
    return {"latest_run": {"run_id": "reconcile_test", "status": status}}


def _routing() -> dict:
    return {
        "condition_event_effective_skip_count": 0,
        "invalid_effective_skip_count": 0,
    }
