from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.runtime.projection_outbox_backlog import (
    build_projection_outbox_backlog_status,
)
from storage.sqlite import initialize_database


def test_non_blocking_shadow_backlog_warns_but_allows_pr11_ready(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-non-blocking.sqlite3")
    old_at = datetime_to_wire(utc_now() - timedelta(hours=1))
    for index in range(3):
        event_id = f"evt_inline_price_{index}"
        _insert_gateway_event(connection, event_id, "price_tick", created_at=old_at)
        _insert_outbox(connection, "market_data", event_id, "price_tick", old_at)
        _insert_price_tick_sample(connection, event_id, created_at=old_at)

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(
            projection_outbox_backlog_warn_pending_count=2,
            projection_outbox_backlog_fail_pending_count=100,
            projection_outbox_backlog_recent_fail_count=100,
            projection_outbox_backlog_condition_event_ready_max_pending=1,
            projection_outbox_backlog_condition_event_ready_recent_max_pending=1,
        ),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.total_pending_count == 3
    assert status.bulk_retire_eligible_count == 3
    assert status.non_blocking_shadow_pending_count == 3
    assert status.blocking_pending_count == 0
    assert status.readiness_status == "WARN"
    assert status.pr11_condition_event_cutover_ready is True
    assert (
        "NON_BLOCKING_SHADOW_BACKLOG_BULK_RETIRE_RECOMMENDED"
        in status.reason_codes
    )
    assert "RUN_BULK_RETIRE_DRY_RUN" in status.operator_actions


def test_condition_event_blocking_pending_fails_pr11_ready(tmp_path) -> None:
    connection = initialize_database(tmp_path / "backlog-condition-blocking.sqlite3")
    old_at = datetime_to_wire(utc_now() - timedelta(hours=1))
    for index in range(3):
        event_id = f"evt_condition_blocking_{index}"
        _insert_gateway_event(
            connection,
            event_id,
            "condition_event",
            created_at=old_at,
            payload={"code": "005930", "action": "ENTER"},
        )
        _insert_outbox(connection, "market_data", event_id, "condition_event", old_at)

    status = build_projection_outbox_backlog_status(
        connection,
        settings=Settings(
            projection_outbox_backlog_fail_pending_count=100,
            projection_outbox_backlog_recent_fail_count=100,
            projection_outbox_backlog_condition_event_ready_max_pending=2,
            projection_outbox_backlog_condition_event_ready_recent_max_pending=100,
        ),
        latest_reconcile=_reconcile("PASS"),
        routing_status=_routing(),
    )
    connection.close()

    assert status.total_pending_count == 3
    assert status.bulk_retire_eligible_count == 0
    assert status.blocking_pending_count == 3
    assert status.condition_event_blocking_pending_count == 3
    assert status.readiness_status == "FAIL"
    assert status.pr11_condition_event_cutover_ready is False
    assert "CONDITION_EVENT_BLOCKING_OUTBOX_BACKLOG" in status.reason_codes


def _insert_gateway_event(
    connection,
    event_id: str,
    event_type: str,
    *,
    created_at: str,
    payload: dict | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO gateway_events (
            event_id, event_type, source, event_ts, received_at, payload_json, status
        )
        VALUES (?, ?, 'test-gateway', ?, ?, ?, 'ACCEPTED')
        """,
        (
            event_id,
            event_type,
            created_at,
            created_at,
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )
    connection.commit()


def _insert_outbox(
    connection,
    projection_name: str,
    event_id: str,
    event_type: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO projection_outbox (
            outbox_id, projection_name, event_id, event_type, status,
            created_at, updated_at, metadata_json
        )
        VALUES (?, ?, ?, ?, 'PENDING', ?, ?, '{}')
        """,
        (
            f"{projection_name}:{event_id}",
            projection_name,
            event_id,
            event_type,
            created_at,
            created_at,
        ),
    )
    connection.commit()


def _insert_price_tick_sample(
    connection,
    event_id: str,
    *,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO market_tick_samples (
            event_id, code, exchange, session, price, cumulative_volume,
            cumulative_trade_value, volume_delta, trade_value_delta,
            execution_strength, event_ts, received_at, source, metadata_json
        )
        VALUES (?, '005930', 'KRX', 'REGULAR', 70000, 1, 70000, 1, 70000,
            100.0, ?, ?, 'test', '{}')
        """,
        (event_id, created_at, created_at),
    )
    connection.commit()


def _reconcile(status: str) -> dict:
    return {"latest_run": {"run_id": "reconcile_test", "status": status}}


def _routing() -> dict:
    return {
        "condition_event_effective_skip_count": 0,
        "invalid_effective_skip_count": 0,
    }
