from __future__ import annotations

import sqlite3

from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime import projection_outbox_worker
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_projection_outbox_worker import _outbox_row, _price_tick_event


def test_projection_outbox_worker_retries_locked_mark_applied(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-lock-retry.sqlite3")
    settings = Settings(
        projection_outbox_shadow_min_age_sec=0,
        operator_sqlite_lock_retry_attempts=2,
        operator_sqlite_lock_retry_base_sleep_sec=0,
        operator_sqlite_lock_retry_max_sleep_sec=0,
    )
    event = _price_tick_event("evt_worker_locked_mark")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    real_mark_applied = projection_outbox_worker.mark_projection_outbox_applied
    calls = {"count": 0}

    def flaky_mark_applied(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_mark_applied(*args, **kwargs)

    monkeypatch.setattr(
        projection_outbox_worker,
        "mark_projection_outbox_applied",
        flaky_mark_applied,
    )

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
    )
    row = _outbox_row(connection, "market_data:evt_worker_locked_mark")
    connection.close()

    assert calls["count"] == 2
    assert result.status == "COMPLETED"
    assert result.applied_count == 1
    assert result.locked_retry_count == 1
    assert result.reason_codes == ("SQLITE_LOCK_RETRIED",)
    assert row["status"] == "APPLIED"


def test_projection_outbox_live_safe_run_once_clamps_batch_size(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-live-safe.sqlite3")
    settings = Settings(
        projection_outbox_shadow_min_age_sec=0,
        projection_outbox_live_run_once_batch_size=1,
    )
    for event_id in ("evt_worker_live_safe_1", "evt_worker_live_safe_2"):
        event = _price_tick_event(event_id)
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
        process_gateway_event(connection, event, settings=settings)

    result = projection_outbox_worker.process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=500,
        live_safe=True,
    )
    connection.close()

    assert result.requested_limit == 500
    assert result.effective_limit == 1
    assert result.live_safe is True
    assert result.claimed_count == 1
    assert result.applied_count == 1
    assert result.remaining_pending_count == 1
