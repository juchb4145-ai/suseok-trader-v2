from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.sqlite import initialize_database
from tests.support_market_regime_projection import (
    count_rows,
    market_regime_settings,
    seed_index_event,
    seed_ready_context,
)


def test_market_regime_worker_is_disabled_by_default_and_does_not_mutate(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-regime-worker-disabled.sqlite3")
    settings = market_regime_settings(projection_outbox_market_regime_apply_enabled=False)
    event = seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_disabled",
        settings=settings,
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    job = _outbox(connection, event.event_id)
    context_count = count_rows(connection, "market_context_snapshots")
    regime_count = count_rows(connection, "market_regime_snapshots")
    connection.close()

    assert result.apply_projection_effective is False
    assert result.market_regime_apply_enabled is False
    assert result.skipped_apply_disabled_count == 1
    assert job["status"] == "SKIPPED"
    assert context_count == 0
    assert regime_count == 0


def test_market_regime_worker_builds_common_context_and_is_idempotent(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-worker.sqlite3")
    settings = market_regime_settings()
    kospi = seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_worker_kospi",
        settings=settings,
    )
    seed_index_event(
        connection,
        "KOSDAQ",
        "evt_regime_worker_kosdaq",
        settings=settings,
    )

    first = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=2,
        apply_projection=True,
        projection_name="market_regime",
    )
    first_job = _outbox(connection, kospi.event_id)
    first_evidence = json.loads(first_job["metadata_json"])["last_worker_evidence"]
    counts_before_retry = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )
    _reset_outbox(connection, kospi.event_id)
    retry = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    counts_after_retry = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )
    connection.close()

    assert first.applied_count == 2
    assert first.applied_by_worker_count == 1
    assert first.applied_by_verify_count == 1
    assert first.market_regime_apply_enabled is True
    assert set(first.mutated_projection_names) == {"market_context", "market_regime"}
    assert first_evidence["apply_mode"] == "MARKET_REGIME_APPLY"
    assert first_evidence["apply_result"] == "APPLIED_BY_WORKER"
    assert first_evidence["market_regime_standalone_apply"]["status"] == ("APPLIED_BY_WORKER")
    assert counts_before_retry == counts_after_retry == (1, 2)
    assert retry.applied_by_verify_count == 1
    assert retry.mutated_projection_names == ()


def test_market_regime_worker_superseded_event_does_not_explode_snapshots(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-regime-worker-superseded.sqlite3")
    settings = market_regime_settings(projection_outbox_retry_limit=3)
    now = utc_now()
    old = seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_old_kospi",
        settings=settings,
        ts=now - timedelta(seconds=2),
    )
    seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_new_kospi",
        settings=settings,
        ts=now - timedelta(seconds=1),
    )
    seed_index_event(
        connection,
        "KOSDAQ",
        "evt_regime_current_kosdaq",
        settings=settings,
        ts=now,
    )

    first = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=3,
        apply_projection=True,
        projection_name="market_regime",
    )
    counts_before_retry = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )
    connection.execute(
        "UPDATE projection_outbox SET available_at = ? WHERE outbox_id = ?",
        (datetime_to_wire(now - timedelta(minutes=1)), f"market_regime:{old.event_id}"),
    )
    connection.commit()
    retry = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    retry_job = _outbox(connection, old.event_id)
    retry_evidence = json.loads(retry_job["metadata_json"])["last_worker_evidence"]
    counts_after_retry = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )
    connection.close()

    assert first.error_count == 1
    assert first.applied_count == 2
    assert counts_before_retry == counts_after_retry == (1, 2)
    assert retry.applied_by_verify_count == 1
    assert retry_evidence["market_regime_standalone_apply"]["source_event_superseded"] is True


def test_market_regime_worker_dead_letters_missing_index_dependency(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-worker-missing-index.sqlite3")
    settings = market_regime_settings(projection_outbox_retry_limit=1)
    event = seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_missing_index",
        settings=settings,
        apply_index=False,
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    job = _outbox(connection, event.event_id)
    connection.close()

    assert result.dead_letter_count == 1
    assert result.projection_apply_error_count == 1
    assert job["status"] == "DEAD_LETTER"
    assert "MARKET_INDEX_DEPENDENCY_NOT_APPLIED" in job["last_error"]


def test_market_regime_effective_skip_forces_event_linked_context_refresh(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-regime-worker-cutover.sqlite3")
    settings = market_regime_settings()
    seed_ready_context(connection, settings=settings, prefix="evt_worker_cutover_base")
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'APPLIED', processed_at = updated_at
        WHERE projection_name = 'market_regime'
        """
    )
    connection.commit()
    current = seed_index_event(
        connection,
        "KOSPI",
        "evt_worker_cutover_current",
        settings=settings,
    )
    connection.execute(
        """
        INSERT INTO market_regime_projection_routing_decisions (
            event_id, event_type, effective_skip_inline, decided_at
        ) VALUES (?, 'market_index_tick', 1, ?)
        """,
        (current.event_id, datetime_to_wire(utc_now())),
    )
    connection.commit()
    before = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_regime",
    )
    after = (
        count_rows(connection, "market_regime_snapshots"),
        count_rows(connection, "market_context_snapshots"),
    )
    latest_sources = {
        row["source_event_id"]
        for row in connection.execute(
            """
            SELECT snapshot.source_event_id
            FROM market_context_latest AS latest
            JOIN market_context_snapshots AS snapshot
              ON snapshot.snapshot_id = latest.snapshot_id
            """
        ).fetchall()
    }
    job = _outbox(connection, current.event_id)
    evidence = json.loads(job["metadata_json"])["last_worker_evidence"]
    connection.close()

    assert result.applied_by_worker_count == 1
    assert set(result.mutated_projection_names) == {"market_context", "market_regime"}
    assert after == (before[0] + 1, before[1] + 2)
    assert latest_sources == {current.event_id}
    assert evidence["market_regime_standalone_apply"]["effective_skip"] is True
    assert evidence["apply_result"] == "APPLIED_BY_WORKER"


def _outbox(connection, event_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (f"market_regime:{event_id}",),
    ).fetchone()


def _reset_outbox(connection, event_id: str) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'PENDING', attempts = 0, last_error = NULL,
            available_at = '2000-01-01T00:00:00Z', locked_by = NULL,
            locked_at = NULL, processed_at = NULL
        WHERE outbox_id = ?
        """,
        (f"market_regime:{event_id}",),
    )
    connection.commit()
