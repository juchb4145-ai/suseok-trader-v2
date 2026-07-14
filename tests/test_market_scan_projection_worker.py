from __future__ import annotations

import sqlite3

from services.market_data_service import process_gateway_event
from services.market_scan_service import process_market_scan_event
from services.runtime import projection_outbox_worker
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.sqlite import initialize_database
from tests.support_market_scan_projection import (
    append_scan_event,
    make_market_scan_event,
    market_scan_settings,
)


def test_market_scan_worker_applies_snapshot_after_market_data_dependency(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker.sqlite3")
    settings = market_scan_settings()
    event = make_market_scan_event("evt_scan_worker")
    append_scan_event(connection, event)
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )

    assert result.status == "COMPLETED"
    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_scan",)
    row = connection.execute(
        """
        SELECT source_event_id, request_id, parser_status, generated_by
        FROM market_scan_snapshots WHERE source_event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    assert row is not None
    assert row["request_id"].startswith("market_scan:")
    assert row["parser_status"] == "KOA_STUDIO_VERIFIED"
    assert str(row["generated_by"]).startswith("projection_outbox_worker:")
    outbox = connection.execute(
        """
        SELECT status, json_extract(metadata_json, '$.last_worker_evidence.apply_result')
               AS apply_result
        FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    assert dict(outbox) == {"status": "APPLIED", "apply_result": "APPLIED_BY_WORKER"}
    connection.close()


def test_market_scan_worker_retries_until_market_data_dependency_is_ready(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker-dependency.sqlite3")
    settings = market_scan_settings(projection_outbox_retry_limit=3)
    event = make_market_scan_event("evt_scan_dependency")
    append_scan_event(connection, event)

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )

    assert result.error_count == 1
    row = connection.execute(
        """
        SELECT status, attempts, last_error FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    assert row["status"] == "PENDING"
    assert row["attempts"] == 1
    assert "market_data sibling projection" in row["last_error"]
    assert connection.execute(
        "SELECT COUNT(*) FROM market_scan_snapshots"
    ).fetchone()[0] == 0
    connection.close()


def test_market_scan_worker_keeps_sqlite_lock_failure_retryable(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker-locked.sqlite3")
    settings = market_scan_settings(projection_outbox_retry_limit=3)
    event = make_market_scan_event("evt_scan_worker_locked")
    append_scan_event(connection, event)
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"

    def raise_locked(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        projection_outbox_worker,
        "process_market_scan_event",
        raise_locked,
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    row = connection.execute(
        """
        SELECT status, attempts, last_error FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.error_count == 1
    assert result.dead_letter_count == 0
    assert row["status"] == "PENDING"
    assert row["attempts"] == 1
    assert "database is locked" in row["last_error"]
    assert connection.execute(
        "SELECT COUNT(*) FROM market_scan_errors WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()[0] == 0
    connection.close()


def test_market_scan_worker_recovers_prior_transient_lock_error(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker-recover.sqlite3")
    settings = market_scan_settings()
    event = make_market_scan_event("evt_scan_worker_recover")
    append_scan_event(connection, event)
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"
    connection.execute(
        """
        INSERT INTO market_scan_errors (
            event_id, request_id, tr_code, scan_type, market,
            reason_code, error_message, payload_json
        ) VALUES (?, ?, 'OPT10032', 'TRADE_VALUE', 'KOSPI',
                  'MARKET_SCAN_PROJECTION_FAILED', 'database is locked', '{}')
        """,
        (event.event_id, event.payload["request_id"]),
    )
    connection.commit()

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    outbox = connection.execute(
        """
        SELECT status, last_error FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()

    assert result.status == "COMPLETED"
    assert result.applied_by_worker_count == 1
    assert outbox["status"] == "APPLIED"
    assert outbox["last_error"] is None
    assert connection.execute(
        "SELECT COUNT(*) FROM market_scan_snapshots WHERE source_event_id = ?",
        (event.event_id,),
    ).fetchone()[0] == 1
    connection.close()


def test_market_scan_worker_is_idempotent_after_inline_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker-inline.sqlite3")
    settings = market_scan_settings()
    event = make_market_scan_event("evt_scan_inline")
    append_scan_event(connection, event)
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"
    assert process_market_scan_event(connection, event, settings=settings).status == "APPLIED"
    before = connection.execute(
        "SELECT COUNT(*) FROM market_scan_snapshots"
    ).fetchone()[0]

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )

    assert result.applied_by_verify_count == 1
    assert result.applied_by_worker_count == 0
    assert connection.execute(
        "SELECT COUNT(*) FROM market_scan_snapshots"
    ).fetchone()[0] == before
    connection.close()
