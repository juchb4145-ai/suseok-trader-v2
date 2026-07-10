from __future__ import annotations

from services.market_data_service import process_gateway_event
from services.market_scan_service import process_market_scan_event
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
