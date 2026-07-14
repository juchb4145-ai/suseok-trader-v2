from __future__ import annotations

from services.runtime.market_scan_projection_reconcile import (
    get_latest_market_scan_projection_reconcile,
    run_market_scan_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.sqlite import initialize_database
from tests.support_market_scan_projection import (
    append_scan_event,
    apply_inline_scan_event,
    make_market_scan_event,
    market_scan_settings,
)


def test_market_scan_reconcile_passes_for_verified_worker_closed_event(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-reconcile.sqlite3")
    settings = market_scan_settings()
    event = make_market_scan_event("evt_scan_reconcile")
    append_scan_event(connection, event)
    apply_inline_scan_event(connection, event, settings=settings)
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=10,
        apply_projection=True,
    )
    assert worker.error_count == 0

    result = run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )

    assert result.status == "PASS"
    assert result.checked_event_count == 1
    assert result.source_row_count == 1
    assert result.projected_row_count == 1
    assert result.event_covered_count == 1
    assert result.parser_verified_event_count == 1
    assert result.data_usable_event_count == 1
    assert result.market_data_dependency_ready_count == 1
    assert result.outbox_applied_count == 1
    assert result.append_only_ready is True
    latest = get_latest_market_scan_projection_reconcile(connection)
    assert latest["status"] == "PASS"
    assert latest["latest_run"]["latest_event_id"] == event.event_id
    connection.close()


def test_market_scan_reconcile_warns_and_blocks_unverified_parser(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-unverified.sqlite3")
    settings = market_scan_settings(market_scan_parser_status="PILOT_UNVERIFIED")
    event = make_market_scan_event(
        "evt_scan_unverified",
        parser_status="PILOT_UNVERIFIED",
    )
    append_scan_event(connection, event)
    apply_inline_scan_event(connection, event, settings=settings)
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=10,
        apply_projection=True,
    )

    result = run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=False,
    )

    assert result.status == "WARN"
    assert result.append_only_ready is False
    assert "MARKET_SCAN_PARSER_UNVERIFIED" in result.reason_codes
    connection.close()
