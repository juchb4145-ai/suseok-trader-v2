from __future__ import annotations

from services.market_context_service import rebuild_market_context_snapshots
from services.runtime.market_regime_projection_reconcile import (
    get_latest_market_regime_projection_reconcile,
    run_market_regime_projection_reconcile,
)
from storage.sqlite import initialize_database
from tests.support_market_regime_projection import (
    market_regime_settings,
    seed_index_event,
    seed_ready_context,
)


def test_market_regime_reconcile_passes_coherent_context_and_persists(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-reconcile.sqlite3")
    settings = market_regime_settings()
    seed_ready_context(connection, settings=settings)

    result = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=True,
    )
    latest = get_latest_market_regime_projection_reconcile(connection)
    rerun = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "PASS"
    assert result.append_only_ready is True
    assert result.latest_event_covered is True
    assert result.context_ready is True
    assert result.checked_event_count == 2
    assert result.observed_index_count == 2
    assert result.outbox_pending_count == 2
    assert result.outbox_error_count == 0
    assert result.outbox_dead_letter_count == 0
    assert latest["latest_run"]["run_id"] == result.run_id
    assert latest["issues"] == []
    assert rerun.status == "PASS"


def test_market_regime_reconcile_fails_when_context_watermark_is_behind(
    tmp_path,
) -> None:
    connection = initialize_database(
        tmp_path / "market-regime-reconcile-behind.sqlite3"
    )
    settings = market_regime_settings()
    seed_ready_context(connection, settings=settings)
    seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_newer_after_context",
        settings=settings,
    )

    result = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.append_only_ready is False
    assert result.latest_event_covered is False
    assert "MARKET_REGIME_CONTEXT_WATERMARK_BEHIND" in result.reason_codes


def test_market_regime_reconcile_fails_on_outbox_error(tmp_path) -> None:
    connection = initialize_database(
        tmp_path / "market-regime-reconcile-outbox.sqlite3"
    )
    settings = market_regime_settings()
    kospi, _ = seed_ready_context(connection, settings=settings)
    connection.execute(
        "UPDATE projection_outbox SET status = 'ERROR' WHERE outbox_id = ?",
        (f"market_regime:{kospi.event_id}",),
    )
    connection.commit()

    result = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.outbox_error_count == 1
    assert "MARKET_REGIME_OUTBOX_ERROR" in result.reason_codes


def test_market_regime_reconcile_warns_on_unverified_parser(tmp_path) -> None:
    connection = initialize_database(
        tmp_path / "market-regime-reconcile-parser.sqlite3"
    )
    settings = market_regime_settings()
    seed_index_event(
        connection,
        "KOSPI",
        "evt_regime_parser_kospi",
        settings=settings,
        parser_status="PILOT_UNVERIFIED",
    )
    kosdaq = seed_index_event(
        connection,
        "KOSDAQ",
        "evt_regime_parser_kosdaq",
        settings=settings,
        parser_status="PILOT_UNVERIFIED",
    )
    rebuild_market_context_snapshots(
        connection,
        settings=settings,
        source_event_id=kosdaq.event_id,
        source_projection="market_regime",
        generated_by="test",
    )

    result = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "WARN"
    assert result.context_ready is False
    assert result.append_only_ready is False
    assert "MARKET_REGIME_CONTEXT_NOT_TRADING_READY" in result.reason_codes
