from __future__ import annotations

from datetime import timedelta

import api.routes.gateway as gateway_routes
from apps.core_api import app
from domain.broker.utils import utc_now
from fastapi.testclient import TestClient
from services.market_data_service import process_gateway_event
from services.market_scan_service import process_market_scan_event
from services.runtime.gateway_market_scan_routing import (
    decide_market_scan_append_only_routing,
    get_latest_market_scan_append_only_routing_status,
)
from services.runtime.market_scan_projection_reconcile import (
    run_market_scan_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.sqlite import initialize_database, open_connection
from tests.support_market_scan_projection import (
    append_scan_event,
    apply_inline_scan_event,
    make_market_scan_event,
    market_scan_cutover_settings,
)


def test_market_scan_limited_cutover_worker_closes_effective_skip(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-cutover.sqlite3")
    settings = market_scan_cutover_settings()
    _prepare_prior(connection, settings=settings, prefix="effective")
    current = _append_current(connection, settings=settings, prefix="effective")

    decision = decide_market_scan_append_only_routing(
        connection,
        current,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is True
    assert decision.controller_status == "PASS"
    assert decision.skip_budget_used == 1
    assert _snapshot_exists(connection, current.event_id) is False
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    assert worker.applied_by_worker_count == 1
    assert _snapshot_exists(connection, current.event_id) is True
    status = get_latest_market_scan_append_only_routing_status(
        connection,
        settings=settings,
    )
    assert status["effective_skip_inline_count"] == 1
    assert status["rollback_required"] is False
    assert set(status["effective_skip_health"].values()) == {0}
    reconcile = run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=False,
    )
    assert reconcile.status == "PASS"
    connection.close()


def test_market_scan_cutover_budget_is_idempotent_and_exhausts(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-budget.sqlite3")
    settings = market_scan_cutover_settings()
    _prepare_prior(connection, settings=settings, prefix="budget")
    first = _append_current(connection, settings=settings, prefix="budget-first")

    first_decision = decide_market_scan_append_only_routing(
        connection,
        first,
        settings=settings,
    )
    duplicate_decision = decide_market_scan_append_only_routing(
        connection,
        first,
        settings=settings,
    )
    assert first_decision.effective_skip_inline is True
    assert duplicate_decision.effective_skip_inline is True
    assert duplicate_decision.skip_budget_used == 1
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    assert run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    ).status == "PASS"

    second = _append_current(connection, settings=settings, prefix="budget-second")
    second_decision = decide_market_scan_append_only_routing(
        connection,
        second,
        settings=settings,
    )
    assert second_decision.would_skip_inline is True
    assert second_decision.effective_skip_inline is False
    assert "MARKET_SCAN_SKIP_BUDGET_EXHAUSTED" in (
        second_decision.blocked_reason_codes
    )
    assert process_market_scan_event(connection, second, settings=settings).status == (
        "APPLIED"
    )
    connection.close()


def test_market_scan_cutover_kill_switch_keeps_inline_without_budget(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-kill.sqlite3")
    settings = market_scan_cutover_settings(
        gateway_market_scan_append_only_global_kill_switch=True
    )
    _prepare_prior(connection, settings=settings, prefix="kill")
    current = _append_current(connection, settings=settings, prefix="kill")

    decision = decide_market_scan_append_only_routing(
        connection,
        current,
        settings=settings,
    )

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert decision.skip_budget_used == 0
    assert "MARKET_SCAN_GLOBAL_KILL_SWITCH" in decision.blocked_reason_codes
    connection.close()


def test_market_scan_cutover_rejects_stale_and_future_events(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-freshness.sqlite3")
    settings = market_scan_cutover_settings(
        gateway_market_scan_append_only_max_event_age_sec=10,
        gateway_market_scan_append_only_max_future_skew_sec=2,
    )
    _prepare_prior(connection, settings=settings, prefix="freshness")
    stale = make_market_scan_event(
        "evt_scan_stale_current",
        code="000660",
        ts=utc_now() - timedelta(seconds=30),
    )
    append_scan_event(connection, stale)
    assert process_gateway_event(connection, stale, settings=settings).status == "APPLIED"
    stale_decision = decide_market_scan_append_only_routing(
        connection,
        stale,
        settings=settings,
    )
    assert stale_decision.effective_skip_inline is False
    assert "MARKET_SCAN_EVENT_STALE" in stale_decision.blocked_reason_codes
    assert process_market_scan_event(connection, stale, settings=settings).status == "APPLIED"
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    assert run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    ).status == "PASS"

    future = make_market_scan_event(
        "evt_scan_future_current",
        code="035420",
        ts=utc_now() + timedelta(seconds=20),
    )
    append_scan_event(connection, future)
    assert process_gateway_event(connection, future, settings=settings).status == "APPLIED"
    future_decision = decide_market_scan_append_only_routing(
        connection,
        future,
        settings=settings,
    )
    assert future_decision.effective_skip_inline is False
    assert "MARKET_SCAN_EVENT_FUTURE_SKEW" in future_decision.blocked_reason_codes
    connection.close()


def test_market_scan_missing_worker_closure_rolls_back_next_event(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-rollback.sqlite3")
    settings = market_scan_cutover_settings(
        gateway_market_scan_append_only_max_skip_per_minute=2
    )
    _prepare_prior(connection, settings=settings, prefix="rollback")
    first = _append_current(connection, settings=settings, prefix="rollback-first")
    assert decide_market_scan_append_only_routing(
        connection,
        first,
        settings=settings,
    ).effective_skip_inline is True

    second = _append_current(connection, settings=settings, prefix="rollback-second")
    decision = decide_market_scan_append_only_routing(
        connection,
        second,
        settings=settings,
    )

    assert decision.effective_skip_inline is False
    assert decision.rollback_required is True
    assert "MARKET_SCAN_EFFECTIVE_SKIP_PENDING_WORKER" in (
        decision.blocked_reason_codes
    )
    assert decision.skip_budget_used == 1
    connection.close()


def test_market_scan_worker_error_forces_inline_rollback(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-worker-error.sqlite3")
    settings = market_scan_cutover_settings(
        gateway_market_scan_append_only_max_skip_per_minute=2
    )
    _prepare_prior(connection, settings=settings, prefix="worker-error")
    first = _append_current(connection, settings=settings, prefix="worker-error-first")
    assert decide_market_scan_append_only_routing(
        connection,
        first,
        settings=settings,
    ).effective_skip_inline is True
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = 'ERROR', last_error = 'fixture worker failure'
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (first.event_id,),
    )
    connection.commit()

    second = _append_current(connection, settings=settings, prefix="worker-error-second")
    decision = decide_market_scan_append_only_routing(
        connection,
        second,
        settings=settings,
    )

    assert decision.effective_skip_inline is False
    assert decision.rollback_required is True
    assert "MARKET_SCAN_EFFECTIVE_SKIP_WORKER_ERROR" in (
        decision.blocked_reason_codes
    )
    assert "MARKET_SCAN_OUTBOX_ERROR" in decision.blocked_reason_codes
    connection.close()


def test_gateway_api_skips_inline_then_scan_worker_applies(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-scan-gateway-api.sqlite3"
    settings = market_scan_cutover_settings(trading_db_path=db_path)
    connection = initialize_database(db_path)
    _prepare_prior(connection, settings=settings, prefix="api")
    connection.close()
    monkeypatch.setattr(gateway_routes, "load_settings", lambda: settings)
    current = make_market_scan_event("evt_scan_api_current", code="000660")

    with TestClient(app) as client:
        posted = client.post(
            "/api/gateway/events",
            json=current.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    assert posted.status_code == 200
    payload = posted.json()
    assert payload["market_scan_append_only_routing"]["effective_skip_inline"] is True
    assert payload["projection_statuses"]["market_scan"] == (
        "SKIPPED_INLINE_APPEND_ONLY_MARKET_SCAN"
    )
    assert _snapshot_exists(connection, current.event_id) is False
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        projection_name="market_scan",
    )
    assert worker.applied_by_worker_count == 1
    assert _snapshot_exists(connection, current.event_id) is True
    assert connection.execute(
        "SELECT COUNT(*) FROM gateway_commands"
    ).fetchone()[0] == 0
    connection.close()


def _prepare_prior(connection, *, settings, prefix: str) -> None:
    prior = make_market_scan_event(f"evt_scan_{prefix}_prior")
    append_scan_event(connection, prior)
    apply_inline_scan_event(connection, prior, settings=settings)
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=10,
        apply_projection=True,
    )
    assert worker.error_count == 0
    reconcile = run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )
    assert reconcile.status == "PASS"


def _append_current(connection, *, settings, prefix: str):
    event = make_market_scan_event(f"evt_scan_{prefix}_current", code="000660")
    append_scan_event(connection, event)
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"
    return event


def _snapshot_exists(connection, event_id: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM market_scan_snapshots WHERE source_event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone() is not None
