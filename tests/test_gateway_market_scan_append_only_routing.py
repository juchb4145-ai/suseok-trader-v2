from __future__ import annotations

from services.market_data_service import process_gateway_event
from services.runtime.gateway_market_scan_routing import (
    MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON,
    decide_market_scan_append_only_routing,
    get_latest_market_scan_append_only_routing_status,
)
from services.runtime.market_scan_projection_reconcile import (
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


def test_market_scan_dry_run_would_skip_but_pr20_keeps_inline(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-routing.sqlite3")
    settings = market_scan_settings()
    prior = make_market_scan_event("evt_scan_prior")
    append_scan_event(connection, prior)
    apply_inline_scan_event(connection, prior, settings=settings)
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=10,
        apply_projection=True,
    )
    reconcile = run_market_scan_projection_reconcile(
        connection,
        settings=settings,
        persist=True,
    )
    assert reconcile.status == "PASS"

    current = make_market_scan_event("evt_scan_current", code="000660")
    append_scan_event(connection, current)
    assert process_gateway_event(connection, current, settings=settings).status == "APPLIED"
    decision = decide_market_scan_append_only_routing(
        connection,
        current,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert decision.market_data_dependency_ready is True
    assert MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON in decision.blocked_reason_codes
    status = get_latest_market_scan_append_only_routing_status(
        connection,
        settings=settings,
    )
    assert status["status"] == "WARN"
    assert status["would_skip_inline_count"] == 1
    assert status["effective_skip_inline_count"] == 0
    assert status["inline_market_scan_path_retained"] is True
    connection.close()


def test_market_scan_dry_run_defaults_fail_closed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-routing-default.sqlite3")
    settings = market_scan_settings(
        gateway_market_scan_append_only_dry_run_enabled=False,
        projection_outbox_market_scan_apply_enabled=False,
    )
    event = make_market_scan_event("evt_scan_default")
    append_scan_event(connection, event)
    decision = decide_market_scan_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )

    assert decision.would_skip_inline is False
    assert decision.effective_skip_inline is False
    assert "MARKET_SCAN_DRY_RUN_DISABLED" in decision.blocked_reason_codes
    assert "MARKET_SCAN_WORKER_APPLY_DISABLED" in decision.blocked_reason_codes
    connection.close()
