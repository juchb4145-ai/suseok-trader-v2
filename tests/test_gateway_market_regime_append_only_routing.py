from __future__ import annotations

from services.config import TradingMode
from services.runtime.gateway_market_regime_routing import (
    decide_market_regime_append_only_routing,
    get_latest_market_regime_append_only_routing_status,
    list_market_regime_append_only_routing_decisions,
)
from services.runtime.market_regime_projection_reconcile import (
    run_market_regime_projection_reconcile,
)
from storage.sqlite import initialize_database
from tests.support_market_regime_projection import (
    market_regime_settings,
    seed_ready_context,
)


def test_market_regime_routing_dry_run_would_skip_but_effective_is_forbidden(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-regime-routing.sqlite3")
    settings = market_regime_settings(
        gateway_market_regime_append_only_dry_run_enabled=True,
    )
    _, event = seed_ready_context(connection, settings=settings)
    reconcile = run_market_regime_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=True,
    )

    first = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=settings,
    )
    second = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=settings,
    )
    status = get_latest_market_regime_append_only_routing_status(
        connection,
        settings=settings,
    )
    decisions = list_market_regime_append_only_routing_decisions(connection)
    connection.close()

    assert reconcile.status == "PASS"
    assert first.would_skip_inline is True
    assert first.effective_skip_inline is False
    assert first.blocked_reason_codes == ("EFFECTIVE_SKIP_DISABLED_IN_PR18",)
    assert second.would_skip_inline is True
    assert len(decisions) == 1
    assert status["status"] == "PASS"
    assert status["would_skip_inline_count"] == 1
    assert status["effective_skip_inline_count"] == 0


def test_market_regime_routing_is_fail_closed_when_disabled_or_unsafe(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-regime-routing-closed.sqlite3")
    ready_settings = market_regime_settings()
    _, event = seed_ready_context(connection, settings=ready_settings)
    run_market_regime_projection_reconcile(
        connection,
        settings=ready_settings,
        limit=10,
        persist=True,
    )

    disabled = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=ready_settings,
    )
    unsafe = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=market_regime_settings(
            trading_mode=TradingMode.LIVE_SIM,
            gateway_market_regime_append_only_dry_run_enabled=True,
        ),
    )
    guard_disabled = decide_market_regime_append_only_routing(
        connection,
        event,
        settings=market_regime_settings(
            gateway_market_regime_append_only_dry_run_enabled=True,
            gateway_market_regime_append_only_effective_skip_disabled_in_pr18=False,
        ),
    )
    connection.close()

    assert disabled.would_skip_inline is False
    assert "DRY_RUN_DISABLED" in disabled.blocked_reason_codes
    assert unsafe.would_skip_inline is False
    assert "MARKET_REGIME_CORE_NOT_OBSERVE_SAFE" in unsafe.blocked_reason_codes
    assert guard_disabled.would_skip_inline is False
    assert "PR18_EFFECTIVE_SKIP_GUARD_DISABLED" in guard_disabled.blocked_reason_codes
    assert disabled.effective_skip_inline is False
    assert unsafe.effective_skip_inline is False
    assert guard_disabled.effective_skip_inline is False
