from __future__ import annotations

from services import dashboard_service
from services.config import Settings
from storage.sqlite import initialize_database


def test_dashboard_fast_path_includes_append_only_controller_without_heavy_builders(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-controller-fast.sqlite3")

    def fail_if_called(*args, **kwargs):
        del args, kwargs
        raise AssertionError("heavy dashboard builder should not run in fast path")

    monkeypatch.setattr(
        dashboard_service,
        "build_ai_explanation_cards",
        fail_if_called,
    )
    monkeypatch.setattr(
        dashboard_service,
        "build_no_buy_sentinel_snapshot",
        fail_if_called,
    )
    monkeypatch.setattr(
        dashboard_service,
        "rebuild_theme_leadership",
        fail_if_called,
    )

    snapshot = dashboard_service.build_dashboard_snapshot_sections(
        connection,
        Settings(),
        sections={
            "market_data_append_only_controller",
            "pipeline_summary",
        },
        limit=20,
    )
    connection.close()

    controller = snapshot["market_data_append_only_controller"]
    summary = snapshot["pipeline_summary"]["market_data_append_only_controller"]
    assert snapshot["fast_path"] is True
    assert controller["read_only"] is True
    assert controller["no_trading_side_effects"] is True
    assert controller["operating_mode"] == "OFF"
    assert "price_tick_gate" in controller
    assert summary["operating_mode"] == "OFF"
    assert summary["global_kill_switch"] is True
    assert "MarketData append-only controller is OFF" in summary["warnings"]
    assert "LIVE_REAL/order behavior unchanged" in summary["warnings"]
    assert summary["order_behavior_changed"] is False
    assert summary["live_real_allowed"] is False


def test_dashboard_full_snapshot_contains_controller_summary(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-controller-full.sqlite3")

    snapshot = dashboard_service.build_dashboard_snapshot(connection, Settings())
    connection.close()

    assert "market_data_append_only_controller" in snapshot
    summary = snapshot["pipeline_summary"]["market_data_append_only_controller"]
    assert summary["read_only"] is True
    assert summary["no_trading_side_effects"] is True
    assert summary["operating_mode"] == "OFF"
    assert summary["effective_cutover_enabled"] is False
    assert summary["global_budget_remaining"] == 0
