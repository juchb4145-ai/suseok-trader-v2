from __future__ import annotations

import inspect
import time

from services import dashboard_service
from services.config import Settings
from storage.sqlite import initialize_database


def test_dashboard_fast_path_does_not_call_heavy_builders(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-fast-heavy.sqlite3")

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
            "gateway",
            "market_data",
            "projection_outbox",
            "pipeline_summary",
            "errors",
        },
        limit=20,
    )

    connection.close()
    assert snapshot["fast_path"] is True
    assert snapshot["pipeline_summary"]["fast_path"] is True
    assert snapshot["pipeline_summary"]["no_order_side_effects"] is True
    assert "ai_explanations" not in snapshot
    assert "no_buy_sentinel" not in snapshot
    assert "themes" not in snapshot


def test_dashboard_pipeline_summary_fast_includes_outbox_reconcile_and_routing(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-fast-pipeline.sqlite3")

    snapshot = dashboard_service.build_dashboard_snapshot_sections(
        connection,
        Settings(),
        sections={
            "gateway",
            "market_data",
            "projection_outbox",
            "market_data_projection_reconcile",
            "market_data_append_only_routing",
            "pipeline_summary",
        },
        limit=20,
    )

    connection.close()
    summary = snapshot["pipeline_summary"]
    assert summary["fast_path"] is True
    assert summary["no_order_side_effects"] is True
    assert "projection_outbox" in summary
    assert "market_data_projection_reconcile" in summary
    assert "market_data_append_only_routing" in summary
    assert summary["order_safety"]["order_commands_allowed"] is False


def test_dashboard_fast_path_includes_projection_outbox_backlog(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-fast-backlog.sqlite3")

    snapshot = dashboard_service.build_dashboard_snapshot_sections(
        connection,
        Settings(),
        sections={
            "projection_outbox",
            "projection_outbox_backlog",
            "pipeline_summary",
        },
        limit=20,
    )

    connection.close()
    assert "projection_outbox_backlog" in snapshot["included_sections"]
    assert "projection_outbox_backlog" in snapshot
    assert snapshot["projection_outbox_backlog"]["read_only"] is True
    assert (
        snapshot["pipeline_summary"]["projection_outbox"][
            "backlog_readiness_status"
        ]
        == snapshot["projection_outbox_backlog"]["readiness_status"]
    )
    assert "pr11_condition_event_cutover_ready" in snapshot["pipeline_summary"][
        "projection_outbox"
    ]


def test_dashboard_fast_path_skips_optional_sections_after_timeout_budget(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-fast-timeout.sqlite3")
    original = dashboard_service._build_dashboard_fast_section

    def slow_gateway(*args, **kwargs):
        if kwargs.get("section") == "gateway":
            time.sleep(0.02)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        dashboard_service,
        "_build_dashboard_fast_section",
        slow_gateway,
    )

    snapshot = dashboard_service.build_dashboard_snapshot_sections(
        connection,
        Settings(),
        sections={"gateway", "errors"},
        limit=20,
        timeout_budget_ms=1,
    )

    connection.close()
    assert "gateway" in snapshot["included_sections"]
    assert "errors" not in snapshot["included_sections"]
    assert {
        "section": "errors",
        "reason": "SKIPPED_TIMEOUT_BUDGET",
    } in snapshot["skipped_sections"]
    assert "SKIPPED_TIMEOUT_BUDGET:errors" in snapshot["warnings"]


def test_dashboard_route_builds_snapshot_outside_cache_lock() -> None:
    source = inspect.getsource(dashboard_service.build_dashboard_snapshot_sections)
    assert "build_ai_explanation_cards" not in source

    from api.routes import dashboard as dashboard_route

    route_source = inspect.getsource(dashboard_route.dashboard_snapshot)
    first_lock = route_source.index("with _SUMMARY_CACHE_LOCK:")
    open_connection = route_source.index("connection = open_connection")
    assert first_lock < open_connection
