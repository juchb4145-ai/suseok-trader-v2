from __future__ import annotations

from datetime import UTC, datetime

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot_sections
from services.market_reference_service import process_market_symbols_event
from services.runtime.market_reference_projection_reconcile import (
    run_market_reference_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)


def test_dashboard_fast_path_includes_market_reference_section(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-market-reference.sqlite3")
    event = GatewayEvent(
        event_id="evt_dashboard_ref",
        event_type="market_symbols",
        source="test-gateway",
        payload={"markets": {"KOSPI": [{"code": "005930", "name": "삼성전자"}]}},
        ts=TS,
    )
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_market_symbols_event(connection, event)
    _mark_outbox(connection, event.event_id, "APPLIED")
    settings = Settings(gateway_market_reference_append_only_min_membership_count=1)
    run_market_reference_projection_reconcile(connection, settings=settings, persist=True)

    def fail_heavy_builder(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AssertionError("fast dashboard should not call full theme leadership builder")

    monkeypatch.setattr(
        "services.dashboard_service.rebuild_theme_leadership",
        fail_heavy_builder,
    )

    snapshot = build_dashboard_snapshot_sections(
        connection,
        settings,
        sections={"market_reference", "pipeline_summary"},
        limit=10,
    )
    connection.close()

    assert "market_reference" in snapshot["included_sections"]
    assert snapshot["market_reference"]["membership_count"] == 1
    assert snapshot["market_reference"]["latest_reconcile_status"] == "PASS"
    assert snapshot["market_reference"]["effective_skip_inline_count"] == 0
    assert (
        snapshot["pipeline_summary"]["market_reference"]["effective_skip_inline_count"]
        == 0
    )
    assert "market_data controller unaffected" in snapshot["market_reference"]["warnings"]


def _mark_outbox(connection, event_id: str, status: str) -> None:
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?, processed_at = datetime('now'), updated_at = datetime('now')
        WHERE projection_name = 'market_reference' AND event_id = ?
        """,
        (status, event_id),
    )
    connection.commit()
