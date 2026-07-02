from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.core_client import CoreClient
from gateway.mock_runtime import MockGatewayRuntime
from services.candidate_service import rebuild_candidates_from_observations
from services.config import Settings
from services.risk_gate import evaluate_risk_observations
from services.strategy_engine import evaluate_candidates
from services.theme_service import calculate_all_theme_snapshots, import_theme_memberships
from storage.sqlite import open_connection
from tests.test_mock_gateway_integration import FastApiClientTransport, integration_settings


def test_dashboard_snapshot_after_mock_observe_pipeline_flow(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "dashboard-integration.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    settings = _fresh_settings()

    with TestClient(app) as client:
        core_client = CoreClient(
            core_url="http://testserver",
            token="test-token",
            transport=FastApiClientTransport(client),
        )
        runtime = MockGatewayRuntime(settings=integration_settings(), client=core_client)
        runtime.start_once()

        connection = open_connection(db_path)
        try:
            import_theme_memberships(connection, _theme_payload())
            calculate_all_theme_snapshots(connection, settings=settings)
            for _ in range(3):
                rebuild_candidates_from_observations(connection, settings=settings)
            evaluate_candidates(connection, settings=settings)
            evaluate_risk_observations(connection, strategy_status=None, settings=settings)
        finally:
            connection.close()

        snapshot = client.get("/api/dashboard/snapshot").json()
        page = client.get("/dashboard")

    assert snapshot["pipeline_summary"]["gateway"]["recent_event_count"] >= 3
    assert snapshot["pipeline_summary"]["market_data"]["latest_tick_count"] >= 1
    assert snapshot["pipeline_summary"]["themes"]["latest_snapshot_count"] >= 1
    assert snapshot["pipeline_summary"]["candidates"]["candidate_count"] >= 1
    assert snapshot["pipeline_summary"]["strategy"]["latest_observation_count"] >= 1
    assert snapshot["pipeline_summary"]["risk"]["latest_observation_count"] >= 1
    assert page.status_code == 200
    assert "운영 관찰 대시보드" in page.text


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "dashboard_integration",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }


def _fresh_settings() -> Settings:
    return Settings(
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
        candidate_source_stale_sec=999_999_999,
        candidate_tick_stale_sec=999_999_999,
        candidate_episode_ttl_sec=999_999_999,
        strategy_engine_stale_tick_sec=999_999_999,
        risk_gate_stale_tick_sec=999_999_999,
        risk_gate_strategy_stale_sec=999_999_999,
    )
