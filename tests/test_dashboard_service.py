from __future__ import annotations

import json

from domain.broker.utils import utc_now
from gateway.event_factory import make_condition_event, make_heartbeat_event, make_price_tick_event
from services.candidate_service import rebuild_candidates_from_observations
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from services.market_data_service import process_gateway_event
from services.risk_gate import evaluate_risk_observations
from services.strategy_engine import evaluate_candidates
from services.theme_service import calculate_all_theme_snapshots, import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_dashboard_snapshot_empty_database_keeps_safety_and_keys(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-empty.sqlite3")

    snapshot = build_dashboard_snapshot(connection, Settings())

    connection.close()
    assert snapshot["safety"]["order_routing_enabled"] is False
    assert snapshot["safety"]["order_controls_available"] is False
    assert snapshot["safety"]["ai_execution_available"] is False
    assert snapshot["safety"]["ai_context_builder_available"] is True
    assert snapshot["safety"]["observe_only_pipeline"] is True
    assert "OBSERVE_PASS는 주문 승인이 아닙니다." in snapshot["safety"]["warnings"]
    assert "MATCHED_OBSERVATION은 매수 신호가 아닙니다." in snapshot["safety"]["warnings"]
    assert snapshot["dry_run"]["exit_engine"]["enabled"] is False
    assert snapshot["dry_run"]["exit_engine"]["gateway_command_allowed"] is False
    assert snapshot["dry_run"]["exit_engine"]["broker_order_sent"] is False
    assert snapshot["pipeline_summary"]["dry_run"]["exit_evaluation_count"] == 0
    assert {
        "gateway",
        "market_data",
        "themes",
        "candidates",
        "strategy",
        "risk",
        "ai_sidecar",
    }.issubset(snapshot["pipeline_summary"])


def test_dashboard_snapshot_with_sample_data_reflects_pipeline_rows(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-sample.sqlite3")
    settings = _fresh_settings()
    _build_observe_pipeline_fixture(connection, settings)
    _insert_dashboard_error_and_ai_rows(connection)

    snapshot = build_dashboard_snapshot(connection, settings, detail="full", limit=20)

    connection.close()
    assert snapshot["gateway"]["recent_event_count"] >= 3
    assert snapshot["pipeline_summary"]["market_data"]["latest_tick_count"] == 1
    assert snapshot["pipeline_summary"]["themes"]["latest_snapshot_count"] >= 1
    assert snapshot["pipeline_summary"]["candidates"]["candidate_count"] >= 1
    assert snapshot["pipeline_summary"]["strategy"]["latest_observation_count"] >= 1
    assert snapshot["pipeline_summary"]["risk"]["latest_observation_count"] >= 1
    assert snapshot["themes"]["latest_snapshots"]
    assert snapshot["candidates"]["candidates"]
    assert "sources" in snapshot["candidates"]["candidates"][0]
    assert snapshot["strategy"]["latest_observations"]
    assert "setup_observations" in snapshot["strategy"]["latest_observations"][0]
    assert snapshot["risk"]["latest_observations"]
    assert "check_observations" in snapshot["risk"]["latest_observations"][0]
    assert "exit_engine" in snapshot["dry_run"]
    assert snapshot["dry_run"]["exit_engine"]["live_order_allowed"] is False
    assert snapshot["errors"]["market_projection_errors"]
    assert snapshot["errors"]["theme_projection_errors"]
    assert snapshot["errors"]["candidate_projection_errors"]
    assert snapshot["errors"]["strategy_errors"]
    assert snapshot["errors"]["risk_errors"]
    assert snapshot["ai_sidecar"]["insight_count"] == 1
    assert snapshot["ai_sidecar"]["status"]["execution_api_available"] is True
    assert snapshot["ai_sidecar"]["status"]["context_builder_available"] is True
    assert snapshot["ai_sidecar"]["execution_controls_available"] is False
    assert snapshot["ai_sidecar"]["status"]["tools_enabled"] is False
    assert snapshot["ai_sidecar"]["status"]["order_tools_enabled"] is False


def _build_observe_pipeline_fixture(connection, settings: Settings) -> None:
    heartbeat = make_heartbeat_event(status="ok")
    tick = make_price_tick_event(
        code="005930",
        name="삼성전자",
        price=70000,
        change_rate=1.6,
        trade_value=70_000_000,
        execution_strength=118.0,
    )
    condition = make_condition_event(code="005930", name="삼성전자", action="ENTER", price=70000)
    append_gateway_event(connection, heartbeat)
    _append_and_project(connection, tick, settings)
    _append_and_project(connection, condition, settings)

    import_theme_memberships(
        connection,
        {
            "source_type": "MOCK",
            "source_name": "dashboard_test",
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
        },
    )
    calculate_all_theme_snapshots(connection, settings=settings)
    for _ in range(3):
        rebuild_candidates_from_observations(connection, settings=settings)
    evaluate_candidates(connection, settings=settings)
    evaluate_risk_observations(connection, strategy_status=None, settings=settings)


def _insert_dashboard_error_and_ai_rows(connection) -> None:
    payload = json.dumps({"fixture": True}, ensure_ascii=False)
    now = utc_now().isoformat()
    connection.execute(
        """
        INSERT INTO market_projection_errors (
            event_id, event_type, code, error_message, payload_json
        )
        VALUES ('evt_bad_tick', 'price_tick', '005930', 'bad market row', ?)
        """,
        (payload,),
    )
    connection.execute(
        """
        INSERT INTO theme_projection_errors (theme_id, code, error_message, payload_json)
        VALUES ('semiconductor', '005930', 'bad theme row', ?)
        """,
        (payload,),
    )
    connection.execute(
        """
        INSERT INTO candidate_projection_errors (
            candidate_instance_id,
            source_event_id,
            code,
            error_message,
            payload_json
        )
        VALUES ('candidate-fixture', 'source-fixture', '005930', 'bad candidate row', ?)
        """,
        (payload,),
    )
    connection.execute(
        """
        INSERT INTO strategy_evaluation_errors (
            run_id,
            candidate_instance_id,
            code,
            error_message,
            payload_json
        )
        VALUES ('strategy-run-fixture', 'candidate-fixture', '005930', 'bad strategy row', ?)
        """,
        (payload,),
    )
    connection.execute(
        """
        INSERT INTO risk_evaluation_errors (
            run_id,
            candidate_instance_id,
            strategy_observation_id,
            code,
            error_message,
            payload_json
        )
        VALUES (
            'risk-run-fixture',
            'candidate-fixture',
            'strategy-fixture',
            '005930',
            'bad risk row',
            ?
        )
        """,
        (payload,),
    )
    connection.execute(
        """
        INSERT INTO ai_insights (
            insight_id,
            request_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            summary,
            root_cause,
            severity,
            operator_action,
            output_json,
            schema_version,
            created_at
        )
        VALUES (
            'insight-dashboard-fixture',
            NULL,
            'NO_TRADE_RCA',
            '2026-06-27',
            'dashboard',
            'fixture',
            '테스트 insight',
            'fixture',
            'INFO',
            '관찰',
            ?,
            'ai_sidecar_insight_v1',
            ?
        )
        """,
        (payload, now),
    )
    connection.commit()


def _append_and_project(connection, event, settings: Settings) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"


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
