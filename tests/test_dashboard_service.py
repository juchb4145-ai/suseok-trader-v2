from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
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
    stage_statuses = {
        row["stage"]: row for row in snapshot["pipeline_summary"]["stage_statuses"]
    }
    assert {
        "Core",
        "Gateway",
        "MarketData",
        "RealtimeSubscription",
        "ConditionFusion",
        "Theme",
        "Candidate",
        "Strategy",
        "Risk",
        "EntryTiming",
        "LiveSim",
        "OrderSafety",
    }.issubset(stage_statuses)
    assert stage_statuses["OrderSafety"]["status"] == "PASS"
    assert stage_statuses["RealtimeSubscription"]["endpoint"] == (
        "/api/operator/realtime-subscriptions/plan"
    )
    assert stage_statuses["OrderSafety"]["count"] == 0
    assert stage_statuses["OrderSafety"]["endpoint"] == "/api/gateway/commands/status"
    assert all("last_updated_at" in row for row in stage_statuses.values())
    assert {
        "gateway",
        "market_data",
        "realtime_subscription",
        "condition_fusion",
        "themes",
        "candidates",
        "strategy",
        "risk",
        "ai_sidecar",
    }.issubset(snapshot["pipeline_summary"])
    assert snapshot["market_indexes"]["status"]["enabled"] is True
    assert snapshot["market_indexes"]["gateway_adapter"]["enabled"] is False
    assert snapshot["market_indexes"]["gateway_adapter"]["health"] == "DISABLED"


def test_dashboard_gateway_stage_warns_on_stale_heartbeat(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-stale-gateway.sqlite3")
    append_gateway_event(
        connection,
        GatewayEvent(
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={"status": "ok"},
            ts=utc_now() - timedelta(seconds=180),
        ),
    )

    snapshot = build_dashboard_snapshot(connection, Settings())

    connection.close()
    stage_statuses = {
        row["stage"]: row for row in snapshot["pipeline_summary"]["stage_statuses"]
    }
    assert stage_statuses["Gateway"]["status"] == "WARN"
    assert "GATEWAY_HEARTBEAT_STALE" in stage_statuses["Gateway"]["reason_codes"]


def test_dashboard_snapshot_separates_market_index_projection_and_gateway_adapter(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-index-adapter.sqlite3")
    append_gateway_event(
        connection,
        GatewayEvent(
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={
                "status": "ok",
                "market_index_enabled": True,
                "market_index_realtime_enabled": True,
                "market_index_registered_codes": ["KOSPI", "KOSDAQ"],
                "market_index_callback_count": 1,
                "parsed_market_index_tick_count": 0,
                "market_index_parse_error_count": 1,
                "latest_market_index_parse_error": {"reason": "INDEX_PARSE_ERROR"},
                "market_index_adapter_health": "PARSE_ERROR",
            },
        ),
    )
    received_at = utc_now()
    for index in range(6):
        event = GatewayEvent(
            event_type="gateway_log",
            source="kiwoom_gateway",
            payload={"message": f"noise-{index}"},
        )
        append_gateway_event(connection, event)
        connection.execute(
            "UPDATE gateway_events SET received_at = ? WHERE event_id = ?",
            (
                datetime_to_wire(received_at + timedelta(seconds=index + 1)),
                event.event_id,
            ),
        )
        connection.commit()

    snapshot = build_dashboard_snapshot(connection, Settings(), limit=5)

    connection.close()
    assert snapshot["market_indexes"]["status"]["projection_error_count"] == 0
    assert snapshot["market_indexes"]["gateway_adapter"]["enabled"] is True
    assert snapshot["market_indexes"]["gateway_adapter"]["registered_codes"] == [
        "KOSPI",
        "KOSDAQ",
    ]
    assert snapshot["market_indexes"]["gateway_adapter"]["parse_error_count"] == 1
    assert snapshot["market_indexes"]["gateway_adapter"]["latest_parse_error"]["reason"] == (
        "INDEX_PARSE_ERROR"
    )


def test_dashboard_top_theme_query_does_not_hide_tradable_themes_behind_latest_sample(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-theme-false-empty.sqlite3")
    base = utc_now()
    for index in range(50):
        _insert_theme_snapshot(
            connection,
            theme_id=f"data-wait-{index:03d}",
            theme_name=f"DATA_WAIT {index:03d}",
            state="DATA_WAIT",
            calculated_at=datetime_to_wire(base - timedelta(seconds=index)),
        )
    for index in range(3):
        _insert_theme_snapshot(
            connection,
            theme_id=f"leading-{index}",
            theme_name=f"LEADING {index}",
            state="LEADING",
            calculated_at=datetime_to_wire(base - timedelta(minutes=10 + index)),
            total_trade_value=300_000_000 - index,
            trade_value_delta_3m=90_000_000 - index,
            trade_value_delta_1m=30_000_000 - index,
        )
    _insert_theme_snapshot(
        connection,
        theme_id="spreading-0",
        theme_name="SPREADING 0",
        state="SPREADING",
        calculated_at=datetime_to_wire(base - timedelta(minutes=14)),
        total_trade_value=500_000_000,
    )
    for index in range(26):
        _insert_theme_snapshot(
            connection,
            theme_id=f"watch-{index:03d}",
            theme_name=f"WATCH {index:03d}",
            state="WATCH",
            calculated_at=datetime_to_wire(base - timedelta(minutes=20 + index)),
        )

    snapshot = build_dashboard_snapshot(connection, Settings(), limit=50)
    connection.close()

    themes = snapshot["themes"]
    assert themes["state_counts"]["LEADING"] == 3
    assert themes["state_counts"]["SPREADING"] == 1
    assert themes["latest_sample_state_counts"]["DATA_WAIT"] == 50
    assert themes["top_tradable_themes"]
    assert [row["state"] for row in themes["top_tradable_themes"]] == [
        "LEADING",
        "LEADING",
        "LEADING",
        "SPREADING",
    ]
    assert "DASHBOARD_SAMPLE_LIMIT_HIDES_TRADABLE_THEME" in themes["dashboard_warnings"]
    assert themes["top_list_source"] == "state_filtered_strength_query"


def test_dashboard_top_leading_and_spreading_use_state_filtered_queries(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-theme-direct-top.sqlite3")
    base = utc_now()
    for index in range(5):
        _insert_theme_snapshot(
            connection,
            theme_id=f"latest-data-wait-{index}",
            theme_name=f"LATEST DATA_WAIT {index}",
            state="DATA_WAIT",
            calculated_at=datetime_to_wire(base - timedelta(seconds=index)),
        )
    _insert_theme_snapshot(
        connection,
        theme_id="older-leading",
        theme_name="오래된 주도 테마",
        state="LEADING",
        calculated_at=datetime_to_wire(base - timedelta(minutes=30)),
        total_trade_value=900_000_000,
    )
    _insert_theme_snapshot(
        connection,
        theme_id="older-spreading",
        theme_name="오래된 확산 테마",
        state="SPREADING",
        calculated_at=datetime_to_wire(base - timedelta(minutes=31)),
        total_trade_value=800_000_000,
    )

    snapshot = build_dashboard_snapshot(connection, Settings(), limit=3)
    connection.close()

    themes = snapshot["themes"]
    assert themes["latest_sample_state_counts"]["DATA_WAIT"] == 3
    assert themes["top_leading_themes"][0]["theme_id"] == "older-leading"
    assert themes["top_spreading_themes"][0]["theme_id"] == "older-spreading"
    assert {row["theme_id"] for row in themes["top_tradable_themes"]} == {
        "older-leading",
        "older-spreading",
    }


def test_dashboard_exposes_leadership_when_legacy_db_theme_is_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-leadership-fallback.sqlite3")
    settings = Settings(
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
        candidate_source_stale_sec=999_999_999,
        candidate_tick_stale_sec=999_999_999,
        candidate_episode_ttl_sec=999_999_999,
        theme_observable_coverage_enabled=False,
    )
    members = [(f"{100000 + index:06d}", f"종목{index:02d}") for index in range(30)]
    import_theme_memberships(
        connection,
        {
            "source_type": "MOCK",
            "source_name": "dashboard_leadership_fixture",
            "themes": [
                {
                    "theme_id": "large-theme",
                    "theme_name": "대형테마",
                    "members": [{"code": code, "name": name} for code, name in members],
                }
            ],
        },
    )
    for index, (code, name) in enumerate(members[:3]):
        _append_and_project(
            connection,
            make_price_tick_event(
                code=code,
                name=name,
                change_rate=2.4 - index * 0.2,
                trade_value=300_000_000 - index * 10_000_000,
                execution_strength=120.0 - index,
            ),
            settings,
        )
    calculate_all_theme_snapshots(connection, settings=settings)

    snapshot = build_dashboard_snapshot(connection, settings, limit=20)
    connection.close()

    themes = snapshot["themes"]
    assert themes["state_counts"]["DATA_WAIT"] == 1
    assert themes["top_tradable_themes"] == []
    assert themes["leadership"]["watchset"]["items"]
    assert themes["leadership"]["eligible_theme_count"] == 1
    assert themes["leadership"]["top_themes"][0]["state"] in {"LEADING", "SPREADING"}


def test_dashboard_theme_snapshot_exposes_age_and_stale_flag(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-theme-stale.sqlite3")
    calculated_at = datetime_to_wire(utc_now() - timedelta(seconds=600))
    _insert_theme_snapshot(
        connection,
        theme_id="stale-leading",
        theme_name="오래된 주도",
        state="LEADING",
        calculated_at=calculated_at,
        total_trade_value=100_000_000,
    )

    snapshot = build_dashboard_snapshot(
        connection,
        Settings(theme_snapshot_stale_sec=300),
        limit=10,
    )
    connection.close()

    row = snapshot["themes"]["top_tradable_themes"][0]
    assert row["theme_id"] == "stale-leading"
    assert row["stale"] is True
    assert row["age_sec"] >= 300
    assert snapshot["themes"]["status"]["snapshot_stale_sec"] == 300


def test_dashboard_market_index_core_status_requires_fresh_kospi_and_kosdaq(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-index-readiness.sqlite3")
    old_ts = datetime_to_wire(utc_now() - timedelta(seconds=120))
    _insert_market_index_latest(connection, "KOSPI", old_ts)
    _insert_market_index_latest(connection, "KOSDAQ", old_ts)

    stale_snapshot = build_dashboard_snapshot(
        connection,
        Settings(market_index_stale_sec=1),
    )
    fresh_snapshot = build_dashboard_snapshot(
        connection,
        Settings(market_index_stale_sec=999_999_999),
    )
    connection.close()

    stale_status = stale_snapshot["market_indexes"]["status"]
    fresh_status = fresh_snapshot["market_indexes"]["status"]
    assert stale_status["latest_tick_count"] == 2
    assert stale_status["core_status"]["status"] != "READY"
    assert stale_status["core_status"]["status"] in {"DATA_WAIT", "DEGRADED"}
    assert fresh_status["core_status"]["status"] == "READY"
    assert set(fresh_status["core_status"]["quality_statuses"].values()) == {"FRESH"}


def test_dashboard_snapshot_with_sample_data_reflects_pipeline_rows(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-sample.sqlite3")
    settings = _fresh_settings()
    _build_observe_pipeline_fixture(connection, settings)
    _insert_dashboard_error_and_ai_rows(connection)

    snapshot = build_dashboard_snapshot(connection, settings, detail="full", limit=20)

    connection.close()
    assert snapshot["gateway"]["recent_event_count"] >= 3
    assert snapshot["pipeline_summary"]["market_data"]["latest_tick_count"] == 1
    assert "register_targets" in snapshot["realtime_subscription"]
    assert snapshot["pipeline_summary"]["themes"]["latest_snapshot_count"] >= 1
    assert snapshot["pipeline_summary"]["candidates"]["candidate_count"] >= 1
    assert snapshot["pipeline_summary"]["strategy"]["latest_observation_count"] >= 1
    assert snapshot["pipeline_summary"]["risk"]["latest_observation_count"] >= 1
    assert snapshot["pipeline_summary"]["condition_fusion"]["fused_code_count"] >= 1
    assert snapshot["condition_fusion"]["status"]["fused_code_count"] >= 1
    assert snapshot["condition_fusion"]["top_priority_codes"]
    assert "role별 admission" in snapshot["condition_fusion"]["summary"]["notice"]
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


def test_dashboard_pipeline_order_safety_blocks_on_order_command_row(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-order-safety.sqlite3")
    now = utc_now().isoformat()
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id,
            command_type,
            source,
            status,
            payload_json,
            payload_hash,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "cmd_send_order",
            "send_order",
            "test",
            "REJECTED",
            "{}",
            "hash",
            now,
        ),
    )

    snapshot = build_dashboard_snapshot(connection, Settings())
    connection.close()
    stage_statuses = {
        row["stage"]: row for row in snapshot["pipeline_summary"]["stage_statuses"]
    }

    assert snapshot["pipeline_summary"]["order_safety"]["order_command_count"] == 1
    assert snapshot["pipeline_summary"]["order_safety"]["command_type_counts"]["send_order"] == 1
    assert stage_statuses["OrderSafety"]["status"] == "BLOCK"
    assert stage_statuses["OrderSafety"]["count"] == 1
    assert "ORDER_COMMAND_ZERO_EXPECTED" in stage_statuses["OrderSafety"]["reason_codes"]


def test_dashboard_gateway_stage_blocks_when_realtime_callbacks_missing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-gateway-callback.sqlite3")
    append_gateway_event(
        connection,
        GatewayEvent(
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={
                "status": "ok",
                "kiwoom_logged_in": True,
                "server_mode": "SIMULATION",
                "condition_load_state": "LOADING",
                "registered_realtime_code_count": 2,
                "latest_realtime_registration_at": "2026-06-29T05:24:22Z",
                "latest_realtime_callback_at": "",
                "realtime_callback_count": 0,
                "realtime_recover_count": 2,
            },
        ),
    )

    snapshot = build_dashboard_snapshot(connection, Settings())
    connection.close()
    stage_statuses = {
        row["stage"]: row for row in snapshot["pipeline_summary"]["stage_statuses"]
    }

    assert snapshot["gateway"]["realtime_callback_count"] == 0
    assert stage_statuses["Gateway"]["status"] == "BLOCK"
    assert "REALTIME_CALLBACK_MISSING" in stage_statuses["Gateway"]["reason_codes"]


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
    condition = make_condition_event(
        code="005930",
        name="삼성전자",
        action="ENTER",
        condition_name="LeaderCondition",
        price=70000,
        metadata=_condition_profile_metadata("LEADER", "LeaderCondition", 90),
    )
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


def _insert_theme_snapshot(
    connection,
    *,
    theme_id: str,
    theme_name: str,
    state: str,
    calculated_at: str,
    total_trade_value: float = 0.0,
    trade_value_delta_3m: float = 0.0,
    trade_value_delta_1m: float = 0.0,
) -> None:
    snapshot_id = f"snapshot-{theme_id}"
    connection.execute(
        """
        INSERT INTO theme_snapshots (
            snapshot_id,
            theme_id,
            theme_name,
            calculated_at,
            member_count,
            active_member_count,
            observed_member_count,
            fresh_member_count,
            fresh_coverage_ratio,
            rising_member_count,
            rising_ratio,
            avg_change_rate,
            max_change_rate,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m,
            leading_code,
            leading_name,
            co_leader_codes_json,
            follower_codes_json,
            state,
            quality_status,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 3, 3, 3, 3, 1.0, 3, 1.0, 1.0, 1.0, ?, ?, ?, 0.0,
            '005930', '삼성전자', '[]', '[]', ?, 'FRESH', '[]', '{}')
        """,
        (
            snapshot_id,
            theme_id,
            theme_name,
            calculated_at,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            state,
        ),
    )
    connection.execute(
        """
        INSERT INTO theme_latest_snapshots (
            theme_id,
            snapshot_id,
            theme_name,
            calculated_at,
            state,
            quality_status,
            leading_code,
            leading_name,
            fresh_coverage_ratio,
            rising_ratio,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
            trade_value_delta_5m
        )
        VALUES (?, ?, ?, ?, ?, 'FRESH', '005930', '삼성전자', 1.0, 1.0, ?, ?, ?, 0.0)
        """,
        (
            theme_id,
            snapshot_id,
            theme_name,
            calculated_at,
            state,
            total_trade_value,
            trade_value_delta_1m,
            trade_value_delta_3m,
        ),
    )
    connection.commit()


def _insert_market_index_latest(connection, index_code: str, event_ts: str) -> None:
    connection.execute(
        """
        INSERT INTO market_index_ticks_latest (
            index_code,
            index_name,
            price,
            change_rate,
            change_value,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            metadata_json,
            updated_at
        )
        VALUES (?, ?, 2800.0, 0.1, 2.8, ?, ?, ?, 'test', ?, 'FRESH', '{}', ?)
        """,
        (
            index_code,
            index_code,
            event_ts,
            event_ts,
            event_ts,
            f"evt-{index_code}",
            event_ts,
        ),
    )
    connection.commit()


def _append_and_project(connection, event, settings: Settings) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"


def _condition_profile_metadata(role: str, condition_name: str, priority: int) -> dict[str, object]:
    return {
        "sensor_evidence": True,
        "not_buy_signal": True,
        "condition_profile_id": f"profile-{condition_name}",
        "condition_role": role,
        "condition_profile": {
            "profile_id": f"profile-{condition_name}",
            "condition_name": condition_name,
            "role": role,
            "priority": priority,
            "ttl_sec": 999_999_999,
            "enabled": True,
            "price_subscribe_policy": "immediate",
        },
        "condition_admission": {
            "subscribed": True,
            "reason_codes": ["TEST"],
        },
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
