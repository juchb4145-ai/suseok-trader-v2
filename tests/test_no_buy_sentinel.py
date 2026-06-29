from __future__ import annotations

import json
from pathlib import Path

from domain.broker.utils import datetime_to_wire, utc_now
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.ai_advisory.storage import save_candidate_scores, save_scoring_run
from services.config import Settings
from services.operator.no_buy_sentinel import build_no_buy_sentinel_snapshot
from services.operator.reason_classifier import aggregate_reason_summary, classify_reason
from services.theme_service import import_theme_memberships
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
    _update_plan,
)
from tests.test_theme_leadership_service import (
    _append_and_project,
    _theme_payload,
)
from tests.test_theme_leadership_service import (
    _settings as _theme_settings,
)


def test_no_buy_schema_config_and_reason_classifier(tmp_path) -> None:
    connection = initialize_database(tmp_path / "no-buy-schema.sqlite3")
    tables = {
        row["name"]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    connection.close()
    settings = Settings()
    classification = classify_reason("LIVE_SIM_RECONCILE_MISMATCH_BLOCK")
    summary = aggregate_reason_summary(
        ["AI_NO_TRADE", "ORDER_PLAN_NOT_READY", "UNEXPECTED_REASON"]
    )

    assert "no_buy_sentinel_snapshots" in tables
    assert settings.no_buy_sentinel_enabled is True
    assert settings.no_buy_sentinel_write_snapshots is True
    assert classification.stage == "RECONCILE"
    assert summary["reason_counts"]["AI_NO_TRADE"] == 1
    assert classify_reason("UNEXPECTED_REASON").stage == "UNKNOWN"


def test_ai_selected_empty_becomes_ai_no_trade(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "ai-no-trade.sqlite3")
    _save_ai_run(connection, selected=[])

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=_pilot_settings(ai_candidate_scorer_enabled=True),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "AI_NO_TRADE"
    assert snapshot["ai_summary"]["classification"] == "AI_NO_TRADE"
    assert snapshot["no_buy_detected"] is True


def test_ai_interest_does_not_override_live_sim_safety_block(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(tmp_path / "ai-system.sqlite3")
    _save_ai_run(connection, selected=["005930"], order_plan_id=order_plan_id)

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=_pilot_settings(
            ai_candidate_scorer_enabled=True,
            live_sim_kill_switch=True,
        ),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "LIVE_SIM_SAFETY_BLOCK"
    assert snapshot["ai_summary"]["classification"] == "SYSTEM_BLOCK_WITH_AI_INTEREST"
    assert snapshot["top_near_miss"][0]["ai_selected"] is True


def test_plan_ready_config_reconcile_and_duplicate_statuses(tmp_path) -> None:
    config_conn, _ = _prepared_order_plan_connection(tmp_path / "config.sqlite3")
    config_snapshot = build_no_buy_sentinel_snapshot(
        config_conn,
        settings=_pilot_settings(live_sim_order_plan_routing_enabled=False),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    config_conn.close()

    reconcile_conn, _ = _prepared_order_plan_connection(tmp_path / "reconcile.sqlite3")
    _insert_reconcile_block(reconcile_conn)
    reconcile_snapshot = build_no_buy_sentinel_snapshot(
        reconcile_conn,
        settings=_pilot_settings(),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    reconcile_conn.close()

    dup_conn, _ = _prepared_order_plan_connection(tmp_path / "duplicate.sqlite3")
    _insert_open_position(dup_conn)
    duplicate_snapshot = build_no_buy_sentinel_snapshot(
        dup_conn,
        settings=_pilot_settings(),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    dup_conn.close()

    assert config_snapshot["status"] == "CONFIG_DISABLED"
    assert reconcile_snapshot["status"] == "RECONCILE_BLOCK"
    assert duplicate_snapshot["status"] == "DUPLICATE_OR_POSITION_BLOCK"


def test_entry_timing_and_theme_data_wait_statuses(tmp_path) -> None:
    entry_conn, order_plan_id = _prepared_order_plan_connection(tmp_path / "entry-wait.sqlite3")
    _update_plan(entry_conn, order_plan_id, status="DATA_WAIT")
    entry_snapshot = build_no_buy_sentinel_snapshot(
        entry_conn,
        settings=_pilot_settings(),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    entry_conn.close()

    theme_conn = initialize_database(tmp_path / "theme-wait.sqlite3")
    theme_settings = _theme_settings()
    import_theme_memberships(
        theme_conn,
        _theme_payload(
            "condition_theme",
            "조건테마",
            [("005930", "삼성전자"), ("000660", "SK하이닉스")],
        ),
    )
    _append_and_project(
        theme_conn,
        make_condition_event(code="005930", name="삼성전자"),
        theme_settings,
    )
    theme_snapshot = build_no_buy_sentinel_snapshot(
        theme_conn,
        settings=theme_settings,
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    theme_conn.close()

    assert entry_snapshot["status"] == "ENTRY_TIMING_WAIT"
    assert theme_snapshot["status"] == "THEME_DATA_WAIT"


def test_market_no_trade_is_separated_from_theme_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-no-trade.sqlite3")
    settings = _theme_settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "weak_theme",
            "약세테마",
            [("005930", "삼성전자"), ("000660", "SK하이닉스")],
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="005930", name="삼성전자", change_rate=-2.0),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="000660", name="SK하이닉스", price=120000, change_rate=-1.0),
        settings,
    )

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "MARKET_NO_TRADE"
    assert snapshot["stage_summary"]["theme"]["state_counts"]["WEAK"] == 1


def test_gateway_realtime_stalled_is_separated_from_theme_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "realtime-stalled.sqlite3")
    settings = _theme_settings(
        market_data_tick_stale_sec=10,
        market_data_degraded_tick_stale_sec=30,
    )
    import_theme_memberships(
        connection,
        _theme_payload(
            "condition_theme",
            "조건테마",
            [("005930", "삼성전자"), ("000660", "SK하이닉스")],
        ),
    )
    _upsert_gateway_status(connection, "last_heartbeat_at", datetime_to_wire(utc_now()))
    _upsert_gateway_status(connection, "gateway_orderable", "true")
    _upsert_gateway_status(connection, "command_queue_healthy", "true")
    _upsert_gateway_status(connection, "registered_realtime_code_count", "2")
    connection.commit()

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "GATEWAY_REALTIME_STALLED"
    assert snapshot["system_summary"]["gateway"]["realtime_stalled"] is True
    assert "GATEWAY_REALTIME_STALLED" in snapshot["reason_summary"]["reason_counts"]


def test_gateway_unavailable_overrides_theme_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "gateway-unavailable.sqlite3")
    settings = _theme_settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "condition_theme",
            "조건테마",
            [("005930", "삼성전자"), ("000660", "SK하이닉스")],
        ),
    )
    _append_and_project(
        connection,
        make_condition_event(code="005930", name="삼성전자"),
        settings,
    )
    _upsert_gateway_status(connection, "last_heartbeat_at", datetime_to_wire(utc_now()))
    _upsert_gateway_status(connection, "gateway_orderable", "false")
    _upsert_gateway_status(connection, "command_queue_healthy", "true")
    connection.commit()

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "GATEWAY_UNAVAILABLE"
    assert snapshot["system_summary"]["gateway"]["unavailable"] is True


def test_ai_unavailable_is_not_system_block_and_unknown_falls_back(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "ai-unavailable.sqlite3")
    _save_ai_run(connection, selected=[], status="TIMEOUT", no_trade_reason=None)

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=_pilot_settings(ai_candidate_scorer_enabled=True),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["ai_summary"]["classification"] == "AI_UNAVAILABLE"
    assert snapshot["status"] != "AI_NO_TRADE"
    assert classify_reason("NOT_A_KNOWN_REASON").block_type == "NOT_APPLICABLE"


def test_live_sim_order_activity_clears_no_buy(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "order-activity.sqlite3")
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            filled_quantity,
            remaining_quantity,
            idempotency_key,
            created_at
        )
        VALUES ('order-activity', 'intent-activity', '2026-06-27', 'SIM-12345678',
            '005930', '삼성전자', 'BUY', 'LIMIT', 1, 97000, 97000, 'COMMAND_QUEUED',
            0, 1, 'key-order-activity', ?)
        """,
        (now,),
    )
    connection.commit()

    snapshot = build_no_buy_sentinel_snapshot(
        connection,
        settings=_pilot_settings(),
        trade_date="2026-06-27",
        manual=True,
        write_snapshot=False,
    ).to_dict()
    connection.close()

    assert snapshot["status"] == "OK_TRADING_ACTIVITY"
    assert snapshot["no_buy_detected"] is False


def test_operator_core_has_no_kiwoom_or_order_creation_imports() -> None:
    root = Path("services/operator")
    forbidden = (
        "PyQt5",
        "QAxWidget",
        "Kiwoom",
        "send_order",
        "cancel_order",
        "modify_order",
        "GatewayCommand(",
        "OrderIntent",
        "LiveSimIntent(",
    )
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(token in text for token in forbidden), path


def _save_ai_run(
    connection,
    *,
    selected: list[str],
    status: str = "COMPLETED",
    no_trade_reason: str | None = "AI 관망",
    order_plan_id: str | None = None,
) -> None:
    save_scoring_run(
        connection,
        run_id=f"ai-run-{status.lower()}-{len(selected)}",
        trade_date="2026-06-27",
        provider="mock",
        model="mock-model",
        status=status,
        candidate_count=1,
        selected_count=len(selected),
        prompt_hash=None,
        raw_response_hash=None,
        summary="AI summary",
        no_trade_reason=no_trade_reason,
    )
    if selected:
        save_candidate_scores(
            connection,
            run_id=f"ai-run-{status.lower()}-{len(selected)}",
            candidates=[
                {
                    "code": "005930",
                    "candidate_instance_id": "CAND-2026-06-27-005930-1",
                    "order_plan_id": order_plan_id,
                }
            ],
            advisory={
                "selected": selected,
                "score": {"005930": 91},
                "confidence": {"005930": 84},
                "analysis": {"005930": "관심"},
                "avoid": {},
                "candidate_flags": {"005930": ["LEADER"]},
            },
        )
    connection.commit()


def _insert_reconcile_block(connection) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id,
            account_id,
            trade_date,
            mismatch_count,
            status,
            snapshot_json,
            created_at,
            blocking_new_buy,
            allow_exit
        )
        VALUES ('reconcile-block', 'SIM-12345678', '2026-06-27', 1,
            'RECONCILE_MISMATCH', ?, ?, 1, 1)
        """,
        (json.dumps({"blocking_new_buy": True}), now),
    )
    connection.commit()


def _insert_open_position(connection) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id,
            account_id,
            trade_date,
            code,
            name,
            quantity,
            available_quantity,
            avg_entry_price,
            total_entry_notional,
            opened_at,
            status,
            created_at,
            updated_at
        )
        VALUES ('position-open', 'SIM-12345678', '2026-06-26', '005930', '삼성전자',
            1, 1, 97000, 97000, ?, 'OPEN', ?, ?)
        """,
        (now, now, now),
    )
    connection.commit()


def _upsert_gateway_status(connection, key: str, value: str) -> None:
    connection.execute(
        """
        INSERT INTO gateway_status (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, value, datetime_to_wire(utc_now())),
    )
