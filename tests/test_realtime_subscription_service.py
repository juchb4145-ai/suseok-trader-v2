from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.realtime_subscription import (
    build_realtime_subscription_plan,
    run_realtime_subscription_once,
)
from storage.sqlite import initialize_database

TRADE_DATE = "2026-06-30"


def test_plan_selects_condition_candidate_and_theme_targets_from_anchor_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-plan.sqlite3")
    settings = _settings(realtime_subscription_max_total=10, realtime_subscription_max_per_theme=3)
    _insert_condition_latest(connection, "035420", "NAVER")
    _insert_candidate(connection, "035720", "카카오", state="WATCHING")
    _insert_theme_latest(connection, "ai", "AI", [("123456", "AI부품")])

    plan = build_realtime_subscription_plan(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660"],
    ).to_dict()
    connection.close()

    register_codes = {target["code"] for target in plan["register_targets"]}
    assert {"035420", "035720", "123456"}.issubset(register_codes)
    assert plan["counts"]["condition_count"] == 1
    assert plan["counts"]["candidate_count"] == 1
    assert plan["counts"]["theme_watchset_count"] >= 1
    assert plan["queue_commands"] is False
    assert plan["command_count"] == 0


def test_already_registered_candidate_does_not_create_duplicate_command(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-duplicate.sqlite3")
    settings = _settings(realtime_subscription_max_total=10)
    _insert_candidate(connection, "035720", "카카오", state="CONTEXT_READY")

    plan = run_realtime_subscription_once(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660", "035720"],
        queue_commands=True,
    ).to_dict()
    command_count = _gateway_command_count(connection)
    connection.close()

    assert "035720" not in {target["code"] for target in plan["register_targets"]}
    assert any(target["code"] == "035720" for target in plan["keep_targets"])
    assert command_count == 0
    assert plan["command_count"] == 0


def test_queue_false_does_not_insert_gateway_command_rows(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-plan-only.sqlite3")
    settings = _settings(realtime_subscription_queue_commands=True)
    _insert_candidate(connection, "035720", "카카오", state="WATCHING")

    plan = run_realtime_subscription_once(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660"],
        queue_commands=False,
    ).to_dict()
    command_count = _gateway_command_count(connection)
    connection.close()

    assert plan["register_targets"]
    assert plan["queue_commands"] is False
    assert plan["command_count"] == 0
    assert command_count == 0


def test_queue_true_creates_only_register_realtime_commands(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-queue.sqlite3")
    settings = _settings(realtime_subscription_max_total=10)
    _insert_candidate(connection, "035720", "카카오", state="DATA_WAIT")

    plan = run_realtime_subscription_once(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660"],
        queue_commands=True,
    ).to_dict()
    rows = connection.execute(
        "SELECT command_type, source, payload_json FROM gateway_commands"
    ).fetchall()
    connection.close()

    assert plan["command_count"] == 1
    assert {row["command_type"] for row in rows} == {"register_realtime"}
    assert all("send_order" not in row["command_type"] for row in rows)
    assert all('"observe_only":true' in row["payload_json"] for row in rows)


def test_max_total_and_max_per_theme_limits_are_applied(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-limits.sqlite3")
    settings = _settings(realtime_subscription_max_total=3, realtime_subscription_max_per_theme=1)
    _insert_condition_latest(connection, "035420", "NAVER")
    _insert_candidate(connection, "035720", "카카오", state="WATCHING")
    _insert_theme_latest(
        connection,
        "robot",
        "로봇",
        [("111111", "로봇A"), ("222222", "로봇B")],
    )

    plan = build_realtime_subscription_plan(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660"],
    ).to_dict()
    connection.close()

    assert len(plan["register_targets"]) == 1
    assert any(
        "MAX_TOTAL_REACHED" in item["reason_codes"] for item in plan["near_miss"]
    )
    assert any(
        "MAX_PER_THEME_REACHED" in item["reason_codes"] for item in plan["near_miss"]
    )


def test_stale_remove_is_plan_only_when_remove_disabled(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-remove.sqlite3")
    settings = _settings(
        realtime_subscription_max_total=10,
        realtime_subscription_queue_commands=True,
        realtime_subscription_allow_remove=False,
        realtime_subscription_stale_sec=10,
        realtime_subscription_remove_stale_after_sec=60,
    )
    _insert_stale_tick(connection, "039490", "키움증권")

    plan = run_realtime_subscription_once(
        connection,
        settings=settings,
        trade_date=TRADE_DATE,
        registered_codes=["005930", "000660", "039490"],
        queue_commands=True,
    ).to_dict()
    rows = connection.execute(
        "SELECT command_type FROM gateway_commands ORDER BY created_at"
    ).fetchall()
    connection.close()

    assert any(target["code"] == "039490" for target in plan["remove_targets"])
    assert plan["command_count"] == 0
    assert rows == []
    assert "REMOVE_COMMAND_DISABLED" in plan["reason_summary"]


def _settings(**overrides) -> Settings:
    values = {
        "realtime_subscription_stale_sec": 999_999_999,
        "realtime_subscription_remove_stale_after_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _insert_condition_latest(connection, code: str, name: str) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_condition_latest (
            condition_id,
            code,
            condition_name,
            name,
            action,
            price,
            event_ts,
            received_at,
            source,
            event_id,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 'ENTER', 10000, ?, ?, 'test', ?, '{}')
        """,
        ("cond-1", code, "돌파", name, now, now, f"evt-cond-{code}"),
    )
    connection.commit()


def _insert_candidate(connection, code: str, name: str, *, state: str) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            previous_state,
            detected_at,
            last_seen_at,
            state_updated_at,
            closed_at,
            primary_source_type,
            primary_source_id,
            source_count,
            active_source_count,
            theme_id,
            theme_name,
            theme_state,
            theme_role,
            market_readiness_status,
            tick_age_sec,
            vwap_ready,
            bar_1m_ready,
            bar_3m_ready,
            bar_5m_ready,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, ?, NULL, 'TEST', ?, 1, 1,
                NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, '[]', '{}')
        """,
        (f"CAND-{code}", TRADE_DATE, code, name, state, now, now, now, f"src-{code}"),
    )
    connection.commit()


def _insert_theme_latest(connection, theme_id: str, theme_name: str, members) -> None:
    snapshot_id = f"snap-{theme_id}"
    now = datetime_to_wire(utc_now())
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
        VALUES (?, ?, ?, ?, 'DATA_WAIT', 'DATA_WAIT', NULL, NULL, 0, 0, 0, 0, 0, 0)
        """,
        (theme_id, snapshot_id, theme_name, now),
    )
    for index, (code, name) in enumerate(members, start=1):
        connection.execute(
            """
            INSERT INTO theme_members (
                theme_id,
                code,
                name,
                source_type,
                source_name,
                active,
                weight,
                metadata_json
            )
            VALUES (?, ?, ?, 'MOCK', 'test', 1, 1.0, ?)
            """,
            (
                theme_id,
                code,
                name,
                json.dumps({"rank": index}, ensure_ascii=False),
            ),
        )
        connection.execute(
            """
            INSERT INTO theme_snapshot_members (
                snapshot_id,
                theme_id,
                code,
                name,
                readiness_status,
                member_role,
                calculated_at,
                metadata_json
            )
            VALUES (?, ?, ?, ?, 'MISSING', 'UNKNOWN', ?, '{}')
            """,
            (snapshot_id, theme_id, code, name, now),
        )
    connection.commit()


def _insert_stale_tick(connection, code: str, name: str) -> None:
    old = datetime_to_wire(utc_now() - timedelta(seconds=3_600))
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
            name,
            price,
            change_rate,
            cumulative_volume,
            cumulative_trade_value,
            execution_strength,
            best_bid,
            best_ask,
            spread_ticks,
            day_high,
            day_low,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status
        )
        VALUES (?, ?, 10000, 0, 0, 0, 0, 9990, 10000, 1, 10000, 9900,
                ?, ?, ?, 'test', ?, 'STALE')
        """,
        (code, name, old, old, old, f"evt-tick-{code}"),
    )
    connection.commit()


def _gateway_command_count(connection) -> int:
    return int(
        connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()["count"]
    )
