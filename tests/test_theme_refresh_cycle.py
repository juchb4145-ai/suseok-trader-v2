from __future__ import annotations

from datetime import timedelta

import pytest
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.market_scan_service import process_market_scan_event
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    EvaluationRunLockError,
)
from services.runtime.theme_refresh_cycle import (
    THEME_REFRESH_LOCK,
    run_theme_refresh_cycle_once,
)
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
from storage.sqlite import initialize_database


def test_theme_refresh_cycle_updates_snapshots_without_order_commands(tmp_path) -> None:
    connection, settings = _prepared_theme_refresh_connection(
        tmp_path / "theme-refresh.sqlite3"
    )
    initial = calculate_theme_snapshot(
        connection,
        "semiconductor",
        calculated_at="2026-06-26T00:00:00Z",
        settings=settings,
    )

    result = run_theme_refresh_cycle_once(
        connection,
        settings=settings,
        queue_market_scan_commands=False,
        queue_realtime_commands=False,
    )
    latest = connection.execute(
        "SELECT calculated_at FROM theme_latest_snapshots WHERE theme_id = 'semiconductor'"
    ).fetchone()
    order_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
        """
    ).fetchone()["count"]
    connection.close()

    payload = result.to_dict()
    assert result.status == "COMPLETED"
    assert payload["order_command_delta"] == {
        "cancel_order": 0,
        "modify_order": 0,
        "send_order": 0,
    }
    assert order_count == 0
    assert latest["calculated_at"] != initial.to_dict(include_members=False)["calculated_at"]


def test_theme_refresh_uses_separate_lock_from_evaluation_pipeline(tmp_path) -> None:
    connection, settings = _prepared_theme_refresh_connection(
        tmp_path / "theme-refresh-separate-lock.sqlite3"
    )
    _insert_runtime_lock(
        connection,
        lock_name=EVALUATION_PIPELINE_LOCK,
        owner_id="live-sim-operating",
    )

    result = run_theme_refresh_cycle_once(
        connection,
        settings=settings,
        queue_market_scan_commands=False,
        queue_realtime_commands=False,
    )
    lock_rows = connection.execute(
        "SELECT lock_name FROM runtime_execution_locks ORDER BY lock_name"
    ).fetchall()
    connection.close()

    assert result.status == "COMPLETED"
    assert [row["lock_name"] for row in lock_rows] == [EVALUATION_PIPELINE_LOCK]


def test_theme_refresh_lock_blocks_duplicate_refresh(tmp_path) -> None:
    connection, settings = _prepared_theme_refresh_connection(
        tmp_path / "theme-refresh-duplicate-lock.sqlite3"
    )
    _insert_runtime_lock(
        connection,
        lock_name=THEME_REFRESH_LOCK,
        owner_id="theme-refresh-running",
    )

    with pytest.raises(EvaluationRunLockError) as exc_info:
        run_theme_refresh_cycle_once(
            connection,
            settings=settings,
            queue_market_scan_commands=False,
            queue_realtime_commands=False,
        )
    connection.close()

    assert exc_info.value.lock_name == THEME_REFRESH_LOCK
    assert exc_info.value.owner_id == "theme-refresh-running"


def _prepared_theme_refresh_connection(db_path):
    connection = initialize_database(db_path)
    settings = Settings(
        market_scan_enabled=True,
        realtime_subscription_queue_commands=False,
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
    )
    import_theme_memberships(
        connection,
        {
            "source_type": "MOCK",
            "source_name": "refresh_fixture",
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
    _project_scan(connection, "scan-refresh-1", settings=settings)
    return connection, settings


def _project_scan(connection, suffix: str, *, settings: Settings) -> None:
    event = GatewayEvent(
        event_id=f"evt_{suffix}",
        event_type="tr_response",
        source="mock_gateway",
        payload={
            "request_id": f"market_scan:TRADE_VALUE:KOSPI:{suffix}",
            "tr_code": "OPT10032",
            "request_name": "market_scan_trade_value_kospi",
            "success": True,
            "rows": [
                {
                    "code": "005930",
                    "name": "삼성전자",
                    "rank": 1,
                    "price": 70_000,
                    "change_rate": 2.0,
                    "trade_value": 500_000_000,
                    "volume": 10_000,
                },
                {
                    "code": "000660",
                    "name": "SK하이닉스",
                    "rank": 2,
                    "price": 120_000,
                    "change_rate": 1.5,
                    "trade_value": 400_000_000,
                    "volume": 8_000,
                },
            ],
        },
    )
    result = process_market_scan_event(connection, event, settings=settings)
    assert result.status == "APPLIED"


def _insert_runtime_lock(
    connection,
    *,
    lock_name: str,
    owner_id: str,
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO runtime_execution_locks (
            lock_name,
            owner_id,
            acquired_at,
            expires_at,
            detail_json
        )
        VALUES (?, ?, ?, ?, '{}')
        """,
        (
            lock_name,
            owner_id,
            datetime_to_wire(now),
            datetime_to_wire(now + timedelta(seconds=300)),
        ),
    )
    connection.commit()
