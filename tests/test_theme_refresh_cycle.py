from __future__ import annotations

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.market_scan_service import process_market_scan_event
from services.runtime.theme_refresh_cycle import run_theme_refresh_cycle_once
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
from storage.sqlite import initialize_database


def test_theme_refresh_cycle_updates_snapshots_without_order_commands(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme-refresh.sqlite3")
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
