from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tools.monitor_live_sim_lifecycle import evaluate_alerts


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def test_monitor_alerts_for_stale_gateway_order_and_reconcile_block() -> None:
    now = datetime(2026, 7, 7, 1, 0, tzinfo=UTC)
    snapshot = {
        "now": _wire(now),
        "gateway_status": {
            "last_heartbeat_at": _wire(now - timedelta(seconds=45)),
        },
        "active_orders": [
            {
                "live_sim_order_id": "order-stale",
                "code": "005930",
                "status": "BROKER_ACKED",
                "created_at": _wire(now - timedelta(seconds=400)),
            }
        ],
        "latest_reconcile": {
            "reconcile_id": "reconcile-block",
            "status": "RECONCILE_MISMATCH",
            "mismatch_count": 1,
            "blocking_new_buy": True,
        },
    }

    alerts = evaluate_alerts(
        snapshot,
        gateway_heartbeat_stale_sec=30,
        stale_order_sec=300,
    )

    assert {alert["key"] for alert in alerts} == {
        "gateway_heartbeat_stale",
        "live_sim_stale_active_orders",
        "live_sim_reconcile_blocks_buy",
    }

