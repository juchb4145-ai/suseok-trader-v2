from __future__ import annotations

from pathlib import Path

from apps.core_api import app
from tests.support_fastapi_routes import iter_app_routes

ROOT = Path(__file__).resolve().parents[1]


def test_dry_run_oms_surface_has_no_live_order_endpoint() -> None:
    paths = {route.path for route in iter_app_routes(app)}

    assert "/api/dry-run/status" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)


def test_dry_run_oms_code_has_no_broker_or_gateway_order_path() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            ROOT / "services" / "oms" / "dry_run_service.py",
            ROOT / "services" / "oms" / "safety_gate.py",
            ROOT / "api" / "routes" / "dry_run_oms.py",
            *sorted((ROOT / "domain" / "oms").glob("*.py")),
        ]
    )

    assert "PyQt5" not in source
    assert "QAxWidget" not in source
    assert "send_order" not in source
    assert "cancel_order" not in source
    assert "modify_order" not in source
    assert "BrokerOrderRequest" not in source
    assert "GatewayCommand(" not in source
    assert "class Live" not in source
    assert "LIVE_ORDER" not in source
    assert "REAL_ORDER" not in source
    assert "READY_TO_SEND" not in source
