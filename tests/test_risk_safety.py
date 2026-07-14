from __future__ import annotations

from pathlib import Path

from apps.core_api import app
from tests.support_fastapi_routes import iter_app_routes

ROOT = Path(__file__).resolve().parents[1]


def test_risk_surface_exposes_observation_routes_only() -> None:
    paths = {route.path for route in iter_app_routes(app)}

    assert "/api/risk/status" in paths
    assert "/api/risk/evaluate" in paths
    assert "/api/risk/observations/latest" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)


def test_risk_code_has_no_execution_or_ai_call_path() -> None:
    risk_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            ROOT / "services" / "risk_gate.py",
            ROOT / "api" / "routes" / "risk.py",
            *sorted((ROOT / "domain" / "risk").glob("*.py")),
        ]
    )

    assert "GatewayCommand" not in risk_source
    assert "send_order" not in risk_source
    assert "cancel_order" not in risk_source
    assert "modify_order" not in risk_source
    assert "class OrderIntent" not in risk_source
    assert "class EntryPlan" not in risk_source
    assert "class PositionSizing" not in risk_source
    assert "class OMS" not in risk_source
    assert "class Oms" not in risk_source
    assert "BUY_ALLOWED" not in risk_source
    assert "ORDER_ALLOWED" not in risk_source
    assert "TRADE_ALLOWED" not in risk_source
    assert "READY_TO_ORDER" not in risk_source
    assert "import openai" not in risk_source
    assert "from openai" not in risk_source
    assert "OpenAI(" not in risk_source
