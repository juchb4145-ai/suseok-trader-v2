from __future__ import annotations

from pathlib import Path

from apps.core_api import app
from tests.support_fastapi_routes import iter_app_routes

ROOT = Path(__file__).resolve().parents[1]


def test_strategy_surface_exposes_observation_routes_only() -> None:
    paths = {route.path for route in iter_app_routes(app)}

    assert "/api/strategy/status" in paths
    assert "/api/strategy/evaluate" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)


def test_strategy_code_has_no_execution_or_ai_call_path() -> None:
    strategy_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            ROOT / "services" / "strategy_engine.py",
            ROOT / "api" / "routes" / "strategy.py",
            *sorted((ROOT / "domain" / "strategy").glob("*.py")),
        ]
    )

    assert "GatewayCommand" not in strategy_source
    assert "send_order" not in strategy_source
    assert "cancel_order" not in strategy_source
    assert "modify_order" not in strategy_source
    assert "BUY_READY" not in strategy_source
    assert "ENTRY_READY" not in strategy_source
    assert "ORDER_READY" not in strategy_source
    assert "READY_TO_BUY" not in strategy_source
    assert "ORDER_INTENT" not in strategy_source
    assert "import openai" not in strategy_source
    assert "from openai" not in strategy_source
    assert "OpenAI(" not in strategy_source
