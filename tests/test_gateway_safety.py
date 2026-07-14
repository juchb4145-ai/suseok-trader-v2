from __future__ import annotations

import re
from pathlib import Path

from apps.core_api import app
from tests.support_fastapi_routes import iter_app_routes

ROOT = Path(__file__).resolve().parents[1]
CODE_DIRS = ("api", "apps", "domain", "gateway", "infrastructure", "services", "storage", "tools")
CORE_CODE_DIRS = ("api", "domain", "infrastructure", "services", "storage", "tools")


def iter_project_python_files() -> list[Path]:
    paths: list[Path] = []
    for directory in CODE_DIRS:
        paths.extend((ROOT / directory).rglob("*.py"))
    return paths


def iter_core_python_files() -> list[Path]:
    paths: list[Path] = []
    for directory in CORE_CODE_DIRS:
        paths.extend((ROOT / directory).rglob("*.py"))
    return paths


def project_python_source(*, core_only: bool = False) -> str:
    paths = iter_core_python_files() if core_only else iter_project_python_files()
    return "\n".join(
        path.read_text(encoding="utf-8") for path in paths
    )


def test_core_has_no_kiwoom_or_pyqt_imports() -> None:
    combined_source = project_python_source(core_only=True)

    assert "PyQt5" not in combined_source
    assert "QAxWidget" not in combined_source
    assert "Kiwoom OpenAPI" not in combined_source


def test_core_has_no_openai_api_call_path() -> None:
    combined_source = project_python_source()

    assert "import openai" not in combined_source
    assert "from openai" not in combined_source
    assert "OpenAI(" not in combined_source


def test_order_execution_apis_and_order_intent_are_not_exposed() -> None:
    paths = {route.path for route in iter_app_routes(app)}
    combined_source = project_python_source()

    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)
    assert "class OrderIntent" not in combined_source


def test_order_execution_functions_strategy_risk_and_oms_are_not_implemented() -> None:
    combined_source = project_python_source(core_only=True)

    order_function_pattern = r"def\s+(send_order|submit_order|cancel_order|modify_order)\b"

    assert re.search(order_function_pattern, combined_source) is None
    assert "class StrategyEngine" not in combined_source
    assert "class RiskGate" not in combined_source
    assert "class OMS" not in combined_source
    assert "class Oms" not in combined_source


def test_pr6_candidate_surface_is_observe_only_and_no_ai_execution_surfaces() -> None:
    paths = {route.path for route in iter_app_routes(app)}
    combined_source = project_python_source()

    assert "/api/candidates" in paths
    assert "/api/candidates/rebuild" in paths
    assert "class CandidateFSM" not in combined_source
    assert "class CandidateStateMachine" not in combined_source
    assert "class AISidecarContextBuilder" not in combined_source
    assert "def build_ai_sidecar_context" not in combined_source
    assert "def call_openai" not in combined_source
    candidate_service_source = (ROOT / "services" / "candidate_service.py").read_text(
        encoding="utf-8"
    )
    assert "GatewayCommand" not in candidate_service_source
