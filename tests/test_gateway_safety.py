from __future__ import annotations

import re
from pathlib import Path

from apps.core_api import app

ROOT = Path(__file__).resolve().parents[1]
CODE_DIRS = ("api", "apps", "domain", "gateway", "infrastructure", "services", "storage", "tools")


def iter_project_python_files() -> list[Path]:
    paths: list[Path] = []
    for directory in CODE_DIRS:
        paths.extend((ROOT / directory).rglob("*.py"))
    return paths


def project_python_source() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8") for path in iter_project_python_files()
    )


def test_core_has_no_kiwoom_or_pyqt_imports() -> None:
    combined_source = project_python_source()

    assert "PyQt5" not in combined_source
    assert "QAxWidget" not in combined_source
    assert "Kiwoom OpenAPI" not in combined_source


def test_core_has_no_openai_api_call_path() -> None:
    combined_source = project_python_source()

    assert "import openai" not in combined_source
    assert "from openai" not in combined_source
    assert "OpenAI(" not in combined_source


def test_order_execution_apis_and_order_intent_are_not_exposed() -> None:
    paths = {route.path for route in app.routes}
    combined_source = project_python_source()

    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)
    assert "class OrderIntent" not in combined_source


def test_order_execution_functions_strategy_risk_and_oms_are_not_implemented() -> None:
    combined_source = project_python_source()

    order_function_pattern = r"def\s+(send_order|submit_order|cancel_order|modify_order)\b"

    assert re.search(order_function_pattern, combined_source) is None
    assert "class StrategyEngine" not in combined_source
    assert "class RiskGate" not in combined_source
    assert "class OMS" not in combined_source
    assert "class Oms" not in combined_source


def test_pr5_does_not_add_candidate_or_ai_execution_surfaces() -> None:
    paths = {route.path for route in app.routes}
    combined_source = project_python_source()

    assert "/api/candidates" not in paths
    assert "class CandidateFSM" not in combined_source
    assert "class CandidateStateMachine" not in combined_source
    assert "class AISidecarContextBuilder" not in combined_source
    assert "def build_ai_sidecar_context" not in combined_source
    assert "def call_openai" not in combined_source
