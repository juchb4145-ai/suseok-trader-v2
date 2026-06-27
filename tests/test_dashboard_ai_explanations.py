from __future__ import annotations

import json
from pathlib import Path

from apps.core_api import app
from domain.ai_sidecar.rca import AIRCAReport, AIRCAReportStatus, AIRCASection
from fastapi.testclient import TestClient
from services.ai_sidecar.context_store import save_context_build_error
from services.ai_sidecar.rca_report_store import save_rca_report, save_rca_report_error
from services.ai_sidecar.request_store import AIRequestStatus, create_ai_request
from services.config import Settings
from services.dashboard_ai_explanations import build_ai_explanation_cards
from services.dashboard_ai_labels import (
    map_ai_severity_label,
    map_ai_status_label,
    map_card_type_label,
    map_rca_category_label,
    map_readonly_operator_action,
)
from services.dashboard_service import build_dashboard_snapshot
from storage.sqlite import initialize_database, open_connection

ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_ai_explanation_service_builds_cards_and_counts(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-ai-cards.sqlite3")
    _save_report(connection, report_id="rca-no-trade", report_type="NO_TRADE_RCA")
    _save_report(
        connection,
        report_id="rca-candidate",
        report_type="CANDIDATE_BLOCK_RCA",
        related_entity_type="candidate",
        related_entity_id="candidate-1",
    )
    _insert_insight(connection, operator_action="REVIEW_ONLY")
    create_ai_request(
        connection,
        request_id="ai-req-invalid",
        task_type="NO_TRADE_RCA",
        status=AIRequestStatus.AI_OUTPUT_INVALID,
        error_message="schema mismatch",
    )
    save_context_build_error(
        connection,
        task_type="NO_TRADE_RCA",
        trade_date="2026-06-27",
        error_message="context fixture error",
    )
    save_rca_report_error(
        connection,
        report_type="CANDIDATE_BLOCK_RCA",
        related_entity_type="candidate",
        related_entity_id="candidate-1",
        error_message="rca fixture error",
    )

    payload = build_ai_explanation_cards(connection, Settings(), limit=20)
    connection.close()
    cards = payload["cards"]
    card_types = {card["card_type"] for card in cards}

    assert {
        "NO_TRADE_RCA",
        "CANDIDATE_BLOCK_RCA",
        "AI_INSIGHT",
        "AI_REQUEST_FAILURE",
        "AI_CONTEXT_WARNING",
    }.issubset(card_types)
    assert payload["rca_report_count"] == 2
    assert payload["ai_insight_count"] == 1
    assert payload["ai_request_failure_count"] == 1
    assert payload["context_warning_count"] == 2
    assert payload["execution_controls_available"] is False
    assert payload["run_buttons_available"] is False
    assert all(card["observe_only"] is True for card in cards)
    assert all(card["no_trading_side_effects"] is True for card in cards)
    assert all(card["actions_available"] is False for card in cards)


def test_dashboard_ai_explanation_labels_and_policy_warning(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-ai-labels.sqlite3")
    _insert_insight(connection, insight_id="unsafe-insight", operator_action="RUN_SOMETHING")

    payload = build_ai_explanation_cards(connection, Settings(), limit=10)
    connection.close()
    insight_card = next(card for card in payload["cards"] if card["card_type"] == "AI_INSIGHT")

    assert map_card_type_label("NO_TRADE_RCA") == "매수/주문 없음 RCA"
    assert map_card_type_label("CANDIDATE_BLOCK_RCA") == "후보 차단 RCA"
    assert map_ai_status_label("AI_OUTPUT_INVALID") == "AI 출력 형식 오류"
    assert map_ai_status_label("POLICY_REJECTED") == "안전 정책 차단"
    assert map_ai_status_label("TIMEOUT") == "시간 초과"
    assert map_ai_status_label("API_KEY_MISSING") == "API key 없음"
    assert map_ai_severity_label("HIGH") == "높음"
    assert map_rca_category_label("RISK_CONTEXT") == "리스크 관측"
    assert map_readonly_operator_action("CHECK_DATA") == "데이터 점검"
    assert insight_card["operator_action"] == "REVIEW_ONLY"
    assert any("POLICY_WARNING" in warning for warning in insight_card["warnings"])


def test_dashboard_snapshot_includes_ai_explanations(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dashboard-ai-snapshot.sqlite3")
    _save_report(connection, report_id="rca-no-trade", report_type="NO_TRADE_RCA")
    create_ai_request(
        connection,
        request_id="ai-req-timeout",
        task_type="NO_TRADE_RCA",
        status=AIRequestStatus.TIMEOUT,
        error_message="timeout",
    )

    snapshot = build_dashboard_snapshot(connection, Settings(), detail="summary", limit=10)
    connection.close()

    assert "ai_explanations" in snapshot
    assert snapshot["ai_explanations"]["execution_controls_available"] is False
    assert snapshot["ai_explanations"]["run_buttons_available"] is False
    assert snapshot["ai_explanations"]["latest_cards"]
    assert "AI 설명 카드는 읽기 전용입니다." in snapshot["ai_explanations"]["warnings"]


def test_dashboard_ai_explanation_api_endpoints_are_get_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "dashboard-ai-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        connection = open_connection(db_path)
        try:
            _save_report(
                connection,
                report_id="rca-candidate",
                report_type="CANDIDATE_BLOCK_RCA",
                related_entity_type="candidate",
                related_entity_id="candidate-1",
            )
            _save_report(connection, report_id="rca-no-trade", report_type="NO_TRADE_RCA")
            create_ai_request(
                connection,
                request_id="ai-req-policy",
                task_type="CANDIDATE_BLOCK_RCA",
                related_entity_type="candidate",
                related_entity_id="candidate-1",
                status=AIRequestStatus.POLICY_REJECTED,
                error_message="blocked",
            )
        finally:
            connection.close()

        cards = client.get("/api/dashboard/ai-explanations?status=POLICY_REJECTED")
        status = client.get("/api/dashboard/ai-explanations/status")
        candidate = client.get("/api/dashboard/ai-explanations/candidate/candidate-1")
        no_trade = client.get("/api/dashboard/ai-explanations/no-trade/2026-06-27")

    assert cards.status_code == 200
    assert cards.json()["execution_controls_available"] is False
    assert cards.json()["cards"][0]["status"] == "POLICY_REJECTED"
    assert status.status_code == 200
    assert status.json()["run_buttons_available"] is False
    assert candidate.status_code == 200
    assert candidate.json()["cards"]
    assert no_trade.status_code == 200
    assert no_trade.json()["cards"]

    dashboard_routes = [route for route in app.routes if route.path.startswith("/api/dashboard")]
    assert all("POST" not in route.methods for route in dashboard_routes)


def test_dashboard_ai_explanation_ui_and_js_safety() -> None:
    html = (ROOT / "web" / "templates" / "dashboard.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "static" / "dashboard.js").read_text(encoding="utf-8")

    assert "ai-explanations-section" in html
    assert "AI 설명 카드 / RCA 리포트" in html
    assert "<button" not in html.lower()
    assert 'method: "POST"' not in js
    assert "method: 'POST'" not in js
    assert "/api/ai-sidecar/run" not in js
    assert "/api/ai-sidecar/rca" not in js
    assert "/api/orders" not in js
    assert "send_order" not in js
    assert "cancel_order" not in js
    assert "modify_order" not in js
    assert "order_intent" not in js
    assert "gateway_command" not in js


def _save_report(
    connection,
    *,
    report_id: str,
    report_type: str,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
) -> None:
    section = AIRCASection(
        section_name="safety",
        status="INFO",
        severity="INFO",
        summary="Read-only RCA section.",
        reason_codes=["OBSERVE_ONLY_PIPELINE"],
        evidence_json={"observe_only": True},
        source_refs=["dashboard"],
    )
    report = AIRCAReport(
        report_id=report_id,
        report_type=report_type,
        trade_date="2026-06-27",
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        title=f"{report_type} fixture",
        summary="Read-only RCA fixture.",
        status=AIRCAReportStatus.COMPLETED,
        severity="INFO",
        root_cause_category="NO_ORDER_PATH_BY_DESIGN",
        root_cause="NO_ORDER_PATH_BY_DESIGN",
        deterministic_sections=[section],
        suggested_checks=["Review deterministic evidence"],
        warnings=["No trading side effects"],
    )
    save_rca_report(connection, report)


def _insert_insight(
    connection,
    *,
    insight_id: str = "insight-1",
    operator_action: str = "REVIEW_ONLY",
) -> None:
    output = {
        "summary": "Insight fixture",
        "severity": "LOW",
        "root_cause": "Review only",
        "operator_action": operator_action,
        "suggested_checks": ["Review stored insight"],
        "confidence": 0.75,
        "forbidden_actions_confirmed": True,
    }
    connection.execute(
        """
        INSERT INTO ai_insights (
            insight_id,
            request_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            summary,
            root_cause,
            severity,
            operator_action,
            output_json,
            schema_version
        )
        VALUES (?, 'ai-req-ok', 'NO_TRADE_RCA', '2026-06-27', NULL, NULL, ?, ?, 'LOW', ?, ?, ?)
        """,
        (
            insight_id,
            "Insight fixture",
            "Review only",
            operator_action,
            json.dumps(output, ensure_ascii=False),
            "ai-sidecar.v1",
        ),
    )
    connection.commit()
