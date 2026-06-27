from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from apps.core_api import app
from domain.ai_sidecar.live_sim_review import (
    LiveSimReviewReport,
    LiveSimReviewReportType,
    LiveSimReviewRootCauseCategory,
    LiveSimReviewSection,
    LiveSimReviewSeverity,
    LiveSimReviewStatus,
)
from domain.ai_sidecar.schemas import AISidecarValidationError
from fastapi.testclient import TestClient
from services.ai_sidecar.live_sim_review_store import (
    get_live_sim_review_report,
    save_live_sim_review_report,
)
from services.ai_sidecar.live_sim_review_workflows import (
    build_live_sim_incident_review,
    build_live_sim_order_review,
    build_live_sim_order_reviews_for_trade_date,
    build_live_sim_reconcile_review,
    build_live_sim_session_review,
)
from services.ai_sidecar.openai_client import MockAISidecarModelClient
from services.config import Settings
from services.dashboard_ai_explanations import build_ai_explanation_cards
from services.dashboard_service import build_dashboard_snapshot
from services.live_sim.live_sim_service import (
    create_live_sim_intent,
    queue_live_sim_order_command,
    reconcile_live_sim,
)
from services.oms.dry_run_service import create_dry_run_intent
from storage.sqlite import initialize_database
from tests.test_live_sim import _live_sim_settings, _mark_gateway_ready
from tests.test_oms_dry_run import _prepared_connection
from tests.test_oms_dry_run import _settings as _dry_run_settings


def test_live_sim_review_schema_tables_are_created(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-review-schema.sqlite3")
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'ai_live_sim_review_reports',
                'ai_live_sim_review_sections',
                'ai_live_sim_review_links',
                'ai_live_sim_review_errors'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in rows} == {
        "ai_live_sim_review_reports",
        "ai_live_sim_review_sections",
        "ai_live_sim_review_links",
        "ai_live_sim_review_errors",
    }


def test_live_sim_review_model_roundtrip_and_safe_flags(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-review-model.sqlite3")
    section = LiveSimReviewSection(
        section_name="safety",
        status="INFO",
        severity=LiveSimReviewSeverity.INFO,
        summary="Read-only review section.",
        reason_codes=["REVIEW_ONLY"],
        evidence_json={"review_only": True},
        source_refs=["dashboard"],
    )
    report = LiveSimReviewReport(
        review_id="review-safe",
        report_type=LiveSimReviewReportType.LIVE_SIM_SESSION_REVIEW,
        trade_date="2026-06-27",
        related_entity_type="live_sim_session",
        related_entity_id="2026-06-27",
        title="LIVE_SIM session review",
        summary="No activity.",
        status=LiveSimReviewStatus.COMPLETED,
        severity=LiveSimReviewSeverity.INFO,
        root_cause_category=LiveSimReviewRootCauseCategory.CONFIGURATION,
        root_cause="NO_LIVE_SIM_ACTIVITY",
        deterministic_sections=[section],
        suggested_checks=["Review only"],
        warnings=["No order action"],
    )

    roundtrip = LiveSimReviewReport.from_dict(report.to_dict())
    review_id = save_live_sim_review_report(connection, roundtrip)
    saved = get_live_sim_review_report(connection, review_id)
    connection.close()

    with pytest.raises(AISidecarValidationError):
        LiveSimReviewReport.from_dict(report.to_dict() | {"live_real_allowed": True})
    with pytest.raises(AISidecarValidationError):
        LiveSimReviewReport.from_dict(report.to_dict() | {"order_action_allowed": True})
    assert review_id == "review-safe"
    assert saved is not None
    assert saved["review_only"] is True
    assert saved["order_action_allowed"] is False
    assert saved["gateway_command_allowed"] is False
    assert saved["live_real_allowed"] is False
    assert saved["deterministic_sections"][0]["reason_codes"] == ["REVIEW_ONLY"]


def test_session_review_no_activity_is_deterministic_without_ai(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-review-session.sqlite3")
    mock = MockAISidecarModelClient()

    result = build_live_sim_session_review(
        connection,
        "2026-06-27",
        run_ai=False,
        model_client=mock,
        settings=Settings(),
    )
    request_count = connection.execute("SELECT COUNT(*) AS count FROM ai_requests").fetchone()
    saved = get_live_sim_review_report(connection, result.report.review_id)
    connection.close()

    assert result.ok is True
    assert result.report.root_cause_category == LiveSimReviewRootCauseCategory.CONFIGURATION
    assert "LIVE_SIM 주문/체결 활동 없음" in result.report.root_cause
    assert request_count["count"] == 0
    assert mock.requests == []
    assert saved["deterministic_sections"]


def test_order_reconcile_incident_and_batch_reviews_are_read_only(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-review-flow.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    intent = create_live_sim_intent(connection, candidate_id, settings=settings)
    order = queue_live_sim_order_command(connection, intent.live_sim_intent_id, settings=settings)
    snapshot = reconcile_live_sim(connection, settings=settings)
    before_command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]

    order_review = build_live_sim_order_review(
        connection,
        order.live_sim_order_id,
        settings=settings,
    )
    reconcile_review = build_live_sim_reconcile_review(
        connection,
        snapshot.reconcile_id,
        settings=settings,
    )
    connection.execute(
        """
        INSERT INTO live_sim_errors (
            live_sim_intent_id, live_sim_order_id, code, error_message, payload_json
        )
        VALUES (?, ?, '005930', 'fixture incident', ?)
        """,
        (intent.live_sim_intent_id, order.live_sim_order_id, json.dumps({"fixture": True})),
    )
    connection.commit()
    incident_review = build_live_sim_incident_review(connection, settings=settings)
    batch = build_live_sim_order_reviews_for_trade_date(
        connection,
        "2026-06-27",
        limit=5,
        settings=settings,
    )
    after_command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    stored_order_status = connection.execute(
        "SELECT status FROM live_sim_orders WHERE live_sim_order_id = ?",
        (order.live_sim_order_id,),
    ).fetchone()["status"]
    connection.close()

    assert order_review.ok is True
    assert order_review.report.live_sim_order_id == order.live_sim_order_id
    assert order_review.report.order_action_allowed is False
    assert order_review.report.root_cause_category in {
        LiveSimReviewRootCauseCategory.BROKER_ACK,
        LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
    }
    assert reconcile_review.ok is True
    assert reconcile_review.report.reconcile_id == snapshot.reconcile_id
    assert reconcile_review.report.root_cause_category == (
        LiveSimReviewRootCauseCategory.LOCAL_ONLY_RECONCILE
    )
    assert incident_review.ok is True
    assert incident_review.report.root_cause_category in {
        LiveSimReviewRootCauseCategory.UNKNOWN,
        LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
    }
    assert len(batch) == 1
    assert before_command_count == after_command_count
    assert stored_order_status == "COMMAND_QUEUED"


def test_live_sim_review_ai_integration_valid_disabled_invalid(tmp_path) -> None:
    settings_enabled = Settings(ai_sidecar_enabled_value=True, ai_sidecar_model="mock-model")
    valid_conn = initialize_database(tmp_path / "live-sim-review-ai-valid.sqlite3")
    valid = build_live_sim_session_review(
        valid_conn,
        "2026-06-27",
        run_ai=True,
        model_client=MockAISidecarModelClient(output=_valid_ai_output()),
        settings=settings_enabled,
    )
    valid_conn.close()

    disabled_conn = initialize_database(tmp_path / "live-sim-review-ai-disabled.sqlite3")
    disabled_mock = MockAISidecarModelClient(output=_valid_ai_output())
    disabled = build_live_sim_session_review(
        disabled_conn,
        "2026-06-27",
        run_ai=True,
        model_client=disabled_mock,
        settings=Settings(ai_sidecar_enabled_value=False, ai_sidecar_model="mock-model"),
    )
    disabled_conn.close()

    invalid_conn = initialize_database(tmp_path / "live-sim-review-ai-invalid.sqlite3")
    invalid_output = {key: value for key, value in _valid_ai_output().items() if key != "summary"}
    invalid = build_live_sim_session_review(
        invalid_conn,
        "2026-06-27",
        run_ai=True,
        model_client=MockAISidecarModelClient(output=invalid_output),
        settings=settings_enabled,
    )
    insight_count = invalid_conn.execute("SELECT COUNT(*) AS count FROM ai_insights").fetchone()
    invalid_conn.close()

    assert valid.report.status == LiveSimReviewStatus.COMPLETED
    assert valid.report.ai_request_id
    assert valid.report.ai_insight_id
    assert disabled.report.status == LiveSimReviewStatus.AI_DISABLED
    assert disabled.report.ai_request_id
    assert disabled.report.ai_insight_id is None
    assert disabled_mock.requests == []
    assert invalid.report.status == LiveSimReviewStatus.AI_OUTPUT_INVALID
    assert invalid.report.ai_request_id
    assert invalid.report.ai_insight_id is None
    assert insight_count["count"] == 0


def test_live_sim_review_api_requires_token_and_reads_reports(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "live-sim-review-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with TestClient(app) as client:
        status = client.get("/api/ai-sidecar/live-sim-review/status")
        missing = client.post("/api/ai-sidecar/live-sim-review/session/2026-06-27")
        accepted = client.post(
            "/api/ai-sidecar/live-sim-review/session/2026-06-27",
            headers={"X-Local-Token": "secret-token"},
        )
        review_id = accepted.json()["review_id"]
        reports = client.get("/api/ai-sidecar/live-sim-review/reports")
        detail = client.get(f"/api/ai-sidecar/live-sim-review/reports/{review_id}")
        errors = client.get("/api/ai-sidecar/live-sim-review/errors")

    assert status.status_code == 200
    assert status.json()["deterministic_reports_available"] is True
    assert status.json()["review_only"] is True
    assert missing.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["order_action_allowed"] is False
    assert reports.json()["reports"][0]["review_id"] == review_id
    assert detail.json()["deterministic_sections"]
    assert errors.status_code == 200


def test_live_sim_review_cli_works_without_openai_key(tmp_path) -> None:
    env = os.environ.copy()
    env["TRADING_DB_PATH"] = str(tmp_path / "live-sim-review-cli.sqlite3")
    env.pop("OPENAI_API_KEY", None)
    run = subprocess.run(
        [
            sys.executable,
            "tools/build_live_sim_session_review.py",
            "--trade-date",
            "2026-06-27",
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    payload = json.loads(run.stdout)

    assert run.returncode == 0
    assert payload["ok"] is True
    assert payload["review_id"]
    assert payload["order_action_allowed"] is False
    assert payload["live_real_allowed"] is False


def test_dashboard_includes_live_sim_review_cards_and_no_post_surface(tmp_path) -> None:
    connection = initialize_database(tmp_path / "live-sim-review-dashboard.sqlite3")
    result = build_live_sim_session_review(connection, "2026-06-27", settings=Settings())
    snapshot = build_dashboard_snapshot(connection, Settings())
    cards = build_ai_explanation_cards(connection, Settings(), limit=20)
    connection.close()
    dashboard_js = Path("web/static/dashboard.js").read_text(encoding="utf-8")
    dashboard_html = Path("web/templates/dashboard.html").read_text(encoding="utf-8")

    assert result.ok is True
    assert snapshot["live_sim"]["live_sim_review_available"] is True
    assert snapshot["live_sim"]["live_sim_review_report_count"] == 1
    assert snapshot["ai_sidecar"]["latest_live_sim_review_reports"][0]["review_id"] == (
        result.report.review_id
    )
    assert any(card["card_type"] == "LIVE_SIM_SESSION_REVIEW" for card in cards["cards"])
    assert "/api/ai-sidecar/live-sim-review" not in dashboard_js
    assert "review 생성" not in dashboard_html + dashboard_js
    assert "order retry" not in (dashboard_html + dashboard_js).lower()


def test_live_sim_review_workflow_safety_regression_no_trading_surface() -> None:
    workflow_source = Path("services/ai_sidecar/live_sim_review_workflows.py").read_text(
        encoding="utf-8"
    )
    api_source = Path("api/routes/ai_live_sim_review.py").read_text(encoding="utf-8")
    combined = workflow_source + api_source

    assert "queue_live_sim_order_command" not in combined
    assert "create_live_sim_intent" not in combined
    assert "enqueue_command(" not in combined
    assert "GatewayCommand(" not in combined
    assert "send_order(" not in combined
    assert "cancel_order(" not in combined
    assert "modify_order(" not in combined
    assert "/api/orders/enqueue" not in combined
    assert "background worker" not in combined.lower()


def _valid_ai_output() -> dict[str, object]:
    return {
        "summary": "LIVE_SIM review inspected by mock AI.",
        "severity": "LOW",
        "root_cause": "Read-only review only.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Review deterministic LIVE_SIM review sections"],
        "confidence": 0.75,
        "forbidden_actions_confirmed": True,
    }
