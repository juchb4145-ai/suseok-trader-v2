from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from apps.core_api import app
from domain.ai_sidecar.rca import (
    AIRCAReport,
    AIRCAReportStatus,
    AIRCAReportType,
    AIRCARootCauseCategory,
    AIRCASection,
    AIRCASeverity,
)
from fastapi.testclient import TestClient
from services.ai_sidecar.openai_client import MockAISidecarModelClient
from services.ai_sidecar.rca_report_store import get_rca_report, save_rca_report
from services.ai_sidecar.rca_workflows import (
    build_candidate_block_rca_report,
    build_candidate_block_rca_reports_for_trade_date,
    build_no_trade_rca_report,
)
from services.ai_sidecar.request_store import list_ai_insights
from services.config import Settings
from storage.sqlite import initialize_database


def test_rca_schema_tables_are_created(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rca-schema.sqlite3")
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'ai_rca_reports',
                'ai_rca_sections',
                'ai_rca_report_links',
                'ai_rca_report_errors',
                'ai_context_packets',
                'ai_requests',
                'ai_insights'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in rows} == {
        "ai_rca_reports",
        "ai_rca_sections",
        "ai_rca_report_links",
        "ai_rca_report_errors",
        "ai_context_packets",
        "ai_requests",
        "ai_insights",
    }


def test_rca_report_roundtrip_and_store_rejects_side_effect_flags(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rca-store.sqlite3")
    section = AIRCASection(
        section_name="safety",
        status="INFO",
        severity=AIRCASeverity.INFO,
        summary="Read-only RCA section.",
        reason_codes=["OBSERVE_ONLY_PIPELINE"],
        evidence_json={"observe_only": True},
        source_refs=["dashboard"],
    )
    report = AIRCAReport(
        report_id="report-safe",
        report_type=AIRCAReportType.NO_TRADE_RCA,
        trade_date="2026-06-27",
        title="No-trade RCA",
        summary="No order path by design.",
        status=AIRCAReportStatus.COMPLETED,
        severity=AIRCASeverity.INFO,
        root_cause_category=AIRCARootCauseCategory.NO_ORDER_PATH_BY_DESIGN,
        root_cause="NO_ORDER_PATH_BY_DESIGN",
        deterministic_sections=[section],
        suggested_checks=["Review only"],
        warnings=["No trading side effects"],
    )

    roundtrip = AIRCAReport.from_dict(report.to_dict())
    report_id = save_rca_report(connection, roundtrip)
    saved = get_rca_report(connection, report_id)
    unsafe = AIRCAReport.from_dict(report.to_dict() | {"no_trading_side_effects": False})
    with pytest.raises(ValueError):
        save_rca_report(connection, unsafe)
    connection.close()

    assert report_id == "report-safe"
    assert saved is not None
    assert saved["observe_only"] is True
    assert saved["no_trading_side_effects"] is True
    assert saved["deterministic_sections"][0]["reason_codes"] == ["OBSERVE_ONLY_PIPELINE"]


def test_no_trade_deterministic_empty_pipeline_saves_without_ai(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rca-no-trade-empty.sqlite3")
    mock = MockAISidecarModelClient()

    result = build_no_trade_rca_report(
        connection,
        "2026-06-27",
        run_ai=False,
        model_client=mock,
        settings=Settings(),
    )
    request_count = connection.execute("SELECT COUNT(*) AS count FROM ai_requests").fetchone()
    saved = get_rca_report(connection, result.report.report_id)
    connection.close()

    assert result.ok is True
    assert result.report.root_cause_category in {
        AIRCARootCauseCategory.NO_ORDER_PATH_BY_DESIGN,
        AIRCARootCauseCategory.OBSERVE_ONLY_PIPELINE,
    }
    assert request_count["count"] == 0
    assert mock.requests == []
    assert saved["deterministic_sections"]


def test_no_trade_classifies_strategy_risk_and_projection_causes(tmp_path) -> None:
    strategy_conn = initialize_database(tmp_path / "rca-no-trade-strategy.sqlite3")
    _insert_candidate(strategy_conn, state="CONTEXT_READY")
    strategy_result = build_no_trade_rca_report(
        strategy_conn,
        "2026-06-27",
        settings=Settings(),
    )
    strategy_conn.close()

    risk_conn = initialize_database(tmp_path / "rca-no-trade-risk.sqlite3")
    _insert_candidate(risk_conn, state="CONTEXT_READY")
    _insert_risk_latest(risk_conn, status="OBSERVE_BLOCK")
    risk_result = build_no_trade_rca_report(risk_conn, "2026-06-27", settings=Settings())
    risk_conn.close()

    error_conn = initialize_database(tmp_path / "rca-no-trade-error.sqlite3")
    error_conn.execute(
        """
        INSERT INTO market_projection_errors (
            event_id, event_type, code, error_message, payload_json
        )
        VALUES ('evt-bad', 'price_tick', '005930', 'bad row', ?)
        """,
        (json.dumps({"bad": True}),),
    )
    error_result = build_no_trade_rca_report(error_conn, "2026-06-27", settings=Settings())
    error_conn.close()

    assert strategy_result.report.root_cause_category == AIRCARootCauseCategory.STRATEGY_CONTEXT
    assert risk_result.report.root_cause_category == AIRCARootCauseCategory.RISK_CONTEXT
    assert error_result.report.root_cause_category == AIRCARootCauseCategory.MARKET_DATA_PROJECTION


def test_candidate_block_deterministic_root_causes_and_links(tmp_path) -> None:
    data_wait_conn = initialize_database(tmp_path / "rca-candidate-data-wait.sqlite3")
    _insert_candidate(data_wait_conn, state="DATA_WAIT")
    data_wait = build_candidate_block_rca_report(
        data_wait_conn,
        "candidate-1",
        settings=Settings(),
    )
    data_wait_saved = get_rca_report(data_wait_conn, data_wait.report.report_id)
    data_wait_conn.close()

    stale_conn = initialize_database(tmp_path / "rca-candidate-stale.sqlite3")
    _insert_candidate(stale_conn, state="STALE")
    stale = build_candidate_block_rca_report(stale_conn, "candidate-1", settings=Settings())
    stale_conn.close()

    strategy_conn = initialize_database(tmp_path / "rca-candidate-strategy.sqlite3")
    _insert_candidate(strategy_conn, state="CONTEXT_READY")
    _insert_strategy_latest(strategy_conn, status="NO_SETUP")
    strategy = build_candidate_block_rca_report(strategy_conn, "candidate-1", settings=Settings())
    strategy_conn.close()

    risk_conn = initialize_database(tmp_path / "rca-candidate-risk.sqlite3")
    _insert_candidate(risk_conn, state="CONTEXT_READY")
    _insert_risk_latest(risk_conn, status="OBSERVE_BLOCK")
    risk = build_candidate_block_rca_report(risk_conn, "candidate-1", settings=Settings())
    risk_conn.close()

    assert data_wait.report.root_cause_category == AIRCARootCauseCategory.DATA_QUALITY
    assert any(link["link_type"] == "related_entity" for link in data_wait_saved["links"])
    assert stale.report.root_cause == "STALE_CONTEXT: candidate observation episode is stale."
    assert strategy.report.root_cause_category == AIRCARootCauseCategory.STRATEGY_CONTEXT
    assert risk.report.root_cause_category == AIRCARootCauseCategory.RISK_CONTEXT


def test_rca_ai_integration_valid_disabled_and_invalid_outputs(tmp_path) -> None:
    settings_enabled = Settings(ai_sidecar_enabled_value=True, ai_sidecar_model="mock-model")
    valid_conn = initialize_database(tmp_path / "rca-ai-valid.sqlite3")
    valid = build_no_trade_rca_report(
        valid_conn,
        "2026-06-27",
        run_ai=True,
        model_client=MockAISidecarModelClient(output=_valid_ai_output()),
        settings=settings_enabled,
    )
    valid_conn.close()

    disabled_conn = initialize_database(tmp_path / "rca-ai-disabled.sqlite3")
    disabled_mock = MockAISidecarModelClient(output=_valid_ai_output())
    disabled = build_no_trade_rca_report(
        disabled_conn,
        "2026-06-27",
        run_ai=True,
        model_client=disabled_mock,
        settings=Settings(ai_sidecar_enabled_value=False, ai_sidecar_model="mock-model"),
    )
    disabled_conn.close()

    invalid_conn = initialize_database(tmp_path / "rca-ai-invalid.sqlite3")
    invalid_output = {key: value for key, value in _valid_ai_output().items() if key != "summary"}
    invalid = build_no_trade_rca_report(
        invalid_conn,
        "2026-06-27",
        run_ai=True,
        model_client=MockAISidecarModelClient(output=invalid_output),
        settings=settings_enabled,
    )
    insights = list_ai_insights(invalid_conn)
    invalid_conn.close()

    assert valid.report.status == AIRCAReportStatus.COMPLETED
    assert valid.report.ai_request_id
    assert valid.report.ai_insight_id
    assert valid.report.ai_summary
    assert disabled.report.status == AIRCAReportStatus.AI_DISABLED
    assert disabled.report.ai_request_id
    assert disabled.report.ai_insight_id is None
    assert disabled_mock.requests == []
    assert invalid.report.status == AIRCAReportStatus.AI_OUTPUT_INVALID
    assert invalid.report.ai_request_id
    assert invalid.report.ai_insight_id is None
    assert insights == []


def test_candidate_batch_limit_and_continues_after_empty_selection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rca-batch.sqlite3")
    for index, state in enumerate(("DATA_WAIT", "STALE", "CLOSED"), start=1):
        _insert_candidate(
            connection,
            candidate_instance_id=f"candidate-{index}",
            code=f"00593{index}",
            state=state,
        )

    results = build_candidate_block_rca_reports_for_trade_date(
        connection,
        "2026-06-27",
        limit=2,
        settings=Settings(),
    )
    connection.close()

    assert len(results) == 2
    assert all(result.ok for result in results)


def test_rca_api_requires_token_and_reads_reports(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "rca-api.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("AI_SIDECAR_ENABLED", "false")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with TestClient(app) as client:
        status = client.get("/api/ai-sidecar/rca/status")
        missing = client.post("/api/ai-sidecar/rca/no-trade/2026-06-27")
        accepted = client.post(
            "/api/ai-sidecar/rca/no-trade/2026-06-27",
            headers={"X-Local-Token": "secret-token"},
        )
        report_id = accepted.json()["report_id"]
        reports = client.get("/api/ai-sidecar/rca/reports")
        detail = client.get(f"/api/ai-sidecar/rca/reports/{report_id}")
        errors = client.get("/api/ai-sidecar/rca/errors")

    assert status.status_code == 200
    assert status.json()["deterministic_reports_available"] is True
    assert missing.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["no_trading_side_effects"] is True
    assert reports.json()["reports"][0]["report_id"] == report_id
    assert detail.json()["deterministic_sections"]
    assert errors.status_code == 200


def test_rca_candidate_api_and_batch_endpoint(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "rca-api-candidate.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    connection = initialize_database(db_path)
    _insert_candidate(connection, state="DATA_WAIT")
    connection.close()

    with TestClient(app) as client:
        candidate = client.post("/api/ai-sidecar/rca/candidate/candidate-1")
        batch = client.post(
            "/api/ai-sidecar/rca/candidates/batch",
            json={"trade_date": "2026-06-27", "limit": 1},
        )

    assert candidate.status_code == 200
    assert candidate.json()["root_cause_category"] == "DATA_QUALITY"
    assert batch.status_code == 200
    assert batch.json()["count"] == 1
    assert batch.json()["run_ai"] is False


def test_rca_cli_deterministic_mode_works_without_openai_key(tmp_path) -> None:
    env = os.environ.copy()
    env["TRADING_DB_PATH"] = str(tmp_path / "rca-cli.sqlite3")
    env.pop("OPENAI_API_KEY", None)
    run = subprocess.run(
        [sys.executable, "tools/build_no_trade_rca.py", "--trade-date", "2026-06-27"],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    payload = json.loads(run.stdout)

    assert run.returncode == 0
    assert payload["ok"] is True
    assert payload["report_id"]
    assert payload["no_trading_side_effects"] is True


def test_dashboard_rca_snapshot_is_read_only_and_no_run_button(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rca-dashboard.sqlite3")
    result = build_no_trade_rca_report(connection, "2026-06-27", settings=Settings())
    from services.dashboard_service import build_dashboard_snapshot

    snapshot = build_dashboard_snapshot(connection, Settings())
    connection.close()
    dashboard_js = Path("web/static/dashboard.js").read_text(encoding="utf-8")
    dashboard_html = Path("web/templates/dashboard.html").read_text(encoding="utf-8")

    assert result.ok is True
    assert snapshot["ai_sidecar"]["rca_available"] is True
    assert snapshot["ai_sidecar"]["rca_report_count"] == 1
    assert snapshot["ai_sidecar"]["latest_rca_reports"][0]["report_id"] == result.report.report_id
    assert "/api/ai-sidecar/rca" not in dashboard_js
    assert "RCA 실행" not in dashboard_html + dashboard_js


def test_rca_workflow_safety_regression_no_trading_surface() -> None:
    workflow_source = Path("services/ai_sidecar/rca_workflows.py").read_text(encoding="utf-8")
    api_source = Path("api/routes/ai_rca.py").read_text(encoding="utf-8")
    combined = workflow_source + api_source

    assert "send_order(" not in combined
    assert "cancel_order(" not in combined
    assert "modify_order(" not in combined
    assert "enqueue_command(" not in combined
    assert "GatewayCommand(" not in combined
    assert "OrderIntent(" not in combined
    assert "background worker" not in combined.lower()


def _valid_ai_output() -> dict[str, object]:
    return {
        "summary": "Deterministic RCA reviewed by mock AI.",
        "severity": "LOW",
        "root_cause": "Read-only review only.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Review deterministic RCA sections"],
        "confidence": 0.75,
        "forbidden_actions_confirmed": True,
    }


def _insert_candidate(
    connection,
    *,
    candidate_instance_id: str = "candidate-1",
    code: str = "005930",
    state: str = "DATA_WAIT",
) -> None:
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            detected_at,
            last_seen_at,
            state_updated_at,
            primary_source_type,
            primary_source_id,
            reason_codes_json
        )
        VALUES (?, '2026-06-27', ?, '삼성전자', 1, ?, ?, ?, ?, 'MANUAL', 'test', ?)
        """,
        (
            candidate_instance_id,
            code,
            state,
            "2026-06-27T00:00:00Z",
            "2026-06-27T00:01:00Z",
            "2026-06-27T00:01:00Z",
            json.dumps([f"CANDIDATE_{state}"]),
        ),
    )
    connection.commit()


def _insert_strategy_latest(connection, *, status: str = "NO_SETUP") -> None:
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            primary_setup_type,
            primary_setup_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (
            'candidate-1',
            'strategy-1',
            '2026-06-27',
            '005930',
            '삼성전자',
            '2026-06-27T00:02:00Z',
            ?,
            'PULLBACK',
            ?,
            0,
            0,
            ?,
            'test',
            1
        )
        """,
        (status, status, json.dumps([f"STRATEGY_{status}"])),
    )
    connection.commit()


def _insert_risk_latest(connection, *, status: str = "OBSERVE_BLOCK") -> None:
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id,
            risk_observation_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (
            'candidate-1',
            'risk-1',
            NULL,
            '2026-06-27',
            '005930',
            '삼성전자',
            '2026-06-27T00:03:00Z',
            ?,
            'HIGH',
            1,
            0,
            0,
            ?,
            'test',
            1
        )
        """,
        (status, json.dumps([f"RISK_{status}"])),
    )
    connection.commit()
