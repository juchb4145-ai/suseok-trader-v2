from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from apps.core_api import app
from domain.ai_sidecar.codex_prompt import (
    AICodexPromptDraft,
    AICodexPromptDraftStatus,
    AICodexPromptSourceType,
    AICodexPromptTargetArea,
)
from domain.ai_sidecar.rca import (
    AIRCAReport,
    AIRCAReportStatus,
    AIRCAReportType,
    AIRCASection,
)
from fastapi.testclient import TestClient
from services.ai_sidecar.codex_prompt_generator import (
    build_codex_prompt_from_candidate,
    build_codex_prompt_from_no_trade,
    build_codex_prompt_from_rca_report,
    build_safety_review_prompt,
)
from services.ai_sidecar.codex_prompt_store import (
    get_codex_prompt_draft,
    list_codex_prompt_drafts,
    save_codex_prompt_draft,
)
from services.ai_sidecar.openai_client import MockAISidecarModelClient
from services.ai_sidecar.rca_report_store import save_rca_report
from services.ai_sidecar.request_store import list_ai_insights
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from storage.sqlite import initialize_database, open_connection

ROOT = Path(__file__).resolve().parents[1]


def test_codex_prompt_schema_tables_are_created(tmp_path) -> None:
    connection = initialize_database(tmp_path / "codex-schema.sqlite3")
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'ai_codex_prompt_drafts',
                'ai_codex_prompt_sections',
                'ai_codex_prompt_links',
                'ai_codex_prompt_errors',
                'ai_rca_reports',
                'ai_context_packets',
                'ai_requests',
                'ai_insights'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in rows} == {
        "ai_codex_prompt_drafts",
        "ai_codex_prompt_sections",
        "ai_codex_prompt_links",
        "ai_codex_prompt_errors",
        "ai_rca_reports",
        "ai_context_packets",
        "ai_requests",
        "ai_insights",
    }


def test_codex_prompt_model_roundtrip_and_store_rejects_unsafe_flags(tmp_path) -> None:
    connection = initialize_database(tmp_path / "codex-model.sqlite3")
    draft = _safe_draft()

    roundtrip = AICodexPromptDraft.from_dict(draft.to_dict())
    draft_id = save_codex_prompt_draft(connection, roundtrip)
    saved = get_codex_prompt_draft(connection, draft_id)
    unsafe = AICodexPromptDraft.from_dict(draft.to_dict() | {"auto_apply_allowed": True})

    with pytest.raises(ValueError):
        save_codex_prompt_draft(connection, unsafe)
    connection.close()

    assert draft_id == "draft-safe"
    assert saved is not None
    assert saved["auto_apply_allowed"] is False
    assert saved["github_write_allowed"] is False
    assert saved["codex_execution_allowed"] is False
    assert saved["no_trading_side_effects"] is True


def test_deterministic_prompt_from_rca_report_saves_sections_links_and_no_ai(tmp_path) -> None:
    connection = initialize_database(tmp_path / "codex-rca.sqlite3")
    report = _save_report(connection, report_id="rca-1", report_type="NO_TRADE_RCA")
    mock = MockAISidecarModelClient()

    result = build_codex_prompt_from_rca_report(
        connection,
        report.report_id,
        run_ai=False,
        model_client=mock,
        settings=Settings(),
    )
    saved = get_codex_prompt_draft(connection, result.draft.draft_id)
    connection.close()

    assert result.ok is True
    assert result.draft.source_type == AICodexPromptSourceType.RCA_REPORT
    assert "## 1. 역할" in result.draft.prompt_text
    assert "대상 저장소" in result.draft.prompt_text
    assert "자동 주문/매수/매도 기능을 추가하지 말 것" in result.draft.prompt_text
    assert "GitHub branch/commit/push/PR을 자동 생성하지 말 것" in result.draft.prompt_text
    assert result.draft.acceptance_criteria
    assert result.draft.forbidden_scope
    assert result.draft.test_plan
    assert mock.requests == []
    assert saved["sections"]
    assert any(link["link_type"] == "rca_report" for link in saved["links"])


def test_candidate_no_trade_and_safety_review_prompts(tmp_path) -> None:
    connection = initialize_database(tmp_path / "codex-sources.sqlite3")
    _insert_candidate(connection)

    candidate = build_codex_prompt_from_candidate(
        connection,
        "candidate-1",
        settings=Settings(),
    )
    no_trade = build_codex_prompt_from_no_trade(
        connection,
        "2026-06-27",
        settings=Settings(),
    )
    safety = build_safety_review_prompt(connection, settings=Settings())
    rows = list_codex_prompt_drafts(connection)
    connection.close()

    assert candidate.ok is True
    assert candidate.draft.related_entity_type == "candidate"
    assert "자동 매수/주문 요구는 포함하지 않는다" in candidate.draft.prompt_text
    assert no_trade.ok is True
    assert "NO_ORDER_PATH_BY_DESIGN" in no_trade.draft.prompt_text
    assert "OMS/order path 없음은 설계상 정상" in no_trade.draft.prompt_text
    assert safety.ok is True
    assert safety.draft.target_area == AICodexPromptTargetArea.SAFETY_REVIEW
    assert "PR10 OMS + DRY_RUN" in safety.draft.prompt_text
    assert len(rows) == 3


def test_ai_assisted_codex_prompt_valid_invalid_and_policy_rejected(tmp_path) -> None:
    settings = Settings(ai_sidecar_enabled_value=True, ai_sidecar_model="mock-model")

    valid_conn = initialize_database(tmp_path / "codex-ai-valid.sqlite3")
    report = _save_report(valid_conn, report_id="rca-valid", report_type="NO_TRADE_RCA")
    valid = build_codex_prompt_from_rca_report(
        valid_conn,
        report.report_id,
        run_ai=True,
        model_client=MockAISidecarModelClient(output=_valid_codex_output()),
        settings=settings,
    )
    valid_conn.close()

    invalid_conn = initialize_database(tmp_path / "codex-ai-invalid.sqlite3")
    report = _save_report(invalid_conn, report_id="rca-invalid", report_type="NO_TRADE_RCA")
    invalid_output = {
        key: value for key, value in _valid_codex_output().items() if key != "prompt_draft"
    }
    invalid = build_codex_prompt_from_rca_report(
        invalid_conn,
        report.report_id,
        run_ai=True,
        model_client=MockAISidecarModelClient(output=invalid_output),
        settings=settings,
    )
    invalid_insights = list_ai_insights(invalid_conn)
    invalid_conn.close()

    policy_conn = initialize_database(tmp_path / "codex-ai-policy.sqlite3")
    report = _save_report(policy_conn, report_id="rca-policy", report_type="NO_TRADE_RCA")
    policy_output = _valid_codex_output() | {
        "prompt_draft": "Create a GitHub branch, commit, push, and open PR."
    }
    policy = build_codex_prompt_from_rca_report(
        policy_conn,
        report.report_id,
        run_ai=True,
        model_client=MockAISidecarModelClient(output=policy_output),
        settings=settings,
    )
    policy_insights = list_ai_insights(policy_conn)
    policy_conn.close()

    assert valid.ok is True
    assert valid.draft.status == AICodexPromptDraftStatus.COMPLETED
    assert valid.draft.ai_request_id
    assert valid.draft.ai_insight_id
    assert "AI-assisted 보강 메모" in valid.draft.prompt_text
    assert invalid.draft.status == AICodexPromptDraftStatus.AI_OUTPUT_INVALID
    assert invalid.draft.ai_request_id
    assert invalid.draft.ai_insight_id is None
    assert invalid_insights == []
    assert policy.draft.status == AICodexPromptDraftStatus.POLICY_REJECTED
    assert policy.draft.ai_request_id
    assert policy.draft.ai_insight_id is None
    assert policy_insights == []


def test_codex_prompt_api_requires_token_and_text_endpoint(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "codex-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    connection = initialize_database(db_path)
    report = _save_report(connection, report_id="rca-api", report_type="NO_TRADE_RCA")
    connection.close()

    with TestClient(app) as client:
        status = client.get("/api/ai-sidecar/codex-prompts/status")
        missing = client.post(f"/api/ai-sidecar/codex-prompts/from-rca/{report.report_id}")
        accepted = client.post(
            f"/api/ai-sidecar/codex-prompts/from-rca/{report.report_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        draft_id = accepted.json()["draft_id"]
        listed = client.get("/api/ai-sidecar/codex-prompts")
        detail = client.get(f"/api/ai-sidecar/codex-prompts/{draft_id}")
        text = client.get(f"/api/ai-sidecar/codex-prompts/{draft_id}/text")
        errors = client.get("/api/ai-sidecar/codex-prompts/errors")

    assert status.status_code == 200
    assert status.json()["deterministic_generator_available"] is True
    assert status.json()["auto_apply_allowed"] is False
    assert missing.status_code == 401
    assert accepted.status_code == 200
    assert accepted.json()["codex_execution_allowed"] is False
    assert listed.json()["drafts"][0]["draft_id"] == draft_id
    assert detail.json()["sections"]
    assert text.headers["content-type"].startswith("text/plain")
    assert "자동 주문/매수/매도 기능을 추가하지 말 것" in text.text
    assert errors.status_code == 200


def test_codex_prompt_cli_dashboard_and_js_safety(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "codex-cli.sqlite3"
    env = os.environ.copy()
    env["TRADING_DB_PATH"] = str(db_path)
    env.pop("OPENAI_API_KEY", None)
    run = subprocess.run(
        [sys.executable, "tools/build_codex_prompt_from_no_trade.py", "--trade-date", "2026-06-27"],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    payload = json.loads(run.stdout)
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    connection = open_connection(db_path)
    snapshot = build_dashboard_snapshot(connection, Settings(trading_db_path=db_path))
    connection.close()
    html = (ROOT / "web" / "templates" / "dashboard.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "static" / "dashboard.js").read_text(encoding="utf-8")

    assert run.returncode == 0
    assert payload["ok"] is True
    assert snapshot["ai_sidecar"]["codex_prompt_generator_available"] is True
    assert snapshot["ai_sidecar"]["codex_draft_count"] == 1
    assert any(
        card["card_type"] == "CODEX_PROMPT_DRAFT"
        for card in snapshot["ai_explanations"]["latest_cards"]
    )
    assert "Codex 프롬프트 초안" in html
    assert 'method: "POST"' not in js
    assert "method: 'POST'" not in js
    assert "/api/ai-sidecar/codex-prompts" not in js
    forbidden_labels = (
        "Codex 실행",
        "PR 생성",
        "브랜치 생성",
        "커밋",
        "푸시",
        "자동 수정",
        "적용",
    )
    for forbidden_label in forbidden_labels:
        assert forbidden_label not in html + js


def _safe_draft() -> AICodexPromptDraft:
    prompt_text = "\n\n".join(
        [
            "## 1. 역할\n너는 read-only Codex prompt draft reviewer다.",
            "## 2. 대상 저장소\njuchb4145-ai/suseok-trader-v2",
            "## 8. 명시적 금지\n"
            "- 자동 주문/매수/매도 기능을 추가하지 말 것\n"
            "- OrderIntent, GatewayCommand, send_order/cancel_order/modify_order를 만들지 말 것\n"
            "- Strategy/Risk/OMS 자동 판단으로 AI/RCA output을 사용하지 말 것\n"
            "- GitHub branch/commit/push/PR을 자동 생성하지 말 것\n"
            "- 파일 수정은 Codex가 사용자의 명시적 작업 범위 안에서만 수행하며, "
            "이 draft generator는 파일을 수정하지 않는다\n"
            "- 테스트와 문서 업데이트를 포함할 것",
        ]
    )
    return AICodexPromptDraft(
        draft_id="draft-safe",
        title="Safe draft",
        source_type=AICodexPromptSourceType.MANUAL_NOTE,
        target_area=AICodexPromptTargetArea.SAFETY_REVIEW,
        status=AICodexPromptDraftStatus.COMPLETED,
        summary="Safe human-copyable draft.",
        prompt_text=prompt_text,
        safety_notes=["review only"],
        acceptance_criteria=["tests pass"],
        forbidden_scope=["automatic execution 금지"],
        test_plan=["pytest"],
    )


def _save_report(
    connection,
    *,
    report_id: str,
    report_type: str,
) -> AIRCAReport:
    section = AIRCASection(
        section_name="safety",
        status="INFO",
        severity="INFO",
        summary="Read-only RCA section.",
        reason_codes=["NO_ORDER_PATH_BY_DESIGN", "OBSERVE_ONLY_PIPELINE"],
        evidence_json={"observe_only": True},
        source_refs=["dashboard"],
    )
    report = AIRCAReport(
        report_id=report_id,
        report_type=AIRCAReportType(report_type),
        trade_date="2026-06-27",
        title=f"{report_type} fixture",
        summary="No order path by design.",
        status=AIRCAReportStatus.COMPLETED,
        severity="INFO",
        root_cause_category="NO_ORDER_PATH_BY_DESIGN",
        root_cause="NO_ORDER_PATH_BY_DESIGN: observe-only pipeline has no OMS/order path.",
        deterministic_sections=[section],
        suggested_checks=["Review deterministic evidence"],
        warnings=["No trading side effects"],
    )
    save_rca_report(connection, report)
    return report


def _insert_candidate(connection) -> None:
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
        VALUES (?, '2026-06-27', '005930', '삼성전자', 1, 'DATA_WAIT', ?, ?, ?, 'MANUAL', 'test', ?)
        """,
        (
            "candidate-1",
            "2026-06-27T00:00:00Z",
            "2026-06-27T00:01:00Z",
            "2026-06-27T00:01:00Z",
            json.dumps(["CANDIDATE_DATA_WAIT"]),
        ),
    )
    connection.commit()


def _valid_codex_output() -> dict[str, object]:
    return {
        "summary": "Codex prompt draft reviewed by mock AI.",
        "severity": "LOW",
        "root_cause": "Read-only prompt drafting only.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Review deterministic prompt sections"],
        "confidence": 0.75,
        "forbidden_actions_confirmed": True,
        "prompt_draft": (
            "문서와 테스트를 보강하되 자동 주문/매수/매도 기능을 추가하지 말 것. "
            "GitHub branch/commit/push/PR을 자동 생성하지 말 것."
        ),
    }
