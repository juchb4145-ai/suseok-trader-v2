from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from domain.ai_sidecar.codex_prompt import (
    AICodexPromptBuildResult,
    AICodexPromptDraft,
    AICodexPromptDraftStatus,
    AICodexPromptSection,
    AICodexPromptSourceType,
    AICodexPromptTargetArea,
    ensure_codex_prompt_policy,
    sanitize_prompt_text,
)
from domain.ai_sidecar.tasks import AISidecarTaskType

from services.ai_sidecar.codex_prompt_store import (
    save_codex_prompt_draft,
    save_codex_prompt_error,
)
from services.ai_sidecar.context_builder import build_context_packet
from services.ai_sidecar.context_store import get_context_packet, save_context_packet
from services.ai_sidecar.openai_client import AISidecarModelClient
from services.ai_sidecar.rca_report_store import get_rca_report, list_rca_reports
from services.ai_sidecar.request_store import get_ai_insight
from services.ai_sidecar.runner import AISidecarRunResult, run_ai_sidecar_task
from services.candidate_service import get_candidate
from services.config import Settings, load_settings
from services.risk_gate import get_latest_risk_observation
from services.strategy_engine import get_latest_strategy_observation

REPOSITORY_NAME = "juchb4145-ai/suseok-trader-v2"

CURRENT_STATUS = [
    "PR 0 Bootstrap 완료",
    "PR 1 Broker-neutral Contract 완료",
    "PR 2A AI Sidecar Read-Only Contract 완료",
    "PR 2B Event Store + Gateway Transport Surface 완료",
    "PR 3 Mock Gateway + Gateway Adapter Skeleton 완료",
    "PR 4 Market Data Service 완료",
    "PR 5 Theme Membership + Theme Snapshot 완료",
    "PR 6 Candidate FSM 완료",
    "PR 7 Strategy Engine observe-only 완료",
    "PR 8 Risk Gate observe-only 완료",
    "PR 9 Dashboard V1 완료",
    "PR AI-1 LLM Context Builder 완료",
    "PR AI-2 OpenAI Client + Structured Outputs 완료",
    "PR AI-3 No-trade RCA / Candidate Block RCA 완료",
    "PR AI-4 Dashboard AI Explanation Cards 완료",
    "현재 PR AI-5 Codex Prompt Generator는 review-only artifact generator다.",
]

REQUIRED_SAFETY_PHRASES = [
    "자동 주문/매수/매도 기능을 추가하지 말 것",
    "OrderIntent, GatewayCommand, send_order/cancel_order/modify_order를 만들지 말 것",
    "Strategy/Risk/OMS 자동 판단으로 AI/RCA output을 사용하지 말 것",
    "GitHub branch/commit/push/PR을 자동 생성하지 말 것",
    (
        "파일 수정은 Codex가 사용자의 명시적 작업 범위 안에서만 수행하며, "
        "이 draft generator는 파일을 수정하지 않는다"
    ),
    "테스트와 문서 업데이트를 포함할 것",
]

BASE_FORBIDDEN_SCOPE = [
    "자동 주문/매수/매도 기능을 추가하지 말 것",
    "OrderIntent, GatewayCommand, send_order/cancel_order/modify_order를 만들지 말 것",
    "Strategy/Risk/OMS 자동 판단으로 AI/RCA output을 사용하지 말 것",
    "GitHub branch/commit/push/PR을 자동 생성하지 말 것",
    "Codex API 직접 호출, GitHub write API 호출, background worker 구현 금지",
    "LIVE_SIM/LIVE_REAL flag를 변경하지 말 것",
    "Candidate/Strategy/Risk/Gateway/Market/Theme state를 mutation하지 말 것",
    "Dashboard에 Codex 실행, PR 생성, 브랜치 생성, 커밋, 푸시, 자동 수정, 적용 버튼을 만들지 말 것",
]

BASE_SAFETY_NOTES = [
    "Codex prompt draft는 human-copyable review artifact다.",
    "draft generator는 repository file을 수정하지 않는다.",
    "Dashboard는 GET/read-only display와 browser clipboard 복사만 허용한다.",
    "AI-assisted draft는 run_ai=true일 때만 시도하고 실패해도 deterministic draft를 유지한다.",
    "AI output은 forbidden action policy와 sanitizer를 통과해야 한다.",
]


def build_codex_prompt_from_rca_report(
    connection: sqlite3.Connection,
    report_id: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AICodexPromptBuildResult:
    resolved_settings = settings or load_settings()
    try:
        report = get_rca_report(connection, report_id, include_sections=True, include_links=True)
        if report is None:
            raise ValueError("RCA report not found")
        draft = _draft_from_report(
            connection,
            report,
            source_type=AICodexPromptSourceType.RCA_REPORT,
            title=f"Codex prompt draft from RCA report {report_id}",
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
        )
        save_codex_prompt_draft(connection, draft)
        return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)
    except Exception as exc:
        save_codex_prompt_error(
            connection,
            source_type=AICodexPromptSourceType.RCA_REPORT,
            target_area=AICodexPromptTargetArea.UNKNOWN,
            error_message=str(exc),
            payload={"report_id": report_id, "run_ai": run_ai},
        )
        return AICodexPromptBuildResult(ok=False, error_message=str(exc))


def build_codex_prompt_from_candidate(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AICodexPromptBuildResult:
    resolved_settings = settings or load_settings()
    try:
        candidate = get_candidate(
            connection,
            candidate_instance_id,
            include_context=True,
            include_sources=True,
            include_transitions=True,
        )
        strategy = get_latest_strategy_observation(
            connection,
            candidate_instance_id,
            include_setups=True,
        )
        risk = get_latest_risk_observation(connection, candidate_instance_id, include_checks=True)
        packet = build_context_packet(
            connection,
            AISidecarTaskType.CANDIDATE_BLOCK_RCA,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        latest_report = _latest_candidate_rca_report(connection, candidate_instance_id)
        reason_codes = _collect_reason_codes_from_payload(
            {
                "candidate": candidate,
                "strategy": strategy,
                "risk": risk,
                "context": packet.payload,
            }
        )
        target_area = infer_target_area_from_reason_codes(reason_codes)
        problem_summary = _candidate_problem_summary(candidate, strategy, risk)
        evidence = _format_evidence(
            {
                "candidate": candidate or {},
                "strategy": strategy or {},
                "risk": risk or {},
                "context_id": packet.context_id,
                "latest_candidate_rca_report_id": (
                    None if latest_report is None else latest_report["report_id"]
                ),
                "reason_codes": reason_codes,
            }
        )
        acceptance = build_acceptance_criteria(
            target_area,
            source_type=AICodexPromptSourceType.CANDIDATE,
        )
        forbidden = build_forbidden_scope()
        test_plan = build_test_plan(target_area, source_type=AICodexPromptSourceType.CANDIDATE)
        sections = build_prompt_sections(
            problem_summary=problem_summary,
            evidence=evidence,
            target_area=target_area,
            implementation_scope=(
                "candidate 상태, context, strategy/risk observation을 근거로 원인 진단과 "
                "테스트 우선 수정 범위를 제안한다. 자동 매수/주문 요구는 포함하지 않는다."
            ),
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            source_refs=("candidates", "strategy_observations_latest", "risk_observations_latest"),
        )
        prompt_text = _render_prompt_text(sections)
        draft = AICodexPromptDraft(
            title=f"Candidate Codex prompt draft for {candidate_instance_id}",
            source_type=AICodexPromptSourceType.CANDIDATE,
            target_area=target_area,
            status=AICodexPromptDraftStatus.COMPLETED,
            trade_date=(candidate or {}).get("trade_date") or packet.trade_date,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            rca_report_id=None if latest_report is None else latest_report["report_id"],
            context_id=packet.context_id,
            summary=problem_summary,
            prompt_text=prompt_text,
            safety_notes=BASE_SAFETY_NOTES,
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            metadata={
                "deterministic_prompt": True,
                "source_context_task": AISidecarTaskType.CANDIDATE_BLOCK_RCA.value,
                "strategy_risk_oms_input": False,
            },
            sections=sections,
            run_ai=run_ai,
        )
        draft = _maybe_apply_ai_assist(
            connection,
            draft,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
        )
        save_codex_prompt_draft(connection, draft)
        return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)
    except Exception as exc:
        save_codex_prompt_error(
            connection,
            source_type=AICodexPromptSourceType.CANDIDATE,
            target_area=AICodexPromptTargetArea.CANDIDATE_FSM,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return AICodexPromptBuildResult(ok=False, error_message=str(exc))


def build_codex_prompt_from_no_trade(
    connection: sqlite3.Connection,
    trade_date: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AICodexPromptBuildResult:
    resolved_settings = settings or load_settings()
    try:
        latest_report = _latest_no_trade_rca_report(connection, trade_date)
        if latest_report is not None:
            report = get_rca_report(
                connection,
                latest_report["report_id"],
                include_sections=True,
                include_links=True,
            )
            if report is not None:
                draft = _draft_from_report(
                    connection,
                    report,
                    source_type=AICodexPromptSourceType.NO_TRADE,
                    title=f"No-trade Codex prompt draft for {trade_date}",
                    run_ai=run_ai,
                    model_client=model_client,
                    settings=resolved_settings,
                )
                save_codex_prompt_draft(connection, draft)
                return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)

        packet = build_context_packet(
            connection,
            AISidecarTaskType.NO_TRADE_RCA,
            trade_date=trade_date,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        reason_codes = _collect_reason_codes_from_payload(packet.payload)
        target_area = infer_target_area_from_reason_codes(reason_codes)
        problem_summary = (
            f"{trade_date} no-trade context packet 기반 Codex prompt draft. "
            "NO_ORDER_PATH_BY_DESIGN: OMS/order path 없음은 설계상 정상이다."
        )
        evidence = _format_evidence(
            {
                "context_id": packet.context_id,
                "pipeline_funnel_counts": packet.payload.get("pipeline_funnel_counts", {}),
                "reason_code_summary": packet.payload.get("reason_code_summary", {}),
                "recent_errors": packet.payload.get("recent_errors", {}),
                "note": "OMS/order path 없음은 설계상 정상이며 자동 주문 활성화 요구 금지.",
            }
        )
        acceptance = build_acceptance_criteria(
            target_area,
            source_type=AICodexPromptSourceType.NO_TRADE,
        )
        forbidden = build_forbidden_scope()
        test_plan = build_test_plan(target_area, source_type=AICodexPromptSourceType.NO_TRADE)
        sections = build_prompt_sections(
            problem_summary=problem_summary,
            evidence=evidence,
            target_area=target_area,
            implementation_scope=(
                "pipeline counts, reason codes, recent errors를 검토해 no-trade 원인을 "
                "테스트 우선으로 좁힌다. OMS/order path 없음은 설계상 정상이라는 note를 유지한다."
            ),
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            source_refs=("ai_context_packets", "candidate/strategy/risk projections"),
        )
        draft = AICodexPromptDraft(
            title=f"No-trade Codex prompt draft for {trade_date}",
            source_type=AICodexPromptSourceType.NO_TRADE,
            target_area=target_area,
            status=AICodexPromptDraftStatus.COMPLETED,
            trade_date=trade_date,
            context_id=packet.context_id,
            summary=problem_summary,
            prompt_text=_render_prompt_text(sections),
            safety_notes=BASE_SAFETY_NOTES,
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            metadata={
                "deterministic_prompt": True,
                "no_order_path_by_design": True,
                "strategy_risk_oms_input": False,
            },
            sections=sections,
            run_ai=run_ai,
        )
        draft = _maybe_apply_ai_assist(
            connection,
            draft,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
        )
        save_codex_prompt_draft(connection, draft)
        return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)
    except Exception as exc:
        save_codex_prompt_error(
            connection,
            source_type=AICodexPromptSourceType.NO_TRADE,
            target_area=AICodexPromptTargetArea.RCA_WORKFLOW,
            trade_date=trade_date,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return AICodexPromptBuildResult(ok=False, error_message=str(exc))


def build_codex_prompt_from_ops_incident(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    related_entity_id: str | None = None,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AICodexPromptBuildResult:
    resolved_settings = settings or load_settings()
    try:
        packet = build_context_packet(
            connection,
            AISidecarTaskType.OPS_INCIDENT_SUMMARY,
            trade_date=trade_date,
            related_entity_type="incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        evidence = _format_evidence(
            {
                "context_id": packet.context_id,
                "gateway_status": packet.payload.get("gateway_status", {}),
                "gateway_problem_events": packet.payload.get("gateway_problem_events", {}),
                "error_summary": packet.payload.get("error_summary", {}),
            }
        )
        acceptance = [
            "운영 장애 원인을 evidence collection과 재현 가능한 테스트로 좁힐 것",
            "trading logic 변경보다 관측 데이터/오류 수집/검증 경로를 우선할 것",
            "Dashboard와 API는 read-only 표시 상태를 유지할 것",
        ]
        forbidden = build_forbidden_scope()
        test_plan = [
            "OPS_INCIDENT_SUMMARY context packet 생성 테스트",
            "gateway/market/theme/candidate/strategy/risk/ai error summary 단위 테스트",
            "Dashboard/API read-only safety regression 테스트",
        ]
        problem_summary = (
            "운영 장애 조사용 Codex prompt draft. "
            "자동 실행 없이 evidence-first RCA를 요구한다."
        )
        sections = build_prompt_sections(
            problem_summary=problem_summary,
            evidence=evidence,
            target_area=AICodexPromptTargetArea.GATEWAY_TRANSPORT,
            implementation_scope=(
                "gateway/market/theme/candidate/strategy/risk/ai errors를 근거로 운영 장애 "
                "조사 범위와 테스트를 제안한다."
            ),
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            source_refs=("gateway_events", "projection_errors", "ai_context_packets"),
        )
        draft = AICodexPromptDraft(
            title="Ops incident Codex prompt draft",
            source_type=AICodexPromptSourceType.OPS_INCIDENT,
            target_area=AICodexPromptTargetArea.GATEWAY_TRANSPORT,
            status=AICodexPromptDraftStatus.COMPLETED,
            trade_date=trade_date,
            related_entity_type="incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            context_id=packet.context_id,
            summary=problem_summary,
            prompt_text=_render_prompt_text(sections),
            safety_notes=BASE_SAFETY_NOTES,
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            metadata={"deterministic_prompt": True, "evidence_first": True},
            sections=sections,
            run_ai=run_ai,
        )
        draft = _maybe_apply_ai_assist(
            connection,
            draft,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
        )
        save_codex_prompt_draft(connection, draft)
        return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)
    except Exception as exc:
        save_codex_prompt_error(
            connection,
            source_type=AICodexPromptSourceType.OPS_INCIDENT,
            target_area=AICodexPromptTargetArea.GATEWAY_TRANSPORT,
            trade_date=trade_date,
            related_entity_type="incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return AICodexPromptBuildResult(ok=False, error_message=str(exc))


def build_safety_review_prompt(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> AICodexPromptBuildResult:
    del settings
    try:
        recent_drafts = connection.execute(
            """
            SELECT status, source_type, target_area, generated_at
            FROM ai_codex_prompt_drafts
            ORDER BY generated_at DESC
            LIMIT 10
            """
        ).fetchall()
        evidence = _format_evidence(
            {
                "recent_codex_prompt_drafts": [
                    {key: row[key] for key in row.keys()} for row in recent_drafts
                ],
                "review_focus": (
                    "PR10 OMS + DRY_RUN 전에 Sidecar/RCA/Codex prompt가 review-only "
                    "artifact로 유지되는지 확인"
                ),
            }
        )
        acceptance = [
            "PR10 OMS + DRY_RUN 도입 전 OrderIntent/OMS 경계와 safety gate가 명시될 것",
            "Sidecar/RCA/Codex prompt output이 Strategy/Risk/OMS 자동 입력이 아님을 확인할 것",
            "Dashboard/API/CLI에 자동 실행, 자동 적용, GitHub write 경로가 없음을 확인할 것",
            "OpenAI tools/function calling과 order tools가 계속 비활성임을 확인할 것",
        ]
        forbidden = build_forbidden_scope()
        test_plan = [
            "safety regression tests for no Codex/GitHub/order side effects",
            "API route inspection for GET-only dashboard display/copy policy",
            "storage flag tests for auto_apply_allowed=false and github_write_allowed=false",
            "OpenAI structured output tests confirming tools/function calling disabled",
        ]
        problem_summary = (
            "PR10 OMS + DRY_RUN 이전 safety review prompt. "
            "review-only artifact 경계가 유지되는지 점검한다."
        )
        sections = build_prompt_sections(
            problem_summary=problem_summary,
            evidence=evidence,
            target_area=AICodexPromptTargetArea.SAFETY_REVIEW,
            implementation_scope=(
                "OrderIntent/OMS 도입 전 acceptance criteria와 safety gate를 검토하고, "
                "현재 Sidecar/RCA/Codex prompt가 review-only인지 확인한다."
            ),
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            source_refs=("docs/roadmap.md", "docs/ai_sidecar_safety_policy.md"),
        )
        draft = AICodexPromptDraft(
            title="PR10 OMS + DRY_RUN safety review Codex prompt",
            source_type=AICodexPromptSourceType.MANUAL_NOTE,
            target_area=AICodexPromptTargetArea.SAFETY_REVIEW,
            status=AICodexPromptDraftStatus.COMPLETED,
            summary=problem_summary,
            prompt_text=_render_prompt_text(sections),
            safety_notes=BASE_SAFETY_NOTES,
            acceptance_criteria=acceptance,
            forbidden_scope=forbidden,
            test_plan=test_plan,
            metadata={
                "deterministic_prompt": True,
                "pr10_safety_review": True,
                "auto_apply_allowed": False,
            },
            sections=sections,
        )
        save_codex_prompt_draft(connection, draft)
        return AICodexPromptBuildResult(ok=True, draft=draft, warnings=draft.warnings)
    except Exception as exc:
        save_codex_prompt_error(
            connection,
            source_type=AICodexPromptSourceType.MANUAL_NOTE,
            target_area=AICodexPromptTargetArea.SAFETY_REVIEW,
            error_message=str(exc),
            payload={"workflow": "safety_review"},
        )
        return AICodexPromptBuildResult(ok=False, error_message=str(exc))


def infer_target_area_from_report(report: Mapping[str, Any]) -> AICodexPromptTargetArea:
    category = str(report.get("root_cause_category") or "").upper()
    mapping = {
        "GATEWAY_TRANSPORT": AICodexPromptTargetArea.GATEWAY_TRANSPORT,
        "MARKET_DATA_PROJECTION": AICodexPromptTargetArea.MARKET_DATA,
        "THEME_PROJECTION": AICodexPromptTargetArea.THEME_SERVICE,
        "THEME_CONTEXT": AICodexPromptTargetArea.THEME_SERVICE,
        "CANDIDATE_PROJECTION": AICodexPromptTargetArea.CANDIDATE_FSM,
        "CANDIDATE_CONTEXT": AICodexPromptTargetArea.CANDIDATE_FSM,
        "DATA_QUALITY": AICodexPromptTargetArea.MARKET_DATA,
        "STRATEGY_CONTEXT": AICodexPromptTargetArea.STRATEGY_ENGINE,
        "STRATEGY_EVALUATION": AICodexPromptTargetArea.STRATEGY_ENGINE,
        "RISK_CONTEXT": AICodexPromptTargetArea.RISK_GATE,
        "RISK_EVALUATION": AICodexPromptTargetArea.RISK_GATE,
        "AI_EXECUTION": AICodexPromptTargetArea.AI_SIDECAR,
        "NO_ORDER_PATH_BY_DESIGN": AICodexPromptTargetArea.SAFETY_REVIEW,
        "OBSERVE_ONLY_PIPELINE": AICodexPromptTargetArea.SAFETY_REVIEW,
    }
    if category in mapping:
        return mapping[category]
    reason_codes = []
    for section in report.get("deterministic_sections", []):
        if isinstance(section, Mapping):
            reason_codes.extend(_strings(section.get("reason_codes")))
    return infer_target_area_from_reason_codes(reason_codes)


def infer_target_area_from_reason_codes(
    reason_codes: Sequence[str],
) -> AICodexPromptTargetArea:
    joined = " ".join(str(reason).upper() for reason in reason_codes)
    if "GATEWAY" in joined:
        return AICodexPromptTargetArea.GATEWAY_TRANSPORT
    if "MARKET" in joined or "TICK" in joined or "BAR" in joined or "VWAP" in joined:
        return AICodexPromptTargetArea.MARKET_DATA
    if "THEME" in joined:
        return AICodexPromptTargetArea.THEME_SERVICE
    if "CANDIDATE" in joined or "SOURCE" in joined:
        return AICodexPromptTargetArea.CANDIDATE_FSM
    if "STRATEGY" in joined or "MATCHED_OBSERVATION" in joined:
        return AICodexPromptTargetArea.STRATEGY_ENGINE
    if "RISK" in joined or "OBSERVE_BLOCK" in joined or "OBSERVE_PASS" in joined:
        return AICodexPromptTargetArea.RISK_GATE
    if "DASHBOARD" in joined:
        return AICodexPromptTargetArea.DASHBOARD
    if "RCA" in joined:
        return AICodexPromptTargetArea.RCA_WORKFLOW
    if "DOC" in joined:
        return AICodexPromptTargetArea.DOCS
    if "TEST" in joined:
        return AICodexPromptTargetArea.TESTING
    if "NO_ORDER_PATH_BY_DESIGN" in joined or "OBSERVE_ONLY" in joined:
        return AICodexPromptTargetArea.SAFETY_REVIEW
    return AICodexPromptTargetArea.UNKNOWN


def build_prompt_sections(
    *,
    problem_summary: str,
    evidence: str,
    target_area: AICodexPromptTargetArea | str,
    implementation_scope: str,
    acceptance_criteria: Sequence[str],
    forbidden_scope: Sequence[str],
    test_plan: Sequence[str],
    source_refs: Sequence[str] = (),
) -> list[AICodexPromptSection]:
    target = (
        target_area
        if isinstance(target_area, AICodexPromptTargetArea)
        else AICodexPromptTargetArea(str(target_area))
    )
    return [
        AICodexPromptSection(
            section_name="role",
            title="역할",
            body=(
                "너는 Kiwoom Open"
                "API+ 기반 국내 주식 자동매매 시스템을 구현하는 "
                "시니어 Python 백엔드 엔지니어, 퀀트 전략 엔지니어, 리스크 엔지니어, "
                "운영 RCA 분석가다. 안전 원칙을 우선하고 테스트를 함께 작성한다."
            ),
            source_refs=source_refs,
            order_index=1,
        ),
        AICodexPromptSection(
            section_name="repository",
            title="대상 저장소",
            body=REPOSITORY_NAME,
            source_refs=source_refs,
            order_index=2,
        ),
        AICodexPromptSection(
            section_name="current_state",
            title="현재 상태",
            body=_bullet_lines(CURRENT_STATUS),
            source_refs=("README.md", "docs/roadmap.md"),
            order_index=3,
        ),
        AICodexPromptSection(
            section_name="problem_summary",
            title="문제/관찰 요약",
            body=problem_summary,
            source_refs=source_refs,
            order_index=4,
        ),
        AICodexPromptSection(
            section_name="evidence",
            title="관련 evidence",
            body=evidence,
            source_refs=source_refs,
            order_index=5,
        ),
        AICodexPromptSection(
            section_name="work_goal",
            title="작업 목표",
            body=(
                f"{target.value} 영역에서 evidence에 기반한 원인을 좁히고, "
                "테스트와 문서 업데이트를 포함한 최소 변경 범위를 제안한다. "
                "파일 수정은 Codex가 사용자의 명시적 작업 범위 안에서만 수행하며, "
                "이 draft generator는 파일을 수정하지 않는다."
            ),
            source_refs=source_refs,
            order_index=6,
        ),
        AICodexPromptSection(
            section_name="implementation_scope",
            title="구현 범위",
            body=implementation_scope,
            source_refs=source_refs,
            order_index=7,
        ),
        AICodexPromptSection(
            section_name="forbidden_scope",
            title="명시적 금지",
            body=_bullet_lines(_unique([*REQUIRED_SAFETY_PHRASES, *forbidden_scope])),
            source_refs=source_refs,
            order_index=8,
        ),
        AICodexPromptSection(
            section_name="test_plan",
            title="테스트 요구사항",
            body=_bullet_lines(test_plan),
            source_refs=source_refs,
            order_index=9,
        ),
        AICodexPromptSection(
            section_name="completion_criteria",
            title="완료 기준",
            body=_bullet_lines(acceptance_criteria),
            source_refs=source_refs,
            order_index=10,
        ),
        AICodexPromptSection(
            section_name="report_format",
            title="작업 후 보고 형식",
            body=_bullet_lines(
                [
                    "요약",
                    "생성/수정 파일 목록",
                    "안전상 유지된 금지 항목",
                    "테스트 결과",
                    "남은 리스크와 다음 PR 추천",
                ]
            ),
            source_refs=source_refs,
            order_index=11,
        ),
    ]


def build_forbidden_scope(
    extra_items: Sequence[str] = (),
) -> list[str]:
    return _unique([*BASE_FORBIDDEN_SCOPE, *extra_items])


def build_acceptance_criteria(
    target_area: AICodexPromptTargetArea | str,
    *,
    source_type: AICodexPromptSourceType | str,
) -> list[str]:
    target = (
        target_area.value
        if isinstance(target_area, AICodexPromptTargetArea)
        else str(target_area)
    )
    source = (
        source_type.value
        if isinstance(source_type, AICodexPromptSourceType)
        else str(source_type)
    )
    return [
        f"{source} evidence를 근거로 {target} 변경 범위가 명확히 정리될 것",
        "deterministic tests가 OpenAI API key 없이 통과할 것",
        "Dashboard/API/CLI가 review-only artifact 경계를 유지할 것",
        "테스트와 문서 업데이트를 포함할 것",
        "자동 주문/매수/매도 기능을 추가하지 말 것",
    ]


def build_test_plan(
    target_area: AICodexPromptTargetArea | str,
    *,
    source_type: AICodexPromptSourceType | str,
) -> list[str]:
    target = (
        target_area.value
        if isinstance(target_area, AICodexPromptTargetArea)
        else str(target_area)
    )
    source = (
        source_type.value
        if isinstance(source_type, AICodexPromptSourceType)
        else str(source_type)
    )
    return [
        f"{source} 기반 deterministic prompt 생성 단위 테스트",
        f"{target} inference와 forbidden scope 포함 테스트",
        "OpenAI API key 없이 run_ai=false 경로 테스트",
        "forbidden action sanitizer safety regression 테스트",
        "Dashboard display/copy-only GET 경로 테스트",
    ]


def validate_prompt_draft_safety(draft: AICodexPromptDraft) -> None:
    if not draft.observe_only:
        raise ValueError("prompt draft must be observe_only")
    if not draft.human_review_required:
        raise ValueError("prompt draft must require human review")
    if draft.auto_apply_allowed or draft.github_write_allowed or draft.codex_execution_allowed:
        raise ValueError("prompt draft cannot allow auto apply, GitHub writes, or Codex execution")
    if not draft.no_trading_side_effects:
        raise ValueError("prompt draft must have no trading side effects")
    ensure_codex_prompt_policy(draft.prompt_text)


def _draft_from_report(
    connection: sqlite3.Connection,
    report: Mapping[str, Any],
    *,
    source_type: AICodexPromptSourceType,
    title: str,
    run_ai: bool,
    model_client: AISidecarModelClient | None,
    settings: Settings,
) -> AICodexPromptDraft:
    context = (
        get_context_packet(connection, report["context_id"]) if report.get("context_id") else None
    )
    insight = (
        get_ai_insight(connection, report["ai_insight_id"])
        if report.get("ai_insight_id")
        else None
    )
    target_area = infer_target_area_from_report(report)
    reason_codes = []
    for section in report.get("deterministic_sections", []):
        if isinstance(section, Mapping):
            reason_codes.extend(_strings(section.get("reason_codes")))
    problem_summary = (
        f"{report.get('title') or report.get('report_type')} 기반 Codex prompt draft. "
        f"RCA summary: {report.get('summary')}"
    )
    evidence = _format_evidence(
        {
            "report_id": report.get("report_id"),
            "report_type": report.get("report_type"),
            "status": report.get("status"),
            "severity": report.get("severity"),
            "root_cause_category": report.get("root_cause_category"),
            "root_cause": report.get("root_cause"),
            "reason_codes": reason_codes,
            "sections": report.get("deterministic_sections", []),
            "context_packet": _compact_context(context),
            "ai_insight": insight or {},
            "warnings": report.get("warnings", []),
        }
    )
    acceptance = build_acceptance_criteria(target_area, source_type=source_type)
    forbidden = build_forbidden_scope()
    test_plan = build_test_plan(target_area, source_type=source_type)
    sections = build_prompt_sections(
        problem_summary=problem_summary,
        evidence=evidence,
        target_area=target_area,
        implementation_scope=(
            "RCA report와 연결된 context/insight를 근거로 원인을 재현하고, "
            "테스트 우선의 최소 구현 또는 문서 보강 범위를 제안한다. "
            "RCA report 자체는 Strategy/Risk/OMS 자동 입력이 아니다."
        ),
        acceptance_criteria=acceptance,
        forbidden_scope=forbidden,
        test_plan=test_plan,
        source_refs=("ai_rca_reports", "ai_rca_sections", "ai_context_packets"),
    )
    draft = AICodexPromptDraft(
        title=title,
        source_type=source_type,
        target_area=target_area,
        status=AICodexPromptDraftStatus.COMPLETED,
        trade_date=report.get("trade_date"),
        related_entity_type=report.get("related_entity_type"),
        related_entity_id=report.get("related_entity_id"),
        rca_report_id=report.get("report_id"),
        context_id=report.get("context_id"),
        ai_request_id=None,
        ai_insight_id=None,
        summary=problem_summary,
        prompt_text=_render_prompt_text(sections),
        safety_notes=BASE_SAFETY_NOTES,
        acceptance_criteria=acceptance,
        forbidden_scope=forbidden,
        test_plan=test_plan,
        metadata={
            "deterministic_prompt": True,
            "source_report_status": report.get("status"),
            "source_report_ai_request_id": report.get("ai_request_id"),
            "source_report_ai_insight_id": report.get("ai_insight_id"),
            "strategy_risk_oms_input": False,
        },
        sections=sections,
        run_ai=run_ai,
    )
    return _maybe_apply_ai_assist(
        connection,
        draft,
        run_ai=run_ai,
        model_client=model_client,
        settings=settings,
    )


def _maybe_apply_ai_assist(
    connection: sqlite3.Connection,
    draft: AICodexPromptDraft,
    *,
    run_ai: bool,
    model_client: AISidecarModelClient | None,
    settings: Settings,
) -> AICodexPromptDraft:
    if not run_ai:
        return draft
    ai_result = run_ai_sidecar_task(
        connection,
        AISidecarTaskType.CODEX_PROMPT_DRAFT,
        trade_date=draft.trade_date,
        related_entity_type=draft.related_entity_type,
        related_entity_id=draft.related_entity_id,
        context_id=draft.context_id,
        persist_context=True,
        model_client=model_client,
        settings=settings,
    )
    warnings = [*draft.warnings]
    if not ai_result.ok:
        warnings.append(f"AI-assisted CODEX_PROMPT_DRAFT failed: {ai_result.status}")
        return replace(
            draft,
            status=_draft_status_from_ai_result(ai_result),
            ai_request_id=ai_result.request_id,
            warnings=_unique(warnings),
        )
    insight = get_ai_insight(connection, ai_result.insight_id or "")
    output = dict((insight or {}).get("output") or {})
    ai_prompt = sanitize_prompt_text(str(output.get("prompt_draft") or ""))
    ensure_codex_prompt_policy(ai_prompt)
    merged_prompt = _merge_ai_prompt(draft.prompt_text, ai_prompt)
    ensure_codex_prompt_policy(merged_prompt)
    warnings.append("AI-assisted CODEX_PROMPT_DRAFT output passed safety sanitizer.")
    return replace(
        draft,
        status=AICodexPromptDraftStatus.COMPLETED,
        prompt_text=merged_prompt,
        ai_request_id=ai_result.request_id,
        ai_insight_id=ai_result.insight_id,
        warnings=_unique(warnings),
    )


def _draft_status_from_ai_result(result: AISidecarRunResult) -> AICodexPromptDraftStatus:
    status = result.status.upper()
    if status == "AI_DISABLED":
        return AICodexPromptDraftStatus.AI_DISABLED
    if status in {"API_KEY_MISSING", "CLIENT_UNAVAILABLE", "MODEL_ERROR", "TIMEOUT"}:
        return AICodexPromptDraftStatus.AI_UNAVAILABLE
    if status == "AI_OUTPUT_INVALID":
        return AICodexPromptDraftStatus.AI_OUTPUT_INVALID
    if status == "POLICY_REJECTED":
        return AICodexPromptDraftStatus.POLICY_REJECTED
    return AICodexPromptDraftStatus.PARTIAL


def _merge_ai_prompt(deterministic_prompt: str, ai_prompt: str) -> str:
    return sanitize_prompt_text(
        "\n\n".join(
            [
                deterministic_prompt,
                "## AI-assisted 보강 메모",
                (
                    "아래 내용은 run_ai=true로 생성된 CODEX_PROMPT_DRAFT output 중 "
                    "safety sanitizer를 통과한 보강 정보다. 사람이 검토하고 복사할 때만 사용한다."
                ),
                ai_prompt,
            ]
        )
    )


def _render_prompt_text(sections: Sequence[AICodexPromptSection]) -> str:
    ordered = sorted(sections, key=lambda section: section.order_index)
    rendered = []
    for index, section in enumerate(ordered, start=1):
        rendered.append(f"## {index}. {section.title}\n{section.body}")
    prompt = "\n\n".join(rendered)
    ensure_codex_prompt_policy(prompt)
    return prompt


def _latest_candidate_rca_report(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any] | None:
    rows = list_rca_reports(
        connection,
        report_type="CANDIDATE_BLOCK_RCA",
        related_entity_type="candidate",
        related_entity_id=candidate_instance_id,
        limit=1,
    )
    return rows[0] if rows else None


def _latest_no_trade_rca_report(
    connection: sqlite3.Connection,
    trade_date: str,
) -> dict[str, Any] | None:
    rows = list_rca_reports(
        connection,
        report_type="NO_TRADE_RCA",
        trade_date=trade_date,
        limit=1,
    )
    return rows[0] if rows else None


def _candidate_problem_summary(
    candidate: Mapping[str, Any] | None,
    strategy: Mapping[str, Any] | None,
    risk: Mapping[str, Any] | None,
) -> str:
    if candidate is None:
        return "candidate row가 없어 projection/context 생성 경로를 점검해야 한다."
    return (
        f"{candidate.get('name') or candidate.get('code')} candidate "
        f"{candidate.get('candidate_instance_id')} 상태={candidate.get('state')}; "
        f"strategy={None if strategy is None else strategy.get('overall_status')}; "
        f"risk={None if risk is None else risk.get('overall_status')}."
    )


def _compact_context(context: Mapping[str, Any] | None) -> dict[str, Any]:
    if not context:
        return {}
    return {
        "context_id": context.get("context_id"),
        "task_type": context.get("task_type"),
        "trade_date": context.get("trade_date"),
        "related_entity_type": context.get("related_entity_type"),
        "related_entity_id": context.get("related_entity_id"),
        "warnings": context.get("warnings", []),
        "source_sections": context.get("source_sections", []),
    }


def _format_evidence(value: Mapping[str, Any]) -> str:
    lines: list[str] = []
    for key, item in value.items():
        lines.append(f"- {key}: {_compact_value(item)}")
    return "\n".join(lines)


def _compact_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, Mapping):
        parts = []
        for index, (key, item) in enumerate(value.items()):
            if index >= 12:
                parts.append("...")
                break
            parts.append(f"{key}={_compact_value(item)}")
        return "{" + ", ".join(parts) + "}"
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        items = list(value)
        rendered = ", ".join(_compact_value(item) for item in items[:8])
        suffix = ", ..." if len(items) > 8 else ""
        return "[" + rendered + suffix + "]"
    return str(value)


def _collect_reason_codes_from_payload(value: object) -> list[str]:
    reason_codes: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in {"reason_codes", "reason_codes_json"}:
                reason_codes.extend(_strings(item))
            else:
                reason_codes.extend(_collect_reason_codes_from_payload(item))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for item in value:
            reason_codes.extend(_collect_reason_codes_from_payload(item))
    return _unique(reason_codes)


def _bullet_lines(items: Sequence[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _strings(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    return []


def _unique(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]
