from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from domain.ai_sidecar.live_sim_review import (
    LiveSimReviewBuildResult,
    LiveSimReviewReport,
    LiveSimReviewReportType,
    LiveSimReviewRootCauseCategory,
    LiveSimReviewSection,
    LiveSimReviewSeverity,
    LiveSimReviewStatus,
)
from domain.ai_sidecar.tasks import AISidecarTaskType

from services.ai_sidecar.context_builder import (
    build_live_sim_incident_context,
    build_live_sim_order_context,
    build_live_sim_reconcile_context,
    build_live_sim_session_context,
)
from services.ai_sidecar.context_store import save_context_packet
from services.ai_sidecar.live_sim_review_store import (
    save_live_sim_review_error,
    save_live_sim_review_report,
)
from services.ai_sidecar.openai_client import AISidecarModelClient
from services.ai_sidecar.redaction import redact_context
from services.ai_sidecar.request_store import get_ai_insight
from services.ai_sidecar.runner import AISidecarRunResult, run_ai_sidecar_task
from services.config import Settings, load_settings
from services.live_sim.live_sim_service import (
    get_live_sim_intent,
    get_live_sim_order,
    get_live_sim_status,
    list_live_sim_errors,
    list_live_sim_executions,
    list_live_sim_intents,
    list_live_sim_orders,
    list_live_sim_reconcile_snapshots,
    list_live_sim_rejections,
)
from services.live_sim.safety_gate import check_live_sim_safety_gate


def build_live_sim_session_review(
    connection: sqlite3.Connection,
    trade_date: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> LiveSimReviewBuildResult:
    resolved_settings = settings or load_settings()
    try:
        packet = build_live_sim_session_context(
            connection,
            trade_date,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        evidence = _session_evidence(connection, trade_date, resolved_settings)
        sections = _session_sections(evidence)
        category, root_cause, severity = classify_live_sim_session_root_cause(evidence)
        ai_task = _session_ai_task(evidence)
        ai_result = _maybe_run_ai(
            connection,
            ai_task,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=trade_date,
            related_entity_type="live_sim_session",
            related_entity_id=trade_date,
            context_id=packet.context_id,
        )
        report = _build_report(
            report_type=LiveSimReviewReportType.LIVE_SIM_SESSION_REVIEW,
            title=f"LIVE_SIM session review for {trade_date}",
            summary=_session_summary(category, evidence),
            root_cause_category=category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=trade_date,
            related_entity_type="live_sim_session",
            related_entity_id=trade_date,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                "LIVE_SIM review는 주문 생성/취소/수정 입력이 아닙니다.",
                "LIVE_REAL은 범위 밖이며 disabled 상태를 유지합니다.",
                "Dashboard는 review report를 읽기 전용으로 표시합니다.",
                *packet.warnings,
            ],
            suggested_checks=_suggest_session_checks(category),
            metadata={"ai_task_type": ai_task.value},
        )
        save_live_sim_review_report(connection, report)
        return LiveSimReviewBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_live_sim_review_error(
            connection,
            report_type=LiveSimReviewReportType.LIVE_SIM_SESSION_REVIEW,
            trade_date=trade_date,
            related_entity_type="live_sim_session",
            related_entity_id=trade_date,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return LiveSimReviewBuildResult(ok=False, error_message=str(exc))


def build_live_sim_order_review(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> LiveSimReviewBuildResult:
    resolved_settings = settings or load_settings()
    try:
        order = get_live_sim_order(connection, live_sim_order_id)
        if order is None:
            raise ValueError(f"LIVE_SIM order not found: {live_sim_order_id}")
        packet = build_live_sim_order_context(
            connection,
            live_sim_order_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        evidence = _order_evidence(connection, order)
        sections = _order_sections(evidence)
        category, root_cause, severity = classify_live_sim_order_root_cause(evidence)
        ai_result = _maybe_run_ai(
            connection,
            AISidecarTaskType.TRADE_REVIEW,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=order.get("trade_date"),
            related_entity_type="live_sim_order",
            related_entity_id=live_sim_order_id,
            context_id=packet.context_id,
        )
        report = _build_report(
            report_type=LiveSimReviewReportType.LIVE_SIM_ORDER_REVIEW,
            title=f"LIVE_SIM order review for {live_sim_order_id}",
            summary=_order_summary(category, order),
            root_cause_category=category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=order.get("trade_date"),
            related_entity_type="live_sim_order",
            related_entity_id=live_sim_order_id,
            live_sim_intent_id=order.get("live_sim_intent_id"),
            live_sim_order_id=live_sim_order_id,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                "Order review는 재주문, 취소, 정정 명령을 만들지 않습니다.",
                "AI insight가 연결되어도 LIVE_SIM order input으로 사용하지 않습니다.",
                *packet.warnings,
            ],
            suggested_checks=_suggest_order_checks(category),
            metadata={"ai_task_type": AISidecarTaskType.TRADE_REVIEW.value},
        )
        save_live_sim_review_report(connection, report)
        return LiveSimReviewBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_live_sim_review_error(
            connection,
            report_type=LiveSimReviewReportType.LIVE_SIM_ORDER_REVIEW,
            related_entity_type="live_sim_order",
            related_entity_id=live_sim_order_id,
            live_sim_order_id=live_sim_order_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return LiveSimReviewBuildResult(ok=False, error_message=str(exc))


def build_live_sim_reconcile_review(
    connection: sqlite3.Connection,
    reconcile_id: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> LiveSimReviewBuildResult:
    resolved_settings = settings or load_settings()
    try:
        snapshot = _get_reconcile_snapshot(connection, reconcile_id)
        if snapshot is None:
            raise ValueError(f"LIVE_SIM reconcile snapshot not found: {reconcile_id}")
        packet = build_live_sim_reconcile_context(
            connection,
            reconcile_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        evidence = _reconcile_evidence(connection, snapshot)
        sections = _reconcile_sections(evidence)
        category, root_cause, severity = classify_live_sim_reconcile_root_cause(evidence)
        ai_result = _maybe_run_ai(
            connection,
            AISidecarTaskType.OPS_INCIDENT_SUMMARY,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=snapshot.get("trade_date"),
            related_entity_type="live_sim_reconcile",
            related_entity_id=reconcile_id,
            context_id=packet.context_id,
        )
        report = _build_report(
            report_type=LiveSimReviewReportType.LIVE_SIM_RECONCILE_REVIEW,
            title=f"LIVE_SIM reconcile review for {reconcile_id}",
            summary=_reconcile_summary(category, snapshot),
            root_cause_category=category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=snapshot.get("trade_date"),
            related_entity_type="live_sim_reconcile",
            related_entity_id=reconcile_id,
            reconcile_id=reconcile_id,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                "Reconcile review는 local-only 복기 artifact이며 broker 명령을 만들지 않습니다.",
                "Broker snapshot unavailable 상태는 운영자가 별도로 확인해야 합니다.",
                *packet.warnings,
            ],
            suggested_checks=_suggest_reconcile_checks(category),
            metadata={"ai_task_type": AISidecarTaskType.OPS_INCIDENT_SUMMARY.value},
        )
        save_live_sim_review_report(connection, report)
        return LiveSimReviewBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_live_sim_review_error(
            connection,
            report_type=LiveSimReviewReportType.LIVE_SIM_RECONCILE_REVIEW,
            related_entity_type="live_sim_reconcile",
            related_entity_id=reconcile_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return LiveSimReviewBuildResult(ok=False, error_message=str(exc))


def build_live_sim_incident_review(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    related_entity_id: str | None = None,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> LiveSimReviewBuildResult:
    resolved_settings = settings or load_settings()
    try:
        packet = build_live_sim_incident_context(
            connection,
            trade_date=trade_date,
            related_entity_id=related_entity_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        evidence = _incident_evidence(connection, trade_date=trade_date)
        sections = _incident_sections(evidence)
        category, root_cause, severity = classify_live_sim_incident_root_cause(evidence)
        ai_result = _maybe_run_ai(
            connection,
            AISidecarTaskType.OPS_INCIDENT_SUMMARY,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=trade_date,
            related_entity_type="live_sim_incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            context_id=packet.context_id,
        )
        report = _build_report(
            report_type=LiveSimReviewReportType.LIVE_SIM_INCIDENT_REVIEW,
            title="LIVE_SIM incident review",
            summary=_incident_summary(category, evidence),
            root_cause_category=category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=trade_date,
            related_entity_type="live_sim_incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                "Incident review는 운영자 복기용이며 자동 복구 worker가 아닙니다.",
                "Review 결과로 주문 재시도/취소/정정을 수행하지 않습니다.",
                *packet.warnings,
            ],
            suggested_checks=_suggest_incident_checks(category),
            metadata={"ai_task_type": AISidecarTaskType.OPS_INCIDENT_SUMMARY.value},
        )
        save_live_sim_review_report(connection, report)
        return LiveSimReviewBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_live_sim_review_error(
            connection,
            report_type=LiveSimReviewReportType.LIVE_SIM_INCIDENT_REVIEW,
            trade_date=trade_date,
            related_entity_type="live_sim_incident" if related_entity_id else None,
            related_entity_id=related_entity_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return LiveSimReviewBuildResult(ok=False, error_message=str(exc))


def build_live_sim_order_reviews_for_trade_date(
    connection: sqlite3.Connection,
    trade_date: str | None,
    *,
    status: str | None = None,
    limit: int = 50,
    run_ai: bool = False,
    settings: Settings | None = None,
    model_client: AISidecarModelClient | None = None,
) -> list[LiveSimReviewBuildResult]:
    bounded_limit = min(max(int(limit), 1), 100)
    targets = _order_review_targets(
        connection,
        trade_date=trade_date,
        status=status,
        limit=bounded_limit,
    )
    results = []
    for target in targets:
        results.append(
            build_live_sim_order_review(
                connection,
                str(target["live_sim_order_id"]),
                run_ai=run_ai,
                model_client=model_client,
                settings=settings,
            )
        )
    return results


def classify_live_sim_session_root_cause(
    evidence: Mapping[str, Any],
) -> tuple[LiveSimReviewRootCauseCategory, str, LiveSimReviewSeverity]:
    counts = _mapping(evidence.get("counts"))
    if int(counts.get("activity_count", 0)) == 0:
        return (
            LiveSimReviewRootCauseCategory.CONFIGURATION,
            "NO_LIVE_SIM_ACTIVITY: LIVE_SIM 주문/체결 활동 없음",
            LiveSimReviewSeverity.INFO,
        )
    if _mismatch_count(evidence) > 0:
        return (
            LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH,
            "RECONCILE_MISMATCH: reconcile snapshot에 불일치가 기록되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    rejections = _rows(evidence.get("rejections"))
    if len(rejections) >= max(1, int(counts.get("order_count", 0))):
        return _classify_rejection_rows(rejections)
    if _has_command_failed(evidence):
        return (
            LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
            "COMMAND_FAILED: Gateway command failure가 기록되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    orders = _rows(evidence.get("orders"))
    if any(_status(row) == "BROKER_REJECTED" for row in orders):
        return (
            LiveSimReviewRootCauseCategory.BROKER_REJECTION,
            "BROKER_REJECTED: broker rejection 상태가 주문에 기록되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if any(_status(row) in {"COMMAND_QUEUED", "COMMAND_DISPATCHED"} for row in orders):
        return (
            LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
            "QUEUED_NO_ACK: command가 queued/dispatched 상태에서 ack로 진행되지 않았습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if any(_status(row) == "PARTIALLY_FILLED" for row in orders):
        return (
            LiveSimReviewRootCauseCategory.PARTIAL_FILL,
            "PARTIAL_FILL: 일부 수량만 체결되었습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if any(_status(row) == "BROKER_ACKED" for row in orders) and not _rows(
        evidence.get("executions")
    ):
        return (
            LiveSimReviewRootCauseCategory.NO_FILL,
            "ACKED_NO_EXECUTION: broker ack 이후 체결 이벤트가 없습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if _has_local_only_reconcile(evidence):
        return (
            LiveSimReviewRootCauseCategory.LOCAL_ONLY_RECONCILE,
            "LOCAL_ONLY_RECONCILE: local-only reconcile snapshot이 기록되었습니다.",
            LiveSimReviewSeverity.LOW,
        )
    return (
        LiveSimReviewRootCauseCategory.EXECUTION_EVENT,
        "LIVE_SIM_REVIEW_COMPLETED: 주문/체결 evidence를 운영자 복기용으로 요약했습니다.",
        LiveSimReviewSeverity.INFO,
    )


def classify_live_sim_order_root_cause(
    evidence: Mapping[str, Any],
) -> tuple[LiveSimReviewRootCauseCategory, str, LiveSimReviewSeverity]:
    order = _mapping(evidence.get("order"))
    status = _status(order)
    command = _mapping(evidence.get("gateway_command"))
    command_status = _status(command)
    errors = _rows(evidence.get("errors"))
    command_events = _rows(evidence.get("command_events"))
    executions = _rows(evidence.get("executions"))
    reconcile_rows = _rows(evidence.get("reconcile_snapshots"))
    if _duplicate_idempotency(evidence):
        return (
            LiveSimReviewRootCauseCategory.DUPLICATE_IDEMPOTENCY,
            "DUPLICATE_IDEMPOTENCY: 동일 idempotency evidence가 감지되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if status == "RECONCILE_MISMATCH" or _any_mismatch(reconcile_rows):
        return (
            LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH,
            "RECONCILE_MISMATCH: 주문과 reconcile snapshot 사이 불일치가 있습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if status in {"FAILED", "EXPIRED"} or command_status in {"FAILED", "EXPIRED"}:
        return (
            LiveSimReviewRootCauseCategory.ORDER_COMMAND_QUEUE,
            "COMMAND_FAILURE: command queue 또는 transport failure로 주문이 실패했습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if any(str(row.get("event_type") or "").lower() == "command_failed" for row in command_events):
        return (
            LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
            "COMMAND_FAILED_EVENT: command_failed event가 기록되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if status == "BROKER_REJECTED" or _has_broker_rejection(errors):
        return (
            LiveSimReviewRootCauseCategory.BROKER_REJECTION,
            "BROKER_REJECTED: broker rejection 또는 rejection error가 기록되었습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if not command:
        return (
            LiveSimReviewRootCauseCategory.ORDER_COMMAND_QUEUE,
            "COMMAND_ROW_MISSING: 주문과 연결된 Gateway command row가 없습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if status in {"COMMAND_QUEUED", "COMMAND_DISPATCHED"} and not order.get("broker_acked_at"):
        return (
            LiveSimReviewRootCauseCategory.BROKER_ACK,
            "NO_BROKER_ACK: command 이후 broker ack가 아직 기록되지 않았습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if status == "PARTIALLY_FILLED":
        return (
            LiveSimReviewRootCauseCategory.PARTIAL_FILL,
            "PARTIAL_FILL: 주문 일부 수량만 체결되었습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if status == "BROKER_ACKED" and not executions:
        return (
            LiveSimReviewRootCauseCategory.NO_FILL,
            "NO_FILL: broker ack 이후 execution row가 없습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    if status == "FILLED" or executions:
        return (
            LiveSimReviewRootCauseCategory.EXECUTION_EVENT,
            "EXECUTION_EVENT: 체결 evidence가 기록되었습니다.",
            LiveSimReviewSeverity.INFO,
        )
    return (
        LiveSimReviewRootCauseCategory.UNKNOWN,
        "ORDER_REVIEW_UNKNOWN: 단일 원인이 우세하지 않습니다.",
        LiveSimReviewSeverity.LOW,
    )


def classify_live_sim_reconcile_root_cause(
    evidence: Mapping[str, Any],
) -> tuple[LiveSimReviewRootCauseCategory, str, LiveSimReviewSeverity]:
    snapshot = _mapping(evidence.get("snapshot"))
    mismatch_count = int(snapshot.get("mismatch_count") or 0)
    if mismatch_count > 0:
        return (
            LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH,
            f"RECONCILE_MISMATCH: mismatch_count={mismatch_count}",
            LiveSimReviewSeverity.HIGH,
        )
    if str(snapshot.get("status") or "").upper() in {
        "LOCAL_ONLY",
        "LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT",
    }:
        return (
            LiveSimReviewRootCauseCategory.LOCAL_ONLY_RECONCILE,
            "LOCAL_ONLY_RECONCILE: broker snapshot 없이 local snapshot만 기록되었습니다.",
            LiveSimReviewSeverity.LOW,
        )
    return (
        LiveSimReviewRootCauseCategory.UNKNOWN,
        "RECONCILE_REVIEW_COMPLETED: reconcile evidence를 요약했습니다.",
        LiveSimReviewSeverity.INFO,
    )


def classify_live_sim_incident_root_cause(
    evidence: Mapping[str, Any],
) -> tuple[LiveSimReviewRootCauseCategory, str, LiveSimReviewSeverity]:
    if _mismatch_count(evidence) > 0:
        return (
            LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH,
            "RECONCILE_MISMATCH: incident review에 reconcile mismatch가 포함됩니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if _rows(evidence.get("broker_rejections")):
        return (
            LiveSimReviewRootCauseCategory.BROKER_REJECTION,
            "BROKER_REJECTION: broker/live_sim rejection evidence가 있습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if _has_command_failed(evidence) or _rows(evidence.get("gateway_errors")):
        return (
            LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT,
            "GATEWAY_TRANSPORT: gateway error 또는 command_failed evidence가 있습니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if _rows(evidence.get("live_sim_errors")):
        return (
            LiveSimReviewRootCauseCategory.UNKNOWN,
            "LIVE_SIM_ERRORS_PRESENT: LIVE_SIM error rows가 기록되었습니다.",
            LiveSimReviewSeverity.MEDIUM,
        )
    return (
        LiveSimReviewRootCauseCategory.CONFIGURATION,
        "NO_INCIDENT_ACTIVITY: incident evidence가 없습니다.",
        LiveSimReviewSeverity.INFO,
    )


def _build_report(
    *,
    report_type: LiveSimReviewReportType,
    title: str,
    summary: str,
    root_cause_category: LiveSimReviewRootCauseCategory,
    root_cause: str,
    severity: LiveSimReviewSeverity,
    sections: Sequence[LiveSimReviewSection],
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    context_id: str,
    ai_result: AISidecarRunResult | None,
    run_ai: bool,
    warnings: Sequence[str],
    suggested_checks: Sequence[str],
    metadata: Mapping[str, Any],
    live_sim_intent_id: str | None = None,
    live_sim_order_id: str | None = None,
    live_sim_execution_id: str | None = None,
    reconcile_id: str | None = None,
) -> LiveSimReviewReport:
    report = LiveSimReviewReport(
        report_type=report_type,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        live_sim_intent_id=live_sim_intent_id,
        live_sim_order_id=live_sim_order_id,
        live_sim_execution_id=live_sim_execution_id,
        reconcile_id=reconcile_id,
        title=title,
        summary=summary,
        status=LiveSimReviewStatus.COMPLETED,
        severity=severity,
        root_cause_category=root_cause_category,
        root_cause=root_cause,
        context_id=context_id,
        deterministic_sections=sections,
        suggested_checks=_unique(suggested_checks),
        warnings=_unique(warnings),
        metadata={
            "deterministic_report": True,
            "ai_run_requested": run_ai,
            "report_is_operator_review_artifact": True,
            "strategy_risk_oms_input": False,
            "live_real_allowed": False,
            "order_action_allowed": False,
            "gateway_command_allowed": False,
            **dict(metadata),
        },
    )
    if ai_result is None:
        return report
    return attach_ai_insight_to_live_sim_review(report, ai_result)


def attach_ai_insight_to_live_sim_review(
    report: LiveSimReviewReport,
    ai_result: AISidecarRunResult,
) -> LiveSimReviewReport:
    if not ai_result.ok or not ai_result.insight_id:
        return replace(
            report,
            status=_report_status_from_ai_result(ai_result),
            ai_request_id=ai_result.request_id,
        )
    return replace(
        report,
        status=LiveSimReviewStatus.COMPLETED,
        ai_request_id=ai_result.request_id,
        ai_insight_id=ai_result.insight_id,
    )


def _maybe_run_ai(
    connection: sqlite3.Connection,
    task_type: AISidecarTaskType,
    *,
    run_ai: bool,
    model_client: AISidecarModelClient | None,
    settings: Settings,
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    context_id: str,
) -> AISidecarRunResult | None:
    if not run_ai:
        return None
    result = run_ai_sidecar_task(
        connection,
        task_type,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        context_id=context_id,
        persist_context=True,
        model_client=model_client,
        settings=settings,
    )
    if result.ok and result.insight_id:
        insight = get_ai_insight(connection, result.insight_id)
        if insight:
            return result
    return result


def _report_status_from_ai_result(result: AISidecarRunResult) -> LiveSimReviewStatus:
    status = result.status.upper()
    if status == "AI_DISABLED":
        return LiveSimReviewStatus.AI_DISABLED
    if status in {"API_KEY_MISSING", "CLIENT_UNAVAILABLE", "MODEL_ERROR", "TIMEOUT"}:
        return LiveSimReviewStatus.AI_UNAVAILABLE
    if status == "AI_OUTPUT_INVALID":
        return LiveSimReviewStatus.AI_OUTPUT_INVALID
    if status == "POLICY_REJECTED":
        return LiveSimReviewStatus.POLICY_REJECTED
    return LiveSimReviewStatus.PARTIAL


def _session_evidence(
    connection: sqlite3.Connection,
    trade_date: str,
    settings: Settings,
) -> dict[str, Any]:
    intents = list_live_sim_intents(connection, trade_date=trade_date, limit=200)
    orders = list_live_sim_orders(connection, trade_date=trade_date, limit=200)
    executions = _executions_for_orders(
        list_live_sim_executions(connection, limit=200),
        orders=orders,
        intents=intents,
    )
    rejections = [
        row
        for row in list_live_sim_rejections(connection, limit=200)
        if row.get("trade_date") == trade_date
    ]
    reconcile_rows = [
        row
        for row in list_live_sim_reconcile_snapshots(connection, limit=50)
        if row.get("trade_date") == trade_date
    ]
    command_ids = _command_ids(intents, orders)
    commands = _gateway_commands(connection, command_ids=command_ids, include_live_sim=True)
    command_events = _gateway_command_events(connection, command_ids)
    gateway_events = _gateway_events(connection, command_ids=command_ids)
    errors = [
        row
        for row in list_live_sim_errors(connection, limit=200)
        if _row_related_to_trade_date(row, trade_date, intents=intents, orders=orders)
    ]
    counts = {
        "intent_count": len(intents),
        "order_count": len(orders),
        "execution_count": len(executions),
        "rejection_count": len(rejections),
        "reconcile_count": len(reconcile_rows),
        "error_count": len(errors),
        "activity_count": len(intents) + len(orders) + len(executions) + len(rejections),
    }
    return _safe_evidence(
        {
            "trade_date": trade_date,
            "status": get_live_sim_status(connection, settings=settings),
            "safety_gate": check_live_sim_safety_gate(connection, settings).to_dict(),
            "counts": counts,
            "intents": intents,
            "orders": orders,
            "executions": executions,
            "rejections": rejections,
            "reconcile_snapshots": reconcile_rows,
            "gateway_commands": commands,
            "command_events": command_events,
            "gateway_events": gateway_events,
            "errors": errors,
        }
    )


def _order_evidence(connection: sqlite3.Connection, order: Mapping[str, Any]) -> dict[str, Any]:
    intent = get_live_sim_intent(connection, str(order.get("live_sim_intent_id") or ""))
    command_id = order.get("gateway_command_id")
    command = _gateway_command(connection, command_id) if command_id else None
    command_events = _gateway_command_events(
        connection,
        [str(command_id)] if command_id else [],
    )
    gateway_events = _gateway_events(
        connection,
        command_ids=[str(command_id)] if command_id else [],
        idempotency_key=order.get("idempotency_key"),
    )
    executions = _executions_for_order(connection, order)
    rejections = _rejections_for_order(connection, order, intent)
    errors = _errors_for_order(connection, order)
    reconcile_rows = [
        row
        for row in list_live_sim_reconcile_snapshots(connection, limit=50)
        if row.get("trade_date") == order.get("trade_date")
        and (row.get("code") in {None, order.get("code")} or not row.get("code"))
    ]
    return _safe_evidence(
        {
            "intent": intent or {},
            "order": dict(order),
            "related_candidate": _related_candidate(connection, intent),
            "related_strategy": _related_strategy(connection, intent),
            "related_risk": _related_risk(connection, intent),
            "dry_run_evidence": _dry_run_evidence(connection, intent),
            "gateway_command": command or {},
            "command_events": command_events,
            "gateway_events": gateway_events,
            "broker_ack": _broker_ack(order, command_events),
            "executions": executions,
            "reconcile_snapshots": reconcile_rows,
            "rejections": rejections,
            "errors": errors,
            "idempotency": _idempotency_evidence(connection, order),
        }
    )


def _reconcile_evidence(
    connection: sqlite3.Connection,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    trade_date = snapshot.get("trade_date")
    code = snapshot.get("code")
    orders = list_live_sim_orders(
        connection,
        trade_date=str(trade_date) if trade_date else None,
        code=str(code) if code else None,
        limit=100,
        open_only=True,
    )
    executions = _executions_for_orders(
        list_live_sim_executions(connection, code=str(code) if code else None, limit=100),
        orders=orders,
        intents=[],
    )
    command_ids = _command_ids([], orders)
    return _safe_evidence(
        {
            "snapshot": dict(snapshot),
            "local_open_orders": orders,
            "local_executions": executions,
            "gateway_commands": _gateway_commands(connection, command_ids=command_ids),
            "mismatches": _mapping(snapshot.get("snapshot_json")).get("mismatches", []),
            "broker_snapshot_available": _broker_snapshot_available(snapshot),
            "broker_snapshot_unavailable_note": (
                "현재 reconcile은 local-only snapshot으로 broker 원장 조회를 수행하지 않습니다."
            ),
        }
    )


def _incident_evidence(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
) -> dict[str, Any]:
    live_sim_errors = list_live_sim_errors(connection, limit=100)
    rejections = list_live_sim_rejections(connection, limit=100)
    reconcile_rows = [
        row
        for row in list_live_sim_reconcile_snapshots(connection, limit=100)
        if int(row.get("mismatch_count") or 0) > 0
    ]
    gateway_errors = _gateway_problem_events(connection)
    command_failed = _gateway_failed_commands(connection)
    if trade_date is not None:
        rejections = [row for row in rejections if row.get("trade_date") == trade_date]
        reconcile_rows = [row for row in reconcile_rows if row.get("trade_date") == trade_date]
    return _safe_evidence(
        {
            "trade_date": trade_date,
            "live_sim_errors": live_sim_errors,
            "gateway_errors": gateway_errors,
            "command_failed": command_failed,
            "broker_rejections": rejections,
            "reconcile_mismatches": reconcile_rows,
            "config_safety_warnings": [],
        }
    )


def _session_sections(evidence: Mapping[str, Any]) -> list[LiveSimReviewSection]:
    status = _mapping(evidence.get("status"))
    safety_gate = _mapping(evidence.get("safety_gate"))
    counts = _mapping(evidence.get("counts"))
    return [
        _section(
            "live sim safety/config posture",
            status="OK" if safety_gate.get("passed") else "WARN",
            severity=(
                LiveSimReviewSeverity.INFO
                if safety_gate.get("passed")
                else LiveSimReviewSeverity.HIGH
            ),
            summary="LIVE_SIM safety gate와 config posture를 읽기 전용으로 요약했습니다.",
            reason_codes=_strings(safety_gate.get("reason_codes")),
            evidence_json={"status": status, "safety_gate": safety_gate},
            source_refs=["live_sim_status", "live_sim_safety_gate"],
        ),
        _section(
            "live_sim_intents summary",
            status="OK" if counts.get("intent_count") else "INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary=f"intent {counts.get('intent_count', 0)}건을 요약했습니다.",
            evidence_json={"items": evidence.get("intents", [])},
            source_refs=["live_sim_intents"],
        ),
        _section(
            "live_sim_orders summary",
            status="OK" if counts.get("order_count") else "INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary=f"order {counts.get('order_count', 0)}건을 요약했습니다.",
            evidence_json={"items": evidence.get("orders", [])},
            source_refs=["live_sim_orders"],
        ),
        _section(
            "live_sim_executions summary",
            status="OK" if counts.get("execution_count") else "INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary=f"execution {counts.get('execution_count', 0)}건을 요약했습니다.",
            evidence_json={"items": evidence.get("executions", [])},
            source_refs=["live_sim_executions"],
        ),
        _section(
            "live_sim_rejections summary",
            status="WARN" if counts.get("rejection_count") else "OK",
            severity=(
                LiveSimReviewSeverity.MEDIUM
                if counts.get("rejection_count")
                else LiveSimReviewSeverity.INFO
            ),
            summary=f"rejection {counts.get('rejection_count', 0)}건을 요약했습니다.",
            reason_codes=_top_reason_codes(_rows(evidence.get("rejections"))),
            evidence_json={"items": evidence.get("rejections", [])},
            source_refs=["live_sim_rejections"],
        ),
        _section(
            "live_sim_reconcile_snapshots summary",
            status="WARN" if _mismatch_count(evidence) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _mismatch_count(evidence)
                else LiveSimReviewSeverity.INFO
            ),
            summary=f"reconcile snapshot {counts.get('reconcile_count', 0)}건을 요약했습니다.",
            reason_codes=["RECONCILE_MISMATCH"] if _mismatch_count(evidence) else [],
            evidence_json={"items": evidence.get("reconcile_snapshots", [])},
            source_refs=["live_sim_reconcile_snapshots"],
        ),
        _section(
            "gateway command status related to live_sim",
            status="WARN" if _has_command_failed(evidence) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _has_command_failed(evidence)
                else LiveSimReviewSeverity.INFO
            ),
            summary="LIVE_SIM 관련 gateway command와 event 상태를 요약했습니다.",
            reason_codes=["COMMAND_FAILED"] if _has_command_failed(evidence) else [],
            evidence_json={
                "commands": evidence.get("gateway_commands", []),
                "command_events": evidence.get("command_events", []),
                "gateway_events": evidence.get("gateway_events", []),
            },
            source_refs=["gateway_commands", "gateway_command_events", "gateway_events"],
        ),
        _section(
            "error summary",
            status="WARN" if counts.get("error_count") else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if counts.get("error_count")
                else LiveSimReviewSeverity.INFO
            ),
            summary=f"LIVE_SIM error {counts.get('error_count', 0)}건을 요약했습니다.",
            evidence_json={"items": evidence.get("errors", [])},
            source_refs=["live_sim_errors"],
        ),
        _section(
            "dashboard safety reminders",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="Dashboard는 LIVE_SIM review를 표시만 하며 실행 버튼을 제공하지 않습니다.",
            reason_codes=["REVIEW_ONLY", "ORDER_ACTION_DISABLED"],
            evidence_json={
                "review_only": True,
                "order_action_allowed": False,
                "live_real_allowed": False,
                "ai_artifacts_are_order_input": False,
            },
            source_refs=["dashboard"],
        ),
    ]


def _order_sections(evidence: Mapping[str, Any]) -> list[LiveSimReviewSection]:
    order = _mapping(evidence.get("order"))
    command = _mapping(evidence.get("gateway_command"))
    executions = _rows(evidence.get("executions"))
    rejections = _rows(evidence.get("rejections"))
    errors = _rows(evidence.get("errors"))
    return [
        _section(
            "intent detail",
            status="OK" if evidence.get("intent") else "WARN",
            severity=LiveSimReviewSeverity.INFO,
            summary="주문과 연결된 intent evidence입니다.",
            evidence_json=evidence.get("intent", {}),
            source_refs=["live_sim_intents"],
        ),
        _section(
            "order detail",
            status=_status(order) or "UNKNOWN",
            severity=LiveSimReviewSeverity.INFO,
            summary="LIVE_SIM order row를 읽기 전용으로 요약했습니다.",
            evidence_json=order,
            source_refs=["live_sim_orders"],
        ),
        _section(
            "related candidate/strategy/risk evidence",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="주문 생성 전 관찰 evidence를 연결했습니다.",
            evidence_json={
                "candidate": evidence.get("related_candidate", {}),
                "strategy": evidence.get("related_strategy", {}),
                "risk": evidence.get("related_risk", {}),
            },
            source_refs=["candidates", "strategy_observations_latest", "risk_observations_latest"],
        ),
        _section(
            "dry_run prerequisite evidence",
            status="OK" if evidence.get("dry_run_evidence") else "INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="LIVE_SIM 전 dry-run prerequisite evidence입니다.",
            evidence_json=evidence.get("dry_run_evidence", {}),
            source_refs=["dry_run_intents", "dry_run_orders"],
        ),
        _section(
            "gateway command row",
            status=_status(command) or "MISSING",
            severity=LiveSimReviewSeverity.MEDIUM if not command else LiveSimReviewSeverity.INFO,
            summary="주문과 연결된 gateway command row입니다.",
            evidence_json=command,
            source_refs=["gateway_commands"],
        ),
        _section(
            "command event rows",
            status="WARN" if _has_command_failed(evidence) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _has_command_failed(evidence)
                else LiveSimReviewSeverity.INFO
            ),
            summary="command_ack/command_failed event evidence입니다.",
            evidence_json={
                "command_events": evidence.get("command_events", []),
                "gateway_events": evidence.get("gateway_events", []),
            },
            source_refs=["gateway_command_events", "gateway_events"],
        ),
        _section(
            "broker ack/reject data",
            status=(
                "WARN" if order.get("broker_result_code") or order.get("broker_message") else "INFO"
            ),
            severity=LiveSimReviewSeverity.MEDIUM,
            summary="broker ack/reject 관련 필드를 요약했습니다.",
            evidence_json=evidence.get("broker_ack", {}),
            source_refs=["live_sim_orders", "gateway_events"],
        ),
        _section(
            "execution rows",
            status="OK" if executions else "INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary=f"execution {len(executions)}건이 연결되었습니다.",
            evidence_json={"items": executions},
            source_refs=["live_sim_executions"],
        ),
        _section(
            "reconcile rows",
            status="WARN" if _any_mismatch(_rows(evidence.get("reconcile_snapshots"))) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _any_mismatch(_rows(evidence.get("reconcile_snapshots")))
                else LiveSimReviewSeverity.INFO
            ),
            summary="주문과 같은 trade_date/code의 reconcile evidence입니다.",
            evidence_json={"items": evidence.get("reconcile_snapshots", [])},
            source_refs=["live_sim_reconcile_snapshots"],
        ),
        _section(
            "errors/rejections",
            status="WARN" if errors or rejections else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if errors
                else LiveSimReviewSeverity.MEDIUM if rejections else LiveSimReviewSeverity.INFO
            ),
            summary=f"error {len(errors)}건, rejection {len(rejections)}건을 연결했습니다.",
            reason_codes=_top_reason_codes(rejections),
            evidence_json={"errors": errors, "rejections": rejections},
            source_refs=["live_sim_errors", "live_sim_rejections"],
        ),
        _section(
            "idempotency evidence",
            status="WARN" if _duplicate_idempotency(evidence) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _duplicate_idempotency(evidence)
                else LiveSimReviewSeverity.INFO
            ),
            summary="idempotency key와 duplicate evidence를 확인했습니다.",
            evidence_json=evidence.get("idempotency", {}),
            source_refs=["live_sim_orders", "live_sim_intents", "gateway_command_dedupe_keys"],
        ),
    ]


def _reconcile_sections(evidence: Mapping[str, Any]) -> list[LiveSimReviewSection]:
    snapshot = _mapping(evidence.get("snapshot"))
    mismatch_count = int(snapshot.get("mismatch_count") or 0)
    return [
        _section(
            "reconcile snapshot detail",
            status=str(snapshot.get("status") or "UNKNOWN"),
            severity=LiveSimReviewSeverity.HIGH if mismatch_count else LiveSimReviewSeverity.INFO,
            summary="reconcile snapshot row를 요약했습니다.",
            reason_codes=["RECONCILE_MISMATCH"] if mismatch_count else [],
            evidence_json=snapshot,
            source_refs=["live_sim_reconcile_snapshots"],
        ),
        _section(
            "local open orders",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="local open LIVE_SIM orders입니다.",
            evidence_json={"items": evidence.get("local_open_orders", [])},
            source_refs=["live_sim_orders"],
        ),
        _section(
            "local positions/executions",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="local execution 기반 position evidence입니다.",
            evidence_json={"items": evidence.get("local_executions", [])},
            source_refs=["live_sim_executions"],
        ),
        _section(
            "gateway command statuses",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="reconcile 대상과 연결된 gateway command status입니다.",
            evidence_json={"items": evidence.get("gateway_commands", [])},
            source_refs=["gateway_commands"],
        ),
        _section(
            "mismatches",
            status="WARN" if mismatch_count else "OK",
            severity=LiveSimReviewSeverity.HIGH if mismatch_count else LiveSimReviewSeverity.INFO,
            summary=f"mismatch_count={mismatch_count}",
            evidence_json={"items": evidence.get("mismatches", [])},
            source_refs=["live_sim_reconcile_snapshots"],
        ),
        _section(
            "broker snapshot unavailable note",
            status="INFO",
            severity=LiveSimReviewSeverity.LOW,
            summary=str(evidence.get("broker_snapshot_unavailable_note") or ""),
            evidence_json={"broker_snapshot_available": evidence.get("broker_snapshot_available")},
            source_refs=["live_sim_reconcile_snapshots"],
        ),
    ]


def _incident_sections(evidence: Mapping[str, Any]) -> list[LiveSimReviewSection]:
    return [
        _section(
            "live_sim errors",
            status="WARN" if _rows(evidence.get("live_sim_errors")) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _rows(evidence.get("live_sim_errors"))
                else LiveSimReviewSeverity.INFO
            ),
            summary="LIVE_SIM error rows를 요약했습니다.",
            evidence_json={"items": evidence.get("live_sim_errors", [])},
            source_refs=["live_sim_errors"],
        ),
        _section(
            "gateway errors",
            status="WARN" if _rows(evidence.get("gateway_errors")) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _rows(evidence.get("gateway_errors"))
                else LiveSimReviewSeverity.INFO
            ),
            summary="gateway error/problem event를 요약했습니다.",
            evidence_json={"items": evidence.get("gateway_errors", [])},
            source_refs=["gateway_events"],
        ),
        _section(
            "command_failed",
            status="WARN" if _rows(evidence.get("command_failed")) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _rows(evidence.get("command_failed"))
                else LiveSimReviewSeverity.INFO
            ),
            summary="failed gateway commands를 요약했습니다.",
            evidence_json={"items": evidence.get("command_failed", [])},
            source_refs=["gateway_commands"],
        ),
        _section(
            "broker rejects",
            status="WARN" if _rows(evidence.get("broker_rejections")) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _rows(evidence.get("broker_rejections"))
                else LiveSimReviewSeverity.INFO
            ),
            summary="broker/live_sim rejection evidence를 요약했습니다.",
            reason_codes=_top_reason_codes(_rows(evidence.get("broker_rejections"))),
            evidence_json={"items": evidence.get("broker_rejections", [])},
            source_refs=["live_sim_rejections"],
        ),
        _section(
            "reconcile mismatches",
            status="WARN" if _rows(evidence.get("reconcile_mismatches")) else "OK",
            severity=(
                LiveSimReviewSeverity.HIGH
                if _rows(evidence.get("reconcile_mismatches"))
                else LiveSimReviewSeverity.INFO
            ),
            summary="reconcile mismatch snapshot을 요약했습니다.",
            evidence_json={"items": evidence.get("reconcile_mismatches", [])},
            source_refs=["live_sim_reconcile_snapshots"],
        ),
        _section(
            "config/safety warnings",
            status="INFO",
            severity=LiveSimReviewSeverity.INFO,
            summary="LIVE_REAL boundary와 review-only 정책을 확인했습니다.",
            reason_codes=["LIVE_REAL_DISABLED", "REVIEW_ONLY"],
            evidence_json={
                "live_real_allowed": False,
                "order_action_allowed": False,
                "gateway_command_allowed": False,
            },
            source_refs=["settings", "dashboard"],
        ),
    ]


def _section(
    section_name: str,
    *,
    status: str,
    severity: LiveSimReviewSeverity | str,
    summary: str,
    reason_codes: Sequence[str] = (),
    evidence_json: Mapping[str, Any] | None = None,
    source_refs: Sequence[str] = (),
) -> LiveSimReviewSection:
    return LiveSimReviewSection(
        section_name=section_name,
        status=status,
        severity=severity,
        summary=summary,
        reason_codes=reason_codes,
        evidence_json=redact_context(evidence_json or {}),
        source_refs=source_refs,
    )


def _session_ai_task(evidence: Mapping[str, Any]) -> AISidecarTaskType:
    if (
        _rows(evidence.get("errors"))
        or _rows(evidence.get("rejections"))
        or _mismatch_count(evidence)
        or _has_command_failed(evidence)
    ):
        return AISidecarTaskType.OPS_INCIDENT_SUMMARY
    return AISidecarTaskType.TRADE_REVIEW


def _classify_rejection_rows(
    rejections: Sequence[Mapping[str, Any]],
) -> tuple[LiveSimReviewRootCauseCategory, str, LiveSimReviewSeverity]:
    reasons = " ".join(reason for row in rejections for reason in _strings(row.get("reason_codes")))
    if any(token in reasons for token in ("ACCOUNT", "SERVER", "BROKER_ENV")):
        return (
            LiveSimReviewRootCauseCategory.ACCOUNT_GUARD,
            "ACCOUNT_GUARD: simulation account/server guard rejection이 우세합니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if any(token in reasons for token in ("LIMIT", "NOTIONAL", "ACTIVE_ORDER", "DAILY")):
        return (
            LiveSimReviewRootCauseCategory.LIMIT_GUARD,
            "LIMIT_GUARD: limit guard rejection이 우세합니다.",
            LiveSimReviewSeverity.HIGH,
        )
    if any(token in reasons for token in ("LIVE_SIM_DISABLED", "GATEWAY", "KILL_SWITCH")):
        return (
            LiveSimReviewRootCauseCategory.SAFETY_GATE,
            "SAFETY_GATE: safety gate rejection이 우세합니다.",
            LiveSimReviewSeverity.HIGH,
        )
    return (
        LiveSimReviewRootCauseCategory.ELIGIBILITY,
        "ELIGIBILITY: LIVE_SIM eligibility rejection이 기록되었습니다.",
        LiveSimReviewSeverity.MEDIUM,
    )


def _session_summary(
    category: LiveSimReviewRootCauseCategory,
    evidence: Mapping[str, Any],
) -> str:
    counts = _mapping(evidence.get("counts"))
    if category is LiveSimReviewRootCauseCategory.CONFIGURATION:
        return "LIVE_SIM 주문/체결 활동 없음."
    return (
        f"LIVE_SIM session review classified {category.value}: "
        f"orders={counts.get('order_count', 0)}, executions={counts.get('execution_count', 0)}, "
        f"rejections={counts.get('rejection_count', 0)}."
    )


def _order_summary(
    category: LiveSimReviewRootCauseCategory,
    order: Mapping[str, Any],
) -> str:
    return (
        f"LIVE_SIM order {order.get('live_sim_order_id')} review classified "
        f"{category.value} with status={order.get('status')}."
    )


def _reconcile_summary(
    category: LiveSimReviewRootCauseCategory,
    snapshot: Mapping[str, Any],
) -> str:
    return (
        f"LIVE_SIM reconcile {snapshot.get('reconcile_id')} review classified "
        f"{category.value}; mismatch_count={snapshot.get('mismatch_count', 0)}."
    )


def _incident_summary(
    category: LiveSimReviewRootCauseCategory,
    evidence: Mapping[str, Any],
) -> str:
    return (
        f"LIVE_SIM incident review classified {category.value}; "
        f"errors={len(_rows(evidence.get('live_sim_errors')))}, "
        f"gateway_errors={len(_rows(evidence.get('gateway_errors')))}."
    )


def _suggest_session_checks(category: LiveSimReviewRootCauseCategory) -> list[str]:
    checks = [
        "LIVE_SIM safety gate와 LIVE_REAL disabled 상태를 확인하세요.",
        "Gateway command ack/failure와 execution event 흐름을 대조하세요.",
        "Review report가 Strategy/Risk/OMS 입력으로 사용되지 않는지 확인하세요.",
    ]
    if category is LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH:
        checks.append("local reconcile snapshot의 mismatch 항목을 주문/체결 row와 대조하세요.")
    if category in {
        LiveSimReviewRootCauseCategory.SAFETY_GATE,
        LiveSimReviewRootCauseCategory.ELIGIBILITY,
    }:
        checks.append("rejection reason_codes와 eligibility evidence를 확인하세요.")
    return checks


def _suggest_order_checks(category: LiveSimReviewRootCauseCategory) -> list[str]:
    checks = [
        "intent, order, gateway command, command event, execution row를 시간순으로 비교하세요.",
        "idempotency_key 중복 또는 dedupe rejection 여부를 확인하세요.",
    ]
    if category is LiveSimReviewRootCauseCategory.NO_FILL:
        checks.append(
            "broker ack 이후 execution_event 유입 여부와 reconcile snapshot을 확인하세요."
        )
    if category is LiveSimReviewRootCauseCategory.BROKER_REJECTION:
        checks.append("broker_result_code, broker_message, gateway event payload를 확인하세요.")
    return checks


def _suggest_reconcile_checks(category: LiveSimReviewRootCauseCategory) -> list[str]:
    checks = [
        "local open order와 execution row를 reconcile snapshot과 비교하세요.",
        "broker snapshot unavailable이면 local-only 한계를 운영 메모에 남기세요.",
    ]
    if category is LiveSimReviewRootCauseCategory.RECONCILE_MISMATCH:
        checks.append("mismatch 항목을 주문 상태, 체결 수량, remaining_quantity와 대조하세요.")
    return checks


def _suggest_incident_checks(category: LiveSimReviewRootCauseCategory) -> list[str]:
    checks = [
        (
            "live_sim_errors, gateway errors, command_failed, rejection rows를 "
            "시간순으로 정렬해 보세요."
        ),
        "AI insight가 있더라도 자동 복구나 주문 액션으로 연결하지 마세요.",
    ]
    if category is LiveSimReviewRootCauseCategory.GATEWAY_TRANSPORT:
        checks.append(
            "gateway heartbeat, command queue status, failed command last_error를 확인하세요."
        )
    return checks


def _order_review_targets(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    status: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date:
        clauses.append("trade_date = ?")
        params.append(trade_date)
    if status:
        clauses.append("status = ?")
        params.append(status.upper())
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    rows = connection.execute(
        f"""
        SELECT live_sim_order_id
        FROM live_sim_orders
        {where_sql}
        ORDER BY created_at DESC, live_sim_order_id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _get_reconcile_snapshot(
    connection: sqlite3.Connection,
    reconcile_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_reconcile_snapshots
        WHERE reconcile_id = ?
        """,
        (reconcile_id,),
    ).fetchone()
    if row is None:
        return None
    item = _row_to_dict(row)
    item["snapshot_json"] = _json_object(item.pop("snapshot_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = False
    return item


def _gateway_command(connection: sqlite3.Connection, command_id: object) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM gateway_commands
        WHERE command_id = ?
        """,
        (str(command_id),),
    ).fetchone()
    if row is None:
        return None
    item = _row_to_dict(row)
    item["payload"] = _json_object(item.pop("payload_json"))
    return item


def _gateway_commands(
    connection: sqlite3.Connection,
    *,
    command_ids: Sequence[str],
    include_live_sim: bool = False,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if command_ids:
        clauses.append(f"command_id IN ({', '.join('?' for _ in command_ids)})")
        params.extend(command_ids)
    if include_live_sim:
        clauses.append("(LOWER(source) = 'live_sim' OR LOWER(command_type) = 'send_order')")
    if not clauses:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_commands
        WHERE {" OR ".join(clauses)}
        ORDER BY created_at DESC, command_id DESC
        LIMIT 200
        """,
        tuple(params),
    ).fetchall()
    commands = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = _json_object(item.pop("payload_json"))
        commands.append(item)
    return commands


def _gateway_command_events(
    connection: sqlite3.Connection,
    command_ids: Sequence[str],
) -> list[dict[str, Any]]:
    ids = [str(command_id) for command_id in command_ids if str(command_id).strip()]
    if not ids:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_command_events
        WHERE command_id IN ({', '.join('?' for _ in ids)})
        ORDER BY id ASC
        """,
        tuple(ids),
    ).fetchall()
    events = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = _json_object(item.pop("payload_json"))
        events.append(item)
    return events


def _gateway_events(
    connection: sqlite3.Connection,
    *,
    command_ids: Sequence[str],
    idempotency_key: object | None = None,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    ids = [str(command_id) for command_id in command_ids if str(command_id).strip()]
    if ids:
        clauses.append(f"command_id IN ({', '.join('?' for _ in ids)})")
        params.extend(ids)
    if idempotency_key:
        clauses.append("idempotency_key = ?")
        params.append(str(idempotency_key))
    if not clauses:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_events
        WHERE {" OR ".join(clauses)}
        ORDER BY received_at ASC, event_id ASC
        LIMIT 200
        """,
        tuple(params),
    ).fetchall()
    events = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = _json_object(item.pop("payload_json"))
        events.append(item)
    return events


def _gateway_problem_events(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM gateway_events
        WHERE status NOT IN ('ACCEPTED')
            OR error_message IS NOT NULL
        ORDER BY received_at DESC, event_id DESC
        LIMIT 100
        """
    ).fetchall()
    events = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = _json_object(item.pop("payload_json"))
        events.append(item)
    return events


def _gateway_failed_commands(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM gateway_commands
        WHERE status IN ('FAILED', 'REJECTED', 'EXPIRED')
        ORDER BY COALESCE(completed_at, created_at) DESC, command_id DESC
        LIMIT 100
        """
    ).fetchall()
    commands = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = _json_object(item.pop("payload_json"))
        commands.append(item)
    return commands


def _executions_for_order(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_executions
        WHERE live_sim_order_id = ?
            OR broker_order_no = ?
        ORDER BY executed_at ASC, live_sim_execution_id ASC
        """,
        (order.get("live_sim_order_id"), order.get("broker_order_no")),
    ).fetchall()
    return [_execution_row_to_dict(row) for row in rows]


def _executions_for_orders(
    executions: Sequence[Mapping[str, Any]],
    *,
    orders: Sequence[Mapping[str, Any]],
    intents: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    order_ids = {str(row.get("live_sim_order_id")) for row in orders}
    intent_ids = {str(row.get("live_sim_intent_id")) for row in intents}
    if not order_ids and not intent_ids:
        return list(executions)
    return [
        dict(row)
        for row in executions
        if str(row.get("live_sim_order_id")) in order_ids
        or str(row.get("live_sim_intent_id")) in intent_ids
    ]


def _rejections_for_order(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    intent: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    candidate_id = (intent or {}).get("candidate_instance_id")
    rows = list_live_sim_rejections(connection, code=order.get("code"), limit=100)
    if candidate_id:
        rows = [
            row
            for row in rows
            if row.get("candidate_instance_id") == candidate_id
            or row.get("trade_date") == order.get("trade_date")
        ]
    return rows


def _errors_for_order(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = list_live_sim_errors(connection, limit=100)
    return [
        row
        for row in rows
        if row.get("live_sim_order_id") == order.get("live_sim_order_id")
        or row.get("live_sim_intent_id") == order.get("live_sim_intent_id")
        or row.get("code") == order.get("code")
    ]


def _related_candidate(
    connection: sqlite3.Connection,
    intent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidate_id = (intent or {}).get("candidate_instance_id")
    if not candidate_id:
        return {}
    row = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    if row is None:
        return {}
    item = _row_to_dict(row)
    if "reason_codes_json" in item:
        item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    return item


def _related_strategy(
    connection: sqlite3.Connection,
    intent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    strategy_id = (intent or {}).get("strategy_observation_id")
    if not strategy_id:
        return {}
    row = connection.execute(
        "SELECT * FROM strategy_observations_latest WHERE strategy_observation_id = ?",
        (strategy_id,),
    ).fetchone()
    return {} if row is None else _row_with_reasons(row)


def _related_risk(
    connection: sqlite3.Connection,
    intent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    risk_id = (intent or {}).get("risk_observation_id")
    if not risk_id:
        return {}
    row = connection.execute(
        "SELECT * FROM risk_observations_latest WHERE risk_observation_id = ?",
        (risk_id,),
    ).fetchone()
    return {} if row is None else _row_with_reasons(row)


def _dry_run_evidence(
    connection: sqlite3.Connection,
    intent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not intent:
        return {}
    dry_run_intent_id = intent.get("dry_run_intent_id")
    dry_run_order_id = intent.get("dry_run_order_id")
    evidence: dict[str, Any] = {}
    if dry_run_intent_id:
        row = connection.execute(
            "SELECT * FROM dry_run_intents WHERE dry_run_intent_id = ?",
            (dry_run_intent_id,),
        ).fetchone()
        if row is not None:
            evidence["intent"] = _row_with_reasons(row)
    if dry_run_order_id:
        row = connection.execute(
            "SELECT * FROM dry_run_orders WHERE dry_run_order_id = ?",
            (dry_run_order_id,),
        ).fetchone()
        if row is not None:
            evidence["order"] = _row_with_reasons(row)
    return evidence


def _idempotency_evidence(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
) -> dict[str, Any]:
    key = order.get("idempotency_key")
    if not key:
        return {"idempotency_key": None, "duplicate": False}
    order_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_orders WHERE idempotency_key = ?",
        (key,),
    ).fetchone()["count"]
    intent_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents WHERE idempotency_key = ?",
        (key,),
    ).fetchone()["count"]
    dedupe = connection.execute(
        "SELECT * FROM gateway_command_dedupe_keys WHERE idempotency_key = ?",
        (key,),
    ).fetchone()
    return {
        "idempotency_key": key,
        "live_sim_order_count": int(order_count),
        "live_sim_intent_count": int(intent_count),
        "dedupe_key": None if dedupe is None else _row_to_dict(dedupe),
        "duplicate": int(order_count) > 1 or int(intent_count) > 1,
    }


def _broker_ack(
    order: Mapping[str, Any],
    command_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "broker_order_no": order.get("broker_order_no"),
        "broker_result_code": order.get("broker_result_code"),
        "broker_message": order.get("broker_message"),
        "broker_acked_at": order.get("broker_acked_at"),
        "command_ack_events": [
            row for row in command_events if str(row.get("event_type")).lower() == "command_ack"
        ],
    }


def _command_ids(
    intents: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
) -> list[str]:
    values = [
        *(row.get("gateway_command_id") for row in intents),
        *(row.get("gateway_command_id") for row in orders),
    ]
    return _unique([str(value) for value in values if value])


def _row_related_to_trade_date(
    row: Mapping[str, Any],
    trade_date: str,
    *,
    intents: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
) -> bool:
    intent_ids = {item.get("live_sim_intent_id") for item in intents}
    order_ids = {item.get("live_sim_order_id") for item in orders}
    if row.get("live_sim_intent_id") in intent_ids or row.get("live_sim_order_id") in order_ids:
        return True
    return str(row.get("created_at") or "").startswith(trade_date)


def _safe_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    return redact_context(value)


def _row_with_reasons(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    for key in ("reason_codes_json", "evidence_json", "metadata_json", "payload_json"):
        if key not in item:
            continue
        value = item.pop(key)
        if key == "reason_codes_json":
            item["reason_codes"] = _json_array(value)
        else:
            item[key.removesuffix("_json")] = _json_object(value)
    return item


def _execution_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["raw_event_json"] = _json_object(item.pop("raw_event_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = False
    return item


def _broker_snapshot_available(snapshot: Mapping[str, Any]) -> bool:
    return (
        int(snapshot.get("broker_open_order_count") or 0) > 0
        or int(snapshot.get("broker_position_count") or 0) > 0
    )


def _mismatch_count(evidence: Mapping[str, Any]) -> int:
    total = 0
    for row in _rows(evidence.get("reconcile_snapshots")) + _rows(
        evidence.get("reconcile_mismatches")
    ):
        total += int(row.get("mismatch_count") or 0)
    snapshot = _mapping(evidence.get("snapshot"))
    total += int(snapshot.get("mismatch_count") or 0)
    return total


def _has_local_only_reconcile(evidence: Mapping[str, Any]) -> bool:
    rows = _rows(evidence.get("reconcile_snapshots"))
    return any(
        str(row.get("status") or "").upper()
        in {"LOCAL_ONLY", "LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT"}
        for row in rows
    )


def _has_command_failed(evidence: Mapping[str, Any]) -> bool:
    rows = [
        *_rows(evidence.get("command_events")),
        *_rows(evidence.get("gateway_commands")),
        *_rows(evidence.get("command_failed")),
    ]
    return any(
        str(row.get("event_type") or "").lower() == "command_failed"
        or str(row.get("status") or "").upper() in {"FAILED", "REJECTED", "EXPIRED"}
        for row in rows
    )


def _has_broker_rejection(errors: Sequence[Mapping[str, Any]]) -> bool:
    return any("BROKER" in str(row.get("error_message") or "").upper() for row in errors)


def _any_mismatch(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(int(row.get("mismatch_count") or 0) > 0 for row in rows)


def _duplicate_idempotency(evidence: Mapping[str, Any]) -> bool:
    return bool(_mapping(evidence.get("idempotency")).get("duplicate"))


def _top_reason_codes(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        for reason in _strings(row.get("reason_codes")):
            counter[reason] += 1
    return [reason for reason, _ in counter.most_common(10)]


def _status(row: Mapping[str, Any]) -> str:
    return str(row.get("status") or "").upper()


def _rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item).strip()]
    return []


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _json_array(value: str | None) -> list[str]:
    if not value:
        return []
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _unique(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]
