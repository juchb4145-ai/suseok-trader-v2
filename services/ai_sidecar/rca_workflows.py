from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

from domain.ai_sidecar.context import AISidecarContextPacket
from domain.ai_sidecar.rca import (
    AIRCAReport,
    AIRCAReportBuildResult,
    AIRCAReportStatus,
    AIRCAReportType,
    AIRCARootCauseCategory,
    AIRCASection,
    AIRCASeverity,
)
from domain.ai_sidecar.tasks import AISidecarTaskType
from storage.event_store import get_gateway_status_values

from services.ai_sidecar.context_builder import build_context_packet
from services.ai_sidecar.context_store import save_context_packet
from services.ai_sidecar.openai_client import AISidecarModelClient
from services.ai_sidecar.rca_report_store import save_rca_report, save_rca_report_error
from services.ai_sidecar.request_store import get_ai_insight
from services.ai_sidecar.runner import AISidecarRunResult, run_ai_sidecar_task
from services.config import Settings, load_settings

DEFAULT_BATCH_STATES = ("DATA_WAIT", "BLOCKED_OBSERVATION", "STALE", "CLOSED")
DEFAULT_BATCH_RISK_STATUSES = ("OBSERVE_BLOCK", "OBSERVE_CAUTION")
DEFAULT_BATCH_STRATEGY_STATUSES = ("DATA_WAIT", "NO_SETUP", "FORMING")


def build_no_trade_rca_report(
    connection: sqlite3.Connection,
    trade_date: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AIRCAReportBuildResult:
    resolved_settings = settings or load_settings()
    try:
        packet = build_context_packet(
            connection,
            AISidecarTaskType.NO_TRADE_RCA,
            trade_date=trade_date,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        sections = _build_no_trade_sections(connection, packet, trade_date)
        root_category, root_cause, severity = classify_no_trade_root_cause(
            connection,
            packet,
            trade_date=trade_date,
        )
        ai_result = _maybe_run_ai(
            connection,
            AISidecarTaskType.NO_TRADE_RCA,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=trade_date,
            related_entity_type=None,
            related_entity_id=None,
            context_id=packet.context_id,
        )
        report = _build_report(
            connection=connection,
            report_type=AIRCAReportType.NO_TRADE_RCA,
            title=f"No-trade RCA for {trade_date}",
            summary=_no_trade_summary(root_category, root_cause),
            root_cause_category=root_category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=trade_date,
            related_entity_type=None,
            related_entity_id=None,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                (
                    "NO_ORDER_PATH_BY_DESIGN: no OMS, OrderIntent, order API, "
                    "or gateway order command path exists in this PR."
                ),
                *packet.warnings,
            ],
            suggested_checks=_suggest_no_trade_checks(root_category),
        )
        save_rca_report(connection, report)
        return AIRCAReportBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_rca_report_error(
            connection,
            report_type=AIRCAReportType.NO_TRADE_RCA,
            trade_date=trade_date,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return AIRCAReportBuildResult(ok=False, error_message=str(exc))


def build_candidate_block_rca_report(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    run_ai: bool = False,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AIRCAReportBuildResult:
    resolved_settings = settings or load_settings()
    try:
        packet = build_context_packet(
            connection,
            AISidecarTaskType.CANDIDATE_BLOCK_RCA,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            settings=resolved_settings,
        )
        save_context_packet(connection, packet)
        sections = _build_candidate_sections(packet)
        root_category, root_cause, severity = classify_candidate_block_root_cause(packet)
        candidate = _section_payload(packet, "candidate_detail")
        trade_date = packet.trade_date or candidate.get("trade_date")
        ai_result = _maybe_run_ai(
            connection,
            AISidecarTaskType.CANDIDATE_BLOCK_RCA,
            run_ai=run_ai,
            model_client=model_client,
            settings=resolved_settings,
            trade_date=trade_date,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            context_id=packet.context_id,
        )
        report = _build_report(
            connection=connection,
            report_type=AIRCAReportType.CANDIDATE_BLOCK_RCA,
            title=f"Candidate block RCA for {candidate_instance_id}",
            summary=_candidate_summary(root_category, root_cause, candidate),
            root_cause_category=root_category,
            root_cause=root_cause,
            severity=severity,
            sections=sections,
            trade_date=trade_date,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            context_id=packet.context_id,
            ai_result=ai_result,
            run_ai=run_ai,
            warnings=[
                "MATCHED_OBSERVATION is not a buy signal.",
                "OBSERVE_PASS is not order approval.",
                *packet.warnings,
            ],
            suggested_checks=_suggest_candidate_checks(root_category),
        )
        save_rca_report(connection, report)
        return AIRCAReportBuildResult(ok=True, report=report, warnings=report.warnings)
    except Exception as exc:
        save_rca_report_error(
            connection,
            report_type=AIRCAReportType.CANDIDATE_BLOCK_RCA,
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            error_message=str(exc),
            payload={"run_ai": run_ai},
        )
        return AIRCAReportBuildResult(ok=False, error_message=str(exc))


def build_candidate_block_rca_reports_for_trade_date(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    *,
    states: Sequence[str] | None = None,
    risk_statuses: Sequence[str] | None = None,
    limit: int = 50,
    run_ai: bool = False,
    settings: Settings | None = None,
    model_client: AISidecarModelClient | None = None,
) -> list[AIRCAReportBuildResult]:
    bounded_limit = min(max(int(limit), 1), 100)
    targets = _batch_candidate_targets(
        connection,
        trade_date=trade_date,
        states=states,
        risk_statuses=risk_statuses,
        limit=bounded_limit,
    )
    results = []
    for candidate in targets:
        results.append(
            build_candidate_block_rca_report(
                connection,
                candidate["candidate_instance_id"],
                run_ai=run_ai,
                model_client=model_client,
                settings=settings,
            )
        )
    return results


def summarize_reason_codes(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for row in rows:
        raw = row.get("reason_codes", row.get("reason_codes_json", []))
        if isinstance(raw, str):
            reasons = [raw]
        elif isinstance(raw, Sequence):
            reasons = raw
        else:
            reasons = []
        for reason in reasons:
            if str(reason).strip():
                counter[str(reason)] += 1
    return [
        {"reason_code": reason, "count": count}
        for reason, count in counter.most_common(20)
    ]


def classify_no_trade_root_cause(
    connection: sqlite3.Connection,
    packet: AISidecarContextPacket,
    *,
    trade_date: str,
) -> tuple[AIRCARootCauseCategory, str, AIRCASeverity]:
    errors = _section_payload(packet, "recent_errors")
    error_category = _projection_error_category(errors)
    if error_category is not None:
        return (
            error_category,
            f"{error_category.value}: recent projection/evaluation errors require operator review.",
            AIRCASeverity.HIGH,
        )

    gateway_values = get_gateway_status_values(connection)
    gateway_event_count = _count_rows(connection, "gateway_events")
    if gateway_event_count > 0 and not gateway_values.get("last_heartbeat_at"):
        return (
            AIRCARootCauseCategory.GATEWAY_TRANSPORT,
            "GATEWAY_HEARTBEAT_MISSING: gateway events exist but heartbeat is absent.",
            AIRCASeverity.MEDIUM,
        )

    risk_counts = _status_counts(
        connection,
        "risk_observations_latest",
        "overall_status",
        trade_date=trade_date,
    )
    if risk_counts.get("OBSERVE_BLOCK", 0) > 0:
        return (
            AIRCARootCauseCategory.RISK_CONTEXT,
            "RISK_OBSERVE_BLOCK_DOMINANT: latest risk observations include blocks.",
            AIRCASeverity.HIGH,
        )

    candidate_count = _count_rows(
        connection,
        "candidates",
        where="trade_date = ?",
        params=(trade_date,),
    )
    strategy_counts = _status_counts(
        connection,
        "strategy_observations_latest",
        "overall_status",
        trade_date=trade_date,
    )
    if candidate_count > 0 and strategy_counts.get("MATCHED_OBSERVATION", 0) == 0:
        return (
            AIRCARootCauseCategory.STRATEGY_CONTEXT,
            "STRATEGY_MATCHED_OBSERVATION_ZERO: candidates did not reach matched observation.",
            AIRCASeverity.MEDIUM,
        )

    if candidate_count == 0 and _has_upstream_activity(connection):
        return (
            AIRCARootCauseCategory.CANDIDATE_CONTEXT,
            "CANDIDATE_FUNNEL_EMPTY: upstream observations did not create candidates.",
            AIRCASeverity.MEDIUM,
        )

    return (
        AIRCARootCauseCategory.NO_ORDER_PATH_BY_DESIGN,
        "NO_ORDER_PATH_BY_DESIGN: observe-only pipeline has no OMS, OrderIntent, or order API.",
        AIRCASeverity.INFO,
    )


def classify_candidate_block_root_cause(
    packet: AISidecarContextPacket,
) -> tuple[AIRCARootCauseCategory, str, AIRCASeverity]:
    candidate = _section_payload(packet, "candidate_detail")
    if not candidate:
        return (
            AIRCARootCauseCategory.CANDIDATE_CONTEXT,
            "CANDIDATE_NOT_FOUND: requested candidate instance is absent.",
            AIRCASeverity.MEDIUM,
        )

    state = str(candidate.get("state") or "").upper()
    market = _section_payload(packet, "latest_market_tick_readiness")
    readiness = _mapping_or_empty(market.get("readiness"))
    theme = _section_payload(packet, "theme_context")
    latest_snapshot = _mapping_or_empty(theme.get("latest_snapshot"))
    strategy = _section_payload(packet, "latest_strategy_observation")
    risk = _section_payload(packet, "latest_risk_observation")
    errors = _section_payload(packet, "related_projection_errors")

    error_category = _projection_error_category(errors)
    if error_category is not None:
        return (
            error_category,
            f"{error_category.value}: related projection/evaluation errors require review.",
            AIRCASeverity.HIGH,
        )
    if state == "DATA_WAIT":
        category = (
            AIRCARootCauseCategory.DATA_QUALITY
            if _market_readiness_missing(readiness)
            else AIRCARootCauseCategory.CANDIDATE_CONTEXT
        )
        return (
            category,
            "DATA_WAIT: candidate is waiting for required context.",
            AIRCASeverity.MEDIUM,
        )
    if state == "STALE":
        return (
            AIRCARootCauseCategory.CANDIDATE_CONTEXT,
            "STALE_CONTEXT: candidate observation episode is stale.",
            AIRCASeverity.MEDIUM,
        )
    if state == "CLOSED":
        return (
            AIRCARootCauseCategory.CANDIDATE_CONTEXT,
            "SOURCE_EXITED: candidate was closed by source exit or episode completion.",
            AIRCASeverity.LOW,
        )

    strategy_status = str(strategy.get("overall_status") or "").upper()
    if strategy_status in {"DATA_WAIT", "NO_SETUP", "WATCH", "FORMING", "STALE_CONTEXT"}:
        return (
            AIRCARootCauseCategory.STRATEGY_CONTEXT,
            f"{strategy_status}: latest strategy observation is not a buy-readiness signal.",
            AIRCASeverity.MEDIUM,
        )

    risk_status = str(risk.get("overall_status") or "").upper()
    if risk_status == "OBSERVE_BLOCK":
        return (
            AIRCARootCauseCategory.RISK_CONTEXT,
            "OBSERVE_BLOCK: risk gate blocked the observation path for review.",
            AIRCASeverity.HIGH,
        )

    if _market_readiness_missing(readiness):
        return (
            AIRCARootCauseCategory.DATA_QUALITY,
            "MARKET_READINESS_MISSING: latest tick/bar/readiness context is incomplete.",
            AIRCASeverity.MEDIUM,
        )

    if not latest_snapshot or str(latest_snapshot.get("state") or "").upper() not in {
        "LEADING",
        "SPREADING",
    }:
        return (
            AIRCARootCauseCategory.THEME_CONTEXT,
            "THEME_CONTEXT_WEAK_OR_MISSING: theme snapshot is absent or not leading/spreading.",
            AIRCASeverity.LOW,
        )

    return (
        AIRCARootCauseCategory.CANDIDATE_CONTEXT,
        "CANDIDATE_CONTEXT_REVIEW: no single blocking reason dominated deterministic evidence.",
        AIRCASeverity.LOW,
    )


def build_rca_section(
    section_name: str,
    *,
    status: str,
    severity: AIRCASeverity | str,
    summary: str,
    reason_codes: Sequence[str] = (),
    evidence_json: Mapping[str, Any] | None = None,
    source_refs: Sequence[str] = (),
) -> AIRCASection:
    return AIRCASection(
        section_name=section_name,
        status=status,
        severity=severity,
        summary=summary,
        reason_codes=reason_codes,
        evidence_json=evidence_json or {},
        source_refs=source_refs,
    )


def attach_ai_insight_to_report(
    connection: sqlite3.Connection,
    report: AIRCAReport,
    ai_result: AISidecarRunResult,
) -> AIRCAReport:
    if not ai_result.ok or not ai_result.insight_id:
        return replace(
            report,
            status=_report_status_from_ai_result(ai_result),
            ai_request_id=ai_result.request_id,
        )
    insight = get_ai_insight(connection, ai_result.insight_id)
    output = _mapping_or_empty((insight or {}).get("output"))
    checks = output.get("suggested_checks", [])
    suggested_checks = [*report.suggested_checks]
    if isinstance(checks, Sequence) and not isinstance(checks, str):
        suggested_checks.extend(str(check) for check in checks if str(check).strip())
    return replace(
        report,
        status=AIRCAReportStatus.COMPLETED,
        ai_request_id=ai_result.request_id,
        ai_insight_id=ai_result.insight_id,
        ai_summary=(insight or {}).get("summary") or ai_result.status,
        suggested_checks=_unique(suggested_checks),
    )


def _build_report(
    *,
    connection: sqlite3.Connection,
    report_type: AIRCAReportType,
    title: str,
    summary: str,
    root_cause_category: AIRCARootCauseCategory,
    root_cause: str,
    severity: AIRCASeverity,
    sections: Sequence[AIRCASection],
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    context_id: str,
    ai_result: AISidecarRunResult | None,
    run_ai: bool,
    warnings: Sequence[str],
    suggested_checks: Sequence[str],
) -> AIRCAReport:
    report = AIRCAReport(
        report_type=report_type,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        title=title,
        summary=summary,
        status=AIRCAReportStatus.COMPLETED,
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
        },
    )
    if ai_result is None:
        return report
    return attach_ai_insight_to_report(connection, report, ai_result)


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
    return run_ai_sidecar_task(
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


def _build_no_trade_sections(
    connection: sqlite3.Connection,
    packet: AISidecarContextPacket,
    trade_date: str,
) -> list[AIRCASection]:
    gateway_values = get_gateway_status_values(connection)
    reason_summary = _section_payload(packet, "reason_code_summary")
    errors = _section_payload(packet, "recent_errors")
    pipeline = _section_payload(packet, "pipeline_funnel_counts")
    heartbeat_missing = not gateway_values.get("last_heartbeat_at")
    candidate_count = _count_rows(connection, "candidates", "trade_date = ?", (trade_date,))
    strategy_counts = _status_counts(
        connection,
        "strategy_observations_latest",
        "overall_status",
        trade_date=trade_date,
    )
    risk_counts = _status_counts(
        connection,
        "risk_observations_latest",
        "overall_status",
        trade_date=trade_date,
    )
    strategy_matched_zero = strategy_counts.get("MATCHED_OBSERVATION", 0) == 0
    risk_block_present = risk_counts.get("OBSERVE_BLOCK", 0) > 0
    error_count = _error_count(errors)
    return [
        build_rca_section(
            "safety posture",
            status="INFO",
            severity=AIRCASeverity.INFO,
            summary="RCA is read-only; no order path or AI order tools are available.",
            reason_codes=["OBSERVE_ONLY_PIPELINE", "NO_ORDER_PATH_BY_DESIGN"],
            evidence_json=_section_payload(packet, "dashboard_safety"),
            source_refs=["dashboard_safety"],
        ),
        build_rca_section(
            "gateway transport",
            status="WARN" if heartbeat_missing else "OK",
            severity=AIRCASeverity.MEDIUM if heartbeat_missing else AIRCASeverity.INFO,
            summary="Gateway heartbeat and command transport state for operator review.",
            reason_codes=["GATEWAY_HEARTBEAT_MISSING"]
            if heartbeat_missing
            else [],
            evidence_json={
                "gateway_status": gateway_values,
                "transport": _section_payload(packet, "gateway_transport_status"),
            },
            source_refs=["gateway_status", "gateway_commands"],
        ),
        build_rca_section(
            "market data",
            status="OK",
            severity=AIRCASeverity.INFO,
            summary="Market data projection counts from the observe-only pipeline.",
            reason_codes=[],
            evidence_json=_mapping_or_empty(pipeline.get("market_data")),
            source_refs=["market_ticks_latest", "market_minute_bars"],
        ),
        build_rca_section(
            "theme snapshots",
            status="OK",
            severity=AIRCASeverity.INFO,
            summary="Theme projection counts from the observe-only pipeline.",
            reason_codes=[],
            evidence_json=_mapping_or_empty(pipeline.get("themes")),
            source_refs=["theme_latest_snapshots"],
        ),
        build_rca_section(
            "candidate funnel",
            status="WARN" if candidate_count == 0 else "OK",
            severity=AIRCASeverity.MEDIUM if candidate_count == 0 else AIRCASeverity.INFO,
            summary="Candidate state counts and candidate reason-code summary.",
            reason_codes=_top_reason_names(reason_summary.get("candidate_reason_codes")),
            evidence_json={
                "status": _section_payload(packet, "candidate_state_counts"),
                "trade_date_candidate_count": candidate_count,
                "reason_codes": reason_summary.get("candidate_reason_codes", []),
            },
            source_refs=["candidates"],
        ),
        build_rca_section(
            "strategy observations",
            status="WARN" if strategy_matched_zero else "OK",
            severity=AIRCASeverity.MEDIUM,
            summary="Strategy observation status and reason-code summary.",
            reason_codes=_top_reason_names(reason_summary.get("strategy_reason_codes")),
            evidence_json={
                "status": _section_payload(packet, "strategy_status_counts"),
                "trade_date_status_counts": strategy_counts,
                "reason_codes": reason_summary.get("strategy_reason_codes", []),
            },
            source_refs=["strategy_observations_latest"],
        ),
        build_rca_section(
            "risk observations",
            status="WARN" if risk_block_present else "OK",
            severity=AIRCASeverity.HIGH if risk_block_present else AIRCASeverity.INFO,
            summary="Risk observation status and reason-code summary.",
            reason_codes=_top_reason_names(reason_summary.get("risk_reason_codes")),
            evidence_json={
                "status": _section_payload(packet, "risk_status_counts"),
                "trade_date_status_counts": risk_counts,
                "reason_codes": reason_summary.get("risk_reason_codes", []),
            },
            source_refs=["risk_observations_latest"],
        ),
        build_rca_section(
            "projection/evaluation errors",
            status="WARN" if error_count else "OK",
            severity=AIRCASeverity.HIGH if error_count else AIRCASeverity.INFO,
            summary="Recent projection and evaluation errors associated with no-trade analysis.",
            reason_codes=["RECENT_ERRORS_PRESENT"] if _error_count(errors) else [],
            evidence_json=errors,
            source_refs=[
                "market_projection_errors",
                "theme_projection_errors",
                "candidate_projection_errors",
                "strategy_evaluation_errors",
                "risk_evaluation_errors",
            ],
        ),
        build_rca_section(
            "AI execution status",
            status="INFO",
            severity=AIRCASeverity.INFO,
            summary="AI execution is optional and defaults to disabled for RCA reports.",
            reason_codes=["AI_RUN_DEFAULT_FALSE"],
            evidence_json=_section_payload(packet, "ai_sidecar_status"),
            source_refs=["ai_requests", "ai_insights"],
        ),
    ]


def _build_candidate_sections(packet: AISidecarContextPacket) -> list[AIRCASection]:
    candidate = _section_payload(packet, "candidate_detail")
    market = _section_payload(packet, "latest_market_tick_readiness")
    readiness = _mapping_or_empty(market.get("readiness"))
    readiness_missing = _market_readiness_missing(readiness)
    theme_snapshot = _mapping_or_empty(
        _section_payload(packet, "theme_context").get("latest_snapshot")
    )
    strategy = _section_payload(packet, "latest_strategy_observation")
    strategy_status = str(strategy.get("overall_status") or "").upper()
    risk = _section_payload(packet, "latest_risk_observation")
    risk_block = str(risk.get("overall_status") or "").upper() == "OBSERVE_BLOCK"
    errors = _section_payload(packet, "related_projection_errors")
    error_count = _error_count(errors)
    return [
        build_rca_section(
            "candidate state",
            status="WARN" if not candidate or candidate.get("state") != "CONTEXT_READY" else "OK",
            severity=AIRCASeverity.MEDIUM if not candidate else AIRCASeverity.INFO,
            summary="Candidate state and observation episode metadata.",
            reason_codes=_as_str_list(candidate.get("reason_codes", [])),
            evidence_json=candidate,
            source_refs=["candidates"],
        ),
        build_rca_section(
            "source attribution",
            status="OK",
            severity=AIRCASeverity.INFO,
            summary="Candidate source events and state transitions used as evidence.",
            reason_codes=[],
            evidence_json={
                "sources": _section_payload(packet, "candidate_sources").get("items", []),
                "transitions": _section_payload(packet, "candidate_transitions").get("items", []),
            },
            source_refs=["candidate_source_events", "candidate_state_transitions"],
        ),
        build_rca_section(
            "market readiness",
            status="WARN" if readiness_missing else "OK",
            severity=AIRCASeverity.MEDIUM if readiness_missing else AIRCASeverity.INFO,
            summary="Latest tick, bars, VWAP, and readiness status for the candidate code.",
            reason_codes=_as_str_list(readiness.get("reason_codes", [])),
            evidence_json=market,
            source_refs=["market_ticks_latest", "market_minute_bars"],
        ),
        build_rca_section(
            "theme context",
            status="WARN" if not theme_snapshot else "OK",
            severity=AIRCASeverity.LOW,
            summary="Latest theme snapshot and member role context.",
            reason_codes=[],
            evidence_json=_section_payload(packet, "theme_context"),
            source_refs=["theme_latest_snapshots", "theme_snapshot_members"],
        ),
        build_rca_section(
            "strategy observation",
            status="WARN"
            if strategy_status in {"DATA_WAIT", "NO_SETUP", "WATCH", "FORMING", "STALE_CONTEXT"}
            or not strategy
            else "OK",
            severity=AIRCASeverity.MEDIUM,
            summary="Latest strategy observation is a classifier result, not buy readiness.",
            reason_codes=_as_str_list(strategy.get("reason_codes", [])),
            evidence_json=strategy,
            source_refs=["strategy_observations_latest", "strategy_setup_observations"],
        ),
        build_rca_section(
            "risk observation",
            status="WARN" if risk_block else "OK",
            severity=AIRCASeverity.HIGH if risk_block else AIRCASeverity.INFO,
            summary="Latest risk observation is observe-only and not order approval.",
            reason_codes=_as_str_list(risk.get("reason_codes", [])),
            evidence_json=risk,
            source_refs=["risk_observations_latest", "risk_check_observations"],
        ),
        build_rca_section(
            "errors",
            status="WARN" if error_count else "OK",
            severity=AIRCASeverity.HIGH if error_count else AIRCASeverity.INFO,
            summary="Projection and evaluation errors related to this candidate/code.",
            reason_codes=["RELATED_ERRORS_PRESENT"] if error_count else [],
            evidence_json=errors,
            source_refs=["projection_errors", "evaluation_errors"],
        ),
        build_rca_section(
            "safety reminders",
            status="INFO",
            severity=AIRCASeverity.INFO,
            summary="RCA report remains read-only and cannot create trading side effects.",
            reason_codes=["OBSERVE_ONLY_PIPELINE"],
            evidence_json=_section_payload(packet, "dashboard_safety_warnings"),
            source_refs=["dashboard_safety"],
        ),
    ]


def _report_status_from_ai_result(result: AISidecarRunResult) -> AIRCAReportStatus:
    status = result.status.upper()
    if status == "AI_DISABLED":
        return AIRCAReportStatus.AI_DISABLED
    if status in {"API_KEY_MISSING", "CLIENT_UNAVAILABLE", "MODEL_ERROR", "TIMEOUT"}:
        return AIRCAReportStatus.AI_UNAVAILABLE
    if status == "AI_OUTPUT_INVALID":
        return AIRCAReportStatus.AI_OUTPUT_INVALID
    if status == "POLICY_REJECTED":
        return AIRCAReportStatus.POLICY_REJECTED
    return AIRCAReportStatus.PARTIAL


def _no_trade_summary(category: AIRCARootCauseCategory, root_cause: str) -> str:
    if category is AIRCARootCauseCategory.NO_ORDER_PATH_BY_DESIGN:
        return "No-trade is consistent with the observe-only design; no order path exists."
    return f"No-trade RCA classified deterministic evidence as {category.value}: {root_cause}"


def _candidate_summary(
    category: AIRCARootCauseCategory,
    root_cause: str,
    candidate: Mapping[str, Any],
) -> str:
    name = candidate.get("name") or candidate.get("code") or "candidate"
    return f"{name} candidate block RCA classified evidence as {category.value}: {root_cause}"


def _suggest_no_trade_checks(category: AIRCARootCauseCategory) -> list[str]:
    checks = [
        "Review dashboard pipeline counts and recent projection errors.",
        "Confirm that no OMS, OrderIntent, or order API exists in this PR.",
    ]
    if category is AIRCARootCauseCategory.GATEWAY_TRANSPORT:
        checks.append("Check gateway heartbeat and event ingest status.")
    if category is AIRCARootCauseCategory.STRATEGY_CONTEXT:
        checks.append("Inspect latest strategy observation statuses and reason codes.")
    if category is AIRCARootCauseCategory.RISK_CONTEXT:
        checks.append("Inspect risk check observations for OBSERVE_BLOCK reasons.")
    return checks


def _suggest_candidate_checks(category: AIRCARootCauseCategory) -> list[str]:
    checks = [
        "Review candidate state, latest context, strategy observation, and risk observation.",
        "Confirm that the RCA report is not used as Strategy/Risk/OMS input.",
    ]
    if category is AIRCARootCauseCategory.DATA_QUALITY:
        checks.append("Check latest tick, minute bars, VWAP readiness, and projection errors.")
    if category is AIRCARootCauseCategory.THEME_CONTEXT:
        checks.append("Check latest theme snapshot state, quality, and member role.")
    if category is AIRCARootCauseCategory.RISK_CONTEXT:
        checks.append("Inspect risk check observations and evidence for block reasons.")
    return checks


def _section_payload(packet: AISidecarContextPacket, section_name: str) -> dict[str, Any]:
    payload = packet.payload.get(section_name, {})
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_str_list(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item).strip()]
    return []


def _top_reason_names(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        return []
    names = []
    for item in value:
        if isinstance(item, Mapping) and item.get("reason_code"):
            names.append(str(item["reason_code"]))
    return names


def _error_count(errors: Mapping[str, Any]) -> int:
    count = 0
    for value in errors.values():
        if isinstance(value, Sequence) and not isinstance(value, str):
            count += len(value)
    return count


def _projection_error_category(errors: Mapping[str, Any]) -> AIRCARootCauseCategory | None:
    mapping = (
        ("market_projection_errors", AIRCARootCauseCategory.MARKET_DATA_PROJECTION),
        ("theme_projection_errors", AIRCARootCauseCategory.THEME_PROJECTION),
        ("candidate_projection_errors", AIRCARootCauseCategory.CANDIDATE_PROJECTION),
        ("strategy_errors", AIRCARootCauseCategory.STRATEGY_EVALUATION),
        ("risk_errors", AIRCARootCauseCategory.RISK_EVALUATION),
    )
    for key, category in mapping:
        value = errors.get(key)
        if isinstance(value, Sequence) and not isinstance(value, str) and value:
            return category
    return None


def _market_readiness_missing(readiness: Mapping[str, Any]) -> bool:
    if not readiness:
        return True
    if not readiness.get("has_latest_tick"):
        return True
    quality = str(readiness.get("quality_status") or "").upper()
    return quality in {"", "MISSING", "STALE", "INVALID"}


def _has_upstream_activity(connection: sqlite3.Connection) -> bool:
    tables = (
        "gateway_events",
        "market_ticks_latest",
        "theme_latest_snapshots",
        "market_condition_latest",
    )
    return any(_count_rows(connection, table_name) > 0 for table_name in tables)


def _status_counts(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    *,
    trade_date: str,
) -> dict[str, int]:
    rows = connection.execute(
        f"""
        SELECT {column_name} AS status, COUNT(*) AS count
        FROM {table_name}
        WHERE trade_date = ?
        GROUP BY {column_name}
        """,
        (trade_date,),
    ).fetchall()
    return {str(row["status"]): int(row["count"]) for row in rows}


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    where: str | None = None,
    params: tuple[Any, ...] = (),
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}",
        params,
    ).fetchone()
    return int(row["count"])


def _batch_candidate_targets(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    states: Sequence[str] | None,
    risk_statuses: Sequence[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_states = tuple(
        str(state).strip().upper() for state in (states or DEFAULT_BATCH_STATES)
    )
    normalized_risk_statuses = tuple(
        str(status).strip().upper() for status in (risk_statuses or DEFAULT_BATCH_RISK_STATUSES)
    )
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date:
        clauses.append("c.trade_date = ?")
        params.append(trade_date)

    selection_sql: list[str] = []
    if normalized_states:
        placeholders = ", ".join("?" for _ in normalized_states)
        selection_sql.append(f"c.state IN ({placeholders})")
        params.extend(normalized_states)
    if normalized_risk_statuses:
        placeholders = ", ".join("?" for _ in normalized_risk_statuses)
        selection_sql.append(
            f"""
            EXISTS (
                SELECT 1
                FROM risk_observations_latest AS r
                WHERE r.candidate_instance_id = c.candidate_instance_id
                    AND r.overall_status IN ({placeholders})
            )
            """
        )
        params.extend(normalized_risk_statuses)
    placeholders = ", ".join("?" for _ in DEFAULT_BATCH_STRATEGY_STATUSES)
    selection_sql.append(
        f"""
        EXISTS (
            SELECT 1
            FROM strategy_observations_latest AS s
            WHERE s.candidate_instance_id = c.candidate_instance_id
                AND s.overall_status IN ({placeholders})
        )
        """
    )
    params.extend(DEFAULT_BATCH_STRATEGY_STATUSES)
    clauses.append("(" + " OR ".join(selection_sql) + ")")
    where_sql = "WHERE " + " AND ".join(clauses)
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT c.*
        FROM candidates AS c
        {where_sql}
        ORDER BY c.last_seen_at DESC, c.candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]


def _unique(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]
