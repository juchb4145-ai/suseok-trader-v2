from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Mapping
from typing import Any

from domain.ai_sidecar.policy import contains_forbidden_action

from services.ai_sidecar.codex_prompt_store import (
    count_codex_prompt_drafts,
    list_codex_prompt_drafts,
    list_codex_prompt_errors,
)
from services.ai_sidecar.context_store import list_context_build_errors
from services.ai_sidecar.live_sim_review_store import (
    count_live_sim_review_reports,
    get_live_sim_review_report,
    list_live_sim_review_errors,
    list_live_sim_review_reports,
)
from services.ai_sidecar.rca_report_store import (
    count_rca_reports,
    get_rca_report,
    list_rca_report_errors,
    list_rca_reports,
)
from services.ai_sidecar.request_store import (
    AIRequestStatus,
    ai_request_row_to_dict,
    count_ai_insights,
    list_ai_insights,
)
from services.config import Settings
from services.dashboard_ai_labels import (
    map_ai_severity_label,
    map_ai_status_label,
    map_card_type_label,
    map_rca_category_label,
    map_readonly_operator_action,
)

READONLY_OPERATOR_ACTIONS = {
    "WATCH_ONLY",
    "REVIEW_ONLY",
    "CHECK_DATA",
    "CHECK_PIPELINE",
    "CHECK_POLICY",
    "NO_ACTION",
    "OBSERVE_ONLY",
    "관찰",
    "검토",
    "점검",
}

AI_REQUEST_FAILURE_STATUSES = tuple(
    status.value
    for status in AIRequestStatus
    if status
    not in {
        AIRequestStatus.QUEUED,
        AIRequestStatus.RUNNING,
        AIRequestStatus.COMPLETED,
    }
)

AI_EXPLANATION_WARNINGS = [
    "AI 설명 카드는 읽기 전용입니다.",
    "AI/RCA 결과는 Strategy/Risk/OMS 자동 입력이 아닙니다.",
    "Codex prompt draft는 사람이 검토하고 복사하는 텍스트입니다.",
    "Dashboard에는 AI 실행 버튼이 없습니다.",
]


def build_ai_explanation_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
    candidate_limit: int = 20,
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit, settings)
    cards = _build_all_cards(
        connection,
        settings,
        limit=max(bounded_limit, 20),
        candidate_limit=candidate_limit,
    )
    latest_cards = _sort_cards(cards)[:bounded_limit]
    status = build_ai_explanation_status(connection, settings)
    return {
        "available": True,
        "cards_enabled": True,
        "execution_controls_available": False,
        "run_buttons_available": False,
        "cards": latest_cards,
        "latest_cards": latest_cards,
        "latest_card_count": len(latest_cards),
        "warnings": list(AI_EXPLANATION_WARNINGS),
        **status,
    }


def build_no_trade_rca_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = list_rca_reports(
        connection,
        report_type="NO_TRADE_RCA",
        limit=_bounded_limit(limit, settings),
    )
    return [_rca_report_card(connection, row) for row in rows]


def build_candidate_rca_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = list_rca_reports(
        connection,
        report_type="CANDIDATE_BLOCK_RCA",
        limit=_bounded_limit(limit, settings),
    )
    return [_rca_report_card(connection, row) for row in rows]


def build_ai_insight_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    insights = list_ai_insights(
        connection,
        limit=_bounded_limit(limit, settings),
        include_output=True,
    )
    return [_ai_insight_card(insight) for insight in insights]


def build_ai_request_failure_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = _list_ai_request_failures(connection, limit=_bounded_limit(limit, settings))
    return [_ai_request_failure_card(row) for row in rows]


def build_ai_explanation_status(
    connection: sqlite3.Connection,
    settings: Settings,
) -> dict[str, Any]:
    cards = _build_all_cards(
        connection,
        settings,
        limit=min(settings.dashboard_max_limit, 500),
        candidate_limit=min(settings.dashboard_max_limit, 500),
    )
    status_counts = Counter(str(card["status"]) for card in cards)
    severity_counts = Counter(str(card["severity"]) for card in cards)
    return {
        "available": True,
        "execution_controls_available": False,
        "run_buttons_available": False,
        "rca_report_count": count_rca_reports(connection),
        "live_sim_review_report_count": count_live_sim_review_reports(connection),
        "codex_prompt_draft_count": count_codex_prompt_drafts(connection),
        "ai_insight_count": count_ai_insights(connection),
        "ai_request_failure_count": _count_ai_request_failures(connection),
        "context_warning_count": _count_context_warnings(connection),
        "status_counts": dict(status_counts),
        "severity_counts": dict(severity_counts),
    }


def filter_ai_explanation_cards(
    cards: list[dict[str, Any]],
    *,
    card_type: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    trade_date: str | None = None,
) -> list[dict[str, Any]]:
    filtered = cards
    for key, expected in (
        ("card_type", card_type),
        ("severity", severity),
        ("status", status),
        ("related_entity_type", related_entity_type),
        ("related_entity_id", related_entity_id),
        ("trade_date", trade_date),
    ):
        if expected is None:
            continue
        normalized = str(expected).strip().upper()
        filtered = [
            card
            for card in filtered
            if str(card.get(key) or "").strip().upper() == normalized
        ]
    return filtered


def _build_all_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int,
    candidate_limit: int,
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit, settings)
    return [
        *build_no_trade_rca_cards(connection, settings, limit=min(5, bounded_limit)),
        *build_candidate_rca_cards(connection, settings, limit=candidate_limit),
        *build_live_sim_review_cards(connection, settings, limit=bounded_limit),
        *build_codex_prompt_draft_cards(connection, settings, limit=bounded_limit),
        *build_ai_insight_cards(connection, settings, limit=bounded_limit),
        *build_ai_request_failure_cards(connection, settings, limit=bounded_limit),
        *_build_ai_context_warning_cards(connection, settings, limit=bounded_limit),
    ]


def build_codex_prompt_draft_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = list_codex_prompt_drafts(connection, limit=_bounded_limit(limit, settings))
    return [_codex_prompt_draft_card(row) for row in rows]


def build_live_sim_review_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = list_live_sim_review_reports(connection, limit=_bounded_limit(limit, settings))
    return [_live_sim_review_card(connection, row) for row in rows]


def _rca_report_card(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    report = get_rca_report(
        connection,
        str(row["report_id"]),
        include_sections=True,
        include_links=True,
    ) or dict(row)
    card_type = str(report["report_type"])
    warnings = _strings(report.get("warnings"))
    warnings.extend(_policy_warnings(report))
    return _decorate_card(
        {
            "card_id": f"rca:{report['report_id']}",
            "card_type": card_type,
            "title": map_card_type_label(card_type),
            "subtitle": report.get("title") or map_card_type_label(card_type),
            "status": str(report["status"]),
            "severity": str(report["severity"]),
            "root_cause_category": report.get("root_cause_category"),
            "root_cause": report.get("root_cause"),
            "summary": report.get("summary"),
            "suggested_checks": _strings(report.get("suggested_checks")),
            "warnings": _unique(warnings),
            "related_entity_type": report.get("related_entity_type"),
            "related_entity_id": report.get("related_entity_id"),
            "trade_date": report.get("trade_date"),
            "generated_at": report.get("generated_at"),
            "ai_request_id": report.get("ai_request_id"),
            "ai_insight_id": report.get("ai_insight_id"),
            "rca_report_id": report.get("report_id"),
            "context_id": report.get("context_id"),
            "source": "ai_rca_reports",
            "report_sections": report.get("deterministic_sections", []),
            "links": report.get("links", []),
            "metadata": report.get("metadata", {}),
        }
    )


def _live_sim_review_card(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    report = get_live_sim_review_report(
        connection,
        str(row["review_id"]),
        include_sections=True,
        include_links=True,
    ) or dict(row)
    card_type = str(report["report_type"])
    warnings = _strings(report.get("warnings"))
    warnings.extend(
        [
            "LIVE_SIM review card는 표시 전용입니다.",
            "이 카드는 주문 재시도/취소/정정 입력이 아닙니다.",
        ]
    )
    return _decorate_card(
        {
            "card_id": f"live-sim-review:{report['review_id']}",
            "card_type": card_type,
            "title": map_card_type_label(card_type),
            "subtitle": report.get("title") or map_card_type_label(card_type),
            "status": str(report["status"]),
            "severity": str(report["severity"]),
            "root_cause_category": report.get("root_cause_category"),
            "root_cause": report.get("root_cause"),
            "summary": report.get("summary"),
            "suggested_checks": _strings(report.get("suggested_checks")),
            "warnings": _unique(warnings),
            "related_entity_type": report.get("related_entity_type"),
            "related_entity_id": report.get("related_entity_id"),
            "trade_date": report.get("trade_date"),
            "generated_at": report.get("generated_at"),
            "ai_request_id": report.get("ai_request_id"),
            "ai_insight_id": report.get("ai_insight_id"),
            "rca_report_id": None,
            "live_sim_review_id": report.get("review_id"),
            "live_sim_order_id": report.get("live_sim_order_id"),
            "reconcile_id": report.get("reconcile_id"),
            "context_id": report.get("context_id"),
            "source": "ai_live_sim_review_reports",
            "report_sections": report.get("deterministic_sections", []),
            "links": report.get("links", []),
            "metadata": report.get("metadata", {}),
            "review_only": True,
            "order_action_allowed": False,
            "gateway_command_allowed": False,
            "live_real_allowed": False,
        }
    )


def _codex_prompt_draft_card(row: Mapping[str, Any]) -> dict[str, Any]:
    warnings = _strings(row.get("warnings"))
    warnings.extend(
        [
            "Codex prompt draft는 Dashboard에서 실행되지 않습니다.",
            "복사는 browser clipboard 동작이며 Codex/GitHub/API 실행이 아닙니다.",
        ]
    )
    prompt_text = str(row.get("prompt_text") or "")
    return _decorate_card(
        {
            "card_id": f"codex-prompt:{row['draft_id']}",
            "card_type": "CODEX_PROMPT_DRAFT",
            "title": map_card_type_label("CODEX_PROMPT_DRAFT"),
            "subtitle": str(row.get("title") or row.get("source_type") or "-"),
            "status": str(row.get("status") or "COMPLETED"),
            "severity": "INFO",
            "root_cause_category": row.get("target_area"),
            "root_cause": row.get("summary"),
            "summary": row.get("summary"),
            "suggested_checks": _strings(row.get("acceptance_criteria")),
            "warnings": _unique(warnings),
            "related_entity_type": row.get("related_entity_type"),
            "related_entity_id": row.get("related_entity_id"),
            "trade_date": row.get("trade_date"),
            "generated_at": row.get("generated_at"),
            "ai_request_id": row.get("ai_request_id"),
            "ai_insight_id": row.get("ai_insight_id"),
            "rca_report_id": row.get("rca_report_id"),
            "context_id": row.get("context_id"),
            "source": "ai_codex_prompt_drafts",
            "draft_id": row.get("draft_id"),
            "target_area": row.get("target_area"),
            "source_type": row.get("source_type"),
            "prompt_text": prompt_text,
            "prompt_preview": prompt_text[:800],
            "acceptance_criteria": row.get("acceptance_criteria", []),
            "forbidden_scope": row.get("forbidden_scope", []),
            "test_plan": row.get("test_plan", []),
            "auto_apply_allowed": False,
            "github_write_allowed": False,
            "codex_execution_allowed": False,
        }
    )


def _ai_insight_card(insight: Mapping[str, Any]) -> dict[str, Any]:
    output = _mapping(insight.get("output"))
    operator_action = _safe_operator_action(
        insight.get("operator_action") or output.get("operator_action")
    )
    warnings = _policy_warnings(insight, operator_action=operator_action["raw"])
    checks = _strings(output.get("suggested_checks"))
    if not checks:
        checks = ["저장된 insight 요약과 연결된 request 상태를 검토하세요."]
    return _decorate_card(
        {
            "card_id": f"insight:{insight['insight_id']}",
            "card_type": "AI_INSIGHT",
            "title": map_card_type_label("AI_INSIGHT"),
            "subtitle": str(insight.get("task_type") or "-"),
            "status": "COMPLETED",
            "severity": str(insight.get("severity") or output.get("severity") or "INFO"),
            "root_cause_category": None,
            "root_cause": insight.get("root_cause") or output.get("root_cause"),
            "summary": insight.get("summary"),
            "suggested_checks": checks,
            "warnings": _unique(warnings),
            "related_entity_type": insight.get("related_entity_type"),
            "related_entity_id": insight.get("related_entity_id"),
            "trade_date": insight.get("trade_date"),
            "generated_at": insight.get("created_at"),
            "ai_request_id": insight.get("request_id"),
            "ai_insight_id": insight.get("insight_id"),
            "rca_report_id": None,
            "context_id": None,
            "source": "ai_insights",
            "operator_action": operator_action["value"],
            "operator_action_label": operator_action["label"],
            "output": output,
        }
    )


def _ai_request_failure_card(request: Mapping[str, Any]) -> dict[str, Any]:
    status = str(request.get("status") or "FAILED").upper()
    warnings = _policy_warnings(request)
    validation_error = request.get("validation_error")
    error_message = request.get("error_message") or validation_error
    return _decorate_card(
        {
            "card_id": f"request-failure:{request['request_id']}",
            "card_type": "AI_REQUEST_FAILURE",
            "title": map_card_type_label("AI_REQUEST_FAILURE"),
            "subtitle": str(request.get("task_type") or "-"),
            "status": status,
            "severity": _failure_severity(status),
            "root_cause_category": "AI_EXECUTION",
            "root_cause": error_message or status,
            "summary": _failure_summary(status, error_message),
            "suggested_checks": _failure_suggested_checks(status),
            "warnings": _unique(warnings),
            "related_entity_type": request.get("related_entity_type"),
            "related_entity_id": request.get("related_entity_id"),
            "trade_date": request.get("trade_date"),
            "generated_at": request.get("completed_at") or request.get("created_at"),
            "ai_request_id": request.get("request_id"),
            "ai_insight_id": None,
            "rca_report_id": None,
            "context_id": request.get("context_id"),
            "source": "ai_requests",
            "validation_error": validation_error,
            "error_message": request.get("error_message"),
            "metadata": request.get("metadata", {}),
        }
    )


def _build_ai_context_warning_cards(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit, settings)
    context_errors = list_context_build_errors(connection, limit=bounded_limit)
    rca_errors = list_rca_report_errors(connection, limit=bounded_limit)
    codex_prompt_errors = list_codex_prompt_errors(connection, limit=bounded_limit)
    live_sim_review_errors = list_live_sim_review_errors(connection, limit=bounded_limit)
    cards = [_context_error_card(error) for error in context_errors]
    cards.extend(_rca_error_card(error) for error in rca_errors)
    cards.extend(_codex_prompt_error_card(error) for error in codex_prompt_errors)
    cards.extend(_live_sim_review_error_card(error) for error in live_sim_review_errors)
    return cards


def _context_error_card(error: Mapping[str, Any]) -> dict[str, Any]:
    return _decorate_card(
        {
            "card_id": f"context-error:{error['id']}",
            "card_type": "AI_CONTEXT_WARNING",
            "title": map_card_type_label("AI_CONTEXT_WARNING"),
            "subtitle": str(error.get("task_type") or "context"),
            "status": "CONTEXT_ERROR",
            "severity": "MEDIUM",
            "root_cause_category": "AI_EXECUTION",
            "root_cause": error.get("error_message"),
            "summary": f"컨텍스트 생성 중 오류가 기록되었습니다: {error.get('error_message')}",
            "suggested_checks": [
                "요청한 trade_date 또는 related entity가 현재 projection에 존재하는지 확인하세요.",
                "최근 projection/evaluation error와 context builder 설정을 확인하세요.",
            ],
            "warnings": ["AI context warning은 표시 전용이며 자동 판단에 쓰이지 않습니다."],
            "related_entity_type": error.get("related_entity_type"),
            "related_entity_id": error.get("related_entity_id"),
            "trade_date": error.get("trade_date"),
            "generated_at": error.get("created_at"),
            "ai_request_id": None,
            "ai_insight_id": None,
            "rca_report_id": None,
            "context_id": None,
            "source": "ai_context_build_errors",
            "payload": error.get("payload", {}),
        }
    )


def _rca_error_card(error: Mapping[str, Any]) -> dict[str, Any]:
    return _decorate_card(
        {
            "card_id": f"rca-error:{error['id']}",
            "card_type": "AI_CONTEXT_WARNING",
            "title": "RCA 리포트 오류",
            "subtitle": str(error.get("report_type") or "rca"),
            "status": "FAILED",
            "severity": "HIGH",
            "root_cause_category": "AI_EXECUTION",
            "root_cause": error.get("error_message"),
            "summary": f"RCA report 생성 오류가 기록되었습니다: {error.get('error_message')}",
            "suggested_checks": [
                "RCA 생성 CLI/API 입력값과 관련 projection 상태를 확인하세요.",
                "동일 조건으로 deterministic report가 다시 생성 가능한지 운영 로그를 확인하세요.",
            ],
            "warnings": ["RCA error card는 표시 전용이며 Dashboard에서 재실행하지 않습니다."],
            "related_entity_type": error.get("related_entity_type"),
            "related_entity_id": error.get("related_entity_id"),
            "trade_date": error.get("trade_date"),
            "generated_at": error.get("created_at"),
            "ai_request_id": None,
            "ai_insight_id": None,
            "rca_report_id": None,
            "context_id": None,
            "source": "ai_rca_report_errors",
            "payload": error.get("payload", {}),
        }
    )


def _codex_prompt_error_card(error: Mapping[str, Any]) -> dict[str, Any]:
    return _decorate_card(
        {
            "card_id": f"codex-prompt-error:{error['id']}",
            "card_type": "AI_CONTEXT_WARNING",
            "title": "Codex prompt draft 오류",
            "subtitle": str(error.get("source_type") or "codex_prompt"),
            "status": "FAILED",
            "severity": "HIGH",
            "root_cause_category": "AI_EXECUTION",
            "root_cause": error.get("error_message"),
            "summary": (
                "Codex prompt draft 생성 오류가 기록되었습니다: "
                f"{error.get('error_message')}"
            ),
            "suggested_checks": [
                "입력 report/candidate/trade_date와 관련 context packet 상태를 확인하세요.",
                "deterministic prompt generator는 실패해도 자동 실행/적용을 하지 않습니다.",
            ],
            "warnings": [
                "Codex prompt error card는 표시 전용이며 Dashboard에서 재실행하지 않습니다."
            ],
            "related_entity_type": error.get("related_entity_type"),
            "related_entity_id": error.get("related_entity_id"),
            "trade_date": error.get("trade_date"),
            "generated_at": error.get("created_at"),
            "ai_request_id": None,
            "ai_insight_id": None,
            "rca_report_id": None,
            "context_id": None,
            "source": "ai_codex_prompt_errors",
            "payload": error.get("payload", {}),
        }
    )


def _live_sim_review_error_card(error: Mapping[str, Any]) -> dict[str, Any]:
    return _decorate_card(
        {
            "card_id": f"live-sim-review-error:{error['id']}",
            "card_type": "AI_CONTEXT_WARNING",
            "title": "LIVE_SIM review 오류",
            "subtitle": str(error.get("report_type") or "live_sim_review"),
            "status": "FAILED",
            "severity": "HIGH",
            "root_cause_category": "AI_EXECUTION",
            "root_cause": error.get("error_message"),
            "summary": (
                "LIVE_SIM review report 생성 오류가 기록되었습니다: "
                f"{error.get('error_message')}"
            ),
            "suggested_checks": [
                "CLI/API 입력값과 관련 LIVE_SIM row 존재 여부를 확인하세요.",
                "오류 카드는 표시 전용이며 Dashboard에서 재실행하지 않습니다.",
            ],
            "warnings": ["LIVE_SIM review error card는 주문 액션을 제공하지 않습니다."],
            "related_entity_type": error.get("related_entity_type"),
            "related_entity_id": error.get("related_entity_id"),
            "trade_date": error.get("trade_date"),
            "generated_at": error.get("created_at"),
            "ai_request_id": None,
            "ai_insight_id": None,
            "rca_report_id": None,
            "context_id": None,
            "source": "ai_live_sim_review_errors",
            "payload": error.get("payload", {}),
        }
    )


def _decorate_card(card: dict[str, Any]) -> dict[str, Any]:
    status = str(card.get("status") or "UNKNOWN").upper()
    severity = str(card.get("severity") or "INFO").upper()
    category = card.get("root_cause_category")
    card["status"] = status
    card["severity"] = severity
    card["card_type_label"] = map_card_type_label(card.get("card_type"))
    card["status_label"] = map_ai_status_label(status)
    card["severity_label"] = map_ai_severity_label(severity)
    card["root_cause_category_label"] = map_rca_category_label(category)
    card["observe_only"] = True
    card["review_only"] = True
    card["no_trading_side_effects"] = True
    card["live_real_allowed"] = False
    card["order_action_allowed"] = False
    card["gateway_command_allowed"] = False
    card["actions_available"] = False
    card["execution_controls_available"] = False
    card["read_only_badge"] = "읽기 전용"
    card["no_side_effect_badge"] = "거래 영향 없음"
    return card


def _list_ai_request_failures(
    connection: sqlite3.Connection,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in AI_REQUEST_FAILURE_STATUSES)
    rows = connection.execute(
        f"""
        SELECT *
        FROM ai_requests
        WHERE status IN ({placeholders})
        ORDER BY COALESCE(completed_at, created_at) DESC, request_id DESC
        LIMIT ?
        """,
        (*AI_REQUEST_FAILURE_STATUSES, limit),
    ).fetchall()
    return [ai_request_row_to_dict(row) for row in rows]


def _count_ai_request_failures(connection: sqlite3.Connection) -> int:
    placeholders = ", ".join("?" for _ in AI_REQUEST_FAILURE_STATUSES)
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM ai_requests
        WHERE status IN ({placeholders})
        """,
        AI_REQUEST_FAILURE_STATUSES,
    ).fetchone()
    return int(row["count"])


def _count_context_warnings(connection: sqlite3.Connection) -> int:
    context_row = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_context_build_errors"
    ).fetchone()
    rca_row = connection.execute("SELECT COUNT(*) AS count FROM ai_rca_report_errors").fetchone()
    codex_row = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_codex_prompt_errors"
    ).fetchone()
    live_sim_review_row = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_live_sim_review_errors"
    ).fetchone()
    return (
        int(context_row["count"])
        + int(rca_row["count"])
        + int(codex_row["count"])
        + int(live_sim_review_row["count"])
    )


def _policy_warnings(
    payload: Mapping[str, Any],
    *,
    operator_action: object | None = None,
) -> list[str]:
    warnings = []
    if contains_forbidden_action(payload):
        warnings.append(
            "POLICY_WARNING: 금지된 action-like output이 감지되어 실행 액션은 노출하지 않습니다."
        )
    if operator_action is not None and _normalize_operator_action(operator_action) not in {
        _normalize_operator_action(action) for action in READONLY_OPERATOR_ACTIONS
    }:
        warnings.append(
            "POLICY_WARNING: operator_action이 read-only 허용 목록 밖이라 REVIEW_ONLY로 표시합니다."
        )
    return warnings


def _safe_operator_action(value: object) -> dict[str, str | object]:
    raw = value
    normalized = _normalize_operator_action(value)
    allowed = {_normalize_operator_action(action) for action in READONLY_OPERATOR_ACTIONS}
    safe_value = str(value).strip().upper() if normalized in allowed else "REVIEW_ONLY"
    return {
        "raw": raw,
        "value": safe_value,
        "label": map_readonly_operator_action(safe_value),
    }


def _failure_severity(status: str) -> str:
    if status in {"POLICY_REJECTED", "AI_OUTPUT_INVALID", "CONTEXT_ERROR", "FAILED"}:
        return "HIGH"
    if status in {"TIMEOUT", "MODEL_ERROR", "CLIENT_UNAVAILABLE", "AI_UNAVAILABLE"}:
        return "MEDIUM"
    if status in {"AI_DISABLED", "API_KEY_MISSING"}:
        return "LOW"
    return "MEDIUM"


def _failure_summary(status: str, error_message: object | None) -> str:
    detail = f" 상세: {error_message}" if error_message else ""
    messages = {
        "AI_DISABLED": "AI Sidecar가 비활성화되어 insight가 생성되지 않았습니다.",
        "API_KEY_MISSING": (
            "API key가 없어 AI insight는 생성되지 않았지만 deterministic RCA report는 유지됩니다."
        ),
        "CLIENT_UNAVAILABLE": (
            "OpenAI client 또는 모델 설정을 사용할 수 없어 요청이 중단되었습니다."
        ),
        "TIMEOUT": "모델 응답 시간이 초과되어 insight가 저장되지 않았습니다.",
        "MODEL_ERROR": "모델 호출 오류가 발생해 insight가 저장되지 않았습니다.",
        "AI_OUTPUT_INVALID": (
            "모델 출력이 schema validation을 통과하지 못해 insight로 저장되지 않았습니다."
        ),
        "POLICY_REJECTED": (
            "금지된 action-like output이 감지되어 insight 저장이 차단되었습니다."
        ),
        "CONTEXT_ERROR": "AI context 생성 또는 로드 중 오류가 발생했습니다.",
        "FAILED": "AI 요청이 실패 상태로 종료되었습니다.",
    }
    return f"{messages.get(status, 'AI 요청이 정상 insight로 저장되지 않았습니다.')}{detail}"


def _failure_suggested_checks(status: str) -> list[str]:
    common = ["AI 요청 오류는 표시 전용이며 Strategy/Risk/OMS 자동 입력이 아닙니다."]
    if status == "API_KEY_MISSING":
        return [*common, "AI_SIDECAR_OPENAI_API_KEY_ENV와 실제 API key 환경 변수를 확인하세요."]
    if status in {"AI_OUTPUT_INVALID", "POLICY_REJECTED"}:
        return [*common, "prompt registry와 structured output schema validation 결과를 확인하세요."]
    if status in {"TIMEOUT", "MODEL_ERROR", "CLIENT_UNAVAILABLE"}:
        return [*common, "모델 설정, 네트워크, SDK 설치 상태를 확인하세요."]
    if status == "CONTEXT_ERROR":
        return [*common, "context packet 생성 조건과 관련 projection row를 확인하세요."]
    return common


def _sort_cards(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        cards,
        key=lambda card: str(card.get("generated_at") or ""),
        reverse=True,
    )


def _strings(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    return []


def _unique(values: list[str]) -> list[str]:
    return [*dict.fromkeys(value for value in values if value.strip())]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_operator_action(value: object) -> str:
    return "" if value is None else str(value).strip().upper()


def _bounded_limit(limit: int, settings: Settings) -> int:
    return min(max(int(limit), 1), settings.dashboard_max_limit)
