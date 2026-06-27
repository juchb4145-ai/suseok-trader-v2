from __future__ import annotations

from typing import Any

from domain.ai_sidecar.live_sim_review import LiveSimReviewReportType
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from services.ai_sidecar.live_sim_review_store import (
    get_live_sim_review_report,
    list_live_sim_review_errors,
    list_live_sim_review_reports,
)
from services.ai_sidecar.live_sim_review_workflows import (
    build_live_sim_incident_review,
    build_live_sim_order_review,
    build_live_sim_order_reviews_for_trade_date,
    build_live_sim_reconcile_review,
    build_live_sim_session_review,
)
from services.ai_sidecar.openai_client import AISidecarModelClient, get_openai_client_status
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/ai-sidecar/live-sim-review")
_MODEL_CLIENT_OVERRIDE: AISidecarModelClient | None = None
_BODY = Body(default=None)


@router.get("/status")
def live_sim_review_status() -> dict[str, Any]:
    settings = load_settings()
    client_status = get_openai_client_status(settings)
    return {
        "available": True,
        "openai_client_available": client_status["available"],
        "deterministic_reports_available": True,
        "ai_run_default": False,
        "supported_report_types": [item.value for item in LiveSimReviewReportType],
        "review_only": True,
        "order_action_allowed": False,
        "gateway_command_allowed": False,
        "live_real_allowed": False,
    }


@router.post("/session/{trade_date}", dependencies=[Depends(require_local_token)])
def create_session_review(
    trade_date: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_live_sim_session_review(
            connection,
            trade_date,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    return _result_response(result)


@router.post("/order/{live_sim_order_id}", dependencies=[Depends(require_local_token)])
def create_order_review(
    live_sim_order_id: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_live_sim_order_review(
            connection,
            live_sim_order_id,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    return _result_response(result)


@router.post("/reconcile/{reconcile_id}", dependencies=[Depends(require_local_token)])
def create_reconcile_review(
    reconcile_id: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_live_sim_reconcile_review(
            connection,
            reconcile_id,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    return _result_response(result)


@router.post("/incident", dependencies=[Depends(require_local_token)])
def create_incident_review(
    body: dict[str, Any] | None = _BODY,
    trade_date: str | None = Query(default=None),
    related_entity_id: str | None = Query(default=None),
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    body = body or {}
    target_trade_date = str(body.get("trade_date") or trade_date or "").strip() or None
    target_related_id = (
        str(body.get("related_entity_id") or related_entity_id or "").strip() or None
    )
    target_run_ai = bool(body.get("run_ai", run_ai))
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_live_sim_incident_review(
            connection,
            trade_date=target_trade_date,
            related_entity_id=target_related_id,
            run_ai=target_run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    return _result_response(result)


@router.post("/orders/batch", dependencies=[Depends(require_local_token)])
def create_order_review_batch(
    body: dict[str, Any] | None = _BODY,
    trade_date: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    body = body or {}
    target_trade_date = str(body.get("trade_date") or trade_date or "").strip() or None
    target_status = str(body.get("status") or status or "").strip() or None
    target_limit = min(max(int(body.get("limit", limit)), 1), 100)
    target_run_ai = bool(body.get("run_ai", run_ai))
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        results = build_live_sim_order_reviews_for_trade_date(
            connection,
            target_trade_date,
            status=target_status,
            limit=target_limit,
            run_ai=target_run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    reports = [result.report.to_dict() for result in results if result.ok and result.report]
    errors = [
        {"error_message": result.error_message, "warnings": list(result.warnings)}
        for result in results
        if not result.ok
    ]
    return {
        "ok": not errors,
        "count": len(reports),
        "error_count": len(errors),
        "limit": target_limit,
        "run_ai": target_run_ai,
        "reports": [_report_response(report) for report in reports],
        "errors": errors,
        "review_only": True,
        "order_action_allowed": False,
        "live_real_allowed": False,
    }


@router.get("/reports")
def reports(
    report_type: str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_live_sim_review_reports(
            connection,
            report_type=report_type,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            status=status,
            limit=limit,
        )
    finally:
        connection.close()
    return {"reports": rows}


@router.get("/reports/{review_id}")
def report_detail(
    review_id: str,
    include_sections: bool = Query(default=True),
    include_links: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        report = get_live_sim_review_report(
            connection,
            review_id,
            include_sections=include_sections,
            include_links=include_links,
        )
    finally:
        connection.close()
    if report is None:
        raise HTTPException(status_code=404, detail="LIVE_SIM review report not found")
    return report


@router.get("/errors")
def errors(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_live_sim_review_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": rows}


def _result_response(result) -> dict[str, Any]:
    if not result.ok or result.report is None:
        raise HTTPException(
            status_code=400,
            detail=result.error_message or "LIVE_SIM review build failed",
        )
    return _report_response(result.report.to_dict())


def _report_response(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_id": report["review_id"],
        "report_type": report["report_type"],
        "status": report["status"],
        "root_cause_category": report["root_cause_category"],
        "summary": report["summary"],
        "ai_request_id": report.get("ai_request_id"),
        "ai_insight_id": report.get("ai_insight_id"),
        "review_only": True,
        "order_action_allowed": False,
        "gateway_command_allowed": False,
        "live_real_allowed": False,
    }
