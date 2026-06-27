from __future__ import annotations

from typing import Any

from domain.ai_sidecar.rca import AIRCAReportType
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from services.ai_sidecar.openai_client import AISidecarModelClient, get_openai_client_status
from services.ai_sidecar.rca_report_store import (
    get_rca_report,
    list_rca_report_errors,
    list_rca_reports,
)
from services.ai_sidecar.rca_workflows import (
    build_candidate_block_rca_report,
    build_candidate_block_rca_reports_for_trade_date,
    build_no_trade_rca_report,
)
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/ai-sidecar/rca")
_MODEL_CLIENT_OVERRIDE: AISidecarModelClient | None = None
_BATCH_BODY = Body(default=None)


@router.get("/status")
def rca_status() -> dict[str, Any]:
    settings = load_settings()
    client_status = get_openai_client_status(settings)
    return {
        "available": True,
        "openai_client_available": client_status["available"],
        "deterministic_reports_available": True,
        "ai_run_default": False,
        "supported_report_types": [
            AIRCAReportType.NO_TRADE_RCA.value,
            AIRCAReportType.CANDIDATE_BLOCK_RCA.value,
        ],
        "no_trading_side_effects": True,
    }


@router.post("/no-trade/{trade_date}", dependencies=[Depends(require_local_token)])
def create_no_trade_rca(
    trade_date: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_no_trade_rca_report(
            connection,
            trade_date,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    if not result.ok or result.report is None:
        raise HTTPException(status_code=400, detail=result.error_message or "RCA build failed")
    return _report_response(result.report.to_dict())


@router.post("/candidate/{candidate_instance_id}", dependencies=[Depends(require_local_token)])
def create_candidate_rca(
    candidate_instance_id: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = build_candidate_block_rca_report(
            connection,
            candidate_instance_id,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    finally:
        connection.close()
    if not result.ok or result.report is None:
        raise HTTPException(status_code=400, detail=result.error_message or "RCA build failed")
    return _report_response(result.report.to_dict())


@router.post("/candidates/batch", dependencies=[Depends(require_local_token)])
def create_candidate_rca_batch(
    body: dict[str, Any] | None = _BATCH_BODY,
    trade_date: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    body = body or {}
    states = _optional_list(body.get("states"))
    risk_statuses = _optional_list(body.get("risk_statuses"))
    target_trade_date = str(body.get("trade_date") or trade_date or "").strip() or None
    target_limit = min(int(body.get("limit", limit)), 100)
    target_run_ai = bool(body.get("run_ai", run_ai))

    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        results = build_candidate_block_rca_reports_for_trade_date(
            connection,
            target_trade_date,
            states=states,
            risk_statuses=risk_statuses,
            limit=target_limit,
            run_ai=target_run_ai,
            settings=settings,
            model_client=_MODEL_CLIENT_OVERRIDE,
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
        "ok": len(errors) == 0,
        "count": len(reports),
        "error_count": len(errors),
        "limit": target_limit,
        "run_ai": target_run_ai,
        "reports": [_report_response(report) for report in reports],
        "errors": errors,
        "no_trading_side_effects": True,
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
        rows = list_rca_reports(
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


@router.get("/reports/{report_id}")
def report_detail(
    report_id: str,
    include_sections: bool = Query(default=True),
    include_links: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        report = get_rca_report(
            connection,
            report_id,
            include_sections=include_sections,
            include_links=include_links,
        )
    finally:
        connection.close()
    if report is None:
        raise HTTPException(status_code=404, detail="RCA report not found")
    return report


@router.get("/errors")
def errors(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_rca_report_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": rows}


def _report_response(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_id": report["report_id"],
        "status": report["status"],
        "root_cause_category": report["root_cause_category"],
        "summary": report["summary"],
        "ai_request_id": report.get("ai_request_id"),
        "ai_insight_id": report.get("ai_insight_id"),
        "no_trading_side_effects": True,
    }


def _optional_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return None
