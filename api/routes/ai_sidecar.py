from __future__ import annotations

from typing import Any

from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from fastapi import APIRouter, Depends, HTTPException, Query
from services.ai_sidecar.context_builder import (
    build_context_builder_status,
    build_context_packet,
)
from services.ai_sidecar.context_store import (
    get_context_packet,
    list_context_build_errors,
    list_context_packets,
    save_context_build_error,
    save_context_packet,
)
from services.ai_sidecar.openai_client import (
    AISidecarModelClient,
    get_openai_client_status,
)
from services.ai_sidecar.request_store import (
    get_ai_insight,
    get_ai_request,
    list_ai_insights,
    list_ai_requests,
)
from services.ai_sidecar.runner import run_ai_sidecar_task
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/ai-sidecar")
_MODEL_CLIENT_OVERRIDE: AISidecarModelClient | None = None


@router.get("/status")
def status() -> dict[str, Any]:
    settings = load_settings()
    client_status = get_openai_client_status(settings)
    return {
        "enabled": settings.ai_sidecar_enabled,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "model": settings.ai_sidecar_model,
        "allowed_tasks": get_allowed_tasks(),
        "forbidden_actions": get_forbidden_actions(),
        "openai_client_available": client_status["available"],
        "execution_api_available": True,
        "context_builder_available": True,
        "tools_enabled": False,
        "order_tools_enabled": False,
    }


@router.get("/execution/status")
def execution_status() -> dict[str, Any]:
    settings = load_settings()
    client_status = get_openai_client_status(settings)
    return {
        "enabled": settings.ai_sidecar_enabled,
        "openai_client_available": client_status["available"],
        "execution_api_available": True,
        "context_builder_available": True,
        "model": settings.ai_sidecar_model,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "tools_enabled": False,
        "order_tools_enabled": False,
        "responses_api_enabled": settings.ai_sidecar_use_responses_api,
        "structured_outputs_enabled": settings.ai_sidecar_structured_outputs_enabled,
        "strict_schema": settings.ai_sidecar_strict_schema,
        "supported_tasks": get_allowed_tasks(),
        "client": client_status,
    }


@router.get("/tasks")
def tasks() -> dict[str, list[str]]:
    return {"tasks": get_allowed_tasks()}


@router.get("/context/status")
def context_status() -> dict[str, Any]:
    settings = load_settings()
    return build_context_builder_status(settings)


@router.get("/context/preview")
def context_preview(
    task_type: str = Query(...),
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=200),
    persist: bool = False,
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packet = build_context_packet(
            connection,
            task_type,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            limit=limit,
            settings=settings,
        )
        if persist or settings.ai_sidecar_context_persist_preview:
            save_context_packet(connection, packet)
        return packet.to_dict()
    except Exception as exc:
        save_context_build_error(
            connection,
            task_type=task_type,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            error_message=str(exc),
            payload={"endpoint": "context_preview"},
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/context/packets")
def context_packets(
    task_type: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packets = list_context_packets(
            connection,
            task_type=task_type,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"packets": packets}


@router.get("/context/packets/{context_id}")
def context_packet_detail(context_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packet = get_context_packet(connection, context_id)
    finally:
        connection.close()
    if packet is None:
        raise HTTPException(status_code=404, detail="context packet not found")
    return packet


@router.get("/context/errors")
def context_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_context_build_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.get("/context/candidate/{candidate_instance_id}")
def candidate_context_preview(candidate_instance_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packet = build_context_packet(
            connection,
            "CANDIDATE_BLOCK_RCA",
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
            settings=settings,
        )
    finally:
        connection.close()
    return packet.to_dict()


@router.get("/context/theme/{theme_id}")
def theme_context_preview(theme_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packet = build_context_packet(
            connection,
            "THEME_BRIEF",
            related_entity_type="theme",
            related_entity_id=theme_id,
            settings=settings,
        )
    finally:
        connection.close()
    return packet.to_dict()


@router.get("/context/no-trade/{trade_date}")
def no_trade_context_preview(trade_date: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        packet = build_context_packet(
            connection,
            "NO_TRADE_RCA",
            trade_date=trade_date,
            settings=settings,
        )
    finally:
        connection.close()
    return packet.to_dict()


@router.get("/insights")
def insights(
    task_type: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_ai_insights(
            connection,
            task_type=task_type,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            limit=limit,
        )
    finally:
        connection.close()

    return {"insights": rows}


@router.get("/insights/{insight_id}")
def insight_detail(insight_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        insight = get_ai_insight(connection, insight_id)
    finally:
        connection.close()
    if insight is None:
        raise HTTPException(status_code=404, detail="AI insight not found")
    return insight


@router.post("/run", dependencies=[Depends(require_local_token)])
def run_ai_sidecar(body: dict[str, Any]) -> dict[str, Any]:
    return _run_from_body(body)


@router.post("/run/candidate/{candidate_instance_id}", dependencies=[Depends(require_local_token)])
def run_candidate_block_rca(candidate_instance_id: str) -> dict[str, Any]:
    return _run_from_body(
        {
            "task_type": "CANDIDATE_BLOCK_RCA",
            "related_entity_type": "candidate",
            "related_entity_id": candidate_instance_id,
            "persist_context": True,
        }
    )


@router.post("/run/no-trade/{trade_date}", dependencies=[Depends(require_local_token)])
def run_no_trade_rca(trade_date: str) -> dict[str, Any]:
    return _run_from_body(
        {
            "task_type": "NO_TRADE_RCA",
            "trade_date": trade_date,
            "persist_context": True,
        }
    )


@router.post("/run/theme/{theme_id}", dependencies=[Depends(require_local_token)])
def run_theme_brief(theme_id: str) -> dict[str, Any]:
    return _run_from_body(
        {
            "task_type": "THEME_BRIEF",
            "related_entity_type": "theme",
            "related_entity_id": theme_id,
            "persist_context": True,
        }
    )


@router.get("/requests")
def requests(
    task_type: str | None = None,
    status: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_ai_requests(
            connection,
            task_type=task_type,
            status=status,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"requests": rows}


@router.get("/requests/{request_id}")
def request_detail(request_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        request = get_ai_request(connection, request_id)
    finally:
        connection.close()
    if request is None:
        raise HTTPException(status_code=404, detail="AI request not found")
    return request


def _run_from_body(body: dict[str, Any]) -> dict[str, Any]:
    settings = load_settings()
    task_type = body.get("task_type")
    if not isinstance(task_type, str) or not task_type.strip():
        raise HTTPException(status_code=422, detail="task_type is required")
    connection = open_connection(settings.trading_db_path)
    try:
        result = run_ai_sidecar_task(
            connection,
            task_type,
            trade_date=body.get("trade_date"),
            related_entity_type=body.get("related_entity_type"),
            related_entity_id=body.get("related_entity_id"),
            context_id=body.get("context_id"),
            persist_context=bool(body.get("persist_context", True)),
            model_client=_MODEL_CLIENT_OVERRIDE,
            settings=settings,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        connection.close()
    return result.to_dict()
