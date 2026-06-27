from __future__ import annotations

from typing import Any

from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from domain.ai_sidecar.schemas import insight_row_to_dict
from fastapi import APIRouter, HTTPException, Query
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
from services.config import load_settings
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/ai-sidecar")


@router.get("/status")
def status() -> dict[str, Any]:
    settings = load_settings()
    return {
        "enabled": settings.ai_sidecar_enabled,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "model": settings.ai_sidecar_model,
        "allowed_tasks": get_allowed_tasks(),
        "forbidden_actions": get_forbidden_actions(),
        "openai_client_available": False,
        "execution_api_available": False,
        "context_builder_available": True,
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
def insights(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = connection.execute(
            """
            SELECT
                insight_id,
                request_id,
                task_type,
                trade_date,
                related_entity_type,
                related_entity_id,
                summary,
                root_cause,
                severity,
                operator_action,
                schema_version,
                created_at
            FROM ai_insights
            ORDER BY created_at DESC, insight_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        connection.close()

    return {"insights": [insight_row_to_dict(row) for row in rows]}
