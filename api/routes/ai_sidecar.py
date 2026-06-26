from __future__ import annotations

from typing import Any

from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from domain.ai_sidecar.schemas import insight_row_to_dict
from fastapi import APIRouter, Query
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
    }


@router.get("/tasks")
def tasks() -> dict[str, list[str]]:
    return {"tasks": get_allowed_tasks()}


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
