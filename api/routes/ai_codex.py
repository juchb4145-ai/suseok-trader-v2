from __future__ import annotations

from typing import Any

from domain.ai_sidecar.codex_prompt import (
    AICodexPromptSourceType,
    AICodexPromptTargetArea,
)
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response
from services.ai_sidecar.codex_prompt_generator import (
    build_codex_prompt_from_candidate,
    build_codex_prompt_from_no_trade,
    build_codex_prompt_from_ops_incident,
    build_codex_prompt_from_rca_report,
    build_safety_review_prompt,
)
from services.ai_sidecar.codex_prompt_store import (
    get_codex_prompt_draft,
    list_codex_prompt_drafts,
    list_codex_prompt_errors,
)
from services.ai_sidecar.openai_client import AISidecarModelClient, get_openai_client_status
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/ai-sidecar/codex-prompts")
_MODEL_CLIENT_OVERRIDE: AISidecarModelClient | None = None
_OPS_BODY = Body(default=None)


@router.get("/status")
def codex_prompt_status() -> dict[str, Any]:
    settings = load_settings()
    client_status = get_openai_client_status(settings)
    return {
        "available": True,
        "deterministic_generator_available": True,
        "ai_assist_available": client_status["available"],
        "auto_apply_allowed": False,
        "github_write_allowed": False,
        "codex_execution_allowed": False,
        "no_trading_side_effects": True,
        "supported_sources": [source.value for source in AICodexPromptSourceType],
        "supported_target_areas": [target.value for target in AICodexPromptTargetArea],
    }


@router.post("/from-rca/{report_id}", dependencies=[Depends(require_local_token)])
def create_from_rca(
    report_id: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    result = _build_with_connection(
        lambda connection: build_codex_prompt_from_rca_report(
            connection,
            report_id,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
        )
    )
    return _draft_response(result)


@router.post("/from-candidate/{candidate_instance_id}", dependencies=[Depends(require_local_token)])
def create_from_candidate(
    candidate_instance_id: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    result = _build_with_connection(
        lambda connection: build_codex_prompt_from_candidate(
            connection,
            candidate_instance_id,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
        )
    )
    return _draft_response(result)


@router.post("/from-no-trade/{trade_date}", dependencies=[Depends(require_local_token)])
def create_from_no_trade(
    trade_date: str,
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    result = _build_with_connection(
        lambda connection: build_codex_prompt_from_no_trade(
            connection,
            trade_date,
            run_ai=run_ai,
            model_client=_MODEL_CLIENT_OVERRIDE,
        )
    )
    return _draft_response(result)


@router.post("/from-ops-incident", dependencies=[Depends(require_local_token)])
def create_from_ops_incident(
    body: dict[str, Any] | None = _OPS_BODY,
    trade_date: str | None = Query(default=None),
    related_entity_id: str | None = Query(default=None),
    run_ai: bool = Query(default=False),
) -> dict[str, Any]:
    body = body or {}
    result = _build_with_connection(
        lambda connection: build_codex_prompt_from_ops_incident(
            connection,
            trade_date=str(body.get("trade_date") or trade_date or "").strip() or None,
            related_entity_id=str(
                body.get("related_entity_id") or related_entity_id or ""
            ).strip()
            or None,
            run_ai=bool(body.get("run_ai", run_ai)),
            model_client=_MODEL_CLIENT_OVERRIDE,
        )
    )
    return _draft_response(result)


@router.post("/safety-review", dependencies=[Depends(require_local_token)])
def create_safety_review() -> dict[str, Any]:
    result = _build_with_connection(lambda connection: build_safety_review_prompt(connection))
    return _draft_response(result)


@router.get("")
def list_drafts(
    source_type: str | None = None,
    target_area: str | None = None,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        drafts = list_codex_prompt_drafts(
            connection,
            source_type=source_type,
            target_area=target_area,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            status=status,
            limit=limit,
        )
    finally:
        connection.close()
    return {"drafts": drafts}


@router.get("/errors")
def errors(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_codex_prompt_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": rows}


@router.get("/{draft_id}")
def draft_detail(
    draft_id: str,
    include_sections: bool = Query(default=True),
    include_links: bool = Query(default=True),
) -> dict[str, Any]:
    draft = _get_draft_or_404(
        draft_id,
        include_sections=include_sections,
        include_links=include_links,
    )
    return draft


@router.get("/{draft_id}/text", response_model=None)
def draft_text(draft_id: str, as_json: bool = Query(default=False)):
    draft = _get_draft_or_404(draft_id, include_sections=False, include_links=False)
    prompt_text = str(draft["prompt_text"])
    if as_json:
        return {"draft_id": draft_id, "prompt_text": prompt_text}
    return Response(content=prompt_text, media_type="text/plain; charset=utf-8")


def _build_with_connection(builder):
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return builder(connection)
    finally:
        connection.close()


def _draft_response(result) -> dict[str, Any]:
    if not result.ok or result.draft is None:
        raise HTTPException(
            status_code=400,
            detail=result.error_message or "Codex prompt draft build failed",
        )
    draft = result.draft
    return {
        "draft_id": draft.draft_id,
        "status": draft.status.value,
        "title": draft.title,
        "target_area": draft.target_area.value,
        "run_ai": draft.run_ai,
        "ai_request_id": draft.ai_request_id,
        "ai_insight_id": draft.ai_insight_id,
        "auto_apply_allowed": False,
        "github_write_allowed": False,
        "codex_execution_allowed": False,
        "no_trading_side_effects": True,
    }


def _get_draft_or_404(
    draft_id: str,
    *,
    include_sections: bool,
    include_links: bool,
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        draft = get_codex_prompt_draft(
            connection,
            draft_id,
            include_sections=include_sections,
            include_links=include_links,
        )
    finally:
        connection.close()
    if draft is None:
        raise HTTPException(status_code=404, detail="Codex prompt draft not found")
    return draft
