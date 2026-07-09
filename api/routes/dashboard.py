from __future__ import annotations

import time
from threading import Lock
from typing import Any, Literal

from fastapi import APIRouter, Query
from services.config import load_settings
from services.dashboard_ai_explanations import (
    build_ai_explanation_cards,
    build_ai_explanation_status,
    filter_ai_explanation_cards,
)
from services.dashboard_service import (
    FAST_DASHBOARD_DEFAULT_SECTIONS,
    build_dashboard_errors,
    build_dashboard_snapshot,
    build_dashboard_snapshot_sections,
    build_dashboard_status,
    parse_dashboard_sections,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/dashboard")
_SUMMARY_CACHE_LOCK = Lock()
_SUMMARY_CACHE: dict[tuple[str, str, int, str, bool], tuple[float, dict[str, Any]]] = {}


@router.get("/snapshot")
def dashboard_snapshot(
    detail: Literal["summary", "full"] = "summary",
    limit: int | None = Query(default=None, ge=1, le=200),
    sections: str | None = Query(default=None),
    fast: bool = Query(default=False),
    timeout_budget_ms: int | None = Query(default=None, ge=100, le=30000),
) -> dict[str, Any]:
    settings = load_settings()
    requested_sections = (
        parse_dashboard_sections(sections)
        if settings.dashboard_snapshot_sections_enabled
        else None
    )
    use_fast_path = bool(fast or requested_sections)
    if use_fast_path and requested_sections is None:
        requested_sections = set(FAST_DASHBOARD_DEFAULT_SECTIONS)

    cache_limit = _dashboard_cache_limit(settings, limit, fast=use_fast_path)
    sections_key = _dashboard_sections_cache_key(requested_sections)
    cache_key = (
        str(settings.trading_db_path),
        detail,
        cache_limit,
        sections_key,
        use_fast_path,
    )
    if detail == "summary":
        now = time.monotonic()
        with _SUMMARY_CACHE_LOCK:
            cached = _SUMMARY_CACHE.get(cache_key)
            ttl_sec = _dashboard_summary_cache_ttl(settings, fast=use_fast_path)
            if cached is not None and now - cached[0] <= ttl_sec:
                return cached[1]

    connection = open_connection(settings.trading_db_path)
    try:
        if use_fast_path:
            snapshot = build_dashboard_snapshot_sections(
                connection,
                settings,
                detail=detail,
                limit=cache_limit,
                sections=requested_sections or set(FAST_DASHBOARD_DEFAULT_SECTIONS),
                timeout_budget_ms=(
                    timeout_budget_ms
                    if timeout_budget_ms is not None
                    else settings.dashboard_snapshot_fast_timeout_budget_ms
                ),
            )
        else:
            snapshot = build_dashboard_snapshot(
                connection,
                settings,
                detail=detail,
                limit=limit,
            )
    finally:
        connection.close()

    if detail == "summary":
        with _SUMMARY_CACHE_LOCK:
            _SUMMARY_CACHE[cache_key] = (time.monotonic(), snapshot)
        return snapshot

    return snapshot


def _dashboard_sections_cache_key(sections: set[str] | None) -> str:
    if not sections:
        return ""
    return ",".join(sorted(sections))


def _dashboard_cache_limit(settings, limit: int | None, *, fast: bool = False) -> int:
    if limit is None and fast:
        value = settings.dashboard_snapshot_fast_default_limit
    else:
        value = settings.dashboard_snapshot_default_limit if limit is None else int(limit)
    return min(max(value, 1), settings.dashboard_max_limit)


def _dashboard_summary_cache_ttl(settings, *, fast: bool = False) -> float:
    if fast:
        return max(float(settings.dashboard_snapshot_fast_cache_ttl_sec), 0.0)
    return min(max(float(settings.dashboard_refresh_sec) - 1.0, 1.0), 5.0)


@router.get("/status")
def dashboard_status() -> dict[str, Any]:
    settings = load_settings()
    return build_dashboard_status(settings)


@router.get("/funnel")
def dashboard_funnel() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = build_dashboard_snapshot(connection, settings, detail="summary", limit=1)
    finally:
        connection.close()
    return snapshot["pipeline_summary"]


@router.get("/errors")
def dashboard_errors(
    limit: int | None = Query(default=None, ge=1, le=200),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_dashboard_errors(connection, settings=settings, limit=limit)
    finally:
        connection.close()


@router.get("/ai-explanations")
def dashboard_ai_explanations(
    limit: int = Query(default=20, ge=1, le=100),
    card_type: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = build_ai_explanation_cards(
            connection,
            settings,
            limit=100,
            candidate_limit=100,
        )
        cards = filter_ai_explanation_cards(
            payload["cards"],
            card_type=card_type,
            severity=severity,
            status=status,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
        )[:limit]
        return {
            "available": True,
            "execution_controls_available": False,
            "run_buttons_available": False,
            "cards": cards,
            "card_count": len(cards),
            "status_counts": payload["status_counts"],
            "severity_counts": payload["severity_counts"],
            "warnings": payload["warnings"],
        }
    finally:
        connection.close()


@router.get("/ai-explanations/status")
def dashboard_ai_explanations_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        status = build_ai_explanation_status(connection, settings)
    finally:
        connection.close()
    return status | {
        "cards_enabled": True,
        "warnings": [
            "AI 설명 카드는 읽기 전용입니다.",
            "AI/RCA 결과는 Strategy/Risk/OMS 자동 입력이 아닙니다.",
            "Dashboard에는 AI 실행 버튼이 없습니다.",
        ],
    }


@router.get("/ai-explanations/candidate/{candidate_instance_id}")
def dashboard_ai_explanations_for_candidate(
    candidate_instance_id: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = build_ai_explanation_cards(
            connection,
            settings,
            limit=100,
            candidate_limit=100,
        )
        cards = filter_ai_explanation_cards(
            payload["cards"],
            related_entity_type="candidate",
            related_entity_id=candidate_instance_id,
        )[:limit]
        return {
            "available": True,
            "execution_controls_available": False,
            "run_buttons_available": False,
            "candidate_instance_id": candidate_instance_id,
            "cards": cards,
            "card_count": len(cards),
        }
    finally:
        connection.close()


@router.get("/ai-explanations/no-trade/{trade_date}")
def dashboard_ai_explanations_for_no_trade(
    trade_date: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = build_ai_explanation_cards(
            connection,
            settings,
            limit=100,
            candidate_limit=100,
        )
        cards = filter_ai_explanation_cards(payload["cards"], trade_date=trade_date)[:limit]
        return {
            "available": True,
            "execution_controls_available": False,
            "run_buttons_available": False,
            "trade_date": trade_date,
            "cards": cards,
            "card_count": len(cards),
        }
    finally:
        connection.close()
