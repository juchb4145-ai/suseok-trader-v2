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
    build_dashboard_errors,
    build_dashboard_snapshot,
    build_dashboard_status,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/dashboard")
_SUMMARY_CACHE_LOCK = Lock()
_SUMMARY_CACHE: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}


@router.get("/snapshot")
def dashboard_snapshot(
    detail: Literal["summary", "full"] = "summary",
    limit: int | None = Query(default=None, ge=1, le=200),
) -> dict[str, Any]:
    settings = load_settings()
    if detail == "summary":
        cache_limit = _dashboard_cache_limit(settings, limit)
        cache_key = (str(settings.trading_db_path), cache_limit)
        now = time.monotonic()
        with _SUMMARY_CACHE_LOCK:
            cached = _SUMMARY_CACHE.get(cache_key)
            if cached is not None and now - cached[0] <= _dashboard_summary_cache_ttl(
                settings
            ):
                return cached[1]
            connection = open_connection(settings.trading_db_path)
            try:
                snapshot = build_dashboard_snapshot(
                    connection,
                    settings,
                    detail=detail,
                    limit=limit,
                )
            finally:
                connection.close()
            _SUMMARY_CACHE[cache_key] = (time.monotonic(), snapshot)
            return snapshot

    connection = open_connection(settings.trading_db_path)
    try:
        return build_dashboard_snapshot(
            connection,
            settings,
            detail=detail,
            limit=limit,
        )
    finally:
        connection.close()


def _dashboard_cache_limit(settings, limit: int | None) -> int:
    value = settings.dashboard_snapshot_default_limit if limit is None else int(limit)
    return min(max(value, 1), settings.dashboard_max_limit)


def _dashboard_summary_cache_ttl(settings) -> float:
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
