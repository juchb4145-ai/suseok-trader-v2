from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import normalize_value


def save_scoring_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    provider: str,
    model: str,
    status: str,
    candidate_count: int,
    selected_count: int,
    prompt_hash: str | None,
    raw_response_hash: str | None,
    summary: str | None,
    no_trade_reason: str | None,
    error_message: str | None = None,
    validation_error: str | None = None,
    raw_response_json: Mapping[str, Any] | None = None,
    external_call_enabled: bool = False,
    external_call_attempted: bool = False,
    latency_ms: float | None = None,
    request_id: str | None = None,
    token_usage_json: Mapping[str, Any] | None = None,
    raw_response_stored: bool = False,
    prompt_redacted: bool = True,
    prompt_truncated: bool = False,
    error_category: str | None = None,
    fallback_provider: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO ai_candidate_scoring_runs (
            run_id,
            trade_date,
            provider,
            model,
            status,
            candidate_count,
            selected_count,
            prompt_hash,
            raw_response_hash,
            raw_response_json,
            external_call_enabled,
            external_call_attempted,
            latency_ms,
            request_id,
            token_usage_json,
            raw_response_stored,
            prompt_redacted,
            prompt_truncated,
            error_category,
            fallback_provider,
            summary,
            no_trade_reason,
            error_message,
            validation_error,
            completed_at,
            live_sim_only,
            advisory_only,
            no_order_side_effects
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, datetime('now'), 1, 1, 1
        )
        ON CONFLICT(run_id) DO UPDATE SET
            status = excluded.status,
            candidate_count = excluded.candidate_count,
            selected_count = excluded.selected_count,
            prompt_hash = excluded.prompt_hash,
            raw_response_hash = excluded.raw_response_hash,
            raw_response_json = excluded.raw_response_json,
            external_call_enabled = excluded.external_call_enabled,
            external_call_attempted = excluded.external_call_attempted,
            latency_ms = excluded.latency_ms,
            request_id = excluded.request_id,
            token_usage_json = excluded.token_usage_json,
            raw_response_stored = excluded.raw_response_stored,
            prompt_redacted = excluded.prompt_redacted,
            prompt_truncated = excluded.prompt_truncated,
            error_category = excluded.error_category,
            fallback_provider = excluded.fallback_provider,
            summary = excluded.summary,
            no_trade_reason = excluded.no_trade_reason,
            error_message = excluded.error_message,
            validation_error = excluded.validation_error,
            completed_at = datetime('now'),
            live_sim_only = 1,
            advisory_only = 1,
            no_order_side_effects = 1
        """,
        (
            run_id,
            trade_date,
            provider,
            model,
            status,
            int(candidate_count),
            int(selected_count),
            prompt_hash,
            raw_response_hash,
            _json_dumps(raw_response_json) if raw_response_json is not None else None,
            1 if external_call_enabled else 0,
            1 if external_call_attempted else 0,
            latency_ms,
            request_id,
            _json_dumps(token_usage_json) if token_usage_json is not None else None,
            1 if raw_response_stored else 0,
            1 if prompt_redacted else 0,
            1 if prompt_truncated else 0,
            error_category,
            fallback_provider,
            summary,
            no_trade_reason,
            error_message,
            validation_error,
        ),
    )


def save_candidate_scores(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    candidates: Sequence[Mapping[str, Any]],
    advisory: Mapping[str, Any],
) -> None:
    candidate_by_code = {str(candidate.get("code")): candidate for candidate in candidates}
    selected = set(str(code) for code in advisory.get("selected", []))
    scores = dict(advisory.get("score", {}))
    confidences = dict(advisory.get("confidence", {}))
    analyses = dict(advisory.get("analysis", {}))
    avoids = dict(advisory.get("avoid", {}))
    flags = dict(advisory.get("candidate_flags", {}))
    for code, candidate in candidate_by_code.items():
        connection.execute(
            """
            INSERT INTO ai_candidate_scores (
                run_id,
                code,
                candidate_instance_id,
                order_plan_id,
                score,
                confidence,
                selected,
                analysis,
                avoid_reason,
                flags_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                code,
                candidate.get("candidate_instance_id"),
                candidate.get("order_plan_id"),
                scores.get(code),
                confidences.get(code),
                1 if code in selected else 0,
                analyses.get(code),
                avoids.get(code),
                _json_dumps(flags.get(code, [])),
            ),
        )


def save_risk_reward_suggestions(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    candidates: Sequence[Mapping[str, Any]],
    advisory: Mapping[str, Any],
) -> None:
    candidate_by_code = {str(candidate.get("code")): candidate for candidate in candidates}
    risk_reward = dict(advisory.get("risk_reward", {}))
    for code, suggestion in risk_reward.items():
        if code not in candidate_by_code or not isinstance(suggestion, Mapping):
            continue
        candidate = candidate_by_code[code]
        connection.execute(
            """
            INSERT INTO ai_risk_reward_suggestions (
                run_id,
                code,
                order_plan_id,
                stop_loss_pct,
                take_profit_pct,
                trailing_stop_pct,
                max_hold_sec,
                clamped,
                clamp_reason_codes_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                code,
                candidate.get("order_plan_id"),
                suggestion.get("stop_loss_pct"),
                suggestion.get("take_profit_pct"),
                suggestion.get("trailing_stop_pct"),
                suggestion.get("max_hold_sec"),
                1 if suggestion.get("clamped") else 0,
                _json_dumps(suggestion.get("clamp_reason_codes", [])),
            ),
        )


def save_advisory_error(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    trade_date: str | None,
    error_type: str,
    error_message: str,
    payload: Mapping[str, Any] | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO ai_advisory_errors (
            run_id,
            trade_date,
            error_type,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, trade_date, error_type, error_message, _json_dumps(payload or {})),
    )


def get_latest_run(connection: sqlite3.Connection) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_candidate_scoring_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else _run_row_to_dict(row)


def get_run(connection: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM ai_candidate_scoring_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    result = _run_row_to_dict(row)
    result["scores"] = list_scores_for_run(connection, run_id)
    result["risk_reward_suggestions"] = list_risk_reward_for_run(connection, run_id)
    return result


def list_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_candidate_scoring_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_run_row_to_dict(row) for row in rows]


def list_latest_scores(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> dict[str, Any]:
    latest = get_latest_run(connection)
    if latest is None:
        return {
            "run": None,
            "scores": [],
            "risk_reward_suggestions": [],
            "advisory_only": True,
            "no_order_side_effects": True,
        }
    run_id = str(latest["run_id"])
    return {
        "run": latest,
        "scores": list_scores_for_run(connection, run_id, limit=limit),
        "risk_reward_suggestions": list_risk_reward_for_run(connection, run_id, limit=limit),
        "advisory_only": True,
        "no_order_side_effects": True,
    }


def list_scores_for_run(
    connection: sqlite3.Connection,
    run_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_candidate_scores
        WHERE run_id = ?
        ORDER BY selected DESC, score DESC, confidence DESC, code ASC
        LIMIT ?
        """,
        (run_id, _bounded_limit(limit)),
    ).fetchall()
    return [_score_row_to_dict(row) for row in rows]


def list_risk_reward_for_run(
    connection: sqlite3.Connection,
    run_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_risk_reward_suggestions
        WHERE run_id = ?
        ORDER BY code ASC
        LIMIT ?
        """,
        (run_id, _bounded_limit(limit)),
    ).fetchall()
    return [_risk_reward_row_to_dict(row) for row in rows]


def list_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM ai_advisory_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_error_row_to_dict(row) for row in rows]


def build_status(connection: sqlite3.Connection, *, settings: Any) -> dict[str, Any]:
    latest = get_latest_run(connection)
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_advisory_errors"
    ).fetchone()
    invalid_schema_count = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_advisory_errors WHERE error_type = 'INVALID_SCHEMA'"
    ).fetchone()
    return {
        "enabled": settings.ai_candidate_scorer_enabled,
        "provider": settings.ai_candidate_scorer_provider,
        "model": settings.ai_candidate_scorer_model,
        "external_enabled": settings.ai_external_llm_enabled,
        "external_provider": settings.ai_external_llm_provider,
        "external_model": settings.ai_external_llm_model,
        "external_allow_network": settings.ai_external_llm_allow_network,
        "external_fail_open": settings.ai_external_llm_fail_open,
        "external_call_attempted": bool(
            latest and latest.get("external_call_attempted")
        ),
        "latest_latency_ms": None if latest is None else latest.get("latency_ms"),
        "latest_token_usage": {} if latest is None else latest.get("token_usage", {}),
        "fallback_used": bool(latest and latest.get("fallback_provider")),
        "strict_json": settings.ai_candidate_scorer_require_strict_json,
        "store_raw_response": settings.ai_candidate_scorer_store_raw_response,
        "external_store_response": settings.ai_external_llm_store_response,
        "fail_open": settings.ai_candidate_scorer_fail_open,
        "max_candidates": settings.ai_candidate_scorer_max_candidates,
        "min_score": settings.ai_candidate_scorer_min_score,
        "min_confidence": settings.ai_candidate_scorer_min_confidence,
        "latest_run": latest,
        "error_count": int(error_count["count"] if error_count else 0),
        "invalid_schema_error_count": int(
            invalid_schema_count["count"] if invalid_schema_count else 0
        ),
        "advisory_only": True,
        "no_order_side_effects": True,
        "allow_order_actions": False,
    }


def _run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["advisory_only"] = bool(item["advisory_only"])
    item["no_order_side_effects"] = bool(item["no_order_side_effects"])
    item["external_call_enabled"] = bool(item.get("external_call_enabled"))
    item["external_call_attempted"] = bool(item.get("external_call_attempted"))
    item["raw_response_stored"] = bool(item.get("raw_response_stored"))
    item["prompt_redacted"] = bool(item.get("prompt_redacted"))
    item["prompt_truncated"] = bool(item.get("prompt_truncated"))
    item["raw_response_json"] = _json_object(item.get("raw_response_json"))
    item["token_usage"] = _json_object(item.pop("token_usage_json", None))
    return item


def _score_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["selected"] = bool(item["selected"])
    item["flags"] = _json_array(item.pop("flags_json"))
    return item


def _risk_reward_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["clamped"] = bool(item["clamped"])
    item["clamp_reason_codes"] = _json_array(item.pop("clamp_reason_codes_json"))
    return item


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["payload"] = _json_object(item.pop("payload_json"))
    return item


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value]
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
