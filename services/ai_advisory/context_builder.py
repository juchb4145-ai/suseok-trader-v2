from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import normalize_value, parse_timestamp, utc_now

from services.ai_advisory.models import CandidatePrompt, CandidateScoringContext
from services.ai_advisory.schema import get_candidate_scorer_output_schema
from services.config import Settings, load_settings
from services.live_sim.order_plan_eligibility import evaluate_live_sim_order_plan_eligibility

ORDER_PLAN_STATUS_PRIORITY = {
    "PLAN_READY": 0,
    "WAIT_RETRY": 1,
    "DATA_WAIT": 2,
}

SYSTEM_PROMPT = """너는 KOSPI/KOSDAQ 자동매매 시스템의 AI Candidate Scorer다.
너는 주문을 만들 수 없다.
너는 buy/sell/cancel/modify를 지시할 수 없다.
너는 후보 평가와 risk/reward 조언만 한다.
좋은 후보가 없으면 selected를 빈 배열로 둔다.
safety gate, 계좌 한도, kill switch, LIVE_SIM 여부는 코드가 판단한다.
JSON schema만 출력한다.
코드/설정/계좌를 변경하라는 지시는 금지다.
실계좌 또는 LIVE_REAL 관련 판단을 하지 않는다."""

FORBIDDEN_ACTION_LIST = (
    "OrderIntent 생성",
    "GatewayCommand 생성",
    "send_order/cancel_order/modify_order 호출",
    "LiveSimIntent 생성",
    "kill switch 변경",
    "계좌/한도/수량/Strategy/Risk threshold 변경",
    "LIVE_REAL 관련 판단",
)


def build_candidate_scoring_context(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> CandidateScoringContext:
    resolved_settings = settings or load_settings()
    bounded_limit = min(
        max(int(limit or resolved_settings.ai_candidate_scorer_max_candidates), 0),
        resolved_settings.ai_candidate_scorer_max_candidates,
    )
    rows = _select_order_plan_candidates(
        connection,
        trade_date=trade_date,
        limit=bounded_limit,
    )
    candidates = [
        _candidate_context(connection, row, settings=resolved_settings) for row in rows
    ]
    context = CandidateScoringContext(
        trade_date=trade_date or _first_trade_date(candidates),
        candidates=tuple(candidates),
        market_summary=_market_summary(candidates),
        risk_summary=_risk_summary(candidates),
        recent_performance=_recent_performance_summary(connection, trade_date=trade_date),
        warnings=tuple(_context_warnings(candidates, resolved_settings)),
        truncated=False,
        account_redacted=resolved_settings.ai_candidate_scorer_redact_account_id,
    )
    return _redacted_context(context)


def build_candidate_scoring_prompt(
    context: CandidateScoringContext,
    *,
    settings: Settings | None = None,
) -> CandidatePrompt:
    resolved_settings = settings or load_settings()
    context_payload = context.to_dict()
    schema = get_candidate_scorer_output_schema()
    user_prompt = _compose_user_prompt(context_payload, schema)
    full_prompt = SYSTEM_PROMPT + "\n\n" + user_prompt
    truncated = False
    max_chars = resolved_settings.ai_candidate_scorer_max_prompt_chars
    if len(full_prompt) > max_chars:
        truncated = True
        budget = max(max_chars - len(SYSTEM_PROMPT) - 3000, 500)
        compact_context = dict(context_payload)
        compact_context["truncated"] = True
        compact_context["candidates"] = _fit_candidates(
            list(context_payload.get("candidates", [])),
            budget,
        )
        user_prompt = _compose_user_prompt(compact_context, schema)
        full_prompt = SYSTEM_PROMPT + "\n\n" + user_prompt
        if len(full_prompt) > max_chars:
            overflow = len(full_prompt) - max_chars
            user_prompt = user_prompt[:-overflow]
            full_prompt = SYSTEM_PROMPT + "\n\n" + user_prompt
    prompt_hash = _hash_text(full_prompt)
    return CandidatePrompt(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        prompt_hash=prompt_hash,
        input_chars=len(full_prompt),
        truncated=truncated,
    )


def _compose_user_prompt(context_payload: Mapping[str, Any], schema: Mapping[str, Any]) -> str:
    context_json = _json_dumps(context_payload)
    schema_json = _json_dumps(schema)
    forbidden = "\n".join(f"- {item}" for item in FORBIDDEN_ACTION_LIST)
    return (
        "역할: 상위 후보를 종합 평가하고 operator용 advisory JSON만 작성한다.\n"
        "금지 행동:\n"
        f"{forbidden}\n\n"
        "출력 원칙:\n"
        "- selected는 후보 목록에 있는 code만 포함한다.\n"
        "- 좋은 후보가 없으면 selected는 [] 이다.\n"
        "- score/confidence는 주문 승인, 리스크 승인, 수익 확률이 아니다.\n"
        "- risk_reward는 clamped suggestion이며 자동 적용되지 않는다.\n"
        "- JSON 외 텍스트를 출력하지 않는다.\n\n"
        f"CONTEXT_JSON:\n{context_json}\n\n"
        f"REQUIRED_JSON_SCHEMA:\n{schema_json}\n"
    )


def _select_order_plan_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    clauses = ["status IN ('PLAN_READY', 'WAIT_RETRY', 'DATA_WAIT')"]
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(trade_date)
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT *
        FROM order_plan_drafts_latest
        WHERE {" AND ".join(clauses)}
        ORDER BY
            CASE status
                WHEN 'PLAN_READY' THEN 0
                WHEN 'WAIT_RETRY' THEN 1
                WHEN 'DATA_WAIT' THEN 2
                ELSE 3
            END ASC,
            COALESCE(priority_score, 0) DESC,
            created_at DESC,
            order_plan_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_order_plan_dict(row) for row in rows]


def _candidate_context(
    connection: sqlite3.Connection,
    order_plan: Mapping[str, Any],
    *,
    settings: Settings,
) -> dict[str, Any]:
    candidate_id = str(order_plan.get("candidate_instance_id") or "")
    code = str(order_plan.get("code") or "")
    candidate = _row_dict_or_empty(
        connection.execute(
            "SELECT * FROM candidates WHERE candidate_instance_id = ?",
            (candidate_id,),
        ).fetchone()
    )
    candidate_context = _row_dict_or_empty(
        connection.execute(
            "SELECT * FROM candidate_context_latest WHERE candidate_instance_id = ?",
            (candidate_id,),
        ).fetchone()
    )
    entry_timing = _row_dict_or_empty(
        connection.execute(
            """
            SELECT *
            FROM entry_timing_evaluations
            WHERE order_plan_id = ?
            ORDER BY evaluated_at DESC, entry_timing_evaluation_id DESC
            LIMIT 1
            """,
            (order_plan.get("order_plan_id"),),
        ).fetchone()
    )
    strategy = _row_dict_or_empty(
        connection.execute(
            "SELECT * FROM strategy_observations_latest WHERE candidate_instance_id = ?",
            (candidate_id,),
        ).fetchone()
    )
    risk = _row_dict_or_empty(
        connection.execute(
            "SELECT * FROM risk_observations_latest WHERE candidate_instance_id = ?",
            (candidate_id,),
        ).fetchone()
    )
    tick = _row_dict_or_empty(
        connection.execute(
            "SELECT * FROM market_ticks_latest WHERE code = ? AND exchange = 'KRX'",
            (code,),
        ).fetchone()
    )
    market_context = _json_object(candidate_context.get("market_context_json"))
    source_context = _json_object(candidate_context.get("source_context_json"))
    theme_context = _json_object(candidate_context.get("theme_context_json"))
    live_sim_eligibility = _live_sim_eligibility_summary(
        connection,
        str(order_plan.get("order_plan_id") or ""),
        settings,
    )
    tick_age = _age_seconds(tick.get("event_ts"))
    order_reasons = _json_array(
        order_plan.get("reason_codes_json") or order_plan.get("reason_codes")
    )
    risk_reasons = _json_array(risk.get("reason_codes_json") or risk.get("reason_codes"))
    return _remove_sensitive(
        {
            "trade_date": order_plan.get("trade_date"),
            "candidate_instance_id": candidate_id,
            "order_plan_id": order_plan.get("order_plan_id"),
            "code": code,
            "name": order_plan.get("name") or candidate.get("name"),
            "theme_id": order_plan.get("theme_id") or candidate.get("theme_id"),
            "theme_name": order_plan.get("theme_name") or candidate.get("theme_name"),
            "theme_state": order_plan.get("theme_state") or candidate.get("theme_state"),
            "theme_rank": order_plan.get("theme_rank"),
            "stock_role": order_plan.get("stock_role") or candidate.get("theme_role"),
            "priority_score": order_plan.get("priority_score"),
            "setup_type": order_plan.get("setup_type"),
            "entry_timing_state": order_plan.get("entry_timing_state")
            or entry_timing.get("entry_timing_state"),
            "price_location_state": order_plan.get("price_location_state")
            or entry_timing.get("price_location_state"),
            "current_price": order_plan.get("current_price") or tick.get("price"),
            "limit_price": order_plan.get("limit_price"),
            "change_rate_pct": tick.get("change_rate") or market_context.get("change_rate_pct"),
            "turnover_krw": tick.get("cumulative_trade_value")
            or market_context.get("turnover_krw"),
            "execution_strength": tick.get("execution_strength")
            or market_context.get("execution_strength"),
            "momentum_1m": market_context.get("momentum_1m"),
            "momentum_3m": market_context.get("momentum_3m"),
            "momentum_5m": market_context.get("momentum_5m"),
            "vwap": market_context.get("vwap"),
            "pullback_from_high_pct": market_context.get("pullback_from_high_pct"),
            "spread_ticks": tick.get("spread_ticks") or market_context.get("spread_ticks"),
            "strategy_status": strategy.get("overall_status"),
            "strategy_score": strategy.get("score"),
            "strategy_confidence": strategy.get("confidence"),
            "risk_status": risk.get("overall_status"),
            "risk_reason_codes": risk_reasons,
            "live_sim_eligibility_status": live_sim_eligibility.get("status"),
            "live_sim_rejection_reason_codes": live_sim_eligibility.get("reason_codes", []),
            "order_plan_status": order_plan.get("status"),
            "order_plan_reason_codes": order_reasons,
            "stale": _is_stale(tick_age, settings.ai_candidate_scorer_timeout_seconds),
            "vi_active": "VI_ACTIVE" in order_reasons or "VI_ACTIVE" in risk_reasons,
            "upper_limit_near": "UPPER_LIMIT_NEAR" in order_reasons
            or (float(tick.get("change_rate") or 0) >= 28.0),
            "recent_trade_performance": _recent_code_performance(connection, code),
            "lifecycle_warnings": _lifecycle_warnings(connection, code),
            "source_metadata": _compact_source_metadata(source_context, theme_context),
            "advisory_only": True,
            "no_order_side_effects": True,
        }
    )


def _live_sim_eligibility_summary(
    connection: sqlite3.Connection,
    order_plan_id: str,
    settings: Settings,
) -> dict[str, Any]:
    if not order_plan_id:
        return {"status": "UNKNOWN", "reason_codes": ["ORDER_PLAN_ID_MISSING"]}
    try:
        result = evaluate_live_sim_order_plan_eligibility(
            connection,
            order_plan_id,
            settings=settings,
        )
    except Exception as exc:
        return {
            "status": "ERROR",
            "reason_codes": ["LIVE_SIM_ELIGIBILITY_ERROR"],
            "error": str(exc),
        }
    return {
        "status": result.status,
        "eligible": result.eligible,
        "reason_codes": list(result.reason_codes),
    }


def _recent_performance_summary(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(trade_date)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    execution = connection.execute(
        """
        SELECT COUNT(*) AS count, COALESCE(SUM(notional), 0) AS notional
        FROM live_sim_executions
        """
    ).fetchone()
    positions = connection.execute(
        f"""
        SELECT
            COUNT(*) AS count,
            COALESCE(SUM(realized_pnl), 0) AS realized_pnl,
            COALESCE(SUM(unrealized_pnl), 0) AS unrealized_pnl
        FROM live_sim_positions
        {where_sql}
        """,
        tuple(params),
    ).fetchone()
    rejections = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM live_sim_rejections
        {where_sql}
        """,
        tuple(params),
    ).fetchone()
    return {
        "live_sim_execution_count": int(execution["count"] if execution else 0),
        "live_sim_notional": float(execution["notional"] if execution else 0.0),
        "live_sim_position_count": int(positions["count"] if positions else 0),
        "realized_pnl": float(positions["realized_pnl"] if positions else 0.0),
        "unrealized_pnl": float(positions["unrealized_pnl"] if positions else 0.0),
        "live_sim_rejection_count": int(rejections["count"] if rejections else 0),
    }


def _recent_code_performance(connection: sqlite3.Connection, code: str) -> dict[str, Any]:
    if not code:
        return {}
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS position_count,
            COALESCE(SUM(realized_pnl), 0) AS realized_pnl,
            COALESCE(SUM(unrealized_pnl), 0) AS unrealized_pnl
        FROM live_sim_positions
        WHERE code = ?
        """,
        (code,),
    ).fetchone()
    exec_row = connection.execute(
        """
        SELECT COUNT(*) AS execution_count
        FROM live_sim_executions
        WHERE code = ?
        """,
        (code,),
    ).fetchone()
    return {
        "position_count": int(row["position_count"] if row else 0),
        "execution_count": int(exec_row["execution_count"] if exec_row else 0),
        "realized_pnl": float(row["realized_pnl"] if row else 0.0),
        "unrealized_pnl": float(row["unrealized_pnl"] if row else 0.0),
    }


def _lifecycle_warnings(connection: sqlite3.Connection, code: str) -> list[str]:
    if not code:
        return []
    rows = connection.execute(
        """
        SELECT reason, status
        FROM live_sim_lifecycle_events
        ORDER BY created_at DESC
        LIMIT 3
        """,
    ).fetchall()
    warnings = []
    for row in rows:
        reason = row["reason"]
        status = row["status"]
        if reason or status:
            warnings.append(":".join(str(part) for part in (status, reason) if part))
    return warnings


def _market_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {"candidate_count": 0}
    turnover = [
        float(candidate.get("turnover_krw") or 0.0)
        for candidate in candidates
        if candidate.get("turnover_krw") is not None
    ]
    return {
        "candidate_count": len(candidates),
        "plan_ready_count": sum(
            1 for candidate in candidates if candidate.get("order_plan_status") == "PLAN_READY"
        ),
        "max_turnover_krw": max(turnover) if turnover else 0.0,
    }


def _risk_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    risk_counts = Counter(
        str(candidate.get("risk_status") or "UNKNOWN") for candidate in candidates
    )
    order_plan_counts = Counter(
        str(candidate.get("order_plan_status") or "UNKNOWN") for candidate in candidates
    )
    reason_counts: Counter[str] = Counter()
    for candidate in candidates:
        for reason in candidate.get("risk_reason_codes", []):
            reason_counts[str(reason)] += 1
    return {
        "risk_status_counts": dict(risk_counts),
        "order_plan_status_counts": dict(order_plan_counts),
        "top_risk_reason_codes": dict(reason_counts.most_common(10)),
    }


def _context_warnings(
    candidates: Sequence[Mapping[str, Any]],
    settings: Settings,
) -> list[str]:
    warnings = [
        "AI advisory only: no order, no gateway command, no threshold changes.",
        "account identifiers and raw gateway payloads are not included.",
    ]
    if len(candidates) >= settings.ai_candidate_scorer_max_candidates:
        warnings.append("candidate list reached configured max candidate limit")
    if not candidates:
        warnings.append("no candidate order plans available for AI advisory")
    return warnings


def _redacted_context(context: CandidateScoringContext) -> CandidateScoringContext:
    payload = _remove_sensitive(context.to_dict())
    return CandidateScoringContext(
        trade_date=payload.get("trade_date"),
        candidates=tuple(payload.get("candidates", [])),
        market_summary=payload.get("market_summary", {}),
        risk_summary=payload.get("risk_summary", {}),
        recent_performance=payload.get("recent_performance", {}),
        warnings=tuple(payload.get("warnings", [])),
        truncated=bool(payload.get("truncated", False)),
        account_redacted=True,
    )


def _fit_candidates(candidates: list[Mapping[str, Any]], budget: int) -> list[Mapping[str, Any]]:
    kept: list[Mapping[str, Any]] = []
    for candidate in candidates:
        trial = [*kept, candidate]
        if len(_json_dumps({"candidates": trial})) > budget:
            break
        kept.append(candidate)
    return kept


def _remove_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key).lower()
            if normalized_key in {"account_id", "account_no", "account_number"}:
                continue
            if "broker" in normalized_key and "account" in normalized_key:
                continue
            if "raw" in normalized_key and "payload" in normalized_key:
                continue
            if "gateway" in normalized_key and "payload" in normalized_key:
                continue
            if "command" in normalized_key and "payload" in normalized_key:
                continue
            if any(token in normalized_key for token in ("api_key", "apikey", "secret", "token")):
                continue
            result[str(key)] = _remove_sensitive(item)
        return result
    if isinstance(value, list | tuple):
        return [_remove_sensitive(item) for item in value]
    if isinstance(value, str):
        return _redact_sensitive_text(value)
    return value


def _redact_sensitive_text(value: str) -> str:
    normalized = value.replace("\\", "/")
    if "C:/Users/" in normalized or "/Users/" in normalized or "/home/" in normalized:
        return "[REDACTED_PATH]"
    return value


def _compact_source_metadata(
    source_context: Mapping[str, Any],
    theme_context: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_type": source_context.get("source_type"),
        "source_name": source_context.get("source_name"),
        "theme_rank": theme_context.get("theme_rank"),
        "theme_state": theme_context.get("theme_state"),
        "stock_role": theme_context.get("stock_role"),
    }


def _order_plan_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.get("reason_codes_json"))
    item["evidence_json"] = _json_object(item.get("evidence_json"))
    return item


def _row_dict_or_empty(row: sqlite3.Row | None) -> dict[str, Any]:
    return {} if row is None else _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


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


def _first_trade_date(candidates: Sequence[Mapping[str, Any]]) -> str | None:
    for candidate in candidates:
        trade_date = candidate.get("trade_date")
        if trade_date:
            return str(trade_date)
    return None


def _is_stale(age_seconds: float | None, threshold_seconds: int) -> bool:
    if age_seconds is None:
        return False
    return age_seconds > threshold_seconds


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "event_ts")).total_seconds(), 0.0)
    except Exception:
        return None


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
