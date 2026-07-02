from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from storage.gateway_command_store import canonical_json

from services.config import Settings, candidate_timezone, load_settings
from services.entry_timing.engine import EntryTimingEngine
from services.entry_timing.models import (
    EntryTimingEvaluation,
    EntryTimingInput,
    OrderPlanDraft,
    OrderPlanStatus,
)
from services.entry_timing.order_plan import OrderPlanDraftBuilder
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    immediate_transaction,
    runtime_execution_lock,
)
from services.theme_leadership import ThemeLeadershipService


@dataclass(frozen=True, kw_only=True)
class EntryTimingEvaluationRunResult:
    trade_date: str | None
    candidate_count: int = 0
    evaluated_count: int = 0
    plan_ready_count: int = 0
    wait_retry_count: int = 0
    data_wait_count: int = 0
    no_plan_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    observe_only: bool = True
    not_order_intent: bool = True
    evaluations: Sequence[EntryTimingEvaluation] = field(default_factory=tuple)
    order_plan_drafts: Sequence[OrderPlanDraft] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "candidate_count": self.candidate_count,
            "evaluated_count": self.evaluated_count,
            "plan_ready_count": self.plan_ready_count,
            "wait_retry_count": self.wait_retry_count,
            "data_wait_count": self.data_wait_count,
            "no_plan_count": self.no_plan_count,
            "error_count": self.error_count,
            "status": self.status,
            "observe_only": True,
            "not_order_intent": True,
            "evaluations": [evaluation.to_dict() for evaluation in self.evaluations],
            "order_plan_drafts": [draft.to_dict() for draft in self.order_plan_drafts],
        }


def evaluate_entry_timing(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    candidate_instance_id: str | None = None,
    limit: int | None = None,
    write_order_plan_drafts: bool | None = None,
    settings: Settings | None = None,
    manage_run_lock: bool = True,
) -> EntryTimingEvaluationRunResult:
    with runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        details={"run_type": "entry_timing_evaluation", "trade_date": trade_date},
        manage_lock=manage_run_lock,
    ):
        with immediate_transaction(connection):
            return _evaluate_entry_timing(
                connection,
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                limit=limit,
                write_order_plan_drafts=write_order_plan_drafts,
                settings=settings,
            )


def _evaluate_entry_timing(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    candidate_instance_id: str | None = None,
    limit: int | None = None,
    write_order_plan_drafts: bool | None = None,
    settings: Settings | None = None,
) -> EntryTimingEvaluationRunResult:
    resolved_settings = settings or load_settings()
    if not resolved_settings.entry_timing_enabled:
        return EntryTimingEvaluationRunResult(trade_date=trade_date, status="DISABLED")

    target_trade_date = _resolve_trade_date(trade_date, resolved_settings)
    bounded_limit = _bounded_limit(limit or resolved_settings.entry_timing_max_plans_per_run)
    should_write = (
        resolved_settings.entry_timing_write_order_plan_drafts
        if write_order_plan_drafts is None
        else bool(write_order_plan_drafts)
    )
    rows = _candidate_rows_for_evaluation(
        connection,
        trade_date=target_trade_date,
        candidate_instance_id=candidate_instance_id,
        limit=bounded_limit,
    )
    inputs: list[EntryTimingInput] = []
    if rows:
        for row in rows:
            inputs.append(
                load_entry_timing_input(
                    connection,
                    row["candidate_instance_id"],
                    settings=resolved_settings,
                )
            )
    elif candidate_instance_id is None:
        inputs.extend(
            _watchset_inputs(
                connection,
                trade_date=target_trade_date,
                limit=bounded_limit,
                settings=resolved_settings,
            )
        )

    engine = EntryTimingEngine(settings=resolved_settings)
    builder = OrderPlanDraftBuilder(settings=resolved_settings)
    evaluations: list[EntryTimingEvaluation] = []
    drafts: list[OrderPlanDraft] = []
    error_count = 0
    for item in inputs:
        try:
            evaluation = engine.evaluate(item)
            final_status, status_reasons = builder.resolve_status(item, evaluation)
            draft = builder.build(item, evaluation)
            if draft is not None:
                evaluation = evaluation.with_status(
                    draft.status,
                    order_plan_id=draft.order_plan_id,
                    reason_codes=draft.reason_codes,
                )
            elif final_status is not evaluation.status:
                evaluation = evaluation.with_status(
                    final_status,
                    reason_codes=_dedupe([*evaluation.reason_codes, *status_reasons]),
                )
            if should_write:
                save_entry_timing_evaluation(connection, evaluation)
                if draft is not None:
                    save_order_plan_draft(connection, draft)
            evaluations.append(evaluation)
            if draft is not None:
                drafts.append(draft)
        except Exception as exc:
            error_count += 1
            _record_evaluation_error(
                connection,
                candidate_instance_id=item.candidate_instance_id,
                code=item.code,
                error_message=str(exc),
                payload=item.to_dict(),
            )
    if should_write:
        connection.commit()

    return EntryTimingEvaluationRunResult(
        trade_date=target_trade_date,
        candidate_count=len(inputs),
        evaluated_count=len(evaluations),
        plan_ready_count=sum(
            1 for evaluation in evaluations if evaluation.status is OrderPlanStatus.PLAN_READY
        ),
        wait_retry_count=sum(
            1 for evaluation in evaluations if evaluation.status is OrderPlanStatus.WAIT_RETRY
        ),
        data_wait_count=sum(
            1 for evaluation in evaluations if evaluation.status is OrderPlanStatus.DATA_WAIT
        ),
        no_plan_count=sum(
            1
            for evaluation in evaluations
            if evaluation.status
            in {
                OrderPlanStatus.NO_PLAN,
                OrderPlanStatus.BLOCKED_CHASE,
                OrderPlanStatus.BLOCKED_OVERHEAT,
                OrderPlanStatus.BLOCKED_STALE,
                OrderPlanStatus.BLOCKED_RISK,
            }
        ),
        error_count=error_count,
        status="COMPLETED_WITH_ERRORS" if error_count else "COMPLETED",
        evaluations=evaluations,
        order_plan_drafts=drafts,
    )


def load_entry_timing_input(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    settings: Settings | None = None,
) -> EntryTimingInput:
    resolved_settings = settings or load_settings()
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    candidate = _candidate_row(connection, normalized_id)
    if candidate is None:
        raise ValueError(f"candidate not found: {normalized_id}")

    code = validate_stock_code(candidate["code"])
    context = _candidate_context(connection, normalized_id)
    condition_fusion = _condition_fusion_from_context(context)
    if not condition_fusion.get("present"):
        condition_fusion = _condition_fusion_from_table(connection, candidate)
    source_meta = _candidate_theme_source_metadata(connection, normalized_id)
    tick = _latest_tick(connection, code)
    latest_1m = _latest_bar(connection, code, 60)
    latest_3m = _latest_bar(connection, code, 180)
    latest_5m = _latest_bar(connection, code, 300)
    strategy = _latest_strategy(connection, normalized_id)
    risk = _latest_risk(connection, normalized_id)
    theme = _theme_context(connection, candidate["theme_id"], code)
    fallback_watchset = _watchset_match(
        connection,
        code=code,
        theme_id=candidate["theme_id"],
        settings=resolved_settings,
        enabled=not _has_theme_metadata(source_meta, candidate, theme),
    )

    theme_meta = _merge_theme_metadata(source_meta, candidate, context, theme, fallback_watchset)
    market = _merge_market_data(candidate, context, tick, latest_1m, latest_3m, latest_5m, theme)
    reason_codes = _dedupe(
        [
            *_json_array(source_meta.get("reason_codes")),
            *_json_array(theme_meta.get("reason_codes")),
            *_json_array(condition_fusion.get("condition_fusion_reason_codes")),
            *_json_array(candidate["reason_codes_json"]),
        ]
    )
    risk_reasons = _json_array(risk["reason_codes_json"]) if risk is not None else []
    stale = bool(market.get("stale"))
    if market.get("tick_age_sec") is not None:
        stale = (
            stale
            or float(market["tick_age_sec"])
            > resolved_settings.entry_timing_stale_max_seconds
        )
    return EntryTimingInput(
        trade_date=candidate["trade_date"],
        candidate_instance_id=normalized_id,
        code=code,
        name=candidate["name"],
        theme_id=_first_text(theme_meta.get("theme_id"), candidate["theme_id"]),
        theme_name=_first_text(theme_meta.get("theme_name"), candidate["theme_name"]),
        theme_state=_first_text(theme_meta.get("theme_state"), candidate["theme_state"]),
        theme_rank=_first_int(theme_meta.get("theme_rank")),
        stock_role=_first_text(theme_meta.get("stock_role"), candidate["theme_role"]),
        theme_priority_score=_first_number(theme_meta.get("priority_score")),
        current_price=_first_number(market.get("current_price")),
        prev_close=_first_number(market.get("prev_close")),
        open_price=_first_number(market.get("open_price")),
        day_high=_first_number(market.get("day_high")),
        day_low=_first_number(market.get("day_low")),
        change_rate_pct=_first_number(market.get("change_rate_pct")),
        turnover_krw=_first_number(market.get("turnover_krw")),
        execution_strength=_first_number(market.get("execution_strength")),
        momentum_1m=_first_number(market.get("momentum_1m")),
        momentum_3m=_first_number(market.get("momentum_3m")),
        momentum_5m=_first_number(market.get("momentum_5m")),
        vwap=_first_number(market.get("vwap")),
        pullback_from_high_pct=_first_number(market.get("pullback_from_high_pct")),
        spread_ticks=_first_int(market.get("spread_ticks")),
        stale=stale,
        vi_active="VI_ACTIVE" in reason_codes,
        upper_limit_near=(
            "UPPER_LIMIT_NEAR" in reason_codes
            or (_first_number(market.get("change_rate_pct")) or 0.0) >= 28.0
        ),
        theme_reason_codes=reason_codes,
        candidate_state=candidate["state"],
        strategy_observation_status=(
            strategy["overall_status"] if strategy is not None else None
        ),
        strategy_setup_type=(
            strategy["primary_setup_type"] if strategy is not None else None
        ),
        strategy_score=_first_number(strategy["score"] if strategy is not None else None),
        strategy_confidence=_first_number(
            strategy["confidence"] if strategy is not None else None
        ),
        risk_observation_status=risk["overall_status"] if risk is not None else None,
        risk_reason_codes=risk_reasons,
        condition_fusion_priority_score=_first_number(
            condition_fusion.get("condition_fusion_priority_score")
        ),
        active_condition_roles=_json_array(condition_fusion.get("active_condition_roles")),
        condition_risk_blocked=bool(condition_fusion.get("condition_risk_blocked")),
        condition_fusion_reason_codes=_json_array(
            condition_fusion.get("condition_fusion_reason_codes")
        ),
        condition_names=_string_array(condition_fusion.get("condition_names")),
        condition_latest_hit_at=_first_text(condition_fusion.get("condition_latest_hit_at")),
        observed_at=_first_text(
            tick["event_ts"] if tick is not None else None,
            context.get("refreshed_at"),
            candidate["last_seen_at"],
        )
        or datetime_to_wire(utc_now()),
        best_bid=_first_number(tick["best_bid"] if tick is not None else None),
        best_ask=_first_number(tick["best_ask"] if tick is not None else None),
        tick_age_sec=_first_number(market.get("tick_age_sec")),
        source="candidate",
        raw_context={
            "candidate": _row_to_dict(candidate),
            "candidate_context": context,
            "condition_fusion": condition_fusion,
            "source_metadata": source_meta,
            "theme_metadata": theme_meta,
            "latest_tick": _row_to_dict(tick) if tick is not None else {},
            "latest_bars": {
                "60": _row_to_dict(latest_1m) if latest_1m is not None else {},
                "180": _row_to_dict(latest_3m) if latest_3m is not None else {},
                "300": _row_to_dict(latest_5m) if latest_5m is not None else {},
            },
            "strategy_observation": _row_to_dict(strategy) if strategy is not None else {},
            "risk_observation": _row_to_dict(risk) if risk is not None else {},
            "watchset_fallback_used": bool(fallback_watchset),
        },
    )


def save_entry_timing_evaluation(
    connection: sqlite3.Connection,
    evaluation: EntryTimingEvaluation,
) -> None:
    data = evaluation.to_dict()
    connection.execute(
        """
        INSERT INTO entry_timing_evaluations (
            entry_timing_evaluation_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            evaluated_at,
            setup_type,
            entry_timing_state,
            price_location_state,
            status,
            order_plan_id,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["entry_timing_evaluation_id"],
            data["trade_date"],
            data["candidate_instance_id"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["setup_type"],
            data["entry_timing_state"],
            data["price_location_state"],
            data["status"],
            data["order_plan_id"],
            _json_dumps(data["reason_codes"]),
            canonical_json(data["evidence_json"]),
            1,
            1,
        ),
    )


def save_order_plan_draft(connection: sqlite3.Connection, draft: OrderPlanDraft) -> None:
    data = draft.to_dict()
    values = _draft_values(data)
    connection.execute(
        """
        INSERT INTO order_plan_drafts (
            order_plan_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            side,
            status,
            setup_type,
            entry_timing_state,
            price_location_state,
            theme_id,
            theme_name,
            theme_state,
            theme_rank,
            stock_role,
            priority_score,
            current_price,
            limit_price,
            limit_price_source,
            limit_price_offset_ticks,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            idempotency_key,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent,
            created_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(order_plan_id) DO UPDATE SET
            status = excluded.status,
            current_price = excluded.current_price,
            limit_price = excluded.limit_price,
            limit_price_source = excluded.limit_price_source,
            suggested_quantity = excluded.suggested_quantity,
            suggested_notional = excluded.suggested_notional,
            expires_at = excluded.expires_at,
            reason_codes_json = excluded.reason_codes_json,
            evidence_json = excluded.evidence_json,
            created_at = excluded.created_at,
            observe_only = 1,
            not_order_intent = 1
        """,
        values,
    )
    connection.execute(
        """
        INSERT INTO order_plan_drafts_latest (
            idempotency_key,
            order_plan_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            side,
            status,
            setup_type,
            entry_timing_state,
            price_location_state,
            theme_id,
            theme_name,
            theme_state,
            theme_rank,
            stock_role,
            priority_score,
            current_price,
            limit_price,
            limit_price_source,
            limit_price_offset_ticks,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent,
            created_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(idempotency_key) DO UPDATE SET
            order_plan_id = excluded.order_plan_id,
            trade_date = excluded.trade_date,
            candidate_instance_id = excluded.candidate_instance_id,
            code = excluded.code,
            name = excluded.name,
            side = excluded.side,
            status = excluded.status,
            setup_type = excluded.setup_type,
            entry_timing_state = excluded.entry_timing_state,
            price_location_state = excluded.price_location_state,
            theme_id = excluded.theme_id,
            theme_name = excluded.theme_name,
            theme_state = excluded.theme_state,
            theme_rank = excluded.theme_rank,
            stock_role = excluded.stock_role,
            priority_score = excluded.priority_score,
            current_price = excluded.current_price,
            limit_price = excluded.limit_price,
            limit_price_source = excluded.limit_price_source,
            limit_price_offset_ticks = excluded.limit_price_offset_ticks,
            suggested_quantity = excluded.suggested_quantity,
            suggested_notional = excluded.suggested_notional,
            max_notional = excluded.max_notional,
            risk_budget_source = excluded.risk_budget_source,
            expires_at = excluded.expires_at,
            reason_codes_json = excluded.reason_codes_json,
            evidence_json = excluded.evidence_json,
            observe_only = 1,
            not_order_intent = 1,
            created_at = excluded.created_at
        """,
        (data["idempotency_key"], *values[:-6], *values[-5:]),
    )


def get_entry_timing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    return {
        "enabled": resolved_settings.entry_timing_enabled,
        "write_order_plan_drafts": resolved_settings.entry_timing_write_order_plan_drafts,
        "observe_only": True,
        "not_order_intent": True,
        "config_version": resolved_settings.entry_timing_config_version,
        "latest_plan_count": _count_rows(connection, "order_plan_drafts_latest"),
        "plan_ready_count": _count_rows(
            connection,
            "order_plan_drafts_latest",
            where="status = 'PLAN_READY'",
        ),
        "wait_retry_count": _count_rows(
            connection,
            "order_plan_drafts_latest",
            where="status = 'WAIT_RETRY'",
        ),
        "data_wait_count": _count_rows(
            connection,
            "order_plan_drafts_latest",
            where="status = 'DATA_WAIT'",
        ),
        "no_plan_count": _count_rows(
            connection,
            "entry_timing_evaluations",
            where=(
                "status IN ("
                "'NO_PLAN',"
                "'BLOCKED_CHASE',"
                "'BLOCKED_OVERHEAT',"
                "'BLOCKED_STALE',"
                "'BLOCKED_RISK'"
                ")"
            ),
        ),
        "evaluation_count": _count_rows(connection, "entry_timing_evaluations"),
        "error_count": _count_rows(connection, "entry_timing_evaluation_errors"),
        "max_plans_per_run": resolved_settings.entry_timing_max_plans_per_run,
        "allow_market_order": resolved_settings.entry_timing_allow_market_order,
    }


def list_latest_order_plan_drafts(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: OrderPlanStatus | str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if status is not None:
        clauses.append("status = ?")
        params.append(OrderPlanStatus(str(status).upper()).value)
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM order_plan_drafts_latest
        {where_sql}
        ORDER BY created_at DESC, priority_score DESC, code ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_draft_row_to_dict(row) for row in rows]


def get_order_plan_draft(
    connection: sqlite3.Connection,
    order_plan_id: str,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(order_plan_id, "order_plan_id")
    row = connection.execute(
        """
        SELECT *
        FROM order_plan_drafts
        WHERE order_plan_id = ?
        """,
        (normalized_id,),
    ).fetchone()
    if row is None:
        return None
    return _draft_row_to_dict(row)


def list_entry_timing_evaluations(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None = None,
    trade_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if candidate_instance_id is not None:
        clauses.append("candidate_instance_id = ?")
        params.append(require_non_empty_str(candidate_instance_id, "candidate_instance_id"))
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT *
        FROM entry_timing_evaluations
        {where_sql}
        ORDER BY evaluated_at DESC, entry_timing_evaluation_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_evaluation_row_to_dict(row) for row in rows]


def list_entry_timing_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM entry_timing_evaluation_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_error_row_to_dict(row) for row in rows]


def _candidate_rows_for_evaluation(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str | None,
    limit: int,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[Any] = []
    if candidate_instance_id is not None:
        clauses.append("c.candidate_instance_id = ?")
        params.append(require_non_empty_str(candidate_instance_id, "candidate_instance_id"))
    else:
        clauses.append("c.trade_date = ?")
        params.append(trade_date)
        placeholders = ",".join("?" for _ in _ENTRY_CANDIDATE_STATES)
        clauses.append(f"c.state IN ({placeholders})")
        params.extend(_ENTRY_CANDIDATE_STATES)
        clauses.append("c.state != ?")
        params.append(CandidateState.BLOCKED_OBSERVATION.value)
        clauses.append("(f.risk_blocked IS NULL OR f.risk_blocked = 0)")
    params.append(limit)
    return connection.execute(
        f"""
        SELECT c.*
        FROM candidates AS c
        LEFT JOIN candidate_condition_fusion AS f
            ON f.trade_date = c.trade_date AND f.code = c.code
        WHERE {" AND ".join(clauses)}
        ORDER BY
            CASE c.state
                WHEN 'CONTEXT_READY' THEN 0
                WHEN 'WATCHING' THEN 1
                WHEN 'DATA_WAIT' THEN 2
                ELSE 3
            END,
            CASE WHEN COALESCE(f.risk_blocked, 0) = 0 THEN 0 ELSE 1 END,
            COALESCE(f.priority_score, 0) DESC,
            CASE
                WHEN c.theme_state IN ('LEADING', 'SPREADING', 'LEADER_ONLY') THEN 0
                WHEN c.theme_state IS NOT NULL THEN 1
                ELSE 2
            END,
            c.last_seen_at DESC,
            c.candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()


def _watchset_inputs(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    limit: int,
    settings: Settings,
) -> list[EntryTimingInput]:
    result = ThemeLeadershipService(settings=settings).rebuild(
        connection,
        trade_date=trade_date,
        write_candidate_sources=False,
    )
    inputs: list[EntryTimingInput] = []
    for item in result.watchset.items[:limit]:
        tick = _latest_tick(connection, item.code)
        latest_1m = _latest_bar(connection, item.code, 60)
        latest_3m = _latest_bar(connection, item.code, 180)
        latest_5m = _latest_bar(connection, item.code, 300)
        market = _market_from_rows(tick, latest_1m, latest_3m, latest_5m, {})
        reason_codes = _dedupe(item.reason_codes)
        inputs.append(
            EntryTimingInput(
                trade_date=trade_date,
                candidate_instance_id=f"WATCHSET-{trade_date}-{item.code}-{item.theme_id}",
                code=item.code,
                name=item.name,
                theme_id=item.theme_id,
                theme_name=item.theme_name,
                theme_state=item.theme_state.value,
                theme_rank=item.theme_rank,
                stock_role=item.stock_role.value,
                theme_priority_score=item.priority_score,
                current_price=_first_number(market.get("current_price")),
                prev_close=_first_number(market.get("prev_close")),
                open_price=_first_number(market.get("open_price")),
                day_high=_first_number(market.get("day_high")),
                day_low=_first_number(market.get("day_low")),
                change_rate_pct=_first_number(market.get("change_rate_pct")),
                turnover_krw=_first_number(market.get("turnover_krw")),
                execution_strength=_first_number(market.get("execution_strength")),
                momentum_1m=_first_number(market.get("momentum_1m")),
                momentum_3m=_first_number(market.get("momentum_3m")),
                momentum_5m=_first_number(market.get("momentum_5m")),
                vwap=_first_number(market.get("vwap")),
                pullback_from_high_pct=_first_number(market.get("pullback_from_high_pct")),
                spread_ticks=_first_int(market.get("spread_ticks")),
                stale=bool(market.get("stale")),
                vi_active="VI_ACTIVE" in reason_codes,
                upper_limit_near="UPPER_LIMIT_NEAR" in reason_codes,
                theme_reason_codes=reason_codes,
                candidate_state=CandidateState.WATCHING.value,
                observed_at=datetime_to_wire(parse_timestamp(item.observed_at, "observed_at")),
                best_bid=_first_number(tick["best_bid"] if tick is not None else None),
                best_ask=_first_number(tick["best_ask"] if tick is not None else None),
                tick_age_sec=_first_number(market.get("tick_age_sec")),
                source="watchset",
                raw_context={"watchset_item": item.to_dict()},
            )
        )
    return inputs


def _merge_theme_metadata(
    source_meta: Mapping[str, Any],
    candidate: sqlite3.Row,
    context: Mapping[str, Any],
    theme: Mapping[str, Any],
    watchset: Mapping[str, Any],
) -> dict[str, Any]:
    theme_context = _dict_or_empty(context.get("theme_context"))
    latest_snapshot = _dict_or_empty(theme.get("latest_snapshot"))
    member = _dict_or_empty(theme.get("member"))
    return {
        "theme_id": _first_text(
            source_meta.get("theme_id"),
            theme_context.get("theme_id"),
            latest_snapshot.get("theme_id"),
            watchset.get("theme_id"),
            candidate["theme_id"],
        ),
        "theme_name": _first_text(
            source_meta.get("theme_name"),
            theme_context.get("theme_name"),
            latest_snapshot.get("theme_name"),
            watchset.get("theme_name"),
            candidate["theme_name"],
        ),
        "theme_state": _first_text(
            source_meta.get("theme_state"),
            theme_context.get("theme_state"),
            latest_snapshot.get("state"),
            watchset.get("theme_state"),
            candidate["theme_state"],
        ),
        "theme_rank": _first_int(source_meta.get("theme_rank"), watchset.get("theme_rank")),
        "stock_role": _first_text(
            source_meta.get("stock_role"),
            theme_context.get("stock_role"),
            theme_context.get("theme_role"),
            member.get("member_role"),
            watchset.get("stock_role"),
            candidate["theme_role"],
        ),
        "priority_score": _first_number(
            source_meta.get("priority_score"),
            watchset.get("priority_score"),
        ),
        "reason_codes": _dedupe(
            [
                *_json_array(source_meta.get("reason_codes")),
                *_json_array(watchset.get("reason_codes")),
            ]
        ),
    }


def _merge_market_data(
    candidate: sqlite3.Row,
    context: Mapping[str, Any],
    tick: sqlite3.Row | None,
    latest_1m: sqlite3.Row | None,
    latest_3m: sqlite3.Row | None,
    latest_5m: sqlite3.Row | None,
    theme: Mapping[str, Any],
) -> dict[str, Any]:
    theme_member = _dict_or_empty(theme.get("member"))
    market = _market_from_rows(tick, latest_1m, latest_3m, latest_5m, theme_member)
    readiness = _dict_or_empty(context.get("readiness"))
    market["tick_age_sec"] = _first_number(
        market.get("tick_age_sec"),
        candidate["tick_age_sec"],
        readiness.get("tick_age_sec"),
    )
    market["stale"] = str(
        _first_text(candidate["market_readiness_status"], readiness.get("quality_status"))
        or ""
    ).upper() in {"STALE", "DEGRADED", "INVALID", "MISSING"}
    return market


def _market_from_rows(
    tick: sqlite3.Row | None,
    latest_1m: sqlite3.Row | None,
    latest_3m: sqlite3.Row | None,
    latest_5m: sqlite3.Row | None,
    theme_member: Mapping[str, Any],
) -> dict[str, Any]:
    current_price = _first_number(
        tick["price"] if tick is not None else None,
        theme_member.get("price"),
    )
    change_rate = _first_number(
        tick["change_rate"] if tick is not None else None,
        theme_member.get("change_rate"),
    )
    day_high = _first_number(tick["day_high"] if tick is not None else None)
    day_low = _first_number(tick["day_low"] if tick is not None else None)
    prev_close = _prev_close(current_price, change_rate)
    open_price = _first_number(
        latest_1m["open"] if latest_1m is not None else None,
        prev_close,
    )
    vwap = _first_number(
        latest_1m["vwap"] if latest_1m is not None else None,
        theme_member.get("vwap"),
    )
    return {
        "current_price": current_price,
        "prev_close": prev_close,
        "open_price": open_price,
        "day_high": day_high,
        "day_low": day_low,
        "change_rate_pct": change_rate,
        "turnover_krw": _first_number(
            tick["cumulative_trade_value"] if tick is not None else None,
            theme_member.get("cumulative_trade_value"),
        ),
        "execution_strength": _first_number(
            tick["execution_strength"] if tick is not None else None,
            theme_member.get("execution_strength"),
        ),
        "momentum_1m": _bar_momentum(latest_1m),
        "momentum_3m": _bar_momentum(latest_3m),
        "momentum_5m": _bar_momentum(latest_5m),
        "vwap": vwap,
        "pullback_from_high_pct": _pullback_from_high(current_price, day_high),
        "spread_ticks": _first_int(tick["spread_ticks"] if tick is not None else None),
        "tick_age_sec": None,
    }


def _candidate_theme_source_metadata(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT payload_json
        FROM candidate_sources_latest
        WHERE candidate_instance_id = ? AND active = 1
        ORDER BY last_seen_at DESC, source_type ASC
        """,
        (candidate_instance_id,),
    ).fetchall()
    for row in rows:
        payload = _json_object(row["payload_json"])
        if payload.get("theme_state") or payload.get("stock_role"):
            return payload
    return {}


def _has_theme_metadata(
    source_meta: Mapping[str, Any],
    candidate: sqlite3.Row,
    theme: Mapping[str, Any],
) -> bool:
    return bool(
        source_meta.get("theme_state")
        and source_meta.get("stock_role")
        and source_meta.get("priority_score") is not None
    ) or bool(candidate["theme_state"] and candidate["theme_role"] and theme.get("member"))


def _watchset_match(
    connection: sqlite3.Connection,
    *,
    code: str,
    theme_id: str | None,
    settings: Settings,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {}
    result = ThemeLeadershipService(settings=settings).rebuild(
        connection,
        write_candidate_sources=False,
    )
    for item in result.watchset.items:
        if item.code == code and (theme_id is None or item.theme_id == theme_id):
            return item.to_dict()
    return {}


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()


def _candidate_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()
    if row is None:
        return {}
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "theme_context": _json_object(row["theme_context_json"]),
        "market_context": _json_object(row["market_context_json"]),
        "source_context": _json_object(row["source_context_json"]),
        "readiness": _json_object(row["readiness_json"]),
        "refreshed_at": row["refreshed_at"],
    }


def _condition_fusion_from_context(context: Mapping[str, Any]) -> dict[str, Any]:
    source_context = _dict_or_empty(context.get("source_context"))
    nested = _dict_or_empty(source_context.get("condition_fusion"))
    return {
        "present": bool(nested.get("present") or source_context.get("condition_fusion_present")),
        "condition_fusion_priority_score": _first_number(
            nested.get("condition_fusion_priority_score"),
            source_context.get("condition_fusion_priority_score"),
        )
        or 0.0,
        "active_condition_roles": _json_array(
            nested.get("active_condition_roles")
            if nested.get("active_condition_roles") is not None
            else source_context.get("active_condition_roles")
        ),
        "condition_risk_blocked": bool(
            nested.get("condition_risk_blocked")
            if nested.get("condition_risk_blocked") is not None
            else source_context.get("condition_risk_blocked")
        ),
        "condition_fusion_reason_codes": _json_array(
            nested.get("condition_fusion_reason_codes")
            if nested.get("condition_fusion_reason_codes") is not None
            else source_context.get("condition_fusion_reason_codes")
        ),
        "condition_names": _string_array(
            nested.get("condition_names")
            if nested.get("condition_names") is not None
            else source_context.get("condition_names")
        ),
        "condition_latest_hit_at": _first_text(
            nested.get("condition_latest_hit_at"),
            source_context.get("condition_latest_hit_at"),
        ),
    }


def _condition_fusion_from_table(
    connection: sqlite3.Connection,
    candidate: sqlite3.Row,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM candidate_condition_fusion
        WHERE trade_date = ? AND code = ?
        """,
        (candidate["trade_date"], candidate["code"]),
    ).fetchone()
    if row is None:
        return {
            "present": False,
            "condition_fusion_priority_score": 0.0,
            "active_condition_roles": [],
            "condition_risk_blocked": False,
            "condition_fusion_reason_codes": [],
            "condition_names": [],
            "condition_latest_hit_at": None,
        }
    return {
        "present": True,
        "condition_fusion_priority_score": _first_number(row["priority_score"]) or 0.0,
        "active_condition_roles": _json_array(row["active_roles_json"]),
        "condition_risk_blocked": bool(row["risk_blocked"]),
        "condition_fusion_reason_codes": _json_array(row["reason_codes_json"]),
        "condition_names": _string_array(row["condition_names_json"]),
        "condition_latest_hit_at": _first_text(row["latest_hit_at"]),
    }


def _latest_tick(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM market_ticks_latest WHERE code = ?",
        (validate_stock_code(code),),
    ).fetchone()


def _latest_bar(
    connection: sqlite3.Connection,
    code: str,
    interval_sec: int,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (validate_stock_code(code), interval_sec),
    ).fetchone()


def _latest_strategy(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM strategy_observations_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _latest_risk(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM risk_observations_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _theme_context(
    connection: sqlite3.Connection,
    theme_id: str | None,
    code: str,
) -> dict[str, Any]:
    if theme_id is None:
        return {}
    row = connection.execute(
        """
        SELECT
            l.theme_id,
            l.snapshot_id,
            l.theme_name,
            l.calculated_at,
            l.state,
            l.quality_status,
            l.leading_code,
            l.leading_name,
            m.code,
            m.name,
            m.price,
            m.change_rate,
            m.cumulative_trade_value,
            m.trade_value_delta_1m,
            m.trade_value_delta_3m,
            m.trade_value_delta_5m,
            m.execution_strength,
            m.vwap,
            m.member_role,
            m.tick_age_sec,
            m.event_ts,
            m.metadata_json
        FROM theme_latest_snapshots AS l
        LEFT JOIN theme_snapshot_members AS m
            ON m.snapshot_id = l.snapshot_id AND m.code = ?
        WHERE l.theme_id = ?
        """,
        (validate_stock_code(code), theme_id),
    ).fetchone()
    if row is None:
        return {}
    data = _row_to_dict(row)
    snapshot_keys = {
        "theme_id",
        "snapshot_id",
        "theme_name",
        "calculated_at",
        "state",
        "quality_status",
        "leading_code",
        "leading_name",
    }
    member = {key: value for key, value in data.items() if key not in snapshot_keys}
    member["metadata"] = _json_object(member.pop("metadata_json", None))
    return {
        "latest_snapshot": {key: data[key] for key in snapshot_keys},
        "member": member,
    }


def _draft_values(data: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        data["order_plan_id"],
        data["trade_date"],
        data["candidate_instance_id"],
        data["code"],
        data["name"],
        data["side"],
        data["status"],
        data["setup_type"],
        data["entry_timing_state"],
        data["price_location_state"],
        data["theme_id"],
        data["theme_name"],
        data["theme_state"],
        data["theme_rank"],
        data["stock_role"],
        data["priority_score"],
        data["current_price"],
        data["limit_price"],
        data["limit_price_source"],
        data["limit_price_offset_ticks"],
        data["suggested_quantity"],
        data["suggested_notional"],
        data["max_notional"],
        data["risk_budget_source"],
        data["expires_at"],
        data["idempotency_key"],
        _json_dumps(data["reason_codes"]),
        canonical_json(data["evidence_json"]),
        1,
        1,
        data["created_at"],
    )


def _draft_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["not_order_intent"] = bool(data["not_order_intent"])
    data["reason_codes"] = _json_array(data.pop("reason_codes_json"))
    data["evidence_json"] = _json_object(data.pop("evidence_json"))
    return data


def _evaluation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["not_order_intent"] = bool(data["not_order_intent"])
    data["reason_codes"] = _json_array(data.pop("reason_codes_json"))
    data["evidence_json"] = _json_object(data.pop("evidence_json"))
    return data


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["payload"] = _json_object(data.pop("payload_json"))
    return data


def _record_evaluation_error(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO entry_timing_evaluation_errors (
            candidate_instance_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            candidate_instance_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            canonical_json(payload),
        ),
    )


def _resolve_trade_date(trade_date: str | None, settings: Settings) -> str:
    if trade_date is not None:
        return require_non_empty_str(trade_date, "trade_date")
    return (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _prev_close(current_price: float | None, change_rate: float | None) -> float | None:
    if current_price is None or current_price <= 0 or change_rate is None or change_rate <= -100:
        return None
    return current_price / (1 + change_rate / 100.0)


def _bar_momentum(row: sqlite3.Row | None) -> float | None:
    if row is None:
        return None
    open_price = _first_number(row["open"])
    close_price = _first_number(row["close"])
    if open_price is None or open_price <= 0 or close_price is None:
        return None
    return (close_price - open_price) / open_price * 100.0


def _pullback_from_high(current_price: float | None, day_high: float | None) -> float | None:
    if current_price is None or current_price <= 0 or day_high is None or day_high <= 0:
        return None
    return max((day_high - current_price) / day_high * 100.0, 0.0)


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return [value.upper()] if value.strip() else []
    else:
        loaded = value
    if not isinstance(loaded, Sequence) or isinstance(loaded, str):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]


def _string_array(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return [value.strip()] if value.strip() else []
    else:
        loaded = value
    if not isinstance(loaded, Sequence) or isinstance(loaded, str):
        return []
    return [str(item).strip() for item in loaded if str(item).strip()]


def _dict_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _first_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_number(*values: object) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_int(*values: object) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    where: str | None = None,
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value).upper() for value in values if str(value).strip())]


_ENTRY_CANDIDATE_STATES = (
    CandidateState.CONTEXT_READY.value,
    CandidateState.WATCHING.value,
    CandidateState.DATA_WAIT.value,
)
