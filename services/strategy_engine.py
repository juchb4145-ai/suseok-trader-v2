from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_str_enum,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from domain.market.quality import tick_age_seconds
from domain.strategy.evaluator import (
    evaluate_breakout_retest as _evaluate_breakout_retest,
)
from domain.strategy.evaluator import (
    evaluate_theme_follower_expansion as _evaluate_theme_follower_expansion,
)
from domain.strategy.evaluator import (
    evaluate_theme_leader_pullback as _evaluate_theme_leader_pullback,
)
from domain.strategy.evaluator import (
    evaluate_vwap_reclaim as _evaluate_vwap_reclaim,
)
from domain.strategy.models import (
    SetupObservation,
    StrategyCandidateContext,
    StrategyObservation,
)
from domain.strategy.reasons import StrategyReasonCode
from domain.strategy.setup import StrategySetupType
from domain.strategy.status import StrategyObservationStatus
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings


@dataclass(frozen=True, kw_only=True)
class StrategyEvaluationRunResult:
    run_id: str
    trade_date: str | None
    candidate_count: int = 0
    evaluated_count: int = 0
    data_wait_count: int = 0
    matched_observation_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    config_version: str = "observe_v1"
    observe_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "candidate_count": self.candidate_count,
            "evaluated_count": self.evaluated_count,
            "data_wait_count": self.data_wait_count,
            "matched_observation_count": self.matched_observation_count,
            "error_count": self.error_count,
            "status": self.status,
            "config_version": self.config_version,
            "observe_only": True,
        }


def load_strategy_candidate_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings,
) -> StrategyCandidateContext:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    candidate = _candidate_row(connection, normalized_id)
    if candidate is None:
        raise ValueError(f"candidate not found: {normalized_id}")

    context_row = connection.execute(
        """
        SELECT *
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (normalized_id,),
    ).fetchone()
    candidate_context = _candidate_context_row_to_dict(context_row)
    tick = _latest_tick_row(connection, candidate["code"])
    bars = {
        interval: _latest_bar_row(connection, candidate["code"], interval)
        for interval in (60, 180, 300)
    }
    theme = _theme_context_row(connection, candidate["theme_id"], candidate["code"])
    readiness = candidate_context.get("readiness", {}) if candidate_context else {}
    market_context = candidate_context.get("market_context", {}) if candidate_context else {}
    source_context = candidate_context.get("source_context", {}) if candidate_context else {}

    latest_tick_from_context = market_context.get("latest_tick") if market_context else None
    tick_source = (
        _row_to_dict(tick) if tick is not None else _dict_or_empty(latest_tick_from_context)
    )
    theme_member = theme.get("member", {})
    latest_snapshot = theme.get("latest_snapshot", {})
    latest_1m = _row_to_dict(bars[60]) if bars[60] is not None else {}
    latest_3m = _row_to_dict(bars[180]) if bars[180] is not None else {}
    latest_5m = _row_to_dict(bars[300]) if bars[300] is not None else {}
    price = _first_number(tick_source.get("price"), theme_member.get("price"))
    vwap = _first_number(latest_1m.get("vwap"), theme_member.get("vwap"))
    tick_age = _first_number(
        candidate["tick_age_sec"],
        readiness.get("tick_age_sec"),
        tick_age_seconds(tick_source["event_ts"]) if tick_source.get("event_ts") else None,
    )
    reason_codes = _context_reason_codes(
        candidate,
        candidate_context=candidate_context,
        tick=tick_source,
        bars=bars,
        readiness=readiness,
    )
    raw_context = {
        "candidate": _candidate_row_to_dict(candidate),
        "candidate_context": candidate_context,
        "latest_tick": tick_source,
        "latest_bars": {
            "60": latest_1m,
            "180": latest_3m,
            "300": latest_5m,
        },
        "theme_latest_snapshot": latest_snapshot,
        "theme_snapshot_member": theme_member,
        "settings": {
            "config_version": settings.strategy_config_version,
            "observe_only": True,
        },
    }
    raw_context["context_hash"] = _context_hash(raw_context)

    return StrategyCandidateContext(
        candidate_instance_id=candidate["candidate_instance_id"],
        trade_date=candidate["trade_date"],
        code=candidate["code"],
        name=candidate["name"],
        candidate_state=candidate["state"],
        theme_id=_first_text(latest_snapshot.get("theme_id"), candidate["theme_id"]),
        theme_name=_first_text(latest_snapshot.get("theme_name"), candidate["theme_name"]),
        theme_state=_first_text(latest_snapshot.get("state"), candidate["theme_state"]),
        theme_role=_first_text(theme_member.get("member_role"), candidate["theme_role"]),
        market_readiness_status=_first_text(
            candidate["market_readiness_status"],
            readiness.get("quality_status"),
        ),
        tick_age_sec=tick_age,
        price=price,
        change_rate=_first_number(tick_source.get("change_rate"), theme_member.get("change_rate")),
        cumulative_trade_value=_first_number(
            tick_source.get("cumulative_trade_value"),
            theme_member.get("cumulative_trade_value"),
        ),
        trade_value_delta_1m=_first_number(
            latest_1m.get("trade_value_delta"),
            theme_member.get("trade_value_delta_1m"),
        ),
        trade_value_delta_3m=_first_number(
            latest_3m.get("trade_value_delta"),
            theme_member.get("trade_value_delta_3m"),
        ),
        trade_value_delta_5m=_first_number(
            latest_5m.get("trade_value_delta"),
            theme_member.get("trade_value_delta_5m"),
        ),
        day_high=_positive_or_none(_first_number(tick_source.get("day_high"))),
        day_low=_positive_or_none(_first_number(tick_source.get("day_low"))),
        vwap=vwap,
        above_vwap=bool(price is not None and vwap is not None and price >= vwap),
        bar_1m_ready=_bool_first(candidate["bar_1m_ready"], readiness.get("has_1m_bar")),
        bar_3m_ready=_bool_first(candidate["bar_3m_ready"], readiness.get("has_3m_bar")),
        bar_5m_ready=_bool_first(candidate["bar_5m_ready"], readiness.get("has_5m_bar")),
        source_count=int(candidate["source_count"] or source_context.get("source_count") or 0),
        active_source_count=int(
            candidate["active_source_count"] or source_context.get("active_source_count") or 0
        ),
        reason_codes=reason_codes,
        raw_context=raw_context,
    )


def evaluate_candidate_strategy(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
) -> StrategyObservation:
    resolved_settings = settings or load_settings()
    context = load_strategy_candidate_context(connection, candidate_instance_id, resolved_settings)
    evaluated_at = datetime_to_wire(utc_now())
    pre_status, pre_reasons = _context_precheck(context, resolved_settings)
    if pre_status is not None:
        setup_observations = _precheck_setup_observations(pre_status, pre_reasons)
        return _observation_from_setups(
            context,
            setup_observations,
            evaluated_at=evaluated_at,
            overall_status=pre_status,
            settings=resolved_settings,
            extra_reasons=pre_reasons,
        )

    setup_observations = [
        evaluate_theme_leader_pullback(context, resolved_settings),
        evaluate_vwap_reclaim(context, resolved_settings),
        evaluate_breakout_retest(context, resolved_settings),
        evaluate_theme_follower_expansion(context, resolved_settings),
    ]
    return _observation_from_setups(
        context,
        setup_observations,
        evaluated_at=evaluated_at,
        overall_status=_overall_status(setup_observations),
        settings=resolved_settings,
        extra_reasons=[],
    )


def evaluate_theme_leader_pullback(
    context: StrategyCandidateContext,
    settings: Settings,
) -> SetupObservation:
    return _evaluate_theme_leader_pullback(context, settings)


def evaluate_vwap_reclaim(
    context: StrategyCandidateContext,
    settings: Settings,
) -> SetupObservation:
    return _evaluate_vwap_reclaim(context, settings)


def evaluate_breakout_retest(
    context: StrategyCandidateContext,
    settings: Settings,
) -> SetupObservation:
    return _evaluate_breakout_retest(context, settings)


def evaluate_theme_follower_expansion(
    context: StrategyCandidateContext,
    settings: Settings,
) -> SetupObservation:
    return _evaluate_theme_follower_expansion(context, settings)


def save_strategy_observation(
    connection: sqlite3.Connection,
    observation: StrategyObservation,
) -> None:
    data = observation.to_dict(include_setups=False)
    connection.execute(
        """
        INSERT INTO strategy_observations (
            strategy_observation_id,
            candidate_instance_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            primary_setup_type,
            primary_setup_status,
            score,
            confidence,
            reason_codes_json,
            evidence_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["strategy_observation_id"],
            data["candidate_instance_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["overall_status"],
            data["primary_setup_type"],
            data["primary_setup_status"],
            data["score"],
            data["confidence"],
            _json_dumps(data["reason_codes"]),
            canonical_json(data["evidence_json"]),
            data["config_version"],
            1,
        ),
    )
    for setup in observation.setup_observations:
        setup_data = setup.to_dict()
        connection.execute(
            """
            INSERT INTO strategy_setup_observations (
                strategy_observation_id,
                candidate_instance_id,
                setup_type,
                status,
                score,
                confidence,
                reason_codes_json,
                evidence_json,
                evaluated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation.strategy_observation_id,
                observation.candidate_instance_id,
                setup_data["setup_type"],
                setup_data["status"],
                setup_data["score"],
                setup_data["confidence"],
                _json_dumps(setup_data["reason_codes"]),
                canonical_json(setup_data["evidence_json"]),
                data["evaluated_at"],
            ),
        )
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            primary_setup_type,
            primary_setup_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_instance_id) DO UPDATE SET
            strategy_observation_id = excluded.strategy_observation_id,
            trade_date = excluded.trade_date,
            code = excluded.code,
            name = excluded.name,
            evaluated_at = excluded.evaluated_at,
            overall_status = excluded.overall_status,
            primary_setup_type = excluded.primary_setup_type,
            primary_setup_status = excluded.primary_setup_status,
            score = excluded.score,
            confidence = excluded.confidence,
            reason_codes_json = excluded.reason_codes_json,
            config_version = excluded.config_version,
            observe_only = 1
        """,
        (
            data["candidate_instance_id"],
            data["strategy_observation_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["overall_status"],
            data["primary_setup_type"],
            data["primary_setup_status"],
            data["score"],
            data["confidence"],
            _json_dumps(data["reason_codes"]),
            data["config_version"],
            1,
        ),
    )


def evaluate_candidates(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    candidate_state: CandidateState | str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    candidate_instance_id: str | None = None,
) -> StrategyEvaluationRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("strategy_run")
    started_at = datetime_to_wire(utc_now())
    bounded_limit = _bounded_limit(limit or resolved_settings.strategy_engine_max_candidates)
    _insert_run(
        connection,
        run_id=run_id,
        trade_date=trade_date,
        started_at=started_at,
        config_version=resolved_settings.strategy_config_version,
        status="RUNNING",
    )
    if not resolved_settings.strategy_engine_enabled:
        _complete_run(
            connection,
            run_id=run_id,
            candidate_count=0,
            evaluated_count=0,
            data_wait_count=0,
            matched_observation_count=0,
            error_count=0,
            status="DISABLED",
        )
        connection.commit()
        return StrategyEvaluationRunResult(
            run_id=run_id,
            trade_date=trade_date,
            status="DISABLED",
            config_version=resolved_settings.strategy_config_version,
        )

    rows = _candidate_rows_for_evaluation(
        connection,
        trade_date=trade_date,
        candidate_state=candidate_state,
        limit=bounded_limit,
        settings=resolved_settings,
        candidate_instance_id=candidate_instance_id,
    )
    candidate_count = len(rows)
    evaluated_count = data_wait_count = matched_count = error_count = 0
    for row in rows:
        try:
            observation = evaluate_candidate_strategy(
                connection,
                row["candidate_instance_id"],
                settings=resolved_settings,
            )
            save_strategy_observation(connection, observation)
            evaluated_count += 1
            if observation.overall_status is StrategyObservationStatus.DATA_WAIT:
                data_wait_count += 1
            if observation.overall_status is StrategyObservationStatus.MATCHED_OBSERVATION:
                matched_count += 1
        except Exception as exc:
            error_count += 1
            _record_evaluation_error(
                connection,
                run_id=run_id,
                candidate_instance_id=row["candidate_instance_id"],
                code=row["code"],
                error_message=str(exc),
                payload=_row_to_dict(row),
            )
    status = "COMPLETED_WITH_ERRORS" if error_count else "COMPLETED"
    _complete_run(
        connection,
        run_id=run_id,
        candidate_count=candidate_count,
        evaluated_count=evaluated_count,
        data_wait_count=data_wait_count,
        matched_observation_count=matched_count,
        error_count=error_count,
        status=status,
    )
    connection.commit()
    return StrategyEvaluationRunResult(
        run_id=run_id,
        trade_date=trade_date,
        candidate_count=candidate_count,
        evaluated_count=evaluated_count,
        data_wait_count=data_wait_count,
        matched_observation_count=matched_count,
        error_count=error_count,
        status=status,
        config_version=resolved_settings.strategy_config_version,
    )


def get_strategy_status(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    return {
        "enabled": resolved_settings.strategy_engine_enabled,
        "observe_only": True,
        "config_version": resolved_settings.strategy_config_version,
        "latest_observation_count": _count_rows(connection, "strategy_observations_latest"),
        "matched_observation_count": _count_rows(
            connection,
            "strategy_observations_latest",
            where="overall_status = 'MATCHED_OBSERVATION'",
        ),
        "forming_count": _count_rows(
            connection,
            "strategy_observations_latest",
            where="overall_status = 'FORMING'",
        ),
        "data_wait_count": _count_rows(
            connection,
            "strategy_observations_latest",
            where="overall_status = 'DATA_WAIT'",
        ),
        "error_count": _count_rows(connection, "strategy_evaluation_errors"),
        "allowed_candidate_states": list(
            resolved_settings.strategy_engine_allowed_candidate_states
        ),
        "stale_tick_sec": resolved_settings.strategy_engine_stale_tick_sec,
        "require_context_ready": resolved_settings.strategy_engine_require_context_ready,
    }


def get_latest_strategy_observation(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    include_setups: bool = False,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    row = connection.execute(
        """
        SELECT
            l.*,
            o.evidence_json
        FROM strategy_observations_latest AS l
        LEFT JOIN strategy_observations AS o
            ON o.strategy_observation_id = l.strategy_observation_id
        WHERE l.candidate_instance_id = ?
        """,
        (normalized_id,),
    ).fetchone()
    if row is None:
        return None
    observation = _latest_observation_row_to_dict(row)
    if include_setups:
        observation["setup_observations"] = list_strategy_setup_observations(
            connection,
            observation["strategy_observation_id"],
        )
    return observation


def list_latest_strategy_observations(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: StrategyObservationStatus | str | None = None,
    setup_type: StrategySetupType | str | None = None,
    code: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("l.trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if status is not None:
        normalized_status = parse_str_enum(status, StrategyObservationStatus, "status")
        clauses.append("l.overall_status = ?")
        params.append(normalized_status.value)
    if setup_type is not None:
        normalized_setup = parse_str_enum(setup_type, StrategySetupType, "setup_type")
        clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM strategy_setup_observations AS s
                WHERE s.strategy_observation_id = l.strategy_observation_id
                    AND s.setup_type = ?
            )
            """
        )
        params.append(normalized_setup.value)
    if code is not None:
        clauses.append("l.code = ?")
        params.append(validate_stock_code(code))
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT
            l.*,
            o.evidence_json
        FROM strategy_observations_latest AS l
        LEFT JOIN strategy_observations AS o
            ON o.strategy_observation_id = l.strategy_observation_id
        {where_sql}
        ORDER BY l.evaluated_at DESC, l.code ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_latest_observation_row_to_dict(row) for row in rows]


def list_strategy_observations_for_candidate(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    rows = connection.execute(
        """
        SELECT *
        FROM strategy_observations
        WHERE candidate_instance_id = ?
        ORDER BY evaluated_at DESC, strategy_observation_id DESC
        LIMIT ?
        """,
        (normalized_id, _bounded_limit(limit)),
    ).fetchall()
    return [_observation_row_to_dict(row) for row in rows]


def list_strategy_setup_observations(
    connection: sqlite3.Connection,
    strategy_observation_id: str,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(
        strategy_observation_id,
        "strategy_observation_id",
    )
    rows = connection.execute(
        """
        SELECT *
        FROM strategy_setup_observations
        WHERE strategy_observation_id = ?
        ORDER BY score DESC, confidence DESC, setup_type ASC
        """,
        (normalized_id,),
    ).fetchall()
    return [_setup_row_to_dict(row) for row in rows]


def list_strategy_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM strategy_evaluation_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def list_strategy_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM strategy_evaluation_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    errors = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = json.loads(item.pop("payload_json"))
        errors.append(item)
    return errors


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _latest_tick_row(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (validate_stock_code(code),),
    ).fetchone()


def _latest_bar_row(
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


def _theme_context_row(
    connection: sqlite3.Connection,
    theme_id: str | None,
    code: str,
) -> dict[str, Any]:
    if theme_id is None:
        return {"latest_snapshot": {}, "member": {}}
    row = connection.execute(
        """
        SELECT
            l.snapshot_id,
            l.theme_id,
            l.theme_name,
            l.calculated_at,
            l.state,
            l.quality_status,
            l.leading_code,
            l.leading_name,
            l.fresh_coverage_ratio,
            l.rising_ratio,
            l.total_trade_value,
            l.trade_value_delta_1m AS theme_trade_value_delta_1m,
            l.trade_value_delta_3m AS theme_trade_value_delta_3m,
            l.trade_value_delta_5m AS theme_trade_value_delta_5m,
            m.code,
            m.name,
            m.price,
            m.change_rate,
            m.cumulative_trade_value,
            m.trade_value_delta_1m,
            m.trade_value_delta_3m,
            m.trade_value_delta_5m,
            m.vwap,
            m.above_vwap,
            m.readiness_status,
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
        return {"latest_snapshot": {}, "member": {}}
    data = _row_to_dict(row)
    latest_snapshot_keys = {
        "snapshot_id",
        "theme_id",
        "theme_name",
        "calculated_at",
        "state",
        "quality_status",
        "leading_code",
        "leading_name",
        "fresh_coverage_ratio",
        "rising_ratio",
        "total_trade_value",
        "theme_trade_value_delta_1m",
        "theme_trade_value_delta_3m",
        "theme_trade_value_delta_5m",
    }
    latest_snapshot = {key: data[key] for key in latest_snapshot_keys}
    member = {key: value for key, value in data.items() if key not in latest_snapshot_keys}
    if member.get("metadata_json") is not None:
        member["metadata"] = json.loads(member.pop("metadata_json"))
    else:
        member.pop("metadata_json", None)
    if "above_vwap" in member and member["above_vwap"] is not None:
        member["above_vwap"] = bool(member["above_vwap"])
    return {"latest_snapshot": latest_snapshot, "member": member}


def _candidate_context_row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "theme_context": _json_load_object(row["theme_context_json"]),
        "market_context": _json_load_object(row["market_context_json"]),
        "source_context": _json_load_object(row["source_context_json"]),
        "readiness": _json_load_object(row["readiness_json"]),
        "refreshed_at": row["refreshed_at"],
    }


def _context_reason_codes(
    candidate: sqlite3.Row,
    *,
    candidate_context: Mapping[str, Any],
    tick: Mapping[str, Any],
    bars: Mapping[int, sqlite3.Row | None],
    readiness: Mapping[str, Any],
) -> list[str]:
    reasons = _json_load_array(candidate["reason_codes_json"])
    if not candidate_context:
        reasons.append(StrategyReasonCode.CANDIDATE_NOT_CONTEXT_READY.value)
    if not tick:
        reasons.append(StrategyReasonCode.TICK_MISSING.value)
    for reason in readiness.get("reason_codes", ()):
        reasons.extend(_map_market_reason(reason))
    if candidate["market_readiness_status"] == "MISSING":
        reasons.append(StrategyReasonCode.MARKET_READINESS_MISSING.value)
    if bars.get(60) is None:
        reasons.append(StrategyReasonCode.BAR_1M_MISSING.value)
    if bars.get(180) is None:
        reasons.append(StrategyReasonCode.BAR_3M_MISSING.value)
    if bars.get(300) is None:
        reasons.append(StrategyReasonCode.BAR_5M_MISSING.value)
    if not candidate["theme_id"]:
        reasons.append(StrategyReasonCode.THEME_CONTEXT_MISSING.value)
    return _merge_reasons(reasons)


def _map_market_reason(reason: object) -> list[str]:
    value = str(reason).strip().upper()
    if value == "TICK_MISSING":
        return [StrategyReasonCode.TICK_MISSING.value]
    if value in {"TICK_STALE", "TICK_DEGRADED", "TICK_INVALID"}:
        return [StrategyReasonCode.MARKET_READINESS_STALE.value]
    if value == "VWAP_MISSING":
        return [StrategyReasonCode.VWAP_MISSING.value]
    if value in {"BAR_MISSING", "BAR_MISSING_60"}:
        return [StrategyReasonCode.BAR_1M_MISSING.value]
    if value == "BAR_MISSING_180":
        return [StrategyReasonCode.BAR_3M_MISSING.value]
    if value == "BAR_MISSING_300":
        return [StrategyReasonCode.BAR_5M_MISSING.value]
    return [value] if value else []


def _context_precheck(
    context: StrategyCandidateContext,
    settings: Settings,
) -> tuple[StrategyObservationStatus | None, list[str]]:
    state = context.candidate_state.upper()
    reason_set = set(context.reason_codes)
    if "CONDITION_RISK_BLOCKED" in reason_set:
        return StrategyObservationStatus.NOT_EVALUATED, [
            StrategyReasonCode.CONDITION_RISK_BLOCKED.value
        ]
    if "DISCOVERY_OBSERVATION_ONLY" in reason_set:
        return StrategyObservationStatus.NOT_EVALUATED, [
            StrategyReasonCode.DISCOVERY_OBSERVATION_ONLY.value
        ]
    if state in {CandidateState.CLOSED.value, CandidateState.STALE.value}:
        return StrategyObservationStatus.STALE_CONTEXT, [
            StrategyReasonCode.CANDIDATE_STALE.value
        ]
    if state == CandidateState.DATA_WAIT.value:
        return StrategyObservationStatus.DATA_WAIT, [
            StrategyReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
        ]
    if state not in settings.strategy_engine_allowed_candidate_states:
        return StrategyObservationStatus.NOT_EVALUATED, [
            StrategyReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
        ]
    if settings.strategy_engine_require_context_ready and state != CandidateState.CONTEXT_READY:
        return StrategyObservationStatus.DATA_WAIT, [
            StrategyReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
        ]
    if (
        state == CandidateState.CONTEXT_READY.value
        and not context.raw_context.get("candidate_context")
    ):
        return StrategyObservationStatus.INVALID_CONTEXT, [
            StrategyReasonCode.CANDIDATE_NOT_CONTEXT_READY.value
        ]
    if context.tick_age_sec is not None and (
        context.tick_age_sec > settings.strategy_engine_stale_tick_sec
    ):
        return StrategyObservationStatus.STALE_CONTEXT, [
            StrategyReasonCode.MARKET_READINESS_STALE.value
        ]
    missing_reasons: list[str] = []
    if context.price is None:
        missing_reasons.append(StrategyReasonCode.TICK_MISSING.value)
    if settings.strategy_engine_require_1m_bar and not context.bar_1m_ready:
        missing_reasons.append(StrategyReasonCode.BAR_1M_MISSING.value)
    if settings.strategy_engine_require_vwap and context.vwap is None:
        missing_reasons.append(StrategyReasonCode.VWAP_MISSING.value)
    if missing_reasons:
        return StrategyObservationStatus.DATA_WAIT, missing_reasons
    return None, []


def _precheck_setup_observations(
    status: StrategyObservationStatus,
    reasons: Sequence[str],
) -> list[SetupObservation]:
    return [
        SetupObservation(
            setup_type=setup_type,
            status=status,
            score=0.0,
            confidence=0.0,
            reason_codes=[*reasons, StrategyReasonCode.OBSERVE_ONLY.value],
            evidence_json={"observe_only": True, "precheck_status": status.value},
        )
        for setup_type in StrategySetupType
    ]


def _observation_from_setups(
    context: StrategyCandidateContext,
    setup_observations: Sequence[SetupObservation],
    *,
    evaluated_at: str,
    overall_status: StrategyObservationStatus,
    settings: Settings,
    extra_reasons: Sequence[str],
) -> StrategyObservation:
    primary = _primary_setup(setup_observations, overall_status)
    reason_codes = _merge_reasons(
        [
            *context.reason_codes,
            *extra_reasons,
            *[
                reason
                for setup in setup_observations
                for reason in setup.reason_codes
            ],
            StrategyReasonCode.OBSERVE_ONLY.value,
        ]
    )
    return StrategyObservation(
        strategy_observation_id=new_message_id("strategy_observation"),
        candidate_instance_id=context.candidate_instance_id,
        trade_date=context.trade_date,
        code=context.code,
        name=context.name,
        evaluated_at=evaluated_at,
        overall_status=overall_status,
        primary_setup_type=primary.setup_type if primary is not None else None,
        primary_setup_status=primary.status if primary is not None else None,
        setup_observations=list(setup_observations),
        reason_codes=reason_codes,
        evidence_json={
            "observe_only": True,
            "context_hash": context.raw_context.get("context_hash"),
            "candidate_state": context.candidate_state,
            "primary_selection": primary.setup_type.value if primary is not None else None,
        },
        config_version=settings.strategy_config_version,
        observe_only=True,
        score=primary.score if primary is not None else 0.0,
        confidence=primary.confidence if primary is not None else 0.0,
    )


def _overall_status(
    setup_observations: Sequence[SetupObservation],
) -> StrategyObservationStatus:
    statuses = [setup.status for setup in setup_observations]
    if StrategyObservationStatus.MATCHED_OBSERVATION in statuses:
        return StrategyObservationStatus.MATCHED_OBSERVATION
    if StrategyObservationStatus.FORMING in statuses:
        return StrategyObservationStatus.FORMING
    if StrategyObservationStatus.WATCH in statuses:
        return StrategyObservationStatus.WATCH
    if StrategyObservationStatus.DATA_WAIT in statuses:
        return StrategyObservationStatus.DATA_WAIT
    return StrategyObservationStatus.NO_SETUP


def _primary_setup(
    setup_observations: Sequence[SetupObservation],
    overall_status: StrategyObservationStatus,
) -> SetupObservation | None:
    if overall_status in {
        StrategyObservationStatus.NOT_EVALUATED,
        StrategyObservationStatus.INVALID_CONTEXT,
        StrategyObservationStatus.STALE_CONTEXT,
    }:
        return None
    if not setup_observations:
        return None
    return max(
        setup_observations,
        key=lambda setup: (_status_rank(setup.status), setup.score, setup.confidence),
    )


def _status_rank(status: StrategyObservationStatus) -> int:
    ranks = {
        StrategyObservationStatus.MATCHED_OBSERVATION: 5,
        StrategyObservationStatus.FORMING: 4,
        StrategyObservationStatus.WATCH: 3,
        StrategyObservationStatus.DATA_WAIT: 2,
        StrategyObservationStatus.NO_SETUP: 1,
        StrategyObservationStatus.NOT_EVALUATED: 0,
        StrategyObservationStatus.INVALID_CONTEXT: 0,
        StrategyObservationStatus.STALE_CONTEXT: 0,
    }
    return ranks[status]


def _candidate_rows_for_evaluation(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    candidate_state: CandidateState | str | None,
    limit: int,
    settings: Settings,
    candidate_instance_id: str | None,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[Any] = []
    if candidate_instance_id is not None:
        clauses.append("candidate_instance_id = ?")
        params.append(require_non_empty_str(candidate_instance_id, "candidate_instance_id"))
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if candidate_state is not None:
        normalized_state = parse_str_enum(candidate_state, CandidateState, "candidate_state")
        clauses.append("state = ?")
        params.append(normalized_state.value)
    elif candidate_instance_id is None:
        states = [CandidateState.CONTEXT_READY.value]
        if (
            not settings.strategy_engine_require_context_ready
            and CandidateState.WATCHING.value in settings.strategy_engine_allowed_candidate_states
        ):
            states.append(CandidateState.WATCHING.value)
        placeholders = ",".join("?" for _ in states)
        clauses.append(f"state IN ({placeholders})")
        params.extend(states)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(limit)
    return connection.execute(
        f"""
        SELECT *
        FROM candidates
        {where_sql}
        ORDER BY
            CASE state WHEN 'CONTEXT_READY' THEN 0 WHEN 'WATCHING' THEN 1 ELSE 2 END,
            last_seen_at ASC,
            candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()


def _insert_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    started_at: str,
    config_version: str,
    status: str,
) -> None:
    connection.execute(
        """
        INSERT INTO strategy_evaluation_runs (
            run_id,
            trade_date,
            started_at,
            config_version,
            status
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, trade_date, started_at, config_version, status),
    )


def _complete_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    candidate_count: int,
    evaluated_count: int,
    data_wait_count: int,
    matched_observation_count: int,
    error_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE strategy_evaluation_runs
        SET completed_at = ?,
            candidate_count = ?,
            evaluated_count = ?,
            data_wait_count = ?,
            matched_observation_count = ?,
            error_count = ?,
            status = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        (
            datetime_to_wire(utc_now()),
            candidate_count,
            evaluated_count,
            data_wait_count,
            matched_observation_count,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )


def _record_evaluation_error(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    candidate_instance_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO strategy_evaluation_errors (
            run_id,
            candidate_instance_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            candidate_instance_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            canonical_json(payload),
        ),
    )


def _latest_observation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    evidence_json = data.pop("evidence_json", None)
    data["evidence_json"] = _json_load_object(evidence_json) if evidence_json else {}
    return data


def _observation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["evidence_json"] = json.loads(data.pop("evidence_json"))
    return data


def _setup_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["evidence_json"] = json.loads(data.pop("evidence_json"))
    return data


def _candidate_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["vwap_ready"] = bool(data["vwap_ready"])
    data["bar_1m_ready"] = bool(data["bar_1m_ready"])
    data["bar_3m_ready"] = bool(data["bar_3m_ready"])
    data["bar_5m_ready"] = bool(data["bar_5m_ready"])
    data["reason_codes"] = json.loads(data.pop("reason_codes_json"))
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _json_load_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else {}


def _json_load_array(value: str | None) -> list[str]:
    if not value:
        return []
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _dict_or_empty(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _positive_or_none(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return value


def _bool_first(*values: object) -> bool:
    for value in values:
        if value is None:
            continue
        return bool(value)
    return False


def _merge_reasons(reasons: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


def _context_hash(payload: Mapping[str, Any]) -> str:
    payload_json = canonical_json(payload)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


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
