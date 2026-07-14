from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json


def resolve_candidate_source_lineage(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    source_run_id: str | None = None,
    generated_by: str,
    fallback_trade_date: str | None = None,
    fallback_observed_at: str | None = None,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT
            c.candidate_instance_id,
            c.trade_date,
            c.code,
            c.state,
            c.state_updated_at,
            c.last_seen_at,
            x.market_context_snapshot_id,
            x.market_context_json,
            x.theme_context_json,
            x.refreshed_at,
            t.event_id AS tick_event_id,
            t.event_ts AS tick_event_ts,
            t.exchange AS tick_exchange,
            (
                SELECT b.bucket_start
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 60
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_1m_bucket_start,
            (
                SELECT b.updated_at
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 60
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_1m_updated_at,
            (
                SELECT b.bucket_start
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 180
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_3m_bucket_start,
            (
                SELECT b.updated_at
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 180
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_3m_updated_at,
            (
                SELECT b.bucket_start
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 300
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_5m_bucket_start,
            (
                SELECT b.updated_at
                FROM market_minute_bars AS b
                WHERE b.code = c.code
                  AND b.exchange = 'KRX'
                  AND b.interval_sec = 300
                ORDER BY b.bucket_start DESC
                LIMIT 1
            ) AS bar_5m_updated_at,
            th.snapshot_id AS theme_snapshot_id,
            th.calculated_at AS theme_calculated_at,
            f.latest_event_id AS condition_event_id,
            f.updated_at AS condition_updated_at
        FROM candidates AS c
        LEFT JOIN candidate_context_latest AS x
            ON x.candidate_instance_id = c.candidate_instance_id
        LEFT JOIN market_ticks_latest AS t
            ON t.code = c.code AND t.exchange = 'KRX'
        LEFT JOIN theme_latest_snapshots AS th
            ON th.theme_id = c.theme_id
        LEFT JOIN candidate_condition_fusion AS f
            ON f.trade_date = c.trade_date AND f.code = c.code
        WHERE c.candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()
    data = _row_dict(row)
    market_context = _json_object(data.get("market_context_json"))
    theme_context = _json_object(data.get("theme_context_json"))
    source_observed_at = _first_text(
        data.get("tick_event_ts"),
        data.get("refreshed_at"),
        fallback_observed_at,
        data.get("state_updated_at"),
        data.get("last_seen_at"),
    )
    watermark = {
        "candidate": {
            "candidate_instance_id": candidate_instance_id,
            "state": data.get("state"),
            "state_updated_at": data.get("state_updated_at"),
            "last_seen_at": data.get("last_seen_at"),
        },
        "candidate_context": {
            "refreshed_at": data.get("refreshed_at"),
            "market_context_snapshot_id": data.get("market_context_snapshot_id"),
            "market_context_watermark_hash": market_context.get(
                "source_watermark_hash"
            ),
        },
        "market_data": {
            "event_id": data.get("tick_event_id"),
            "event_ts": data.get("tick_event_ts"),
            "exchange": data.get("tick_exchange"),
            "bars": {
                "60": {
                    "bucket_start": data.get("bar_1m_bucket_start"),
                    "updated_at": data.get("bar_1m_updated_at"),
                },
                "180": {
                    "bucket_start": data.get("bar_3m_bucket_start"),
                    "updated_at": data.get("bar_3m_updated_at"),
                },
                "300": {
                    "bucket_start": data.get("bar_5m_bucket_start"),
                    "updated_at": data.get("bar_5m_updated_at"),
                },
            },
        },
        "theme": {
            "snapshot_id": _first_text(
                data.get("theme_snapshot_id"),
                theme_context.get("snapshot_id"),
                theme_context.get("theme_snapshot_id"),
            ),
            "calculated_at": _first_text(
                data.get("theme_calculated_at"),
                theme_context.get("calculated_at"),
                theme_context.get("snapshot_at"),
            ),
        },
        "condition_fusion": {
            "event_id": data.get("condition_event_id"),
            "updated_at": data.get("condition_updated_at"),
        },
    }
    watermark_json = canonical_json(watermark)
    watermark_hash = hashlib.sha256(watermark_json.encode("utf-8")).hexdigest()
    resolved_run_id = _first_text(source_run_id) or f"pipeline_source:{watermark_hash[:20]}"
    return {
        "source_run_id": resolved_run_id,
        "source_watermark": watermark,
        "source_watermark_json": watermark_json,
        "source_watermark_hash": watermark_hash,
        "source_event_id": _first_text(
            data.get("tick_event_id"),
            data.get("condition_event_id"),
        ),
        "source_observed_at": source_observed_at,
        "data_age_sec": _age_seconds(source_observed_at),
        "trade_date": _first_text(data.get("trade_date"), fallback_trade_date),
        "generated_by": generated_by,
        "candidate_instance_id": candidate_instance_id,
        "candidate_present": bool(data),
    }


def lineage_from_row(
    row: sqlite3.Row | Mapping[str, Any] | None,
    *,
    generated_by: str | None = None,
) -> dict[str, Any] | None:
    data = _row_dict(row)
    watermark = _json_object(data.get("source_watermark"))
    stored_watermark_hash = _first_text(data.get("source_watermark_hash"))
    calculated_watermark_hash = hashlib.sha256(
        canonical_json(watermark).encode("utf-8")
    ).hexdigest()
    if (
        not data
        or not _first_text(data.get("source_run_id"))
        or not watermark
        or not stored_watermark_hash
        or not _first_text(data.get("source_observed_at"))
        or data.get("data_age_sec") is None
        or not _first_text(data.get("generated_by"))
    ):
        return None
    source_observed_at = _first_text(data.get("source_observed_at"))
    return {
        "source_run_id": _first_text(data.get("source_run_id")),
        "source_watermark": watermark,
        "source_watermark_json": str(data.get("source_watermark") or "{}"),
        "source_watermark_hash": stored_watermark_hash,
        "source_watermark_hash_valid": (
            stored_watermark_hash == calculated_watermark_hash
        ),
        "source_event_id": _first_text(data.get("source_event_id")),
        "source_observed_at": source_observed_at,
        "data_age_sec": _age_seconds(source_observed_at),
        "trade_date": _first_text(data.get("trade_date")),
        "generated_by": generated_by or _first_text(data.get("generated_by")),
        "candidate_instance_id": _first_text(data.get("candidate_instance_id")),
    }


def lineage_for_strategy_observation(
    connection: sqlite3.Connection,
    strategy_observation_id: str | None,
    *,
    generated_by: str,
) -> dict[str, Any] | None:
    if not strategy_observation_id:
        return None
    row = connection.execute(
        """
        SELECT *
        FROM strategy_observations
        WHERE strategy_observation_id = ?
        """,
        (strategy_observation_id,),
    ).fetchone()
    return lineage_from_row(row, generated_by=generated_by)


def assess_strategy_risk_lineage(
    strategy: sqlite3.Row | Mapping[str, Any] | None,
    risk: sqlite3.Row | Mapping[str, Any] | None,
    *,
    max_age_sec: float,
    expected_source_run_id: str | None = None,
) -> dict[str, Any]:
    strategy_data = _row_dict(strategy)
    risk_data = _row_dict(risk)
    reasons: list[str] = []
    if not strategy_data.get("strategy_observation_id"):
        reasons.append("STRATEGY_OBSERVATION_MISSING")
    if not risk_data.get("risk_observation_id"):
        reasons.append("RISK_OBSERVATION_MISSING")
    if reasons:
        return _lineage_assessment(
            status="FAIL",
            reasons=reasons,
            strategy=strategy_data,
            risk=risk_data,
        )

    strategy_lineage = lineage_from_row(strategy_data)
    risk_lineage = lineage_from_row(risk_data)
    if strategy_lineage is None:
        reasons.append("STRATEGY_LINEAGE_MISSING")
    elif not strategy_lineage["source_watermark_hash_valid"]:
        reasons.append("STRATEGY_WATERMARK_HASH_INVALID")
    if risk_lineage is None:
        reasons.append("RISK_LINEAGE_MISSING")
    elif not risk_lineage["source_watermark_hash_valid"]:
        reasons.append("RISK_WATERMARK_HASH_INVALID")
    if risk_data.get("strategy_observation_id") != strategy_data.get(
        "strategy_observation_id"
    ):
        reasons.append("RISK_STRATEGY_OBSERVATION_MISMATCH")
    if strategy_data.get("trade_date") != risk_data.get("trade_date"):
        reasons.append("STRATEGY_RISK_TRADE_DATE_MISMATCH")
    if strategy_lineage is not None and risk_lineage is not None:
        if strategy_lineage["source_watermark_hash"] != risk_lineage[
            "source_watermark_hash"
        ]:
            reasons.append("STRATEGY_RISK_WATERMARK_MISMATCH")
        if strategy_lineage["source_run_id"] != risk_lineage["source_run_id"]:
            reasons.append("STRATEGY_RISK_SOURCE_RUN_MISMATCH")
        if expected_source_run_id and strategy_lineage["source_run_id"] != str(
            expected_source_run_id
        ):
            reasons.append("EXPECTED_SOURCE_RUN_MISMATCH")
        if float(strategy_lineage["data_age_sec"] or 0.0) > float(max_age_sec):
            reasons.append("PIPELINE_SOURCE_STALE")

    lineage = strategy_lineage or risk_lineage
    return _lineage_assessment(
        status="PASS" if not reasons else "FAIL",
        reasons=reasons,
        strategy=strategy_data,
        risk=risk_data,
        lineage=lineage,
    )


def assess_candidate_pipeline_lineage(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    strategy: sqlite3.Row | Mapping[str, Any] | None,
    risk: sqlite3.Row | Mapping[str, Any] | None,
    *,
    max_age_sec: float,
    expected_source_run_id: str | None = None,
) -> dict[str, Any]:
    assessment = assess_strategy_risk_lineage(
        strategy,
        risk,
        max_age_sec=max_age_sec,
        expected_source_run_id=expected_source_run_id,
    )
    upstream_lineage = _json_object(assessment.get("lineage"))
    current_lineage = resolve_candidate_source_lineage(
        connection,
        candidate_instance_id,
        source_run_id=_first_text(upstream_lineage.get("source_run_id")),
        generated_by="pipeline_coherency_probe",
        fallback_trade_date=_first_text(
            _row_dict(strategy).get("trade_date"),
            _row_dict(risk).get("trade_date"),
        ),
    )
    reasons = list(assessment.get("reason_codes") or [])
    if not current_lineage["candidate_present"]:
        reasons.append("CANDIDATE_SOURCE_MISSING")
    if upstream_lineage and (
        upstream_lineage.get("source_watermark_hash")
        != current_lineage.get("source_watermark_hash")
    ):
        reasons.append("CURRENT_SOURCE_WATERMARK_MISMATCH")
    reasons = _dedupe(reasons)
    return {
        **assessment,
        "status": "PASS" if not reasons else "FAIL",
        "reason_codes": reasons,
        "current_lineage": current_lineage,
    }


def lineage_for_entry_input(
    item_raw_context: Mapping[str, Any],
    *,
    generated_by: str,
) -> dict[str, Any] | None:
    assessment = _json_object(item_raw_context.get("pipeline_coherency"))
    lineage = _json_object(assessment.get("lineage"))
    if not lineage:
        return None
    lineage["generated_by"] = generated_by
    lineage["data_age_sec"] = _age_seconds(lineage.get("source_observed_at"))
    if "source_watermark_json" not in lineage:
        lineage["source_watermark_json"] = canonical_json(
            _json_object(lineage.get("source_watermark"))
        )
    return lineage


def lineage_db_values(lineage: Mapping[str, Any]) -> tuple[Any, ...]:
    watermark_json = lineage.get("source_watermark_json")
    if not isinstance(watermark_json, str):
        watermark_json = canonical_json(_json_object(lineage.get("source_watermark")))
    return (
        lineage.get("source_run_id"),
        watermark_json,
        lineage.get("source_watermark_hash"),
        lineage.get("source_event_id"),
        lineage.get("source_observed_at"),
        lineage.get("data_age_sec"),
        lineage.get("generated_by"),
    )


def assess_order_plan_lineage(
    connection: sqlite3.Connection,
    order_plan: Mapping[str, Any],
    evaluation: sqlite3.Row | Mapping[str, Any] | None,
    *,
    max_age_sec: float,
) -> dict[str, Any]:
    candidate_id = str(order_plan.get("candidate_instance_id") or "")
    strategy = connection.execute(
        "SELECT * FROM strategy_observations_latest WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    risk = connection.execute(
        "SELECT * FROM risk_observations_latest WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    upstream = assess_strategy_risk_lineage(
        strategy,
        risk,
        max_age_sec=max_age_sec,
    )
    reasons = list(upstream["reason_codes"])
    strategy_data = _row_dict(strategy)
    risk_data = _row_dict(risk)
    evaluation_data = _row_dict(evaluation)
    plan_lineage = lineage_from_row(order_plan)
    evaluation_lineage = lineage_from_row(evaluation_data)
    expected = {
        "strategy_observation_id": strategy_data.get("strategy_observation_id"),
        "risk_observation_id": risk_data.get("risk_observation_id"),
        "entry_timing_evaluation_id": evaluation_data.get(
            "entry_timing_evaluation_id"
        ),
        "source_run_id": strategy_data.get("source_run_id"),
        "source_watermark_hash": strategy_data.get("source_watermark_hash"),
        "source_observed_at": strategy_data.get("source_observed_at"),
        "trade_date": strategy_data.get("trade_date"),
    }
    for field_name in (
        "strategy_observation_id",
        "risk_observation_id",
        "entry_timing_evaluation_id",
        "source_run_id",
        "source_watermark_hash",
        "source_observed_at",
        "trade_date",
    ):
        if not order_plan.get(field_name):
            reasons.append(f"ORDER_PLAN_{field_name.upper()}_MISSING")
        elif order_plan.get(field_name) != expected.get(field_name):
            reasons.append(f"ORDER_PLAN_{field_name.upper()}_MISMATCH")
    if evaluation_data:
        for field_name in (
            "strategy_observation_id",
            "risk_observation_id",
            "source_run_id",
            "source_watermark_hash",
            "source_observed_at",
            "trade_date",
        ):
            if evaluation_data.get(field_name) != expected.get(field_name):
                reasons.append(f"ENTRY_TIMING_{field_name.upper()}_MISMATCH")
    else:
        reasons.append("ENTRY_TIMING_EVALUATION_MISSING")
    if plan_lineage is None:
        reasons.append("ORDER_PLAN_LINEAGE_MISSING")
    elif not plan_lineage["source_watermark_hash_valid"]:
        reasons.append("ORDER_PLAN_WATERMARK_HASH_INVALID")
    if evaluation_data and evaluation_lineage is None:
        reasons.append("ENTRY_TIMING_LINEAGE_MISSING")
    elif evaluation_lineage is not None and not evaluation_lineage[
        "source_watermark_hash_valid"
    ]:
        reasons.append("ENTRY_TIMING_WATERMARK_HASH_INVALID")
    reasons = _dedupe(reasons)
    return {
        "status": "PASS" if not reasons else "FAIL",
        "reason_codes": reasons,
        "expected": expected,
        "upstream": upstream,
        "read_only": True,
        "no_order_side_effects": True,
    }


def build_pipeline_coherency_status(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    max_age_sec: float = 60.0,
    limit: int = 100,
) -> dict[str, Any]:
    target_trade_date = _resolve_pipeline_trade_date(connection, trade_date)
    if target_trade_date is None:
        return _empty_pipeline_coherency_status(max_age_sec=max_age_sec)
    rows = connection.execute(
        """
        WITH entry_latest AS (
            SELECT *
            FROM (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.candidate_instance_id
                        ORDER BY e.evaluated_at DESC, e.entry_timing_evaluation_id DESC
                    ) AS row_number
                FROM entry_timing_evaluations AS e
                WHERE e.trade_date = ?
            )
            WHERE row_number = 1
        ),
        plan_latest AS (
            SELECT *
            FROM (
                SELECT
                    o.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.candidate_instance_id
                        ORDER BY o.created_at DESC, o.order_plan_id DESC
                    ) AS row_number
                FROM order_plan_drafts_latest AS o
                WHERE o.trade_date = ?
            )
            WHERE row_number = 1
        ),
        pipeline_candidates AS (
            SELECT candidate_instance_id FROM strategy_observations_latest
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM risk_observations_latest
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM entry_latest
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM plan_latest
            WHERE trade_date = ?
        )
        SELECT
            p.candidate_instance_id AS pipeline__candidate_instance_id,
            COALESCE(s.trade_date, r.trade_date, e.trade_date, o.trade_date)
                AS pipeline__trade_date,
            COALESCE(s.code, r.code, e.code, o.code) AS pipeline__code,
            s.*,
            r.risk_observation_id AS risk__risk_observation_id,
            r.strategy_observation_id AS risk__strategy_observation_id,
            r.trade_date AS risk__trade_date,
            r.evaluated_at AS risk__evaluated_at,
            r.overall_status AS risk__overall_status,
            r.source_run_id AS risk__source_run_id,
            r.source_watermark AS risk__source_watermark,
            r.source_watermark_hash AS risk__source_watermark_hash,
            r.source_event_id AS risk__source_event_id,
            r.source_observed_at AS risk__source_observed_at,
            r.data_age_sec AS risk__data_age_sec,
            r.generated_by AS risk__generated_by,
            e.entry_timing_evaluation_id AS entry__entry_timing_evaluation_id,
            e.order_plan_id AS entry__order_plan_id,
            e.trade_date AS entry__trade_date,
            e.evaluated_at AS entry__evaluated_at,
            e.status AS entry__status,
            e.strategy_observation_id AS entry__strategy_observation_id,
            e.risk_observation_id AS entry__risk_observation_id,
            e.source_run_id AS entry__source_run_id,
            e.source_watermark AS entry__source_watermark,
            e.source_watermark_hash AS entry__source_watermark_hash,
            e.source_event_id AS entry__source_event_id,
            e.source_observed_at AS entry__source_observed_at,
            e.data_age_sec AS entry__data_age_sec,
            e.generated_by AS entry__generated_by,
            o.order_plan_id AS plan__order_plan_id,
            o.trade_date AS plan__trade_date,
            o.status AS plan__status,
            o.created_at AS plan__created_at,
            o.entry_timing_evaluation_id AS plan__entry_timing_evaluation_id,
            o.strategy_observation_id AS plan__strategy_observation_id,
            o.risk_observation_id AS plan__risk_observation_id,
            o.source_run_id AS plan__source_run_id,
            o.source_watermark AS plan__source_watermark,
            o.source_watermark_hash AS plan__source_watermark_hash,
            o.source_event_id AS plan__source_event_id,
            o.source_observed_at AS plan__source_observed_at,
            o.data_age_sec AS plan__data_age_sec,
            o.generated_by AS plan__generated_by
        FROM pipeline_candidates AS p
        LEFT JOIN strategy_observations_latest AS s
            ON s.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN risk_observations_latest AS r
            ON r.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN entry_latest AS e
            ON e.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN plan_latest AS o
            ON o.candidate_instance_id = p.candidate_instance_id
        ORDER BY COALESCE(
            s.evaluated_at,
            r.evaluated_at,
            e.evaluated_at,
            o.created_at
        ) DESC, p.candidate_instance_id
        LIMIT ?
        """,
        (
            target_trade_date,
            target_trade_date,
            target_trade_date,
            target_trade_date,
            target_trade_date,
            target_trade_date,
            min(max(int(limit), 1), 500),
        ),
    ).fetchall()
    items = [
        _coherency_item(connection, row, max_age_sec=max_age_sec) for row in rows
    ]
    fail_count = sum(1 for item in items if item["status"] == "FAIL")
    warn_count = sum(1 for item in items if item["status"] == "WARN")
    status = "FAIL" if fail_count else ("WARN" if warn_count or not items else "PASS")
    reasons = _dedupe(
        reason for item in items for reason in item.get("reason_codes", [])
    )
    if not items:
        reasons.append("NO_PIPELINE_OBSERVATIONS")
    return {
        "status": status,
        "trade_date": target_trade_date,
        "reason_codes": reasons,
        "candidate_count": len(items),
        "coherent_count": sum(1 for item in items if item["status"] == "PASS"),
        "warning_count": warn_count,
        "mismatch_count": fail_count,
        "missing_lineage_count": sum(
            1
            for item in items
            if any("LINEAGE_MISSING" in reason for reason in item["reason_codes"])
        ),
        "stale_count": sum(
            1
            for item in items
            if any("STALE" in reason for reason in item["reason_codes"])
        ),
        "max_age_sec": float(max_age_sec),
        "items": items,
        "generated_at": datetime_to_wire(utc_now()),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def _resolve_pipeline_trade_date(
    connection: sqlite3.Connection,
    requested_trade_date: str | None,
) -> str | None:
    if requested_trade_date is not None and str(requested_trade_date).strip():
        return str(requested_trade_date).strip()
    row = connection.execute(
        """
        SELECT MAX(trade_date) AS trade_date
        FROM (
            SELECT trade_date FROM candidates
            UNION ALL
            SELECT trade_date FROM strategy_observations_latest
            UNION ALL
            SELECT trade_date FROM risk_observations_latest
            UNION ALL
            SELECT trade_date FROM entry_timing_evaluations
            UNION ALL
            SELECT trade_date FROM order_plan_drafts_latest
        )
        """
    ).fetchone()
    return _first_text(row["trade_date"] if row is not None else None)


def _empty_pipeline_coherency_status(*, max_age_sec: float) -> dict[str, Any]:
    return {
        "status": "WARN",
        "trade_date": None,
        "reason_codes": ["NO_PIPELINE_OBSERVATIONS"],
        "candidate_count": 0,
        "coherent_count": 0,
        "warning_count": 0,
        "mismatch_count": 0,
        "missing_lineage_count": 0,
        "stale_count": 0,
        "max_age_sec": float(max_age_sec),
        "items": [],
        "generated_at": datetime_to_wire(utc_now()),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def _coherency_item(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    max_age_sec: float,
) -> dict[str, Any]:
    data = _row_dict(row)
    strategy = _stage_data(data, "", "strategy_observation_id", "evaluated_at")
    risk = _stage_data(
        data,
        "risk__",
        "risk_observation_id",
        "evaluated_at",
        strategy_observation_id=data.get("risk__strategy_observation_id"),
    )
    entry = _stage_data(
        data,
        "entry__",
        "entry_timing_evaluation_id",
        "evaluated_at",
        strategy_observation_id=data.get("entry__strategy_observation_id"),
        risk_observation_id=data.get("entry__risk_observation_id"),
    )
    plan = _stage_data(
        data,
        "plan__",
        "order_plan_id",
        "created_at",
        strategy_observation_id=data.get("plan__strategy_observation_id"),
        risk_observation_id=data.get("plan__risk_observation_id"),
        entry_timing_evaluation_id=data.get("plan__entry_timing_evaluation_id"),
    )
    upstream = assess_candidate_pipeline_lineage(
        connection,
        str(data.get("pipeline__candidate_instance_id") or ""),
        strategy,
        risk,
        max_age_sec=max_age_sec,
    )
    reasons = list(upstream["reason_codes"])
    if not entry.get("entry_timing_evaluation_id"):
        reasons.append("ENTRY_TIMING_MISSING")
    else:
        _append_stage_mismatches(
            reasons,
            stage_name="ENTRY_TIMING",
            stage=entry,
            strategy=strategy,
            risk=risk,
        )
        entry_lineage = lineage_from_row(entry)
        if entry_lineage is None:
            reasons.append("ENTRY_TIMING_LINEAGE_MISSING")
        elif not entry_lineage["source_watermark_hash_valid"]:
            reasons.append("ENTRY_TIMING_WATERMARK_HASH_INVALID")
    if not plan.get("order_plan_id"):
        reasons.append("ORDER_PLAN_MISSING")
    else:
        _append_stage_mismatches(
            reasons,
            stage_name="ORDER_PLAN",
            stage=plan,
            strategy=strategy,
            risk=risk,
            entry=entry,
        )
        plan_lineage = lineage_from_row(plan)
        if plan_lineage is None:
            reasons.append("ORDER_PLAN_LINEAGE_MISSING")
        elif not plan_lineage["source_watermark_hash_valid"]:
            reasons.append("ORDER_PLAN_WATERMARK_HASH_INVALID")
    age_sec = _age_seconds(strategy.get("source_observed_at"))
    if age_sec > float(max_age_sec):
        reasons.append(
            "PLAN_READY_SOURCE_STALE"
            if plan.get("status") == "PLAN_READY"
            else "PIPELINE_SOURCE_STALE"
        )
    reasons = _dedupe(reasons)
    fail_reasons = [
        reason
        for reason in reasons
        if reason not in {"ENTRY_TIMING_MISSING", "ORDER_PLAN_MISSING", "PIPELINE_SOURCE_STALE"}
    ]
    status = "FAIL" if fail_reasons else ("WARN" if reasons else "PASS")
    return {
        "candidate_instance_id": data.get("pipeline__candidate_instance_id"),
        "trade_date": data.get("pipeline__trade_date"),
        "code": data.get("pipeline__code"),
        "status": status,
        "reason_codes": reasons,
        "data_age_sec": age_sec,
        "current_source": _public_lineage_metadata(
            _json_object(upstream.get("current_lineage"))
        ),
        "stages": {
            "strategy": _public_stage_metadata(strategy),
            "risk": _public_stage_metadata(risk),
            "entry_timing": _public_stage_metadata(entry),
            "order_plan": _public_stage_metadata(plan),
        },
    }


def _append_stage_mismatches(
    reasons: list[str],
    *,
    stage_name: str,
    stage: Mapping[str, Any],
    strategy: Mapping[str, Any],
    risk: Mapping[str, Any],
    entry: Mapping[str, Any] | None = None,
) -> None:
    checks = {
        "STRATEGY_OBSERVATION": (
            stage.get("strategy_observation_id"),
            strategy.get("strategy_observation_id"),
        ),
        "RISK_OBSERVATION": (
            stage.get("risk_observation_id"),
            risk.get("risk_observation_id"),
        ),
        "SOURCE_RUN": (stage.get("source_run_id"), strategy.get("source_run_id")),
        "WATERMARK": (
            stage.get("source_watermark_hash"),
            strategy.get("source_watermark_hash"),
        ),
        "TRADE_DATE": (stage.get("trade_date"), strategy.get("trade_date")),
    }
    if entry is not None:
        checks["ENTRY_TIMING_EVALUATION"] = (
            stage.get("entry_timing_evaluation_id"),
            entry.get("entry_timing_evaluation_id"),
        )
    for label, (actual, expected) in checks.items():
        if actual is None:
            reasons.append(f"{stage_name}_{label}_LINEAGE_MISSING")
        elif actual != expected:
            reasons.append(f"{stage_name}_{label}_MISMATCH")


def _stage_data(
    data: Mapping[str, Any],
    prefix: str,
    id_field: str,
    generated_at_field: str,
    **extra: Any,
) -> dict[str, Any]:
    def value(name: str) -> Any:
        return data.get(f"{prefix}{name}")

    stage = {
        id_field: value(id_field),
        "trade_date": value("trade_date"),
        "status": value("status") or value("overall_status"),
        "generated_at": value(generated_at_field),
        "source_run_id": value("source_run_id"),
        "source_watermark": value("source_watermark"),
        "source_watermark_hash": value("source_watermark_hash"),
        "source_event_id": value("source_event_id"),
        "source_observed_at": value("source_observed_at"),
        "data_age_sec": _age_seconds(value("source_observed_at")),
        "generated_by": value("generated_by"),
    }
    stage.update(extra)
    return stage


def _public_stage_metadata(stage: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(stage)
    data["source_watermark"] = _json_object(data.get("source_watermark"))
    lineage = lineage_from_row(stage)
    data["source_watermark_hash_valid"] = (
        None if lineage is None else bool(lineage["source_watermark_hash_valid"])
    )
    return data


def _public_lineage_metadata(lineage: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(lineage)
    data.pop("source_watermark_json", None)
    return data


def _lineage_assessment(
    *,
    status: str,
    reasons: list[str],
    strategy: Mapping[str, Any],
    risk: Mapping[str, Any],
    lineage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason_codes": _dedupe(reasons),
        "strategy_observation_id": strategy.get("strategy_observation_id"),
        "risk_observation_id": risk.get("risk_observation_id"),
        "lineage": dict(lineage or {}),
        "read_only": True,
        "no_order_side_effects": True,
    }


def _age_seconds(value: object, *, now: datetime | None = None) -> float:
    if not value:
        return 0.0
    try:
        observed = parse_timestamp(value, "source_observed_at")
    except (TypeError, ValueError):
        return 0.0
    current = now or utc_now()
    return round(max((current - observed).total_seconds(), 0.0), 3)


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _row_dict(row: sqlite3.Row | Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _first_text(*values: object) -> str | None:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _dedupe(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
