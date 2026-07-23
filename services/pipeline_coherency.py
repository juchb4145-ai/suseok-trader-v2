from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from typing import Any

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

PipelineDispositionResolver = Callable[
    [str, Sequence[Mapping[str, Any]]], Mapping[str, Any]
]
PipelineLegacyEvidenceResolver = Callable[
    [str, Sequence[Mapping[str, Any]]], Mapping[str, Any]
]

_PIPELINE_RCA_PAGE_LIMIT = 500
_PIPELINE_RCA_CLASSIFICATIONS = (
    "HISTORICAL_CLOSED",
    "MISSING_CANDIDATE_MANUAL_REVIEW",
    "STALE_OTHER_DATE_MANUAL_REVIEW",
    "ACTIVE_CURRENT",
)
_PIPELINE_LEGACY_EVIDENCE_ITEM_KEYS = frozenset(
    {
        "contract",
        "status",
        "authoritative",
        "pre_schema59",
        "terminal_closed",
        "active_source_zero",
        "current_source_no_drift",
        "pipeline_fingerprint",
        "subject_version",
        "source_fingerprint",
        "closure_fingerprint",
        "pipeline_stage_fingerprint",
        "provenance_sha256",
    }
)


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


def build_fast5_plan_ready_coherency_status(
    connection: sqlite3.Connection,
    *,
    pipeline_status: Mapping[str, Any],
    trade_date: str,
    max_age_sec: float = 60.0,
    as_of: datetime | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Qualify every latest, unexpired PLAN_READY row for the FAST-5 gate.

    The canonical whole-inventory status remains a diagnostic.  FAST-5 only needs
    every currently actionable plan to be internally coherent, while historical
    non-ready rows may remain WARN/FAIL diagnostics.  The caller still has to prove
    whole-inventory count/coverage separately before accepting this result.
    """

    observed_at = as_of or utc_now()
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM order_plan_drafts_latest
        WHERE trade_date = ?
          AND status = 'PLAN_READY'
        ORDER BY created_at DESC, order_plan_id DESC
        LIMIT ?
        """,
        (str(trade_date), bounded_limit + 1),
    ).fetchall()
    selection_truncated = len(rows) > bounded_limit
    selected_rows = rows[:bounded_limit]
    diagnostic_items = {
        str(item.get("candidate_instance_id") or ""): item
        for item in pipeline_status.get("items", [])
        if isinstance(item, Mapping)
    }
    unexpired_items: list[dict[str, Any]] = []
    expired_plan_ready_count = 0
    invalid_expiry_count = 0
    invalid_expiry_ids: list[str] = []
    for row in selected_rows:
        plan = _row_dict(row)
        order_plan_id = str(plan.get("order_plan_id") or "")
        try:
            expires_at = parse_timestamp(plan.get("expires_at"), "expires_at")
            unexpired = expires_at > observed_at
        except (TypeError, ValueError):
            invalid_expiry_count += 1
            invalid_expiry_ids.append(order_plan_id)
            continue
        if not unexpired:
            expired_plan_ready_count += 1
            continue

        candidate_id = str(plan.get("candidate_instance_id") or "")
        canonical = diagnostic_items.get(candidate_id)
        draft_row = connection.execute(
            "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
            (plan.get("order_plan_id"),),
        ).fetchone()
        draft = _row_dict(draft_row)
        plan_for_lineage = draft or plan
        evaluation = connection.execute(
            """
            SELECT *
            FROM entry_timing_evaluations
            WHERE entry_timing_evaluation_id = ?
            """,
            (plan_for_lineage.get("entry_timing_evaluation_id"),),
        ).fetchone()
        exact_lineage = assess_order_plan_lineage(
            connection,
            plan_for_lineage,
            evaluation,
            max_age_sec=max_age_sec,
        )
        reasons: list[str] = []
        if not draft:
            reasons.append("ORDER_PLAN_DRAFT_MISSING")
        else:
            for field_name in (
                "candidate_instance_id",
                "trade_date",
                "status",
                "expires_at",
                "entry_timing_evaluation_id",
                "strategy_observation_id",
                "risk_observation_id",
                "source_run_id",
                "source_watermark_hash",
                "source_observed_at",
            ):
                if draft.get(field_name) != plan.get(field_name):
                    reasons.append(
                        f"ORDER_PLAN_LATEST_{field_name.upper()}_MISMATCH"
                    )
        evaluation_data = _row_dict(evaluation)
        if evaluation_data:
            if not evaluation_data.get("order_plan_id"):
                reasons.append("ENTRY_TIMING_ORDER_PLAN_LINEAGE_MISSING")
            elif evaluation_data.get("order_plan_id") != plan.get(
                "order_plan_id"
            ):
                reasons.append("ENTRY_TIMING_ORDER_PLAN_MISMATCH")
        canonical_plan_id: object = None
        if canonical is None:
            reasons.append("PLAN_READY_PIPELINE_CANDIDATE_NOT_COVERED")
        else:
            reasons.extend(canonical.get("reason_codes") or [])
            stages = canonical.get("stages")
            if isinstance(stages, Mapping):
                order_plan_stage = stages.get("order_plan")
                if isinstance(order_plan_stage, Mapping):
                    canonical_plan_id = order_plan_stage.get("order_plan_id")
            if canonical_plan_id != plan.get("order_plan_id"):
                reasons.append("PLAN_READY_LATEST_ORDER_PLAN_MISMATCH")
        reasons.extend(exact_lineage.get("reason_codes") or [])
        reasons = _dedupe(reasons)
        unexpired_items.append(
            {
                "candidate_instance_id": candidate_id,
                "order_plan_id": order_plan_id,
                "expires_at": plan.get("expires_at"),
                "status": "PASS" if not reasons else "FAIL",
                "reason_codes": reasons,
            }
        )

    non_pass_count = sum(
        1 for item in unexpired_items if item.get("status") != "PASS"
    )
    missing_lineage_count = sum(
        1
        for item in unexpired_items
        if any("MISSING" in reason for reason in item.get("reason_codes", []))
    )
    stale_count = sum(
        1
        for item in unexpired_items
        if any("STALE" in reason for reason in item.get("reason_codes", []))
    )
    mismatch_count = sum(
        1
        for item in unexpired_items
        if any(
            "MISMATCH" in reason or "INVALID" in reason
            for reason in item.get("reason_codes", [])
        )
    )
    reason_codes = _dedupe(
        reason
        for item in unexpired_items
        for reason in item.get("reason_codes", [])
    )
    if not unexpired_items:
        reason_codes.append("NO_UNEXPIRED_PLAN_READY")
    if invalid_expiry_count:
        reason_codes.append("PLAN_READY_EXPIRY_INVALID")
    if selection_truncated:
        reason_codes.append("PLAN_READY_SELECTION_TRUNCATED")
    passed = bool(
        unexpired_items
        and non_pass_count == 0
        and invalid_expiry_count == 0
        and not selection_truncated
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "trade_date": str(trade_date),
        "reason_codes": _dedupe(reason_codes),
        "latest_plan_ready_count": len(selected_rows),
        "unexpired_plan_ready_count": len(unexpired_items),
        "coherent_plan_ready_count": sum(
            1 for item in unexpired_items if item.get("status") == "PASS"
        ),
        "non_pass_plan_ready_count": non_pass_count,
        "missing_lineage_plan_ready_count": missing_lineage_count,
        "stale_plan_ready_count": stale_count,
        "mismatch_plan_ready_count": mismatch_count,
        "expired_plan_ready_count": expired_plan_ready_count,
        "invalid_expiry_plan_ready_count": invalid_expiry_count,
        "invalid_expiry_order_plan_ids": invalid_expiry_ids,
        "selection_truncated": selection_truncated,
        "max_age_sec": float(max_age_sec),
        "items": unexpired_items,
        "generated_at": datetime_to_wire(observed_at),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def build_pipeline_coherency_rca_status(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    max_age_sec: float = 60.0,
    limit: int = 100,
    offset: int = 0,
    candidate_instance_id: str | None = None,
    disposition_resolver: PipelineDispositionResolver | None = None,
    legacy_evidence_resolver: PipelineLegacyEvidenceResolver | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Build an exact, paginated FAST-0 RCA view without changing canonical state.

    ``disposition_resolver`` is an optional read-only integration hook for a future
    append-only ledger.  It receives the target trade date and detached subject
    summaries, but never the writable SQLite connection, and returns ``schema_ready``
    plus an ``items`` mapping keyed by candidate instance id.  Until that storage
    contract exists, the default is deliberately fail-closed.
    """

    target_trade_date = _resolve_pipeline_trade_date(connection, trade_date)
    bounded_limit = min(max(int(limit), 1), _PIPELINE_RCA_PAGE_LIMIT)
    bounded_offset = max(int(offset), 0)
    exact_candidate_id = (
        None
        if candidate_instance_id is None
        else str(candidate_instance_id).strip()
    )
    observed_at = as_of or utc_now()
    if target_trade_date is None:
        return _empty_pipeline_coherency_rca_status(
            limit=bounded_limit,
            offset=bounded_offset,
            candidate_instance_id=exact_candidate_id,
        )

    return _build_pipeline_rca_read_snapshot(
        connection,
        trade_date=target_trade_date,
        max_age_sec=max_age_sec,
        limit=bounded_limit,
        offset=bounded_offset,
        candidate_instance_id=exact_candidate_id,
        disposition_resolver=disposition_resolver,
        legacy_evidence_resolver=legacy_evidence_resolver,
        as_of=observed_at,
    )


def _build_pipeline_rca_read_snapshot(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    max_age_sec: float,
    limit: int,
    offset: int,
    candidate_instance_id: str | None,
    disposition_resolver: PipelineDispositionResolver | None,
    legacy_evidence_resolver: PipelineLegacyEvidenceResolver | None,
    as_of: datetime,
) -> dict[str, Any]:
    nested_transaction = connection.in_transaction
    savepoint = "fast0_pipeline_rca_read_snapshot"
    if nested_transaction:
        connection.execute(f"SAVEPOINT {savepoint}")
    else:
        connection.execute("BEGIN DEFERRED")
    try:
        report = _build_pipeline_rca_snapshot_report(
            connection,
            trade_date=trade_date,
            max_age_sec=max_age_sec,
            limit=limit,
            offset=offset,
            candidate_instance_id=candidate_instance_id,
            disposition_resolver=disposition_resolver,
            legacy_evidence_resolver=legacy_evidence_resolver,
            as_of=as_of,
        )
    except BaseException:
        if nested_transaction:
            connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        else:
            connection.rollback()
        raise
    if nested_transaction:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
    else:
        connection.rollback()
    return report


def _build_pipeline_rca_snapshot_report(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    max_age_sec: float,
    limit: int,
    offset: int,
    candidate_instance_id: str | None,
    disposition_resolver: PipelineDispositionResolver | None,
    legacy_evidence_resolver: PipelineLegacyEvidenceResolver | None,
    as_of: datetime,
) -> dict[str, Any]:
    inventory_ids = _pipeline_rca_inventory_ids(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_instance_id,
    )
    inventory_digest = _pipeline_rca_inventory_digest(inventory_ids)
    full_count = _pipeline_rca_inventory_count(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_instance_id,
    )
    summaries: list[dict[str, Any]] = []
    page_items: list[dict[str, Any]] = []
    seen_candidate_ids: list[str] = []
    scan_offset = 0
    while scan_offset < full_count:
        rows = _pipeline_rca_rows(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_instance_id,
            limit=_PIPELINE_RCA_PAGE_LIMIT,
            offset=scan_offset,
        )
        if not rows:
            break
        for row in rows:
            item_index = len(summaries)
            item = _pipeline_rca_item(
                connection,
                row,
                target_trade_date=trade_date,
                max_age_sec=max_age_sec,
                as_of=as_of,
            )
            seen_candidate_ids.append(
                str(item.get("candidate_instance_id") or "")
            )
            summaries.append(_pipeline_rca_subject_summary(item))
            if offset <= item_index < offset + limit:
                page_items.append(item)
        scan_offset += len(rows)

    ending_inventory_ids = _pipeline_rca_inventory_ids(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_instance_id,
    )
    ending_inventory_digest = _pipeline_rca_inventory_digest(ending_inventory_ids)
    inventory_duplicate_key_count = max(
        len(inventory_ids) - len(set(inventory_ids)),
        len(seen_candidate_ids) - len(set(seen_candidate_ids)),
        len(ending_inventory_ids) - len(set(ending_inventory_ids)),
    )
    inventory_count_consistent = bool(
        inventory_duplicate_key_count == 0
        and full_count == len(inventory_ids)
        and full_count == len(summaries)
        and inventory_ids == seen_candidate_ids
        and inventory_ids == ending_inventory_ids
        and inventory_digest == ending_inventory_digest
    )
    legacy_evidence_state = _resolve_pipeline_legacy_evidence(
        connection,
        trade_date=trade_date,
        subjects=summaries,
        resolver=legacy_evidence_resolver,
    )
    _apply_pipeline_legacy_evidence(
        summaries,
        page_items=page_items,
        evidence_state=legacy_evidence_state,
    )
    disposition_state = _resolve_pipeline_dispositions(
        connection,
        trade_date=trade_date,
        subjects=summaries,
        resolver=disposition_resolver,
    )
    for item in page_items:
        item["effective_disposition"] = _effective_pipeline_disposition(
            disposition_state,
            str(item.get("candidate_instance_id") or ""),
        )

    canonical = _pipeline_rca_canonical_summary(summaries)
    qualification = _pipeline_rca_qualification_summary(
        summaries,
        disposition_state=disposition_state,
        inventory_count_consistent=inventory_count_consistent,
    )
    returned_count = len(page_items)
    has_more = offset + returned_count < full_count
    next_offset = offset + returned_count if has_more else None
    disposition_public = {
        key: value
        for key, value in disposition_state.items()
        if key != "items"
    }
    legacy_evidence_public = {
        key: value
        for key, value in legacy_evidence_state.items()
        if key != "items"
    }
    return {
        "status": qualification["status"],
        "qualification_status": qualification["status"],
        "qualification_reason_codes": qualification["reason_codes"],
        "canonical_status": canonical["status"],
        "canonical_reason_codes": canonical["reason_codes"],
        "canonical": canonical,
        "trade_date": trade_date,
        "candidate_instance_id_filter": candidate_instance_id,
        "classification_counts": qualification["classification_counts"],
        "legacy_warn_candidate_count": qualification[
            "legacy_warn_candidate_count"
        ],
        "manual_review_count": qualification["manual_review_count"],
        "manual_review_pending_count": qualification[
            "manual_review_pending_count"
        ],
        "legacy_evidence_ready": legacy_evidence_state["evidence_ready"],
        "legacy_evidence": legacy_evidence_public,
        "unexpired_plan_count": qualification["unexpired_plan_count"],
        "plan_expiry_unknown_count": qualification[
            "plan_expiry_unknown_count"
        ],
        "current_source_drift_count": qualification[
            "current_source_drift_count"
        ],
        "current_source_drift_pending_count": qualification[
            "current_source_drift_pending_count"
        ],
        "current_source_drift_unknown_count": qualification[
            "current_source_drift_unknown_count"
        ],
        "current_source_drift_unknown_pending_count": qualification[
            "current_source_drift_unknown_pending_count"
        ],
        "disposition_required_count": qualification[
            "disposition_required_count"
        ],
        "disposition_pending_count": qualification[
            "disposition_pending_count"
        ],
        "schema_ready": disposition_state["schema_ready"],
        "disposition": disposition_public,
        "limit": limit,
        "offset": offset,
        "returned_count": returned_count,
        "full_count": full_count,
        "has_more": has_more,
        "next_offset": next_offset,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "returned_count": returned_count,
            "full_count": full_count,
            "has_more": has_more,
            "next_offset": next_offset,
        },
        "inventory_count_consistent": inventory_count_consistent,
        "inventory_digest": inventory_digest,
        "inventory_end_digest": ending_inventory_digest,
        "inventory_duplicate_key_count": inventory_duplicate_key_count,
        "items": page_items,
        "fingerprint_contract": {
            "algorithm": "SHA-256",
            "canonicalization": "canonical_json",
            "pipeline_fingerprint": "pipeline-coherency-rca.v1",
            "subject_version": "pipeline-coherency-subject.v1",
        },
        "generated_at": datetime_to_wire(as_of),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


_PIPELINE_RCA_CTE = """
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
pipeline_inventory AS (
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
),
filtered_inventory AS (
    SELECT candidate_instance_id
    FROM pipeline_inventory
    WHERE (? IS NULL OR candidate_instance_id = ?)
)
"""


def _pipeline_rca_inventory_count(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str | None,
) -> int:
    row = connection.execute(
        _PIPELINE_RCA_CTE + "SELECT COUNT(*) AS count FROM filtered_inventory",
        _pipeline_rca_params(trade_date, candidate_instance_id),
    ).fetchone()
    return int(row["count"] if row is not None else 0)


def _pipeline_rca_inventory_ids(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str | None,
) -> list[str]:
    rows = connection.execute(
        _PIPELINE_RCA_CTE
        + """
        SELECT p.candidate_instance_id
        FROM filtered_inventory AS p
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
        """,
        _pipeline_rca_params(trade_date, candidate_instance_id),
    ).fetchall()
    return [str(row["candidate_instance_id"] or "") for row in rows]


def _pipeline_rca_inventory_digest(candidate_instance_ids: Sequence[str]) -> str:
    return _canonical_sha256(
        {
            "contract": "pipeline-coherency-inventory.v1",
            "candidate_instance_ids": list(candidate_instance_ids),
        }
    )


def _pipeline_rca_rows(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str | None,
    limit: int,
    offset: int,
) -> list[sqlite3.Row]:
    rows = connection.execute(
        _PIPELINE_RCA_CTE
        + """
        SELECT
            p.candidate_instance_id AS pipeline__candidate_instance_id,
            COALESCE(s.trade_date, r.trade_date, e.trade_date, o.trade_date)
                AS pipeline__trade_date,
            COALESCE(s.code, r.code, e.code, o.code) AS pipeline__code,
            COALESCE(s.evaluated_at, r.evaluated_at, e.evaluated_at, o.created_at)
                AS pipeline__sort_at,
            c.candidate_instance_id AS candidate__candidate_instance_id,
            c.trade_date AS candidate__trade_date,
            c.code AS candidate__code,
            c.state AS candidate__state,
            c.detected_at AS candidate__detected_at,
            c.last_seen_at AS candidate__last_seen_at,
            c.state_updated_at AS candidate__state_updated_at,
            c.closed_at AS candidate__closed_at,
            c.active_source_count AS candidate__active_source_count,
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
            o.expires_at AS plan__expires_at,
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
        FROM filtered_inventory AS p
        LEFT JOIN candidates AS c
            ON c.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN strategy_observations_latest AS s
            ON s.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN risk_observations_latest AS r
            ON r.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN entry_latest AS e
            ON e.candidate_instance_id = p.candidate_instance_id
        LEFT JOIN plan_latest AS o
            ON o.candidate_instance_id = p.candidate_instance_id
        ORDER BY pipeline__sort_at DESC, p.candidate_instance_id
        LIMIT ? OFFSET ?
        """,
        _pipeline_rca_params(trade_date, candidate_instance_id)
        + (min(max(int(limit), 1), _PIPELINE_RCA_PAGE_LIMIT), max(int(offset), 0)),
    ).fetchall()
    return list(rows)


def _pipeline_rca_params(
    trade_date: str,
    candidate_instance_id: str | None,
) -> tuple[Any, ...]:
    return (
        trade_date,
        trade_date,
        trade_date,
        trade_date,
        trade_date,
        trade_date,
        candidate_instance_id,
        candidate_instance_id,
    )


def _pipeline_rca_item(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    target_trade_date: str,
    max_age_sec: float,
    as_of: datetime,
) -> dict[str, Any]:
    data = _row_dict(row)
    canonical = _coherency_item(connection, row, max_age_sec=max_age_sec)
    candidate = {
        "present": bool(data.get("candidate__candidate_instance_id")),
        "candidate_instance_id": data.get("candidate__candidate_instance_id"),
        "trade_date": data.get("candidate__trade_date"),
        "code": data.get("candidate__code"),
        "state": data.get("candidate__state"),
        "detected_at": data.get("candidate__detected_at"),
        "last_seen_at": data.get("candidate__last_seen_at"),
        "state_updated_at": data.get("candidate__state_updated_at"),
        "closed_at": data.get("candidate__closed_at"),
        "active_source_count": _optional_int(
            data.get("candidate__active_source_count")
        ),
    }
    latest_plan = _pipeline_rca_latest_plan(data, as_of=as_of)
    classification = _pipeline_rca_classification(
        candidate,
        target_trade_date=target_trade_date,
    )
    drift = _pipeline_rca_current_source_drift(
        canonical,
        candidate=candidate,
        classification=classification,
    )
    legacy_warn_eligible, legacy_warn_reasons = _legacy_warn_candidate(
        canonical,
        candidate=candidate,
        latest_plan=latest_plan,
        classification=classification,
        current_source_drift=drift["value"],
    )
    manual_review_required = classification in {
        "MISSING_CANDIDATE_MANUAL_REVIEW",
        "STALE_OTHER_DATE_MANUAL_REVIEW",
    }
    canonical_status = str(canonical.get("status") or "FAIL")
    active_recovery_required = bool(
        classification == "ACTIVE_CURRENT" and canonical_status != "PASS"
    )
    disposition_required = bool(
        canonical_status != "PASS"
        and classification != "ACTIVE_CURRENT"
    )
    pipeline_fingerprint = _pipeline_rca_fingerprint(
        canonical,
        target_trade_date=target_trade_date,
    )
    subject_version = _pipeline_rca_subject_version(
        candidate_instance_id=str(canonical.get("candidate_instance_id") or ""),
        candidate=candidate,
        latest_plan=latest_plan,
    )
    return {
        **canonical,
        "canonical_status": canonical_status,
        "canonical_reason_codes": list(canonical.get("reason_codes") or []),
        "classification": classification,
        "candidate": candidate,
        "latest_plan": latest_plan,
        "current_source_drift": drift["value"],
        "current_source_drift_status": drift["status"],
        "current_source_drift_reason": drift["reason"],
        "legacy_warn_eligible": legacy_warn_eligible,
        "legacy_warn_candidate": False,
        "legacy_warn_reason_codes": legacy_warn_reasons,
        "legacy_warn_evidence": {
            "status": "PENDING_AUTHORITATIVE_EVIDENCE",
            "authoritative": False,
            "pre_schema59": False,
            "terminal_closed": False,
            "active_source_zero": False,
            "current_source_no_drift": False,
            "cas_valid": False,
        },
        "manual_review_required": manual_review_required,
        "active_recovery_required": active_recovery_required,
        "disposition_required": disposition_required,
        "pipeline_fingerprint": pipeline_fingerprint,
        "subject_version": subject_version,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
    }


def _pipeline_rca_latest_plan(
    data: Mapping[str, Any],
    *,
    as_of: datetime,
) -> dict[str, Any]:
    order_plan_id = data.get("plan__order_plan_id")
    expires_at = data.get("plan__expires_at")
    if not order_plan_id:
        unexpired: bool | None = False
        expiration_status = "ABSENT"
    elif not expires_at:
        unexpired = None
        expiration_status = "UNKNOWN"
    else:
        try:
            expires = parse_timestamp(expires_at, "expires_at")
        except (TypeError, ValueError):
            unexpired = None
            expiration_status = "INVALID"
        else:
            try:
                unexpired = expires > as_of
            except TypeError:
                unexpired = None
                expiration_status = "INVALID"
            else:
                expiration_status = "UNEXPIRED" if unexpired else "EXPIRED"
    return {
        "present": bool(order_plan_id),
        "order_plan_id": order_plan_id,
        "status": data.get("plan__status"),
        "created_at": data.get("plan__created_at"),
        "expires_at": expires_at,
        "unexpired": unexpired,
        "expiration_status": expiration_status,
        "entry_timing_evaluation_id": data.get(
            "plan__entry_timing_evaluation_id"
        ),
        "strategy_observation_id": data.get("plan__strategy_observation_id"),
        "risk_observation_id": data.get("plan__risk_observation_id"),
        "source_run_id": data.get("plan__source_run_id"),
        "source_watermark_hash": data.get("plan__source_watermark_hash"),
        "source_event_id": data.get("plan__source_event_id"),
        "source_observed_at": data.get("plan__source_observed_at"),
        "generated_by": data.get("plan__generated_by"),
    }


def _pipeline_rca_classification(
    candidate: Mapping[str, Any],
    *,
    target_trade_date: str,
) -> str:
    if not candidate.get("present"):
        return "MISSING_CANDIDATE_MANUAL_REVIEW"
    if str(candidate.get("trade_date") or "") != target_trade_date:
        return "STALE_OTHER_DATE_MANUAL_REVIEW"
    if str(candidate.get("state") or "").upper() == "CLOSED":
        return "HISTORICAL_CLOSED"
    return "ACTIVE_CURRENT"


def _pipeline_rca_current_source_drift(
    canonical: Mapping[str, Any],
    *,
    candidate: Mapping[str, Any],
    classification: str,
) -> dict[str, Any]:
    reason_codes = set(canonical.get("reason_codes") or [])
    if classification == "MISSING_CANDIDATE_MANUAL_REVIEW":
        return {
            "value": None,
            "status": "UNKNOWN",
            "reason": "CANDIDATE_MISSING",
        }
    if classification == "STALE_OTHER_DATE_MANUAL_REVIEW":
        return {
            "value": True,
            "status": "DRIFT",
            "reason": "CANDIDATE_TRADE_DATE_DRIFT",
        }
    if "CURRENT_SOURCE_WATERMARK_MISMATCH" in reason_codes:
        return {
            "value": True,
            "status": "DRIFT",
            "reason": "CURRENT_SOURCE_WATERMARK_MISMATCH",
        }
    stages = _json_object(canonical.get("stages"))
    stored_hash = _first_text(
        _json_object(stages.get("strategy")).get("source_watermark_hash"),
        _json_object(stages.get("risk")).get("source_watermark_hash"),
        _json_object(stages.get("entry_timing")).get("source_watermark_hash"),
        _json_object(stages.get("order_plan")).get("source_watermark_hash"),
    )
    current_hash = _first_text(
        _json_object(canonical.get("current_source")).get("source_watermark_hash")
    )
    if stored_hash and current_hash:
        drifted = stored_hash != current_hash
        return {
            "value": drifted,
            "status": "DRIFT" if drifted else "NO_DRIFT",
            "reason": (
                "CURRENT_SOURCE_WATERMARK_MISMATCH"
                if drifted
                else "CURRENT_SOURCE_WATERMARK_MATCH"
            ),
        }
    if (
        classification == "HISTORICAL_CLOSED"
        and candidate.get("active_source_count") == 0
    ):
        return {
            "value": False,
            "status": "NO_DRIFT",
            "reason": "CLOSED_WITH_NO_ACTIVE_SOURCE",
        }
    return {
        "value": None,
        "status": "UNKNOWN",
        "reason": "CURRENT_SOURCE_COMPARISON_UNAVAILABLE",
    }


def _legacy_warn_candidate(
    canonical: Mapping[str, Any],
    *,
    candidate: Mapping[str, Any],
    latest_plan: Mapping[str, Any],
    classification: str,
    current_source_drift: bool | None,
) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    canonical_status = str(canonical.get("status") or "FAIL")
    reason_codes = [str(value) for value in canonical.get("reason_codes") or []]
    if classification != "HISTORICAL_CLOSED":
        blockers.append("NOT_HISTORICAL_CLOSED")
    if not candidate.get("closed_at"):
        blockers.append("CLOSED_AT_MISSING")
    if candidate.get("active_source_count") != 0:
        blockers.append("ACTIVE_SOURCE_NOT_PROVEN_ZERO")
    if canonical_status == "PASS":
        blockers.append("CANONICAL_ALREADY_PASS")
    if not any("LINEAGE_MISSING" in reason for reason in reason_codes):
        blockers.append("LEGACY_LINEAGE_MISSING_NOT_PROVEN")
    if any(not _legacy_warn_reason_allowed(reason) for reason in reason_codes):
        blockers.append("NON_LEGACY_CANONICAL_REASON_PRESENT")
    if str(latest_plan.get("status") or "").upper() == "PLAN_READY":
        blockers.append("LATEST_PLAN_READY")
    if latest_plan.get("unexpired") is True:
        blockers.append("LATEST_PLAN_UNEXPIRED")
    if latest_plan.get("unexpired") is None:
        blockers.append("LATEST_PLAN_EXPIRY_UNKNOWN")
    if current_source_drift is True:
        blockers.append("CURRENT_SOURCE_DRIFT_PRESENT")
    elif current_source_drift is None:
        blockers.append("CURRENT_SOURCE_DRIFT_UNKNOWN")
    return not blockers, _dedupe(blockers)


def _legacy_warn_reason_allowed(reason: str) -> bool:
    normalized = str(reason).upper()
    return "LINEAGE_MISSING" in normalized or normalized in {
        "ENTRY_TIMING_MISSING",
        "ORDER_PLAN_MISSING",
        "PIPELINE_SOURCE_STALE",
    }


def _pipeline_rca_fingerprint(
    canonical: Mapping[str, Any],
    *,
    target_trade_date: str,
) -> str:
    stages = _json_object(canonical.get("stages"))
    payload = {
        "contract": "pipeline-coherency-rca.v1",
        "candidate_instance_id": canonical.get("candidate_instance_id"),
        "trade_date": target_trade_date,
        "status": canonical.get("status"),
        "reason_codes": list(canonical.get("reason_codes") or []),
        "stages": {
            name: _without_volatile_age(_json_object(stages.get(name)))
            for name in ("strategy", "risk", "entry_timing", "order_plan")
        },
        "current_source": _without_volatile_age(
            _json_object(canonical.get("current_source"))
        ),
    }
    return _canonical_sha256(payload)


def _pipeline_rca_subject_version(
    *,
    candidate_instance_id: str,
    candidate: Mapping[str, Any],
    latest_plan: Mapping[str, Any],
) -> str:
    plan_fields = {
        key: latest_plan.get(key)
        for key in (
            "present",
            "order_plan_id",
            "status",
            "created_at",
            "expires_at",
            "entry_timing_evaluation_id",
            "strategy_observation_id",
            "risk_observation_id",
            "source_run_id",
            "source_watermark_hash",
            "source_event_id",
            "source_observed_at",
            "generated_by",
        )
    }
    payload = {
        "contract": "pipeline-coherency-subject.v1",
        "candidate_instance_id": candidate_instance_id,
        "candidate": dict(candidate),
        "latest_plan": plan_fields,
    }
    return _canonical_sha256(payload)


def _without_volatile_age(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "data_age_sec"}


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _is_canonical_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _pipeline_rca_subject_summary(item: Mapping[str, Any]) -> dict[str, Any]:
    latest_plan = _json_object(item.get("latest_plan"))
    return {
        "candidate_instance_id": item.get("candidate_instance_id"),
        "trade_date": item.get("trade_date"),
        "code": item.get("code"),
        "canonical_status": item.get("canonical_status"),
        "canonical_reason_codes": list(item.get("canonical_reason_codes") or []),
        "classification": item.get("classification"),
        "legacy_warn_eligible": bool(item.get("legacy_warn_eligible")),
        "legacy_warn_candidate": bool(item.get("legacy_warn_candidate")),
        "legacy_warn_evidence": deepcopy(item.get("legacy_warn_evidence")),
        "manual_review_required": bool(item.get("manual_review_required")),
        "active_recovery_required": bool(item.get("active_recovery_required")),
        "disposition_required": bool(item.get("disposition_required")),
        "latest_plan_present": bool(latest_plan.get("present")),
        "latest_plan_status": latest_plan.get("status"),
        "latest_plan_expires_at": latest_plan.get("expires_at"),
        "latest_plan_unexpired": latest_plan.get("unexpired"),
        "latest_plan_expiration_status": latest_plan.get("expiration_status"),
        "current_source_drift": item.get("current_source_drift"),
        "pipeline_fingerprint": item.get("pipeline_fingerprint"),
        "subject_version": item.get("subject_version"),
    }


def _pipeline_rca_connection_fingerprint(connection: sqlite3.Connection) -> str:
    databases = [
        {
            "sequence": int(row[0]),
            "name": str(row[1]),
            "file": str(row[2] or ""),
        }
        for row in connection.execute("PRAGMA database_list").fetchall()
    ]
    schema_version_row = connection.execute("PRAGMA schema_version").fetchone()
    data_version_row = connection.execute("PRAGMA data_version").fetchone()
    return _canonical_sha256(
        {
            "contract": "pipeline-coherency-resolver-boundary.v1",
            "databases": databases,
            "schema_version": int(schema_version_row[0]),
            "data_version": int(data_version_row[0]),
            "total_changes": int(connection.total_changes),
            "in_transaction": bool(connection.in_transaction),
        }
    )


def _invoke_pipeline_read_only_resolver(
    connection: sqlite3.Connection,
    resolver: Callable[
        [str, Sequence[Mapping[str, Any]]], Mapping[str, Any]
    ],
    *,
    trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any] | None, str | None]:
    query_only_row = connection.execute("PRAGMA query_only").fetchone()
    original_query_only = bool(int(query_only_row[0]))
    fingerprint_before = _pipeline_rca_connection_fingerprint(connection)
    callback_error = False
    resolved: Mapping[str, Any] | object | None = None
    connection.execute("PRAGMA query_only = ON")
    try:
        resolved = resolver(trade_date, deepcopy(tuple(subjects)))
    except Exception:
        callback_error = True
    finally:
        query_only_after_row = connection.execute("PRAGMA query_only").fetchone()
        query_only_after = bool(int(query_only_after_row[0]))
        fingerprint_after = _pipeline_rca_connection_fingerprint(connection)
        connection.execute(
            f"PRAGMA query_only = {1 if original_query_only else 0}"
        )
    if not query_only_after or fingerprint_before != fingerprint_after:
        return None, "READ_ONLY_VIOLATION"
    if callback_error:
        return None, "RESOLVER_ERROR"
    if not isinstance(resolved, Mapping):
        return None, "INVALID_RESULT"
    return resolved, None


def _resolve_pipeline_legacy_evidence(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
    resolver: PipelineLegacyEvidenceResolver | None,
) -> dict[str, Any]:
    if resolver is None:
        return {
            "evidence_ready": False,
            "status": "EVIDENCE_NOT_CONFIGURED",
            "reason_codes": ["PIPELINE_LEGACY_EVIDENCE_NOT_CONFIGURED"],
            "items": {},
        }
    resolved, error = _invoke_pipeline_read_only_resolver(
        connection,
        resolver,
        trade_date=trade_date,
        subjects=subjects,
    )
    if error is not None:
        reason = {
            "READ_ONLY_VIOLATION": "PIPELINE_LEGACY_EVIDENCE_READ_ONLY_VIOLATION",
            "RESOLVER_ERROR": "PIPELINE_LEGACY_EVIDENCE_RESOLVER_ERROR",
            "INVALID_RESULT": "PIPELINE_LEGACY_EVIDENCE_RESULT_INVALID",
        }[error]
        return {
            "evidence_ready": False,
            "status": error,
            "reason_codes": [reason],
            "items": {},
        }
    assert resolved is not None
    raw_items = resolved.get("items")
    raw_reasons = resolved.get("reason_codes")
    invalid_items = not isinstance(raw_items, Mapping)
    invalid_reasons = bool(
        raw_reasons is not None
        and (
            isinstance(raw_reasons, (str, bytes))
            or not isinstance(raw_reasons, Sequence)
        )
    )
    reasons = _dedupe(raw_reasons or []) if not invalid_reasons else []
    if invalid_items or invalid_reasons:
        reasons.append("PIPELINE_LEGACY_EVIDENCE_RESULT_INVALID")
    raw_status = resolved.get("status")
    invalid_status = not isinstance(raw_status, str) or not raw_status
    if invalid_status:
        reasons.append("PIPELINE_LEGACY_EVIDENCE_RESULT_INVALID")
    status = str(raw_status or "")
    evidence_ready = bool(
        status == "READY"
        and not reasons
        and isinstance(raw_items, Mapping)
    )
    normalized_items = _validated_pipeline_legacy_evidence_items(
        subjects,
        raw_items=raw_items if isinstance(raw_items, Mapping) else {},
        evidence_ready=evidence_ready,
    )
    return {
        "evidence_ready": evidence_ready,
        "status": status or "INVALID_RESOLVER_RESULT",
        "reason_codes": _dedupe(reasons),
        "items": normalized_items,
    }


def _validated_pipeline_legacy_evidence_items(
    subjects: Sequence[Mapping[str, Any]],
    *,
    raw_items: Mapping[str, Any],
    evidence_ready: bool,
) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for subject in subjects:
        candidate_id = str(subject.get("candidate_instance_id") or "")
        raw = raw_items.get(candidate_id)
        if not isinstance(raw, Mapping):
            continue
        item_contract_valid = set(raw) == _PIPELINE_LEGACY_EVIDENCE_ITEM_KEYS
        item = {
            key: raw.get(key) for key in _PIPELINE_LEGACY_EVIDENCE_ITEM_KEYS
        }
        pipeline_fingerprint = item.get("pipeline_fingerprint")
        subject_version = item.get("subject_version")
        cas_valid = bool(
            evidence_ready
            and _is_canonical_sha256(pipeline_fingerprint)
            and _is_canonical_sha256(subject_version)
            and pipeline_fingerprint == subject.get("pipeline_fingerprint")
            and subject_version == subject.get("subject_version")
        )
        authoritative = bool(
            cas_valid
            and item_contract_valid
            and item.get("contract") == "pipeline-legacy-evidence-item.v1"
            and item.get("status") == "AUTHORITATIVE"
            and item.get("authoritative") is True
            and item.get("pre_schema59") is True
            and item.get("terminal_closed") is True
            and item.get("active_source_zero") is True
            and item.get("current_source_no_drift") is True
            and all(
                _is_canonical_sha256(item.get(key))
                for key in (
                    "source_fingerprint",
                    "closure_fingerprint",
                    "pipeline_stage_fingerprint",
                    "provenance_sha256",
                )
            )
        )
        item["cas_valid"] = cas_valid
        item["authoritative"] = authoritative
        item["pre_schema59"] = bool(item.get("pre_schema59") is True)
        item["terminal_closed"] = bool(item.get("terminal_closed") is True)
        item["active_source_zero"] = bool(item.get("active_source_zero") is True)
        item["current_source_no_drift"] = bool(
            item.get("current_source_no_drift") is True
        )
        if not cas_valid:
            item["status"] = "INVALID_CAS"
        elif not authoritative:
            item["status"] = "INVALID_AUTHORITATIVE_EVIDENCE"
        normalized[candidate_id] = item
    return normalized


def _effective_pipeline_legacy_evidence(
    evidence_state: Mapping[str, Any],
    candidate_instance_id: str,
) -> dict[str, Any]:
    if evidence_state.get("evidence_ready") is not True:
        return {
            "status": "PENDING_AUTHORITATIVE_EVIDENCE",
            "authoritative": False,
            "pre_schema59": False,
            "terminal_closed": False,
            "active_source_zero": False,
            "current_source_no_drift": False,
            "cas_valid": False,
        }
    items = evidence_state.get("items")
    item = items.get(candidate_instance_id) if isinstance(items, Mapping) else None
    if not isinstance(item, Mapping):
        return {
            "status": "MISSING_AUTHORITATIVE_EVIDENCE",
            "authoritative": False,
            "pre_schema59": False,
            "terminal_closed": False,
            "active_source_zero": False,
            "current_source_no_drift": False,
            "cas_valid": False,
        }
    return dict(item)


def _apply_pipeline_legacy_evidence(
    summaries: Sequence[dict[str, Any]],
    *,
    page_items: Sequence[dict[str, Any]],
    evidence_state: Mapping[str, Any],
) -> None:
    page_by_candidate_id = {
        str(item.get("candidate_instance_id") or ""): item for item in page_items
    }
    for summary in summaries:
        candidate_id = str(summary.get("candidate_instance_id") or "")
        evidence = _effective_pipeline_legacy_evidence(
            evidence_state,
            candidate_id,
        )
        blockers = [str(value) for value in summary.get("legacy_warn_reason_codes") or []]
        if evidence.get("authoritative") is True and evidence.get("cas_valid") is True:
            if evidence.get("active_source_zero") is True:
                blockers = [
                    value for value in blockers if value != "ACTIVE_SOURCE_NOT_PROVEN_ZERO"
                ]
            if evidence.get("current_source_no_drift") is True:
                blockers = [
                    value for value in blockers if value != "CURRENT_SOURCE_DRIFT_UNKNOWN"
                ]
        legacy_warn_candidate = bool(
            not blockers
            and evidence.get("authoritative") is True
            and evidence.get("pre_schema59") is True
            and evidence.get("terminal_closed") is True
            and evidence.get("active_source_zero") is True
            and evidence.get("current_source_no_drift") is True
            and evidence.get("cas_valid") is True
        )
        summary["legacy_warn_candidate"] = legacy_warn_candidate
        summary["legacy_warn_effective_reason_codes"] = blockers
        summary["legacy_warn_evidence"] = evidence
        summary["disposition_required"] = bool(
            summary.get("canonical_status") != "PASS"
            and not legacy_warn_candidate
            and summary.get("classification") != "ACTIVE_CURRENT"
        )
        page_item = page_by_candidate_id.get(candidate_id)
        if page_item is not None:
            page_item["legacy_warn_candidate"] = legacy_warn_candidate
            page_item["legacy_warn_effective_reason_codes"] = blockers
            page_item["legacy_warn_evidence"] = evidence
            page_item["disposition_required"] = summary["disposition_required"]


def _resolve_pipeline_dispositions(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
    resolver: PipelineDispositionResolver | None,
) -> dict[str, Any]:
    if resolver is None:
        return {
            "schema_ready": False,
            "resolver_ready": False,
            "status": "SCHEMA_NOT_READY",
            "reason_codes": ["PIPELINE_DISPOSITION_SCHEMA_NOT_READY"],
            "items": {},
        }
    resolved, error = _invoke_pipeline_read_only_resolver(
        connection,
        resolver,
        trade_date=trade_date,
        subjects=subjects,
    )
    if error is not None:
        reason = {
            "READ_ONLY_VIOLATION": "PIPELINE_DISPOSITION_RESOLVER_READ_ONLY_VIOLATION",
            "RESOLVER_ERROR": "PIPELINE_DISPOSITION_RESOLVER_ERROR",
            "INVALID_RESULT": "PIPELINE_DISPOSITION_RESULT_INVALID",
        }[error]
        return {
            "schema_ready": False,
            "resolver_ready": False,
            "status": error,
            "reason_codes": [reason],
            "items": {},
        }
    assert resolved is not None
    raw_items = resolved.get("items")
    invalid_items = not isinstance(raw_items, Mapping)
    normalized_raw_items: Mapping[str, Any] = (
        raw_items if isinstance(raw_items, Mapping) else {}
    )
    schema_ready = resolved.get("schema_ready") is True
    raw_reasons = resolved.get("reason_codes")
    invalid_reasons = bool(
        raw_reasons is not None
        and (
            isinstance(raw_reasons, (str, bytes))
            or not isinstance(raw_reasons, Sequence)
        )
    )
    reasons = _dedupe(raw_reasons or []) if not invalid_reasons else []
    if invalid_items:
        reasons.append("PIPELINE_DISPOSITION_RESULT_INVALID")
    if invalid_reasons:
        reasons.append("PIPELINE_DISPOSITION_RESULT_INVALID")
    if not schema_ready:
        reasons.append("PIPELINE_DISPOSITION_SCHEMA_NOT_READY")
    raw_status = resolved.get("status")
    invalid_status = not isinstance(raw_status, str) or not raw_status
    if invalid_status:
        reasons.append("PIPELINE_DISPOSITION_RESULT_INVALID")
    status = (
        "INVALID_RESOLVER_RESULT"
        if invalid_items or invalid_reasons or invalid_status
        else str(raw_status)
    )
    reasons = _dedupe(reasons)
    resolver_ready = bool(schema_ready and status == "READY" and not reasons)
    normalized_items = _validated_pipeline_disposition_items(
        subjects,
        raw_items=normalized_raw_items,
        resolver_ready=resolver_ready,
    )
    return {
        "schema_ready": schema_ready,
        "resolver_ready": resolver_ready,
        "status": status,
        "reason_codes": reasons,
        "items": normalized_items,
    }


def _validated_pipeline_disposition_items(
    subjects: Sequence[Mapping[str, Any]],
    *,
    raw_items: Mapping[str, Any],
    resolver_ready: bool,
) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for subject in subjects:
        candidate_id = str(subject.get("candidate_instance_id") or "")
        raw = raw_items.get(candidate_id)
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        pipeline_fingerprint = item.get("pipeline_fingerprint")
        subject_version = item.get("subject_version")
        hashes_valid = bool(
            _is_canonical_sha256(pipeline_fingerprint)
            and _is_canonical_sha256(subject_version)
            and pipeline_fingerprint == subject.get("pipeline_fingerprint")
            and subject_version == subject.get("subject_version")
        )
        cas_valid = bool(
            resolver_ready and hashes_valid
        )
        status_valid = item.get("status") == "EFFECTIVE"
        item["cas_valid"] = cas_valid
        item["effective"] = bool(
            item.get("effective") is True and status_valid and cas_valid
        )
        if raw.get("effective") is True and not hashes_valid:
            item["status"] = "INVALID_CAS"
        elif raw.get("effective") is True and not status_valid:
            item["status"] = "INVALID_STATUS"
        normalized[candidate_id] = item
    return normalized


def _effective_pipeline_disposition(
    disposition_state: Mapping[str, Any],
    candidate_instance_id: str,
) -> dict[str, Any]:
    if disposition_state.get("schema_ready") is not True:
        return {
            "status": "PENDING_SCHEMA",
            "effective": False,
            "cas_valid": False,
        }
    if disposition_state.get("resolver_ready") is not True:
        return {
            "status": "PENDING_RESOLVER",
            "effective": False,
            "cas_valid": False,
        }
    items = disposition_state.get("items")
    item = items.get(candidate_instance_id) if isinstance(items, Mapping) else None
    if not isinstance(item, Mapping):
        return {"status": "PENDING", "effective": False, "cas_valid": False}
    return dict(item)


def _pipeline_rca_canonical_summary(
    summaries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    fail_count = sum(
        1 for item in summaries if item.get("canonical_status") == "FAIL"
    )
    warn_count = sum(
        1 for item in summaries if item.get("canonical_status") == "WARN"
    )
    reasons = _dedupe(
        reason
        for item in summaries
        for reason in item.get("canonical_reason_codes") or []
    )
    if not summaries:
        reasons.append("NO_PIPELINE_OBSERVATIONS")
    return {
        "status": (
            "FAIL" if fail_count else ("WARN" if warn_count or not summaries else "PASS")
        ),
        "reason_codes": reasons,
        "candidate_count": len(summaries),
        "coherent_count": sum(
            1 for item in summaries if item.get("canonical_status") == "PASS"
        ),
        "warning_count": warn_count,
        "mismatch_count": fail_count,
        "missing_lineage_count": sum(
            1
            for item in summaries
            if any(
                "LINEAGE_MISSING" in str(reason)
                for reason in item.get("canonical_reason_codes") or []
            )
        ),
        "stale_count": sum(
            1
            for item in summaries
            if any(
                "STALE" in str(reason)
                for reason in item.get("canonical_reason_codes") or []
            )
        ),
        "read_only": True,
        "no_order_side_effects": True,
    }


def _pipeline_rca_qualification_summary(
    summaries: Sequence[Mapping[str, Any]],
    *,
    disposition_state: Mapping[str, Any],
    inventory_count_consistent: bool,
) -> dict[str, Any]:
    classification_counts = {name: 0 for name in _PIPELINE_RCA_CLASSIFICATIONS}
    for item in summaries:
        classification = str(item.get("classification") or "")
        if classification in classification_counts:
            classification_counts[classification] += 1

    disposition_required = [
        item for item in summaries if item.get("disposition_required") is True
    ]
    disposition_pending = [
        item
        for item in disposition_required
        if _effective_pipeline_disposition(
            disposition_state,
            str(item.get("candidate_instance_id") or ""),
        ).get("effective")
        is not True
    ]
    effectively_disposed_ids = {
        str(item.get("candidate_instance_id") or "")
        for item in disposition_required
        if _effective_pipeline_disposition(
            disposition_state,
            str(item.get("candidate_instance_id") or ""),
        ).get("effective")
        is True
    }
    legacy_warn_count = sum(
        1 for item in summaries if item.get("legacy_warn_candidate") is True
    )
    manual_count = sum(
        1 for item in summaries if item.get("manual_review_required") is True
    )
    manual_pending_count = sum(
        1
        for item in summaries
        if item.get("manual_review_required") is True
        and str(item.get("candidate_instance_id") or "")
        not in effectively_disposed_ids
    )
    active_non_pass_count = sum(
        1 for item in summaries if item.get("active_recovery_required") is True
    )
    unexpired_plan_count = sum(
        1 for item in summaries if item.get("latest_plan_unexpired") is True
    )
    unexpired_plan_ready_count = sum(
        1
        for item in summaries
        if item.get("latest_plan_unexpired") is True
        and str(item.get("latest_plan_status") or "").upper() == "PLAN_READY"
    )
    plan_expiry_unknown_count = sum(
        1
        for item in summaries
        if item.get("latest_plan_present") is True
        and item.get("latest_plan_unexpired") is None
    )
    drift_count = sum(
        1 for item in summaries if item.get("current_source_drift") is True
    )
    drift_pending_count = sum(
        1
        for item in summaries
        if item.get("current_source_drift") is True
        and str(item.get("candidate_instance_id") or "")
        not in effectively_disposed_ids
    )
    drift_unknown_count = sum(
        1 for item in summaries if item.get("current_source_drift") is None
    )
    drift_unknown_pending_count = sum(
        1
        for item in summaries
        if item.get("current_source_drift") is None
        and str(item.get("candidate_instance_id") or "")
        not in effectively_disposed_ids
    )

    blockers: list[str] = []
    if disposition_state.get("schema_ready") is not True:
        blockers.append("PIPELINE_DISPOSITION_SCHEMA_NOT_READY")
    elif disposition_state.get("resolver_ready") is not True:
        blockers.append("PIPELINE_DISPOSITION_RESOLVER_NOT_READY")
    if not inventory_count_consistent:
        blockers.append("PIPELINE_INVENTORY_CHANGED_DURING_READ")
    if manual_pending_count:
        blockers.append("PIPELINE_MANUAL_REVIEW_PRESENT")
    if active_non_pass_count:
        blockers.append("PIPELINE_ACTIVE_CURRENT_NON_PASS")
    if unexpired_plan_ready_count:
        blockers.append("PIPELINE_UNEXPIRED_PLAN_READY_PRESENT")
    if plan_expiry_unknown_count:
        blockers.append("PIPELINE_PLAN_EXPIRY_UNKNOWN")
    if drift_pending_count:
        blockers.append("PIPELINE_CURRENT_SOURCE_DRIFT_PRESENT")
    if drift_unknown_pending_count:
        blockers.append("PIPELINE_CURRENT_SOURCE_DRIFT_UNKNOWN")
    if disposition_pending:
        blockers.append("PIPELINE_DISPOSITION_PENDING")
    warnings: list[str] = []
    if legacy_warn_count:
        warnings.append("PIPELINE_LEGACY_WARN_CANDIDATES_PRESENT")
    if not summaries:
        warnings.append("NO_PIPELINE_OBSERVATIONS")
    status = "BLOCKED" if blockers else ("WARN" if warnings else "PASS")
    return {
        "status": status,
        "reason_codes": _dedupe([*blockers, *warnings]),
        "classification_counts": classification_counts,
        "legacy_warn_candidate_count": legacy_warn_count,
        "manual_review_count": manual_count,
        "manual_review_pending_count": manual_pending_count,
        "active_current_non_pass_count": active_non_pass_count,
        "unexpired_plan_count": unexpired_plan_count,
        "unexpired_plan_ready_count": unexpired_plan_ready_count,
        "plan_expiry_unknown_count": plan_expiry_unknown_count,
        "current_source_drift_count": drift_count,
        "current_source_drift_pending_count": drift_pending_count,
        "current_source_drift_unknown_count": drift_unknown_count,
        "current_source_drift_unknown_pending_count": drift_unknown_pending_count,
        "disposition_required_count": len(disposition_required),
        "disposition_pending_count": len(disposition_pending),
    }


def _empty_pipeline_coherency_rca_status(
    *,
    limit: int,
    offset: int,
    candidate_instance_id: str | None,
) -> dict[str, Any]:
    classification_counts = {name: 0 for name in _PIPELINE_RCA_CLASSIFICATIONS}
    reason_codes = [
        "PIPELINE_DISPOSITION_SCHEMA_NOT_READY",
        "NO_PIPELINE_OBSERVATIONS",
    ]
    return {
        "status": "BLOCKED",
        "qualification_status": "BLOCKED",
        "qualification_reason_codes": reason_codes,
        "canonical_status": "WARN",
        "canonical_reason_codes": ["NO_PIPELINE_OBSERVATIONS"],
        "canonical": {
            "status": "WARN",
            "reason_codes": ["NO_PIPELINE_OBSERVATIONS"],
            "candidate_count": 0,
            "coherent_count": 0,
            "warning_count": 0,
            "mismatch_count": 0,
            "missing_lineage_count": 0,
            "stale_count": 0,
            "read_only": True,
            "no_order_side_effects": True,
        },
        "trade_date": None,
        "candidate_instance_id_filter": candidate_instance_id,
        "classification_counts": classification_counts,
        "legacy_warn_candidate_count": 0,
        "manual_review_count": 0,
        "manual_review_pending_count": 0,
        "legacy_evidence_ready": False,
        "legacy_evidence": {
            "evidence_ready": False,
            "status": "EVIDENCE_NOT_CONFIGURED",
            "reason_codes": ["PIPELINE_LEGACY_EVIDENCE_NOT_CONFIGURED"],
        },
        "unexpired_plan_count": 0,
        "plan_expiry_unknown_count": 0,
        "current_source_drift_count": 0,
        "current_source_drift_pending_count": 0,
        "current_source_drift_unknown_count": 0,
        "current_source_drift_unknown_pending_count": 0,
        "disposition_required_count": 0,
        "disposition_pending_count": 0,
        "schema_ready": False,
        "disposition": {
            "schema_ready": False,
            "resolver_ready": False,
            "status": "SCHEMA_NOT_READY",
            "reason_codes": ["PIPELINE_DISPOSITION_SCHEMA_NOT_READY"],
        },
        "limit": limit,
        "offset": offset,
        "returned_count": 0,
        "full_count": 0,
        "has_more": False,
        "next_offset": None,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "returned_count": 0,
            "full_count": 0,
            "has_more": False,
            "next_offset": None,
        },
        "inventory_count_consistent": True,
        "inventory_digest": _pipeline_rca_inventory_digest([]),
        "inventory_end_digest": _pipeline_rca_inventory_digest([]),
        "inventory_duplicate_key_count": 0,
        "items": [],
        "fingerprint_contract": {
            "algorithm": "SHA-256",
            "canonicalization": "canonical_json",
            "pipeline_fingerprint": "pipeline-coherency-rca.v1",
            "subject_version": "pipeline-coherency-subject.v1",
        },
        "generated_at": datetime_to_wire(utc_now()),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, str, bytes, bytearray)):
        return int(value)
    raise TypeError(f"unsupported integer value: {type(value).__name__}")


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
    # Keep the public value finite/JSON-safe, but above any practical finite
    # freshness allowance so missing, invalid, or materially future timestamps
    # cannot be made fresh by a large diagnostic max_age_sec.
    invalid_age_sec = 1e300
    if not value:
        return invalid_age_sec
    try:
        observed = parse_timestamp(value, "source_observed_at")
    except (TypeError, ValueError):
        return invalid_age_sec
    current = now or utc_now()
    delta_sec = (current - observed).total_seconds()
    if delta_sec < -5.0:
        return invalid_age_sec
    return round(max(delta_sec, 0.0), 3)


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
