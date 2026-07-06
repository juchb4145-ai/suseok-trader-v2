from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, normalize_value, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus

from services.ai_advisory.storage import get_latest_run as get_latest_ai_advisory_run
from services.config import Settings, load_settings
from services.entry_timing.service import evaluate_entry_timing
from services.live_sim.live_sim_service import (
    _complete_run,
    _record_error,
    _save_rejection,
    queue_live_sim_order_command,
)
from services.live_sim.order_plan_eligibility import (
    evaluate_live_sim_order_plan_eligibility,
    record_live_sim_order_plan_rejection,
    select_live_sim_order_plan_candidates,
)
from services.live_sim.order_plan_intent import create_live_sim_intent_from_order_plan
from services.live_sim.safety_gate import check_live_sim_safety_gate
from services.runtime.evaluation_run_guard import EvaluationRunLockError

PIPELINE_SOURCE = "order_plan_pipeline"


@dataclass(frozen=True, kw_only=True)
class LiveSimPilotPipelineRunResult:
    run_id: str
    trade_date: str | None = None
    evaluated_count: int = 0
    eligible_count: int = 0
    intent_count: int = 0
    command_count: int = 0
    rejection_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    selected_order_plans: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    eligibilities: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    rejections: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    intents: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    commands: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    preparation: Mapping[str, Any] = field(default_factory=dict)
    safety_gate: Mapping[str, Any] = field(default_factory=dict)
    ai_advisory_summary: Mapping[str, Any] = field(default_factory=dict)
    live_sim_only: bool = True
    live_real_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "evaluated_count": self.evaluated_count,
            "eligible_count": self.eligible_count,
            "intent_count": self.intent_count,
            "command_count": self.command_count,
            "rejection_count": self.rejection_count,
            "error_count": self.error_count,
            "status": self.status,
            "selected_order_plans": normalize_value(list(self.selected_order_plans)),
            "eligibilities": normalize_value(list(self.eligibilities)),
            "rejections": normalize_value(list(self.rejections)),
            "intents": normalize_value(list(self.intents)),
            "commands": normalize_value(list(self.commands)),
            "preparation": normalize_value(dict(self.preparation)),
            "safety_gate": normalize_value(dict(self.safety_gate)),
            "ai_advisory_summary": normalize_value(dict(self.ai_advisory_summary)),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }


def run_live_sim_pilot_pipeline_once(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
    trade_date: str | None = None,
    limit: int | None = None,
    queue_commands: bool | None = None,
) -> LiveSimPilotPipelineRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("live_sim_pilot_run")
    started_at = datetime_to_wire(utc_now())
    _insert_pilot_run(connection, run_id=run_id, trade_date=trade_date, started_at=started_at)
    safety_gate = check_live_sim_safety_gate(
        connection,
        resolved_settings,
        purpose="NEW_BUY",
    )
    ai_advisory_summary = _latest_ai_advisory_summary(connection)
    selected_order_plans: list[dict[str, Any]] = []
    eligibilities: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    intents: list[dict[str, Any]] = []
    commands: list[dict[str, Any]] = []
    preparation: dict[str, Any] = {"entry_timing_evaluated": False}
    evaluated_count = eligible_count = intent_count = command_count = rejection_count = 0
    error_count = 0

    try:
        if not resolved_settings.live_sim_pilot_pipeline_enabled:
            reason_codes = [LiveSimReasonCode.PILOT_PIPELINE_DISABLED.value]
            evidence = _pipeline_evidence(
                run_id=run_id,
                trade_date=trade_date,
                reason_codes=reason_codes,
                safety_gate=safety_gate.to_dict(),
            )
            evidence["ai_advisory_summary"] = ai_advisory_summary
            _save_rejection(
                connection,
                candidate_instance_id=None,
                strategy_observation_id=None,
                risk_observation_id=None,
                trade_date=trade_date,
                account_id=resolved_settings.live_sim_account_id,
                code=None,
                reason_codes=reason_codes,
                evidence=evidence,
            )
            rejection_count = 1
            rejections.append(evidence)
            status = "BLOCKED"
            _complete_pilot_run(
                connection,
                run_id=run_id,
                evaluated_count=0,
                eligible_count=0,
                intent_count=0,
                command_count=0,
                rejection_count=rejection_count,
                error_count=0,
                status=status,
                metadata=evidence,
            )
            connection.commit()
            return LiveSimPilotPipelineRunResult(
                run_id=run_id,
                trade_date=trade_date,
                rejection_count=rejection_count,
                status=status,
                rejections=rejections,
                safety_gate=safety_gate.to_dict(),
                ai_advisory_summary=ai_advisory_summary,
            )

        if _has_candidate_targets(connection, trade_date=trade_date):
            entry_result = evaluate_entry_timing(
                connection,
                trade_date=trade_date,
                limit=limit or resolved_settings.live_sim_order_plan_max_plans_per_run,
                settings=resolved_settings,
            )
            preparation = entry_result.to_dict()
            preparation["entry_timing_evaluated"] = True

        selected_order_plans = select_live_sim_order_plan_candidates(
            connection,
            trade_date=trade_date,
            limit=limit or resolved_settings.live_sim_order_plan_max_plans_per_run,
        )
        should_queue = (
            resolved_settings.live_sim_pilot_auto_queue_command
            if queue_commands is None
            else bool(queue_commands)
        )

        for plan in selected_order_plans:
            try:
                eligibility = evaluate_live_sim_order_plan_eligibility(
                    connection,
                    plan["order_plan_id"],
                    settings=resolved_settings,
                )
                evaluated_count += 1
                eligibilities.append(eligibility.to_dict())
                if not eligibility.eligible:
                    record_live_sim_order_plan_rejection(
                        connection,
                        eligibility,
                        account_id=resolved_settings.live_sim_account_id,
                        source=PIPELINE_SOURCE,
                    )
                    rejection_count += 1
                    rejections.append(eligibility.to_dict())
                    continue
                eligible_count += 1
                intent = create_live_sim_intent_from_order_plan(
                    connection,
                    plan["order_plan_id"],
                    settings=resolved_settings,
                    source=PIPELINE_SOURCE,
                )
                intents.append(intent.to_dict())
                if intent.status in {
                    LiveSimIntentStatus.CREATED,
                    LiveSimIntentStatus.COMMAND_QUEUED,
                }:
                    intent_count += 1
                if not should_queue:
                    continue
                if not resolved_settings.live_sim_pilot_auto_queue_command:
                    _save_rejection(
                        connection,
                        candidate_instance_id=intent.candidate_instance_id,
                        strategy_observation_id=intent.strategy_observation_id,
                        risk_observation_id=intent.risk_observation_id,
                        trade_date=intent.trade_date,
                        account_id=intent.account_id,
                        code=intent.code,
                        reason_codes=[LiveSimReasonCode.PILOT_AUTO_QUEUE_DISABLED.value],
                        evidence={
                            "run_id": run_id,
                            "source": PIPELINE_SOURCE,
                            "order_plan_id": plan["order_plan_id"],
                            "intent": intent.to_dict(),
                            "live_sim_only": True,
                            "live_real_allowed": False,
                        },
                    )
                    rejection_count += 1
                    rejections.append(
                        {
                            "order_plan_id": plan["order_plan_id"],
                            "reason_codes": [
                                LiveSimReasonCode.PILOT_AUTO_QUEUE_DISABLED.value
                            ],
                        }
                    )
                    continue
                if command_count >= resolved_settings.live_sim_order_plan_max_commands_per_run:
                    continue
                order = queue_live_sim_order_command(
                    connection,
                    intent.live_sim_intent_id,
                    settings=resolved_settings,
                )
                command_count += 1
                commands.append(order.to_dict())
            except Exception as exc:
                error_count += 1
                _record_error(
                    connection,
                    live_sim_intent_id=None,
                    live_sim_order_id=None,
                    code=plan.get("code"),
                    error_message=str(exc),
                    payload={"run_id": run_id, "order_plan": plan},
                    run_id=run_id,
                )

        status = "COMPLETED" if error_count == 0 else "COMPLETED_WITH_ERRORS"
        metadata = {
            "pipeline_source": PIPELINE_SOURCE,
            "queue_commands_requested": should_queue,
            "selected_order_plans": selected_order_plans,
            "preparation": preparation,
            "ai_advisory_summary": ai_advisory_summary,
            "live_sim_only": True,
            "live_real_allowed": False,
        }
        _complete_pilot_run(
            connection,
            run_id=run_id,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            intent_count=intent_count,
            command_count=command_count,
            rejection_count=rejection_count,
            error_count=error_count,
            status=status,
            metadata=metadata,
        )
        connection.commit()
        return LiveSimPilotPipelineRunResult(
            run_id=run_id,
            trade_date=trade_date,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            intent_count=intent_count,
            command_count=command_count,
            rejection_count=rejection_count,
            error_count=error_count,
            status=status,
            selected_order_plans=selected_order_plans,
            eligibilities=eligibilities,
            rejections=rejections,
            intents=intents,
            commands=commands,
            preparation=preparation,
            safety_gate=safety_gate.to_dict(),
            ai_advisory_summary=ai_advisory_summary,
        )
    except EvaluationRunLockError:
        connection.rollback()
        raise
    except Exception as exc:
        error_count += 1
        _complete_pilot_run(
            connection,
            run_id=run_id,
            evaluated_count=evaluated_count,
            eligible_count=eligible_count,
            intent_count=intent_count,
            command_count=command_count,
            rejection_count=rejection_count,
            error_count=error_count,
            status="FAILED",
            error_message=str(exc),
            metadata={"pipeline_source": PIPELINE_SOURCE, "error": str(exc)},
        )
        connection.commit()
        raise


def list_live_sim_pilot_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_runs
        WHERE pipeline_source = ?
        ORDER BY started_at DESC, run_id DESC
        LIMIT ?
        """,
        (PIPELINE_SOURCE, _bounded_limit(limit)),
    ).fetchall()
    return [_run_row_to_dict(row) for row in rows]


def _insert_pilot_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    started_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_runs (
            run_id,
            trade_date,
            pipeline_source,
            started_at,
            status,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 'RUNNING', '{}')
        """,
        (run_id, trade_date, PIPELINE_SOURCE, started_at),
    )


def _complete_pilot_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    evaluated_count: int,
    eligible_count: int,
    intent_count: int,
    command_count: int,
    rejection_count: int,
    error_count: int,
    status: str,
    metadata: Mapping[str, Any],
    error_message: str | None = None,
) -> None:
    _complete_run(
        connection,
        run_id=run_id,
        evaluated_count=evaluated_count,
        eligible_count=eligible_count,
        intent_count=intent_count,
        command_count=command_count,
        rejection_count=rejection_count,
        error_count=error_count,
        status=status,
        error_message=error_message,
    )
    connection.execute(
        """
        UPDATE live_sim_runs
        SET pipeline_source = ?,
            metadata_json = ?
        WHERE run_id = ?
        """,
        (PIPELINE_SOURCE, _json_dumps(metadata), run_id),
    )


def _has_candidate_targets(connection: sqlite3.Connection, *, trade_date: str | None) -> bool:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(trade_date)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM candidates {where_sql}",
        tuple(params),
    ).fetchone()
    return int(row["count"]) > 0


def _latest_ai_advisory_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    try:
        latest = get_latest_ai_advisory_run(connection)
    except sqlite3.Error:
        latest = None
    if latest is None:
        return {
            "available": False,
            "advisory_only": True,
            "no_order_side_effects": True,
            "ai_advisory_used_for_routing": False,
        }
    return {
        "available": True,
        "run_id": latest.get("run_id"),
        "status": latest.get("status"),
        "selected_count": latest.get("selected_count"),
        "summary": latest.get("summary"),
        "no_trade_reason": latest.get("no_trade_reason"),
        "advisory_only": True,
        "no_order_side_effects": True,
        "ai_advisory_used_for_routing": False,
    }


def _pipeline_evidence(
    *,
    run_id: str,
    trade_date: str | None,
    reason_codes: Sequence[str],
    safety_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "trade_date": trade_date,
        "pipeline_source": PIPELINE_SOURCE,
        "reason_codes": list(reason_codes),
        "safety_gate": dict(safety_gate),
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
    }


def _run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["metadata_json"] = _json_object(item.get("metadata_json"))
    item["live_sim_only"] = True
    item["live_real_allowed"] = False
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    return item


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


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
