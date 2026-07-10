from __future__ import annotations

import sqlite3
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.operator.no_buy_sentinel import (
    build_no_buy_sentinel_snapshot,
    get_latest_no_buy_sentinel_snapshot,
    rebuild_no_buy_sentinel_snapshot,
)
from services.operator.operator_status import build_operator_status
from services.realtime_subscription import (
    build_realtime_subscription_plan,
    run_realtime_subscription_once,
)
from services.runtime.evaluation_run_guard import (
    EvaluationRunLockError,
    get_runtime_execution_lock_status,
)
from services.runtime.gateway_market_index_routing import (
    get_latest_market_index_append_only_routing_status,
    list_market_index_append_only_routing_decisions,
)
from services.runtime.gateway_market_reference_routing import (
    build_market_reference_status,
    get_latest_market_reference_append_only_routing_status,
    list_market_reference_append_only_routing_decisions,
)
from services.runtime.gateway_projection_routing import (
    get_latest_market_data_append_only_routing_status,
    list_market_data_append_only_routing_decisions,
)
from services.runtime.incremental_evaluation import (
    get_incremental_evaluation_status,
    process_incremental_evaluation_batch,
)
from services.runtime.market_data_append_only_controller import (
    build_market_data_append_only_controller_status,
    list_market_data_append_only_auto_rollback_events,
    persist_market_data_append_only_controller_snapshot,
)
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
    run_market_data_projection_reconcile,
)
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
    run_market_index_projection_reconcile,
)
from services.runtime.market_open_observe_cycle import (
    get_latest_market_open_observe_cycle_run,
    list_market_open_observe_cycle_runs,
    run_market_open_observe_cycle_once,
)
from services.runtime.market_reference_projection_reconcile import (
    get_latest_market_reference_projection_reconcile,
    run_market_reference_projection_reconcile,
)
from services.runtime.projection_outbox_backlog import (
    build_projection_outbox_backlog_status,
    projection_outbox_backlog_summary_fields,
)
from services.runtime.projection_outbox_bulk_retire import (
    bulk_retire_projection_outbox,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from services.runtime.projection_replay import get_projection_replay_status
from storage.event_retention import (
    EventRetentionSafetyError,
    get_event_retention_status,
    prune_event_store_events,
)
from storage.gateway_order_broker_boundary import (
    get_order_broker_boundary_status,
    list_order_broker_boundaries,
)
from storage.live_sim_order_plan_uniqueness import (
    get_live_sim_order_plan_uniqueness_status,
)
from storage.projection_outbox import get_projection_outbox_status
from storage.projection_retention import build_projection_retention_rca
from storage.projection_watermarks import (
    ProjectionWatermarkBackfillSafetyError,
    backfill_projection_event_results_from_outbox,
    get_projection_watermark_status,
    list_projection_event_results,
)
from storage.sqlite import open_connection
from storage.sqlite_locking import (
    configure_sqlite_busy_timeout,
    is_sqlite_locked_error,
    retry_sqlite_locked,
    sqlite_lock_retry_metadata,
)

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/operator")


@router.get("/status")
def operator_status(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_operator_status(
            connection,
            settings=settings,
            trade_date=trade_date,
            include_no_buy_rebuild=True,
        )
    finally:
        connection.close()


@router.get("/no-buy")
def operator_no_buy(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=100),
    include_ai: bool | None = Query(default=None),
    include_debug: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = build_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=trade_date,
            manual=True,
            limit=limit,
            include_ai=include_ai,
            include_debug=include_debug,
            write_snapshot=False,
        )
        return snapshot.to_dict()
    finally:
        connection.close()


@router.get("/no-buy/latest")
def operator_no_buy_latest(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = get_latest_no_buy_sentinel_snapshot(connection, trade_date=trade_date)
        return {
            "snapshot": snapshot,
            "read_only": True,
            "no_order_side_effects": True,
        }
    finally:
        connection.close()


@router.post("/no-buy/rebuild", dependencies=[Depends(require_local_token)])
def operator_no_buy_rebuild(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=100),
    include_ai: bool | None = Query(default=None),
    include_debug: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = rebuild_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=trade_date,
            limit=limit,
            include_ai=include_ai,
            include_debug=include_debug,
        )
        return snapshot.to_dict()
    finally:
        connection.close()


@router.post("/observe-cycle/run-once", dependencies=[Depends(require_local_token)])
def operator_observe_cycle_run_once(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = run_market_open_observe_cycle_once(
                connection,
                settings=settings,
                trade_date=trade_date,
                limit=limit,
            )
        except EvaluationRunLockError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.get("/observe-cycle/runs")
def operator_observe_cycle_runs(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_market_open_observe_cycle_runs(connection, limit=limit)
    finally:
        connection.close()
    return {
        "runs": runs,
        "read_only": True,
        "observe_only": True,
        "no_order_controls": True,
    }


@router.get("/observe-cycle/runs/latest")
def operator_observe_cycle_run_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        run = get_latest_market_open_observe_cycle_run(connection)
    finally:
        connection.close()
    return {
        "run": run,
        "read_only": True,
        "observe_only": True,
        "no_order_controls": True,
    }


@router.get("/incremental-evaluation/status")
def operator_incremental_evaluation_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_incremental_evaluation_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/projection-outbox/status")
def operator_projection_outbox_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        status = get_projection_outbox_status(connection, settings=settings)
        backlog = build_projection_outbox_backlog_status(
            connection,
            settings=settings,
            sample_limit=3,
        ).to_dict()
        status.update(projection_outbox_backlog_summary_fields(backlog))
        status["backlog"] = backlog
        return status
    finally:
        connection.close()


@router.get("/projection-replay/status")
def operator_projection_replay_status() -> dict[str, Any]:
    return get_projection_replay_status()


@router.get("/runtime-execution-locks/status")
def operator_runtime_execution_locks_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_runtime_execution_lock_status(connection)
    finally:
        connection.close()


@router.get("/live-sim/order-plan-uniqueness/status")
def operator_live_sim_order_plan_uniqueness_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_live_sim_order_plan_uniqueness_status(connection)
    finally:
        connection.close()


@router.get("/gateway/order-broker-boundaries/status")
def operator_order_broker_boundaries_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_order_broker_boundary_status(connection)
    finally:
        connection.close()


@router.get("/gateway/order-broker-boundaries")
def operator_order_broker_boundaries(
    state: str | None = Query(default=None, min_length=1, max_length=32),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        items = list_order_broker_boundaries(
            connection,
            state=state,
            limit=limit,
        )
        return {
            "items": items,
            "count": len(items),
            "read_only": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }
    finally:
        connection.close()


@router.get("/projection-outbox/backlog")
def operator_projection_outbox_backlog() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_projection_outbox_backlog_status(
            connection,
            settings=settings,
            sample_limit=3,
        ).to_dict()
    finally:
        connection.close()


@router.post("/projection-outbox/run-once", dependencies=[Depends(require_local_token)])
def operator_projection_outbox_run_once(
    limit: int | None = Query(default=None, ge=1, le=500),
    apply_projection: bool = Query(default=False),
    live_safe: bool = Query(default=True),
    projection_name: str | None = Query(default=None, min_length=1, max_length=64),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        try:
            result = retry_sqlite_locked(
                lambda: process_projection_outbox_batch(
                    connection,
                    settings=settings,
                    limit=limit,
                    apply_projection=apply_projection,
                    live_safe=live_safe,
                    projection_name=projection_name,
                ),
                attempts=settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_retry,
            )
        except sqlite3.OperationalError as exc:
            if is_sqlite_locked_error(exc):
                return _locked_retryable_operator_response(
                    settings=settings,
                    endpoint="projection_outbox_run_once",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=not bool(
                        apply_projection
                        and settings.projection_outbox_apply_projection_enabled
                        and (
                            settings.projection_outbox_market_data_apply_enabled
                            or settings.projection_outbox_market_reference_apply_enabled
                        )
                    ),
                    operator_action=(
                        "retry with a smaller live_safe batch after ingest pressure drops"
                    ),
                )
            raise
        payload = result.to_dict()
        payload["read_only_projection"] = not payload["projection_side_effects_allowed"]
        payload["retryable"] = bool(payload.get("retryable"))
        return payload
    finally:
        connection.close()


@router.post("/projection-outbox/drain-once", dependencies=[Depends(require_local_token)])
def operator_projection_outbox_drain_once(
    limit: int = Query(default=100, ge=1, le=500),
    apply_projection: bool = Query(default=True),
    live_safe: bool = Query(default=True),
    max_batches: int = Query(default=1, ge=1, le=100),
    stop_on_locked: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0
    batches_run = 0
    claimed_count = 0
    applied_count = 0
    skipped_count = 0
    error_count = 0
    dead_letter_count = 0
    projection_side_effects_allowed = False
    result_statuses: list[str] = []
    locked_payload: dict[str, Any] | None = None
    pending_before = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        pending_before = int(
            build_projection_outbox_backlog_status(
                connection,
                settings=settings,
                sample_limit=0,
            ).total_pending_count
        )
        for _ in range(max_batches):
            try:
                result = retry_sqlite_locked(
                    lambda: process_projection_outbox_batch(
                        connection,
                        settings=settings,
                        limit=limit,
                        apply_projection=apply_projection,
                        live_safe=live_safe,
                    ),
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                    max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                    on_retry=_on_retry,
                )
            except sqlite3.OperationalError as exc:
                if not is_sqlite_locked_error(exc):
                    raise
                locked_payload = _locked_retryable_operator_payload(
                    settings=settings,
                    endpoint="projection_outbox_drain_once",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=not bool(
                        apply_projection
                        and settings.projection_outbox_apply_projection_enabled
                        and (
                            settings.projection_outbox_market_data_apply_enabled
                            or settings.projection_outbox_market_reference_apply_enabled
                        )
                    ),
                    operator_action=(
                        "retry with a smaller live_safe drain batch after ingest pressure drops"
                    ),
                )
                if stop_on_locked:
                    break
                continue
            payload = result.to_dict()
            batches_run += 1
            claimed_count += int(payload.get("claimed_count") or 0)
            applied_count += int(payload.get("applied_count") or 0)
            skipped_count += int(payload.get("skipped_count") or 0)
            error_count += int(payload.get("error_count") or 0)
            dead_letter_count += int(payload.get("dead_letter_count") or 0)
            locked_retry_count += int(payload.get("locked_retry_count") or 0)
            projection_side_effects_allowed = (
                projection_side_effects_allowed
                or bool(payload.get("projection_side_effects_allowed"))
            )
            result_statuses.append(str(payload.get("status") or "UNKNOWN"))
            if int(payload.get("claimed_count") or 0) == 0:
                break
        pending_after = int(
            build_projection_outbox_backlog_status(
                connection,
                settings=settings,
                sample_limit=0,
            ).total_pending_count
        )
        if locked_payload is not None:
            status = "LOCKED_RETRYABLE" if batches_run == 0 else "PARTIAL"
        elif error_count > 0 or dead_letter_count > 0:
            status = "COMPLETED_WITH_WARNINGS"
        elif any(value.startswith("PARTIAL") for value in result_statuses):
            status = "PARTIAL"
        else:
            status = "COMPLETED"
        return {
            "status": status,
            "retryable": locked_payload is not None,
            "batches_run": batches_run,
            "claimed_count": claimed_count,
            "applied_count": applied_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "dead_letter_count": dead_letter_count,
            "pending_before": pending_before,
            "pending_after": pending_after,
            "pending_delta": pending_after - pending_before,
            "locked_retry_count": locked_retry_count,
            "lock_metadata": (
                None if locked_payload is None else locked_payload.get("lock_metadata")
            ),
            "reason_codes": (
                [] if locked_payload is None else locked_payload.get("reason_codes", [])
            ),
            "operator_action": (
                None if locked_payload is None else locked_payload.get("operator_action")
            ),
            "read_only_projection": not projection_side_effects_allowed,
            "no_trading_side_effects": True,
            "projection_side_effects_allowed": projection_side_effects_allowed,
            "projection_side_effects_requested": bool(apply_projection),
            "live_safe": bool(live_safe),
            "result_statuses": result_statuses,
        }
    finally:
        connection.close()


@router.post("/projection-outbox/bulk-retire", dependencies=[Depends(require_local_token)])
def operator_projection_outbox_bulk_retire(
    limit: int = Query(default=5000, ge=1, le=20000),
    dry_run: bool = Query(default=True),
    older_than_sec: int = Query(default=60, ge=0, le=86400),
    include_projection_names: str | None = Query(default=None),
    exclude_recent_condition_events: bool = Query(default=True),
    live_safe: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        try:
            result = retry_sqlite_locked(
                lambda: bulk_retire_projection_outbox(
                    connection,
                    settings=settings,
                    limit=limit,
                    dry_run=dry_run,
                    older_than_sec=older_than_sec,
                    include_projection_names=_parse_csv_query(include_projection_names),
                    exclude_recent_condition_events=exclude_recent_condition_events,
                    live_safe=live_safe,
                ),
                attempts=settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_retry,
            )
        except sqlite3.OperationalError as exc:
            if is_sqlite_locked_error(exc):
                payload = _locked_retryable_operator_payload(
                    settings=settings,
                    endpoint="projection_outbox_bulk_retire",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=True,
                    operator_action=(
                        "retry bulk retire later or run dry_run while live ingest pressure drops"
                    ),
                )
                payload.update(
                    {
                        "dry_run": bool(dry_run),
                        "no_trading_side_effects": True,
                        "projection_side_effects_allowed": False,
                    }
                )
                return payload
            raise
        payload = result.to_dict()
        payload["locked_retry_count"] = int(payload.get("locked_retry_count") or 0) + (
            locked_retry_count
        )
        payload["read_only_projection"] = True
        payload["retryable"] = False
        return payload
    finally:
        connection.close()


@router.get("/market-data-projection-reconcile/latest")
def operator_market_data_projection_reconcile_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_data_projection_reconcile(connection)
    finally:
        connection.close()


@router.post(
    "/market-data-projection-reconcile/run-once",
    dependencies=[Depends(require_local_token)],
)
def operator_market_data_projection_reconcile_run_once(
    limit: int = Query(default=500, ge=1, le=5000),
    min_event_rowid: int | None = Query(default=None, ge=1),
    max_event_rowid: int | None = Query(default=None, ge=1),
    persist: bool | None = Query(default=None),
    live_safe: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    effective_persist = (
        bool(settings.market_data_reconcile_live_default_persist)
        if persist is None and live_safe
        else bool(True if persist is None else persist)
    )
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        try:
            result = retry_sqlite_locked(
                lambda: run_market_data_projection_reconcile(
                    connection,
                    settings=settings,
                    limit=limit,
                    min_event_rowid=min_event_rowid,
                    max_event_rowid=max_event_rowid,
                    persist=effective_persist,
                ),
                attempts=settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_retry,
            )
            locked_fallback_used = False
        except sqlite3.OperationalError as exc:
            if (
                is_sqlite_locked_error(exc)
                and live_safe
                and effective_persist
                and settings.market_data_reconcile_locked_fallback_to_read_only
            ):
                fallback_connection = open_connection(settings.trading_db_path)
                _configure_operator_run_once_connection(
                    fallback_connection,
                    settings=settings,
                )
                try:
                    result = retry_sqlite_locked(
                        lambda: run_market_data_projection_reconcile(
                            fallback_connection,
                            settings=settings,
                            limit=limit,
                            min_event_rowid=min_event_rowid,
                            max_event_rowid=max_event_rowid,
                            persist=False,
                        ),
                        attempts=settings.operator_sqlite_lock_retry_attempts,
                        base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                        max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                        on_retry=_on_retry,
                    )
                except sqlite3.OperationalError as fallback_exc:
                    if is_sqlite_locked_error(fallback_exc):
                        return _locked_retryable_operator_response(
                            settings=settings,
                            endpoint="market_data_projection_reconcile_run_once",
                            exc=fallback_exc,
                            attempts=settings.operator_sqlite_lock_retry_attempts,
                            elapsed_ms=(time.monotonic() - started_at) * 1000,
                            locked_retry_count=locked_retry_count,
                            read_only_projection=True,
                            operator_action=(
                                "retry later; even read-only reconcile was locked "
                                "during live ingest pressure"
                            ),
                        )
                    raise
                finally:
                    fallback_connection.close()
                payload = result.to_dict()
                payload["status"] = "WARN_READ_ONLY_RESULT"
                payload["persisted"] = False
                payload["persist_requested"] = bool(effective_persist)
                payload["locked_fallback_used"] = True
                payload["retryable"] = True
                payload["reason_codes"] = sorted(
                    set(payload.get("reason_codes") or []) | {"SQLITE_DATABASE_LOCKED"}
                )
                payload["lock_metadata"] = sqlite_lock_retry_metadata(
                    exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                )
                payload["locked_retry_count"] = locked_retry_count
                payload["read_only_projection"] = True
                payload["read_only"] = True
                payload["no_trading_side_effects"] = True
                return payload
            if is_sqlite_locked_error(exc):
                return _locked_retryable_operator_response(
                    settings=settings,
                    endpoint="market_data_projection_reconcile_run_once",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=True,
                    operator_action=(
                        "retry later or call with persist=false during live ingest pressure"
                    ),
                )
            raise
        payload = result.to_dict()
        payload["read_only_projection"] = True
        payload["read_only"] = True
        payload["persist_requested"] = bool(effective_persist)
        payload["locked_fallback_used"] = locked_fallback_used
        payload["retryable"] = False
        payload["locked_retry_count"] = locked_retry_count
        return payload
    finally:
        connection.close()


@router.get("/market-data-append-only-routing/status")
def operator_market_data_append_only_routing_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_data_append_only_routing_status(
            connection,
            settings=settings,
        )
    finally:
        connection.close()


@router.get("/market-data-append-only/controller/status")
def operator_market_data_append_only_controller_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = build_market_data_append_only_controller_status(
            connection,
            settings=settings,
        ).to_dict()
        payload["read_only"] = True
        payload["no_trading_side_effects"] = True
        return payload
    finally:
        connection.close()


@router.post(
    "/market-data-append-only/controller/snapshot",
    dependencies=[Depends(require_local_token)],
)
def operator_market_data_append_only_controller_snapshot() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        status = build_market_data_append_only_controller_status(
            connection,
            settings=settings,
        )
        snapshot_id = persist_market_data_append_only_controller_snapshot(
            connection,
            status,
        )
        payload = status.to_dict()
        payload["snapshot_id"] = snapshot_id
        payload["config_changed"] = False
        payload["read_only"] = True
        payload["no_trading_side_effects"] = True
        return payload
    finally:
        connection.close()


@router.get("/market-data-append-only/controller/rollback-events")
def operator_market_data_append_only_controller_rollback_events(
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return {
            "events": list_market_data_append_only_auto_rollback_events(
                connection,
                limit=limit,
            ),
            "read_only": True,
            "no_trading_side_effects": True,
        }
    finally:
        connection.close()


@router.get("/market-data-append-only-routing/decisions")
def operator_market_data_append_only_routing_decisions(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return {
            "decisions": list_market_data_append_only_routing_decisions(
                connection,
                limit=limit,
            ),
            "read_only": True,
            "no_trading_side_effects": True,
        }
    finally:
        connection.close()


@router.get("/market-reference-projection-reconcile/latest")
def operator_market_reference_projection_reconcile_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_reference_projection_reconcile(connection)
    finally:
        connection.close()


@router.post(
    "/market-reference-projection-reconcile/run-once",
    dependencies=[Depends(require_local_token)],
)
def operator_market_reference_projection_reconcile_run_once(
    limit: int = Query(default=100, ge=1, le=5000),
    persist: bool = Query(default=True),
    live_safe: bool = Query(default=True),
) -> dict[str, Any]:
    del live_safe
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        try:
            result = retry_sqlite_locked(
                lambda: run_market_reference_projection_reconcile(
                    connection,
                    settings=settings,
                    limit=limit,
                    persist=persist,
                ),
                attempts=settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_retry,
            )
        except sqlite3.OperationalError as exc:
            if is_sqlite_locked_error(exc):
                return _locked_retryable_operator_response(
                    settings=settings,
                    endpoint="market_reference_projection_reconcile_run_once",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=True,
                    operator_action=(
                        "retry later or call with persist=false during live ingest pressure"
                    ),
                )
            raise
        payload = result.to_dict()
        payload["read_only_projection"] = True
        payload["read_only"] = True
        payload["persist_requested"] = bool(persist)
        payload["retryable"] = False
        payload["locked_retry_count"] = locked_retry_count
        return payload
    finally:
        connection.close()


@router.get("/market-reference-append-only-routing/status")
def operator_market_reference_append_only_routing_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_reference_append_only_routing_status(
            connection,
            settings=settings,
        )
    finally:
        connection.close()


@router.get("/market-reference-append-only-routing/decisions")
def operator_market_reference_append_only_routing_decisions(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        decisions = list_market_reference_append_only_routing_decisions(
            connection,
            limit=limit,
        )
    finally:
        connection.close()
    return {
        "decisions": decisions,
        "read_only": True,
        "no_trading_side_effects": True,
    }


@router.get("/market-reference/status")
def operator_market_reference_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = build_market_reference_status(connection, settings=settings)
        payload["read_only"] = True
        return payload
    finally:
        connection.close()


@router.get("/market-index-projection-reconcile/latest")
def operator_market_index_projection_reconcile_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_index_projection_reconcile(connection)
    finally:
        connection.close()


@router.post(
    "/market-index-projection-reconcile/run-once",
    dependencies=[Depends(require_local_token)],
)
def operator_market_index_projection_reconcile_run_once(
    limit: int = Query(default=100, ge=1, le=5000),
    persist: bool = Query(default=True),
    live_safe: bool = Query(default=True),
) -> dict[str, Any]:
    del live_safe
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    _configure_operator_run_once_connection(connection, settings=settings)
    started_at = time.monotonic()
    locked_retry_count = 0

    def _on_retry(exc: BaseException, attempt: int) -> None:
        del exc, attempt
        nonlocal locked_retry_count
        locked_retry_count += 1

    try:
        try:
            result = retry_sqlite_locked(
                lambda: run_market_index_projection_reconcile(
                    connection,
                    settings=settings,
                    limit=limit,
                    persist=persist,
                ),
                attempts=settings.operator_sqlite_lock_retry_attempts,
                base_sleep_sec=settings.operator_sqlite_lock_retry_base_sleep_sec,
                max_sleep_sec=settings.operator_sqlite_lock_retry_max_sleep_sec,
                on_retry=_on_retry,
            )
        except sqlite3.OperationalError as exc:
            if is_sqlite_locked_error(exc):
                return _locked_retryable_operator_response(
                    settings=settings,
                    endpoint="market_index_projection_reconcile_run_once",
                    exc=exc,
                    attempts=settings.operator_sqlite_lock_retry_attempts,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    locked_retry_count=locked_retry_count,
                    read_only_projection=True,
                    operator_action=(
                        "retry later or call with persist=false during live ingest pressure"
                    ),
                )
            raise
        payload = result.to_dict()
        payload["read_only_projection"] = True
        payload["read_only"] = True
        payload["persist_requested"] = bool(persist)
        payload["retryable"] = False
        payload["locked_retry_count"] = locked_retry_count
        return payload
    finally:
        connection.close()


@router.get("/market-index-append-only-routing/status")
def operator_market_index_append_only_routing_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_latest_market_index_append_only_routing_status(
            connection,
            settings=settings,
        )
    finally:
        connection.close()


@router.get("/market-index-append-only-routing/decisions")
def operator_market_index_append_only_routing_decisions(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        decisions = list_market_index_append_only_routing_decisions(
            connection,
            limit=limit,
        )
    finally:
        connection.close()
    return {
        "decisions": decisions,
        "read_only": True,
        "no_trading_side_effects": True,
    }


@router.post("/incremental-evaluation/run-once", dependencies=[Depends(require_local_token)])
def operator_incremental_evaluation_run_once(
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = process_incremental_evaluation_batch(
                connection,
                settings=settings,
                limit=limit,
            )
        except EvaluationRunLockError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.get("/event-retention/status")
def operator_event_retention_status(
    retention_days: int | None = Query(default=None, ge=1),
    exact_counts: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = get_event_retention_status(
            connection,
            settings=settings,
            retention_days=retention_days,
            exact_counts=exact_counts,
        )
        payload["read_only"] = True
        return payload
    finally:
        connection.close()


@router.post("/event-retention/prune", dependencies=[Depends(require_local_token)])
def operator_event_retention_prune(
    retention_days: int | None = Query(default=None, ge=1),
    dry_run: bool = Query(default=True),
    limit: int | None = Query(default=None, ge=1, le=100000),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = prune_event_store_events(
                connection,
                settings=settings,
                retention_days=retention_days,
                dry_run=dry_run,
                limit=limit,
            )
        except EventRetentionSafetyError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.get("/realtime-subscriptions/plan")
def operator_realtime_subscriptions_plan(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plan = build_realtime_subscription_plan(
            connection,
            settings=settings,
            trade_date=trade_date,
            queue_commands=False,
        )
        payload = plan.to_dict()
        payload["read_only"] = True
        payload["queue_commands"] = False
        payload["command_count"] = 0
        return payload
    finally:
        connection.close()


@router.post("/realtime-subscriptions/run-once", dependencies=[Depends(require_local_token)])
def operator_realtime_subscriptions_run_once(
    trade_date: str | None = Query(default=None),
    queue_commands: bool | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plan = run_realtime_subscription_once(
            connection,
            settings=settings,
            trade_date=trade_date,
            queue_commands=queue_commands,
        )
        return plan.to_dict()
    finally:
        connection.close()


@router.get("/projection-watermarks/status")
def operator_projection_watermarks_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_projection_watermark_status(connection)
    finally:
        connection.close()


@router.get("/projection-watermarks/results")
def operator_projection_watermark_results(
    projection_name: str | None = Query(default=None, min_length=1, max_length=64),
    status: str | None = Query(default=None, min_length=1, max_length=16),
    event_id: str | None = Query(default=None, min_length=1, max_length=128),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        items = list_projection_event_results(
            connection,
            projection_name=projection_name,
            status=status,
            event_id=event_id,
            limit=limit,
        )
        return {
            "items": items,
            "count": len(items),
            "read_only": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }
    finally:
        connection.close()


@router.post(
    "/projection-watermarks/backfill",
    dependencies=[Depends(require_local_token)],
)
def operator_projection_watermark_backfill(
    limit: int = Query(default=100, ge=1, le=5000),
    dry_run: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            return backfill_projection_event_results_from_outbox(
                connection,
                limit=limit,
                dry_run=dry_run,
                apply_enabled=settings.projection_event_result_backfill_enabled,
            )
        except ProjectionWatermarkBackfillSafetyError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
    finally:
        connection.close()


@router.get("/projection-retention/rca")
def operator_projection_retention_rca(
    event_id: str | None = Query(default=None, min_length=1, max_length=128),
    retention_days: int | None = Query(default=None, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    blocked_only: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        retention = get_event_retention_status(
            connection,
            settings=settings,
            retention_days=retention_days,
        )
        return build_projection_retention_rca(
            connection,
            cutoff_at=str(retention["cutoff_at"]),
            event_id=event_id,
            limit=limit,
            blocked_only=blocked_only,
        )
    finally:
        connection.close()


def _locked_retryable_operator_response(
    *,
    settings: Any,
    endpoint: str,
    exc: BaseException,
    attempts: int,
    elapsed_ms: float,
    locked_retry_count: int,
    read_only_projection: bool,
    operator_action: str,
) -> dict[str, Any]:
    payload = _locked_retryable_operator_payload(
        settings=settings,
        endpoint=endpoint,
        exc=exc,
        attempts=attempts,
        elapsed_ms=elapsed_ms,
        locked_retry_count=locked_retry_count,
        read_only_projection=read_only_projection,
        operator_action=operator_action,
    )
    configured_status = int(getattr(settings, "operator_run_once_locked_http_status", 409))
    if configured_status == 200:
        return payload
    raise HTTPException(status_code=configured_status, detail=payload) from exc


def _locked_retryable_operator_payload(
    *,
    settings: Any,
    endpoint: str,
    exc: sqlite3.OperationalError,
    attempts: int,
    elapsed_ms: float,
    locked_retry_count: int,
    read_only_projection: bool,
    operator_action: str,
) -> dict[str, Any]:
    del settings
    return {
        "status": "LOCKED_RETRYABLE",
        "retryable": True,
        "reason_codes": ["SQLITE_DATABASE_LOCKED"],
        "message": "SQLite write lock contention observed during live ingest.",
        "endpoint": endpoint,
        "locked_retry_count": int(locked_retry_count),
        "lock_metadata": sqlite_lock_retry_metadata(
            exc,
            attempts=attempts,
            elapsed_ms=elapsed_ms,
        ),
        "read_only_projection": bool(read_only_projection),
        "no_trading_side_effects": True,
        "operator_action": operator_action,
    }


def _configure_operator_run_once_connection(
    connection: sqlite3.Connection,
    *,
    settings: Any,
) -> None:
    configure_sqlite_busy_timeout(
        connection,
        timeout_ms=int(getattr(settings, "operator_sqlite_busy_timeout_ms", 500)),
    )


def _parse_csv_query(value: str | None) -> list[str] | None:
    if value is None:
        return None
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed or None
