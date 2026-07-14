from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tools.ops_market_data_tr_response_side_effect_check import (  # noqa: E402
    fetch_json,
)

_ALLOWED_COMMAND_STATUSES = frozenset(
    {
        "QUEUED",
        "DISPATCHED",
        "CLAIMED",
        "GATEWAY_STARTED",
        "PRE_ACK_RECORDED",
        "BROKER_ACCEPTED",
        "CHEJAN_CONFIRMED",
        "UNCONFIRMED",
        "ACKED",
        "REJECTED",
        "FAILED",
        "EXPIRED",
        "CANCELLED",
    }
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check durable order broker-boundary state without routing orders."
    )
    parser.add_argument(
        "--core-url",
        default=os.environ.get("TRADING_CORE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("TRADING_CORE_TOKEN")
        or os.environ.get("GATEWAY_CORE_TOKEN", ""),
    )
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--expected-db", required=True)
    parser.add_argument(
        "--require-effective-clear",
        action="store_true",
        help=(
            "Fail unless the effective broker-boundary contract is clear. "
            "Raw historical UNCONFIRMED rows remain visible as warnings."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "order_broker_boundary"),
    )
    args = parser.parse_args()

    report = run_report(
        core_url=args.core_url,
        token=args.token,
        timeout_sec=args.timeout_sec,
        expected_db=Path(args.expected_db),
        out_dir=Path(args.out_dir),
        require_effective_clear=args.require_effective_clear,
    )
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"PASS", "WARN"} else 2


def run_report(
    *,
    core_url: str,
    token: str,
    timeout_sec: float,
    expected_db: Path,
    out_dir: Path,
    require_effective_clear: bool = False,
) -> dict[str, Any]:
    base_url = core_url.rstrip("/")
    expected_database_path = str(expected_db.expanduser().resolve())
    dashboard_query = urllib.parse.urlencode(
        {
            "fast": "true",
            "sections": "order_broker_boundaries,pipeline_summary",
            "timeout_budget_ms": "5000",
        }
    )
    report = {
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "require_effective_clear": bool(require_effective_clear),
        "core_url": base_url,
        "expected_database_path": expected_database_path,
        "core_status": fetch_json(
            f"{base_url}/api/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status": fetch_json(
            f"{base_url}/api/gateway/commands/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "boundary_status": fetch_json(
            f"{base_url}/api/operator/gateway/order-broker-boundaries/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "boundary_rows": fetch_json(
            f"{base_url}/api/operator/gateway/order-broker-boundaries?limit=100",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "dashboard_snapshot": fetch_json(
            f"{base_url}/api/dashboard/snapshot?{dashboard_query}",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
        "command_status_after": fetch_json(
            f"{base_url}/api/gateway/commands/status",
            token=token,
            method="GET",
            timeout_sec=timeout_sec,
        ),
    }
    report["verdict"] = evaluate_report(
        report,
        require_effective_clear=require_effective_clear,
    )
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(path) for key, path in paths.items()}
    return report


def evaluate_report(
    report: Mapping[str, Any],
    *,
    require_effective_clear: bool = False,
) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for key in (
        "core_status",
        "command_status",
        "command_status_after",
        "boundary_status",
        "boundary_rows",
        "dashboard_snapshot",
    ):
        payload = report.get(key)
        if not isinstance(payload, Mapping) or not payload.get("ok", True):
            failures.append(f"{key.upper()}_API_ERROR")

    core_status = _data(report, "core_status")
    command_status = _data(report, "command_status")
    command_status_after = _data(report, "command_status_after")
    status = _data(report, "boundary_status")
    rows = _data(report, "boundary_rows")
    dashboard = _data(report, "dashboard_snapshot")
    dashboard_status = dashboard.get("order_broker_boundaries")
    dashboard_status = (
        dashboard_status if isinstance(dashboard_status, Mapping) else {}
    )

    if core_status.get("mode") != "OBSERVE":
        failures.append("CORE_NOT_OBSERVE")
    if core_status.get("profile") != "OBSERVE":
        failures.append("CORE_PROFILE_NOT_OBSERVE")
    expected_database_path = _canonical_path(
        report.get("expected_database_path")
    )
    observed_database_path = _canonical_path(core_status.get("database_path"))
    if expected_database_path is None:
        failures.append("EXPECTED_DATABASE_PATH_MISSING")
    elif observed_database_path is None:
        failures.append("CORE_DATABASE_PATH_MISSING")
    elif observed_database_path != expected_database_path:
        failures.append("CORE_DATABASE_PATH_MISMATCH")
    if core_status.get("live_sim_allowed") is not False:
        failures.append("LIVE_SIM_ALLOWED")
    if core_status.get("live_real_allowed") is not False:
        failures.append("LIVE_REAL_ALLOWED")
    if command_status.get("order_commands_allowed") is not False:
        failures.append("ORDER_COMMANDS_ALLOWED")
    if command_status_after.get("order_commands_allowed") is not False:
        failures.append("ORDER_COMMANDS_ALLOWED_AFTER_CHECK")

    command_counts = _validated_count_map(
        command_status.get("counts"),
        allowed_keys=_ALLOWED_COMMAND_STATUSES,
    )
    command_counts_after = _validated_count_map(
        command_status_after.get("counts"),
        allowed_keys=_ALLOWED_COMMAND_STATUSES,
    )
    command_type_counts = _validated_count_map(
        command_status.get("command_type_counts")
    )
    command_type_counts_after = _validated_count_map(
        command_status_after.get("command_type_counts")
    )
    if command_counts is None or command_counts_after is None:
        failures.append("COMMAND_STATUS_COUNTS_INVALID")
    elif command_counts != command_counts_after:
        failures.append("COMMAND_STATE_COUNTS_CHANGED_DURING_CHECK")
    if command_type_counts is None or command_type_counts_after is None:
        failures.append("COMMAND_TYPE_COUNTS_INVALID")
    elif command_type_counts != command_type_counts_after:
        failures.append("COMMAND_TYPE_COUNTS_CHANGED_DURING_CHECK")

    command_count_delta = _command_count(command_status_after) - _command_count(
        command_status
    )
    order_command_count = _validated_nonnegative_int(
        command_status.get("order_command_count")
    )
    order_command_count_after = _validated_nonnegative_int(
        command_status_after.get("order_command_count")
    )
    if order_command_count is None or order_command_count_after is None:
        failures.append("ORDER_COMMAND_COUNT_INVALID")
        order_command_count_delta = 0
    else:
        order_command_count_delta = (
            order_command_count_after - order_command_count
        )
    modify_order_count = (command_type_counts or {}).get("modify_order", 0)
    modify_order_count_after = (command_type_counts_after or {}).get(
        "modify_order", 0
    )
    if command_count_delta != 0:
        failures.append("COMMAND_COUNT_CHANGED_DURING_CHECK")
    if order_command_count_delta != 0:
        failures.append("ORDER_COMMAND_COUNT_CHANGED_DURING_CHECK")
    if modify_order_count or modify_order_count_after:
        failures.append("MODIFY_ORDER_COMMAND_PRESENT")

    raw_boundary_status = str(status.get("status") or "")
    effective_boundary_status = str(
        status.get("effective_status") or raw_boundary_status
    )
    if raw_boundary_status == "FAIL" or raw_boundary_status not in {"PASS", "WARN"}:
        failures.append("ORDER_BROKER_BOUNDARY_STATUS_FAIL")
    if (
        effective_boundary_status == "FAIL"
        or effective_boundary_status not in {"PASS", "WARN"}
    ):
        failures.append("ORDER_BROKER_BOUNDARY_EFFECTIVE_STATUS_FAIL")
    if not bool(status.get("table_exists")):
        failures.append("ORDER_BROKER_BOUNDARY_TABLE_MISSING")
    if not bool(status.get("required_indexes_present")):
        failures.append("ORDER_BROKER_BOUNDARY_INDEX_MISSING")
    for field in (
        "missing_boundary_count",
        "durable_pre_ack_gap_count",
        "duplicate_idempotency_count",
        "command_state_mismatch_count",
        "unknown_command_status_count",
        "unknown_state_count",
        "orphan_boundary_count",
        "unexpected_boundary_count",
        "linked_command_type_invalid_count",
        "linked_command_type_mismatch_count",
        "invalid_command_type_count",
        "invalid_scope_count",
        "invalid_resolution_chain_count",
    ):
        count = _validated_nonnegative_int(status.get(field))
        if count is None:
            failures.append(f"ORDER_BOUNDARY_{field.upper()}_INVALID")
        elif count > 0:
            failures.append(f"ORDER_BOUNDARY_{field.upper()}")

    raw_unconfirmed_value = _validated_nonnegative_int(
        status.get("unconfirmed_count")
    )
    if raw_unconfirmed_value is None:
        failures.append("ORDER_BOUNDARY_UNCONFIRMED_COUNT_INVALID")
    raw_unconfirmed_count = raw_unconfirmed_value or 0
    effective_unconfirmed_count = _effective_unconfirmed_count(status)
    effective_contract_errors = _effective_contract_errors(status)
    effective_contract_present = not effective_contract_errors
    raw_block_new_order_routing = status.get("block_new_order_routing") is True
    effective_block_new_order_routing = _effective_block_new_order_routing(status)
    qualification_block_new_order_routing = (
        _qualification_block_new_order_routing(status)
    )
    maintenance_fence_active = (
        status.get("resolution_maintenance_fence_active") is True
    )
    invalidated_resolution_count = _nonnegative_int(
        status.get("invalidated_resolution_count")
    )
    active_order_command_value = _validated_nonnegative_int(
        status.get("active_order_command_count")
    )
    if active_order_command_value is None:
        failures.append("ACTIVE_ORDER_COMMAND_COUNT_INVALID")
    active_order_command_count = active_order_command_value or 0
    if raw_unconfirmed_count > 0:
        warnings.append("UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE")
        if not raw_block_new_order_routing:
            failures.append("UNCONFIRMED_ORDER_BOUNDARY_NOT_BLOCKING")
    if effective_unconfirmed_count > 0:
        warnings.append("EFFECTIVE_UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE")
        if not effective_block_new_order_routing:
            failures.append("EFFECTIVE_UNCONFIRMED_ORDER_BOUNDARY_NOT_BLOCKING")
    if invalidated_resolution_count > 0:
        warnings.append("ORDER_BOUNDARY_RESOLUTION_INVALIDATED")
        if not effective_block_new_order_routing:
            failures.append("INVALIDATED_ORDER_BOUNDARY_NOT_BLOCKING")
    if maintenance_fence_active and not effective_block_new_order_routing:
        failures.append("RESOLUTION_MAINTENANCE_FENCE_NOT_BLOCKING")
    if active_order_command_count > 0:
        warnings.append("ACTIVE_ORDER_COMMANDS_PRESENT")
    if require_effective_clear and active_order_command_count > 0:
        failures.append("FAST_0_ACTIVE_ORDER_COMMANDS_PRESENT")
    if require_effective_clear and not effective_contract_present:
        failures.append("FAST_0_EFFECTIVE_CONTRACT_INVALID")
    if require_effective_clear and (
        effective_unconfirmed_count > 0
        or qualification_block_new_order_routing
    ):
        failures.append("FAST_0_EFFECTIVE_ORDER_BOUNDARY_NOT_CLEAR")
    if status.get("no_order_side_effects") is not True:
        failures.append("ORDER_SIDE_EFFECT_FLAG_FALSE")
    if status.get("no_trading_side_effects") is not True:
        failures.append("TRADING_SIDE_EFFECT_FLAG_FALSE")
    if not bool(rows.get("read_only")):
        failures.append("ORDER_BOUNDARY_LIST_NOT_READ_ONLY")
    if not dashboard_status:
        failures.append("DASHBOARD_ORDER_BROKER_BOUNDARY_MISSING")
    elif dashboard_status.get("status") != status.get("status"):
        failures.append("DASHBOARD_ORDER_BROKER_BOUNDARY_MISMATCH")
    elif dashboard_status.get("effective_status") != status.get("effective_status"):
        failures.append("DASHBOARD_EFFECTIVE_ORDER_BROKER_BOUNDARY_MISMATCH")

    boundary_contract_failed = any(
        failure.startswith("ORDER_BROKER_BOUNDARY_")
        or failure.startswith("ORDER_BOUNDARY_")
        or failure.startswith("EFFECTIVE_UNCONFIRMED_")
        or failure.startswith("INVALIDATED_ORDER_BOUNDARY_")
        or failure.startswith("RESOLUTION_MAINTENANCE_")
        or failure.startswith("FAST_0_")
        for failure in failures
    )
    derived_fast_0_status = (
        "BLOCKED"
        if (
            boundary_contract_failed
            or effective_unconfirmed_count > 0
            or qualification_block_new_order_routing
            or active_order_command_count > 0
        )
        else "CLEAR"
    )
    reported_fast_0_status = str(
        status.get("fast_0_status") or derived_fast_0_status
    ).upper()
    if reported_fast_0_status not in {"BLOCKED", "CLEAR"}:
        failures.append("ORDER_BROKER_BOUNDARY_FAST_0_STATUS_INVALID")
    elif reported_fast_0_status != derived_fast_0_status:
        failures.append("ORDER_BROKER_BOUNDARY_FAST_0_STATUS_MISMATCH")
    fast_0_status = (
        reported_fast_0_status
        if reported_fast_0_status == derived_fast_0_status
        else "BLOCKED"
    )
    verdict_status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": verdict_status,
        "failures": sorted(set(failures)),
        "warnings": sorted(set(warnings)),
        "block_next_pr": bool(failures),
        "raw_status": raw_boundary_status,
        "effective_status": effective_boundary_status,
        "fast_0_status": fast_0_status,
        "require_effective_clear": bool(require_effective_clear),
        "block_new_order_routing": raw_block_new_order_routing,
        "effective_block_new_order_routing": effective_block_new_order_routing,
        "qualification_block_new_order_routing": (
            qualification_block_new_order_routing
        ),
        "resolution_maintenance_fence_active": maintenance_fence_active,
        "boundary_count": _nonnegative_int(status.get("boundary_count")),
        "durable_pre_ack_count": _nonnegative_int(
            status.get("durable_pre_ack_count")
        ),
        "unconfirmed_count": raw_unconfirmed_count,
        "raw_unconfirmed_count": raw_unconfirmed_count,
        "effective_unconfirmed_count": effective_unconfirmed_count,
        "effective_resolution_count": _nonnegative_int(
            status.get("effective_resolution_count")
        ),
        "invalidated_resolution_count": invalidated_resolution_count,
        "active_order_command_count": active_order_command_count,
        "effective_contract_present": effective_contract_present,
        "effective_contract_errors": effective_contract_errors,
        "command_count_delta": command_count_delta,
        "order_command_count_delta": order_command_count_delta,
        "modify_order_count": modify_order_count,
        "modify_order_count_after": modify_order_count_after,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return "\n".join(
        [
            "# Order Broker-Boundary Check",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- boundary_count: `{verdict.get('boundary_count')}`",
            f"- durable_pre_ack_count: `{verdict.get('durable_pre_ack_count')}`",
            f"- raw_status: `{verdict.get('raw_status')}`",
            f"- effective_status: `{verdict.get('effective_status')}`",
            f"- fast_0_status: `{verdict.get('fast_0_status')}`",
            f"- raw_unconfirmed_count: `{verdict.get('raw_unconfirmed_count')}`",
            (
                "- effective_unconfirmed_count: "
                f"`{verdict.get('effective_unconfirmed_count')}`"
            ),
            (
                "- effective_resolution_count: "
                f"`{verdict.get('effective_resolution_count')}`"
            ),
            (
                "- invalidated_resolution_count: "
                f"`{verdict.get('invalidated_resolution_count')}`"
            ),
            f"- command_count_delta: `{verdict.get('command_count_delta')}`",
            (
                "- order_command_count_delta: "
                f"`{verdict.get('order_command_count_delta')}`"
            ),
            f"- modify_order_count: `{verdict.get('modify_order_count')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            f"- warnings: `{', '.join(verdict.get('warnings') or []) or '-'}`",
            "",
            "This check is read-only and never polls, queues, or sends an order.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict")
    verdict = verdict if isinstance(verdict, Mapping) else {}
    return (
        "Order broker-boundary: "
        f"{verdict.get('status')} fast_0={verdict.get('fast_0_status')} "
        f"boundaries={verdict.get('boundary_count')} "
        f"pre_ack={verdict.get('durable_pre_ack_count')} "
        f"raw_unconfirmed={verdict.get('raw_unconfirmed_count')} "
        f"effective_unconfirmed={verdict.get('effective_unconfirmed_count')}"
    )


def _data(report: Mapping[str, Any], key: str) -> dict[str, Any]:
    payload = report.get(key)
    if not isinstance(payload, Mapping):
        return {}
    data = payload.get("data")
    return dict(data) if isinstance(data, Mapping) else dict(payload)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _canonical_path(value: object) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return os.path.normcase(str(Path(normalized).expanduser().resolve()))
    except OSError:
        return None


def _validated_nonnegative_int(value: object) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        return None
    return value


def _nonnegative_int(value: object) -> int:
    parsed = _validated_nonnegative_int(value)
    return 0 if parsed is None else parsed


def _validated_count_map(
    value: object,
    *,
    allowed_keys: frozenset[str] | None = None,
) -> dict[str, int] | None:
    if not isinstance(value, Mapping):
        return None
    normalized: dict[str, int] = {}
    for key, count in value.items():
        parsed = _validated_nonnegative_int(count)
        if (
            not isinstance(key, str)
            or not key
            or parsed is None
            or (allowed_keys is not None and key not in allowed_keys)
        ):
            return None
        normalized[key] = parsed
    return normalized


def _command_count(status: Mapping[str, Any]) -> int:
    counts = _validated_count_map(
        status.get("counts"),
        allowed_keys=_ALLOWED_COMMAND_STATUSES,
    )
    return 0 if counts is None else sum(counts.values())


def _effective_unconfirmed_count(status: Mapping[str, Any]) -> int:
    value = status.get("effective_unconfirmed_count")
    if isinstance(value, int) and not isinstance(value, bool):
        return max(value, 0)
    return int(status.get("unconfirmed_count") or 0)


def _effective_block_new_order_routing(status: Mapping[str, Any]) -> bool:
    value = status.get("effective_block_new_order_routing")
    if isinstance(value, bool):
        return value
    return status.get("block_new_order_routing") is True


def _qualification_block_new_order_routing(status: Mapping[str, Any]) -> bool:
    value = status.get("qualification_block_new_order_routing")
    if isinstance(value, bool):
        return value
    return _effective_block_new_order_routing(status)


def _effective_contract_errors(status: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field, allowed in (
        ("effective_status", {"PASS", "WARN", "FAIL"}),
        ("fast_0_status", {"CLEAR", "BLOCKED"}),
    ):
        value = status.get(field)
        if not isinstance(value, str) or value.upper() not in allowed:
            errors.append(f"{field.upper()}_INVALID")
    for field in (
        "effective_unconfirmed_count",
        "effective_resolution_count",
        "invalidated_resolution_count",
        "resolution_maintenance_fence_active_count",
        "active_order_command_count",
    ):
        value = status.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            errors.append(f"{field.upper()}_INVALID")
    for field in (
        "effective_block_new_order_routing",
        "qualification_block_new_order_routing",
        "resolution_maintenance_fence_active",
    ):
        if not isinstance(status.get(field), bool):
            errors.append(f"{field.upper()}_INVALID")
    for field in (
        "resolution_schema_ready",
        "resolution_source_schema_ready",
        "resolution_table_exists",
        "resolution_required_indexes_present",
        "resolution_append_only_triggers_present",
    ):
        if status.get(field) is not True:
            errors.append(f"{field.upper()}_NOT_READY")
    validated_state_counts = _validated_count_map(
        status.get("effective_state_counts")
    )
    allowed_states = {
        "CLAIMED",
        "GATEWAY_STARTED",
        "PRE_ACK_RECORDED",
        "BROKER_ACCEPTED",
        "CHEJAN_CONFIRMED",
        "UNCONFIRMED",
        "RESOLVED_BROKER_NOT_REACHED",
    }
    if (
        validated_state_counts is None
        or set(validated_state_counts) != allowed_states
    ):
        errors.append("EFFECTIVE_STATE_COUNTS_INVALID")
        return errors
    raw_state_counts = _validated_count_map(status.get("state_counts"))
    raw_states = allowed_states - {"RESOLVED_BROKER_NOT_REACHED"}
    if raw_state_counts is None or set(raw_state_counts) != raw_states:
        errors.append("RAW_STATE_COUNTS_INVALID")
    boundary_count = _validated_nonnegative_int(status.get("boundary_count"))
    if boundary_count is None:
        errors.append("BOUNDARY_COUNT_INVALID")
    else:
        if sum(validated_state_counts.values()) != boundary_count:
            errors.append("EFFECTIVE_STATE_COUNT_TOTAL_MISMATCH")
        if raw_state_counts is not None and set(raw_state_counts) == raw_states:
            if sum(raw_state_counts.values()) != boundary_count:
                errors.append("RAW_STATE_COUNT_TOTAL_MISMATCH")

    effective_unconfirmed_count = _validated_nonnegative_int(
        status.get("effective_unconfirmed_count")
    )
    effective_resolution_count = _validated_nonnegative_int(
        status.get("effective_resolution_count")
    )
    invalidated_resolution_count = _validated_nonnegative_int(
        status.get("invalidated_resolution_count")
    )
    maintenance_fence_count = _validated_nonnegative_int(
        status.get("resolution_maintenance_fence_active_count")
    )
    active_order_command_count = _validated_nonnegative_int(
        status.get("active_order_command_count")
    )
    if (
        effective_unconfirmed_count is not None
        and validated_state_counts.get("UNCONFIRMED", 0)
        != effective_unconfirmed_count
    ):
        errors.append("EFFECTIVE_UNCONFIRMED_COUNT_MISMATCH")
    if (
        effective_resolution_count is not None
        and validated_state_counts.get("RESOLVED_BROKER_NOT_REACHED", 0)
        != effective_resolution_count
    ):
        errors.append("EFFECTIVE_RESOLUTION_COUNT_MISMATCH")
    maintenance_active = status.get("resolution_maintenance_fence_active")
    if maintenance_fence_count is not None and isinstance(
        maintenance_active, bool
    ):
        if maintenance_active != (maintenance_fence_count > 0):
            errors.append("RESOLUTION_MAINTENANCE_FENCE_COUNT_MISMATCH")
    qualification_blocked = status.get("qualification_block_new_order_routing")
    effective_blocked = status.get("effective_block_new_order_routing")
    if isinstance(qualification_blocked, bool):
        must_qualify_block = bool(
            (effective_unconfirmed_count or 0) > 0
            or (invalidated_resolution_count or 0) > 0
            or (active_order_command_count or 0) > 0
        )
        if must_qualify_block and not qualification_blocked:
            errors.append("QUALIFICATION_BLOCK_INCONSISTENT")
        fast_0_status = status.get("fast_0_status")
        if isinstance(fast_0_status, str) and (
            (fast_0_status.upper() == "BLOCKED") != qualification_blocked
        ):
            errors.append("FAST_0_STATUS_INCONSISTENT")
    if isinstance(effective_blocked, bool) and isinstance(
        maintenance_active, bool
    ):
        boundary_requires_block = bool(
            (effective_unconfirmed_count or 0) > 0
            or (invalidated_resolution_count or 0) > 0
            or maintenance_active
        )
        if boundary_requires_block and not effective_blocked:
            errors.append("EFFECTIVE_ROUTING_BLOCK_INCONSISTENT")
    return errors


if __name__ == "__main__":
    raise SystemExit(main())
