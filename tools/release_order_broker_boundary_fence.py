from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import datetime_to_wire, market_today, utc_now  # noqa: E402
from services.config import (  # noqa: E402
    Settings,
    clear_settings_cache,
    load_settings,
)
from storage.gateway_order_broker_boundary import (  # noqa: E402
    FENCE_APPROVAL_CONTRACT,
    FENCE_EXPECTED_APP_NAME,
    FENCE_EXPECTED_SCHEMA_VERSION,
    FENCE_REINSTATE_REASON_CODE,
    FENCE_RELEASE_REASON_CODE,
    OrderBrokerBoundaryResolutionError,
    build_order_broker_boundary_fence_approval,
    get_order_broker_boundary_fence_approval_binding,
    get_order_broker_boundary_status,
    preview_order_broker_boundary_fence_reinstate,
    preview_order_broker_boundary_fence_release,
    reinstate_order_broker_boundary_maintenance_fence,
    release_order_broker_boundary_maintenance_fence,
)
from tools.resolve_order_broker_boundary import (  # noqa: E402
    BoundaryResolutionCliError,
    _assert_apply_runtime_safe,
    _open_existing_read_write,
    _open_strict_read_only,
    _require_safe_label,
    _sanitize,
)

PREFLIGHT_CONTRACT = "gateway-order-boundary-fence-preflight.v1"
EVIDENCE_CONTRACT = "gateway-order-boundary-fence-evidence.v1"
APPROVAL_CONTRACT = FENCE_APPROVAL_CONTRACT
RELEASE_REASON_CODE = FENCE_RELEASE_REASON_CODE
REINSTATE_REASON_CODE = FENCE_REINSTATE_REASON_CODE
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TRADE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class FenceReleaseCliError(BoundaryResolutionCliError):
    pass


def build_fence_event_preflight(
    connection: sqlite3.Connection,
    *,
    action: str,
    command_id: str,
    command_alias: str,
    approval_id: str,
    request_id: str,
    operator_id: str,
    trade_date: str,
) -> dict[str, Any]:
    normalized_action = str(action).strip().upper()
    if normalized_action not in {"RELEASE", "REINSTATE"}:
        raise FenceReleaseCliError("FENCE_ACTION_INVALID")
    _validate_public_inputs(
        command_alias=command_alias,
        approval_id=approval_id,
        request_id=request_id,
        operator_id=operator_id,
        trade_date=trade_date,
    )
    preview = (
        preview_order_broker_boundary_fence_release(connection, command_id)
        if normalized_action == "RELEASE"
        else preview_order_broker_boundary_fence_reinstate(connection, command_id)
    )
    binding = get_order_broker_boundary_fence_approval_binding(connection)
    counts = binding["gateway_commands"]
    schema_version = binding["schema_version"]
    target = _approval_target(normalized_action, preview)
    preflight_reason_codes = list(preview.get("reason_codes") or [])
    if binding["app_name"] != FENCE_EXPECTED_APP_NAME:
        preflight_reason_codes.append("APP_NAME_MISMATCH")
    if binding["schema_version"] != FENCE_EXPECTED_SCHEMA_VERSION:
        preflight_reason_codes.append("SCHEMA_VERSION_MISMATCH")
    if str(trade_date) != market_today():
        preflight_reason_codes.append("PREFLIGHT_NOT_CURRENT_TRADE_DATE")
    preflight_reason_codes = list(dict.fromkeys(preflight_reason_codes))
    evidence = {
        "contract": EVIDENCE_CONTRACT,
        "action": normalized_action,
        "trade_date": str(trade_date),
        "command_alias": str(command_alias),
        "command_id": str(command_id),
        "schema_version": schema_version,
        "app_name": binding["app_name"],
        "database_identity_sha256": binding["database_identity_sha256"],
        "eligible": bool(preview.get("eligible")),
        "reason_codes": preflight_reason_codes,
        "target": target,
        "broker_reach_evidence_count": int(preview.get("broker_reach_evidence_count") or 0),
        "fence_event_count": int(preview.get("fence_event_count") or 0),
        "fence_chain_valid": bool(preview.get("fence_chain_valid", True)),
        "maintenance_fence_active": bool(preview.get("maintenance_fence_active")),
        "maintenance_fence_released": bool(preview.get("maintenance_fence_released")),
        "gateway_commands": {
            "total_count": int(counts["total_count"]),
            "order_count": int(counts["order_count"]),
            "state_fingerprint": str(counts["state_fingerprint"]),
        },
        "raw_boundary_changed": False,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_only": True,
        "live_real_allowed": False,
    }
    evidence_canonical_json = _canonical_json(evidence)
    evidence_sha256 = _sha256_text(evidence_canonical_json)
    reason_code = RELEASE_REASON_CODE if normalized_action == "RELEASE" else REINSTATE_REASON_CODE
    target_complete = bool(
        target.get("resolution_id")
        and _SHA256_RE.fullmatch(str(target.get("resolution_request_hash") or ""))
        and _SHA256_RE.fullmatch(str(target.get("source_boundary_fingerprint") or ""))
        and (normalized_action == "RELEASE" or target.get("expected_release_event_id"))
    )
    approval = (
        build_order_broker_boundary_fence_approval(
            action=normalized_action,
            approval_id=approval_id,
            request_id=request_id,
            operator_id=operator_id,
            reason_code=reason_code,
            approval_trade_date=trade_date,
            command_alias=command_alias,
            command_id=command_id,
            expected_previous_fence_event_id=(
                target.get("expected_previous_fence_event_id")
                if normalized_action == "RELEASE"
                else target.get("expected_release_event_id")
            ),
            expected_resolution_id=str(target["resolution_id"]),
            expected_resolution_request_hash=str(target["resolution_request_hash"]),
            expected_source_boundary_fingerprint=str(target["source_boundary_fingerprint"]),
            evidence_sha256=evidence_sha256,
            database_identity_sha256=str(binding["database_identity_sha256"]),
            expected_app_name=FENCE_EXPECTED_APP_NAME,
            expected_schema_version=FENCE_EXPECTED_SCHEMA_VERSION,
            expected_gateway_command_total_count=int(counts["total_count"]),
            expected_order_command_count=int(counts["order_count"]),
            expected_gateway_command_state_fingerprint=str(counts["state_fingerprint"]),
        )
        if target_complete
        else {"payload": None, "canonical_json": None, "sha256": None}
    )
    eligible = bool(
        preview.get("eligible")
        and binding["app_name"] == FENCE_EXPECTED_APP_NAME
        and binding["schema_version"] == FENCE_EXPECTED_SCHEMA_VERSION
        and target_complete
        and str(trade_date) == market_today()
    )
    return {
        "contract": PREFLIGHT_CONTRACT,
        "status": "APPROVAL_READY" if eligible else "BLOCKED",
        "action": normalized_action,
        "trade_date": str(trade_date),
        "evidence": {
            "payload": evidence,
            "canonical_json": evidence_canonical_json,
            "sha256": evidence_sha256,
        },
        "approval": {
            "payload": approval["payload"],
            "canonical_json": approval["canonical_json"],
            "expected_sha256": approval["sha256"],
        },
        "generated_at": datetime_to_wire(utc_now()),
        "read_only": True,
        "query_only": True,
        "apply_authorized": False,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_real_allowed": False,
    }


def run_fence_event_preflight(
    db_path: Path,
    *,
    action: str,
    command_id: str,
    command_alias: str,
    approval_id: str,
    request_id: str,
    operator_id: str,
    trade_date: str,
    out_path: Path | None = None,
) -> dict[str, Any]:
    connection = _open_strict_read_only(db_path)
    try:
        report = build_fence_event_preflight(
            connection,
            action=action,
            command_id=command_id,
            command_alias=command_alias,
            approval_id=approval_id,
            request_id=request_id,
            operator_id=operator_id,
            trade_date=trade_date,
        )
    finally:
        connection.close()
    report_bytes = _json_bytes(report)
    if out_path is not None:
        _write_atomic(out_path, report_bytes)
    return {
        **report,
        "report_sha256": hashlib.sha256(report_bytes).hexdigest(),
    }


def apply_fence_event_from_preflight(
    db_path: Path,
    *,
    preflight_report: Path,
    expected_preflight_report_sha256: str,
    expected_approval_sha256: str,
    settings: Settings,
) -> dict[str, Any]:
    _require_sha256(
        "PREFLIGHT_REPORT_SHA256_INVALID",
        expected_preflight_report_sha256,
    )
    _require_sha256("APPROVAL_SHA256_INVALID", expected_approval_sha256)
    report, actual_report_sha256 = _read_stable_json(preflight_report)
    if actual_report_sha256 != expected_preflight_report_sha256:
        raise FenceReleaseCliError("PREFLIGHT_REPORT_SHA256_MISMATCH")
    normalized = _validate_preflight_document(
        report,
        expected_approval_sha256=expected_approval_sha256,
    )
    if str(normalized["trade_date"]) != str(market_today()):
        raise FenceReleaseCliError("PREFLIGHT_NOT_CURRENT_TRADE_DATE")
    _assert_apply_runtime_safe(settings, db_path)

    connection = _open_existing_read_write(db_path)
    committed = False
    apply_unknown = False
    verification_error: str | None = None
    verification_exception_type: str | None = None
    after_counts: Mapping[str, Any] | None = None
    boundary_status: Mapping[str, Any] = {
        "status": "POST_APPLY_VERIFICATION_UNAVAILABLE"
    }
    event_status: Mapping[str, Any] = {
        "status": "POST_APPLY_VERIFICATION_UNAVAILABLE"
    }
    try:
        before_binding = get_order_broker_boundary_fence_approval_binding(connection)
        before_counts = before_binding["gateway_commands"]
        payload = normalized["approval_payload"]
        target = normalized["target"]
        approved_commands = _mapping(
            payload.get("gateway_commands"),
            "APPROVAL_GATEWAY_COMMANDS_INVALID",
        )
        common_apply = {
            "approval_id": str(payload["approval_id"]),
            "command_alias": str(payload["command_alias"]),
            "approval_trade_date": str(payload["trade_date"]),
            "approval_sha256": expected_approval_sha256,
            "evidence_sha256": str(normalized["evidence_sha256"]),
            "database_identity_sha256": str(payload["database_identity_sha256"]),
            "expected_app_name": str(payload["expected_app_name"]),
            "expected_schema_version": int(payload["expected_schema_version"]),
            "expected_gateway_command_total_count": int(approved_commands["total_count"]),
            "expected_order_command_count": int(approved_commands["order_count"]),
            "expected_gateway_command_state_fingerprint": str(
                approved_commands["state_fingerprint"]
            ),
            "reason_code": str(payload["reason_code"]),
            "operator_id": str(payload["operator_id"]),
        }
        if normalized["action"] == "RELEASE":
            result = release_order_broker_boundary_maintenance_fence(
                connection,
                command_id=str(payload["command_id"]),
                request_id=str(payload["request_id"]),
                expected_previous_fence_event_id=_optional_text(
                    target.get("expected_previous_fence_event_id")
                ),
                expected_resolution_id=str(target["resolution_id"]),
                expected_resolution_request_hash=str(target["resolution_request_hash"]),
                expected_source_boundary_fingerprint=str(target["source_boundary_fingerprint"]),
                **common_apply,
            )
        else:
            result = reinstate_order_broker_boundary_maintenance_fence(
                connection,
                command_id=str(payload["command_id"]),
                request_id=str(payload["request_id"]),
                expected_release_event_id=str(target["expected_release_event_id"]),
                expected_resolution_id=str(target["resolution_id"]),
                expected_resolution_request_hash=str(target["resolution_request_hash"]),
                expected_source_boundary_fingerprint=str(target["source_boundary_fingerprint"]),
                **common_apply,
            )
        # Both storage entry points return only after their append-only transaction
        # has committed.  Everything below this boundary is verification; a
        # failure here must never be reported as a pre-commit rejection.
        committed = True
        try:
            after_binding = get_order_broker_boundary_fence_approval_binding(connection)
            after_counts = after_binding["gateway_commands"]
            boundary_status = get_order_broker_boundary_status(connection)
            event_status = (
                preview_order_broker_boundary_fence_release(
                    connection,
                    str(payload["command_id"]),
                )
                if normalized["action"] == "RELEASE"
                else preview_order_broker_boundary_fence_reinstate(
                    connection,
                    str(payload["command_id"]),
                )
            )
        except Exception as exc:
            after_counts = None
            boundary_status = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            event_status = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            verification_error = "POST_APPLY_READBACK_FAILED"
            verification_exception_type = type(exc).__name__
            apply_unknown = True
    finally:
        try:
            connection.close()
        except Exception as exc:
            if not committed:
                raise
            after_counts = None
            boundary_status = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            event_status = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            verification_error = "POST_APPLY_CONNECTION_CLOSE_FAILED"
            verification_exception_type = type(exc).__name__
            apply_unknown = True

    invariant_ok = bool(
        after_counts is not None
        and int(after_counts["total_count"]) == int(before_counts["total_count"])
        and int(after_counts["order_count"]) == int(before_counts["order_count"])
        and str(after_counts["state_fingerprint"]) == str(before_counts["state_fingerprint"])
    )
    replayed = bool(result.get("idempotent_replay"))
    replay_effective = bool(result.get("idempotent_replay_effective"))
    result_release_status = result.get("fence_release_status")
    release_effective = bool(
        normalized["action"] != "RELEASE"
        or (
            result.get("maintenance_fence_released") is True
            and result_release_status in {None, "RELEASED"}
            and event_status.get("maintenance_fence_released") is True
            and event_status.get("fence_release_status") == "RELEASED"
        )
    )
    if verification_error is not None:
        status = "APPLIED_WITH_VERIFICATION_FAILURE"
    elif not release_effective and not replayed:
        status = "APPLIED_NOT_EFFECTIVE"
    elif replayed and not replay_effective:
        status = "REPLAYED_NOT_EFFECTIVE"
    elif not invariant_ok:
        status = "APPLIED_WITH_CONCURRENT_COMMAND_CHANGE"
    elif replayed:
        status = "REPLAYED_EFFECTIVE"
    else:
        status = "APPLIED"
    return {
        "status": status,
        "action": normalized["action"],
        "result": _sanitize(result),
        "boundary_status": _sanitize(boundary_status),
        "event_status": _sanitize(event_status),
        "preflight_report_sha256": actual_report_sha256,
        "approval_sha256": expected_approval_sha256,
        "evidence_sha256": normalized["evidence_sha256"],
        "command_count_invariant_ok": invariant_ok,
        "post_apply_verification_error": verification_error,
        "post_apply_verification_exception_type": verification_exception_type,
        "committed": committed,
        "apply_unknown": apply_unknown,
        "raw_boundary_changed": False,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_real_allowed": False,
    }


def _approval_target(action: str, preview: Mapping[str, Any]) -> dict[str, Any]:
    target = {
        "resolution_id": preview.get("resolution_id"),
        "resolution_request_hash": preview.get("resolution_request_hash"),
        "source_boundary_fingerprint": preview.get("source_boundary_fingerprint"),
    }
    if action == "RELEASE":
        target["expected_previous_fence_event_id"] = preview.get("expected_previous_fence_event_id")
    else:
        target["expected_release_event_id"] = preview.get("expected_release_event_id")
    return target


def _validate_preflight_document(
    report: Mapping[str, Any],
    *,
    expected_approval_sha256: str,
) -> dict[str, Any]:
    if report.get("contract") != PREFLIGHT_CONTRACT:
        raise FenceReleaseCliError("PREFLIGHT_CONTRACT_MISMATCH")
    if report.get("status") != "APPROVAL_READY":
        raise FenceReleaseCliError("PREFLIGHT_NOT_APPROVAL_READY")
    evidence = _mapping(report.get("evidence"), "PREFLIGHT_EVIDENCE_INVALID")
    approval = _mapping(report.get("approval"), "PREFLIGHT_APPROVAL_INVALID")
    evidence_payload = _mapping(evidence.get("payload"), "PREFLIGHT_EVIDENCE_PAYLOAD_INVALID")
    approval_payload = _mapping(approval.get("payload"), "PREFLIGHT_APPROVAL_PAYLOAD_INVALID")
    evidence_canonical = _canonical_json(evidence_payload)
    evidence_sha256 = _sha256_text(evidence_canonical)
    if evidence.get("canonical_json") != evidence_canonical:
        raise FenceReleaseCliError("PREFLIGHT_EVIDENCE_CANONICAL_MISMATCH")
    if evidence.get("sha256") != evidence_sha256:
        raise FenceReleaseCliError("PREFLIGHT_EVIDENCE_SHA256_MISMATCH")
    approval_canonical = _canonical_json(approval_payload)
    approval_sha256 = _sha256_text(approval_canonical)
    if approval.get("canonical_json") != approval_canonical:
        raise FenceReleaseCliError("PREFLIGHT_APPROVAL_CANONICAL_MISMATCH")
    if approval.get("expected_sha256") != approval_sha256:
        raise FenceReleaseCliError("PREFLIGHT_APPROVAL_SHA256_MISMATCH")
    if approval_sha256 != expected_approval_sha256:
        raise FenceReleaseCliError("APPROVAL_SHA256_MISMATCH")
    if approval_payload.get("contract") != APPROVAL_CONTRACT:
        raise FenceReleaseCliError("APPROVAL_CONTRACT_MISMATCH")
    if approval_payload.get("evidence_sha256") != evidence_sha256:
        raise FenceReleaseCliError("APPROVAL_EVIDENCE_BINDING_MISMATCH")
    action = str(approval_payload.get("action") or "").upper()
    if action not in {"RELEASE", "REINSTATE"}:
        raise FenceReleaseCliError("FENCE_ACTION_INVALID")
    expected_reason = RELEASE_REASON_CODE if action == "RELEASE" else REINSTATE_REASON_CODE
    if approval_payload.get("reason_code") != expected_reason:
        raise FenceReleaseCliError("APPROVAL_REASON_CODE_MISMATCH")
    if not all(
        approval_payload.get(name) is value
        for name, value in (
            ("one_shot", True),
            ("append_only", True),
            ("raw_boundary_changed", False),
            ("no_order_commands_created", True),
            ("no_broker_calls", True),
            ("live_sim_only", True),
            ("live_real_allowed", False),
        )
    ):
        raise FenceReleaseCliError("APPROVAL_SAFETY_CONTRACT_MISMATCH")
    target = _mapping(approval_payload.get("target"), "APPROVAL_TARGET_INVALID")
    if target != evidence_payload.get("target"):
        raise FenceReleaseCliError("APPROVAL_TARGET_BINDING_MISMATCH")
    for name in (
        "command_id",
        "command_alias",
        "trade_date",
        "database_identity_sha256",
    ):
        if approval_payload.get(name) != evidence_payload.get(name):
            raise FenceReleaseCliError("APPROVAL_EVIDENCE_BINDING_MISMATCH")
    if approval_payload.get("expected_app_name") != evidence_payload.get("app_name"):
        raise FenceReleaseCliError("APPROVAL_EVIDENCE_BINDING_MISMATCH")
    if approval_payload.get("expected_schema_version") != evidence_payload.get("schema_version"):
        raise FenceReleaseCliError("APPROVAL_EVIDENCE_BINDING_MISMATCH")
    approval_commands = _mapping(
        approval_payload.get("gateway_commands"),
        "APPROVAL_GATEWAY_COMMANDS_INVALID",
    )
    evidence_commands = _mapping(
        evidence_payload.get("gateway_commands"),
        "PREFLIGHT_GATEWAY_COMMANDS_INVALID",
    )
    if approval_commands != evidence_commands:
        raise FenceReleaseCliError("APPROVAL_EVIDENCE_BINDING_MISMATCH")
    try:
        rebuilt = build_order_broker_boundary_fence_approval(
            action=action,
            approval_id=str(approval_payload.get("approval_id") or ""),
            request_id=str(approval_payload.get("request_id") or ""),
            operator_id=str(approval_payload.get("operator_id") or ""),
            reason_code=str(approval_payload.get("reason_code") or ""),
            approval_trade_date=str(approval_payload.get("trade_date") or ""),
            command_alias=str(approval_payload.get("command_alias") or ""),
            command_id=str(approval_payload.get("command_id") or ""),
            expected_previous_fence_event_id=(
                _optional_text(target.get("expected_previous_fence_event_id"))
                if action == "RELEASE"
                else _optional_text(target.get("expected_release_event_id"))
            ),
            expected_resolution_id=str(target.get("resolution_id") or ""),
            expected_resolution_request_hash=str(target.get("resolution_request_hash") or ""),
            expected_source_boundary_fingerprint=str(
                target.get("source_boundary_fingerprint") or ""
            ),
            evidence_sha256=evidence_sha256,
            database_identity_sha256=str(approval_payload.get("database_identity_sha256") or ""),
            expected_app_name=str(approval_payload.get("expected_app_name") or ""),
            expected_schema_version=int(approval_payload.get("expected_schema_version")),
            expected_gateway_command_total_count=int(approval_commands.get("total_count")),
            expected_order_command_count=int(approval_commands.get("order_count")),
            expected_gateway_command_state_fingerprint=str(
                approval_commands.get("state_fingerprint") or ""
            ),
        )
    except (OrderBrokerBoundaryResolutionError, TypeError, ValueError) as exc:
        raise FenceReleaseCliError("APPROVAL_STORAGE_CONTRACT_INVALID") from exc
    if rebuilt["payload"] != approval_payload:
        raise FenceReleaseCliError("APPROVAL_STORAGE_CONTRACT_MISMATCH")
    if rebuilt["canonical_json"] != approval_canonical:
        raise FenceReleaseCliError("APPROVAL_STORAGE_CONTRACT_MISMATCH")
    if rebuilt["sha256"] != approval_sha256:
        raise FenceReleaseCliError("APPROVAL_STORAGE_CONTRACT_MISMATCH")
    return {
        "action": action,
        "trade_date": str(approval_payload.get("trade_date") or ""),
        "approval_payload": approval_payload,
        "target": target,
        "evidence_sha256": evidence_sha256,
    }


def _validate_public_inputs(
    *,
    command_alias: str,
    approval_id: str,
    request_id: str,
    operator_id: str,
    trade_date: str,
) -> None:
    _require_safe_label(command_alias, "COMMAND_ALIAS_INVALID")
    _require_safe_label(approval_id, "APPROVAL_ID_INVALID")
    _require_safe_label(request_id, "REQUEST_ID_INVALID")
    _require_safe_label(operator_id, "OPERATOR_ID_INVALID")
    if not _TRADE_DATE_RE.fullmatch(str(trade_date)):
        raise FenceReleaseCliError("TRADE_DATE_INVALID")


def _mapping(value: object, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FenceReleaseCliError(code)
    return value


def _require_sha256(code: str, value: object) -> str:
    normalized = str(value or "")
    if not _SHA256_RE.fullmatch(normalized):
        raise FenceReleaseCliError(code)
    return normalized


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_atomic(path: Path, content: bytes) -> None:
    target = path.expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_stable_json(path: Path) -> tuple[Mapping[str, Any], str]:
    target = path.expanduser()
    if not target.is_file():
        raise FenceReleaseCliError("PREFLIGHT_REPORT_NOT_FOUND")
    try:
        before = target.stat()
        content = target.read_bytes()
        after = target.stat()
    except OSError as exc:
        raise FenceReleaseCliError("PREFLIGHT_REPORT_UNREADABLE") from exc
    identity_before = (before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or len(content) != after.st_size:
        raise FenceReleaseCliError("PREFLIGHT_REPORT_CHANGED_DURING_READ")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FenceReleaseCliError("PREFLIGHT_REPORT_JSON_INVALID") from exc
    if not isinstance(payload, Mapping):
        raise FenceReleaseCliError("PREFLIGHT_REPORT_JSON_INVALID")
    return payload, hashlib.sha256(content).hexdigest()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create an exact-SHA preflight or append one LIVE_SIM-only "
            "broker-boundary maintenance-fence event."
        )
    )
    parser.add_argument("--db", type=Path)
    parser.add_argument("--action", choices=("release", "reinstate"), default="release")
    parser.add_argument("--command-id")
    parser.add_argument("--command-alias")
    parser.add_argument("--approval-id")
    parser.add_argument("--request-id")
    parser.add_argument("--operator-id")
    parser.add_argument("--trade-date", default=market_today())
    parser.add_argument("--out", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--preflight-report-sha256")
    parser.add_argument("--approval-sha256")
    parser.add_argument("--acknowledge-append-only-fence-change", action="store_true")
    parser.add_argument("--acknowledge-raw-history-preserved", action="store_true")
    parser.add_argument("--acknowledge-late-evidence-fail-closed", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.apply:
            missing = []
            for value, code in (
                (args.preflight_report, "PREFLIGHT_REPORT_REQUIRED"),
                (
                    args.preflight_report_sha256,
                    "PREFLIGHT_REPORT_SHA256_REQUIRED",
                ),
                (args.approval_sha256, "APPROVAL_SHA256_REQUIRED"),
            ):
                if value is None or not str(value).strip():
                    missing.append(code)
            if not args.acknowledge_append_only_fence_change:
                missing.append("APPEND_ONLY_FENCE_CHANGE_ACK_REQUIRED")
            if not args.acknowledge_raw_history_preserved:
                missing.append("RAW_HISTORY_PRESERVATION_ACK_REQUIRED")
            if not args.acknowledge_late_evidence_fail_closed:
                missing.append("LATE_EVIDENCE_FAIL_CLOSED_ACK_REQUIRED")
            if missing:
                raise FenceReleaseCliError(*missing)
            clear_settings_cache()
            settings = load_settings()
            db_path = (
                Path(args.db).expanduser()
                if args.db is not None
                else Path(settings.trading_db_path).expanduser()
            )
            payload = apply_fence_event_from_preflight(
                db_path,
                preflight_report=Path(args.preflight_report),
                expected_preflight_report_sha256=str(args.preflight_report_sha256),
                expected_approval_sha256=str(args.approval_sha256),
                settings=settings,
            )
        else:
            missing = []
            for value, code in (
                (args.command_id, "COMMAND_ID_REQUIRED"),
                (args.command_alias, "COMMAND_ALIAS_REQUIRED"),
                (args.approval_id, "APPROVAL_ID_REQUIRED"),
                (args.request_id, "REQUEST_ID_REQUIRED"),
                (args.operator_id, "OPERATOR_ID_REQUIRED"),
                (args.out, "PREFLIGHT_OUT_REQUIRED"),
            ):
                if value is None or not str(value).strip():
                    missing.append(code)
            if missing:
                raise FenceReleaseCliError(*missing)
            if args.db is not None:
                db_path = Path(args.db).expanduser()
            else:
                clear_settings_cache()
                db_path = Path(load_settings().trading_db_path).expanduser()
            payload = run_fence_event_preflight(
                db_path,
                action=str(args.action),
                command_id=str(args.command_id),
                command_alias=str(args.command_alias),
                approval_id=str(args.approval_id),
                request_id=str(args.request_id),
                operator_id=str(args.operator_id),
                trade_date=str(args.trade_date),
                out_path=Path(args.out),
            )
    except (BoundaryResolutionCliError, OrderBrokerBoundaryResolutionError) as exc:
        reason_codes = (
            list(exc.reason_codes) if isinstance(exc, BoundaryResolutionCliError) else [exc.code]
        )
        payload = {
            "status": "REJECTED",
            "reason_codes": reason_codes,
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except (OSError, ValueError, sqlite3.Error):
        payload = {
            "status": "REJECTED",
            "reason_codes": ["FENCE_TOOL_OPERATION_FAILED"],
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(_sanitize(payload), ensure_ascii=False, indent=2, sort_keys=True))
    status = str(payload.get("status") or "")
    return 0 if status in {"APPROVAL_READY", "APPLIED", "REPLAYED_EFFECTIVE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
