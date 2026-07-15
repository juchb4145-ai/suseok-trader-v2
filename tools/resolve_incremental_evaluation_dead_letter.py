from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.config import (  # noqa: E402
    DEFAULT_ENV_FILE_PATH,
    ENV_FILE_PATH_ENV,
    Settings,
    TradingMode,
    TradingProfile,
    clear_settings_cache,
    load_settings,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (  # noqa: E402
    build_incremental_evaluation_dead_letter_effective_status,
    preview_incremental_evaluation_dead_letter_disposition,
    record_incremental_evaluation_dead_letter_disposition,
    recover_incremental_evaluation_dead_letters,
    revoke_incremental_evaluation_dead_letter_disposition,
    verify_incremental_evaluation_recovery_canary,
)

ACTION_DISPOSE = "DISPOSE_OBSOLETE_CLOSED_CANDIDATE"
ACTION_REVOKE = "REVOKE"
ACTION_RESET_CANARY = "RESET_CANARY"
ACTION_VERIFY_CANARY = "VERIFY_CANARY"
ACTION_RESET_BATCH = "RESET_BATCH"
ACTIONS = (
    ACTION_DISPOSE,
    ACTION_REVOKE,
    ACTION_RESET_CANARY,
    ACTION_VERIFY_CANARY,
    ACTION_RESET_BATCH,
)

DISPOSITION_TYPE = "OBSOLETE_CLOSED_CANDIDATE"
DISPOSITION_REASON_CODE = "OPERATOR_DISPOSED_OBSOLETE_CLOSED_CANDIDATE"
DISPOSITION_EVIDENCE_TYPE = "FAST0R2_INCREMENTAL_DEAD_LETTER_RCA"
REVOKE_REASON_CODE = "OPERATOR_REVOKED_INCREMENTAL_DEAD_LETTER_DISPOSITION"
REVOKE_EVIDENCE_TYPE = "FAST0R2_INCREMENTAL_DEAD_LETTER_CORRECTION"
RECOVERY_REASON_CODE = "OPERATOR_APPROVED_BOUNDED_INCREMENTAL_RECOVERY"
RECOVERY_EVIDENCE_TYPE = "FAST0R2_INCREMENTAL_RECOVERY_EVIDENCE"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,127}$")
_LONG_DIGIT_RUN_RE = re.compile(r"[0-9]{8,}")
_SEPARATED_DIGIT_RUN_RE = re.compile(r"(?:[0-9][_.:@-]*){8,}")
_WINDOWS_PATH_RE = re.compile(r"(?i)(?:[A-Z]:[\\/]|\\\\)[^\s\"']+")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:[a-z0-9_-]*(?:token|secret)|api[_-]?key|"
    r"authorization|password|credential)"
    r"\b\s*[:=]\s*[^\s,;]+"
)
_AUTHORIZATION_VALUE_RE = re.compile(r"(?im)\bauthorization\b\s*[:=]\s*[^\r\n]*")
_BEARER_TOKEN_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_EMBEDDED_JSON_SENSITIVE_RE = re.compile(
    r"(?i)([\"'](?:token|secret|api[_-]?key|authorization|"
    r"password|credential|account(?:_id)?|broker_account)"
    r"[\"']\s*:\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^,}\]\s]+)"
)
_LABELED_ACCOUNT_RE = re.compile(
    r"(?i)(?:\baccount(?:_id)?\b|\bbroker_account\b|계좌(?:번호)?)"
    r"\s*[:=#]?\s*[0-9][0-9_.:@-]{7,}"
)
_UNLABELED_ACCOUNT_RE = re.compile(
    r"(?<![0-9A-Za-z])(?:[0-9]{8,14}|[0-9]{4}[- ][0-9]{4}[- ][0-9]{4}|"
    r"[0-9]{4}[- ][0-9]{4})(?![0-9A-Za-z])"
)
_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
_PRODUCER_SETTING_NAMES = (
    "realtime_subscription_queue_commands",
    "dry_run_oms_enabled",
    "dry_run_intent_creation_enabled",
    "dry_run_order_routing_enabled",
    "dry_run_gateway_command_enabled",
    "dry_run_exit_engine_enabled",
    "dry_run_exit_intent_creation_enabled",
    "dry_run_exit_order_creation_enabled",
    "dry_run_exit_order_routing_enabled",
    "dry_run_exit_gateway_command_enabled",
    "live_sim_enabled",
    "live_sim_order_routing_enabled",
    "live_sim_gateway_command_enabled",
    "live_sim_reprice_enabled",
    "live_sim_pilot_pipeline_enabled",
    "live_sim_pilot_auto_queue_command",
    "live_sim_order_plan_routing_enabled",
    "live_sim_cancel_enabled",
    "live_sim_cancel_unfilled_enabled",
    "live_sim_exit_engine_enabled",
    "live_sim_exit_order_creation_enabled",
    "live_sim_exit_gateway_command_enabled",
    "live_sim_exit_eod_flatten_enabled",
    "live_sim_reconcile_request_broker_snapshot_enabled",
    "live_sim_operating_cycle_enabled",
    "live_sim_operating_loop_enabled",
    "live_sim_operating_loop_queue_commands",
    "theme_refresh_queue_market_scan_commands",
)
_EXTERNAL_PRODUCER_ENV_NAMES = {
    "theme_refresh_queue_market_scan_commands": ("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS"),
}
_SENSITIVE_KEY_PARTS = (
    "account",
    "payload",
    "snapshot",
    "evidence_content",
    "file_path",
    "db_path",
    "env_path",
    "token",
    "secret",
    "api_key",
    "api-key",
    "authorization",
    "password",
    "credential",
)
_DISPOSITION_TABLE = "incremental_evaluation_dead_letter_dispositions"
_RECOVERY_GUARD_WRITE_TABLES = frozenset(
    {
        _DISPOSITION_TABLE,
        "runtime_execution_locks",
        "runtime_execution_lock_fences",
    }
)
_RESET_WRITE_TABLES = _RECOVERY_GUARD_WRITE_TABLES | {"incremental_evaluation_queue"}
_DISPOSITION_WRITE_TABLES = frozenset({_DISPOSITION_TABLE})
_SQLITE_WRITE_ACTIONS = frozenset(
    {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
)
_EVALUATION_SIDE_EFFECT_TABLES = (
    "strategy_observations",
    "strategy_observations_latest",
    "strategy_setup_observations",
    "strategy_evaluation_runs",
    "strategy_evaluation_errors",
    "risk_observations",
    "risk_observations_latest",
    "risk_check_observations",
    "risk_evaluation_runs",
    "risk_evaluation_errors",
    "entry_timing_evaluations",
    "entry_timing_evaluation_errors",
    "order_plan_drafts",
    "order_plan_drafts_latest",
)
_ORDER_SIDE_EFFECT_TABLES = (
    "dry_run_intents",
    "dry_run_orders",
    "dry_run_executions",
    "dry_run_intent_rejections",
    "dry_run_runs",
    "dry_run_errors",
    "dry_run_exit_evaluations",
    "dry_run_exit_signals",
    "dry_run_exit_intents",
    "dry_run_exit_orders",
    "dry_run_exit_executions",
    "dry_run_exit_runs",
    "dry_run_exit_errors",
    "live_sim_intents",
    "live_sim_orders",
    "live_sim_executions",
    "live_sim_runs",
    "live_sim_rejections",
    "live_sim_exit_intents",
    "live_sim_cancel_intents",
)
_AUTO_SIDE_EFFECT_TABLES = (
    *_EVALUATION_SIDE_EFFECT_TABLES,
    *_ORDER_SIDE_EFFECT_TABLES,
)


class IncrementalDeadLetterCliError(RuntimeError):
    def __init__(self, *reason_codes: str) -> None:
        normalized = tuple(dict.fromkeys(str(code) for code in reason_codes if str(code)))
        super().__init__(", ".join(normalized) or "INCREMENTAL_DEAD_LETTER_ACTION_REJECTED")
        self.reason_codes = normalized or ("INCREMENTAL_DEAD_LETTER_ACTION_REJECTED",)


def preview_action(
    db_path: Path,
    *,
    dead_letter_ids: Sequence[str],
    action: str,
    expected_fingerprints: Sequence[str | None] | None = None,
    expected_candidate_versions: Sequence[str | None] | None = None,
) -> dict[str, Any]:
    fingerprints = list(expected_fingerprints or [None] * len(dead_letter_ids))
    candidate_versions = list(expected_candidate_versions or [None] * len(dead_letter_ids))
    connection = _open_strict_read_only(db_path)
    try:
        previews = [
            preview_incremental_evaluation_dead_letter_disposition(
                connection,
                dead_letter_id,
                action=action,
                expected_dead_letter_fingerprint=fingerprints[index],
                expected_candidate_version=candidate_versions[index],
            )
            for index, dead_letter_id in enumerate(dead_letter_ids)
        ]
        effective_status = build_incremental_evaluation_dead_letter_effective_status(connection)
    finally:
        connection.close()
    payload: dict[str, Any] = {
        "status": "PREVIEW",
        "mode": "STRICT_READ_ONLY",
        "action": action,
        "previews": previews,
        "effective_status": effective_status,
        "read_only": True,
        "query_only": True,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_real_allowed": False,
    }
    if len(previews) == 1:
        payload["preview"] = previews[0]
    return payload


def apply_action(
    db_path: Path,
    *,
    action: str,
    dead_letter_ids: Sequence[str],
    request_id: str,
    expected_fingerprints: Sequence[str],
    expected_candidate_versions: Sequence[str],
    evidence_file: Path,
    evidence_ref: str,
    operator_id: str,
    settings: Settings,
    revoke_disposition_id: str | None,
    recovery_session_id: str | None,
    verify_canary_disposition_id: str | None,
) -> dict[str, Any]:
    evidence_sha256 = _sha256_file(evidence_file)
    evidence_type, reason_code = _evidence_contract(action)
    connection = _open_existing_read_write(db_path)
    verification_error: str | None = None
    try:
        commands_before = _command_counts(connection)
        side_effects_before = _auto_side_effect_counts(connection)
        raw_before = _dead_letter_rows_fingerprint(connection, dead_letter_ids)
        disposition_before = _table_row_snapshot(
            connection,
            _DISPOSITION_TABLE,
            "disposition_id",
        )
        queue_before = _table_row_snapshot(
            connection,
            "incremental_evaluation_queue",
            "candidate_instance_id",
        )
        runtime_locks_before = _table_row_snapshot(
            connection,
            "runtime_execution_locks",
            "lock_name",
        )
        runtime_fences_before = _table_row_snapshot(
            connection,
            "runtime_execution_lock_fences",
            "lock_name",
        )
        target_write_contract = _build_apply_target_write_contract(
            connection,
            action=action,
            dead_letter_ids=dead_letter_ids,
            request_id=request_id,
            expected_fingerprints=expected_fingerprints,
            expected_candidate_versions=expected_candidate_versions,
            evidence_type=evidence_type,
            evidence_ref=evidence_ref,
            evidence_sha256=evidence_sha256,
            operator_id=operator_id,
            reason_code=reason_code,
            revoke_disposition_id=revoke_disposition_id,
            recovery_session_id=recovery_session_id,
            verify_canary_disposition_id=verify_canary_disposition_id,
            disposition_before=disposition_before,
            queue_before=queue_before,
            runtime_locks_before=runtime_locks_before,
            runtime_fences_before=runtime_fences_before,
        )
        row_guard_violations: list[str] = []
        _install_apply_row_guards(
            connection,
            contract=target_write_contract,
            violations=row_guard_violations,
        )
        blocked_write_tables: list[str] = []
        connection.set_authorizer(
            _build_apply_write_authorizer(
                action=action,
                blocked_write_tables=blocked_write_tables,
            )
        )
        try:
            if action == ACTION_DISPOSE:
                result = record_incremental_evaluation_dead_letter_disposition(
                    connection,
                    dead_letter_id=dead_letter_ids[0],
                    request_id=request_id,
                    expected_dead_letter_fingerprint=expected_fingerprints[0],
                    expected_candidate_version=expected_candidate_versions[0],
                    disposition_type=DISPOSITION_TYPE,
                    reason_code=reason_code,
                    evidence_type=evidence_type,
                    evidence_ref=evidence_ref,
                    evidence_sha256=evidence_sha256,
                    operator_id=operator_id,
                )
            elif action == ACTION_REVOKE:
                result = revoke_incremental_evaluation_dead_letter_disposition(
                    connection,
                    dead_letter_id=dead_letter_ids[0],
                    request_id=request_id,
                    expected_dead_letter_fingerprint=expected_fingerprints[0],
                    expected_candidate_version=expected_candidate_versions[0],
                    supersedes_disposition_id=str(revoke_disposition_id),
                    reason_code=reason_code,
                    evidence_type=evidence_type,
                    evidence_ref=evidence_ref,
                    evidence_sha256=evidence_sha256,
                    operator_id=operator_id,
                )
            elif action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
                expected_versions = [
                    {
                        "dead_letter_id": dead_letter_id,
                        "dead_letter_fingerprint": expected_fingerprints[index],
                        "candidate_version": expected_candidate_versions[index],
                    }
                    for index, dead_letter_id in enumerate(dead_letter_ids)
                ]
                result = recover_incremental_evaluation_dead_letters(
                    connection,
                    dead_letter_ids=list(dead_letter_ids),
                    mode=action,
                    recovery_session_id=str(recovery_session_id),
                    request_id=request_id,
                    expected_versions=expected_versions,
                    settings=settings,
                    operator_id=operator_id,
                    evidence_type=evidence_type,
                    evidence_ref=evidence_ref,
                    evidence_sha256=evidence_sha256,
                    reason_code=reason_code,
                    verify_canary_disposition_id=verify_canary_disposition_id,
                )
            else:
                result = verify_incremental_evaluation_recovery_canary(
                    connection,
                    dead_letter_id=dead_letter_ids[0],
                    recovery_session_id=str(recovery_session_id),
                    request_id=request_id,
                    expected_dead_letter_fingerprint=expected_fingerprints[0],
                    expected_candidate_version=expected_candidate_versions[0],
                    settings=settings,
                    operator_id=operator_id,
                    evidence_type=evidence_type,
                    evidence_ref=evidence_ref,
                    evidence_sha256=evidence_sha256,
                    reason_code=reason_code,
                )
        except sqlite3.DatabaseError as exc:
            if blocked_write_tables or row_guard_violations:
                raise IncrementalDeadLetterCliError("UNEXPECTED_DATABASE_WRITE_BLOCKED") from exc
            raise
        finally:
            connection.set_authorizer(None)
        try:
            commands_after = _command_counts(connection)
            side_effects_after = _auto_side_effect_counts(connection)
            raw_after = _dead_letter_rows_fingerprint(connection, dead_letter_ids)
            disposition_after = _table_row_snapshot(
                connection,
                _DISPOSITION_TABLE,
                "disposition_id",
            )
            queue_after = _table_row_snapshot(
                connection,
                "incremental_evaluation_queue",
                "candidate_instance_id",
            )
            runtime_locks_after = _table_row_snapshot(
                connection,
                "runtime_execution_locks",
                "lock_name",
            )
            runtime_fences_after = _table_row_snapshot(
                connection,
                "runtime_execution_lock_fences",
                "lock_name",
            )
            target_write_invariant_ok, target_write_invariant_reason_codes = (
                _verify_apply_target_write_invariant(
                    contract=target_write_contract,
                    disposition_before=disposition_before,
                    disposition_after=disposition_after,
                    queue_before=queue_before,
                    queue_after=queue_after,
                    runtime_locks_before=runtime_locks_before,
                    runtime_locks_after=runtime_locks_after,
                    runtime_fences_before=runtime_fences_before,
                    runtime_fences_after=runtime_fences_after,
                )
            )
            effective_status = build_incremental_evaluation_dead_letter_effective_status(connection)
        except (sqlite3.Error, RuntimeError, ValueError, TypeError, KeyError):
            commands_after = None
            side_effects_after = None
            raw_after = None
            target_write_invariant_ok = None
            target_write_invariant_reason_codes = ["TARGET_WRITE_READBACK_FAILED"]
            effective_status = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            verification_error = "POST_APPLY_READBACK_FAILED"
    finally:
        connection.close()

    command_count_delta = (
        None if commands_after is None else commands_after["total"] - commands_before["total"]
    )
    order_command_count_delta = (
        None if commands_after is None else commands_after["order"] - commands_before["order"]
    )
    command_state_unchanged = bool(
        commands_after is not None
        and commands_before["state_fingerprint"] == commands_after["state_fingerprint"]
    )
    command_invariant_ok = bool(
        commands_after is not None
        and command_count_delta == 0
        and order_command_count_delta == 0
        and command_state_unchanged
    )
    side_effect_count_deltas = (
        None
        if side_effects_after is None
        else {
            table: side_effects_after[table] - side_effects_before[table]
            for table in _AUTO_SIDE_EFFECT_TABLES
        }
    )
    side_effect_count_invariant_ok = bool(
        side_effect_count_deltas is not None
        and all(delta == 0 for delta in side_effect_count_deltas.values())
    )
    no_evaluation_run = (
        None
        if side_effect_count_deltas is None
        else all(side_effect_count_deltas[table] == 0 for table in _EVALUATION_SIDE_EFFECT_TABLES)
    )
    no_order_artifacts_created = (
        None
        if side_effect_count_deltas is None
        else all(side_effect_count_deltas[table] == 0 for table in _ORDER_SIDE_EFFECT_TABLES)
    )
    no_order_commands_created = (
        None if order_command_count_delta is None else order_command_count_delta == 0
    )
    no_broker_calls = (
        None
        if no_order_artifacts_created is None or no_order_commands_created is None
        else no_order_artifacts_created and no_order_commands_created
    )
    raw_unchanged = None if raw_after is None else raw_before == raw_after
    replayed = bool(
        _value(result, "idempotent_replay")
        or str(_value(result, "status") or "").upper() == "REPLAYED"
    )
    if verification_error is not None:
        status = "APPLIED_WITH_VERIFICATION_FAILURE"
    elif target_write_invariant_ok is not True:
        status = "APPLIED_WITH_TARGET_WRITE_CHANGE"
    elif not side_effect_count_invariant_ok:
        status = "APPLIED_WITH_SIDE_EFFECT_CHANGE"
    elif not command_invariant_ok:
        status = "APPLIED_WITH_CONCURRENT_COMMAND_CHANGE"
    elif raw_unchanged is not True:
        status = "APPLIED_WITH_RAW_DEAD_LETTER_CHANGE"
    elif replayed:
        status = "REPLAYED"
    elif action == ACTION_REVOKE:
        status = "REVOKED"
    elif action == ACTION_RESET_CANARY:
        status = "RECOVERY_CANARY_APPLIED"
    elif action == ACTION_VERIFY_CANARY:
        status = "RECOVERY_CANARY_VERIFIED"
    elif action == ACTION_RESET_BATCH:
        status = "RECOVERY_BATCH_APPLIED"
    else:
        status = "APPLIED"
    return {
        "status": status,
        "mode": action,
        "action": action,
        "result": result,
        "effective_status": effective_status,
        "evidence_type": evidence_type,
        "evidence_ref": evidence_ref,
        "evidence_sha256": evidence_sha256,
        "operator_id": operator_id,
        "command_count_delta": command_count_delta,
        "order_command_count_delta": order_command_count_delta,
        "command_state_unchanged": command_state_unchanged,
        "command_count_invariant_ok": command_invariant_ok,
        "auto_side_effect_count_deltas": side_effect_count_deltas,
        "auto_side_effect_count_invariant_ok": side_effect_count_invariant_ok,
        "raw_dead_letters_unchanged": raw_unchanged,
        "target_write_invariant_ok": target_write_invariant_ok,
        "target_write_invariant_reason_codes": target_write_invariant_reason_codes,
        "post_apply_verification_error": verification_error,
        "no_evaluation_run": no_evaluation_run,
        "no_order_artifacts_created": no_order_artifacts_created,
        "no_order_commands_created": no_order_commands_created,
        "no_broker_calls": no_broker_calls,
        "live_real_allowed": False,
        "runtime_safety": _runtime_safety_summary(settings),
    }


def _evidence_contract(action: str) -> tuple[str, str]:
    if action == ACTION_REVOKE:
        return REVOKE_EVIDENCE_TYPE, REVOKE_REASON_CODE
    if action in {ACTION_RESET_CANARY, ACTION_VERIFY_CANARY, ACTION_RESET_BATCH}:
        return RECOVERY_EVIDENCE_TYPE, RECOVERY_REASON_CODE
    return DISPOSITION_EVIDENCE_TYPE, DISPOSITION_REASON_CODE


def _assert_apply_runtime_safe(settings: Settings, db_path: Path) -> None:
    reason_codes: list[str] = []
    configured_env = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    env_path: Path | None = None
    if not configured_env:
        reason_codes.append("EXPLICIT_TRADING_ENV_FILE_REQUIRED")
    else:
        env_path = Path(configured_env).expanduser()
        if not env_path.is_file():
            reason_codes.append("TRADING_ENV_FILE_NOT_FOUND")
        elif _same_path(env_path, DEFAULT_ENV_FILE_PATH):
            reason_codes.append("DEFAULT_DOTENV_FORBIDDEN_FOR_APPLY")

    if settings.trading_profile is not TradingProfile.OBSERVE:
        reason_codes.append("TRADING_PROFILE_NOT_OBSERVE")
    if settings.trading_mode is not TradingMode.OBSERVE:
        reason_codes.append("TRADING_MODE_NOT_OBSERVE")
    if settings.trading_allow_live_sim:
        reason_codes.append("LIVE_SIM_ALLOWED")
    if settings.trading_allow_live_real:
        reason_codes.append("LIVE_REAL_ALLOWED")
    if not settings.live_sim_kill_switch:
        reason_codes.append("LIVE_SIM_KILL_SWITCH_OFF")
    if settings.incremental_evaluation_worker_enabled:
        reason_codes.append("INCREMENTAL_EVALUATION_WORKER_ENABLED")
    for name in _PRODUCER_SETTING_NAMES:
        if _producer_enabled(settings, name, env_path=env_path):
            reason_codes.append(f"COMMAND_PRODUCER_ENABLED:{name.upper()}")

    if env_path is not None and env_path.is_file():
        theme_value = _explicit_env_value(env_path, "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS")
        if theme_value is None:
            reason_codes.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_EXPLICIT")
        elif theme_value.strip().lower() not in _FALSE_VALUES:
            reason_codes.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE")

    configured_db = Path(settings.trading_db_path).expanduser()
    if not _same_path(configured_db, db_path):
        reason_codes.append("DB_PATH_DOES_NOT_MATCH_SAFE_ENV")
    if not db_path.is_file():
        reason_codes.append("DATABASE_NOT_FOUND")
    elif not reason_codes:
        try:
            connection = _open_live_read_only(db_path)
            try:
                lease_count = int(
                    connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
                )
            finally:
                connection.close()
        except sqlite3.Error:
            reason_codes.append("RUNTIME_EXECUTION_LEASE_STATUS_UNAVAILABLE")
        else:
            if lease_count:
                reason_codes.append("RUNTIME_EXECUTION_LEASE_PRESENT")
    if reason_codes:
        raise IncrementalDeadLetterCliError(*reason_codes)


def _runtime_safety_summary(settings: Settings) -> dict[str, Any]:
    return {
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "live_sim_allowed": bool(settings.trading_allow_live_sim),
        "live_real_allowed": bool(settings.trading_allow_live_real),
        "kill_switch_active": bool(settings.live_sim_kill_switch),
        "incremental_worker_enabled": bool(settings.incremental_evaluation_worker_enabled),
        "enabled_command_producers": [
            name
            for name in _PRODUCER_SETTING_NAMES
            if _producer_enabled(settings, name, env_path=_configured_env_path())
        ],
        "explicit_env_file": True,
    }


def _configured_env_path() -> Path | None:
    value = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    return Path(value).expanduser() if value else None


def _producer_enabled(
    settings: Settings,
    name: str,
    *,
    env_path: Path | None,
) -> bool:
    external_env_name = _EXTERNAL_PRODUCER_ENV_NAMES.get(name)
    if external_env_name is not None:
        if env_path is None or not env_path.is_file():
            return True
        value = _explicit_env_value(env_path, external_env_name)
        return value is None or value.strip().lower() not in _FALSE_VALUES
    return bool(getattr(settings, name, True))


def _explicit_env_value(path: Path, env_name: str) -> str | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    value: str | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, candidate = line.split("=", 1)
        if key.strip() != env_name:
            continue
        value = candidate.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
    return value


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    resolved_path = _validated_database_path(path)
    sidecar_paths = (
        Path(f"{resolved_path}-wal"),
        Path(f"{resolved_path}-journal"),
    )
    try:
        if any(sidecar.is_file() and sidecar.stat().st_size for sidecar in sidecar_paths):
            raise IncrementalDeadLetterCliError("STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE")
    except OSError as exc:
        raise IncrementalDeadLetterCliError("STRICT_READ_ONLY_SIDECAR_STATUS_UNAVAILABLE") from exc
    connection = sqlite3.connect(
        _sqlite_uri(resolved_path, mode="ro", immutable=True),
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _open_live_read_only(path: Path) -> sqlite3.Connection:
    resolved_path = _validated_database_path(path)
    _assert_no_rollback_journal(resolved_path)
    connection = sqlite3.connect(
        _sqlite_uri(resolved_path, mode="ro"),
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _open_existing_read_write(path: Path) -> sqlite3.Connection:
    resolved_path = _validated_database_path(path)
    _assert_no_rollback_journal(resolved_path)
    connection = sqlite3.connect(
        _sqlite_uri(resolved_path, mode="rw"),
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _validated_database_path(path: Path) -> Path:
    try:
        resolved_path = path.resolve(strict=True)
        stat_result = resolved_path.stat()
    except OSError as exc:
        raise IncrementalDeadLetterCliError("DATABASE_NOT_FOUND") from exc
    if not resolved_path.is_file():
        raise IncrementalDeadLetterCliError("DATABASE_NOT_FOUND")
    if int(stat_result.st_nlink) != 1:
        raise IncrementalDeadLetterCliError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved_path


def _assert_no_rollback_journal(path: Path) -> None:
    journal_path = Path(f"{path}-journal")
    try:
        if journal_path.is_file() and journal_path.stat().st_size:
            raise IncrementalDeadLetterCliError("DATABASE_ROLLBACK_JOURNAL_PRESENT")
    except OSError as exc:
        raise IncrementalDeadLetterCliError("DATABASE_SIDECAR_STATUS_UNAVAILABLE") from exc


def _sqlite_uri(path: Path, *, mode: str, immutable: bool = False) -> str:
    uri_path = quote(path.resolve().as_posix(), safe="/:")
    immutable_parameter = "&immutable=1" if immutable else ""
    return f"file:{uri_path}?mode={mode}{immutable_parameter}"


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        raise IncrementalDeadLetterCliError("EVIDENCE_FILE_NOT_FOUND")
    digest = hashlib.sha256()
    byte_count = 0
    try:
        path_before = path.stat()
        with path.open("rb") as stream:
            stream_before = os.fstat(stream.fileno())
            while chunk := stream.read(1024 * 1024):
                byte_count += len(chunk)
                digest.update(chunk)
            stream_after = os.fstat(stream.fileno())
        path_after = path.stat()
    except OSError as exc:
        raise IncrementalDeadLetterCliError("EVIDENCE_FILE_UNREADABLE") from exc
    if byte_count == 0:
        raise IncrementalDeadLetterCliError("EVIDENCE_FILE_EMPTY")
    identities = {
        _file_identity(stat_result)
        for stat_result in (path_before, stream_before, stream_after, path_after)
    }
    if len(identities) != 1 or byte_count != int(path_after.st_size):
        raise IncrementalDeadLetterCliError("EVIDENCE_FILE_CHANGED_DURING_HASH")
    return digest.hexdigest()


def _file_identity(stat_result: os.stat_result) -> tuple[int, int, int]:
    return (
        int(stat_result.st_ino),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
    )


def _command_counts(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS total_count,
            SUM(
                CASE WHEN lower(command_type) IN (
                    'send_order', 'cancel_order', 'modify_order'
                ) THEN 1 ELSE 0 END
            ) AS order_count
        FROM gateway_commands
        """
    ).fetchone()
    state_rows = connection.execute(
        """
        SELECT command_id, command_type, status, attempts, created_at,
               available_at, dispatched_at, completed_at, expires_at
        FROM gateway_commands
        ORDER BY command_id
        """
    ).fetchall()
    state_payload = [{key: state_row[key] for key in state_row.keys()} for state_row in state_rows]
    return {
        "total": int(row["total_count"] or 0),
        "order": int(row["order_count"] or 0),
        "state_fingerprint": hashlib.sha256(
            json.dumps(
                state_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
    }


def _auto_side_effect_counts(connection: sqlite3.Connection) -> dict[str, int]:
    placeholders = ",".join("?" for _ in _AUTO_SIDE_EFFECT_TABLES)
    rows = connection.execute(
        f"""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name IN ({placeholders})
        """,
        _AUTO_SIDE_EFFECT_TABLES,
    ).fetchall()
    existing = {str(row["name"]) for row in rows}
    if existing != set(_AUTO_SIDE_EFFECT_TABLES):
        raise IncrementalDeadLetterCliError("AUTO_SIDE_EFFECT_INVARIANT_SCHEMA_UNAVAILABLE")
    return {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in _AUTO_SIDE_EFFECT_TABLES
    }


def _table_row_snapshot(
    connection: sqlite3.Connection,
    table_name: str,
    primary_key: str,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(f'SELECT * FROM "{table_name}" ORDER BY "{primary_key}"').fetchall()
    return {str(row[primary_key]): {key: row[key] for key in row.keys()} for row in rows}


def _build_apply_target_write_contract(
    connection: sqlite3.Connection,
    *,
    action: str,
    dead_letter_ids: Sequence[str],
    request_id: str,
    expected_fingerprints: Sequence[str],
    expected_candidate_versions: Sequence[str],
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    operator_id: str,
    reason_code: str,
    revoke_disposition_id: str | None,
    recovery_session_id: str | None,
    verify_canary_disposition_id: str | None,
    disposition_before: Mapping[str, Mapping[str, Any]],
    queue_before: Mapping[str, Mapping[str, Any]],
    runtime_locks_before: Mapping[str, Mapping[str, Any]],
    runtime_fences_before: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    placeholders = ",".join("?" for _ in dead_letter_ids)
    raw_rows = connection.execute(
        f"SELECT * FROM incremental_evaluation_dead_letters "
        f"WHERE dead_letter_id IN ({placeholders})",
        tuple(dead_letter_ids),
    ).fetchall()
    raw_by_id = {str(row["dead_letter_id"]): row for row in raw_rows}
    prior_by_dead_letter: dict[str, list[Mapping[str, Any]]] = {}
    for row in disposition_before.values():
        prior_by_dead_letter.setdefault(str(row["dead_letter_id"]), []).append(row)
    for rows in prior_by_dead_letter.values():
        rows.sort(
            key=lambda row: (
                int(row["sequence_no"]),
                str(row["created_at"]),
                str(row["disposition_id"]),
            )
        )

    recovery_action = action in {ACTION_RESET_CANARY, ACTION_VERIFY_CANARY, ACTION_RESET_BATCH}
    batch_size = len(dead_letter_ids) if recovery_action else None
    prior_fence = runtime_fences_before.get("evaluation_pipeline")
    expected_fencing_token = (
        None
        if not recovery_action
        else 1
        if prior_fence is None
        else _required_int(prior_fence["last_fencing_token"]) + 1
    )
    existing_requests = {str(row["request_id"]) for row in disposition_before.values()}
    disposition_specs: dict[str, dict[str, Any]] = {}
    queue_specs: dict[str, dict[str, Any]] = {}
    for index, dead_letter_id in enumerate(dead_letter_ids):
        item_request_id = (
            request_id if len(dead_letter_ids) == 1 else f"{request_id}.item-{index + 1}"
        )
        prior = prior_by_dead_letter.get(str(dead_letter_id), [])
        latest_disposition_id = None if not prior else str(prior[-1]["disposition_id"])
        supersedes_disposition_id = (
            str(revoke_disposition_id) if action == ACTION_REVOKE else latest_disposition_id
        )
        authorization_disposition_id = (
            str(verify_canary_disposition_id) if action == ACTION_RESET_BATCH else None
        )
        request_payload = {
            "action": action,
            "dead_letter_id": str(dead_letter_id),
            "request_id": item_request_id,
            "expected_dead_letter_fingerprint": str(expected_fingerprints[index]),
            "expected_candidate_version": str(expected_candidate_versions[index]),
            "reason_code": reason_code,
            "operator_id": operator_id,
            "evidence_type": evidence_type,
            "evidence_ref": evidence_ref,
            "evidence_sha256": evidence_sha256,
            "supersedes_disposition_id": supersedes_disposition_id,
            "recovery_session_id": recovery_session_id if recovery_action else None,
            "batch_size": batch_size,
            "authorization_disposition_id": authorization_disposition_id,
        }
        raw = raw_by_id.get(str(dead_letter_id))
        candidate_instance_id = None if raw is None else str(raw["candidate_instance_id"])
        disposition_specs[item_request_id] = {
            **request_payload,
            "request_hash": _json_sha256(request_payload),
            "sequence_no": len(prior) + 1,
            "candidate_instance_id": candidate_instance_id,
            "fencing_token_required": recovery_action,
            "expected_fencing_token": expected_fencing_token,
        }
        if action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH} and raw is not None:
            queue_candidate_id = str(raw["candidate_instance_id"])
            queue_specs[queue_candidate_id] = {
                "candidate_instance_id": queue_candidate_id,
                "trade_date": str(raw["trade_date"]),
                "code": str(raw["code"]),
                "reason": str(raw["reason"]),
                "source_event_id": raw["source_event_id"],
                "priority": int(raw["priority"]),
                "attempts": 0,
                "last_error": None,
            }

    allowed_new_requests = set(disposition_specs) - existing_requests
    expected_new_queue_ids = {
        str(disposition_specs[item_request_id]["candidate_instance_id"])
        for item_request_id in allowed_new_requests
        if action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}
        and disposition_specs[item_request_id]["candidate_instance_id"] is not None
    }
    runtime_lock_owner_id = None
    runtime_lock_detail_run_type = None
    if recovery_action:
        session = str(recovery_session_id)
        runtime_lock_owner_id = (
            f"fast0r2.verify.{session}" if action == ACTION_VERIFY_CANARY else f"fast0r2.{session}"
        )
        runtime_lock_detail_run_type = (
            "incremental_recovery_canary_verification"
            if action == ACTION_VERIFY_CANARY
            else "incremental_dead_letter_recovery"
        )
    return {
        "disposition_specs": disposition_specs,
        "allowed_new_requests": allowed_new_requests,
        "queue_specs": queue_specs,
        "allowed_new_queue_ids": expected_new_queue_ids - set(queue_before),
        "recovery_action": recovery_action,
        "runtime_lock_owner_id": runtime_lock_owner_id,
        "runtime_lock_detail_run_type": runtime_lock_detail_run_type,
        "expected_fencing_token": expected_fencing_token,
        "runtime_locks_before": dict(runtime_locks_before),
        "runtime_fences_before": dict(runtime_fences_before),
    }


def _install_apply_row_guards(
    connection: sqlite3.Connection,
    *,
    contract: Mapping[str, Any],
    violations: list[str],
) -> None:
    disposition_specs: dict[str, dict[str, Any]] = {
        str(key): dict(value) for key, value in dict(contract["disposition_specs"]).items()
    }
    allowed_new_requests = set(contract["allowed_new_requests"])
    queue_specs: dict[str, dict[str, Any]] = {
        str(key): dict(value) for key, value in dict(contract["queue_specs"]).items()
    }
    allowed_new_queue_ids = set(contract["allowed_new_queue_ids"])
    seen_requests: set[str] = set()
    seen_queue_ids: set[str] = set()

    def disposition_guard(*values: object) -> int:
        (
            disposition_id,
            request_id,
            request_hash,
            dead_letter_id,
            sequence_no,
            action,
            supersedes_disposition_id,
            reason_code,
            operator_id,
            expected_fingerprint,
            expected_candidate_version,
            evidence_ref,
            evidence_sha256,
            evidence_json,
            recovery_session_id,
            batch_size,
            fencing_token,
            safety_snapshot_json,
            observe_only,
            live_sim_allowed,
            live_real_allowed,
            auto_run_evaluation,
        ) = values
        normalized_request = str(request_id)
        spec = disposition_specs.get(normalized_request)
        if (
            spec is None
            or normalized_request not in allowed_new_requests
            or normalized_request in seen_requests
        ):
            violations.append("DISPOSITION_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        try:
            valid = (
                str(disposition_id).strip() != ""
                and str(request_hash) == str(spec["request_hash"])
                and str(dead_letter_id) == str(spec["dead_letter_id"])
                and _required_int(sequence_no) == _required_int(spec["sequence_no"])
                and str(action) == str(spec["action"])
                and _nullable_text(supersedes_disposition_id)
                == _nullable_text(spec["supersedes_disposition_id"])
                and str(reason_code) == str(spec["reason_code"])
                and str(operator_id) == str(spec["operator_id"])
                and str(expected_fingerprint) == str(spec["expected_dead_letter_fingerprint"])
                and str(expected_candidate_version) == str(spec["expected_candidate_version"])
                and str(evidence_ref) == str(spec["evidence_ref"])
                and str(evidence_sha256) == str(spec["evidence_sha256"])
                and _nullable_text(recovery_session_id)
                == _nullable_text(spec["recovery_session_id"])
                and _nullable_int(batch_size) == _nullable_int(spec["batch_size"])
                and _required_int(observe_only) == 1
                and _required_int(live_sim_allowed) == 0
                and _required_int(live_real_allowed) == 0
                and _required_int(auto_run_evaluation) == 0
            )
        except (TypeError, ValueError):
            valid = False
        if valid:
            fencing_required = bool(spec["fencing_token_required"])
            valid = (
                _nullable_int(fencing_token) == _nullable_int(spec["expected_fencing_token"])
                if fencing_required
                else fencing_token is None
            )
        if valid:
            try:
                evidence = json.loads(str(evidence_json or "{}"))
                safety = json.loads(str(safety_snapshot_json or "{}"))
            except (TypeError, ValueError):
                valid = False
            else:
                valid = (
                    isinstance(evidence, dict)
                    and isinstance(safety, dict)
                    and evidence.get("request_payload")
                    == {
                        key: spec[key]
                        for key in (
                            "action",
                            "dead_letter_id",
                            "request_id",
                            "expected_dead_letter_fingerprint",
                            "expected_candidate_version",
                            "reason_code",
                            "operator_id",
                            "evidence_type",
                            "evidence_ref",
                            "evidence_sha256",
                            "supersedes_disposition_id",
                            "recovery_session_id",
                            "batch_size",
                            "authorization_disposition_id",
                        )
                    }
                    and evidence.get("evidence_type") == spec["evidence_type"]
                    and safety.get("auto_run_evaluation") is False
                )
        if not valid:
            violations.append("DISPOSITION_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_requests.add(normalized_request)
        return 1

    def queue_guard(*values: object) -> int:
        (
            candidate_instance_id,
            trade_date,
            code,
            reason,
            source_event_id,
            priority,
            enqueued_at,
            updated_at,
            attempts,
            last_error,
        ) = values
        normalized_candidate_id = str(candidate_instance_id)
        spec = queue_specs.get(normalized_candidate_id)
        if (
            spec is None
            or normalized_candidate_id not in allowed_new_queue_ids
            or normalized_candidate_id in seen_queue_ids
        ):
            violations.append("QUEUE_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        try:
            valid = (
                str(trade_date) == str(spec["trade_date"])
                and str(code) == str(spec["code"])
                and str(reason) == str(spec["reason"])
                and _nullable_text(source_event_id) == _nullable_text(spec["source_event_id"])
                and _required_int(priority) == _required_int(spec["priority"])
                and bool(str(enqueued_at).strip())
                and str(enqueued_at) == str(updated_at)
                and _required_int(attempts) == 0
                and last_error is None
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            violations.append("QUEUE_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_queue_ids.add(normalized_candidate_id)
        return 1

    connection.create_function("fast0r2_guard_disposition_insert", 22, disposition_guard)
    connection.create_function("fast0r2_guard_queue_insert", 10, queue_guard)
    connection.execute("DROP TRIGGER IF EXISTS temp.fast0r2_guard_disposition_insert")
    connection.execute("DROP TRIGGER IF EXISTS temp.fast0r2_guard_queue_insert")
    connection.execute(
        f"""
        CREATE TEMP TRIGGER fast0r2_guard_disposition_insert
        BEFORE INSERT ON main.{_DISPOSITION_TABLE}
        BEGIN
            SELECT CASE WHEN fast0r2_guard_disposition_insert(
                NEW.disposition_id, NEW.request_id, NEW.request_hash,
                NEW.dead_letter_id, NEW.sequence_no, NEW.action,
                NEW.supersedes_disposition_id, NEW.reason_code, NEW.operator_id,
                NEW.expected_dead_letter_fingerprint,
                NEW.expected_candidate_version, NEW.evidence_ref,
                NEW.evidence_sha256, NEW.evidence_json,
                NEW.recovery_session_id, NEW.batch_size, NEW.fencing_token,
                NEW.safety_snapshot_json, NEW.observe_only,
                NEW.live_sim_allowed, NEW.live_real_allowed,
                NEW.auto_run_evaluation
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_DISPOSITION_INSERT_GUARD') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_queue_insert
        BEFORE INSERT ON main.incremental_evaluation_queue
        BEGIN
            SELECT CASE WHEN fast0r2_guard_queue_insert(
                NEW.candidate_instance_id, NEW.trade_date, NEW.code,
                NEW.reason, NEW.source_event_id, NEW.priority,
                NEW.enqueued_at, NEW.updated_at, NEW.attempts, NEW.last_error
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_QUEUE_INSERT_GUARD') END;
        END
        """
    )
    if not bool(contract["recovery_action"]):
        return

    expected_owner_id = str(contract["runtime_lock_owner_id"])
    expected_run_type = str(contract["runtime_lock_detail_run_type"])
    expected_fencing_token = _required_int(contract["expected_fencing_token"])
    fence_preexisting = "evaluation_pipeline" in contract["runtime_fences_before"]
    seen_lock_insert = False
    seen_lock_delete = False
    seen_fence_insert = False
    seen_fence_update = False

    def runtime_lock_insert_guard(*values: object) -> int:
        nonlocal seen_lock_insert
        lock_name, owner_id, fencing_token, detail_json = values
        try:
            detail = json.loads(str(detail_json or "{}"))
            valid = (
                not seen_lock_insert
                and str(lock_name) == "evaluation_pipeline"
                and str(owner_id) == expected_owner_id
                and _required_int(fencing_token) == expected_fencing_token
                and isinstance(detail, dict)
                and detail.get("run_type") == expected_run_type
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            violations.append("RUNTIME_LOCK_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_lock_insert = True
        return 1

    def runtime_lock_update_guard(*_values: object) -> int:
        violations.append("RUNTIME_LOCK_UPDATE_OUTSIDE_APPROVED_TARGET")
        return 0

    def runtime_lock_delete_guard(*values: object) -> int:
        nonlocal seen_lock_delete
        lock_name, owner_id, fencing_token = values
        try:
            valid = (
                seen_lock_insert
                and not seen_lock_delete
                and str(lock_name) == "evaluation_pipeline"
                and str(owner_id) == expected_owner_id
                and _required_int(fencing_token) == expected_fencing_token
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            violations.append("RUNTIME_LOCK_DELETE_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_lock_delete = True
        return 1

    def runtime_fence_insert_guard(*values: object) -> int:
        nonlocal seen_fence_insert
        lock_name, fencing_token = values
        try:
            valid = (
                not seen_fence_insert
                and str(lock_name) == "evaluation_pipeline"
                and _required_int(fencing_token) == 1
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            violations.append("RUNTIME_FENCE_INSERT_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_fence_insert = True
        return 1

    def runtime_fence_update_guard(*values: object) -> int:
        nonlocal seen_fence_update
        old_lock_name, old_token, new_lock_name, new_token = values
        try:
            valid = (
                fence_preexisting
                and seen_fence_insert
                and not seen_fence_update
                and str(old_lock_name) == "evaluation_pipeline"
                and str(new_lock_name) == "evaluation_pipeline"
                and _required_int(new_token) == _required_int(old_token) + 1
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            violations.append("RUNTIME_FENCE_UPDATE_OUTSIDE_APPROVED_TARGET")
            return 0
        seen_fence_update = True
        return 1

    connection.create_function("fast0r2_guard_runtime_lock_insert", 4, runtime_lock_insert_guard)
    connection.create_function("fast0r2_guard_runtime_lock_update", 1, runtime_lock_update_guard)
    connection.create_function("fast0r2_guard_runtime_lock_delete", 3, runtime_lock_delete_guard)
    connection.create_function("fast0r2_guard_runtime_fence_insert", 2, runtime_fence_insert_guard)
    connection.create_function("fast0r2_guard_runtime_fence_update", 4, runtime_fence_update_guard)
    for trigger_name in (
        "fast0r2_guard_runtime_lock_insert",
        "fast0r2_guard_runtime_lock_update",
        "fast0r2_guard_runtime_lock_delete",
        "fast0r2_guard_runtime_fence_insert",
        "fast0r2_guard_runtime_fence_update",
    ):
        connection.execute(f'DROP TRIGGER IF EXISTS temp."{trigger_name}"')
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_runtime_lock_insert
        BEFORE INSERT ON main.runtime_execution_locks
        BEGIN
            SELECT CASE WHEN fast0r2_guard_runtime_lock_insert(
                NEW.lock_name, NEW.owner_id, NEW.fencing_token, NEW.detail_json
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_RUNTIME_LOCK_INSERT_GUARD') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_runtime_lock_update
        BEFORE UPDATE ON main.runtime_execution_locks
        BEGIN
            SELECT CASE WHEN fast0r2_guard_runtime_lock_update(NEW.lock_name) = 1
                THEN 1
                ELSE RAISE(ABORT, 'FAST0R2_RUNTIME_LOCK_UPDATE_GUARD') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_runtime_lock_delete
        BEFORE DELETE ON main.runtime_execution_locks
        BEGIN
            SELECT CASE WHEN fast0r2_guard_runtime_lock_delete(
                OLD.lock_name, OLD.owner_id, OLD.fencing_token
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_RUNTIME_LOCK_DELETE_GUARD') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_runtime_fence_insert
        BEFORE INSERT ON main.runtime_execution_lock_fences
        BEGIN
            SELECT CASE WHEN fast0r2_guard_runtime_fence_insert(
                NEW.lock_name, NEW.last_fencing_token
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_RUNTIME_FENCE_INSERT_GUARD') END;
        END
        """
    )
    connection.execute(
        """
        CREATE TEMP TRIGGER fast0r2_guard_runtime_fence_update
        BEFORE UPDATE ON main.runtime_execution_lock_fences
        BEGIN
            SELECT CASE WHEN fast0r2_guard_runtime_fence_update(
                OLD.lock_name, OLD.last_fencing_token,
                NEW.lock_name, NEW.last_fencing_token
            ) = 1 THEN 1
            ELSE RAISE(ABORT, 'FAST0R2_RUNTIME_FENCE_UPDATE_GUARD') END;
        END
        """
    )


def _verify_apply_target_write_invariant(
    *,
    contract: Mapping[str, Any],
    disposition_before: Mapping[str, Mapping[str, Any]],
    disposition_after: Mapping[str, Mapping[str, Any]],
    queue_before: Mapping[str, Mapping[str, Any]],
    queue_after: Mapping[str, Mapping[str, Any]],
    runtime_locks_before: Mapping[str, Mapping[str, Any]],
    runtime_locks_after: Mapping[str, Mapping[str, Any]],
    runtime_fences_before: Mapping[str, Mapping[str, Any]],
    runtime_fences_after: Mapping[str, Mapping[str, Any]],
) -> tuple[bool, list[str]]:
    reason_codes: list[str] = []
    if set(disposition_before) - set(disposition_after):
        reason_codes.append("DISPOSITION_ROW_REMOVED")
    if any(
        _json_sha256(disposition_before[row_id]) != _json_sha256(disposition_after[row_id])
        for row_id in set(disposition_before) & set(disposition_after)
    ):
        reason_codes.append("DISPOSITION_ROW_CHANGED")
    new_disposition_rows = [
        disposition_after[row_id] for row_id in set(disposition_after) - set(disposition_before)
    ]
    actual_new_requests = [str(row["request_id"]) for row in new_disposition_rows]
    if len(actual_new_requests) != len(set(actual_new_requests)) or set(actual_new_requests) != set(
        contract["allowed_new_requests"]
    ):
        reason_codes.append("DISPOSITION_INSERT_SET_MISMATCH")

    if set(queue_before) - set(queue_after):
        reason_codes.append("QUEUE_ROW_REMOVED")
    if any(
        _json_sha256(queue_before[row_id]) != _json_sha256(queue_after[row_id])
        for row_id in set(queue_before) & set(queue_after)
    ):
        reason_codes.append("QUEUE_ROW_CHANGED")
    actual_new_queue_ids = set(queue_after) - set(queue_before)
    if actual_new_queue_ids != set(contract["allowed_new_queue_ids"]):
        reason_codes.append("QUEUE_INSERT_SET_MISMATCH")

    if _json_sha256(runtime_locks_before) != _json_sha256(runtime_locks_after):
        reason_codes.append("RUNTIME_LOCK_FINAL_STATE_CHANGED")
    if bool(contract["recovery_action"]):
        expected_fence_keys = set(runtime_fences_before) | {"evaluation_pipeline"}
        if set(runtime_fences_after) != expected_fence_keys:
            reason_codes.append("RUNTIME_FENCE_KEY_SET_MISMATCH")
        if any(
            _json_sha256(runtime_fences_before[lock_name])
            != _json_sha256(runtime_fences_after[lock_name])
            for lock_name in set(runtime_fences_before) & set(runtime_fences_after)
            if lock_name != "evaluation_pipeline"
        ):
            reason_codes.append("UNRELATED_RUNTIME_FENCE_CHANGED")
        before_fence = runtime_fences_before.get("evaluation_pipeline")
        after_fence = runtime_fences_after.get("evaluation_pipeline")
        try:
            expected_token = (
                1 if before_fence is None else _required_int(before_fence["last_fencing_token"]) + 1
            )
            actual_token = (
                None if after_fence is None else _required_int(after_fence["last_fencing_token"])
            )
        except (KeyError, TypeError, ValueError):
            actual_token = None
            expected_token = -1
        if actual_token != expected_token:
            reason_codes.append("RUNTIME_FENCE_TOKEN_MISMATCH")
    elif _json_sha256(runtime_fences_before) != _json_sha256(runtime_fences_after):
        reason_codes.append("RUNTIME_FENCE_FINAL_STATE_CHANGED")
    return not reason_codes, reason_codes


def _json_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _nullable_text(value: object) -> str | None:
    return None if value is None else str(value)


def _nullable_int(value: object) -> int | None:
    return None if value is None else _required_int(value)


def _required_int(value: object) -> int:
    return int(str(value))


def _build_apply_write_authorizer(
    *,
    action: str,
    blocked_write_tables: list[str],
) -> Callable[[int, str | None, str | None, str | None, str | None], int]:
    allowed_operations: dict[str, frozenset[int]] = {
        _DISPOSITION_TABLE: frozenset({sqlite3.SQLITE_INSERT}),
    }
    if action in {ACTION_RESET_CANARY, ACTION_VERIFY_CANARY, ACTION_RESET_BATCH}:
        allowed_operations.update(
            {
                "runtime_execution_locks": frozenset(
                    {
                        sqlite3.SQLITE_INSERT,
                        sqlite3.SQLITE_UPDATE,
                        sqlite3.SQLITE_DELETE,
                    }
                ),
                "runtime_execution_lock_fences": frozenset(
                    {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE}
                ),
            }
        )
    if action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
        allowed_operations["incremental_evaluation_queue"] = frozenset({sqlite3.SQLITE_INSERT})

    def authorize(
        action_code: int,
        first_argument: str | None,
        second_argument: str | None,
        database_name: str | None,
        _trigger_name: str | None,
    ) -> int:
        if action_code in {sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH}:
            blocked_write_tables.append(
                "ATTACH" if action_code == sqlite3.SQLITE_ATTACH else "DETACH"
            )
            return sqlite3.SQLITE_DENY
        if action_code == sqlite3.SQLITE_PRAGMA:
            pragma_name = str(first_argument or "UNKNOWN").lower()
            if pragma_name == "table_info":
                return sqlite3.SQLITE_OK
            if second_argument is None and pragma_name in {
                "database_list",
                "foreign_keys",
                "journal_mode",
                "query_only",
            }:
                return sqlite3.SQLITE_OK
            blocked_write_tables.append(f"PRAGMA:{pragma_name}")
            return sqlite3.SQLITE_DENY
        if action_code not in _SQLITE_WRITE_ACTIONS:
            return sqlite3.SQLITE_OK
        table = str(first_argument or "UNKNOWN")
        if str(database_name or "") != "main":
            blocked_write_tables.append(f"{database_name or 'UNKNOWN'}.{table}")
            return sqlite3.SQLITE_DENY
        if action_code in allowed_operations.get(table, frozenset()):
            return sqlite3.SQLITE_OK
        blocked_write_tables.append(table)
        return sqlite3.SQLITE_DENY

    return authorize


def _dead_letter_rows_fingerprint(
    connection: sqlite3.Connection,
    dead_letter_ids: Sequence[str],
) -> str:
    placeholders = ",".join("?" for _ in dead_letter_ids)
    rows = connection.execute(
        f"""
        SELECT *
        FROM incremental_evaluation_dead_letters
        WHERE dead_letter_id IN ({placeholders})
        ORDER BY dead_letter_id
        """,
        tuple(dead_letter_ids),
    ).fetchall()
    payload = [{key: row[key] for key in row.keys()} for row in rows]
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _validate_args(args: argparse.Namespace) -> None:
    reason_codes: list[str] = []
    ids = [str(value).strip() for value in args.dead_letter_id or [] if str(value).strip()]
    if not ids:
        reason_codes.append("DEAD_LETTER_ID_REQUIRED")
    if len(ids) != len(set(ids)):
        reason_codes.append("DUPLICATE_DEAD_LETTER_ID")
    if args.action in {
        ACTION_DISPOSE,
        ACTION_REVOKE,
        ACTION_RESET_CANARY,
        ACTION_VERIFY_CANARY,
    }:
        if len(ids) != 1:
            reason_codes.append("ACTION_REQUIRES_SINGLE_DEAD_LETTER")
    if args.action == ACTION_RESET_BATCH:
        if len(ids) < 2:
            reason_codes.append("RESET_BATCH_REQUIRES_AT_LEAST_TWO_DEAD_LETTERS")
        if len(ids) > 5:
            reason_codes.append("RECOVERY_BATCH_LIMIT_EXCEEDED")
        if not args.verify_canary_disposition_id:
            reason_codes.append("VERIFIED_CANARY_DISPOSITION_REQUIRED")
    elif args.verify_canary_disposition_id:
        reason_codes.append("CANARY_DISPOSITION_ONLY_ALLOWED_FOR_BATCH")
    if args.action == ACTION_REVOKE:
        if not args.revoke_disposition_id:
            reason_codes.append("REVOKE_DISPOSITION_ID_REQUIRED")
    elif args.revoke_disposition_id:
        reason_codes.append("REVOKE_DISPOSITION_ONLY_ALLOWED_FOR_REVOKE")
    if args.action in {ACTION_RESET_CANARY, ACTION_VERIFY_CANARY, ACTION_RESET_BATCH}:
        if not args.recovery_session_id:
            reason_codes.append("RECOVERY_SESSION_ID_REQUIRED")
    elif args.recovery_session_id:
        reason_codes.append("RECOVERY_SESSION_ONLY_ALLOWED_FOR_RECOVERY")

    fingerprints = list(args.expected_fingerprint or [])
    candidate_versions = list(args.expected_candidate_version or [])
    if (args.apply or fingerprints) and len(fingerprints) != len(ids):
        reason_codes.append("EXPECTED_FINGERPRINT_COUNT_MISMATCH")
    if (args.apply or candidate_versions) and len(candidate_versions) != len(ids):
        reason_codes.append("EXPECTED_CANDIDATE_VERSION_COUNT_MISMATCH")
    if any(not _SHA256_RE.fullmatch(str(value)) for value in fingerprints):
        reason_codes.append("EXPECTED_FINGERPRINT_INVALID")
    if any(not _SHA256_RE.fullmatch(str(value)) for value in candidate_versions):
        reason_codes.append("EXPECTED_CANDIDATE_VERSION_INVALID")

    if not args.apply:
        if reason_codes:
            raise IncrementalDeadLetterCliError(*reason_codes)
        return

    for value, code in (
        (args.request_id, "REQUEST_ID_REQUIRED"),
        (args.evidence_file, "EVIDENCE_FILE_REQUIRED"),
        (args.evidence_ref, "EVIDENCE_REF_REQUIRED"),
        (args.operator_id, "OPERATOR_ID_REQUIRED"),
    ):
        if value is None or not str(value).strip():
            reason_codes.append(code)
    if not args.acknowledge_effective_status_change:
        reason_codes.append("EFFECTIVE_STATUS_CHANGE_ACK_REQUIRED")
    if not args.acknowledge_no_auto_evaluation:
        reason_codes.append("NO_AUTO_EVALUATION_ACK_REQUIRED")
    if args.action == ACTION_DISPOSE:
        if not args.confirm_terminal_evidence_reviewed:
            reason_codes.append("TERMINAL_EVIDENCE_REVIEW_CONFIRMATION_REQUIRED")
        if not args.acknowledge_raw_audit_preserved:
            reason_codes.append("RAW_AUDIT_PRESERVATION_ACK_REQUIRED")
    elif args.action == ACTION_REVOKE:
        if not args.acknowledge_manual_review_restored:
            reason_codes.append("MANUAL_REVIEW_RESTORATION_ACK_REQUIRED")
    elif args.action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
        if not args.confirm_root_cause_fixed:
            reason_codes.append("ROOT_CAUSE_FIX_CONFIRMATION_REQUIRED")
        if args.acknowledge_raw_audit_preserved:
            reason_codes.append("RAW_AUDIT_ACK_FORBIDDEN_FOR_RECOVERY")
    else:
        if not args.confirm_canary_verified:
            reason_codes.append("CANARY_VERIFICATION_CONFIRMATION_REQUIRED")
        if args.acknowledge_raw_audit_preserved:
            reason_codes.append("RAW_AUDIT_ACK_FORBIDDEN_FOR_RECOVERY")

    if reason_codes:
        raise IncrementalDeadLetterCliError(*reason_codes)
    _require_safe_label(str(args.request_id), "REQUEST_ID_INVALID")
    _require_safe_label(str(args.evidence_ref), "EVIDENCE_REF_INVALID")
    _require_safe_label(str(args.operator_id), "OPERATOR_ID_INVALID")
    if args.recovery_session_id:
        _require_safe_label(str(args.recovery_session_id), "RECOVERY_SESSION_ID_INVALID")
    if args.revoke_disposition_id:
        _require_safe_label(
            str(args.revoke_disposition_id),
            "REVOKE_DISPOSITION_ID_INVALID",
            reject_account_like_digits=False,
        )
    if args.verify_canary_disposition_id:
        _require_safe_label(
            str(args.verify_canary_disposition_id),
            "VERIFY_CANARY_DISPOSITION_ID_INVALID",
            reject_account_like_digits=False,
        )


def _require_safe_label(
    value: str,
    code: str,
    *,
    reject_account_like_digits: bool = True,
) -> None:
    if not _SAFE_LABEL_RE.fullmatch(value):
        raise IncrementalDeadLetterCliError(code)
    if reject_account_like_digits and _contains_account_like_digit_sequence(value):
        raise IncrementalDeadLetterCliError(code)


def _contains_account_like_digit_sequence(value: str) -> bool:
    return bool(_LONG_DIGIT_RUN_RE.search(value) or _SEPARATED_DIGIT_RUN_RE.search(value))


def _same_path(first: Path, second: Path) -> bool:
    try:
        return first.resolve() == second.resolve()
    except OSError:
        return False


def _sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized_mapping: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            normalized = text_key.lower()
            if any(part in normalized for part in _SENSITIVE_KEY_PARTS):
                sanitized_mapping[text_key] = "[REDACTED]"
            else:
                sanitized_mapping[text_key] = _sanitize(item)
        return sanitized_mapping
    if isinstance(value, list | tuple):
        return [_sanitize(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                nested = json.loads(stripped)
            except (TypeError, ValueError):
                pass
            else:
                if isinstance(nested, Mapping | list):
                    return json.dumps(
                        _sanitize(nested),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
        sanitized_text = _AUTHORIZATION_VALUE_RE.sub("[REDACTED_AUTHORIZATION]", value)
        sanitized_text = _BEARER_TOKEN_RE.sub("[REDACTED_TOKEN]", sanitized_text)
        sanitized_text = _sanitize_embedded_json_fragments(sanitized_text)
        sanitized_text = _EMBEDDED_JSON_SENSITIVE_RE.sub(
            r'\1"[REDACTED]"',
            sanitized_text,
        )
        sanitized_text = _WINDOWS_PATH_RE.sub("[REDACTED_PATH]", sanitized_text)
        sanitized_text = _SECRET_ASSIGNMENT_RE.sub("[REDACTED_SECRET]", sanitized_text)
        sanitized_text = _LABELED_ACCOUNT_RE.sub("[REDACTED_ACCOUNT]", sanitized_text)
        return _UNLABELED_ACCOUNT_RE.sub("[REDACTED_ACCOUNT]", sanitized_text)
    return value


def _sanitize_embedded_json_fragments(value: str) -> str:
    decoder = json.JSONDecoder()
    chunks: list[str] = []
    cursor = 0
    while cursor < len(value):
        object_start = value.find("{", cursor)
        array_start = value.find("[", cursor)
        starts = [index for index in (object_start, array_start) if index >= 0]
        if not starts:
            chunks.append(value[cursor:])
            break
        start = min(starts)
        chunks.append(value[cursor:start])
        try:
            nested, end = decoder.raw_decode(value, start)
        except (TypeError, ValueError):
            chunks.append(value[start])
            cursor = start + 1
            continue
        if not isinstance(nested, Mapping | list):
            chunks.append(value[start:end])
        else:
            chunks.append(
                json.dumps(
                    _sanitize(nested),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        cursor = end
    return "".join(chunks)


def _value(value: object, key: str) -> object:
    return value.get(key) if isinstance(value, Mapping) else None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Strictly preview or append an audited FAST-0R2 incremental "
            "dead-letter disposition/recovery action without starting workers, "
            "running evaluation, or creating order commands."
        )
    )
    parser.add_argument("--dead-letter-id", action="append", required=True)
    parser.add_argument("--db", type=Path, help="SQLite DB; defaults to configured DB")
    parser.add_argument("--action", choices=ACTIONS, default=ACTION_DISPOSE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--request-id")
    parser.add_argument("--expected-fingerprint", action="append")
    parser.add_argument("--expected-candidate-version", action="append")
    parser.add_argument("--evidence-file", type=Path)
    parser.add_argument("--evidence-ref")
    parser.add_argument("--operator-id")
    parser.add_argument("--revoke-disposition-id")
    parser.add_argument("--recovery-session-id")
    parser.add_argument("--verify-canary-disposition-id")
    parser.add_argument("--acknowledge-effective-status-change", action="store_true")
    parser.add_argument("--acknowledge-no-auto-evaluation", action="store_true")
    parser.add_argument("--acknowledge-raw-audit-preserved", action="store_true")
    parser.add_argument("--confirm-terminal-evidence-reviewed", action="store_true")
    parser.add_argument("--acknowledge-manual-review-restored", action="store_true")
    parser.add_argument("--confirm-root-cause-fixed", action="store_true")
    parser.add_argument("--confirm-canary-verified", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        _validate_args(args)
        ids = [str(value).strip() for value in args.dead_letter_id]
        if args.apply:
            clear_settings_cache()
            settings = load_settings()
            db_path = (
                Path(args.db).expanduser()
                if args.db is not None
                else Path(settings.trading_db_path).expanduser()
            )
            _assert_apply_runtime_safe(settings, db_path)
            payload = apply_action(
                db_path,
                action=str(args.action),
                dead_letter_ids=ids,
                request_id=str(args.request_id),
                expected_fingerprints=[str(value) for value in args.expected_fingerprint],
                expected_candidate_versions=[
                    str(value) for value in args.expected_candidate_version
                ],
                evidence_file=Path(args.evidence_file),
                evidence_ref=str(args.evidence_ref),
                operator_id=str(args.operator_id),
                settings=settings,
                revoke_disposition_id=(
                    None if args.revoke_disposition_id is None else str(args.revoke_disposition_id)
                ),
                recovery_session_id=(
                    None if args.recovery_session_id is None else str(args.recovery_session_id)
                ),
                verify_canary_disposition_id=(
                    None
                    if args.verify_canary_disposition_id is None
                    else str(args.verify_canary_disposition_id)
                ),
            )
        else:
            if args.db is not None:
                db_path = Path(args.db).expanduser()
            else:
                clear_settings_cache()
                db_path = Path(load_settings().trading_db_path).expanduser()
            payload = preview_action(
                db_path,
                dead_letter_ids=ids,
                action=str(args.action),
                expected_fingerprints=(
                    None
                    if not args.expected_fingerprint
                    else [str(value) for value in args.expected_fingerprint]
                ),
                expected_candidate_versions=(
                    None
                    if not args.expected_candidate_version
                    else [str(value) for value in args.expected_candidate_version]
                ),
            )
    except IncrementalDeadLetterCliError as exc:
        failure_mode = (
            _apply_failure_mode(args)
            if "UNEXPECTED_DATABASE_WRITE_BLOCKED" in exc.reason_codes
            else "NO_WRITE"
        )
        payload = _rejected_payload(
            reason_codes=exc.reason_codes,
            mode=failure_mode,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except sqlite3.Error:
        payload = _rejected_payload(
            reason_codes=("SQLITE_OPERATION_FAILED",),
            mode=_apply_failure_mode(args),
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except (OSError, ValueError):
        payload = _rejected_payload(
            reason_codes=("CONFIGURATION_LOAD_FAILED",),
            mode=_apply_failure_mode(args),
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except RuntimeError as exc:
        to_dict = getattr(exc, "to_dict", None)
        details = to_dict() if callable(to_dict) else None
        payload = _rejected_payload(
            reason_codes=(str(getattr(exc, "code", "CORE_OPERATION_REJECTED")),),
            operation_error=details,
            mode=_apply_failure_mode(args),
        )
        print(json.dumps(_sanitize(payload), ensure_ascii=False, indent=2, sort_keys=True))
        return 2

    sanitized = _sanitize(payload)
    print(json.dumps(sanitized, ensure_ascii=False, indent=2, sort_keys=True))
    status = str(payload.get("status") or "")
    return 2 if status.startswith("APPLIED_WITH_") else 0


def _rejected_payload(
    *,
    reason_codes: Sequence[str],
    operation_error: object | None = None,
    mode: str = "NO_WRITE",
) -> dict[str, Any]:
    return {
        "status": "REJECTED",
        "mode": mode,
        "reason_codes": list(dict.fromkeys(str(code) for code in reason_codes)),
        "operation_error": operation_error,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_real_allowed": False,
    }


def _apply_failure_mode(args: argparse.Namespace) -> str:
    if not bool(args.apply):
        return "NO_WRITE"
    if str(args.action) in {
        ACTION_RESET_CANARY,
        ACTION_VERIFY_CANARY,
        ACTION_RESET_BATCH,
    }:
        return "GUARD_WRITE_POSSIBLE_TARGET_STATE_REQUIRES_READBACK"
    return "TARGET_WRITE_STATE_REQUIRES_READBACK"


if __name__ == "__main__":
    raise SystemExit(main())
