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
from storage.gateway_order_broker_boundary import (  # noqa: E402
    OrderBrokerBoundaryResolutionError,
    preview_order_broker_boundary_resolution,
    record_order_broker_boundary_resolution,
    revoke_order_broker_boundary_resolution,
)

EVIDENCE_TYPE = "SIMULATION_HTS_ORDER_HISTORY_EXPORT"
REASON_CODE = "OPERATOR_CONFIRMED_BROKER_NOT_REACHED"
REVOKE_EVIDENCE_TYPE = "SIMULATION_HTS_ORDER_HISTORY_CORRECTION"
REVOKE_REASON_CODE = "OPERATOR_REVOKED_BROKER_NOT_REACHED"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,127}$")
_LONG_DIGIT_RUN_RE = re.compile(r"[0-9]{8,}")
_SEPARATED_DIGIT_RUN_RE = re.compile(
    r"(?:[0-9][_.:@-]*){8,}"
)
_PRODUCER_SETTING_NAMES = (
    "realtime_subscription_queue_commands",
    "dry_run_order_routing_enabled",
    "dry_run_gateway_command_enabled",
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
    "theme_refresh_queue_market_scan_commands": (
        "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS"
    ),
}
_SENSITIVE_KEY_PARTS = (
    "account",
    "idempotency",
    "payload",
    "snapshot",
    "evidence_content",
    "file_path",
    "db_path",
    "env_path",
    "token",
    "secret",
)


class BoundaryResolutionCliError(RuntimeError):
    def __init__(self, *reason_codes: str) -> None:
        normalized = tuple(dict.fromkeys(str(code) for code in reason_codes if str(code)))
        super().__init__(", ".join(normalized) or "ORDER_BOUNDARY_RESOLUTION_REJECTED")
        self.reason_codes = normalized or ("ORDER_BOUNDARY_RESOLUTION_REJECTED",)


def preview_resolution(db_path: Path, command_id: str) -> dict[str, Any]:
    connection = _open_strict_read_only(db_path)
    try:
        preview = preview_order_broker_boundary_resolution(connection, command_id)
    finally:
        connection.close()
    return {
        "status": "PREVIEW",
        "mode": "READ_ONLY",
        "preview": _sanitize(preview),
        "read_only": True,
        "query_only": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_real_allowed": False,
    }


def apply_resolution(
    db_path: Path,
    *,
    command_id: str,
    request_id: str,
    expected_fingerprint: str,
    evidence_file: Path,
    evidence_ref: str,
    operator_id: str,
    revoke_resolution_id: str | None,
    settings: Settings,
) -> dict[str, Any]:
    evidence_sha256 = _sha256_file(evidence_file)
    is_revoke = revoke_resolution_id is not None
    evidence_type = REVOKE_EVIDENCE_TYPE if is_revoke else EVIDENCE_TYPE
    reason_code = REVOKE_REASON_CODE if is_revoke else REASON_CODE
    verification_error: str | None = None
    connection = _open_existing_read_write(db_path)
    try:
        before_counts = _command_counts(connection)
        if is_revoke:
            result = revoke_order_broker_boundary_resolution(
                connection,
                command_id=command_id,
                request_id=request_id,
                expected_fingerprint=expected_fingerprint,
                supersedes_resolution_id=str(revoke_resolution_id),
                evidence_type=evidence_type,
                evidence_ref=evidence_ref,
                evidence_sha256=evidence_sha256,
                operator_id=operator_id,
                reason_code=reason_code,
            )
        else:
            result = record_order_broker_boundary_resolution(
                connection,
                command_id=command_id,
                request_id=request_id,
                expected_fingerprint=expected_fingerprint,
                evidence_type=evidence_type,
                evidence_ref=evidence_ref,
                evidence_sha256=evidence_sha256,
                operator_id=operator_id,
                reason_code=reason_code,
            )
        try:
            after_counts = _command_counts(connection)
            preview_after = preview_order_broker_boundary_resolution(
                connection, command_id
            )
        except (sqlite3.Error, OrderBrokerBoundaryResolutionError):
            after_counts = None
            preview_after = {"status": "POST_APPLY_VERIFICATION_UNAVAILABLE"}
            verification_error = "POST_APPLY_READBACK_FAILED"
    finally:
        connection.close()

    command_count_delta = (
        None
        if after_counts is None
        else after_counts["total"] - before_counts["total"]
    )
    order_command_count_delta = (
        None
        if after_counts is None
        else after_counts["order"] - before_counts["order"]
    )
    command_state_unchanged = bool(
        after_counts is not None
        and before_counts["state_fingerprint"]
        == after_counts["state_fingerprint"]
    )
    command_count_invariant_ok = bool(
        after_counts is not None
        and command_count_delta == 0
        and order_command_count_delta == 0
        and command_state_unchanged
    )
    replayed = result.get("idempotent_replay") is True
    replay_effective = result.get("idempotent_replay_effective") is True
    if verification_error is not None:
        result_status = "APPLIED_WITH_VERIFICATION_FAILURE"
    elif replayed and not replay_effective:
        result_status = "REPLAYED_NOT_EFFECTIVE"
    elif not command_count_invariant_ok:
        result_status = "APPLIED_WITH_CONCURRENT_COMMAND_CHANGE"
    elif replayed:
        result_status = "REPLAYED_EFFECTIVE"
    else:
        result_status = "REVOKED" if is_revoke else "APPLIED"
    return {
        "status": result_status,
        "mode": (
            "APPEND_ONLY_REVOCATION" if is_revoke else "APPEND_ONLY_RESOLUTION"
        ),
        "result": _sanitize(result),
        "preview_after": _sanitize(preview_after),
        "evidence_type": evidence_type,
        "evidence_ref": evidence_ref,
        "evidence_sha256": evidence_sha256,
        "operator_id": operator_id,
        "reason_code": reason_code,
        "command_count_delta": command_count_delta,
        "order_command_count_delta": order_command_count_delta,
        "command_count_invariant_ok": command_count_invariant_ok,
        "command_state_unchanged": command_state_unchanged,
        "post_apply_verification_error": verification_error,
        "raw_boundary_changed": False,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "routing_safety_state_recomputed": True,
        "live_real_allowed": False,
        "runtime_safety": _runtime_safety_summary(settings),
    }


def _assert_apply_runtime_safe(settings: Settings, db_path: Path) -> None:
    reason_codes: list[str] = []
    configured_env = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
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
    for name in _PRODUCER_SETTING_NAMES:
        if _producer_enabled(settings, name):
            reason_codes.append(f"COMMAND_PRODUCER_ENABLED:{name.upper()}")

    configured_db = Path(settings.trading_db_path).expanduser()
    if not _same_path(configured_db, db_path):
        reason_codes.append("DB_PATH_DOES_NOT_MATCH_SAFE_ENV")
    if not db_path.is_file():
        reason_codes.append("DATABASE_NOT_FOUND")
    if reason_codes:
        raise BoundaryResolutionCliError(*reason_codes)


def _runtime_safety_summary(settings: Settings) -> dict[str, Any]:
    return {
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "live_sim_allowed": bool(settings.trading_allow_live_sim),
        "live_real_allowed": bool(settings.trading_allow_live_real),
        "kill_switch_active": bool(settings.live_sim_kill_switch),
        "enabled_command_producers": [
            name for name in _PRODUCER_SETTING_NAMES if _producer_enabled(settings, name)
        ],
        "explicit_env_file": True,
    }


def _producer_enabled(settings: Settings, name: str) -> bool:
    external_env_name = _EXTERNAL_PRODUCER_ENV_NAMES.get(name)
    if external_env_name is not None:
        return _explicit_env_flag_is_not_false(external_env_name)
    return bool(getattr(settings, name, True))


def _explicit_env_flag_is_not_false(env_name: str) -> bool:
    configured_env = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    if not configured_env:
        return True
    try:
        lines = Path(configured_env).expanduser().read_text(encoding="utf-8").splitlines()
    except OSError:
        return True
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
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "f", "no", "n", "off"}


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise BoundaryResolutionCliError("DATABASE_NOT_FOUND")
    connection = sqlite3.connect(
        _sqlite_uri(path, mode="ro"),
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _open_existing_read_write(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise BoundaryResolutionCliError("DATABASE_NOT_FOUND")
    connection = sqlite3.connect(
        _sqlite_uri(path, mode="rw"),
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _sqlite_uri(path: Path, *, mode: str) -> str:
    uri_path = quote(path.resolve().as_posix(), safe="/:")
    return f"file:{uri_path}?mode={mode}"


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        raise BoundaryResolutionCliError("EVIDENCE_FILE_NOT_FOUND")
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
        raise BoundaryResolutionCliError("EVIDENCE_FILE_UNREADABLE") from exc
    if byte_count == 0:
        raise BoundaryResolutionCliError("EVIDENCE_FILE_EMPTY")
    identities = {
        _file_identity(stat_result)
        for stat_result in (
            path_before,
            stream_before,
            stream_after,
            path_after,
        )
    }
    if len(identities) != 1 or byte_count != int(path_after.st_size):
        raise BoundaryResolutionCliError("EVIDENCE_FILE_CHANGED_DURING_HASH")
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
    state_payload = [
        {key: state_row[key] for key in state_row.keys()}
        for state_row in state_rows
    ]
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


def _validate_apply_args(args: argparse.Namespace) -> None:
    missing_codes: list[str] = []
    for value, code in (
        (args.request_id, "REQUEST_ID_REQUIRED"),
        (args.expected_fingerprint, "EXPECTED_FINGERPRINT_REQUIRED"),
        (args.evidence_file, "EVIDENCE_FILE_REQUIRED"),
        (args.evidence_ref, "EVIDENCE_REF_REQUIRED"),
        (args.operator_id, "OPERATOR_ID_REQUIRED"),
    ):
        if value is None or not str(value).strip():
            missing_codes.append(code)
    is_revoke = args.revoke_resolution_id is not None
    if is_revoke:
        if not args.acknowledge_correction_or_contradiction:
            missing_codes.append("CORRECTION_OR_CONTRADICTION_ACK_REQUIRED")
        if args.confirm_no_broker_order_or_execution:
            missing_codes.append("NO_BROKER_CONFIRMATION_FORBIDDEN_ON_REVOKE")
    else:
        if not args.confirm_no_broker_order_or_execution:
            missing_codes.append("NO_BROKER_EVIDENCE_CONFIRMATION_REQUIRED")
        if args.acknowledge_correction_or_contradiction:
            missing_codes.append("CORRECTION_ACK_FORBIDDEN_ON_RESOLVE")
    if not args.acknowledge_late_evidence_precedence:
        missing_codes.append("LATE_EVIDENCE_PRECEDENCE_ACK_REQUIRED")
    if not args.acknowledge_routing_gate_change:
        missing_codes.append("ROUTING_GATE_CHANGE_ACK_REQUIRED")
    if missing_codes:
        raise BoundaryResolutionCliError(*missing_codes)

    if not _SHA256_RE.fullmatch(str(args.expected_fingerprint)):
        raise BoundaryResolutionCliError("EXPECTED_FINGERPRINT_INVALID")
    _require_safe_label(str(args.request_id), "REQUEST_ID_INVALID")
    _require_safe_label(str(args.evidence_ref), "EVIDENCE_REF_INVALID")
    _require_safe_label(str(args.operator_id), "OPERATOR_ID_INVALID")
    if args.revoke_resolution_id is not None:
        _require_safe_label(
            str(args.revoke_resolution_id),
            "REVOKE_RESOLUTION_ID_INVALID",
            reject_account_like_digits=False,
        )


def _require_safe_label(
    value: str,
    code: str,
    *,
    reject_account_like_digits: bool = True,
) -> None:
    if not _SAFE_LABEL_RE.fullmatch(value):
        raise BoundaryResolutionCliError(code)
    if reject_account_like_digits and _contains_account_like_digit_sequence(value):
        raise BoundaryResolutionCliError(code)


def _contains_account_like_digit_sequence(value: str) -> bool:
    return bool(
        _LONG_DIGIT_RUN_RE.search(value)
        or _SEPARATED_DIGIT_RUN_RE.search(value)
    )


def _same_path(first: Path, second: Path) -> bool:
    try:
        return first.resolve() == second.resolve()
    except OSError:
        return False


def _sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            normalized = text_key.lower()
            if any(part in normalized for part in _SENSITIVE_KEY_PARTS):
                sanitized[text_key] = "[REDACTED]"
            else:
                sanitized[text_key] = _sanitize(item)
        return sanitized
    if isinstance(value, list | tuple):
        return [_sanitize(item) for item in value]
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or append an audited LIVE_SIM broker-boundary resolution "
            "without starting Core/Gateway or creating commands."
        )
    )
    parser.add_argument("--command-id", required=True)
    parser.add_argument("--db", type=Path, help="SQLite DB. Defaults to configured DB.")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--request-id")
    parser.add_argument("--expected-fingerprint")
    parser.add_argument("--evidence-file", type=Path)
    parser.add_argument("--evidence-ref")
    parser.add_argument("--operator-id")
    parser.add_argument(
        "--revoke-resolution-id",
        help="Append a correction that revokes the specified active resolution.",
    )
    parser.add_argument(
        "--confirm-no-broker-order-or-execution",
        action="store_true",
    )
    parser.add_argument(
        "--acknowledge-correction-or-contradiction",
        action="store_true",
    )
    parser.add_argument(
        "--acknowledge-late-evidence-precedence",
        action="store_true",
    )
    parser.add_argument(
        "--acknowledge-routing-gate-change",
        action="store_true",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.apply:
            clear_settings_cache()
            settings = load_settings()
            db_path = (
                Path(args.db).expanduser()
                if args.db is not None
                else Path(settings.trading_db_path).expanduser()
            )
            _validate_apply_args(args)
            _assert_apply_runtime_safe(settings, db_path)
            payload = apply_resolution(
                db_path,
                command_id=str(args.command_id),
                request_id=str(args.request_id),
                expected_fingerprint=str(args.expected_fingerprint),
                evidence_file=Path(args.evidence_file),
                evidence_ref=str(args.evidence_ref),
                operator_id=str(args.operator_id),
                revoke_resolution_id=(
                    None
                    if args.revoke_resolution_id is None
                    else str(args.revoke_resolution_id)
                ),
                settings=settings,
            )
        else:
            if args.revoke_resolution_id is not None:
                raise BoundaryResolutionCliError("APPLY_REQUIRED_FOR_REVOKE")
            if args.db is not None:
                db_path = Path(args.db).expanduser()
            else:
                clear_settings_cache()
                db_path = Path(load_settings().trading_db_path).expanduser()
            payload = preview_resolution(db_path, str(args.command_id))
    except OrderBrokerBoundaryResolutionError as exc:
        payload = {
            "status": "REJECTED",
            "mode": "NO_WRITE",
            "resolution_error": _sanitize(exc.to_dict()),
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except BoundaryResolutionCliError as exc:
        payload = {
            "status": "REJECTED",
            "mode": "NO_WRITE",
            "reason_codes": list(exc.reason_codes),
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except (OSError, ValueError):
        payload = {
            "status": "REJECTED",
            "mode": "NO_WRITE",
            "reason_codes": ["CONFIGURATION_LOAD_FAILED"],
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except sqlite3.Error:
        payload = {
            "status": "REJECTED",
            "mode": "NO_WRITE",
            "reason_codes": ["SQLITE_OPERATION_FAILED"],
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 2

    print(json.dumps(_sanitize(payload), ensure_ascii=False, indent=2, sort_keys=True))
    status = str(payload.get("status") or "")
    return 2 if status.startswith("APPLIED_WITH_") or status == "REPLAYED_NOT_EFFECTIVE" else 0


if __name__ == "__main__":
    raise SystemExit(main())
