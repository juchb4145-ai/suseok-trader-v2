from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.runtime.targeted_pipeline_rebuild import (  # noqa: E402
    TargetedPipelineRebuildError,
    preview_targeted_pipeline_rebuild,
    run_targeted_pipeline_rebuild,
)
from storage.gateway_command_store import canonical_json  # noqa: E402

RUN_ACK = "RUN_TARGETED_PIPELINE_REBUILD_WITHOUT_ORDER_INTENT"


class TargetedPipelineRebuildCliError(RuntimeError):
    def __init__(self, *reason_codes: str) -> None:
        self.reason_codes = list(dict.fromkeys(str(code) for code in reason_codes if str(code)))
        super().__init__(", ".join(self.reason_codes) or "TARGETED_PIPELINE_REBUILD_CLI_ERROR")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview, or explicitly run, a max-five current-active pipeline rebuild "
            "that cannot create order plans or commands."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument(
        "--candidate-instance-id",
        action="append",
        dest="candidate_instance_ids",
        required=True,
    )
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--expected-preview-sha256")
    parser.add_argument("--acknowledge")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "targeted_pipeline_rebuild"),
    )
    args = parser.parse_args()
    try:
        if args.run:
            report = run_rebuild(
                db_path=Path(args.db),
                trade_date=args.trade_date,
                candidate_instance_ids=args.candidate_instance_ids,
                expected_preview_sha256=args.expected_preview_sha256,
                acknowledge=args.acknowledge,
                out_dir=Path(args.out_dir),
            )
        else:
            report = preview_rebuild(
                db_path=Path(args.db),
                trade_date=args.trade_date,
                candidate_instance_ids=args.candidate_instance_ids,
                out_dir=Path(args.out_dir),
            )
    except (
        OSError,
        sqlite3.Error,
        ValueError,
        TargetedPipelineRebuildError,
        TargetedPipelineRebuildCliError,
    ) as exc:
        reason = (
            ",".join(exc.reason_codes)
            if isinstance(exc, (TargetedPipelineRebuildError, TargetedPipelineRebuildCliError))
            else type(exc).__name__
        )
        print(f"targeted pipeline rebuild: ERROR {reason}", file=sys.stderr)
        return 2
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"ELIGIBLE", "COMPLETED"} else 2


def preview_rebuild(
    *,
    db_path: Path,
    trade_date: str,
    candidate_instance_ids: Sequence[str],
    out_dir: Path,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    path = _validated_database_path(db_path)
    before = _file_state(path)
    connection = _open_strict_read_only(path)
    try:
        _require_schema_62(connection)
        preview = preview_targeted_pipeline_rebuild(
            connection,
            candidate_instance_ids,
            trade_date=trade_date,
            environ=environ,
            not_order_intent=True,
        )
    finally:
        connection.close()
    after = _file_state(path)
    if before != after:
        raise TargetedPipelineRebuildCliError("DATABASE_DATA_FILE_CHANGED_DURING_PREVIEW")
    preview_sha256 = _preview_sha256(preview)
    report: dict[str, Any] = {
        "contract": "fast0r3-targeted-pipeline-rebuild-preview.v1",
        "generated_at": _now(),
        "mode": "PREVIEW",
        "database": {
            "filename": path.name,
            "schema_version": "62",
            "files_before": before,
            "files_after": after,
        },
        "preview": preview,
        "preview_sha256": preview_sha256,
        "read_only": True,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "verdict": {"status": "ELIGIBLE", "failures": []},
    }
    report["report_paths"] = _write_report(report, out_dir=out_dir)
    return report


def run_rebuild(
    *,
    db_path: Path,
    trade_date: str,
    candidate_instance_ids: Sequence[str],
    expected_preview_sha256: str | None,
    acknowledge: str | None,
    out_dir: Path,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if acknowledge != RUN_ACK:
        raise TargetedPipelineRebuildCliError("EXACT_RUN_ACKNOWLEDGEMENT_REQUIRED")
    expected_sha = _require_sha256("expected_preview_sha256", expected_preview_sha256)
    path = _validated_database_path(db_path)
    files_before = _file_state(path)
    connection = _open_existing_read_write(path)
    result: dict[str, Any] | None = None
    result_status = ""
    operation_error: Exception | None = None
    try:
        _require_schema_62(connection)
        if _runtime_lease_count(connection):
            raise TargetedPipelineRebuildCliError("RUNTIME_EXECUTION_LEASE_PRESENT")
        preview = preview_targeted_pipeline_rebuild(
            connection,
            candidate_instance_ids,
            trade_date=trade_date,
            environ=environ,
            not_order_intent=True,
        )
        actual_sha = _preview_sha256(preview)
        if actual_sha != expected_sha:
            raise TargetedPipelineRebuildCliError("TARGETED_REBUILD_PREVIEW_CAS_MISMATCH")
        result = run_targeted_pipeline_rebuild(
            connection,
            candidate_instance_ids,
            trade_date=trade_date,
            environ=environ,
            not_order_intent=True,
        )
        result_status = _validate_run_result(result)
    except Exception as exc:
        operation_error = exc
    close_error: Exception | None = None
    try:
        connection.close()
    except Exception as exc:
        close_error = exc

    outcome_kind = _result_outcome_kind(result)
    if operation_error is not None:
        if outcome_kind in {"COMMITTED", "UNKNOWN"}:
            report = _post_result_failure_report(
                path=path,
                files_before=files_before,
                files_after=None,
                expected_sha=expected_sha,
                result=result,
                status=(
                    "COMMITTED_POSTCHECK_FAILED"
                    if outcome_kind == "COMMITTED"
                    else "OUTCOME_UNKNOWN"
                ),
                failure_code="RESULT_POSTCHECK_FAILED",
                error=operation_error,
            )
            return _write_run_report_fail_closed(report, out_dir=out_dir)
        raise operation_error.with_traceback(operation_error.__traceback__)
    if close_error is not None:
        if outcome_kind in {"COMMITTED", "UNKNOWN"}:
            report = _post_result_failure_report(
                path=path,
                files_before=files_before,
                files_after=None,
                expected_sha=expected_sha,
                result=result,
                status=(
                    "COMMITTED_POSTCHECK_FAILED"
                    if outcome_kind == "COMMITTED"
                    else "OUTCOME_UNKNOWN"
                ),
                failure_code="CONNECTION_CLOSE_FAILED",
                error=close_error,
            )
            return _write_run_report_fail_closed(report, out_dir=out_dir)
        raise close_error.with_traceback(close_error.__traceback__)

    assert result is not None
    try:
        files_after = _file_state(path)
    except Exception as exc:
        if outcome_kind in {"COMMITTED", "UNKNOWN"}:
            report = _post_result_failure_report(
                path=path,
                files_before=files_before,
                files_after=None,
                expected_sha=expected_sha,
                result=result,
                status=(
                    "COMMITTED_POSTCHECK_FAILED"
                    if outcome_kind == "COMMITTED"
                    else "OUTCOME_UNKNOWN"
                ),
                failure_code="DATABASE_FILE_STATE_POSTCHECK_FAILED",
                error=exc,
            )
            return _write_run_report_fail_closed(report, out_dir=out_dir)
        raise

    try:
        report = _build_run_report(
            path=path,
            files_before=files_before,
            files_after=files_after,
            expected_sha=expected_sha,
            result=result,
            result_status=result_status,
        )
    except Exception as exc:
        if outcome_kind in {"COMMITTED", "UNKNOWN"}:
            report = _post_result_failure_report(
                path=path,
                files_before=files_before,
                files_after=files_after,
                expected_sha=expected_sha,
                result=result,
                status=(
                    "COMMITTED_REPORT_FAILED"
                    if outcome_kind == "COMMITTED"
                    else "OUTCOME_UNKNOWN"
                ),
                failure_code="REPORT_CONSTRUCTION_FAILED",
                error=exc,
            )
            return _write_run_report_fail_closed(report, out_dir=out_dir)
        raise
    return _write_run_report_fail_closed(report, out_dir=out_dir)


def _validate_run_result(result: Mapping[str, Any]) -> str:
    result_status = str(result.get("status") or "")
    no_order_contract_valid = (
        result.get("order_artifacts_unchanged") is True
        and result.get("command_type_counts_unchanged") is True
        and result.get("no_order_plans_created") is True
        and result.get("no_order_commands_created") is True
    )
    if not no_order_contract_valid:
        raise TargetedPipelineRebuildCliError(
            "TARGETED_REBUILD_RESULT_CONTRACT_INVALID"
        )
    if result_status == "COMPLETED":
        if result.get("data_committed") is not True:
            raise TargetedPipelineRebuildCliError(
                "TARGETED_REBUILD_RESULT_CONTRACT_INVALID"
            )
        return result_status
    if result_status.startswith("COMMITTED_"):
        if (
            result.get("data_committed") is not True
            or result.get("operator_action_required") is not True
        ):
            raise TargetedPipelineRebuildCliError(
                "TARGETED_REBUILD_RESULT_CONTRACT_INVALID"
            )
        if result_status == "COMMITTED_CLEANUP_FAILED":
            lock_cleanup = result.get("lock_cleanup")
            if (
                not isinstance(lock_cleanup, Mapping)
                or lock_cleanup.get("status") != "FAILED"
                or lock_cleanup.get("reason_code") != "COMMITTED_CLEANUP_FAILED"
                or lock_cleanup.get("operator_action_required") is not True
            ):
                raise TargetedPipelineRebuildCliError(
                    "TARGETED_REBUILD_CLEANUP_FAILURE_EVIDENCE_INVALID"
                )
        transaction_outcome = result.get("transaction_outcome")
        if result_status == "COMMITTED_TRANSACTION_SIGNAL_FAILED" and (
            not isinstance(transaction_outcome, Mapping)
            or transaction_outcome.get("outcome") != "COMMITTED_RECONCILED"
            or transaction_outcome.get("reconciled_after_error") is not True
        ):
            raise TargetedPipelineRebuildCliError(
                "TARGETED_REBUILD_COMMIT_RECONCILIATION_EVIDENCE_INVALID"
            )
        return result_status
    if result_status == "OUTCOME_UNKNOWN":
        transaction_outcome = result.get("transaction_outcome")
        if (
            result.get("data_committed") is not None
            or result.get("operator_action_required") is not True
            or not isinstance(transaction_outcome, Mapping)
            or transaction_outcome.get("outcome") != "OUTCOME_UNKNOWN"
        ):
            raise TargetedPipelineRebuildCliError(
                "TARGETED_REBUILD_OUTCOME_UNKNOWN_EVIDENCE_INVALID"
            )
        return result_status
    raise TargetedPipelineRebuildCliError("TARGETED_REBUILD_RESULT_CONTRACT_INVALID")


def _result_outcome_kind(result: Mapping[str, Any] | None) -> str:
    if not isinstance(result, Mapping):
        return "UNAPPLIED"
    if result.get("data_committed") is True:
        return "COMMITTED"
    if str(result.get("status") or "") == "OUTCOME_UNKNOWN":
        return "UNKNOWN"
    return "UNAPPLIED"


def _result_failure_codes(result: Mapping[str, Any]) -> list[str]:
    raw = result.get("failure_codes")
    failures = (
        [str(item) for item in raw if str(item).strip()]
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes))
        else []
    )
    status = str(result.get("status") or "")
    if status != "COMPLETED" and status not in failures:
        failures.append(status)
    return list(dict.fromkeys(failures))


def _build_run_report(
    *,
    path: Path,
    files_before: Mapping[str, Any],
    files_after: Mapping[str, Any],
    expected_sha: str,
    result: Mapping[str, Any],
    result_status: str,
) -> dict[str, Any]:
    outcome_kind = _result_outcome_kind(result)
    return {
        "contract": "fast0r3-targeted-pipeline-rebuild-run.v1",
        "generated_at": _now(),
        "mode": "RUN",
        "database": {
            "filename": path.name,
            "schema_version": "62",
            "files_before": dict(files_before),
            "files_after": dict(files_after),
        },
        "authorized_preview_sha256": expected_sha,
        "result": dict(result),
        "read_only": False,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": result_status,
            "failures": _result_failure_codes(result),
            "committed": True if outcome_kind == "COMMITTED" else None,
            "operator_action_required": (
                result_status != "COMPLETED"
                or result.get("operator_action_required") is True
            ),
        },
    }


def _post_result_failure_report(
    *,
    path: Path,
    files_before: Mapping[str, Any],
    files_after: Mapping[str, Any] | None,
    expected_sha: str,
    result: Mapping[str, Any] | None,
    status: str,
    failure_code: str,
    error: Exception,
) -> dict[str, Any]:
    result_payload = dict(result or {})
    outcome_kind = _result_outcome_kind(result)
    failures = _result_failure_codes(result_payload)
    failures.extend([failure_code])
    return {
        "contract": "fast0r3-targeted-pipeline-rebuild-run.v1",
        "generated_at": None,
        "mode": "RUN",
        "database": {
            "filename": path.name,
            "schema_version": "62",
            "files_before": dict(files_before),
            "files_after": (
                dict(files_after)
                if files_after is not None
                else {"available": False}
            ),
        },
        "authorized_preview_sha256": expected_sha,
        "result": result_payload,
        "read_only": False,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": status,
            "failures": list(dict.fromkeys(failures)),
            "committed": True if outcome_kind == "COMMITTED" else None,
            "operator_action_required": True,
            "error_type": type(error).__name__,
        },
    }


def _write_run_report_fail_closed(
    report: dict[str, Any],
    *,
    out_dir: Path,
) -> dict[str, Any]:
    try:
        report["report_paths"] = _write_report(report, out_dir=out_dir)
    except Exception as exc:
        verdict = dict(report.get("verdict") or {})
        failures = [str(item) for item in verdict.get("failures") or []]
        failures.append("EVIDENCE_WRITE_FAILED")
        committed = verdict.get("committed") is True
        report["verdict"] = {
            "status": (
                "COMMITTED_EVIDENCE_WRITE_FAILED"
                if committed
                else "OUTCOME_UNKNOWN"
            ),
            "failures": list(dict.fromkeys(failures)),
            "committed": True if committed else None,
            "operator_action_required": True,
            "error_type": type(exc).__name__,
        }
        report["report_paths"] = {}
    return report


def _preview_sha256(preview: Mapping[str, Any]) -> str:
    candidates = preview.get("candidates")
    runtime_safety = preview.get("runtime_safety")
    artifact_snapshot = preview.get("artifact_snapshot")
    if (
        not isinstance(candidates, Sequence)
        or isinstance(candidates, (str, bytes))
        or not isinstance(runtime_safety, Mapping)
        or not isinstance(artifact_snapshot, Mapping)
    ):
        raise TargetedPipelineRebuildCliError("TARGETED_REBUILD_PREVIEW_INVALID")
    stable_candidates = []
    for raw in candidates:
        if not isinstance(raw, Mapping):
            raise TargetedPipelineRebuildCliError("TARGETED_REBUILD_PREVIEW_INVALID")
        stable_candidates.append(
            {
                key: raw.get(key)
                for key in (
                    "candidate_instance_id",
                    "trade_date",
                    "state",
                    "active_source_count",
                    "active_source_fingerprint",
                    "source_watermark_hash",
                )
            }
        )
    payload = {
        "contract": "fast0r3-targeted-pipeline-rebuild-preview-cas.v1",
        "trade_date": preview.get("trade_date"),
        "candidate_instance_ids": list(preview.get("candidate_instance_ids") or []),
        "candidates": stable_candidates,
        "runtime_safety": {
            key: runtime_safety.get(key)
            for key in (
                "trading_env_file_sha256",
                "settings_sha256",
                "strategy_config_version",
                "risk_config_version",
                "entry_timing_config_version",
                "trading_profile",
                "trading_mode",
                "live_sim_allowed",
                "live_real_allowed",
                "kill_switch_active",
                "incremental_worker_enabled",
                "theme_refresh_queue_market_scan_commands",
                "enabled_command_producers",
                "database_path_matches",
            )
        },
        "artifact_snapshot": dict(artifact_snapshot),
        "not_order_intent": preview.get("not_order_intent"),
    }
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def _validated_database_path(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
        stat_result = resolved.stat()
    except OSError as exc:
        raise TargetedPipelineRebuildCliError("DATABASE_NOT_FOUND") from exc
    if not resolved.is_file():
        raise TargetedPipelineRebuildCliError("DATABASE_NOT_FOUND")
    if int(stat_result.st_nlink) != 1:
        raise TargetedPipelineRebuildCliError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved


def _assert_quiescent_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            raise TargetedPipelineRebuildCliError("QUIESCENT_DATABASE_REQUIRED")


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    _assert_quiescent_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _open_existing_read_write(path: Path) -> sqlite3.Connection:
    _assert_quiescent_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=rw",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _require_schema_62(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT value FROM app_metadata WHERE key='schema_version'").fetchone()
    if row is None or str(row[0]) != "62":
        raise TargetedPipelineRebuildCliError("SCHEMA_62_REQUIRED")


def _runtime_lease_count(connection: sqlite3.Connection) -> int:
    return int(connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0])


def _require_sha256(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise TargetedPipelineRebuildCliError(f"INVALID_SHA256:{name.upper()}")
    return normalized


def _file_state(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(f"{path}{suffix}")
        key = "main" if not suffix else suffix.removeprefix("-")
        if not candidate.exists():
            result[key] = {"exists": False, "size": 0, "mtime_ns": None}
            continue
        stat_result = candidate.stat()
        result[key] = {
            "exists": True,
            "size": int(stat_result.st_size),
            "mtime_ns": int(stat_result.st_mtime_ns),
        }
    return result


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, str]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    commands_path = report_dir / "commands.txt"
    raw_path.write_text(
        json.dumps(_redact(report), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    commands_path.write_text(
        "preview: python -B -m tools.run_targeted_pipeline_rebuild <redacted arguments>\n"
        "run: requires exact preview SHA, audited env, acknowledgement, and separate approval\n",
        encoding="utf-8",
    )
    return {
        "raw_json": str(raw_path),
        "summary_md": str(summary_path),
        "commands_txt": str(commands_path),
    }


def _redact(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if any(
        part in lowered
        for part in ("account", "token", "password", "secret", "env_file", "database_path")
    ):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(child): _redact(item, key=str(child)) for child, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    return value


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = dict(report.get("verdict") or {})
    preview_sha256 = report.get("preview_sha256") or report.get("authorized_preview_sha256")
    return "\n".join(
        [
            "# FAST-0R3 Targeted Pipeline Rebuild",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- mode: `{report.get('mode')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- preview_sha256: `{preview_sha256}`",
            "",
            "The run contract forbids order plans, intents, commands, and broker calls.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = dict(report.get("verdict") or {})
    return f"targeted pipeline rebuild: {verdict.get('status')} mode={report.get('mode')}"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
