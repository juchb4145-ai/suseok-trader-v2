from __future__ import annotations

import argparse
import hashlib
import json
import re
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

from services.live_sim.execution_lifecycle_status import (  # noqa: E402
    build_live_sim_execution_lifecycle_status,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402

_EXPECTED_SCHEMA_VERSION = "63"
_PAGE_LIMIT = 500
_DATA_FILE_SUFFIXES = ("", "-wal", "-shm", "-journal")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_CLASSIFICATIONS = frozenset(
    {
        "ACTIVE_LIFECYCLE_BLOCKER",
        "HISTORICAL_RUNTIME_STATUS_AUDIT",
        "MANUAL_REVIEW_BLOCKER",
    }
)
_PUBLIC_ITEM_KEYS = frozenset(
    {
        "subject_id",
        "subject_fingerprint",
        "classification",
        "reason_codes",
        "mirror_status",
        "error_surface_count",
        "lifecycle_surface_count",
        "created_at",
        "code",
        "inner_event_type",
        "payload_sha256",
        "event_metadata_consistent",
        "identifier_free",
    }
)
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:token|password|secret|api[_-]?key|account)\s*[:=]\s*\S+"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)
_HYPHENATED_ACCOUNT_PATTERN = re.compile(
    r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)"
)
_ISO_DATE_PATTERN = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")


class LiveSimExecutionLifecycleCheckError(RuntimeError):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Qualify LIVE_SIM execution lifecycle state from a strict read-only "
            "schema-62 SQLite snapshot."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--code")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "live_sim_execution_lifecycle"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            code=args.code,
            run_quick_check=True,
            out_dir=Path(args.out_dir),
        )
    except Exception as exc:
        print(
            "live-sim execution lifecycle: ERROR "
            f"error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    return 0 if _mapping(report.get("verdict")).get("status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    code: str | None,
    run_quick_check: bool,
    out_dir: Path,
) -> dict[str, Any]:
    if run_quick_check is not True:
        raise LiveSimExecutionLifecycleCheckError("DATABASE_QUICK_CHECK_IS_MANDATORY")
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise LiveSimExecutionLifecycleCheckError("CODE_TARGET_SCHEMA_MISMATCH")

    resolved_path = _validated_database_path(db_path)
    _assert_no_sidecars(resolved_path)
    files_before = _file_fingerprints(resolved_path)
    observed_at = datetime.now(UTC)
    connection = _open_strict_read_only(resolved_path)
    try:
        query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
        connection.execute("BEGIN DEFERRED")
        try:
            identity = _read_database_identity(connection)
            lifecycle = _read_all_pages(connection, code=code)
        finally:
            connection.rollback()
    finally:
        connection.close()

    _validated_database_path(resolved_path)
    _assert_no_sidecars(resolved_path)
    files_after = _file_fingerprints(resolved_path)
    report: dict[str, Any] = {
        "contract": "fast0-live-sim-execution-lifecycle-qualification.v1",
        "generated_at": _wire(observed_at),
        "database": {
            "path": str(resolved_path),
            "identity": identity,
            "files_before": files_before,
            "files_after": files_after,
            "quick_check": quick_check,
            "connection": {
                "mode": "ro",
                "immutable": True,
                "query_only": query_only == 1,
            },
        },
        "execution_lifecycle": lifecycle,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "raw_rows_recorded": False,
        "core_started": False,
        "gateway_started": False,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    return report


def _read_database_identity(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT key, value
        FROM app_metadata
        WHERE key IN ('app_name', 'schema_version')
        ORDER BY key
        """
    ).fetchall()
    values: dict[str, list[str]] = {"app_name": [], "schema_version": []}
    for row in rows:
        key = str(row["key"])
        if key in values:
            values[key].append(str(row["value"]))
    return {
        "app_name": values["app_name"][0] if len(values["app_name"]) == 1 else None,
        "app_name_row_count": len(values["app_name"]),
        "schema_version": (
            values["schema_version"][0]
            if len(values["schema_version"]) == 1
            else None
        ),
        "schema_version_row_count": len(values["schema_version"]),
        "expected_app_name": APP_NAME,
        "expected_schema_version": _EXPECTED_SCHEMA_VERSION,
    }


def _read_all_pages(
    connection: sqlite3.Connection,
    *,
    code: str | None,
) -> dict[str, Any]:
    unfiltered_baseline: dict[str, Any] | None = None
    if code is not None:
        baseline = build_live_sim_execution_lifecycle_status(
            connection,
            limit=1,
            offset=0,
            code=None,
        )
        if not isinstance(baseline, Mapping):
            raise LiveSimExecutionLifecycleCheckError(
                "LIFECYCLE_GLOBAL_BASELINE_INVALID"
            )
        unfiltered_baseline = dict(baseline)
    first: dict[str, Any] | None = None
    pages: list[dict[str, Any]] = []
    subject_ids: list[str] = []
    collected_classification_counts = {
        classification: 0 for classification in _CLASSIFICATIONS
    }
    invalid_item_count = 0
    offset = 0

    while True:
        page = build_live_sim_execution_lifecycle_status(
            connection,
            limit=_PAGE_LIMIT,
            offset=offset,
            code=code,
        )
        if not isinstance(page, Mapping):
            raise LiveSimExecutionLifecycleCheckError("LIFECYCLE_PAGE_INVALID")
        normalized_page = dict(page)
        if first is None:
            first = normalized_page

        page_items = normalized_page.get("items")
        if not isinstance(page_items, Sequence) or isinstance(page_items, (str, bytes)):
            raise LiveSimExecutionLifecycleCheckError("LIFECYCLE_ITEMS_INVALID")
        for item in page_items:
            if not isinstance(item, Mapping) or not _valid_public_item(item):
                invalid_item_count += 1
                continue
            subject_id = item.get("subject_id")
            classification = item.get("classification")
            assert isinstance(subject_id, str)
            assert isinstance(classification, str)
            subject_ids.append(subject_id)
            collected_classification_counts[classification] += 1

        pages.append(
            {
                "offset": normalized_page.get("offset"),
                "returned_count": normalized_page.get("returned_count"),
                "full_count": normalized_page.get("full_count"),
                "has_more": normalized_page.get("has_more"),
                "next_offset": normalized_page.get("next_offset"),
                "inventory_count_consistent": normalized_page.get(
                    "inventory_count_consistent"
                ),
                "inventory_digest": normalized_page.get("inventory_digest"),
                "scanned_inventory_digest": normalized_page.get(
                    "scanned_inventory_digest"
                ),
                "ending_inventory_digest": normalized_page.get(
                    "ending_inventory_digest"
                ),
                "qualification_status": normalized_page.get("qualification_status"),
                "effective_blocker_count": normalized_page.get(
                    "effective_blocker_count"
                ),
            }
        )
        if not bool(normalized_page.get("has_more")):
            break
        next_offset = normalized_page.get("next_offset")
        if not isinstance(next_offset, int) or isinstance(next_offset, bool):
            raise LiveSimExecutionLifecycleCheckError("LIFECYCLE_PAGINATION_STALLED")
        if next_offset <= offset:
            raise LiveSimExecutionLifecycleCheckError("LIFECYCLE_PAGINATION_STALLED")
        offset = next_offset

    if first is None:
        raise LiveSimExecutionLifecycleCheckError("LIFECYCLE_PAGE_MISSING")
    filter_global_contract_consistent = (
        unfiltered_baseline is None
        or _global_contract_projection(unfiltered_baseline)
        == _global_contract_projection(first)
    )
    first_classification_counts = first.get("classification_counts")
    item_classification_counts_consistent = bool(
        sum(collected_classification_counts.values()) == len(subject_ids)
        and (
            code is not None
            or (
                isinstance(first_classification_counts, Mapping)
                and frozenset(first_classification_counts) == _CLASSIFICATIONS
                and all(
                    first_classification_counts.get(classification) == count
                    for classification, count in collected_classification_counts.items()
                )
            )
        )
    )
    result = {
        key: first.get(key)
        for key in (
            "status",
            "qualification_status",
            "qualification_reason_codes",
            "canonical_status",
            "canonical_reason_codes",
            "canonical",
            "classification_counts",
            "raw_error_count",
            "mirror_lifecycle_count",
            "logical_subject_count",
            "active_lifecycle_blocker_count",
            "historical_runtime_status_audit_count",
            "manual_review_blocker_count",
            "active_reconcile_blocker_count",
            "historical_reconcile_event_count",
            "reconcile_manual_review_count",
            "effective_blocker_count",
            "mirrored_pair_count",
            "mirror_consistent",
            "reconcile",
            "code_filter",
            "code_filter_diagnostic_only",
            "full_count",
            "inventory_count_consistent",
            "inventory_digest",
            "scanned_inventory_digest",
            "ending_inventory_digest",
            "read_only",
            "observe_only",
            "no_order_side_effects",
            "real_order_allowed",
        )
    }
    result.update(
        {
            "collected_count": len(subject_ids),
            "unique_subject_count": len(set(subject_ids)),
            "invalid_item_count": invalid_item_count,
            "item_classification_counts_consistent": (
                item_classification_counts_consistent
            ),
            "page_count": len(pages),
            "pages": pages,
            "raw_rows_recorded": False,
            "filter_global_contract_consistent": filter_global_contract_consistent,
            "requested_code_filter": code,
        }
    )
    return result


def _global_contract_projection(status: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: status.get(key)
        for key in (
            "status",
            "qualification_status",
            "qualification_reason_codes",
            "canonical_status",
            "canonical_reason_codes",
            "canonical",
            "classification_counts",
            "raw_error_count",
            "mirror_lifecycle_count",
            "logical_subject_count",
            "active_lifecycle_blocker_count",
            "historical_runtime_status_audit_count",
            "manual_review_blocker_count",
            "active_reconcile_blocker_count",
            "historical_reconcile_event_count",
            "reconcile_manual_review_count",
            "effective_blocker_count",
            "mirrored_pair_count",
            "mirror_consistent",
            "reconcile",
            "inventory_count_consistent",
            "inventory_digest",
            "scanned_inventory_digest",
            "ending_inventory_digest",
            "read_only",
            "observe_only",
            "no_order_side_effects",
            "real_order_allowed",
        )
    }


def _valid_public_item(item: Mapping[str, Any]) -> bool:
    if frozenset(item) != _PUBLIC_ITEM_KEYS:
        return False
    subject_id = item.get("subject_id")
    if (
        not _is_digest(subject_id)
        or not _is_digest(item.get("subject_fingerprint"))
    ):
        return False
    if item.get("classification") not in _CLASSIFICATIONS:
        return False
    reason_codes = item.get("reason_codes")
    if not isinstance(reason_codes, list) or not all(
        isinstance(reason, str) for reason in reason_codes
    ):
        return False
    for key in ("error_surface_count", "lifecycle_surface_count"):
        value = item.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return False
    for key in ("mirror_status", "created_at", "code", "inner_event_type"):
        if item.get(key) is not None and not isinstance(item.get(key), str):
            return False
    if not _is_digest(item.get("payload_sha256")):
        return False
    event_metadata_consistent = item.get("event_metadata_consistent")
    identifier_free = item.get("identifier_free")
    if not isinstance(event_metadata_consistent, bool) or not isinstance(
        identifier_free, bool
    ):
        return False
    is_historical_runtime_mirror = bool(
        item.get("classification") == "HISTORICAL_RUNTIME_STATUS_AUDIT"
        and item.get("mirror_status") == "EXACT_1_TO_1"
    )
    return not is_historical_runtime_mirror or (
        event_metadata_consistent is True and identifier_free is True
    )


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    database = _mapping(report.get("database"))
    identity = _mapping(database.get("identity"))
    connection_contract = _mapping(database.get("connection"))
    lifecycle = _mapping(report.get("execution_lifecycle"))
    failures: list[str] = []

    files_before = database.get("files_before")
    files_after = database.get("files_after")
    if not isinstance(files_before, Mapping) or files_before != files_after:
        failures.append("DATABASE_DATA_FILE_CHANGED")
    if database.get("quick_check") != ["ok"]:
        failures.append("DATABASE_QUICK_CHECK_FAILED")
    if (
        identity.get("app_name") != APP_NAME
        or identity.get("app_name_row_count") != 1
    ):
        failures.append("DATABASE_APP_IDENTITY_MISMATCH")
    if (
        identity.get("schema_version") != _EXPECTED_SCHEMA_VERSION
        or identity.get("schema_version_row_count") != 1
    ):
        failures.append("DATABASE_SCHEMA_VERSION_MISMATCH")
    if (
        connection_contract.get("mode") != "ro"
        or connection_contract.get("immutable") is not True
        or connection_contract.get("query_only") is not True
    ):
        failures.append("STRICT_READ_ONLY_CONNECTION_INVALID")

    full_count = _integer(lifecycle.get("full_count"), default=-1)
    collected_count = _integer(lifecycle.get("collected_count"), default=-2)
    unique_count = _integer(lifecycle.get("unique_subject_count"), default=-3)
    invalid_item_count = _integer(lifecycle.get("invalid_item_count"), default=-4)
    if full_count < 0 or full_count != collected_count:
        failures.append("LIFECYCLE_FULL_COUNT_MISMATCH")
    if lifecycle.get("code_filter") != lifecycle.get("requested_code_filter"):
        failures.append("LIFECYCLE_CODE_FILTER_MISMATCH")
    if collected_count != unique_count:
        failures.append("LIFECYCLE_DUPLICATE_SUBJECT")
    if invalid_item_count != 0:
        failures.append("LIFECYCLE_ITEM_CONTRACT_INVALID")
    if lifecycle.get("item_classification_counts_consistent") is not True:
        failures.append("LIFECYCLE_ITEM_CLASSIFICATION_TOTAL_MISMATCH")
    if lifecycle.get("filter_global_contract_consistent") is not True:
        failures.append("LIFECYCLE_CODE_FILTER_CHANGED_GLOBAL_CONTRACT")
    if lifecycle.get("inventory_count_consistent") is not True:
        failures.append("LIFECYCLE_INVENTORY_COUNT_INCONSISTENT")

    status = lifecycle.get("status")
    qualification_status_value = lifecycle.get("qualification_status")
    qualification_reason_codes = lifecycle.get("qualification_reason_codes")
    canonical_status = lifecycle.get("canonical_status")
    canonical_reason_codes = lifecycle.get("canonical_reason_codes")
    classification_counts = lifecycle.get("classification_counts")
    if (
        not isinstance(status, str)
        or status not in {"PASS", "BLOCKED"}
        or not isinstance(qualification_status_value, str)
        or qualification_status_value not in {"PASS", "BLOCKED"}
        or status != qualification_status_value
        or not isinstance(qualification_reason_codes, list)
        or not all(isinstance(reason, str) for reason in qualification_reason_codes)
        or not isinstance(canonical_status, str)
        or canonical_status not in {"PASS", "BLOCKED"}
        or not isinstance(canonical_reason_codes, list)
        or not all(isinstance(reason, str) for reason in canonical_reason_codes)
    ):
        failures.append("LIFECYCLE_STATUS_CONTRACT_INVALID")
    if (
        not isinstance(classification_counts, Mapping)
        or frozenset(classification_counts) != _CLASSIFICATIONS
        or not all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in classification_counts.values()
        )
    ):
        failures.append("LIFECYCLE_CLASSIFICATION_COUNTS_INVALID")

    count_values = {
        key: _integer(lifecycle.get(key), default=-1)
        for key in (
            "raw_error_count",
            "mirror_lifecycle_count",
            "logical_subject_count",
            "active_lifecycle_blocker_count",
            "historical_runtime_status_audit_count",
            "manual_review_blocker_count",
            "active_reconcile_blocker_count",
            "historical_reconcile_event_count",
            "reconcile_manual_review_count",
            "effective_blocker_count",
        )
    }
    if any(value < 0 for value in count_values.values()):
        failures.append("LIFECYCLE_GLOBAL_COUNTS_INVALID")
    if (
        lifecycle.get("requested_code_filter") is None
        and full_count != count_values["logical_subject_count"]
    ):
        failures.append("LIFECYCLE_GLOBAL_FULL_COUNT_MISMATCH")
    classification_counts_valid = bool(
        isinstance(classification_counts, Mapping)
        and frozenset(classification_counts) == _CLASSIFICATIONS
        and all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in classification_counts.values()
        )
    )
    if classification_counts_valid:
        assert isinstance(classification_counts, Mapping)
        classification_aliases = {
            "ACTIVE_LIFECYCLE_BLOCKER": "active_lifecycle_blocker_count",
            "HISTORICAL_RUNTIME_STATUS_AUDIT": (
                "historical_runtime_status_audit_count"
            ),
            "MANUAL_REVIEW_BLOCKER": "manual_review_blocker_count",
        }
        if any(
            classification_counts.get(classification) != count_values[alias]
            for classification, alias in classification_aliases.items()
        ):
            failures.append("LIFECYCLE_CLASSIFICATION_ALIAS_MISMATCH")
        if sum(int(value) for value in classification_counts.values()) != count_values[
            "logical_subject_count"
        ]:
            failures.append("LIFECYCLE_CLASSIFICATION_TOTAL_MISMATCH")
    if count_values["effective_blocker_count"] != (
        count_values["active_lifecycle_blocker_count"]
        + count_values["manual_review_blocker_count"]
    ):
        failures.append("LIFECYCLE_EFFECTIVE_BLOCKER_COUNT_MISMATCH")
    if any(
        count_values[reconcile_key] > count_values[classification_key]
        for reconcile_key, classification_key in (
            (
                "active_reconcile_blocker_count",
                "active_lifecycle_blocker_count",
            ),
            (
                "historical_reconcile_event_count",
                "historical_runtime_status_audit_count",
            ),
            (
                "reconcile_manual_review_count",
                "manual_review_blocker_count",
            ),
        )
    ):
        failures.append("LIFECYCLE_RECONCILE_CLASSIFICATION_SUBSET_MISMATCH")
    if not isinstance(lifecycle.get("mirror_consistent"), bool):
        failures.append("LIFECYCLE_MIRROR_CONSISTENCY_INVALID")

    inventory_digest = lifecycle.get("inventory_digest")
    scanned_digest = lifecycle.get("scanned_inventory_digest")
    ending_digest = lifecycle.get("ending_inventory_digest")
    if not all(_is_digest(value) for value in (inventory_digest, scanned_digest, ending_digest)):
        failures.append("LIFECYCLE_INVENTORY_DIGEST_INVALID")
    elif not inventory_digest == scanned_digest == ending_digest:
        failures.append("LIFECYCLE_INVENTORY_CHANGED_DURING_SNAPSHOT")

    qualification_status = str(qualification_status_value or "UNKNOWN").upper()
    if qualification_status != "PASS":
        failures.append("LIFECYCLE_QUALIFICATION_NOT_PASS")
    if qualification_status == "PASS" and lifecycle.get("mirror_consistent") is not True:
        failures.append("LIFECYCLE_MIRROR_INCONSISTENT")
    for key, failure in (
        ("active_lifecycle_blocker_count", "ACTIVE_LIFECYCLE_BLOCKER_PRESENT"),
        ("manual_review_blocker_count", "LIFECYCLE_MANUAL_REVIEW_BLOCKER_PRESENT"),
        ("active_reconcile_blocker_count", "ACTIVE_RECONCILE_BLOCKER_PRESENT"),
        ("reconcile_manual_review_count", "RECONCILE_MANUAL_REVIEW_BLOCKER_PRESENT"),
        ("effective_blocker_count", "EFFECTIVE_LIFECYCLE_BLOCKER_PRESENT"),
    ):
        if _integer(lifecycle.get(key), default=-1) != 0:
            failures.append(failure)

    if (
        lifecycle.get("read_only") is not True
        or lifecycle.get("observe_only") is not True
        or lifecycle.get("no_order_side_effects") is not True
        or lifecycle.get("real_order_allowed") is not False
    ):
        failures.append("LIFECYCLE_READ_ONLY_CONTRACT_INVALID")

    pages = lifecycle.get("pages")
    if not isinstance(pages, Sequence) or isinstance(pages, (str, bytes)) or not pages:
        failures.append("LIFECYCLE_PAGINATION_EVIDENCE_MISSING")
    else:
        expected_offset = 0
        page_collected_count = 0
        for index, page_value in enumerate(pages):
            if not isinstance(page_value, Mapping):
                failures.append("LIFECYCLE_PAGE_EVIDENCE_INVALID")
                continue
            page = dict(page_value)
            page_offset = _integer(page.get("offset"), default=-1)
            returned_count = _integer(page.get("returned_count"), default=-1)
            page_full_count = _integer(page.get("full_count"), default=-1)
            has_more = page.get("has_more")
            if (
                page_offset != expected_offset
                or returned_count < 0
                or page_full_count != full_count
                or not isinstance(has_more, bool)
            ):
                failures.append("LIFECYCLE_PAGE_RANGE_INVALID")
            page_collected_count += max(returned_count, 0)
            expected_offset += max(returned_count, 0)
            if (
                page.get("inventory_count_consistent") is not True
                or page.get("inventory_digest") != inventory_digest
                or page.get("scanned_inventory_digest") != inventory_digest
                or page.get("ending_inventory_digest") != inventory_digest
                or str(page.get("qualification_status") or "").upper()
                != qualification_status
                or page.get("effective_blocker_count")
                != lifecycle.get("effective_blocker_count")
            ):
                failures.append("LIFECYCLE_PAGE_SNAPSHOT_MISMATCH")
            is_last = index == len(pages) - 1
            if not isinstance(has_more, bool) or has_more == is_last:
                failures.append("LIFECYCLE_PAGE_TERMINATION_INVALID")
        if page_collected_count != collected_count:
            failures.append("LIFECYCLE_PAGE_COUNT_MISMATCH")

    if (
        report.get("read_only") is not True
        or report.get("observe_only") is not True
        or report.get("no_order_side_effects") is not True
        or report.get("no_trading_side_effects") is not True
        or report.get("raw_rows_recorded") is not False
        or lifecycle.get("raw_rows_recorded") is not False
    ):
        failures.append("READ_ONLY_EVIDENCE_CONTRACT_INVALID")

    return {
        "status": "PASS" if not failures else "BLOCKED",
        "failures": sorted(set(failures)),
        "qualification_status": qualification_status,
        "qualification_reason_codes": list(
            lifecycle.get("qualification_reason_codes") or []
        ),
        "canonical_status": lifecycle.get("canonical_status"),
        "active_lifecycle_blocker_count": lifecycle.get(
            "active_lifecycle_blocker_count"
        ),
        "historical_runtime_status_audit_count": lifecycle.get(
            "historical_runtime_status_audit_count"
        ),
        "manual_review_blocker_count": lifecycle.get("manual_review_blocker_count"),
        "active_reconcile_blocker_count": lifecycle.get(
            "active_reconcile_blocker_count"
        ),
        "historical_reconcile_event_count": lifecycle.get(
            "historical_reconcile_event_count"
        ),
        "reconcile_manual_review_count": lifecycle.get(
            "reconcile_manual_review_count"
        ),
        "effective_blocker_count": lifecycle.get("effective_blocker_count"),
        "full_count": full_count,
        "collected_count": collected_count,
        "inventory_digest": inventory_digest,
        "database_files_unchanged": files_before == files_after,
        "raw_rows_recorded": False,
        "read_only": True,
        "no_order_side_effects": True,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    redacted = _redact(report)
    raw_path.write_text(
        json.dumps(redacted, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(redacted), encoding="utf-8")
    return {"raw_json": raw_path, "summary_md": summary_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    lifecycle = _mapping(report.get("execution_lifecycle"))
    return "\n".join(
        [
            "# FAST-0 LIVE_SIM Execution Lifecycle Qualification",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- verdict: `{verdict.get('status')}`",
            f"- canonical_status: `{verdict.get('canonical_status')}`",
            f"- qualification_status: `{verdict.get('qualification_status')}`",
            (
                "- lifecycle active/historical/manual: "
                f"`{verdict.get('active_lifecycle_blocker_count')}/"
                f"{verdict.get('historical_runtime_status_audit_count')}/"
                f"{verdict.get('manual_review_blocker_count')}`"
            ),
            (
                "- reconcile active/historical/manual: "
                f"`{verdict.get('active_reconcile_blocker_count')}/"
                f"{verdict.get('historical_reconcile_event_count')}/"
                f"{verdict.get('reconcile_manual_review_count')}`"
            ),
            f"- effective_blocker_count: `{verdict.get('effective_blocker_count')}`",
            f"- full/collected: `{verdict.get('full_count')}/{verdict.get('collected_count')}`",
            f"- page_count: `{lifecycle.get('page_count')}`",
            f"- inventory_digest: `{verdict.get('inventory_digest')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            "- raw_rows_recorded: `false`",
            "",
            "The database was opened immutable/query-only; Core and Gateway were not started.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "live-sim execution lifecycle: "
        f"{verdict.get('status')} qualification={verdict.get('qualification_status')} "
        f"active={verdict.get('active_lifecycle_blocker_count')} "
        f"historical={verdict.get('historical_runtime_status_audit_count')} "
        f"manual={verdict.get('manual_review_blocker_count')} "
        f"reconcile_active={verdict.get('active_reconcile_blocker_count')} "
        f"reconcile_manual={verdict.get('reconcile_manual_review_count')} "
        f"effective={verdict.get('effective_blocker_count')} "
        f"full={verdict.get('full_count')} collected={verdict.get('collected_count')}"
    )


def _validated_database_path(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
        stat_result = resolved.stat()
    except OSError as exc:
        raise LiveSimExecutionLifecycleCheckError("DATABASE_NOT_FOUND") from exc
    if not resolved.is_file():
        raise LiveSimExecutionLifecycleCheckError("DATABASE_NOT_FOUND")
    if int(stat_result.st_nlink) != 1:
        raise LiveSimExecutionLifecycleCheckError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    _assert_no_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _assert_no_sidecars(path: Path) -> None:
    if any(Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm", "-journal")):
        raise LiveSimExecutionLifecycleCheckError(
            "STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE"
        )


def _file_fingerprints(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for suffix in _DATA_FILE_SUFFIXES:
        candidate = Path(f"{path}{suffix}")
        key = "main" if not suffix else suffix.removeprefix("-")
        if not candidate.exists():
            result[key] = {"exists": False, "size": 0, "sha256": None}
            continue
        stat_before = candidate.stat()
        digest = hashlib.sha256()
        with candidate.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        stat_after = candidate.stat()
        if (
            stat_before.st_size != stat_after.st_size
            or stat_before.st_mtime_ns != stat_after.st_mtime_ns
        ):
            raise LiveSimExecutionLifecycleCheckError(
                "DATABASE_FILE_CHANGED_DURING_HASH"
            )
        result[key] = {
            "exists": True,
            "size": int(stat_after.st_size),
            "mtime_ns": int(stat_after.st_mtime_ns),
            "sha256": digest.hexdigest(),
        }
    return result


def _redact(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if any(part in lowered for part in ("account", "token", "password", "secret", "payload")):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(child): _redact(item, key=str(child)) for child, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, str) and _contains_sensitive_value(value):
        return "[REDACTED]"
    return value


def _contains_sensitive_value(value: str) -> bool:
    if any(pattern.search(value) is not None for pattern in _SENSITIVE_VALUE_PATTERNS):
        return True
    without_iso_dates = _ISO_DATE_PATTERN.sub("", value)
    return _HYPHENATED_ACCOUNT_PATTERN.search(without_iso_dates) is not None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _integer(value: Any, *, default: int) -> int:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else default


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and _DIGEST_PATTERN.fullmatch(value) is not None


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
