from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.incremental_dead_letter_campaign import (  # noqa: E402
    CAMPAIGN_TARGET_COUNT,
    IncrementalDeadLetterCampaignError,
    canonical_sha256,
    read_stable_json_document,
    require_sha256,
    require_utc_z_timestamp,
    validate_private_target_manifest,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402
from tools import ops_incremental_dead_letter_campaign_preflight as preflight_tool  # noqa: E402

HANDOFF_CONTRACT = "fast0-incremental-dead-letter-campaign-handoff.v1"
APPLY_CHAIN_CONTRACT = "fast0-incremental-dead-letter-campaign-apply-chain.v1"
_EXPECTED_SCHEMA_VERSION = "63"


class IncrementalDeadLetterCampaignHandoffError(RuntimeError):
    pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify all U01-U38 apply reports and seal the final strict "
            "read-only FAST-0R9 handoff for downstream R8 work."
        )
    )
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--blocker-plan-report", required=True, type=Path)
    parser.add_argument("--blocker-plan-report-sha256", required=True)
    parser.add_argument("--private-target-manifest", required=True, type=Path)
    parser.add_argument("--private-target-manifest-sha256", required=True)
    parser.add_argument("--apply-report", action="append", required=True, type=Path)
    parser.add_argument("--apply-report-sha256", action="append", required=True)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT_DIR / "reports" / "fast0_incremental_dead_letter_campaign_handoff",
    )
    args = parser.parse_args(argv)
    try:
        report = run_report(
            db_path=args.db,
            blocker_plan_report=args.blocker_plan_report,
            expected_blocker_plan_report_sha256=str(args.blocker_plan_report_sha256).lower(),
            private_target_manifest=args.private_target_manifest,
            expected_private_target_manifest_sha256=str(
                args.private_target_manifest_sha256
            ).lower(),
            apply_reports=list(args.apply_report),
            expected_apply_report_sha256s=[
                str(value).lower() for value in args.apply_report_sha256
            ],
            out_dir=args.out_dir,
        )
    except Exception as exc:
        print(
            f"incremental dead-letter campaign handoff: ERROR error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    return 0 if _mapping(report.get("verdict")).get("evidence_status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    blocker_plan_report: Path,
    expected_blocker_plan_report_sha256: str,
    private_target_manifest: Path,
    expected_private_target_manifest_sha256: str,
    apply_reports: Sequence[Path],
    expected_apply_report_sha256s: Sequence[str],
    out_dir: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise IncrementalDeadLetterCampaignHandoffError("CODE_TARGET_SCHEMA_MISMATCH")
    if (
        len(apply_reports) != CAMPAIGN_TARGET_COUNT
        or len(expected_apply_report_sha256s) != CAMPAIGN_TARGET_COUNT
    ):
        raise IncrementalDeadLetterCampaignHandoffError("APPLY_REPORT_CHAIN_COUNT_MISMATCH")
    collected_at = observed_at or datetime.now(UTC)
    if collected_at.tzinfo is None or collected_at.utcoffset() is None:
        raise IncrementalDeadLetterCampaignHandoffError("OBSERVED_AT_INVALID")
    collected_at = collected_at.astimezone(UTC)

    try:
        plan_document = read_stable_json_document(
            blocker_plan_report,
            expected_sha256=require_sha256(
                "BLOCKER_PLAN_REPORT_SHA256", expected_blocker_plan_report_sha256
            ),
        )
        plan = preflight_tool._validate_plan(
            plan_document.payload,
            report_sha256=plan_document.sha256,
        )
        manifest_document = read_stable_json_document(
            private_target_manifest,
            expected_sha256=require_sha256(
                "PRIVATE_TARGET_MANIFEST_SHA256",
                expected_private_target_manifest_sha256,
            ),
        )
        manifest = validate_private_target_manifest(
            manifest_document,
            expected_semantic_sha256=str(plan["target_set_sha256"]),
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignHandoffError(exc.code) from exc

    documents = []
    for path, expected_sha256 in zip(apply_reports, expected_apply_report_sha256s, strict=True):
        try:
            documents.append(
                read_stable_json_document(
                    path,
                    expected_sha256=require_sha256("APPLY_REPORT_SHA256", expected_sha256),
                )
            )
        except IncrementalDeadLetterCampaignError as exc:
            raise IncrementalDeadLetterCampaignHandoffError(exc.code) from exc
    apply_chain = _validate_apply_chain(
        documents,
        plan=plan,
        manifest=manifest,
        not_after=collected_at,
    )

    resolved_path = fast0_tool._validated_database_path(db_path)
    fast0_tool._assert_no_sidecars(resolved_path)
    writer_probe_before = fast0_tool._probe_no_other_open_handles(resolved_path)
    identity_pin = fast0_tool._PinnedDatabaseIdentity(resolved_path)
    with identity_pin:
        files_before = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        final_main_matches = fast0_tool._fingerprints_exact(
            apply_chain["final_database_main"], files_before["main"]
        )
        connection = fast0_tool._open_strict_read_only(
            resolved_path,
            sqlite_source=identity_pin.sqlite_source,
        )
        connection.execute("BEGIN DEFERRED")
        try:
            quick_check_raw = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
            identity = fast0_tool._read_database_identity(connection)
            runtime_lock_count = int(
                connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
            )
            campaign = preflight_tool._campaign_snapshot(
                connection,
                manifest=manifest,
                expected_completed=CAMPAIGN_TARGET_COUNT,
                source_plan_report_sha256=plan_document.sha256,
                collected_at=collected_at,
            )
            protected_database = preflight_tool._protected_database_digest(connection)
        finally:
            connection.rollback()
            connection.close()
        identity_pin.assert_path_identity()
        fast0_tool._assert_no_sidecars(resolved_path)
        files_after = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
    writer_probe_after = fast0_tool._probe_no_other_open_handles(resolved_path)

    documents_after = [
        read_stable_json_document(path, expected_sha256=document.sha256)
        for path, document in zip(apply_reports, documents, strict=True)
    ]
    inputs_unchanged = bool(
        _stable_identity(plan_document)
        == _stable_identity(
            read_stable_json_document(
                blocker_plan_report,
                expected_sha256=plan_document.sha256,
            )
        )
        and _stable_identity(manifest_document)
        == _stable_identity(
            read_stable_json_document(
                private_target_manifest,
                expected_sha256=manifest_document.sha256,
            )
        )
        and all(
            _stable_identity(before) == _stable_identity(after)
            for before, after in zip(documents, documents_after, strict=True)
        )
    )
    whole_window_writer_absence_proven = bool(
        writer_probe_before.get("status") == "PASS"
        and writer_probe_before.get("no_other_open_handles") is True
        and writer_probe_after.get("status") == "PASS"
        and writer_probe_after.get("no_other_open_handles") is True
        and identity_pin.public_status().get("status") == "PASS"
    )
    protected_matches_chain = bool(
        protected_database.get("sha256") == apply_chain["protected_database_sha256"]
    )
    stored_chain_matches_apply_chain = bool(
        campaign.get("chain_sha256") == apply_chain["final_chain_sha256"]
    )

    report: dict[str, Any] = {
        "contract": HANDOFF_CONTRACT,
        "generated_at": _wire(collected_at),
        "source": {
            "contract": plan["contract"],
            "report_sha256": plan_document.sha256,
            "database_main": plan["database_main"],
            "private_manifest_sha256": manifest_document.sha256,
            "target_set_sha256": manifest["target_set_sha256"],
            "target_count": manifest["count"],
            "alias_contract": "U{ordinal:02d}",
            "first_alias": "U01",
            "last_alias": "U38",
            "path_recorded": False,
        },
        "campaign": {
            "status": "COMPLETE" if campaign.get("progress_valid") else "BLOCKED",
            **campaign,
            "final_apply_report_sha256": apply_chain["final_apply_report_sha256"],
            "apply_chain_sha256": apply_chain["apply_chain_sha256"],
            "protected_database_sha256": protected_database.get("sha256"),
            "protected_database_matches_chain": protected_matches_chain,
            "stored_chain_matches_apply_chain": stored_chain_matches_apply_chain,
            "identifiers_recorded": False,
            "raw_rows_recorded": False,
        },
        "apply_chain": apply_chain["entries"],
        "database": {
            "identity": identity,
            "identity_pin": identity_pin.public_status(),
            "quick_check": ["ok"] if quick_check_raw == ["ok"] else ["not_ok"],
            "files_before": files_before,
            "files_after": files_after,
            "final_apply_main_matches": final_main_matches,
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": writer_probe_before,
            "writer_probe_after": writer_probe_after,
            "whole_window_writer_absence_proven": whole_window_writer_absence_proven,
            "runtime_lock_count": runtime_lock_count,
        },
        "inputs_unchanged": inputs_unchanged,
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    report["verdict"] = evaluate_report(report)
    preflight_tool._assert_public_report(report)
    write_report(report, out_dir=out_dir)
    return report


def _validate_apply_chain(
    documents: Sequence[Any],
    *,
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    not_after: datetime,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    previous_report_sha256: str | None = None
    previous_chain_sha256: str | None = None
    previous_main: Mapping[str, Any] = _mapping(plan.get("database_main"))
    protected_database_sha256: str | None = None
    previous_generated_at: datetime | None = None
    for ordinal, document in enumerate(documents, start=1):
        alias = f"U{ordinal:02d}"
        payload = document.payload
        try:
            generated_at = require_utc_z_timestamp(
                "APPLY_REPORT_GENERATED_AT", payload.get("generated_at")
            )
        except IncrementalDeadLetterCampaignError as exc:
            raise IncrementalDeadLetterCampaignHandoffError(exc.code) from exc
        if generated_at > not_after:
            raise IncrementalDeadLetterCampaignHandoffError("APPLY_REPORT_FUTURE_TIMESTAMP")
        if previous_generated_at is not None and generated_at < previous_generated_at:
            raise IncrementalDeadLetterCampaignHandoffError("APPLY_REPORT_TIME_ORDER_INVALID")
        validated = preflight_tool.validate_apply_report(
            payload,
            report_sha256=document.sha256,
            expected_alias=alias,
            source_plan_report_sha256=str(plan["report_sha256"]),
            private_manifest_sha256=str(manifest["manifest_file_sha256"]),
            target_set_sha256=str(manifest["target_set_sha256"]),
        )
        campaign = _mapping(payload.get("campaign"))
        database = _mapping(payload.get("database"))
        main_after = _mapping(_mapping(database.get("files_after")).get("main"))
        approved_main = {
            "sha256": campaign.get("approved_database_main_sha256"),
            "size": campaign.get("approved_database_main_size"),
        }
        expected_predecessor_kind = None if ordinal == 1 else "CAMPAIGN_APPLY_REPORT"
        expected_predecessor_report = None if ordinal == 1 else previous_report_sha256
        expected_predecessor_chain = None if ordinal == 1 else previous_chain_sha256
        invariant = _mapping(payload.get("invariants"))
        current_protected_sha256 = invariant.get("protected_database_sha256")
        if ordinal == 1:
            protected_database_sha256 = str(current_protected_sha256)
        valid_transition = bool(
            campaign.get("predecessor_kind") == expected_predecessor_kind
            and campaign.get("predecessor_report_sha256") == expected_predecessor_report
            and campaign.get("predecessor_chain_sha256") == expected_predecessor_chain
            and approved_main.get("sha256") == previous_main.get("sha256")
            and approved_main.get("size") == previous_main.get("size")
            and preflight_tool.plan_tool._fingerprint_valid(main_after)
            and preflight_tool.plan_tool._is_sha256(
                campaign.get("approved_preflight_report_sha256")
            )
            and preflight_tool.plan_tool._is_sha256(current_protected_sha256)
            and current_protected_sha256 == protected_database_sha256
        )
        if not valid_transition:
            raise IncrementalDeadLetterCampaignHandoffError("APPLY_REPORT_CHAIN_TRANSITION_INVALID")
        entry = {
            "alias": alias,
            "ordinal": ordinal,
            "approved_preflight_report_sha256": campaign["approved_preflight_report_sha256"],
            "apply_report_sha256": document.sha256,
            "approval_binding_sha256": campaign["approval_binding_sha256"],
            "predecessor_report_sha256": expected_predecessor_report,
            "predecessor_chain_sha256": expected_predecessor_chain,
            "approved_database_main_sha256": approved_main["sha256"],
            "approved_database_main_size": approved_main["size"],
            "post_database_main_sha256": main_after["sha256"],
            "post_database_main_size": main_after["size"],
            "chain_sha256": validated["campaign_chain_sha256"],
        }
        entries.append(entry)
        previous_report_sha256 = document.sha256
        previous_chain_sha256 = str(validated["campaign_chain_sha256"])
        previous_main = main_after
        previous_generated_at = generated_at
    return {
        "entries": entries,
        "apply_chain_sha256": canonical_sha256(
            {"contract": APPLY_CHAIN_CONTRACT, "items": entries}
        ),
        "final_apply_report_sha256": previous_report_sha256,
        "final_chain_sha256": previous_chain_sha256,
        "final_database_main": dict(previous_main),
        "protected_database_sha256": protected_database_sha256,
    }


def validate_handoff_report(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    expected_source_plan_report_sha256: str,
    expected_source_database_main: Mapping[str, Any],
    expected_target_set_sha256: str,
    expected_target_count: int = CAMPAIGN_TARGET_COUNT,
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    source = _mapping(payload.get("source"))
    campaign = _mapping(payload.get("campaign"))
    database = _mapping(payload.get("database"))
    final_main = _mapping(_mapping(database.get("files_after")).get("main"))
    chain = payload.get("apply_chain")
    chain_valid = bool(
        isinstance(chain, list)
        and len(chain) == expected_target_count
        and all(
            isinstance(entry, Mapping)
            and entry.get("alias") == f"U{ordinal:02d}"
            and entry.get("ordinal") == ordinal
            for ordinal, entry in enumerate(chain, start=1)
        )
    )
    expected_chain_sha256 = (
        canonical_sha256({"contract": APPLY_CHAIN_CONTRACT, "items": chain})
        if chain_valid
        else None
    )
    valid = bool(
        payload.get("contract") == HANDOFF_CONTRACT
        and verdict == evaluate_report(payload)
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("handoff_status") == "COMPLETE"
        and source.get("report_sha256") == expected_source_plan_report_sha256
        and source.get("target_set_sha256") == expected_target_set_sha256
        and source.get("target_count") == expected_target_count
        and source.get("database_main") == expected_source_database_main
        and source.get("path_recorded") is False
        and chain_valid
        and campaign.get("apply_chain_sha256") == expected_chain_sha256
        and campaign.get("completed_count") == expected_target_count
        and campaign.get("pending_count") == 0
        and campaign.get("effective_dead_letter_count") == 0
        and campaign.get("historical_pending_disposition_count") == 0
        and campaign.get("historical_disposed_dead_letter_count") == expected_target_count
        and campaign.get("invalid_row_count") == 0
        and campaign.get("invalid_chain_count") == 0
        and campaign.get("foreign_row_count") == 0
        and campaign.get("unexpected_action_count") == 0
        and campaign.get("stored_chain_matches_apply_chain") is True
        and database.get("final_apply_main_matches") is True
        and preflight_tool.plan_tool._fingerprint_valid(final_main)
        and payload.get("read_only") is True
        and payload.get("database_write_performed") is False
        and payload.get("apply_authorized") is False
        and payload.get("identifiers_recorded") is False
        and payload.get("raw_rows_recorded") is False
        and payload.get("raw_payloads_recorded") is False
    )
    if not valid:
        raise IncrementalDeadLetterCampaignHandoffError("CAMPAIGN_HANDOFF_REPORT_CONTRACT_INVALID")
    preflight_tool._assert_public_report(payload)
    final_entry = _mapping(chain[-1] if isinstance(chain, list) and chain else None)
    return {
        "contract": HANDOFF_CONTRACT,
        "report_sha256": require_sha256("HANDOFF_REPORT_SHA256", report_sha256),
        "source_plan_report_sha256": source["report_sha256"],
        "source_database_main": dict(expected_source_database_main),
        "private_manifest_sha256": source["private_manifest_sha256"],
        "target_set_sha256": source["target_set_sha256"],
        "target_count": source["target_count"],
        "apply_chain_sha256": campaign["apply_chain_sha256"],
        "final_chain_sha256": final_entry["chain_sha256"],
        "final_database_main": dict(final_main),
        "generated_at": payload["generated_at"],
    }


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    source = _mapping(report.get("source"))
    campaign = _mapping(report.get("campaign"))
    database = _mapping(report.get("database"))
    chain = report.get("apply_chain")
    if not (
        source.get("target_count") == CAMPAIGN_TARGET_COUNT
        and preflight_tool.plan_tool._is_sha256(source.get("report_sha256"))
        and preflight_tool.plan_tool._is_sha256(source.get("target_set_sha256"))
        and source.get("path_recorded") is False
    ):
        failures.append("HANDOFF_SOURCE_INVALID")
    if not (
        isinstance(chain, list)
        and len(chain) == CAMPAIGN_TARGET_COUNT
        and campaign.get("apply_chain_sha256")
        == canonical_sha256({"contract": APPLY_CHAIN_CONTRACT, "items": chain})
    ):
        failures.append("HANDOFF_APPLY_CHAIN_INVALID")
    if not (
        campaign.get("status") == "COMPLETE"
        and campaign.get("progress_valid") is True
        and campaign.get("target_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("completed_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("valid_completed_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("pending_count") == 0
        and campaign.get("raw_dead_letter_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("effective_dead_letter_count") == 0
        and campaign.get("historical_pending_disposition_count") == 0
        and campaign.get("historical_disposed_dead_letter_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("disposition_ledger_row_count") == CAMPAIGN_TARGET_COUNT
        and campaign.get("invalid_row_count") == 0
        and campaign.get("invalid_chain_count") == 0
        and campaign.get("foreign_row_count") == 0
        and campaign.get("unexpected_action_count") == 0
        and campaign.get("protected_database_matches_chain") is True
        and campaign.get("stored_chain_matches_apply_chain") is True
    ):
        failures.append("HANDOFF_CAMPAIGN_STATE_INVALID")
    if not (
        database.get("quick_check") == ["ok"]
        and database.get("files_before") == database.get("files_after")
        and database.get("final_apply_main_matches") is True
        and database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and database.get("whole_window_writer_absence_proven") is True
        and database.get("runtime_lock_count") == 0
        and _mapping(database.get("identity")).get("app_name") == APP_NAME
        and _mapping(database.get("identity")).get("schema_version") == _EXPECTED_SCHEMA_VERSION
    ):
        failures.append("HANDOFF_DATABASE_INVALID")
    if report.get("inputs_unchanged") is not True:
        failures.append("HANDOFF_INPUT_CHANGED_DURING_READ")
    if not (
        report.get("read_only") is True
        and report.get("observe_only") is True
        and report.get("database_write_performed") is False
        and report.get("apply_authorized") is False
        and report.get("identifiers_recorded") is False
        and report.get("raw_rows_recorded") is False
        and report.get("raw_payloads_recorded") is False
        and report.get("no_evaluation_run") is True
        and report.get("no_order_commands_created") is True
        and report.get("no_broker_calls") is True
        and report.get("live_sim_allowed") is False
        and report.get("live_real_allowed") is False
    ):
        failures.append("HANDOFF_SAFETY_CONTRACT_INVALID")
    status = "PASS" if not failures else "FAIL"
    return {
        "status": status,
        "evidence_status": status,
        "handoff_status": "COMPLETE" if not failures else "BLOCKED",
        "evidence_failures": sorted(set(failures)),
        "apply_authorized": False,
        "read_only": True,
        "tool_order_side_effects_invoked": False,
        "tool_trading_side_effects_invoked": False,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    preflight_tool._assert_public_report(report)
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw": raw_path, "summary": summary_path}


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    campaign = _mapping(report.get("campaign"))
    return (
        "FAST-0R9 incremental dead-letter campaign handoff\n"
        f"- completed: {campaign.get('completed_count')}/{campaign.get('target_count')}\n"
        f"- effective blockers: {campaign.get('effective_dead_letter_count')}\n"
        f"- evidence: {verdict.get('evidence_status')}\n"
        f"- handoff: {verdict.get('handoff_status')}\n"
        "- apply: NOT AUTHORIZED"
    )


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    campaign = _mapping(report.get("campaign"))
    return "\n".join(
        [
            "# FAST-0R9 incremental dead-letter campaign handoff",
            "",
            f"- Completed: `{campaign.get('completed_count')}/{campaign.get('target_count')}`",
            f"- Effective blockers: `{campaign.get('effective_dead_letter_count')}`",
            f"- Evidence: `{verdict.get('evidence_status')}`",
            f"- Handoff: `{verdict.get('handoff_status')}`",
            "- Apply authorized: `false`",
            "- Raw identifiers recorded: `false`",
            "",
        ]
    )


def _stable_identity(value: Any) -> tuple[Any, ...]:
    return (
        value.sha256,
        value.size,
        value.device,
        value.inode,
        value.mtime_ns,
    )


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
