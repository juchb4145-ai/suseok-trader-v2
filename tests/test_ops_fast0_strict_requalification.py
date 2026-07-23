from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from storage import sqlite as storage_sqlite
from storage.sqlite import initialize_database
from tools import ops_fast0_strict_requalification as tool


def test_strict_requalification_is_aggregate_only_and_preserves_database(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "operating.sqlite3"
    initialize_database(db_path).close()
    baseline = _write_baseline(tmp_path, db_path)
    runtime_log = tmp_path / "runtime.log"
    runtime_log.write_text("clean runtime evidence\n", encoding="utf-8")
    monkeypatch.setattr(tool, "_probe_no_other_open_handles", _passing_writer_probe)

    report = tool.run_report(
        db_path=db_path,
        migration_apply_report=baseline,
        runtime_logs=[runtime_log],
        pipeline_max_age_sec=60.0,
        incremental_retry_limit=3,
        projection_stale_processing_sec=120,
        min_free_bytes=tool._POLICY_MIN_FREE_BYTES,
        run_quick_check=True,
        out_dir=tmp_path / "evidence",
        expected_migration_apply_report_sha256=_sha256_file(baseline),
    )
    raw_text = Path(report["report_paths"]["raw_json"]).read_text(encoding="utf-8")

    assert report["verdict"]["evidence_status"] == "PASS"
    assert report["verdict"]["fast0_status"] == "BLOCKED"
    assert "PIPELINE_QUALIFICATION_NOT_PASS" in report["verdict"]["qualification_blockers"]
    assert "FINAL_OBSERVE_SMOKE_NOT_RUN" in report["verdict"]["qualification_blockers"]
    assert report["database"]["quick_check"] == ["ok"]
    assert report["database"]["files_before"] == report["database"]["files_after"]
    assert report["database"]["identity_pin"]["status"] == "PASS"
    assert report["database"]["identity_pin"]["held_across_snapshot"] is True
    assert report["database"]["whole_window_writer_absence_proven"] is False
    assert report["logical_table_inventory"]["table_count"] > 100
    assert report["logical_table_inventory"]["full_table_row_scan_performed"] is False
    assert report["logical_table_inventory"]["raw_rows_recorded"] is False
    assert "LATEST_RECONCILE_MISSING" in report["projection"]["readiness_reason_codes"]
    assert report["pipeline_coherency"]["raw_rows_recorded"] is False
    assert '"items"' not in raw_text
    assert '"candidate_instance_id"' not in raw_text
    assert '"owner_id"' not in raw_text
    assert str(tmp_path) not in raw_text


def test_fast0_policy_blocks_raw_broker_unconfirmed_even_when_effective_clear() -> None:
    report = _qualification_ready_report()
    report["broker_boundary"]["raw_unconfirmed_count"] = 3
    report["broker_boundary"]["effective_unconfirmed_count"] = 0
    report["broker_boundary"]["semantic_contract_clear"] = True

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "BLOCKED"
    assert "BROKER_RAW_UNCONFIRMED_PRESENT" in verdict["qualification_blockers"]


def test_fast0_policy_allows_preserved_incremental_history_only_when_effective_clear() -> None:
    report = _qualification_ready_report()
    report["incremental_evaluation"]["raw_dead_letter_status_count"] = 38

    clear = tool.evaluate_report(report)
    report["incremental_evaluation"]["effective_dead_letter_count"] = 1
    blocked = tool.evaluate_report(report)

    assert clear["status"] == "PASS"
    assert "INCREMENTAL_RAW_DEAD_LETTER_HISTORY_PRESERVED" in clear["warnings"]
    assert blocked["status"] == "BLOCKED"
    assert "INCREMENTAL_EFFECTIVE_DEAD_LETTER_PRESENT" in blocked["qualification_blockers"]


def test_fast0_policy_uses_pipeline_qualification_without_erasing_canonical_audit() -> None:
    report = _qualification_ready_report()
    report["pipeline_coherency"]["canonical_status"] = "FAIL"
    report["pipeline_coherency"]["qualification_status"] = "PASS"

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert "PIPELINE_CANONICAL_AUDIT_NON_PASS_PRESERVED" in verdict["warnings"]


@pytest.mark.parametrize(
    ("mutator", "expected"),
    [
        (
            lambda report: report["file_log_marker_scan"].update(coverage_provided=False),
            "RUNTIME_LOG_EVIDENCE_NOT_PROVIDED",
        ),
        (
            lambda report: report["file_log_marker_scan"]["marker_counts"].update(
                EVALUATION_RUN_FENCE_LOST=1
            ),
            "RUNTIME_LOG_MARKER_PRESENT",
        ),
        (
            lambda report: report["observe_smoke"].update(command_delta_verified=False),
            "FINAL_OBSERVE_SMOKE_NOT_RUN",
        ),
    ],
)
def test_fast0_policy_fails_closed_on_external_evidence_gaps(mutator, expected) -> None:
    report = _qualification_ready_report()
    mutator(report)

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "BLOCKED"
    assert expected in verdict["qualification_blockers"]


def test_pipeline_collector_recomputes_hidden_qualification_counts(monkeypatch) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    subject_ids = [f"candidate-{index:04d}" for index in range(503)]
    digest = "a" * 64

    def fake_builder(_connection, *, offset, limit, **_kwargs):
        page_ids = subject_ids[offset : offset + limit]
        items = [
            {
                "candidate_instance_id": subject_id,
                "classification": "HISTORICAL_CLOSED",
                "manual_review_required": False,
                "current_source_drift": False,
                "disposition_required": True,
                "active_recovery_required": False,
                "latest_plan_present": False,
                "latest_plan_unexpired": False,
                "latest_plan_status": None,
                "effective_disposition": {
                    "status": "PENDING",
                    "effective": False,
                },
            }
            for subject_id in page_ids
        ]
        return {
            "qualification_status": "BLOCKED",
            "qualification_reason_codes": ["PIPELINE_DISPOSITION_PENDING"],
            "canonical_status": "FAIL",
            "canonical_reason_codes": ["TEST"],
            "classification_counts": {
                "HISTORICAL_CLOSED": len(subject_ids),
                "MISSING_CANDIDATE_MANUAL_REVIEW": 0,
                "STALE_OTHER_DATE_MANUAL_REVIEW": 0,
                "ACTIVE_CURRENT": 0,
            },
            "manual_review_count": 0,
            "manual_review_pending_count": 0,
            "current_source_drift_count": 0,
            "current_source_drift_pending_count": 0,
            "current_source_drift_unknown_count": 0,
            "current_source_drift_unknown_pending_count": 0,
            "disposition_required_count": len(subject_ids),
            "disposition_pending_count": len(subject_ids),
            "plan_expiry_unknown_count": 0,
            "schema_ready": True,
            "full_count": len(subject_ids),
            "offset": offset,
            "returned_count": len(items),
            "has_more": offset + len(items) < len(subject_ids),
            "next_offset": (
                offset + len(items) if offset + len(items) < len(subject_ids) else None
            ),
            "inventory_count_consistent": True,
            "inventory_digest": digest,
            "inventory_end_digest": digest,
            "items": items,
        }

    monkeypatch.setattr(tool, "build_pipeline_coherency_rca_status", fake_builder)
    monkeypatch.setattr(
        tool,
        "is_pipeline_coherency_disposition_schema_ready",
        lambda _connection: True,
    )

    result = tool._read_pipeline_all_pages(
        connection,
        max_age_sec=60.0,
        as_of=datetime(2026, 7, 15, tzinfo=UTC),
    )
    connection.close()

    assert result["full_count"] == 503
    assert result["collected_count"] == 503
    assert result["unique_subject_count"] == 503
    assert result["page_count"] == 2
    assert result["independent_counts"]["disposition_pending_count"] == 503
    assert result["aggregate_contract_consistent"] is True
    assert "items" not in result


def test_order_plan_exact_contract_rejects_wrong_partial_predicate(tmp_path) -> None:
    connection = initialize_database(tmp_path / "wrong-index.sqlite3")
    connection.execute("DROP INDEX uq_live_sim_intents_order_plan_id")
    connection.execute(
        """
        CREATE UNIQUE INDEX uq_live_sim_intents_order_plan_id
        ON live_sim_intents(order_plan_id)
        WHERE trim(order_plan_id) != ''
        """
    )

    status = tool._collect_order_plan_status(connection)
    connection.close()

    assert status["status"] == "FAIL"
    assert status["predicate_valid"] is False
    assert status["exact_index_contract"] is False


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_strict_requalification_rejects_any_sidecar(tmp_path, suffix: str) -> None:
    db_path = tmp_path / "sidecar.sqlite3"
    initialize_database(db_path).close()
    db_path.with_name(f"{db_path.name}{suffix}").write_bytes(b"")

    with pytest.raises(
        tool.Fast0StrictRequalificationError,
        match="STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE",
    ):
        tool.run_report(
            db_path=db_path,
            migration_apply_report=tmp_path / "unused.json",
            runtime_logs=[],
            pipeline_max_age_sec=60.0,
            incremental_retry_limit=3,
            projection_stale_processing_sec=120,
            min_free_bytes=tool._POLICY_MIN_FREE_BYTES,
            run_quick_check=True,
            out_dir=tmp_path / "evidence",
            expected_migration_apply_report_sha256="a" * 64,
        )


def test_runtime_log_scanner_counts_chunk_boundary_without_recording_lines(
    tmp_path,
) -> None:
    marker = b"EVALUATION_RUN_FENCE_LOST"
    path = tmp_path / "runtime.log"
    path.write_bytes(b"x" * (1024 * 1024 - 7) + marker + b"\n")

    result = tool._scan_runtime_logs([path])

    assert result["marker_counts"]["EVALUATION_RUN_FENCE_LOST"] == 1
    assert result["marker_counts"]["OWNER_ALIVE_AFTER_TTL"] == 0
    assert result["matching_lines_recorded"] is False
    assert result["paths_recorded"] is False


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be"])
def test_runtime_log_scanner_detects_utf16_markers(tmp_path, encoding: str) -> None:
    path = tmp_path / f"runtime-{encoding}.log"
    path.write_bytes("OWNER_ALIVE_AFTER_TTL".encode(encoding))

    result = tool._scan_runtime_logs([path])

    assert result["marker_counts"]["OWNER_ALIVE_AFTER_TTL"] == 1
    assert result["coverage_authoritative"] is False
    assert result["coverage_scope"] == "BOUNDED_HISTORICAL_FILES"


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("CREATE TABLE x (a b)", "CREATE TABLE x (ab)"),
        (
            "CREATE TABLE x (a TEXT DEFAULT 'a b')",
            "CREATE TABLE x (a TEXT DEFAULT 'ab')",
        ),
    ],
)
def test_sql_token_hash_separates_legacy_normalization_collisions(
    left: str,
    right: str,
) -> None:
    assert tool._normalize_sql(left) == tool._normalize_sql(right)
    assert tool._sql_token_sha256(left) != tool._sql_token_sha256(right)


def test_sql_token_hash_ignores_formatting_and_comments() -> None:
    compact = "CREATE TABLE x(a INTEGER DEFAULT 'a b');"
    formatted = "create /* format only */ table x ( a integer default 'a b' )"

    assert tool._sql_token_sha256(compact) == tool._sql_token_sha256(formatted)


@pytest.mark.parametrize(
    "sql",
    ["CREATE TABLE x(a TEXT DEFAULT 'unterminated)", "CREATE /* unterminated"],
)
def test_sql_token_hash_rejects_unterminated_tokens(sql: str) -> None:
    with pytest.raises(tool.Fast0StrictRequalificationError):
        tool._sql_token_sha256(sql)


@pytest.mark.parametrize(
    ("name", "historical_sql", "upgrade"),
    [
        (
            "runtime_execution_locks",
            """
            CREATE TABLE runtime_execution_locks (
              lock_name TEXT PRIMARY KEY,
              owner_id TEXT NOT NULL,
              acquired_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              detail_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            storage_sqlite._create_operator_tables,
        ),
        (
            "market_data_projection_reconcile_runs",
            """
            CREATE TABLE market_data_projection_reconcile_runs (
              run_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              checked_event_count INTEGER NOT NULL,
              checked_price_tick_count INTEGER NOT NULL,
              checked_condition_event_count INTEGER NOT NULL,
              checked_tr_response_count INTEGER NOT NULL,
              outbox_job_count INTEGER NOT NULL,
              outbox_pending_count INTEGER NOT NULL,
              outbox_processing_count INTEGER NOT NULL,
              outbox_applied_count INTEGER NOT NULL,
              outbox_skipped_count INTEGER NOT NULL,
              outbox_error_count INTEGER NOT NULL,
              outbox_dead_letter_count INTEGER NOT NULL,
              missing_projection_count INTEGER NOT NULL,
              inline_projection_error_count INTEGER NOT NULL,
              outbox_error_issue_count INTEGER NOT NULL,
              duplicate_or_conflict_count INTEGER NOT NULL,
              synthetic_child_event_issue_count INTEGER NOT NULL,
              watermark_risk_count INTEGER NOT NULL,
              event_rowid_min INTEGER,
              event_rowid_max INTEGER,
              append_only_ready INTEGER NOT NULL DEFAULT 0,
              reason_codes_json TEXT NOT NULL DEFAULT '[]',
              summary_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              no_trading_side_effects INTEGER NOT NULL DEFAULT 1,
              tr_response_effective_skip_count INTEGER NOT NULL DEFAULT 0,
              tr_response_pending_within_sla_count INTEGER NOT NULL DEFAULT 0,
              tr_response_worker_applied_count INTEGER NOT NULL DEFAULT 0,
              tr_response_deferred_quote_refresh_count INTEGER NOT NULL DEFAULT 0,
              tr_response_deferred_quote_refresh_error_count INTEGER NOT NULL DEFAULT 0,
              condition_event_effective_skip_count INTEGER NOT NULL DEFAULT 0,
              invalid_effective_skip_count INTEGER NOT NULL DEFAULT 0,
              condition_event_worker_applied_count INTEGER NOT NULL DEFAULT 0,
              condition_event_deferred_fusion_refresh_count INTEGER NOT NULL DEFAULT 0,
              condition_event_deferred_fusion_refresh_error_count INTEGER NOT NULL DEFAULT 0,
              condition_event_side_effect_duplicate_count INTEGER NOT NULL DEFAULT 0,
              condition_event_candidate_ingest_in_worker_count INTEGER NOT NULL DEFAULT 0,
              condition_event_pending_within_sla_count INTEGER NOT NULL DEFAULT 0,
              condition_event_artifact_missing_after_worker_count INTEGER NOT NULL DEFAULT 0
            )
            """,
            storage_sqlite._create_market_data_projection_reconcile_tables,
        ),
        (
            "market_data_projection_routing_decisions",
            """
            CREATE TABLE market_data_projection_routing_decisions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id TEXT NOT NULL,
              event_type TEXT NOT NULL,
              projection_name TEXT NOT NULL DEFAULT 'market_data',
              dry_run_enabled INTEGER NOT NULL DEFAULT 0,
              cutover_enabled INTEGER NOT NULL DEFAULT 0,
              reconcile_required INTEGER NOT NULL DEFAULT 1,
              latest_reconcile_run_id TEXT,
              latest_reconcile_status TEXT,
              latest_reconcile_created_at TEXT,
              latest_reconcile_age_sec REAL,
              append_only_ready INTEGER NOT NULL DEFAULT 0,
              outbox_status TEXT,
              outbox_job_present INTEGER NOT NULL DEFAULT 0,
              would_skip_inline INTEGER NOT NULL DEFAULT 0,
              effective_skip_inline INTEGER NOT NULL DEFAULT 0,
              blocked_reason_codes_json TEXT NOT NULL DEFAULT '[]',
              evidence_json TEXT NOT NULL DEFAULT '{}',
              decided_at TEXT NOT NULL DEFAULT (datetime('now')),
              cutover_scope TEXT,
              skip_budget_limit INTEGER,
              skip_budget_used INTEGER,
              skip_budget_remaining INTEGER,
              worker_apply_enabled INTEGER NOT NULL DEFAULT 0,
              fallback_inline_projection_expected INTEGER NOT NULL DEFAULT 1,
              post_apply_deferred_side_effects_json TEXT NOT NULL DEFAULT '{}',
              tr_response_rows_count INTEGER,
              tr_response_skip_budget_limit INTEGER,
              tr_response_skip_budget_used INTEGER,
              tr_response_skip_budget_remaining INTEGER,
              synthetic_child_guard_status TEXT,
              worker_side_effect_ready INTEGER,
              deferred_side_effect_required INTEGER,
              condition_event_skip_budget_limit INTEGER,
              condition_event_skip_budget_used INTEGER,
              condition_event_skip_budget_remaining INTEGER,
              condition_event_worker_side_effect_ready INTEGER,
              condition_event_fusion_enabled INTEGER,
              condition_event_backlog_ready INTEGER,
              condition_event_code TEXT,
              condition_event_action TEXT,
              candidate_ingest_executed INTEGER NOT NULL DEFAULT 0,
              UNIQUE(event_id, projection_name)
            )
            """,
            storage_sqlite._create_market_data_projection_routing_tables,
        ),
    ],
)
def test_exact_historical_schema_variants_are_accepted(
    name: str,
    historical_sql: str,
    upgrade,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(historical_sql)
    upgrade(connection)
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()

    assert row is not None
    assert tool._normalized_sql_sha256(row["sql"]) in {
        tool._SCHEMA_OBJECT_MANIFEST[name][1],
        *tool._SCHEMA_OBJECT_ALTERNATE_SHA256.get(name, frozenset()),
    }
    assert tool._sql_token_sha256(row["sql"]) in {
        tool._SCHEMA_OBJECT_TOKEN_SHA256[name],
        *tool._SCHEMA_OBJECT_ALTERNATE_TOKEN_SHA256.get(name, frozenset()),
    }
    assert tool._schema_objects_valid(connection, (name,)) is True
    connection.close()


def test_historical_schema_one_token_mutation_is_rejected() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE runtime_execution_locks (
          lock_name TEXT PRIMARY KEY,
          owner_id TEXT NOT NULL,
          acquired_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          detail_json TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    storage_sqlite._create_operator_tables(connection)

    assert tool._schema_objects_valid(connection, ("runtime_execution_locks",)) is False
    connection.close()


def test_redactor_handles_sensitive_and_reserved_mapping_keys_without_collision() -> None:
    token = "ghp_" + "a" * 36
    value = {
        "[REDACTED_KEY_1]": "safe-a",
        token: "safe-b",
        1: "safe-c",
        "1": "account=1234567890",
    }

    redacted = tool._redact(value)
    serialized = json.dumps(redacted, sort_keys=True)

    assert len(redacted) == len(value)
    assert len(set(redacted)) == len(value)
    assert token not in serialized
    assert "1234567890" not in serialized
    assert "[REDACTED]" in serialized


def test_fast0_policy_treats_small_ordinary_incremental_queue_as_warning() -> None:
    report = _qualification_ready_report()
    report["incremental_evaluation"]["queued_count"] = 1

    verdict = tool.evaluate_report(report)

    assert verdict["status"] == "PASS"
    assert "INCREMENTAL_QUEUE_ROWS_PRESERVED" in verdict["warnings"]
    assert "INCREMENTAL_QUEUE_BACKLOG_FAIL" not in verdict["qualification_blockers"]


def test_fast0_policy_warns_on_projection_pending_but_blocks_processing() -> None:
    report = _qualification_ready_report()
    report["projection"]["outbox_pending_count"] = 1
    pending = tool.evaluate_report(report)
    report["projection"]["outbox_processing_count"] = 1
    processing = tool.evaluate_report(report)

    assert pending["status"] == "PASS"
    assert "PROJECTION_PENDING_ROWS_PRESERVED" in pending["warnings"]
    assert processing["status"] == "BLOCKED"
    assert "QUIESCENT_PROJECTION_PROCESSING_PRESENT" in processing[
        "qualification_blockers"
    ]


@pytest.mark.parametrize(
    ("section", "field", "value", "expected"),
    [
        (
            "incremental_evaluation",
            "backlog_fail_count",
            1_000_000,
            "INCREMENTAL_THRESHOLD_CONTRACT_INVALID",
        ),
        (
            "projection",
            "backlog_fail_pending_count",
            1_000_000,
            "PROJECTION_THRESHOLD_CONTRACT_INVALID",
        ),
    ],
)
def test_evaluator_rejects_section_threshold_policy_drift(
    section: str,
    field: str,
    value: int,
    expected: str,
) -> None:
    report = _qualification_ready_report()
    report[section]["thresholds"][field] = value

    verdict = tool.evaluate_report(report)

    assert verdict["evidence_status"] == "FAIL"
    assert expected in verdict["evidence_failures"]


def test_projection_collector_blocks_missing_reconcile_and_invalid_effective_skip(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "projection.sqlite3")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
          event_id, event_type, effective_skip_inline
        ) VALUES ('event-invalid-skip', 'unknown_market_event', 1)
        """
    )

    status = tool._collect_projection_status(
        connection,
        stale_processing_sec=tool._POLICY_PROJECTION_STALE_PROCESSING_SEC,
        observed_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    connection.close()

    assert status["readiness_status"] == "FAIL"
    assert status["latest_reconcile_present"] is False
    assert status["invalid_effective_skip_count"] == 1
    assert "LATEST_RECONCILE_MISSING" in status["readiness_reason_codes"]
    assert "INVALID_EFFECTIVE_SKIP_EVENT_TYPE" in status["readiness_reason_codes"]


def test_lifecycle_sanitizer_drops_unknown_future_fields(tmp_path) -> None:
    connection = initialize_database(tmp_path / "lifecycle.sqlite3")
    source = tool.lifecycle_tool._read_all_pages(connection, code=None)
    connection.close()
    source["future_identifier_sample"] = "candidate-raw-123"

    sanitized = tool._sanitize_lifecycle_report(source)

    assert "future_identifier_sample" not in sanitized
    assert "candidate-raw-123" not in json.dumps(sanitized)
    assert sanitized["privacy_contract_valid"] is False


def test_run_report_rejects_policy_relaxation_before_database_scan(tmp_path) -> None:
    with pytest.raises(
        tool.Fast0StrictRequalificationError,
        match="FAST0_POLICY_RELAXATION_REJECTED",
    ):
        tool.run_report(
            db_path=tmp_path / "missing.sqlite3",
            migration_apply_report=tmp_path / "missing.json",
            runtime_logs=[],
            pipeline_max_age_sec=60.0,
            incremental_retry_limit=3,
            projection_stale_processing_sec=120,
            min_free_bytes=0,
            run_quick_check=True,
            out_dir=tmp_path / "evidence",
            expected_migration_apply_report_sha256="a" * 64,
        )


def test_migration_baseline_rejects_preservation_probe_tamper(tmp_path) -> None:
    db_path = tmp_path / "operating.sqlite3"
    initialize_database(db_path).close()
    baseline_path = _write_baseline(tmp_path, db_path)
    payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    payload["safety"]["target_contract_probes"]["valid_revoke"] = False
    baseline_path.write_text(json.dumps(payload), encoding="utf-8")

    baseline = tool._read_migration_baseline(
        baseline_path,
        expected_report_sha256=_sha256_file(baseline_path),
        current_path=db_path.resolve(),
        current_main=tool._file_fingerprints(db_path)["main"],
    )

    assert baseline["target_probe_contract_valid"] is False
    assert baseline["preservation_contract_valid"] is False
    assert baseline["contract_ready"] is False


def test_stable_json_rejects_duplicate_keys(tmp_path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"status":"PASS","status":"FAIL"}', encoding="utf-8")

    with pytest.raises(tool.Fast0StrictRequalificationError, match="TEST_INVALID"):
        tool._read_stable_json(
            path,
            missing_reason="TEST_MISSING",
            invalid_reason="TEST_INVALID",
        )


def test_commands_template_includes_authorized_migration_report_sha(tmp_path) -> None:
    paths = tool.write_report(_qualification_ready_report(), out_dir=tmp_path)
    command = paths["commands_txt"].read_text(encoding="utf-8")

    assert "--migration-apply-report-sha256" in command
    assert "<approved-migration-report-sha256>" in command


def test_database_identity_never_emits_malformed_metadata_values() -> None:
    token = "ghp_" + "a" * 36
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE app_metadata (key TEXT, value TEXT)")
    connection.executemany(
        "INSERT INTO app_metadata (key, value) VALUES (?, ?)",
        [("app_name", token), ("schema_version", "candidate-raw-identifier")],
    )

    identity = tool._read_database_identity(connection)
    connection.close()
    serialized = json.dumps(identity)

    assert identity["app_name"] is None
    assert identity["schema_version"] is None
    assert identity["app_name_value_valid"] is False
    assert identity["schema_version_value_valid"] is False
    assert token not in serialized
    assert "candidate-raw-identifier" not in serialized


def test_summary_uses_redacted_report_values(tmp_path) -> None:
    token = "ghp_" + "a" * 36
    report = _qualification_ready_report()
    report["pipeline_coherency"]["canonical_status"] = token

    paths = tool.write_report(report, out_dir=tmp_path)
    summary = paths["summary_md"].read_text(encoding="utf-8")
    raw = paths["raw_json"].read_text(encoding="utf-8")

    assert token not in summary
    assert token not in raw
    assert "[REDACTED]" in summary


def test_logical_preservation_is_not_complete_for_invalid_chain() -> None:
    status = tool._baseline_logical_preservation(
        {
            "contract_ready": False,
            "current_after_matches_migration": True,
            "existing_table_count": 1,
        }
    )

    assert status["verification_complete"] is False


def test_cli_hides_sensitive_exception_detail(monkeypatch, capsys) -> None:
    token = "ghp_" + "a" * 36

    def raise_sensitive(**_kwargs):
        raise RuntimeError(f"token={token}")

    monkeypatch.setattr(tool, "run_report", raise_sensitive)
    monkeypatch.setattr(
        tool.sys,
        "argv",
        [
            "ops_fast0_strict_requalification",
            "--db",
            "missing.sqlite3",
            "--migration-apply-report",
            "missing.json",
            "--migration-apply-report-sha256",
            "a" * 64,
        ],
    )

    exit_code = tool.main()
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "error_type=RuntimeError" in captured.err
    assert token not in captured.err
    assert "token=" not in captured.err


def _write_baseline(tmp_path: Path, db_path: Path) -> Path:
    main = tool._file_fingerprints(db_path)["main"]
    preflight_true_keys = (
        "backup_tables_preserved",
        "backup_table_content_preserved",
        "target_table_set_exact",
        "existing_table_content_preserved",
        "migration_table_content_preserved",
        "order_state_preserved",
        "projection_outbox_preserved",
        "source_data_files_unchanged",
        "sqlite_sequence_preserved",
        "target_objects_absent_at_source",
        "target_objects_exact",
        "target_contract_valid",
        "target_ledger_empty",
        "initializer_noop",
        "quick_checks_ok",
    )
    preflight_payload = {
        "preflight_contract_version": tool._MIGRATION_PREFLIGHT_CONTRACT,
        "required_source_schema": tool._MIGRATION_SOURCE_SCHEMA,
        "target_schema_version": tool._EXPECTED_SCHEMA_VERSION,
        "source": {
            "opened_read_only": True,
            "query_only": True,
            "immutable": True,
            "quick_check": ["ok"],
            "path": str(db_path.resolve()),
            "files_after": {"main": copy.deepcopy(main)},
        },
        "verdict": {
            "status": "PASS",
            "failures": [],
            "operating_database_mutated": False,
            "source_schema_version": tool._MIGRATION_SOURCE_SCHEMA,
            "target_schema_version": tool._EXPECTED_SCHEMA_VERSION,
            "quick_check": ["ok"],
            "exact_quick_check": ["ok"],
            **{key: True for key in preflight_true_keys},
        },
    }
    preflight_path = tmp_path / "migration-preflight.json"
    preflight_path.write_text(json.dumps(preflight_payload), encoding="utf-8")
    preflight_sha256 = _sha256_file(preflight_path)
    lease_counts = {key: 0 for key in tool._MIGRATION_LEASE_KEYS}
    payload = {
        "status": "PASS",
        "committed": True,
        "migration": {
            "source_schema": tool._MIGRATION_SOURCE_SCHEMA,
            "target_schema": tool._EXPECTED_SCHEMA_VERSION,
            "exact_function": tool._MIGRATION_APPLY_FUNCTION,
            "acknowledgement_matched": True,
            "authorizer_denial_count": 0,
        },
        "source": {
            "path": str(db_path.resolve()),
            "schema_before": tool._MIGRATION_SOURCE_SCHEMA,
            "schema_after": tool._EXPECTED_SCHEMA_VERSION,
            "strict_read_only_quick_check": ["ok"],
            "post_commit_quick_check": ["ok"],
            "exclusive_post_commit_quick_check": ["ok"],
            "hardlink_count": 1,
            "sidecar_count_after": 0,
            "preflight_main": copy.deepcopy(main),
            "post_main": copy.deepcopy(main),
            "exclusive_post_commit_main": copy.deepcopy(main),
        },
        "preflight": {
            "path": str(preflight_path.resolve()),
            "sha256": preflight_sha256,
            "age_sec": 1,
            "verdict": "PASS",
        },
        "backup": {
            "schema_version": tool._MIGRATION_SOURCE_SCHEMA,
            "quick_check": ["ok"],
            "byte_identical": True,
            "hardlink_count": 1,
            "sidecar_count": 0,
            "sha256": main["sha256"],
            "size": main["size"],
        },
        "counts": {
            "existing_table_count": 147,
            "target_ledger_row_count": 0,
        },
        "safety": {
            "exact_precommit_postcommit_snapshot_equal": True,
            "postcommit_readonly_exclusive_snapshot_equal": True,
            "exclusive_postcommit_final_fingerprint_equal": True,
            "existing_table_content_preserved": True,
            "sqlite_sequence_preserved": True,
            "projection_outbox_preserved": True,
            "target_ledger_empty": True,
            "exclusive_transaction_acquired": True,
            "exclusive_post_commit_transaction_acquired": True,
            "lease_counts": copy.deepcopy(lease_counts),
            "exclusive_lease_counts": copy.deepcopy(lease_counts),
            "exclusive_post_commit_lease_counts": copy.deepcopy(lease_counts),
            "exclusive_post_commit_runtime_lease_count": 0,
            "source_sidecar_count_before": 0,
            "target_contract_probes": {
                key: True for key in tool._MIGRATION_TARGET_PROBES
            },
        },
        "evidence_policy": {
            "raw_rows_included": False,
            "secrets_included": False,
            "account_numbers_included": False,
            "hashes_and_counts_only": True,
        },
    }
    path = tmp_path / "migration-apply.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _passing_writer_probe(_path: Path) -> dict:
    return {
        "status": "PASS",
        "method": "TEST",
        "no_other_open_handles": True,
    }


def _qualification_ready_report() -> dict:
    digest = hashlib.sha256(b"ready").hexdigest()
    files = {
        "main": {"exists": True, "size": 1, "mtime_ns": 1, "sha256": digest},
        "wal": {"exists": False, "size": 0, "mtime_ns": None, "sha256": None},
        "shm": {"exists": False, "size": 0, "mtime_ns": None, "sha256": None},
        "journal": {"exists": False, "size": 0, "mtime_ns": None, "sha256": None},
    }
    marker_counts = {marker: 0 for marker in tool._RUNTIME_MARKERS}
    return {
        "policy": {
            "contract": "fast0-strict-offline-policy.v1",
            "pipeline_max_age_sec": tool._POLICY_PIPELINE_MAX_AGE_SEC,
            "incremental_retry_limit": tool._POLICY_INCREMENTAL_RETRY_LIMIT,
            "incremental_backlog_warn_count": (
                tool._DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT
            ),
            "incremental_backlog_fail_count": (
                tool._DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT
            ),
            "incremental_stale_warn_sec": tool._DEFAULT_INCREMENTAL_STALE_WARN_SEC,
            "incremental_stale_fail_sec": tool._DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
            "projection_stale_processing_sec": (
                tool._POLICY_PROJECTION_STALE_PROCESSING_SEC
            ),
            "projection_blocking_older_than_sec": (
                tool._POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC
            ),
            "projection_backlog_warn_pending_count": (
                tool._POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT
            ),
            "projection_backlog_fail_pending_count": (
                tool._POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT
            ),
            "projection_recent_window_sec": tool._POLICY_PROJECTION_RECENT_WINDOW_SEC,
            "projection_recent_fail_count": tool._POLICY_PROJECTION_RECENT_FAIL_COUNT,
            "projection_condition_max_pending": (
                tool._POLICY_PROJECTION_CONDITION_MAX_PENDING
            ),
            "projection_condition_recent_max_pending": (
                tool._POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING
            ),
            "min_free_bytes": tool._POLICY_MIN_FREE_BYTES,
            "runtime_log_max_files": tool._MAX_RUNTIME_LOG_FILES,
            "runtime_log_max_total_bytes": tool._MAX_RUNTIME_LOG_TOTAL_BYTES,
        },
        "database": {
            "identity": {
                "app_name": "suseok-trader-v2",
                "app_name_row_count": 1,
                "schema_version": "63",
                "schema_version_row_count": 1,
            },
            "database_list": [{"seq": 0, "name": "main", "file_present": True}],
            "quick_check": ["ok"],
            "connection": {
                "mode": "ro",
                "immutable": True,
                "query_only": True,
                "single_deferred_snapshot": True,
            },
            "files_before": copy.deepcopy(files),
            "files_after": copy.deepcopy(files),
            "writer_probe_before": {"status": "PASS"},
            "writer_probe_after": {"status": "PASS"},
            "identity_pin": {
                "status": "PASS",
                "method": "TEST",
                "held_across_snapshot": True,
                "path_identity_stable": True,
                "raw_identity_recorded": False,
            },
            "writer_quiescence_scope": (
                "PINNED_IDENTITY_PLUS_POINT_IN_TIME_PROBES_AND_PHYSICAL_HASH"
            ),
            "whole_window_writer_absence_proven": False,
            "disk": {"sufficient": True},
        },
        "migration_baseline": {
            "contract_ready": True,
            "current_before_matches_migration": True,
            "current_after_matches_migration": True,
        },
        "schema_manifest": {"ready": True},
        "pipeline_lineage_schema": {"ready": True},
        "logical_table_inventory": {
            "verification_complete": True,
            "verification_method": "MIGRATION_CHAIN_AND_CURRENT_PHYSICAL_HASH",
            "full_table_row_scan_performed": False,
            "table_count": 1,
            "inventory_digest": digest,
        },
        "order_state_inventory": {"preservation_contract_valid": True},
        "order_plan_uniqueness": {"status": "PASS", "exact_index_contract": True},
        "broker_boundary": {
            "structure_ready": True,
            "state_bucket_contract_valid": True,
            "semantic_contract_clear": True,
            "raw_unconfirmed_count": 0,
            "effective_unconfirmed_count": 0,
        },
        "incremental_evaluation": {
            "schema_ready": True,
            "raw_status_contract_valid": True,
            "bucket_conservation_valid": True,
            "queue_timestamp_contract_valid": True,
            "queued_count": 0,
            "stale_warn_count": 0,
            "stale_fail_count": 0,
            "retry_exhausted_count": 0,
            "raw_dead_letter_status_count": 0,
            "effective_dead_letter_count": 0,
            "retry_limit": tool._POLICY_INCREMENTAL_RETRY_LIMIT,
            "thresholds": {
                "backlog_warn_count": tool._DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT,
                "backlog_fail_count": tool._DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT,
                "stale_warn_sec": tool._DEFAULT_INCREMENTAL_STALE_WARN_SEC,
                "stale_fail_sec": tool._DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
            },
        },
        "pipeline_coherency": {
            "schema_ready": True,
            "classification_bucket_contract_valid": True,
            "disposition_bucket_contract_valid": True,
            "inventory_count_consistent": True,
            "page_contract_consistent": True,
            "aggregate_contract_consistent": True,
            "invalid_item_count": 0,
            "full_count": 0,
            "collected_count": 0,
            "unique_subject_count": 0,
            "qualification_status": "PASS",
            "canonical_status": "PASS",
        },
        "execution_lifecycle": {
            "strict_verdict": {"status": "PASS"},
            "classification_bucket_contract_valid": True,
            "reason_code_contract_valid": True,
            "privacy_contract_valid": True,
            "qualification_status": "PASS",
            "canonical_status": "PASS",
        },
        "projection": {
            "schema_ready": True,
            "aggregate_contract_valid": True,
            "readiness_status": "PASS",
            "outbox_error_count": 0,
            "outbox_dead_letter_count": 0,
            "outbox_processing_count": 0,
            "stale_processing_count": 0,
            "event_result_error_count": 0,
            "applied_without_success_count": 0,
            "outbox_pending_count": 0,
            "thresholds": {
                "stale_processing_sec": tool._POLICY_PROJECTION_STALE_PROCESSING_SEC,
                "blocking_older_than_sec": (
                    tool._POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC
                ),
                "backlog_warn_pending_count": (
                    tool._POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT
                ),
                "backlog_fail_pending_count": (
                    tool._POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT
                ),
                "recent_window_sec": tool._POLICY_PROJECTION_RECENT_WINDOW_SEC,
                "recent_fail_count": tool._POLICY_PROJECTION_RECENT_FAIL_COUNT,
                "condition_max_pending": tool._POLICY_PROJECTION_CONDITION_MAX_PENDING,
                "condition_recent_max_pending": (
                    tool._POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING
                ),
            },
        },
        "runtime_execution": {
            "schema_ready": True,
            "fencing_contract_valid": True,
            "lock_count": 0,
        },
        "database_marker_scan": {
            "scan_complete": True,
            "marker_counts": copy.deepcopy(marker_counts),
        },
        "file_log_marker_scan": {
            "scan_complete": True,
            "coverage_provided": True,
            "coverage_authoritative": True,
            "marker_counts": copy.deepcopy(marker_counts),
        },
        "modify_order": {"gateway_commands_count": 0},
        "observe_smoke": {"command_delta_verified": True},
        "read_only": True,
        "raw_rows_recorded": False,
        "identifiers_recorded": False,
    }
