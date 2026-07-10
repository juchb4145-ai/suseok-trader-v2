from __future__ import annotations

from tools.ops_market_index_projection_check import (
    evaluate_report,
    write_report,
)


def test_market_index_ops_report_passes_safe_pr15_contract() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["failures"] == []
    assert verdict["effective_skip_inline_count"] == 0
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0


def test_market_index_ops_report_warns_for_unverified_parser() -> None:
    report = _report()
    report["reconcile_run"]["data"]["parser_unverified_count"] = 2

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["warnings"] == ["MARKET_INDEX_PARSER_UNVERIFIED"]


def test_market_index_ops_report_fails_on_forbidden_cutover_or_bootstrap(tmp_path) -> None:
    report = _report()
    report["routing_status"]["data"]["effective_skip_inline_count"] = 1
    report["reconcile_run"]["data"]["tr_bootstrap_source_count"] = 1
    report["command_status_after"]["data"]["status_counts"]["QUEUED"] = 1

    verdict = evaluate_report(report)
    report["verdict"] = verdict
    paths = write_report(report, out_dir=tmp_path)

    assert verdict["status"] == "FAIL"
    assert "MARKET_INDEX_EFFECTIVE_SKIP_FORBIDDEN_IN_PR15" in verdict["failures"]
    assert "MARKET_INDEX_TR_BOOTSTRAP_SOURCE_NOT_IMPLEMENTED" in verdict["failures"]
    assert "COMMAND_COUNT_CHANGED_DURING_CHECK" in verdict["failures"]
    assert paths["raw_json"].exists()
    assert paths["summary_md"].exists()


def _report() -> dict:
    return {
        "generated_at": "2026-07-10T03:00:00Z",
        "run_worker": False,
        "core_status": {
            "ok": True,
            "data": {
                "mode": "OBSERVE",
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        },
        "command_status_before": {
            "ok": True,
            "data": {
                "status_counts": {"QUEUED": 0, "FAILED": 0},
                "order_command_count": 0,
            },
        },
        "worker_run": {"ok": True, "data": {"status": "NOT_RUN"}},
        "reconcile_run": {
            "ok": True,
            "data": {
                "status": "PASS",
                "checked_event_count": 2,
                "data_unusable_count": 0,
                "parser_unverified_count": 0,
                "tr_bootstrap_source_count": 0,
                "unknown_source_count": 0,
            },
        },
        "latest_reconcile": {"ok": True, "data": {"latest_run": {}}},
        "routing_status": {
            "ok": True,
            "data": {
                "effective_skip_inline_count": 0,
                "tr_bootstrap_adapter_status": "NOT_IMPLEMENTED",
            },
        },
        "projection_outbox": {
            "ok": True,
            "data": {
                "by_projection_name": {
                    "market_index": {"error_count": 0, "dead_letter_count": 0}
                }
            },
        },
        "dashboard_snapshot": {
            "ok": True,
            "data": {
                "market_indexes": {},
                "market_index_projection_reconcile": {},
                "market_index_append_only_routing": {},
            },
        },
        "command_status_after": {
            "ok": True,
            "data": {
                "status_counts": {"QUEUED": 0, "FAILED": 0},
                "order_command_count": 0,
            },
        },
    }
