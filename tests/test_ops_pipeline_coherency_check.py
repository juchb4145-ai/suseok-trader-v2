from tools.ops_pipeline_coherency_check import evaluate_report


def _report(*, status: str = "PASS", mismatch_count: int = 0):
    coherency = {
        "status": status,
        "reason_codes": [] if status == "PASS" else ["NO_PIPELINE_OBSERVATIONS"],
        "candidate_count": 1 if status == "PASS" else 0,
        "coherent_count": 1 if status == "PASS" else 0,
        "mismatch_count": mismatch_count,
        "missing_lineage_count": mismatch_count,
        "stale_count": 0,
    }
    return {
        "core_status": {
            "mode": "OBSERVE",
            "live_sim_allowed": False,
            "live_real_allowed": False,
        },
        "command_status_before": {
            "order_commands_allowed": False,
            "order_command_count": 0,
            "counts": {"ACKED": 2},
        },
        "command_status_after": {
            "order_commands_allowed": False,
            "order_command_count": 0,
            "counts": {"ACKED": 2},
        },
        "pipeline_coherency": coherency,
        "entry_timing_status": {"latest_plan_count": 1},
        "dashboard_snapshot": {
            "pipeline_coherency": coherency,
            "pipeline_summary": {"coherency": coherency},
        },
    }


def test_ops_pipeline_coherency_passes_consistent_read_only_snapshot() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["coherent_count"] == 1
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0


def test_ops_pipeline_coherency_fails_mixed_latest_rows() -> None:
    verdict = evaluate_report(_report(status="FAIL", mismatch_count=1))

    assert verdict["status"] == "FAIL"
    assert "PIPELINE_COHERENCY_FAIL" in verdict["failures"]
    assert "PIPELINE_LINEAGE_MISMATCH_PRESENT" in verdict["failures"]
    assert "PIPELINE_LINEAGE_MISSING" in verdict["failures"]
