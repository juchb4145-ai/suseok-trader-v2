from tools.ops_theme_coherency_check import evaluate_report


def _report(*, status: str = "PASS", snapshot_mismatch_count: int = 0):
    coherency = {
        "status": status,
        "reason_codes": [] if status == "PASS" else ["THEME_TOP_SET_MISMATCH"],
        "db_top_count": 1,
        "leadership_top_count": 1,
        "source_mismatch_count": 0,
        "snapshot_mismatch_count": snapshot_mismatch_count,
        "missing_snapshot_count": 0,
        "top_set_mismatch_count": 0,
        "stale_count": 0,
        "db_top": _source("THEME_LATEST_SNAPSHOT"),
        "leadership": _source("THEME_FLOW_SNAPSHOT"),
        "db_top_items": [_item("THEME_LATEST_SNAPSHOT")],
        "leadership_items": [_item("THEME_FLOW_SNAPSHOT")],
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
        "theme_coherency": coherency,
        "operator_status": {"theme_coherency": coherency},
        "dashboard_snapshot": {
            "theme_coherency": coherency,
            "pipeline_summary": {"theme_coherency": coherency},
        },
    }


def test_ops_theme_coherency_passes_consistent_read_only_snapshot() -> None:
    verdict = evaluate_report(_report())

    assert verdict["status"] == "PASS"
    assert verdict["snapshot_mismatch_count"] == 0
    assert verdict["command_count_delta"] == 0
    assert verdict["order_command_count_delta"] == 0


def test_ops_theme_coherency_fails_pointer_mismatch() -> None:
    verdict = evaluate_report(_report(status="FAIL", snapshot_mismatch_count=1))

    assert verdict["status"] == "FAIL"
    assert "THEME_COHERENCY_FAIL" in verdict["failures"]
    assert "THEME_SNAPSHOT_MISMATCH_PRESENT" in verdict["failures"]


def test_ops_theme_coherency_accepts_explicit_source_warning() -> None:
    report = _report(status="WARN")
    report["theme_coherency"]["reason_codes"] = [
        "THEME_LEADERSHIP_SOURCE_DIFFERS_FROM_DB_TOP"
    ]

    verdict = evaluate_report(report)

    assert verdict["status"] == "WARN"
    assert verdict["failures"] == []
    assert verdict["warnings"] == ["THEME_LEADERSHIP_SOURCE_DIFFERS_FROM_DB_TOP"]


def _source(source: str) -> dict:
    return {
        "source": source,
        "snapshot_id": "snapshot-theme-a",
        "snapshot_ids": ["snapshot-theme-a"],
        "calculated_at": "2026-07-10T00:00:00Z",
        "data_age_sec": 1.0,
        "watchset_selection_source": None,
    }


def _item(source: str) -> dict:
    return {
        "theme_id": "theme-a",
        "source": source,
        "snapshot_id": "snapshot-theme-a",
        "calculated_at": "2026-07-10T00:00:00Z",
        "data_age_sec": 1.0,
        "watchset_selection_source": None,
    }
