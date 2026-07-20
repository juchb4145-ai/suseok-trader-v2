from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from services.champion_challenger import (
    CHAMPION_CHALLENGER_INPUT_FORMAT,
    evaluate_experiment,
    load_experiment_bundle,
)
from services.parallel_shadow.models import canonical_sha256
from tools.ops_champion_challenger import (
    build_champion_challenger_report,
    write_champion_challenger_report,
)


def test_offline_comparison_selects_only_better_challenger_for_review(tmp_path) -> None:
    manifest = _experiment(tmp_path)

    first = evaluate_experiment(load_experiment_bundle(manifest))
    second = evaluate_experiment(load_experiment_bundle(manifest))

    assert first.status == "REVIEW_READY"
    assert first.selected_challenger_id == "challenger-1"
    assert first.comparisons[0].verdict == "MEETS_OFFLINE_PROMOTION_CRITERIA"
    assert first.comparisons[0].oos_improvement_ratio == 0.2
    assert first.promotion_applied is False
    assert first.live_sim_activation_changed is False
    assert first.no_trading_side_effects is True
    assert first.result_sha256 == second.result_sha256


def test_fast5_gate_blocks_promotion_but_preserves_offline_result(tmp_path) -> None:
    manifest = _experiment(tmp_path, fast5_status="BLOCKED")

    result = evaluate_experiment(load_experiment_bundle(manifest))

    assert result.status == "BLOCKED_BY_FAST_5"
    assert result.selected_challenger_id == "challenger-1"
    assert "FAST_5_NOT_QUALIFIED" in result.blocker_reasons
    assert result.comparisons[0].verdict == "MEETS_OFFLINE_PROMOTION_CRITERIA"
    assert result.promotion_applied is False


def test_weaker_challenger_retains_champion(tmp_path) -> None:
    manifest = _experiment(tmp_path, challenger_expectancy=9.0, challenger_drawdown=3.0)

    result = evaluate_experiment(load_experiment_bundle(manifest))

    comparison = result.comparisons[0]
    assert result.status == "RETAIN_CHAMPION"
    assert result.selected_challenger_id is None
    assert comparison.verdict == "RETAIN_CHAMPION"
    assert "OOS_EXPECTANCY_IMPROVEMENT_BELOW_MINIMUM" in comparison.reason_codes
    assert "DRAWDOWN_REGRESSION" in comparison.reason_codes


def test_manifest_rejects_more_than_one_changed_axis(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    challenger = manifest["challengers"][0]
    challenger["axis_config_sha256"]["ENTRY"] = _sha("different-entry-axis")
    challenger["entry_config_sha256"] = _sha("different-entry-config")
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="exactly changed_axis"):
        load_experiment_bundle(path)


def test_manifest_rejects_automatic_promotion_mode(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    manifest["promotion_mode"] = "AUTOMATIC"
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="REVIEW_ONLY"):
        load_experiment_bundle(path)


def test_manifest_rejects_candidate_code_commit_difference(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    manifest["challengers"][0]["git_commit_sha"] = "b" * 40
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="same git_commit_sha"):
        load_experiment_bundle(path)


def test_manifest_rejects_non_lowercase_evidence_sha(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    manifest["fast5_evidence_sha256"] = str(manifest["fast5_evidence_sha256"]).upper()
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="lowercase SHA-256"):
        load_experiment_bundle(path)


def test_artifact_file_hash_tampering_is_rejected(tmp_path) -> None:
    manifest = _experiment(tmp_path)
    parsed = json.loads(manifest.read_text(encoding="utf-8"))
    profit_path = tmp_path / parsed["champion"]["profit_lab_artifact"]["path"]
    profit_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact file SHA-256 mismatch"):
        load_experiment_bundle(manifest)


def test_commit_and_model_identity_mismatch_are_rejected(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    champion_profit = tmp_path / manifest["champion"]["profit_lab_artifact"]["path"]
    report = json.loads(champion_profit.read_text(encoding="utf-8"))
    report["identity"]["commit_sha"] = "f" * 40
    manifest["champion"]["profit_lab_artifact"] = _artifact(champion_profit, report)
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="Profit Lab commit identity mismatch"):
        evaluate_experiment(load_experiment_bundle(path))


def test_evaluation_config_change_outside_declared_axis_is_rejected(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    for artifact_name in ("profit_lab_artifact", "parallel_shadow_artifact"):
        reference = manifest["challengers"][0][artifact_name]
        path = tmp_path / reference["path"]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["config"]["minimum_filled_trades"] = 1
        manifest["challengers"][0][artifact_name] = _artifact(path, report)
    path = _write_json(tmp_path / "manifest.json", manifest)

    with pytest.raises(ValueError, match="outside changed_axis"):
        evaluate_experiment(load_experiment_bundle(path))


def test_unqualified_evidence_fails_closed(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    challenger_profit = tmp_path / manifest["challengers"][0]["profit_lab_artifact"]["path"]
    report = json.loads(challenger_profit.read_text(encoding="utf-8"))
    report["status"] = "WARN"
    report["qualification"] = "INSUFFICIENT_SAMPLE"
    manifest["challengers"][0]["profit_lab_artifact"] = _artifact(
        challenger_profit,
        report,
    )
    path = _write_json(tmp_path / "manifest.json", manifest)

    result = evaluate_experiment(load_experiment_bundle(path))

    assert result.status == "BLOCKED"
    assert "challenger-1:ALPHA_NOT_QUALIFIED" in result.blocker_reasons
    assert result.comparisons[0].verdict == "EVIDENCE_BLOCKED"
    assert result.promotion_applied is False


def test_challenger_live_sim_evidence_is_rejected(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    challenger_shadow = tmp_path / manifest["challengers"][0]["parallel_shadow_artifact"]["path"]
    report = _shadow_report("a" * 40, live_canary_count=1, stop_loss_pct=2.5)
    manifest["challengers"][0]["parallel_shadow_artifact"] = _artifact(
        challenger_shadow,
        report,
    )
    path = _write_json(tmp_path / "manifest.json", manifest)

    result = evaluate_experiment(load_experiment_bundle(path))

    assert result.status == "BLOCKED"
    assert "challenger-1:CHALLENGER_LIVE_SIM_EVIDENCE_NOT_ALLOWED" in result.blocker_reasons


def test_missing_explicit_safety_count_fails_closed(tmp_path) -> None:
    manifest = _experiment_value(tmp_path)
    reference = manifest["challengers"][0]["parallel_shadow_artifact"]
    shadow_path = tmp_path / reference["path"]
    report = json.loads(shadow_path.read_text(encoding="utf-8"))
    del report["safety"]["broker_call_count"]
    manifest["challengers"][0]["parallel_shadow_artifact"] = _artifact(
        shadow_path,
        report,
    )
    path = _write_json(tmp_path / "manifest.json", manifest)

    result = evaluate_experiment(load_experiment_bundle(path))

    assert result.status == "BLOCKED"
    assert (
        "challenger-1:PARALLEL_SHADOW_BROKER_CALL_COUNT_EVIDENCE_MISSING"
        in result.blocker_reasons
    )


def test_report_writer_keeps_review_only_safety_contract(tmp_path) -> None:
    result = evaluate_experiment(load_experiment_bundle(_experiment(tmp_path / "input")))
    report = build_champion_challenger_report(result.to_dict(), run_id="fast6-test-run")

    paths = write_champion_challenger_report(report, out_dir=tmp_path / "reports")
    raw = json.loads(paths["raw_json"].read_text(encoding="utf-8"))
    summary = paths["summary_md"].read_text(encoding="utf-8")

    assert raw["verdict"]["status"] == "REVIEW_READY"
    assert raw["verdict"]["no_trading_side_effects"] is True
    assert raw["safety"]["automatic_promotion_available"] is False
    assert raw["safety"]["operational_db_opened"] is False
    assert "FAST-6 Champion / Challenger" in summary
    assert "does not promote a strategy" in summary


def _experiment(
    tmp_path: Path,
    *,
    fast5_status: str = "PASS",
    challenger_expectancy: float = 12.0,
    challenger_drawdown: float = 1.5,
) -> Path:
    value = _experiment_value(
        tmp_path,
        fast5_status=fast5_status,
        challenger_expectancy=challenger_expectancy,
        challenger_drawdown=challenger_drawdown,
    )
    return _write_json(tmp_path / "manifest.json", value)


def _experiment_value(
    tmp_path: Path,
    *,
    fast5_status: str = "PASS",
    challenger_expectancy: float = 12.0,
    challenger_drawdown: float = 1.5,
) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    dates = (
        ("2026-07-01", "TRAIN"),
        ("2026-07-02", "VALIDATION"),
        ("2026-07-03", "TEST"),
    )
    split_sha = canonical_sha256(
        [{"trade_date": trade_date, "dataset_split": split} for trade_date, split in dates]
    )
    champion_commit = "a" * 40
    challenger_commit = champion_commit
    champion_profit = tmp_path / "champion-profit.json"
    champion_shadow = tmp_path / "champion-shadow.json"
    challenger_profit = tmp_path / "challenger-profit.json"
    challenger_shadow = tmp_path / "challenger-shadow.json"
    common_axes = {axis: _sha(f"axis-{axis}") for axis in _axes()}
    challenger_axes = deepcopy(common_axes)
    challenger_axes["STOP"] = _sha("axis-STOP-challenger")
    strategy_sha = _sha("strategy")
    entry_sha = _sha("entry")
    return {
        "format": CHAMPION_CHALLENGER_INPUT_FORMAT,
        "experiment_id": "fast6-stop-001",
        "changed_axis": "STOP",
        "data_start": "2026-07-01",
        "data_end": "2026-07-03",
        "data_split_sha256": split_sha,
        "execution_model_version": "conservative_limit/v1",
        "cost_model_version": "verified-cost/v1",
        "fast5_status": fast5_status,
        "fast5_evidence_sha256": _sha("fast5") if fast5_status == "PASS" else "",
        "promotion_mode": "REVIEW_ONLY",
        "policy": {
            "minimum_oos_expectancy_improvement_ratio": 0.05,
            "minimum_profit_factor_ratio": 1.0,
            "maximum_drawdown_increase_r": 0.0,
        },
        "champion": {
            "candidate_id": "champion",
            "git_commit_sha": champion_commit,
            "strategy_config_sha256": strategy_sha,
            "risk_config_sha256": _sha("risk-champion"),
            "entry_config_sha256": entry_sha,
            "axis_config_sha256": common_axes,
            "profit_lab_artifact": _artifact(
                champion_profit,
                _profit_report(
                    champion_commit,
                    dates,
                    expectancy=10.0,
                    drawdown=2.0,
                    stop_loss_pct=3.0,
                ),
            ),
            "parallel_shadow_artifact": _artifact(
                champion_shadow,
                _shadow_report(champion_commit, live_canary_count=1, stop_loss_pct=3.0),
            ),
        },
        "challengers": [
            {
                "candidate_id": "challenger-1",
                "git_commit_sha": challenger_commit,
                "strategy_config_sha256": strategy_sha,
                "risk_config_sha256": _sha("risk-challenger"),
                "entry_config_sha256": entry_sha,
                "axis_config_sha256": challenger_axes,
                "profit_lab_artifact": _artifact(
                    challenger_profit,
                    _profit_report(
                        challenger_commit,
                        dates,
                        expectancy=challenger_expectancy,
                        drawdown=challenger_drawdown,
                        stop_loss_pct=2.5,
                    ),
                ),
                "parallel_shadow_artifact": _artifact(
                    challenger_shadow,
                    _shadow_report(
                        challenger_commit,
                        live_canary_count=0,
                        stop_loss_pct=2.5,
                    ),
                ),
            }
        ],
    }


def _profit_report(
    commit_sha: str,
    dates: tuple[tuple[str, str], ...],
    *,
    expectancy: float,
    drawdown: float,
    stop_loss_pct: float,
) -> dict[str, object]:
    return {
        "format": "conservative-profit-lab-report/v1",
        "status": "PASS",
        "qualification": "ALPHA_QUALIFIED",
        "qualification_reasons": [],
        "warnings": [],
        "identity": {"commit_sha": commit_sha},
        "result_sha256": _sha(f"profit-{commit_sha}-{expectancy}-{drawdown}"),
        "config": _model_config(stop_loss_pct=stop_loss_pct),
        "cost_model_complete": True,
        "trades": [
            {"trade_date": trade_date, "dataset_split": split} for trade_date, split in dates
        ],
        "metrics": {
            "closed_trade_count": 120,
            "distinct_trade_dates": 12,
            "profit_factor": 1.5 if expectancy > 10 else 1.4,
            "max_drawdown_r": drawdown,
        },
        "grouped_metrics": {
            "dataset_split": {
                "TRAIN": {"net_expectancy": expectancy},
                "VALIDATION": {"net_expectancy": expectancy},
                "TEST": {"net_expectancy": expectancy},
            }
        },
        "source_quality": {
            "alpha_qualified": True,
            "point_in_time_violation_count": 0,
        },
        "safety": {
            "point_in_time_violation_count": 0,
            "operational_db_write_count": 0,
            "gateway_command_write_count": 0,
            "live_sim_write_count": 0,
            "dry_run_write_count": 0,
            "no_trading_side_effects": True,
        },
    }


def _shadow_report(
    commit_sha: str,
    *,
    live_canary_count: int,
    stop_loss_pct: float,
) -> dict[str, object]:
    return {
        "format": "parallel-shadow-report/v1",
        "status": "PASS" if live_canary_count else "WARN",
        "blocker_reasons": [],
        "warnings": [] if live_canary_count else ["NO_LIVE_CANARY_COMPARISON"],
        "identity": {"commit_sha": commit_sha},
        "result_sha256": _sha(f"shadow-{commit_sha}-{live_canary_count}"),
        "config": _model_config(stop_loss_pct=stop_loss_pct),
        "cost_model_complete": True,
        "metrics": {
            "duplicate_shadow_plan_count": 0,
            "shadow_plan_coverage_gap_count": 0,
            "comparison_linkage_gap_count": 0,
            "ai_influenced_plan_count": 0,
            "live_canary_plan_count": live_canary_count,
        },
        "safety": {
            "operational_db_write_count": 0,
            "gateway_command_write_count": 0,
            "live_sim_write_count": 0,
            "broker_call_count": 0,
            "no_trading_side_effects": True,
        },
    }


def _artifact(path: Path, value: dict[str, object]) -> dict[str, str]:
    _write_json(path, value)
    return {"path": path.name, "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    return path


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _model_config(*, stop_loss_pct: float) -> dict[str, object]:
    return {
        "execution_model_version": "conservative_limit/v1",
        "cost_model_version": "verified-cost/v1",
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": 5.0,
        "minimum_filled_trades": 100,
    }


def _axes() -> tuple[str, ...]:
    return ("ENTRY", "STOP", "TAKE_PROFIT", "THEME_THRESHOLD", "MARKET_REGIME")
