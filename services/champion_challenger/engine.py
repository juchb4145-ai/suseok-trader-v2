from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.champion_challenger.models import (
    CHAMPION_CHALLENGER_REPORT_FORMAT,
    CandidateSpec,
    ExperimentManifest,
    require_sha256,
)
from services.parallel_shadow.models import (
    PARALLEL_SHADOW_REPORT_FORMAT,
    canonical_sha256,
)
from services.profit_lab.models import PROFIT_LAB_REPORT_FORMAT


@dataclass(frozen=True, kw_only=True)
class LoadedCandidate:
    spec: CandidateSpec
    profit_lab_report: Mapping[str, Any]
    parallel_shadow_report: Mapping[str, Any]


@dataclass(frozen=True, kw_only=True)
class ExperimentBundle:
    manifest: ExperimentManifest
    manifest_sha256: str
    champion: LoadedCandidate
    challengers: tuple[LoadedCandidate, ...]


@dataclass(frozen=True, kw_only=True)
class CandidateEvidence:
    candidate_id: str
    role: str
    qualification_verdict: str
    blocker_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    git_commit_sha: str
    strategy_config_sha256: str
    risk_config_sha256: str
    entry_config_sha256: str
    axis_config_sha256: Mapping[str, str]
    profit_lab_file_sha256: str
    profit_lab_result_sha256: str
    parallel_shadow_file_sha256: str
    parallel_shadow_result_sha256: str
    validation_net_expectancy: float | None
    test_net_expectancy: float | None
    profit_factor: float | None
    max_drawdown_r: float | None
    closed_trade_count: int
    distinct_trade_dates: int
    shadow_live_canary_plan_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "role": self.role,
            "qualification_verdict": self.qualification_verdict,
            "blocker_reasons": list(self.blocker_reasons),
            "warnings": list(self.warnings),
            "git_commit_sha": self.git_commit_sha,
            "config_sha256": {
                "strategy": self.strategy_config_sha256,
                "risk": self.risk_config_sha256,
                "entry": self.entry_config_sha256,
                "experiment_axes": dict(self.axis_config_sha256),
            },
            "evidence_sha256": {
                "profit_lab_file": self.profit_lab_file_sha256,
                "profit_lab_result": self.profit_lab_result_sha256,
                "parallel_shadow_file": self.parallel_shadow_file_sha256,
                "parallel_shadow_result": self.parallel_shadow_result_sha256,
            },
            "metrics": {
                "validation_net_expectancy": self.validation_net_expectancy,
                "test_net_expectancy": self.test_net_expectancy,
                "profit_factor": self.profit_factor,
                "max_drawdown_r": self.max_drawdown_r,
                "closed_trade_count": self.closed_trade_count,
                "distinct_trade_dates": self.distinct_trade_dates,
                "shadow_live_canary_plan_count": self.shadow_live_canary_plan_count,
            },
        }


@dataclass(frozen=True, kw_only=True)
class ChallengerComparison:
    candidate_id: str
    verdict: str
    reason_codes: tuple[str, ...]
    oos_improvement_ratio: float | None
    profit_factor_ratio: float | None
    drawdown_increase_r: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "verdict": self.verdict,
            "reason_codes": list(self.reason_codes),
            "oos_improvement_ratio": self.oos_improvement_ratio,
            "profit_factor_ratio": self.profit_factor_ratio,
            "drawdown_increase_r": self.drawdown_increase_r,
        }


@dataclass(frozen=True, kw_only=True)
class ChampionChallengerResult:
    status: str
    blocker_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    manifest: ExperimentManifest
    manifest_sha256: str
    result_sha256: str
    candidates: tuple[CandidateEvidence, ...]
    comparisons: tuple[ChallengerComparison, ...]
    selected_challenger_id: str | None
    promotion_applied: bool = False
    live_sim_activation_changed: bool = False
    operational_db_write_count: int = 0
    gateway_command_write_count: int = 0
    broker_call_count: int = 0

    @property
    def no_trading_side_effects(self) -> bool:
        return (
            not self.promotion_applied
            and not self.live_sim_activation_changed
            and self.operational_db_write_count == 0
            and self.gateway_command_write_count == 0
            and self.broker_call_count == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": CHAMPION_CHALLENGER_REPORT_FORMAT,
            "status": self.status,
            "blocker_reasons": list(self.blocker_reasons),
            "warnings": list(self.warnings),
            "identity": {
                "experiment_id": self.manifest.experiment_id,
                "manifest_sha256": self.manifest_sha256,
                "result_sha256": self.result_sha256,
                "changed_axis": self.manifest.changed_axis,
                "data_start": self.manifest.data_start,
                "data_end": self.manifest.data_end,
                "data_split_sha256": self.manifest.data_split_sha256,
                "execution_model_version": self.manifest.execution_model_version,
                "cost_model_version": self.manifest.cost_model_version,
            },
            "fast5_gate": {
                "status": self.manifest.fast5_status,
                "evidence_sha256": self.manifest.fast5_evidence_sha256,
            },
            "promotion": {
                "mode": self.manifest.promotion_mode,
                "selected_challenger_id": self.selected_challenger_id,
                "applied": self.promotion_applied,
                "live_sim_activation_changed": self.live_sim_activation_changed,
            },
            "policy": {
                "minimum_oos_expectancy_improvement_ratio": (
                    self.manifest.policy.minimum_oos_expectancy_improvement_ratio
                ),
                "minimum_profit_factor_ratio": self.manifest.policy.minimum_profit_factor_ratio,
                "maximum_drawdown_increase_r": (
                    self.manifest.policy.maximum_drawdown_increase_r
                ),
            },
            "candidates": [item.to_dict() for item in self.candidates],
            "comparisons": [item.to_dict() for item in self.comparisons],
            "safety": {
                "observe_only": True,
                "review_only": True,
                "promotion_applied": self.promotion_applied,
                "live_sim_activation_changed": self.live_sim_activation_changed,
                "operational_db_write_count": self.operational_db_write_count,
                "gateway_command_write_count": self.gateway_command_write_count,
                "broker_call_count": self.broker_call_count,
                "no_trading_side_effects": self.no_trading_side_effects,
            },
        }


def load_experiment_bundle(path: str | Path) -> ExperimentBundle:
    manifest_path = Path(path).expanduser().resolve()
    raw = manifest_path.read_bytes()
    parsed = json.loads(raw.decode("utf-8"))
    manifest = ExperimentManifest.from_mapping(_mapping(parsed, "experiment manifest"))
    base_dir = manifest_path.parent
    champion = _load_candidate(manifest.champion, base_dir=base_dir)
    challengers = tuple(
        _load_candidate(candidate, base_dir=base_dir) for candidate in manifest.challengers
    )
    return ExperimentBundle(
        manifest=manifest,
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        champion=champion,
        challengers=challengers,
    )


def evaluate_experiment(bundle: ExperimentBundle) -> ChampionChallengerResult:
    _validate_controlled_model_configs(bundle)
    champion = _candidate_evidence(bundle, bundle.champion, role="CHAMPION")
    challengers = tuple(
        _candidate_evidence(bundle, candidate, role="CHALLENGER")
        for candidate in bundle.challengers
    )
    comparisons = tuple(_compare(champion, candidate, bundle.manifest) for candidate in challengers)
    eligible = [
        comparison
        for comparison in comparisons
        if comparison.verdict == "MEETS_OFFLINE_PROMOTION_CRITERIA"
    ]
    selected = min(
        eligible,
        key=lambda item: (
            -(item.oos_improvement_ratio or 0.0),
            item.candidate_id,
        ),
        default=None,
    )

    blockers: list[str] = []
    warnings: list[str] = []
    for evidence in (champion, *challengers):
        blockers.extend(f"{evidence.candidate_id}:{reason}" for reason in evidence.blocker_reasons)
        warnings.extend(f"{evidence.candidate_id}:{reason}" for reason in evidence.warnings)
    if champion.shadow_live_canary_plan_count != 1:
        blockers.append("CHAMPION_LIVE_SIM_EVIDENCE_INVALID")
    if bundle.manifest.fast5_status != "PASS":
        blockers.append("FAST_5_NOT_QUALIFIED")

    if any(reason != "FAST_5_NOT_QUALIFIED" for reason in blockers):
        status = "BLOCKED"
    elif "FAST_5_NOT_QUALIFIED" in blockers:
        status = "BLOCKED_BY_FAST_5"
    elif selected is not None:
        status = "REVIEW_READY"
    else:
        status = "RETAIN_CHAMPION"

    deterministic = {
        "status": status,
        "blocker_reasons": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "manifest_sha256": bundle.manifest_sha256,
        "candidates": [item.to_dict() for item in (champion, *challengers)],
        "comparisons": [item.to_dict() for item in comparisons],
        "selected_challenger_id": selected.candidate_id if selected else None,
        "promotion_mode": "REVIEW_ONLY",
        "promotion_applied": False,
    }
    return ChampionChallengerResult(
        status=status,
        blocker_reasons=tuple(sorted(set(blockers))),
        warnings=tuple(sorted(set(warnings))),
        manifest=bundle.manifest,
        manifest_sha256=bundle.manifest_sha256,
        result_sha256=canonical_sha256(deterministic),
        candidates=(champion, *challengers),
        comparisons=comparisons,
        selected_challenger_id=selected.candidate_id if selected else None,
    )


def _load_candidate(spec: CandidateSpec, *, base_dir: Path) -> LoadedCandidate:
    return LoadedCandidate(
        spec=spec,
        profit_lab_report=_read_bound_json(
            spec.profit_lab_artifact.path,
            expected_sha256=spec.profit_lab_artifact.file_sha256,
            base_dir=base_dir,
        ),
        parallel_shadow_report=_read_bound_json(
            spec.parallel_shadow_artifact.path,
            expected_sha256=spec.parallel_shadow_artifact.file_sha256,
            base_dir=base_dir,
        ),
    )


def _validate_controlled_model_configs(bundle: ExperimentBundle) -> None:
    allowed_differences = {
        "ENTRY": set(),
        "STOP": {"stop_loss_pct", "trailing_activation_pct", "trailing_stop_pct"},
        "TAKE_PROFIT": {"take_profit_pct"},
        "THEME_THRESHOLD": set(),
        "MARKET_REGIME": set(),
    }[bundle.manifest.changed_axis]
    champion_profit = _unwrap(bundle.champion.profit_lab_report, "profit_lab")
    champion_config = dict(_mapping(champion_profit.get("config"), "Champion Profit Lab config"))
    _validate_candidate_model_config_pair(bundle.champion, champion_config)
    for challenger in bundle.challengers:
        challenger_profit = _unwrap(challenger.profit_lab_report, "profit_lab")
        challenger_config = dict(
            _mapping(challenger_profit.get("config"), "Challenger Profit Lab config")
        )
        _validate_candidate_model_config_pair(challenger, challenger_config)
        changed_fields = {
            key
            for key in set(champion_config) | set(challenger_config)
            if champion_config.get(key) != challenger_config.get(key)
        }
        if changed_fields - allowed_differences:
            raise ValueError(
                f"{challenger.spec.candidate_id} changes Profit Lab config outside changed_axis"
            )
        if allowed_differences and not changed_fields:
            raise ValueError(
                f"{challenger.spec.candidate_id} does not change the declared Profit Lab axis"
            )


def _validate_candidate_model_config_pair(
    candidate: LoadedCandidate,
    profit_config: Mapping[str, Any],
) -> None:
    shadow = _unwrap(candidate.parallel_shadow_report, "parallel_shadow")
    shadow_config = _mapping(shadow.get("config"), "Parallel Shadow config")
    if dict(profit_config) != dict(shadow_config):
        raise ValueError(
            f"{candidate.spec.candidate_id} Profit Lab and Parallel Shadow configs differ"
        )


def _read_bound_json(
    path_value: str,
    *,
    expected_sha256: str,
    base_dir: Path,
) -> Mapping[str, Any]:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    raw = path.resolve().read_bytes()
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(f"artifact file SHA-256 mismatch: {path_value}")
    return _mapping(json.loads(raw.decode("utf-8")), f"artifact {path_value}")


def _candidate_evidence(
    bundle: ExperimentBundle,
    loaded: LoadedCandidate,
    *,
    role: str,
) -> CandidateEvidence:
    manifest = bundle.manifest
    spec = loaded.spec
    profit = _unwrap(loaded.profit_lab_report, "profit_lab")
    shadow = _unwrap(loaded.parallel_shadow_report, "parallel_shadow")
    if profit.get("format") != PROFIT_LAB_REPORT_FORMAT:
        raise ValueError(f"{spec.candidate_id} Profit Lab report format is unsupported")
    if shadow.get("format") != PARALLEL_SHADOW_REPORT_FORMAT:
        raise ValueError(f"{spec.candidate_id} Parallel Shadow report format is unsupported")

    profit_identity = _mapping(profit.get("identity"), "Profit Lab identity")
    shadow_identity = _mapping(shadow.get("identity"), "Parallel Shadow identity")
    if str(profit_identity.get("commit_sha") or "").strip() != spec.git_commit_sha:
        raise ValueError(f"{spec.candidate_id} Profit Lab commit identity mismatch")
    if str(shadow_identity.get("commit_sha") or "").strip() != spec.git_commit_sha:
        raise ValueError(f"{spec.candidate_id} Parallel Shadow commit identity mismatch")
    profit_result_sha = require_sha256(
        profit.get("result_sha256"),
        f"{spec.candidate_id}.profit_lab_result_sha256",
    )
    shadow_result_sha = require_sha256(
        shadow.get("result_sha256"),
        f"{spec.candidate_id}.parallel_shadow_result_sha256",
    )

    profit_config = _mapping(profit.get("config"), "Profit Lab config")
    shadow_config = _mapping(shadow.get("config"), "Parallel Shadow config")
    for report_name, config in (("Profit Lab", profit_config), ("Parallel Shadow", shadow_config)):
        if config.get("execution_model_version") != manifest.execution_model_version:
            raise ValueError(f"{spec.candidate_id} {report_name} execution model mismatch")
        if config.get("cost_model_version") != manifest.cost_model_version:
            raise ValueError(f"{spec.candidate_id} {report_name} cost model mismatch")

    trades = profit.get("trades")
    if not isinstance(trades, list):
        raise ValueError(f"{spec.candidate_id} Profit Lab trades must be a list")
    data_dates, split_sha256 = _data_identity(trades)
    if data_dates and (data_dates[0] != manifest.data_start or data_dates[-1] != manifest.data_end):
        raise ValueError(f"{spec.candidate_id} Profit Lab data range mismatch")
    if split_sha256 != manifest.data_split_sha256:
        raise ValueError(f"{spec.candidate_id} Profit Lab data split mismatch")

    blockers: list[str] = []
    warnings: list[str] = []
    if not data_dates:
        blockers.append("EMPTY_PROFIT_LAB_DATA_RANGE")
    if profit.get("status") != "PASS" or profit.get("qualification") != "ALPHA_QUALIFIED":
        blockers.append("ALPHA_NOT_QUALIFIED")
    if not bool(profit.get("cost_model_complete")):
        blockers.append("COST_MODEL_INCOMPLETE")
    warnings.extend(str(item) for item in profit.get("warnings") or ())
    _append_safety_blockers(blockers, profit, prefix="PROFIT_LAB")
    source_quality = _mapping(profit.get("source_quality") or {}, "source_quality")
    if not bool(source_quality.get("alpha_qualified")):
        blockers.append("SOURCE_ALPHA_NOT_QUALIFIED")
    if int(source_quality.get("point_in_time_violation_count") or 0):
        blockers.append("POINT_IN_TIME_VIOLATION")

    shadow_blockers = [str(item) for item in shadow.get("blocker_reasons") or ()]
    shadow_status = str(shadow.get("status") or "")
    if shadow_status == "BLOCKED" or shadow_blockers:
        blockers.append("PARALLEL_SHADOW_BLOCKED")
    if not bool(shadow.get("cost_model_complete")):
        blockers.append("SHADOW_COST_MODEL_INCOMPLETE")
    _append_safety_blockers(blockers, shadow, prefix="PARALLEL_SHADOW")
    shadow_metrics = _mapping(shadow.get("metrics"), "Parallel Shadow metrics")
    for metric_name in (
        "duplicate_shadow_plan_count",
        "shadow_plan_coverage_gap_count",
        "comparison_linkage_gap_count",
        "ai_influenced_plan_count",
    ):
        if int(shadow_metrics.get(metric_name) or 0):
            blockers.append(f"SHADOW_{metric_name.upper()}")
    shadow_warnings = {str(item) for item in shadow.get("warnings") or ()}
    allowed_shadow_warnings = {"NO_LIVE_CANARY_COMPARISON"} if role == "CHALLENGER" else set()
    if shadow_warnings - allowed_shadow_warnings:
        blockers.append("PARALLEL_SHADOW_WARNING_NOT_ALLOWED")
    if role == "CHAMPION" and shadow_status != "PASS":
        blockers.append("CHAMPION_PARALLEL_SHADOW_NOT_PASS")
    if role == "CHALLENGER" and shadow_status not in {"PASS", "WARN"}:
        blockers.append("CHALLENGER_PARALLEL_SHADOW_INVALID")
    warnings.extend(shadow_warnings)

    metrics = _mapping(profit.get("metrics"), "Profit Lab metrics")
    grouped = _mapping(profit.get("grouped_metrics"), "Profit Lab grouped_metrics")
    splits = _mapping(grouped.get("dataset_split"), "Profit Lab dataset_split metrics")
    validation = _optional_metric(splits, "VALIDATION", "net_expectancy")
    test = _optional_metric(splits, "TEST", "net_expectancy")
    profit_factor = _optional_number(metrics.get("profit_factor"))
    max_drawdown = _optional_number(metrics.get("max_drawdown_r"))
    if any(value is None for value in (validation, test, profit_factor, max_drawdown)):
        blockers.append("REQUIRED_COMPARISON_METRIC_MISSING")
    elif (
        validation is not None
        and test is not None
        and profit_factor is not None
        and max_drawdown is not None
        and (
            validation <= 0
            or test <= 0
            or profit_factor <= 0
            or max_drawdown < 0
        )
    ):
        blockers.append("QUALIFICATION_METRIC_INCONSISTENT")
    live_canary_count = int(shadow_metrics.get("live_canary_plan_count") or 0)
    if role == "CHALLENGER" and live_canary_count:
        blockers.append("CHALLENGER_LIVE_SIM_EVIDENCE_NOT_ALLOWED")

    return CandidateEvidence(
        candidate_id=spec.candidate_id,
        role=role,
        qualification_verdict="EVIDENCE_BLOCKED" if blockers else "EVALUABLE",
        blocker_reasons=tuple(sorted(set(blockers))),
        warnings=tuple(sorted(set(warnings))),
        git_commit_sha=spec.git_commit_sha,
        strategy_config_sha256=spec.strategy_config_sha256,
        risk_config_sha256=spec.risk_config_sha256,
        entry_config_sha256=spec.entry_config_sha256,
        axis_config_sha256=spec.axis_config_sha256,
        profit_lab_file_sha256=spec.profit_lab_artifact.file_sha256,
        profit_lab_result_sha256=profit_result_sha,
        parallel_shadow_file_sha256=spec.parallel_shadow_artifact.file_sha256,
        parallel_shadow_result_sha256=shadow_result_sha,
        validation_net_expectancy=validation,
        test_net_expectancy=test,
        profit_factor=profit_factor,
        max_drawdown_r=max_drawdown,
        closed_trade_count=int(metrics.get("closed_trade_count") or 0),
        distinct_trade_dates=int(metrics.get("distinct_trade_dates") or 0),
        shadow_live_canary_plan_count=live_canary_count,
    )


def _compare(
    champion: CandidateEvidence,
    challenger: CandidateEvidence,
    manifest: ExperimentManifest,
) -> ChallengerComparison:
    if champion.blocker_reasons or challenger.blocker_reasons:
        return ChallengerComparison(
            candidate_id=challenger.candidate_id,
            verdict="EVIDENCE_BLOCKED",
            reason_codes=("CANDIDATE_EVIDENCE_BLOCKED",),
            oos_improvement_ratio=None,
            profit_factor_ratio=None,
            drawdown_increase_r=None,
        )
    champion_validation = float(champion.validation_net_expectancy or 0.0)
    champion_test = float(champion.test_net_expectancy or 0.0)
    challenger_validation = float(challenger.validation_net_expectancy or 0.0)
    challenger_test = float(challenger.test_net_expectancy or 0.0)
    validation_ratio = _improvement_ratio(champion_validation, challenger_validation)
    test_ratio = _improvement_ratio(champion_test, challenger_test)
    oos_ratio = min(validation_ratio, test_ratio)
    profit_factor_ratio = float(challenger.profit_factor or 0.0) / float(
        champion.profit_factor or 1.0
    )
    drawdown_increase = float(challenger.max_drawdown_r or 0.0) - float(
        champion.max_drawdown_r or 0.0
    )
    reasons: list[str] = []
    if oos_ratio < manifest.policy.minimum_oos_expectancy_improvement_ratio:
        reasons.append("OOS_EXPECTANCY_IMPROVEMENT_BELOW_MINIMUM")
    if profit_factor_ratio < manifest.policy.minimum_profit_factor_ratio:
        reasons.append("PROFIT_FACTOR_REGRESSION")
    if drawdown_increase > manifest.policy.maximum_drawdown_increase_r:
        reasons.append("DRAWDOWN_REGRESSION")
    return ChallengerComparison(
        candidate_id=challenger.candidate_id,
        verdict="RETAIN_CHAMPION" if reasons else "MEETS_OFFLINE_PROMOTION_CRITERIA",
        reason_codes=tuple(reasons),
        oos_improvement_ratio=_round(oos_ratio),
        profit_factor_ratio=_round(profit_factor_ratio),
        drawdown_increase_r=_round(drawdown_increase),
    )


def _data_identity(trades: Sequence[object]) -> tuple[list[str], str]:
    split_by_date: dict[str, str] = {}
    for item in trades:
        trade = _mapping(item, "Profit Lab trade")
        trade_date = str(trade.get("trade_date") or "")
        split = str(trade.get("dataset_split") or "").upper()
        if not trade_date or split not in {"TRAIN", "VALIDATION", "TEST"}:
            raise ValueError("Profit Lab trade requires valid trade_date and dataset_split")
        existing = split_by_date.setdefault(trade_date, split)
        if existing != split:
            raise ValueError("Profit Lab trade date has inconsistent dataset splits")
    identity = [
        {"trade_date": trade_date, "dataset_split": split_by_date[trade_date]}
        for trade_date in sorted(split_by_date)
    ]
    return sorted(split_by_date), canonical_sha256(identity)


def _append_safety_blockers(
    blockers: list[str],
    report: Mapping[str, Any],
    *,
    prefix: str,
) -> None:
    safety = _mapping(report.get("safety") or {}, f"{prefix} safety")
    if not bool(safety.get("no_trading_side_effects")):
        blockers.append(f"{prefix}_TRADING_SIDE_EFFECT_NOT_ZERO")
    required_fields = {
        "PROFIT_LAB": (
            "point_in_time_violation_count",
            "operational_db_write_count",
            "gateway_command_write_count",
            "live_sim_write_count",
            "dry_run_write_count",
        ),
        "PARALLEL_SHADOW": (
            "operational_db_write_count",
            "gateway_command_write_count",
            "live_sim_write_count",
            "broker_call_count",
        ),
    }[prefix]
    for field_name in required_fields:
        if field_name not in safety:
            blockers.append(f"{prefix}_{field_name.upper()}_EVIDENCE_MISSING")
            continue
        if int(safety.get(field_name) or 0):
            blockers.append(f"{prefix}_{field_name.upper()}_NOT_ZERO")


def _unwrap(report: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    nested = report.get(key)
    return _mapping(nested, key) if nested is not None else report


def _optional_metric(
    splits: Mapping[str, Any],
    split_name: str,
    metric_name: str,
) -> float | None:
    item = splits.get(split_name)
    if not isinstance(item, Mapping):
        return None
    return _optional_number(item.get(metric_name))


def _optional_number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _improvement_ratio(baseline: float, candidate: float) -> float:
    if baseline <= 0:
        return float("-inf")
    return (candidate - baseline) / abs(baseline)


def _round(value: float) -> float:
    return round(float(value), 10)


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value
