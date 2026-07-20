from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from domain.broker.utils import require_non_empty_str

CHAMPION_CHALLENGER_INPUT_FORMAT = "champion-challenger-experiment/v1"
CHAMPION_CHALLENGER_REPORT_FORMAT = "champion-challenger-report/v1"
EXPERIMENT_AXES = (
    "ENTRY",
    "STOP",
    "TAKE_PROFIT",
    "THEME_THRESHOLD",
    "MARKET_REGIME",
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{7,64}$")


def require_sha256(value: object, field_name: str) -> str:
    normalized = require_non_empty_str(value, field_name)
    if normalized != normalized.lower() or not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return normalized


@dataclass(frozen=True, kw_only=True)
class ArtifactReference:
    path: str
    file_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", require_non_empty_str(self.path, "artifact.path"))
        object.__setattr__(
            self,
            "file_sha256",
            require_sha256(self.file_sha256, "artifact.file_sha256"),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ArtifactReference:
        return cls(
            path=str(value.get("path") or ""),
            file_sha256=str(value.get("file_sha256") or ""),
        )


@dataclass(frozen=True, kw_only=True)
class CandidateSpec:
    candidate_id: str
    git_commit_sha: str
    strategy_config_sha256: str
    risk_config_sha256: str
    entry_config_sha256: str
    axis_config_sha256: Mapping[str, str]
    profit_lab_artifact: ArtifactReference
    parallel_shadow_artifact: ArtifactReference

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_id",
            require_non_empty_str(self.candidate_id, "candidate_id"),
        )
        commit_sha = require_non_empty_str(self.git_commit_sha, "git_commit_sha")
        if commit_sha != commit_sha.lower() or not _GIT_SHA_PATTERN.fullmatch(commit_sha):
            raise ValueError("git_commit_sha must be a lowercase hexadecimal Git SHA")
        object.__setattr__(self, "git_commit_sha", commit_sha)
        for field_name in (
            "strategy_config_sha256",
            "risk_config_sha256",
            "entry_config_sha256",
        ):
            object.__setattr__(
                self,
                field_name,
                require_sha256(getattr(self, field_name), field_name),
            )
        axes = {
            str(key).strip().upper(): str(value)
            for key, value in self.axis_config_sha256.items()
        }
        if set(axes) != set(EXPERIMENT_AXES):
            raise ValueError("axis_config_sha256 must contain every supported experiment axis")
        object.__setattr__(
            self,
            "axis_config_sha256",
            {
                axis: require_sha256(axes[axis], f"axis_config_sha256.{axis}")
                for axis in EXPERIMENT_AXES
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "git_commit_sha": self.git_commit_sha,
            "strategy_config_sha256": self.strategy_config_sha256,
            "risk_config_sha256": self.risk_config_sha256,
            "entry_config_sha256": self.entry_config_sha256,
            "axis_config_sha256": dict(self.axis_config_sha256),
            "profit_lab_artifact": asdict(self.profit_lab_artifact),
            "parallel_shadow_artifact": asdict(self.parallel_shadow_artifact),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CandidateSpec:
        return cls(
            candidate_id=str(value.get("candidate_id") or ""),
            git_commit_sha=str(value.get("git_commit_sha") or ""),
            strategy_config_sha256=str(value.get("strategy_config_sha256") or ""),
            risk_config_sha256=str(value.get("risk_config_sha256") or ""),
            entry_config_sha256=str(value.get("entry_config_sha256") or ""),
            axis_config_sha256=_mapping(value.get("axis_config_sha256"), "axis_config_sha256"),
            profit_lab_artifact=ArtifactReference.from_mapping(
                _mapping(value.get("profit_lab_artifact"), "profit_lab_artifact")
            ),
            parallel_shadow_artifact=ArtifactReference.from_mapping(
                _mapping(value.get("parallel_shadow_artifact"), "parallel_shadow_artifact")
            ),
        )


@dataclass(frozen=True, kw_only=True)
class PromotionPolicy:
    minimum_oos_expectancy_improvement_ratio: float = 0.05
    minimum_profit_factor_ratio: float = 1.0
    maximum_drawdown_increase_r: float = 0.0

    def __post_init__(self) -> None:
        raw_values = (
            self.minimum_oos_expectancy_improvement_ratio,
            self.minimum_profit_factor_ratio,
            self.maximum_drawdown_increase_r,
        )
        if any(isinstance(value, bool) for value in raw_values):
            raise ValueError("promotion policy values must be finite numbers")
        minimum_expectancy = float(self.minimum_oos_expectancy_improvement_ratio)
        minimum_profit_factor = float(self.minimum_profit_factor_ratio)
        maximum_drawdown = float(self.maximum_drawdown_increase_r)
        if not all(
            math.isfinite(value)
            for value in (minimum_expectancy, minimum_profit_factor, maximum_drawdown)
        ):
            raise ValueError("promotion policy values must be finite numbers")
        if not 0 <= minimum_expectancy <= 10:
            raise ValueError("minimum_oos_expectancy_improvement_ratio must be in [0, 10]")
        if minimum_profit_factor < 1:
            raise ValueError("minimum_profit_factor_ratio must be >= 1")
        if maximum_drawdown < 0:
            raise ValueError("maximum_drawdown_increase_r must be >= 0")
        object.__setattr__(self, "minimum_oos_expectancy_improvement_ratio", minimum_expectancy)
        object.__setattr__(self, "minimum_profit_factor_ratio", minimum_profit_factor)
        object.__setattr__(self, "maximum_drawdown_increase_r", maximum_drawdown)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PromotionPolicy:
        return cls(**dict(value))


@dataclass(frozen=True, kw_only=True)
class ExperimentManifest:
    experiment_id: str
    changed_axis: str
    data_start: str
    data_end: str
    data_split_sha256: str
    execution_model_version: str
    cost_model_version: str
    fast5_status: str
    fast5_evidence_sha256: str
    champion: CandidateSpec
    challengers: tuple[CandidateSpec, ...]
    policy: PromotionPolicy = PromotionPolicy()
    promotion_mode: str = "REVIEW_ONLY"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "experiment_id",
            require_non_empty_str(self.experiment_id, "experiment_id"),
        )
        changed_axis = require_non_empty_str(self.changed_axis, "changed_axis").upper()
        if changed_axis not in EXPERIMENT_AXES:
            raise ValueError(f"changed_axis must be one of {', '.join(EXPERIMENT_AXES)}")
        object.__setattr__(self, "changed_axis", changed_axis)
        start = date.fromisoformat(require_non_empty_str(self.data_start, "data_start"))
        end = date.fromisoformat(require_non_empty_str(self.data_end, "data_end"))
        if start > end:
            raise ValueError("data_start must be on or before data_end")
        object.__setattr__(self, "data_start", start.isoformat())
        object.__setattr__(self, "data_end", end.isoformat())
        object.__setattr__(
            self,
            "data_split_sha256",
            require_sha256(self.data_split_sha256, "data_split_sha256"),
        )
        for field_name in ("execution_model_version", "cost_model_version"):
            object.__setattr__(
                self,
                field_name,
                require_non_empty_str(getattr(self, field_name), field_name),
            )
        fast5_status = require_non_empty_str(self.fast5_status, "fast5_status").upper()
        if fast5_status not in {"BLOCKED", "PASS"}:
            raise ValueError("fast5_status must be BLOCKED or PASS")
        object.__setattr__(self, "fast5_status", fast5_status)
        evidence_sha = str(self.fast5_evidence_sha256 or "").strip()
        if evidence_sha:
            evidence_sha = require_sha256(evidence_sha, "fast5_evidence_sha256")
        if fast5_status == "PASS" and not evidence_sha:
            raise ValueError("FAST-5 PASS requires fast5_evidence_sha256")
        object.__setattr__(self, "fast5_evidence_sha256", evidence_sha)
        if not 1 <= len(self.challengers) <= 2:
            raise ValueError("an experiment requires one or two challengers")
        ids = [self.champion.candidate_id, *(item.candidate_id for item in self.challengers)]
        if len(ids) != len(set(ids)):
            raise ValueError("candidate_id must be unique within an experiment")
        promotion_mode = require_non_empty_str(self.promotion_mode, "promotion_mode").upper()
        if promotion_mode != "REVIEW_ONLY":
            raise ValueError("promotion_mode must remain REVIEW_ONLY")
        object.__setattr__(self, "promotion_mode", promotion_mode)
        self._validate_single_axis_contract()

    def _validate_single_axis_contract(self) -> None:
        config_field_by_axis = {
            "ENTRY": "entry_config_sha256",
            "STOP": "risk_config_sha256",
            "TAKE_PROFIT": "risk_config_sha256",
            "THEME_THRESHOLD": "strategy_config_sha256",
            "MARKET_REGIME": "strategy_config_sha256",
        }
        allowed_config_field = config_field_by_axis[self.changed_axis]
        config_fields = (
            "strategy_config_sha256",
            "risk_config_sha256",
            "entry_config_sha256",
        )
        for challenger in self.challengers:
            if challenger.git_commit_sha != self.champion.git_commit_sha:
                raise ValueError("all candidates must use the same git_commit_sha")
            changed_axes = {
                axis
                for axis in EXPERIMENT_AXES
                if challenger.axis_config_sha256[axis]
                != self.champion.axis_config_sha256[axis]
            }
            if changed_axes != {self.changed_axis}:
                raise ValueError(
                    "each challenger must differ from champion on exactly changed_axis"
                )
            for field_name in config_fields:
                same = getattr(challenger, field_name) == getattr(self.champion, field_name)
                if field_name == allowed_config_field and same:
                    raise ValueError(f"{field_name} must change with {self.changed_axis}")
                if field_name != allowed_config_field and not same:
                    raise ValueError(f"{field_name} must not change with {self.changed_axis}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": CHAMPION_CHALLENGER_INPUT_FORMAT,
            "experiment_id": self.experiment_id,
            "changed_axis": self.changed_axis,
            "data_start": self.data_start,
            "data_end": self.data_end,
            "data_split_sha256": self.data_split_sha256,
            "execution_model_version": self.execution_model_version,
            "cost_model_version": self.cost_model_version,
            "fast5_status": self.fast5_status,
            "fast5_evidence_sha256": self.fast5_evidence_sha256,
            "promotion_mode": self.promotion_mode,
            "policy": asdict(self.policy),
            "champion": self.champion.to_dict(),
            "challengers": [item.to_dict() for item in self.challengers],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ExperimentManifest:
        if value.get("format") != CHAMPION_CHALLENGER_INPUT_FORMAT:
            raise ValueError("unsupported Champion/Challenger manifest format")
        challengers = value.get("challengers")
        if not isinstance(challengers, list):
            raise ValueError("challengers must be a list")
        return cls(
            experiment_id=str(value.get("experiment_id") or ""),
            changed_axis=str(value.get("changed_axis") or ""),
            data_start=str(value.get("data_start") or ""),
            data_end=str(value.get("data_end") or ""),
            data_split_sha256=str(value.get("data_split_sha256") or ""),
            execution_model_version=str(value.get("execution_model_version") or ""),
            cost_model_version=str(value.get("cost_model_version") or ""),
            fast5_status=str(value.get("fast5_status") or ""),
            fast5_evidence_sha256=str(value.get("fast5_evidence_sha256") or ""),
            promotion_mode=str(value.get("promotion_mode") or "REVIEW_ONLY"),
            policy=PromotionPolicy.from_mapping(_mapping(value.get("policy") or {}, "policy")),
            champion=CandidateSpec.from_mapping(_mapping(value.get("champion"), "champion")),
            challengers=tuple(
                CandidateSpec.from_mapping(_mapping(item, "challenger")) for item in challengers
            ),
        )


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value
