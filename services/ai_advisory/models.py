from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import normalize_value


@dataclass(frozen=True, kw_only=True)
class RiskRewardBounds:
    stop_loss_min: float
    stop_loss_max: float
    take_profit_min: float
    take_profit_max: float
    trailing_min: float
    trailing_max: float
    max_hold_min_sec: int
    max_hold_max_sec: int


@dataclass(frozen=True, kw_only=True)
class CandidateScoringContext:
    trade_date: str | None
    candidates: Sequence[Mapping[str, Any]]
    market_summary: Mapping[str, Any] = field(default_factory=dict)
    risk_summary: Mapping[str, Any] = field(default_factory=dict)
    recent_performance: Mapping[str, Any] = field(default_factory=dict)
    warnings: Sequence[str] = field(default_factory=tuple)
    truncated: bool = False
    advisory_only: bool = True
    no_order_side_effects: bool = True
    account_redacted: bool = True

    def to_dict(self) -> dict[str, Any]:
        return normalize_value(
            {
                "trade_date": self.trade_date,
                "candidate_count": len(self.candidates),
                "candidates": list(self.candidates),
                "market_summary": dict(self.market_summary),
                "risk_summary": dict(self.risk_summary),
                "recent_performance": dict(self.recent_performance),
                "warnings": list(self.warnings),
                "truncated": self.truncated,
                "advisory_only": True,
                "no_order_side_effects": True,
                "account_redacted": True,
            }
        )


@dataclass(frozen=True, kw_only=True)
class CandidatePrompt:
    system_prompt: str
    user_prompt: str
    prompt_hash: str
    input_chars: int
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "prompt_hash": self.prompt_hash,
            "input_chars": self.input_chars,
            "truncated": self.truncated,
        }


@dataclass(frozen=True, kw_only=True)
class ValidatedRiskReward:
    stop_loss_pct: float
    take_profit_pct: float
    trailing_stop_pct: float
    max_hold_sec: int
    clamped: bool = False
    clamp_reason_codes: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stop_loss_pct": self.stop_loss_pct,
            "take_profit_pct": self.take_profit_pct,
            "trailing_stop_pct": self.trailing_stop_pct,
            "max_hold_sec": self.max_hold_sec,
            "clamped": self.clamped,
            "clamp_reason_codes": list(self.clamp_reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class ValidatedAdvisory:
    selected: Sequence[str]
    analysis: Mapping[str, str]
    score: Mapping[str, int]
    confidence: Mapping[str, int]
    risk_reward: Mapping[str, ValidatedRiskReward]
    candidate_flags: Mapping[str, Sequence[str]]
    avoid: Mapping[str, str]
    summary: str
    no_trade_reason: str | None
    warnings: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return normalize_value(
            {
                "selected": list(self.selected),
                "analysis": dict(self.analysis),
                "score": dict(self.score),
                "confidence": dict(self.confidence),
                "risk_reward": {
                    code: suggestion.to_dict() for code, suggestion in self.risk_reward.items()
                },
                "candidate_flags": {
                    code: list(flags) for code, flags in self.candidate_flags.items()
                },
                "avoid": dict(self.avoid),
                "summary": self.summary,
                "no_trade_reason": self.no_trade_reason,
                "warnings": list(self.warnings),
            }
        )


@dataclass(frozen=True, kw_only=True)
class AiCandidateScoringResult:
    ok: bool
    run_id: str
    status: str
    trade_date: str | None
    provider: str
    model: str
    candidate_count: int = 0
    selected_count: int = 0
    prompt_hash: str | None = None
    raw_response_hash: str | None = None
    summary: str | None = None
    no_trade_reason: str | None = None
    error_message: str | None = None
    validation_error: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)
    prompt: Mapping[str, Any] = field(default_factory=dict)
    advisory: Mapping[str, Any] = field(default_factory=dict)
    dry_run: bool = False
    advisory_only: bool = True
    no_order_side_effects: bool = True
    live_sim_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return normalize_value(
            {
                "ok": self.ok,
                "run_id": self.run_id,
                "status": self.status,
                "trade_date": self.trade_date,
                "provider": self.provider,
                "model": self.model,
                "candidate_count": self.candidate_count,
                "selected_count": self.selected_count,
                "prompt_hash": self.prompt_hash,
                "raw_response_hash": self.raw_response_hash,
                "summary": self.summary,
                "no_trade_reason": self.no_trade_reason,
                "error_message": self.error_message,
                "validation_error": self.validation_error,
                "context": dict(self.context),
                "prompt": dict(self.prompt),
                "advisory": dict(self.advisory),
                "dry_run": self.dry_run,
                "advisory_only": True,
                "no_order_side_effects": True,
                "live_sim_only": True,
            }
        )

