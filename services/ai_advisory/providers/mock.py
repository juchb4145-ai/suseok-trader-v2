from __future__ import annotations

import json
from collections.abc import Mapping
from time import perf_counter
from typing import Any

from domain.broker.utils import normalize_value

from services.ai_advisory.models import (
    AiProviderRawResult,
    CandidatePrompt,
    CandidateScoringContext,
)
from services.config import Settings


class MockCandidateScorerProvider:
    name = "mock"

    def score_candidates(
        self,
        context: CandidateScoringContext,
        *,
        prompt: CandidatePrompt | None = None,
        settings: Settings,
    ) -> AiProviderRawResult:
        started = perf_counter()
        parsed = self._score_context(context.to_dict(), settings=settings)
        raw_text = json.dumps(normalize_value(parsed), ensure_ascii=False, sort_keys=True)
        return AiProviderRawResult(
            provider=self.name,
            model=settings.ai_candidate_scorer_model,
            status="COMPLETED",
            raw_text=raw_text,
            parsed_json=parsed,
            latency_ms=_elapsed_ms(started),
            external_call_enabled=False,
            external_call_attempted=False,
            attempts=1,
        )

    def score(self, context: Mapping[str, Any], *, settings: Settings) -> object:
        response = self._score_context(context, settings=settings)
        return json.dumps(normalize_value(response), ensure_ascii=False, sort_keys=True)

    def _score_context(
        self,
        context: Mapping[str, Any],
        *,
        settings: Settings,
    ) -> dict[str, Any]:
        candidates = list(context.get("candidates", []))
        scored = [_score_candidate(candidate, settings) for candidate in candidates]
        scored.sort(key=lambda item: (-int(item["score"]), -int(item["confidence"]), item["code"]))
        selected = [
            item["code"]
            for item in scored
            if int(item["score"]) >= settings.ai_candidate_scorer_min_score
            and int(item["confidence"]) >= settings.ai_candidate_scorer_min_confidence
        ][:3]
        return {
            "selected": selected,
            "analysis": {item["code"]: item["analysis"] for item in scored},
            "score": {item["code"]: item["score"] for item in scored},
            "confidence": {item["code"]: item["confidence"] for item in scored},
            "risk_reward": {item["code"]: item["risk_reward"] for item in scored},
            "candidate_flags": {item["code"]: item["flags"] for item in scored},
            "avoid": {
                item["code"]: item["avoid_reason"]
                for item in scored
                if item.get("avoid_reason")
            },
            "summary": _summary(selected, scored),
            "no_trade_reason": None if selected else "기준을 만족하는 advisory 후보가 없습니다.",
        }


def _score_candidate(candidate: Mapping[str, Any], settings: Settings) -> dict[str, Any]:
    code = str(candidate.get("code") or "")
    score = 50.0
    confidence = 55.0
    flags: list[str] = []
    avoid_reason = ""

    if candidate.get("order_plan_status") == "PLAN_READY":
        score += 18
        confidence += 8
        flags.append("PLAN_READY")
    elif candidate.get("order_plan_status") == "WAIT_RETRY":
        score += 3
        flags.append("WAIT_RETRY")
    else:
        score -= 8
        flags.append("DATA_WAIT")

    if candidate.get("strategy_status") == "MATCHED_OBSERVATION":
        score += 10
        confidence += 6
        flags.append("STRATEGY_MATCHED")
    if candidate.get("risk_status") == "OBSERVE_PASS":
        score += 8
        confidence += 6
        flags.append("RISK_PASS")
    elif candidate.get("risk_status") == "OBSERVE_BLOCK":
        score -= 25
        confidence -= 5
        flags.append("RISK_BLOCK")
        avoid_reason = "리스크 관찰 상태가 OBSERVE_BLOCK입니다."
    elif candidate.get("risk_status") == "OBSERVE_CAUTION":
        score -= 8
        flags.append("RISK_CAUTION")

    strategy_score = _number(candidate.get("strategy_score"))
    strategy_confidence = _number(candidate.get("strategy_confidence"))
    score += min(max(strategy_score, 0.0), 100.0) * 0.08
    confidence += min(max(strategy_confidence, 0.0), 100.0) * 0.05

    if candidate.get("entry_timing_state") in {"GOOD_PULLBACK", "PULLBACK_RECLAIM", "VWAP_RECLAIM"}:
        score += 5
        flags.append(str(candidate.get("entry_timing_state")))
    if candidate.get("price_location_state") in {"CHASE_NEAR_HIGH", "OVERHEATED"}:
        score -= 8
        flags.append(str(candidate.get("price_location_state")))
        avoid_reason = avoid_reason or "추격 또는 과열 가격 위치입니다."
    if bool(candidate.get("stale")):
        score -= 12
        confidence -= 12
        flags.append("STALE")
        avoid_reason = avoid_reason or "시장 데이터가 stale 상태입니다."
    if bool(candidate.get("vi_active")):
        score -= 20
        flags.append("VI_ACTIVE")
        avoid_reason = avoid_reason or "VI 활성 또는 변동성 주의 후보입니다."
    if bool(candidate.get("upper_limit_near")):
        score -= 12
        flags.append("UPPER_LIMIT_NEAR")
        avoid_reason = avoid_reason or "상한가 근접으로 추격 리스크가 큽니다."

    score_int = _bounded_int(score)
    confidence_int = _bounded_int(confidence)
    risk_reward = _risk_reward(score_int, candidate)
    analysis = _analysis(candidate, score_int, confidence_int, flags)
    return {
        "code": code,
        "score": score_int,
        "confidence": confidence_int,
        "flags": flags or ["OBSERVE"],
        "analysis": analysis,
        "avoid_reason": avoid_reason,
        "risk_reward": risk_reward,
    }


def _risk_reward(score: int, candidate: Mapping[str, Any]) -> dict[str, Any]:
    if score >= 80:
        stop_loss = 2.5
        take_profit = 6.0
        trailing = 2.0
        max_hold = 1800
    elif score >= 70:
        stop_loss = 2.2
        take_profit = 5.0
        trailing = 1.8
        max_hold = 1500
    else:
        stop_loss = 1.8
        take_profit = 3.2
        trailing = 1.5
        max_hold = 900
    if candidate.get("risk_status") == "OBSERVE_CAUTION":
        stop_loss -= 0.3
        take_profit -= 0.5
        max_hold = min(max_hold, 900)
    return {
        "stop_loss_pct": stop_loss,
        "take_profit_pct": take_profit,
        "trailing_stop_pct": trailing,
        "max_hold_sec": max_hold,
    }


def _analysis(
    candidate: Mapping[str, Any],
    score: int,
    confidence: int,
    flags: list[str],
) -> str:
    name = candidate.get("name") or candidate.get("code")
    theme = candidate.get("theme_name") or "테마 미확인"
    return (
        f"{name}는 {theme} 후보이며 order/status, strategy, risk, entry timing을 "
        f"종합한 mock advisory score {score}, confidence {confidence}입니다. "
        f"주요 플래그: {', '.join(flags) if flags else 'OBSERVE'}."
    )


def _summary(selected: list[str], scored: list[Mapping[str, Any]]) -> str:
    if not scored:
        return "평가할 후보가 없어 selected는 빈 배열입니다."
    top = scored[0]
    if selected:
        return (
            f"mock advisory 기준 상위 후보는 {', '.join(selected)}이며 "
            f"최상위 score는 {top['score']}입니다."
        )
    return "mock advisory 기준 score/confidence 임계값을 동시에 만족하는 후보가 없습니다."


def _bounded_int(value: float) -> int:
    return min(max(int(round(value)), 0), 100)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return 0.0
    return float(value)


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000.0, 3)
