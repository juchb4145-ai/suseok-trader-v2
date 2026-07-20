from __future__ import annotations

from domain.exit.policy import (
    ExitPolicyConfig,
    ExitTriggerType,
    LongPositionSnapshot,
    evaluate_long_exit_policy,
)
from services.entry_timing.tick_size import price_tick_distance, subtract_ticks


def test_shared_exit_policy_covers_stop_take_trailing_max_hold_and_eod() -> None:
    policy = _policy()

    stop = _decision(current=96, highest=100, observed="2026-07-20T00:01:00Z", policy=policy)
    take = _decision(current=106, highest=106, observed="2026-07-20T00:01:00Z", policy=policy)
    trailing = _decision(
        current=107,
        highest=110,
        observed="2026-07-20T00:01:00Z",
        policy=policy,
    )
    max_hold = _decision(
        current=100,
        highest=100,
        observed="2026-07-20T00:20:00Z",
        policy=policy,
    )
    eod = _decision(
        current=100,
        highest=100,
        observed="2026-07-20T06:15:00Z",
        policy=policy,
    )

    assert stop.primary_trigger.trigger_type is ExitTriggerType.STOP_LOSS
    assert take.primary_trigger.trigger_type is ExitTriggerType.TAKE_PROFIT
    assert trailing.primary_trigger.trigger_type is ExitTriggerType.TRAILING_STOP
    assert max_hold.primary_trigger.trigger_type is ExitTriggerType.MAX_HOLD
    assert eod.primary_trigger.trigger_type is ExitTriggerType.EOD_FLATTEN
    assert all(decision.close_only for decision in (stop, take, trailing, max_hold, eod))
    assert all(not decision.allow_short for decision in (stop, take, trailing, max_hold, eod))


def test_tick_subtraction_handles_krx_price_band_boundaries() -> None:
    assert subtract_ticks(2_000, 1) == 1_999
    assert subtract_ticks(5_000, 1) == 4_995
    assert subtract_ticks(20_000, 1) == 19_990
    assert subtract_ticks(500_000, 1) == 499_500
    assert subtract_ticks(100, 2) == 98
    assert price_tick_distance(1_999, 2_000) == 1
    assert price_tick_distance(5_000, 4_995) == -1
    assert price_tick_distance(100, 105) == 5


def _decision(*, current: float, highest: float, observed: str, policy: ExitPolicyConfig):
    return evaluate_long_exit_policy(
        LongPositionSnapshot(
            entry_price=100,
            current_price=current,
            highest_price=highest,
            quantity=1,
            opened_at="2026-07-20T00:00:00Z",
            observed_at=observed,
        ),
        policy,
    )


def _policy() -> ExitPolicyConfig:
    return ExitPolicyConfig(
        stop_loss_pct=3,
        take_profit_pct=5,
        trailing_activation_pct=2,
        trailing_stop_pct=2.5,
        minimum_hold_sec=30,
        maximum_hold_sec=600,
        eod_flatten_enabled=True,
        eod_flatten_time="15:15:00",
    )
