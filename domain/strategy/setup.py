from __future__ import annotations

from enum import StrEnum


class StrategySetupType(StrEnum):
    THEME_LEADER_PULLBACK = "THEME_LEADER_PULLBACK"
    VWAP_RECLAIM = "VWAP_RECLAIM"
    BREAKOUT_RETEST = "BREAKOUT_RETEST"
    THEME_FOLLOWER_EXPANSION = "THEME_FOLLOWER_EXPANSION"
