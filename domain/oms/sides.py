from __future__ import annotations

from enum import StrEnum


class DryRunSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class DryRunOrderType(StrEnum):
    MARKET_SIM = "MARKET_SIM"
    LIMIT_SIM = "LIMIT_SIM"
