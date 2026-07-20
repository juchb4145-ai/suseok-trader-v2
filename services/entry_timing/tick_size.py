from __future__ import annotations

import math


def krx_tick_size(price: float) -> int:
    if price <= 0:
        raise ValueError("price must be > 0")
    if price < 2_000:
        return 1
    if price < 5_000:
        return 5
    if price < 20_000:
        return 10
    if price < 50_000:
        return 50
    if price < 200_000:
        return 100
    if price < 500_000:
        return 500
    return 1_000


def add_ticks(price: float, ticks: int) -> int:
    if price <= 0:
        raise ValueError("price must be > 0")
    if ticks < 0:
        raise ValueError("ticks must be >= 0")
    adjusted = normalize_tick_price(price)
    for _ in range(ticks):
        adjusted += krx_tick_size(adjusted)
    return adjusted


def subtract_ticks(price: float, ticks: int) -> int:
    if price <= 0:
        raise ValueError("price must be > 0")
    if ticks < 0:
        raise ValueError("ticks must be >= 0")
    adjusted = normalize_tick_price(price)
    for _ in range(ticks):
        if adjusted <= 1:
            return 1
        probe = adjusted - 1
        tick = krx_tick_size(probe)
        adjusted = max(int(math.floor(probe / tick) * tick), tick)
    return adjusted


def normalize_tick_price(price: float) -> int:
    if price <= 0:
        raise ValueError("price must be > 0")
    tick = krx_tick_size(price)
    return max(int(math.floor(price / tick) * tick), tick)
