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


def price_tick_distance(start_price: float, end_price: float) -> int:
    """Return signed KRX ticks from start_price to end_price."""
    start = normalize_tick_price(start_price)
    end = normalize_tick_price(end_price)
    if start == end:
        return 0
    direction = 1 if end > start else -1
    current = start
    distance = 0
    while current != end:
        next_price = add_ticks(current, 1) if direction > 0 else subtract_ticks(current, 1)
        if (direction > 0 and next_price > end) or (direction < 0 and next_price < end):
            raise ValueError("end_price must align to a KRX tick from start_price")
        current = next_price
        distance += direction
    return distance


def normalize_tick_price(price: float) -> int:
    if price <= 0:
        raise ValueError("price must be > 0")
    tick = krx_tick_size(price)
    return max(int(math.floor(price / tick) * tick), tick)
