from __future__ import annotations

import pytest
from services.live_sim import safety_gate
from storage import gateway_command_store


@pytest.mark.parametrize(
    ("status", "expected"),
    (
        (
            {
                "block_new_order_routing": True,
                "effective_block_new_order_routing": False,
            },
            False,
        ),
        (
            {
                "block_new_order_routing": False,
                "effective_block_new_order_routing": True,
            },
            True,
        ),
        ({"block_new_order_routing": True}, True),
        ({"block_new_order_routing": False}, False),
        (
            {
                "block_new_order_routing": True,
                "effective_block_new_order_routing": None,
            },
            True,
        ),
    ),
)
def test_order_routing_consumers_prefer_effective_with_raw_fallback(
    status: dict[str, object],
    expected: bool,
) -> None:
    assert gateway_command_store._order_broker_boundary_blocks_routing(status) is expected
    assert safety_gate._order_broker_boundary_blocks_routing(status) is expected
