from __future__ import annotations

from datetime import datetime

from domain.broker.events import GatewayEvent
from domain.broker.utils import utc_now
from services.config import Settings, TradingMode, TradingProfile
from services.market_data_service import process_gateway_event
from services.market_scan_service import process_market_scan_event
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event


def market_scan_settings(**overrides) -> Settings:
    values = {
        "trading_profile": TradingProfile.OBSERVE,
        "trading_mode": TradingMode.OBSERVE,
        "trading_allow_live_sim": False,
        "trading_allow_live_real": False,
        "market_scan_enabled": True,
        "market_scan_parser_status": "KOA_STUDIO_VERIFIED",
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
        "projection_outbox_market_reference_apply_enabled": False,
        "projection_outbox_market_index_apply_enabled": False,
        "projection_outbox_market_regime_apply_enabled": False,
        "projection_outbox_market_scan_apply_enabled": True,
        "projection_outbox_market_scan_apply_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
        "gateway_market_scan_append_only_dry_run_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def market_scan_cutover_settings(**overrides) -> Settings:
    values = {
        "gateway_market_scan_append_only_cutover_enabled": True,
        "gateway_market_scan_append_only_global_kill_switch": False,
        "gateway_market_scan_append_only_max_skip_per_minute": 1,
        "gateway_market_scan_append_only_effective_skip_disabled_in_pr20": False,
    }
    values.update(overrides)
    return market_scan_settings(**values)


def make_market_scan_event(
    event_id: str,
    *,
    request_suffix: str | None = None,
    parser_status: str = "KOA_STUDIO_VERIFIED",
    market: str = "KOSPI",
    scan_type: str = "TRADE_VALUE",
    code: str = "005930",
    ts: datetime | None = None,
) -> GatewayEvent:
    suffix = request_suffix or event_id
    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="test-gateway",
        ts=ts or utc_now(),
        payload={
            "request_id": f"market_scan:{scan_type}:{market}:{suffix}",
            "tr_code": "OPT10032" if scan_type == "TRADE_VALUE" else "OPT10027",
            "request_name": f"market_scan_{scan_type.lower()}_{market.lower()}",
            "success": True,
            "rows": [
                {
                    "종목코드": code,
                    "종목명": "삼성전자",
                    "순위": "1",
                    "현재가": "+70000",
                    "등락률": "1.25",
                    "거래대금": "1000000000",
                    "거래량": "100000",
                }
            ],
            "metadata": {
                "source": "market_scan_service",
                "parser_status": parser_status,
                "observe_only": True,
                "no_order_side_effects": True,
            },
        },
    )


def append_scan_event(connection, event: GatewayEvent) -> None:
    result = append_gateway_event(connection, event)
    assert result.status == "ACCEPTED"
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    assert {job["projection_name"] for job in outbox.jobs} == {
        "market_data",
        "market_scan",
    }


def apply_inline_scan_event(
    connection,
    event: GatewayEvent,
    *,
    settings: Settings,
) -> None:
    assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"
    assert process_market_scan_event(connection, event, settings=settings).status == "APPLIED"
