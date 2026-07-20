from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class BrokerSnapshotStatus(StrEnum):
    REQUESTED = "REQUESTED"
    COLLECTING = "COLLECTING"
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    FAILED = "FAILED"
    STALE = "STALE"


class BrokerSnapshotSection(StrEnum):
    OPEN_ORDERS = "OPEN_ORDERS"
    EXECUTIONS = "EXECUTIONS"
    POSITIONS = "POSITIONS"


@dataclass(frozen=True, kw_only=True)
class KiwoomSnapshotTrSpec:
    section: BrokerSnapshotSection
    tr_code: str
    request_name: str
    screen_no: str
    output_record_name: str
    fields: tuple[str, ...]

    def params(self, account_id: str) -> dict[str, str]:
        if self.section is BrokerSnapshotSection.OPEN_ORDERS:
            return {
                "계좌번호": account_id,
                "전체종목구분": "0",
                "매매구분": "0",
                "종목코드": "",
                "체결구분": "1",
            }
        if self.section is BrokerSnapshotSection.EXECUTIONS:
            return {
                "종목코드": "",
                "조회구분": "0",
                "매도수구분": "0",
                "계좌번호": account_id,
                "비밀번호": "",
                "주문번호": "",
                "체결구분": "0",
            }
        return {
            "계좌번호": account_id,
            "비밀번호": "",
            "비밀번호입력매체구분": "00",
            "조회구분": "2",
        }


KIWOOM_LIVE_SIM_SNAPSHOT_TR_SPECS: tuple[KiwoomSnapshotTrSpec, ...] = (
    KiwoomSnapshotTrSpec(
        section=BrokerSnapshotSection.OPEN_ORDERS,
        tr_code="OPT10075",
        request_name="live_sim_broker_snapshot_open_orders",
        screen_no="8751",
        output_record_name="미체결",
        fields=(
            "계좌번호",
            "주문번호",
            "종목코드",
            "종목명",
            "주문상태",
            "주문수량",
            "주문가격",
            "미체결수량",
            "체결량",
            "체결누계금액",
            "원주문번호",
            "주문구분",
            "매매구분",
            "시간",
        ),
    ),
    KiwoomSnapshotTrSpec(
        section=BrokerSnapshotSection.EXECUTIONS,
        tr_code="OPT10076",
        request_name="live_sim_broker_snapshot_executions",
        screen_no="8752",
        output_record_name="체결",
        fields=(
            "주문번호",
            "체결번호",
            "종목코드",
            "종목명",
            "주문상태",
            "주문수량",
            "주문가격",
            "체결가",
            "체결량",
            "미체결수량",
            "주문구분",
            "매매구분",
            "주문시간",
            "체결시간",
        ),
    ),
    KiwoomSnapshotTrSpec(
        section=BrokerSnapshotSection.POSITIONS,
        tr_code="OPW00018",
        request_name="live_sim_broker_snapshot_positions",
        screen_no="8753",
        output_record_name="계좌평가잔고개별합산",
        fields=(
            "종목번호",
            "종목명",
            "보유수량",
            "매매가능수량",
            "매입가",
            "현재가",
            "매입금액",
            "평가금액",
            "평가손익",
            "수익률(%)",
        ),
    ),
)


def mask_account_id(value: object) -> str:
    normalized = "".join(
        character for character in str(value or "").strip() if character.isalnum()
    )
    if not normalized:
        return "UNCONFIGURED"
    if len(normalized) <= 4:
        return "*" * len(normalized)
    return f"***{normalized[-4:]}"


def canonical_snapshot_status(value: Any) -> BrokerSnapshotStatus:
    try:
        return BrokerSnapshotStatus(str(value or "").strip().upper())
    except ValueError:
        return BrokerSnapshotStatus.INCOMPLETE
