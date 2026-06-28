# Operator Dashboard

운영 대시보드는 read-only 화면이다. buy, sell, cancel, settings toggle, AI 실행 버튼을 두지
않는다. LIVE_REAL 구현이나 실계좌/실서버 허용도 포함하지 않는다.

## 메인 패널

1. 시스템 상태: Core, Gateway, Kiwoom mode, LIVE_SIM flags, kill switch, heartbeat,
   account/server/broker simulation 여부.
2. 오늘 시장/테마: ThemeLeadership top themes, DATA_WAIT count, Watchset count.
3. 매수 후보/OrderPlan: PLAN_READY, WAIT_RETRY, DATA_WAIT, top near-miss 후보.
4. LIVE_SIM 주문/포지션: today intents/orders/commands, open orders, open positions,
   cancel candidates, exit signals, reconcile status.
5. AI Advisory: provider/model, latest run status, selected count, top scores,
   AI no_trade_reason, external error/fallback.
6. 왜 안 샀나: No-Buy Sentinel status, stage summary, reason summary, top near-miss,
   operator checklist.

Raw/debug 데이터는 접기 영역에만 둔다. 대시보드 문구는 한글 친화 라벨을 우선 사용한다.

## API

- `GET /api/operator/status`
- `GET /api/operator/no-buy`
- `GET /api/operator/no-buy/latest`
- `POST /api/operator/no-buy/rebuild`

POST rebuild는 로컬 토큰 보호 대상이며 진단 스냅샷만 저장한다. 모든 응답은
`read_only=true`, `no_order_side_effects=true`를 포함한다.
