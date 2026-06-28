# No-Buy Sentinel

No-Buy Sentinel은 장중에 “왜 안 샀는지”를 설명하는 진단 전용 서비스다. OrderIntent,
LiveSimIntent, GatewayCommand를 생성하지 않고, send/cancel/modify 주문 호출도 하지 않는다.

## 판정 축

- 후보 없음: ThemeLeadership watchset, candidate, OrderPlanDraft가 비어 있으면
  `NO_CANDIDATE`로 본다.
- 테마 데이터 보류: ThemeLeadership 결과가 `DATA_WAIT` 중심이면 `THEME_DATA_WAIT`로 본다.
- EntryTiming 보류: 후보나 watchset은 있으나 `WAIT_RETRY`/`DATA_WAIT` OrderPlanDraft가
  대부분이면 `ENTRY_TIMING_WAIT`로 본다.
- 시스템 차단: PLAN_READY가 있어도 LIVE_SIM safety gate, Gateway, config, reconcile,
  duplicate/position, limit이 막으면 시스템 block으로 분리한다.
- AI 관망: AI selected가 비어 있고 `no_trade_reason`이 있으면 `AI_NO_TRADE`로 표시한다.

## AI와 시스템 reason 분리

AI score는 주문 승인, 리스크 승인, 수익 확률이 아니다. AI selected가 있어도 safety gate,
reconcile, config가 막으면 시스템 block이 우선이다. AI timeout, invalid schema, provider
error는 `AI_UNAVAILABLE`로 표시하며 시스템 무매수 원인으로 보지 않는다.

## DATA_WAIT 의미

`DATA_WAIT`는 hard block이 아니라 데이터 보강 후 재평가할 상태다. 운영자는 누락된 tick/bar,
테마 coverage, candidate context를 확인한다.

## 스냅샷

기본값 `NO_BUY_SENTINEL_WRITE_SNAPSHOTS=true`일 때 `no_buy_sentinel_snapshots`에 진단
스냅샷을 저장한다. 저장 실패는 거래 파이프라인 장애로 전파하지 않는다.

## CLI

```powershell
python -m tools.inspect_no_buy_sentinel --trade-date 2026-06-29 --json
python -m tools.rebuild_no_buy_sentinel --trade-date 2026-06-29 --json
python -m tools.inspect_operator_status --json
```

모든 CLI는 read-only 진단이다. rebuild CLI도 진단 스냅샷만 저장한다.
