# NXT 프리마켓 관찰 파이프라인

## 목적

NXT 08:00-08:50 KST 구간의 실시간 tick을 관찰 데이터로 저장해 장 시작 전 갭과 테마 흐름을 볼 수 있게 한다. 이 기능은 주문 라우팅이 아니며 매수 신호도 아니다.

## 기본값

모든 신규 기능은 기본 비활성이다.

| 설정 | 기본값 | 의미 |
| --- | --- | --- |
| `MARKET_DATA_PREMARKET_SNAPSHOT_ENABLED` | `false` | NXT premarket snapshot 생성 |
| `THEME_PREMARKET_OBSERVABLES_ENABLED` | `false` | Theme snapshot metadata에 premarket 요약 추가 |
| `ENTRY_TIMING_PREMARKET_CONTEXT_ENABLED` | `false` | EntryTiming input에 `premarket_gap` 컨텍스트 추가 |

## 수집 경로

NXT 실시간은 기존 opt-in 경로를 사용한다.

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --realtime-codes 005930,000660 `
  --realtime-exchange nxt `
  --observe-only
```

또는 Core 쪽 realtime subscription planner는 `REALTIME_SUBSCRIPTION_EXCHANGE=NXT`나 `ALL`일 때 `register_realtime` payload에 `exchange=NXT|ALL`을 담는다. Gateway는 Kiwoom 등록 코드 suffix를 붙이고, Core price tick payload metadata에는 base code와 `exchange=NXT`를 남긴다.

## Snapshot 규칙

`MARKET_DATA_PREMARKET_SNAPSHOT_ENABLED=true`일 때만 `market_premarket_snapshots`가 갱신된다.

| 항목 | 규칙 |
| --- | --- |
| 포함 세션 | NXT `PREMARKET_NXT` |
| 시간 경계 | 08:00:00 이상, 08:50:00 미만 KST |
| key | `(trade_date, code)` |
| first/last | 프리마켓 첫 tick 가격과 마지막 tick 가격 |
| gap | 직전 KRX regular close가 있을 때만 계산 |
| 08:50 tick | 제외, `OFF_HOURS`로 분리 |

조회:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/premarket/2026-06-26
```

## Downstream

Theme opt-in은 테마별 평균 premarket gap과 leader code를 metadata에 넣는다. EntryTiming opt-in은 `EntryTimingInput.premarket_gap`과 raw context에만 넣는다.

`MATCHED_OBSERVATION`은 매수 신호가 아니고 `OBSERVE_PASS`는 주문 승인이 아니다. 프리마켓 gap은 주문 수량, 가격, side, gateway command 생성에 사용하지 않는다.
