# Append-only Readiness Runbook KO

## 목적

이 runbook은 Gateway request path를 raw append + durable enqueue 전용으로 바꿀 수 있는지 읽기 전용으로 판정한다. 판정은 설정을 수정하지 않고 주문 command를 만들지 않는다.

현재 구현은 readiness report만 제공한다. 다음 동작은 자동으로 수행하지 않는다.

- cutover flag 변경
- global kill switch 해제
- emergency inline fallback 제거
- Gateway request path 코드 제거
- LIVE_SIM/LIVE_REAL 활성화
- 주문, 정정, 취소, SOR 호출

## API

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/operator/append-only-readiness/status
Invoke-RestMethod "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=append_only_readiness"
```

두 API는 read-only다. Dashboard fast는 section을 명시한 경우에만 기간 evidence를 집계한다.

## Ops check

```powershell
python tools/ops_append_only_readiness_check.py `
  --core-url http://127.0.0.1:8000
```

10거래일 evidence와 현재 health가 모두 준비됐음을 기대하는 검토 시점에만 다음 옵션을 사용한다.

```powershell
python tools/ops_append_only_readiness_check.py `
  --core-url http://127.0.0.1:8000 `
  --expect-ready
```

`--expect-ready`도 설정을 바꾸지 않는다.

## 일별 evidence 계약

KRX 거래일마다 다음 persisted evidence의 최신 run이 같은 local trade date를 가리켜야 한다.

- market-data projection reconcile
- market-reference projection reconcile
- market-index projection reconcile
- market-regime projection reconcile
- market-scan projection reconcile
- LIVE_SIM lifecycle durable consumer run

Projection evidence는 다음을 모두 요구한다.

- reconcile `PASS`
- `checked_event_count > 0`
- `append_only_ready=true`
- run date와 latest source event date 일치
- PENDING/PROCESSING/SKIPPED/ERROR/DEAD_LETTER 0
- projection artifact/error/side-effect gap 0
- `no_trading_side_effects=true`

Market-index evidence는 추가로 모든 checked event가 explicit `REALTIME`이어야 한다. TR bootstrap, UNKNOWN source, NXT tick은 10일 KRX anchor로 계산하지 않는다.

Lifecycle evidence는 같은 날짜에 `IDLE` 또는 `COMPLETED` worker run이 있고 error/dead-letter/stale reset이 0이어야 한다.

## 현재 configuration gate

기간 evidence 외에도 다음 fail-closed configuration을 요구한다.

- OBSERVE, LIVE_SIM/LIVE_REAL 허용 false
- projection worker와 market-data/reference/index/regime/scan apply enabled
- market-data `MARKET_DATA_FULL_GUARDED`, 각 event cutover와 budget 활성
- market-reference/index/regime/scan guarded cutover 활성
- 각 global kill switch 해제와 legacy effective-skip guard 해제
- lifecycle consumer/worker/cutover 활성
- lifecycle emergency inline fallback 유지

이 configuration은 운영자가 각 단계의 장중 gate를 별도로 통과한 기간에만 사용한다. readiness checker가 configuration을 자동 변경하지 않는다.

## 상태 해석

- `FAIL_SAFETY`: OBSERVE 안전 조건 위반
- `BLOCKED_SCHEMA`: required reconcile/lifecycle evidence table 미존재
- `BLOCKED_CONFIG`: 기간 측정에 필요한 guarded configuration 미충족
- `BLOCKED_HEALTH`: 현재 outbox/lifecycle health 미충족
- `BLOCKED_EVIDENCE`: 연속 qualified evidence가 10거래일 미만
- `READY_FOR_OPERATOR_REVIEW`: 코드상 10일 교차 evidence 충족

`READY_FOR_OPERATOR_REVIEW`도 자동 승인 상태가 아니다. 다음 수동 검토가 남는다.

- 공식 KRX 거래일 calendar와 evidence 날짜 대조
- KOA Studio parser/FID 확인
- command/order-command delta 0
- invalid effective skip 0
- deferred side-effect 누락 0
- ERROR/DEAD_LETTER 0
- emergency rollback drill 결과

따라서 API는 모든 상태에서 `automatic_cutover_allowed=false`, `flag_cleanup_allowed=false`, `request_path_removal_performed=false`를 반환한다.

## NXT 원칙

NXT는 명시적 `exchange=NXT`인 venue-neutral market-data/replay/worker health 검증에 사용할 수 있다. 다음 KRX 전용 evidence를 대체하지 않는다.

- market-index realtime anchor
- market-regime KRX index continuity
- Kiwoom condition-event/condition-fusion
- KRX market-reference membership
- 공식 KRX 거래일 확인

NXT만 수신한 날은 KRX 10거래일 count를 증가시키지 않는다.

## Rollback 및 cleanup

evidence가 부족하거나 current health가 실패하면 기존 inline fallback과 모든 scaffolding flag를 유지한다. `.env` 자동 수정, flag 자동 정리, request-path 제거는 금지한다.

실제 flag cleanup은 별도 operator 승인과 구조 감사 문서 갱신 후 독립 PR로 수행한다. 첫 단계에서도 단일 emergency rollback flag를 유지하고, 추가 안정화 뒤에만 최종 inline 코드를 제거한다.
