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

## OBSERVE evidence launcher

장중 probe는 저장소 `.env`를 수정하지 않고 명시적 격리 DB를 사용한다.

```powershell
$db = Join-Path $env:TEMP 'append-only-evidence.sqlite3'
.\tools\start_market_open_observe.ps1 `
  -AppendOnlyEvidence `
  -DbPath $db `
  -RunCore `
  -RunGateway
```

`-AppendOnlyEvidence`는 OBSERVE/LIVE false, projection worker/apply, guarded cutover와 작은
budget(global 3/min, event/projection별 1/min), lifecycle worker를 임시 env에 명시한다.
condition-event worker의 candidate ingest는 false다. launcher는 Core API의 profile/mode/live
flags, 실제 DB path, configuration readiness를 확인하고 실패하면 중단한다. uvicorn reload는
사용하지 않는다.

`-RealtimeFidValidation`은 condition profile, stock realtime, theme refresh와 index TR
bootstrap을 제거한 더 좁은 mode다. 두 evidence mode는 `-DbPath`가 필수이고, 명시적
`-AllowOperatingDatabase` 없이는 현재 운영 DB를 거부한다. condition mode가 `NONE`이면 자식
Gateway 임시 env에서도 condition 이름/profile/file을 빈 값으로 덮어쓴다.

위 TEMP 예시는 단일 probe 전용이며 10거래일 누적 evidence가 아니다. TEMP DB를 삭제하면
reconcile ledger도 함께 사라지므로 qualified day로 보존되지 않는다.

## 영속 10거래일 시작/마감

공식 KRX calendar에서 당일 거래일을 확인한 뒤 영속 evidence wrapper를 사용한다. 기본 DB는
`%LOCALAPPDATA%\SuseokTrading\evidence\append-only-10day.sqlite3`이며 운영 DB와 분리된다.

```powershell
.\tools\start_append_only_daily_evidence.ps1 `
  -KrxTradingDayConfirmed
```

start wrapper는 다음을 강제한다.

- 현재 날짜만 허용하고 주말을 거부한다.
- `-KrxTradingDayConfirmed`가 없으면 시작하지 않는다.
- TEMP 경로와 이미 점유된 Core port를 거부한다.
- `OBSERVE`, LIVE_SIM/LIVE_REAL false, `MARKET_DATA_FULL_GUARDED`를 사용한다.
- global budget 3/min, projection별 1/min을 유지한다.
- Core를 먼저 띄워 command/failed/order baseline sidecar를 기록한 뒤 Gateway 로그인과 condition
  load를 확인한다. 기본 15초 안정화 후에만 KRX theme refresh를 시작한다.
- PR-30에서 확인한 설치본에 한해 Core 임시 env의 market-scan parser를
  `KOA_STUDIO_VERIFIED`로 명시한다. 일반 launcher 기본값은 계속 unverified다.
- KRX theme refresh, Kiwoom Gateway와 Core를 영속 DB에 연결한다.

장 마감 후에는 Gateway/theme ingest를 먼저 내리고 같은 DB를 마감한다.

```powershell
.\tools\close_append_only_daily_evidence.ps1
```

close wrapper는 Gateway와 해당 Core URL의 theme loop만 중지한 뒤
`tools/ops_append_only_daily_evidence.py`를 실행한다. Python closer는 다음 순서로 동작한다.

1. Core `OBSERVE`, LIVE false, expected DB path와 append-only configuration을 확인한다.
2. settle 구간의 heartbeat/event timestamp가 변하지 않아 ingest가 멈췄는지 확인한다.
3. projection outbox를 bounded short batch로 drain한다.
4. lifecycle worker를 실행하고 최신 market-index watermark 기준으로 market context를 재빌드한다.
5. market-data/reference/index/regime/scan reconcile을 persist한다. market-scan은 최소 120초
   timeout을 사용한다.
6. 당일 여섯 component가 모두 qualified인지, command/order-command delta가 0인지 확인한다.
7. session sidecar 대비 FAILED/order-command delta가 0인지 확인한다.
8. 누적 readiness와 일별 JSON/Markdown 보고서를 남긴다.

마감 성공 시 Core를 종료하지만 영속 DB는 보존한다. 실패하면 Gateway ingest는 중지된 상태로
두고 Core를 유지해 원인을 조사할 수 있게 한다. historical/future date backfill, TEMP DB,
자동 flag 변경, 주문 command 생성은 허용하지 않는다. 보고서는
`reports/append_only_daily_evidence/YYYY-MM-DD/` 아래에 저장한다.
성공한 마감만 `.session.json` sidecar를 제거한다. sidecar가 남아 있으면 다음 start를 거부하므로
실패 원인을 조사하고 같은 세션을 마감하기 전 임의 삭제하지 않는다.

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

각 날짜는 영속 DB 안에서만 누적한다. Markdown 보고서만 복사하거나 과거 날짜를 재생해
qualified day를 합성하지 않는다. 당일 마감이 FAIL이면 그 날짜는 수정 없이 실패 evidence로
남기고 원인 해결 후 같은 거래일의 최신 reconcile을 다시 마감한다.

영속 evidence DB에 대해서는 장중 또는 마감 전 `projection-outbox/bulk-retire` apply를 실행하지
않는다. API는 기본적으로 현재 KST 거래일 source event를 보호하지만, 이 보호를 해제해 당일
job을 `SKIPPED` 처리하면 해당 날짜의 일별 evidence가 무효가 될 수 있다.

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

운영 schema 적용 전에는 `docs/runbook_database_migration_preflight_ko.md` 절차로 read-only
online backup clone에서 migration, 멱등 재실행, `quick_check(1)`, outbox backlog 보존을 먼저
확인한다.
