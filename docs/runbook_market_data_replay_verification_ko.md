# MarketData 격리 replay parity runbook

대상: 기록된 `gateway_events`를 이용해 Gateway inline projection과
`projection_outbox` worker apply 결과를 같은 입력 순서로 비교하는 오프라인 검증.

## 안전 경계

- 이 절차는 `OBSERVE` 전용이다. `LIVE_SIM`, `LIVE_REAL`, 실제 주문/정정/취소/SOR
  호출을 허용하지 않는다.
- 운영 DB는 `export`의 SQLite read-only source로만 사용할 수 있다.
- `import`와 `parity` target은 존재하지 않는 새 SQLite 경로여야 한다. 설정된 운영 DB와
  같은 경로는 거부하며 기존 replay DB도 덮어쓰지 않는다.
- API와 Dashboard는 최신 report를 읽기만 한다. API에서 replay를 시작하거나 target을
  지정하는 endpoint는 없다.
- replay connection의 SQLite authorizer는 `raw_events`, `gateway_events`,
  `projection_outbox`, market-data projection table 등 명시된 replay table만 쓸 수 있게
  한다. `gateway_commands`, order plan, incremental/candidate, DRY_RUN/LIVE_SIM 테이블
  쓰기 시도는 `SQLITE_DENY`로 차단되고 parity는 FAIL한다.
- retention apply, 운영 migration, `.env` 수정은 이 절차에 포함하지 않는다.

## 1. Event export

하루치 accepted market-data event를 원본 `gateway_events.rowid` 순서로 내보낸다.

```powershell
python -m tools.ops_projection_replay export `
  --source-db storage\suseok-trader-v2.sqlite3 `
  --trade-date 2026-07-10 `
  --bundle-dir reports\projection_replay_bundles\2026-07-10
```

bundle은 다음 두 파일로 구성된다.

- `manifest.json`: event count/type/venue, source rowid 범위, record/order SHA-256
- `events.jsonl`: sequence, source rowid/received_at/status/payload hash, event envelope

`price_tick`, `condition_event`, `tr_response`만 기본 export한다. 주문 lifecycle event와
`command_*`, Chejan은 bundle allowlist에 포함되지 않는다.

## 2. Bundle 검증

```powershell
python -m tools.ops_projection_replay validate `
  --bundle-dir reports\projection_replay_bundles\2026-07-10
```

sequence gap, source rowid 역행, duplicate event id, payload/record/order hash 불일치,
unsafe event type이 하나라도 있으면 중단한다.

## 3. 단일 import 확인

필요할 때만 새 격리 DB로 import한다.

```powershell
python -m tools.ops_projection_replay import `
  --bundle-dir reports\projection_replay_bundles\2026-07-10 `
  --target-db storage\replay\2026-07-10-import.sqlite3 `
  --operational-db storage\suseok-trader-v2.sqlite3
```

확인값:

```text
order_preserved=true
no_order_side_effects=true
no_trading_side_effects=true
side_effect_table_delta 전부 0
blocked_write_attempts=[]
```

## 4. Inline-worker parity

같은 bundle을 두 새 DB에 import한다.

- `inline.sqlite3`: append/enqueue -> inline market-data apply -> outbox shadow verify
- `worker.sqlite3`: append/enqueue -> `market_data` worker apply

```powershell
python -m tools.ops_projection_replay parity `
  --bundle-dir reports\projection_replay_bundles\2026-07-10 `
  --work-root storage\replay\projection_parity `
  --operational-db storage\suseok-trader-v2.sqlite3 `
  --batch-size 10 `
  --out-dir reports\projection_replay
```

projection hash는 `updated_at`, `created_at`, watermark 처리 시각처럼 실행마다 달라지는
컬럼을 제외하고 계산한다. `tr_response` synthetic price tick은 parent event의
`event_ts`와 `received_at`을 상속한다.

PASS 기준:

```text
event order hash 일치
inline/worker projection overall hash 일치
mismatched_tables=[]
inline reconcile=PASS
worker reconcile=PASS
market_data outbox PENDING/PROCESSING/ERROR/DEAD_LETTER=0
금지 table delta=0
blocked write attempt=0
```

## KRX/NXT 판정

- manifest와 summary의 `venue_counts`, projection의
  `projection_counts_by_venue`를 KRX/NXT별로 확인한다.
- `exchange=NXT`가 payload/metadata/row에 명시된 event만 NXT evidence다.
- venue를 식별할 수 없는 TR은 `UNKNOWN`, 여러 venue가 섞이면 `MIXED`로 남긴다.
- NXT price tick replay는 venue-neutral market-data parity evidence로 사용할 수 있다.
- 조건검색은 KRX 기반이므로 `condition_event`는 KRX evidence다. NXT tick이 있어도
  condition cutover/condition_fusion PASS를 대신하지 않는다.
- NXT event가 없으면 parity 자체는 `WARN(NXT_EVENT_NOT_PRESENT)`일 수 있지만 synthetic
  NXT PASS를 만들지 않는다.

## Read-only 운영 표시

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/operator/projection-replay/status

Invoke-RestMethod `
  "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=projection_replay"
```

표시는 최신 `reports/projection_replay/*/raw.json`만 읽는다.
`replay_execution_available=false`, `production_db_writes_allowed=false`가 유지되어야 한다.

## 실패 대응

- hash mismatch: `mismatched_tables`와 table별 row count/hash를 먼저 비교한다.
- reconcile WARN/FAIL: 해당 chunk의 reason code와 issue event id를 확인한다.
- blocked write: replay가 비projection side effect를 호출한 것이므로 다음 PR을 중단하고
  호출 경로를 제거한다. authorizer allowlist를 넓혀 우회하지 않는다.
- 운영 DB path 거부: target 경로를 새 격리 경로로 바꾼다. 보호 검사를 끄지 않는다.
- replay DB 정리는 절대 경로가 `storage/replay` 아래임을 확인한 뒤에만 수행한다.

이 PR은 append-only cutover가 아니다. 다음 PR 진입은 parity PASS/WARN 중 hash/reconcile/
side-effect 실패가 없는 경우에만 허용한다.
