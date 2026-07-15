# Database Migration Preflight Runbook

## 목적

운영 SQLite를 직접 migration하기 전에 일관된 clone에서 현재 `storage.sqlite.SCHEMA_VERSION`
까지의 additive migration, 멱등성, 무결성과 outbox 보존을 검증한다. 이 절차는 운영 DB를
변경하거나 운영 schema를 승격하지 않는다.

## 안전 계약

- source는 SQLite URI `mode=ro&immutable=1`와 `PRAGMA query_only=ON`으로만 연다.
- source에 WAL/SHM sidecar가 하나라도 있으면 완전한 quiescence를 증명할 수 없으므로 DB를 열기 전에 fail-closed한다. sidecar를 도구가 삭제하거나 checkpoint하지 않는다.
- source와 clone 경로가 같거나 clone/WAL/SHM이 이미 있으면 fail-closed한다.
- migration과 재실행은 clone에서만 수행한다. `quick_check(1)`은 immutable source와 clone에서 각각 수행한다.
- source main/WAL/SHM의 size/mtime/SHA-256 지문을 전후 대조한다.
- 모든 기존 table의 row count, typed content hash와 rowid lineage, `sqlite_sequence`, `projection_outbox`를 source→backup→migration 후 대조한다. `app_metadata`는 `schema_version` row만 정규화에서 제외하고 나머지 metadata를 보존한다.
- clone volume의 free space가 source 크기와 migration 여유 예산보다 큰지 실행 전에 확인한다.
- `projection_outbox` 상태별 수량이 backup 전, migration 전, migration 후 모두 같아야 한다.
- 도구는 clone을 자동 삭제하지 않는다. report 확인 뒤 경로를 다시 검증하고 삭제한다.

## 실행

Core/Gateway가 운영 DB를 사용 중이지 않은지 먼저 확인한다. clone은 충분한 여유 공간이 있는
workspace 하위 새 경로를 사용한다.

```powershell
$source = (Resolve-Path .\storage\suseok-trader-v2.sqlite3).Path
$actualSchema = $actualSchemaFromStrictReadOnlyEvidence
$clone = Join-Path (Get-Location) `
  'storage\migration-preflight\suseok-trader-v2-schema61-preflight.sqlite3'

& $python -B -m tools.ops_database_migration_preflight `
  --source-db $source `
  --clone-db $clone `
  --require-source-schema $actualSchema `
  --out-dir reports/database_migration_preflight
```

`$actualSchemaFromStrictReadOnlyEvidence`는 이 실행 직전에 별도 strict read-only 점검으로 확인한 실제 source schema다. `52` 같은 과거 값을 복사해 넣거나 `--require-source-schema`를 생략하지 않는다. source schema가 현재 코드 target보다 높으면 downgrade 위험으로 FAIL한다. 현재 코드 target은 `61`이다.

PASS 조건은 다음과 같다.

- source/backup schema 일치, clone target schema `61`
- broker-boundary resolution ledger와 incremental dead-letter disposition ledger를 포함한 target required table/column/index/append-only trigger 및 동작 probe 통과
- source/clone outbox status count 일치
- source main/WAL/SHM 지문 불변, source와 clone `quick_check(1)=ok`
- 전체 기존 table content, rowid와 `sqlite_sequence` 보존
- source schema가 60 미만이면 선행 broker-boundary resolution table/row가 없어야 함
- source schema가 61 미만이면 선행 incremental dead-letter disposition table/row가 없어야 하며, migration 후 새 ledger는 비어 있어야 함
- migration 재실행 성공

결과는 `reports/database_migration_preflight/<UTC>/raw.json`과 `summary.md`에 저장된다.

## Backlog read-only 판정

clone Core를 OBSERVE로 띄운 별도 terminal에서 drain 없이 backlog를 조회한다.

```powershell
.\tools\start_market_open_observe.ps1 `
  -DbPath $clone `
  -RunCore `
  -CorePort 8042 `
  -CoreUrl http://127.0.0.1:8042
```

```powershell
python .\tools\ops_projection_outbox_backlog_drain.py `
  --core-url http://127.0.0.1:8042 `
  --token $env:TRADING_CORE_TOKEN
```

첫 사전검증에서는 `--drain` 또는 bulk-retire를 사용하지 않는다. historical shadow PENDING은
`blocking=0`, ERROR/DEAD_LETTER 0이면 다음 PR을 막지 않을 수 있지만, 운영 적용 전 별도
승인 없이 retire하지 않는다.

## 2026-07-11 과거 evidence

아래는 schema 52→59 당시의 보존 기록이며 현재 실행 명령이나 source schema 기대값으로 재사용하지 않는다.

- source `25.17GiB`, schema `52`, clone target `59`
- backup `138.947s`, migration `1.455s`, idempotent rerun `0.058s`
- `quick_check(1)=ok`, `581.904s`
- outbox `APPLIED=65159`, `PENDING=207`, `SKIPPED=2831` 전후 동일
- PENDING 207은 recent/condition/blocking `0/0/0`, bulk-retire eligible `207`
- ERROR/DEAD_LETTER/stale processing `0/0/0`, `pr11_ready=true`, next PR block false
- source main DB size/mtime 불변, 운영 Core/Gateway 0, 검증 후 clone 삭제

Reports: `reports/database_migration_preflight/20260710T233350Z/summary.md`,
`reports/projection_outbox_backlog/20260710T233625Z/summary.md`.
