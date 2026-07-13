# Database Migration Preflight Runbook

## 목적

운영 SQLite를 직접 migration하기 전에 일관된 clone에서 현재 `storage.sqlite.SCHEMA_VERSION`
까지의 additive migration, 멱등성, 무결성과 outbox 보존을 검증한다. 이 절차는 운영 DB를
변경하거나 운영 schema를 승격하지 않는다.

## 안전 계약

- source는 SQLite URI `mode=ro`와 `PRAGMA query_only=ON`으로만 연다.
- source와 clone 경로가 같거나 clone/WAL/SHM이 이미 있으면 fail-closed한다.
- migration, 재실행, `quick_check(1)`은 clone에서만 수행한다.
- source main DB와 기존 WAL의 size/mtime 지문을 전후 대조한다.
- `projection_outbox` 상태별 수량이 backup 전, migration 전, migration 후 모두 같아야 한다.
- 도구는 clone을 자동 삭제하지 않는다. report 확인 뒤 경로를 다시 검증하고 삭제한다.

## 실행

Core/Gateway가 운영 DB를 사용 중이지 않은지 먼저 확인한다. clone은 충분한 여유 공간이 있는
workspace 하위 새 경로를 사용한다.

```powershell
$source = (Resolve-Path .\storage\suseok-trader-v2.sqlite3).Path
$clone = Join-Path (Get-Location) `
  'storage\migration-preflight\suseok-trader-v2-schema59-preflight.sqlite3'

python .\tools\ops_database_migration_preflight.py `
  --source-db $source `
  --clone-db $clone `
  --require-source-schema 52
```

PASS 조건은 다음과 같다.

- source/backup schema 일치, clone target schema `59`
- required schema 54~58 table 존재
- source/clone outbox status count 일치
- source data file 지문 불변
- migration 재실행 성공
- clone `quick_check(1)=ok`

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

## 2026-07-11 evidence

- source `25.17GiB`, schema `52`, clone target `59`
- backup `138.947s`, migration `1.455s`, idempotent rerun `0.058s`
- `quick_check(1)=ok`, `581.904s`
- outbox `APPLIED=65159`, `PENDING=207`, `SKIPPED=2831` 전후 동일
- PENDING 207은 recent/condition/blocking `0/0/0`, bulk-retire eligible `207`
- ERROR/DEAD_LETTER/stale processing `0/0/0`, `pr11_ready=true`, next PR block false
- source main DB size/mtime 불변, 운영 Core/Gateway 0, 검증 후 clone 삭제

Reports: `reports/database_migration_preflight/20260710T233350Z/summary.md`,
`reports/projection_outbox_backlog/20260710T233625Z/summary.md`.
