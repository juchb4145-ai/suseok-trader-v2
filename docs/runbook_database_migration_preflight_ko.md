# Database Migration Preflight Runbook

## 목적

운영 SQLite를 직접 migration하기 전에 일관된 clone에서 현재 `storage.sqlite.SCHEMA_VERSION`
까지의 exact one-shot migration, CAS guard, target-schema initializer logical no-op, 무결성과
outbox 보존을 검증한다. 이 절차는 운영 DB를 변경하거나 운영 schema를 승격하지 않는다.

## 안전 계약

- source는 SQLite URI `mode=ro&immutable=1`와 `PRAGMA query_only=ON`으로만 연다.
- source에 WAL/SHM/rollback-journal sidecar가 하나라도 있으면 완전한 quiescence와 rollback 완료를 증명할 수 없으므로 DB를 열기 전에 fail-closed한다. 크기가 0인 sidecar도 거부하며, 도구가 삭제·checkpoint·복구하지 않는다.
- source와 clone 경로가 같거나 clone/WAL/SHM/rollback-journal이 이미 있으면 fail-closed한다.
- exact migration과 migration 후 target-schema 일반 초기화 logical no-op 검증은 clone에서만 수행한다. exact migration 함수는 재실행하지 않는다. `quick_check(1)`은 immutable source와 clone에서 각각 수행한다.
- source main/WAL/SHM/rollback-journal의 존재 여부와 size/mtime/SHA-256 지문을 전후 대조한다.
- 모든 기존 table의 row count, typed content hash와 rowid lineage, `sqlite_sequence`, `projection_outbox`를 source→backup→migration 후 대조한다. `app_metadata.schema_version`은 `value` 내용만 target 차이로 정규화하고 SQL type·key·updated_at·rowid·미래 열은 별도 record fingerprint로 계속 대조한다.
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
  'storage\migration-preflight\suseok-trader-v2-schema62-preflight.sqlite3'

& $python -B -m tools.ops_database_migration_preflight `
  --source-db $source `
  --clone-db $clone `
  --require-source-schema $actualSchema `
  --out-dir reports/database_migration_preflight
```

`$actualSchemaFromStrictReadOnlyEvidence`는 이 실행 직전에 별도 strict read-only 점검으로 확인한 실제 source schema다. `52` 같은 과거 값을 복사해 넣거나 `--require-source-schema`를 생략하지 않는다. source schema가 현재 코드 target보다 높으면 downgrade 위험으로 FAIL하고, target과 같아도 migration 대상이 아니므로 `SOURCE_SCHEMA_NOT_OLDER_THAN_TARGET`로 거부한다. FAST-0R3 코드 target은 `62`이고 승인된 운영 source는 정확히 `61`이어야 한다.

PASS 조건은 다음과 같다.

- source/backup schema 일치, clone target schema `62`
- broker-boundary resolution, incremental dead-letter disposition, pipeline coherency disposition ledger를 포함한 target required table/column/index/append-only trigger 및 동작 probe 통과
- source/clone outbox status count 일치
- source main/WAL/SHM/rollback-journal 지문 불변, source와 clone `quick_check(1)=ok`
- 전체 기존 table content, rowid와 `sqlite_sequence` 보존
- source schema가 60 미만이면 선행 broker-boundary resolution table/row가 없어야 함
- source schema가 61 미만이면 선행 incremental dead-letter disposition table/row가 없어야 하며, migration 후 새 ledger는 비어 있어야 함
- source schema가 62 미만이면 선행 pipeline coherency disposition table/row가 없어야 하며, migration 후 새 ledger는 비어 있어야 함
- exact migration 직후와 contract probe rollback 직후 snapshot이 같고, 이후 target-schema 일반 `initialize_database` 실행도 schema/data를 전혀 바꾸지 않음. 이것이 initializer no-op/idempotent 검증이며 exact migration 재실행을 뜻하지 않음

Preflight PASS는 운영 DB write 승인이 아니다. 실제 migration은 source fingerprint 재확인,
byte-identical backup과 backup `quick_check(1)=ok`, writer/lease/sidecar 0, 충분한 disk, exact
source schema를 다시 확인한 별도 운영 단계에서만 수행한다. `--skip-quick-check`는 사용하지 않는다.

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

## 승인된 exact schema 61→62 apply

이 절차는 별도 승인된 schema `61→62` 운영 단계에서만 사용한다. generic migration이나
`initialize_database`를 운영 source에 실행하지 않는다. 직전 preflight의 `raw.json`은 생성 후
1시간 이내의 `PASS`여야 하고, 그 evidence의 절대 source 경로·main SHA-256·typed table
fingerprint·source schema `61`·target schema `62`가 현재 source와 모두 같아야 한다.
기존 DB의 schema가 current target `62`와 정확히 같지 않거나 `app_name` identity가 일치하지 않으면
`initialize_database`도 fail-closed하므로 Core startup으로 승격·다운그레이드·identity 복구를 우회할
수 없다. historical generic additive migration은 격리 clone에서만 전용 offline initializer를 사용하며,
schema `61` clone preflight는 그 경로도 거부하고 같은 exact `migrate_schema_61_to_62` 함수를 사용한다.
이 exact 함수는 schema `61` CAS와 target object 부재를 요구하는 의도적인 one-shot이며
재실행 가능(idempotent) 함수가 아니다. preflight의 `idempotent rerun`은 exact 함수를 다시
호출한다는 뜻이 아니라, exact 직후 snapshot과 contract probe rollback 직후 snapshot이 같고,
이미 schema `62`인 clone에 일반 `initialize_database`를 한 번 실행해도 schema object·모든 table
typed content·`sqlite_sequence`·outbox를 포함한 논리 snapshot이 전혀 변하지 않아야 한다는 뜻이다.
따라서 일반 초기화가 누락된 legacy table/index/trigger를 복구하면 preflight는 FAIL한다.

backup과 그 `-wal`/`-shm`/`-journal`은 존재하지 않는 새 경로를 지정한다.

```powershell
$source = (Resolve-Path .\storage\suseok-trader-v2.sqlite3).Path
$preflightRaw = (Resolve-Path `
  .\reports\database_migration_preflight\<UTC>\raw.json).Path
$backup = Join-Path (Get-Location) `
  'storage\migration-backup\suseok-trader-v2-schema61-before-62.sqlite3'

& $python -B -m tools.ops_database_migration_apply `
  --source-db $source `
  --preflight-raw $preflightRaw `
  --backup-db $backup `
  --acknowledge APPLY_EXACT_SCHEMA_61_TO_62_PIPELINE_LEDGER `
  --out-dir reports/database_migration_apply
```

도구는 write 전에 source sidecar 0, hardlink 1, disk 여유, runtime/processing lease 0,
strict read-only `quick_check(1)=ok`와 fingerprint 재일치를 요구한다. `BEGIN EXCLUSIVE`를
획득한 뒤 source와 byte-identical인 새 backup을 만들고 backup SHA-256,
`quick_check(1)=ok`, typed snapshot을 다시 확인한다. 실제 transaction의 authorizer는 새
pipeline disposition table/index/trigger 생성과 `schema_version`의 CAS `61→62` update만
허용한다. commit 전 모든 기존 table의 typed content/rowid, `sqlite_sequence`, outbox,
target DDL/probe와 빈 ledger를 검증하며 불일치 시 rollback한다. commit 뒤에도 strict
read-only quick check, 동일 invariant와 sidecar 0을 재검증한다. 성공 raw evidence에는 exclusive
postcommit quick check, 전체 lease count, precommit/strict-RO/exclusive snapshot equality와
locked/final main fingerprint equality를 함께 기록한다.

결과는 `reports/database_migration_apply/<UTC>/raw.json`과 `summary.md`에 hash/count만
기록한다. 실패 evidence의 `committed`가 `true`이면 자동 재실행하거나 backup을 덮어쓰지
말고 즉시 중단해 source/backup을 별도 확인한다. 이 값은 fail-closed하게
`APPLIED_OR_UNKNOWN_FAIL_CLOSED`를 포함하므로 실제 적용 여부를 재검증하기 전에는
미적용으로 해석하지 않는다.
