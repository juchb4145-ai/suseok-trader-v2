# FAST-0R5 Strict Offline Requalification Runbook

## 목적과 현재 상태

이 절차는 schema-62 운영 SQLite를 strict read-only로 다시 확인하고 FAST-0의 남은
승급 조건을 한 보고서에서 판정한다. Core, Gateway, Kiwoom 또는 worker를 시작하지 않고,
운영 DB를 migration하거나 row를 수정하지 않는다.

2026-07-15 R5 운영 실행은 완료됐다. aggregate evidence는 `PASS`지만 checker가 읽는
bounded historical log는 완전한 runtime window 또는 현재 owner liveness를 증명하지 않으며
최종 OBSERVE smoke도 실행하지 않는다. 따라서 checker 단독 FAST-0 결과는 `BLOCKED`다.

## 판정 축

R5는 다음 두 상태를 분리한다.

1. `evidence_status`
   - 승인된 DB·로그·migration report를 비파괴적으로 읽었는지 판정한다.
   - schema/app identity, sidecar 부재, strict read-only 연결, quick check, 검사 구간 동안
     고정한 DB file identity, 전후 파일 fingerprint와 aggregate-only 출력 계약을 포함한다.
2. `fast0_status`
   - 실제 FAST-0 승급 blocker가 남아 있는지 판정한다.
   - offline evidence가 완전해도 최종 OBSERVE smoke가 아직 별도 완료되지 않았다면
     `BLOCKED`다.

`evidence_status=PASS`와 `fast0_status=BLOCKED`는 모순이 아니다. 전자는 증거 수집의
무결성을, 후자는 다음 단계 승급 가능 여부를 뜻한다.

## 실행 전 필수 조건

- 승인된 schema-62 운영 DB 절대경로를 사용한다.
- Core, Gateway, Kiwoom, theme/evidence, projection/lifecycle worker와 기타 DB writer가
  모두 종료됐음을 별도로 확인한다.
- main DB 옆의 `-wal`, `-shm`, `-journal`이 없어 checkpointed 상태임을 확인한다.
- sidecar를 수동 삭제하거나 이동해서 조건을 만들지 않는다. writer가 있었다면 정상
  종료와 checkpoint 절차부터 다시 수행한다.
- schema `61→62` migration apply의 승인된 `raw.json`과, 그 apply report가 참조하는
  preflight `raw.json`을 원래 경로에 준비한다. migration을 다시 실행하지 않는다.
- apply `raw.json`의 SHA-256은 검사 대상 파일에서 즉석 계산한 값이 아니라 승인 기록에서
  독립적으로 고정한 lowercase 64-hex 값을 사용한다. 실행 전에 현재 파일 hash와 일치하는지
  비교한다.
- runtime fencing/owner 이상을 조사할 명시적으로 승인된 bounded historical runtime log를
  준비한다. 여러 파일은 `--runtime-log`를 반복해 전달한다. 최대 32개, 합계 1 GiB이며
  중복 경로는 허용하지 않는다.
- DB가 있는 volume에 최소 1 GiB 여유 공간이 있고 DB main file을 읽을 수 있는지 확인한다.
- 별도로 output directory를 만들고 report를 쓸 권한·여유 공간이 있는지 수동 확인한다.
- 실행 명령과 산출물에는 local token, 비밀번호, 원 계좌번호, raw payload 또는 raw 오류
  문구를 넣지 않는다.

sidecar 부재와 checker의 실행 전·후 open-handle probe는 각각 point-in-time 증거일 뿐,
실행 전체 구간의 writer 부재를 증명하지 않는다. checker는 하나의 pinned read handle을
hash·SQLite snapshot·최종 hash 전체에 유지해 경로 교체로 다른 DB를 읽는 것을 막지만,
별도의 프로세스 종료 증거와 전후 물리 파일 fingerprint 불변 증거도 함께 보존한다.

## 금지 작업

- `LIVE_SIM` 또는 `LIVE_REAL` 활성화
- Core, Gateway, Kiwoom, worker 또는 운영 cycle 시작
- 운영 DB migration 재실행, `initialize_database()` 실행 또는 일반 RW 연결
- 운영 DB의 `INSERT`, `UPDATE`, `DELETE`, disposition/resolution 추가
- `-wal`, `-shm`, `-journal` 삭제·격리·이동
- projection outbox drain/bulk retire, lifecycle retry/reset, dead-letter reset
- broker-boundary raw 상태 변경 또는 historical audit row 정리
- order command 생성과 `send_order`, `cancel_order`, `modify_order` 호출
- safety gate, test 또는 qualification 기준 완화
- raw DB row, raw JSON, token, 원 계좌번호를 report/Issue/PR/`commands.txt`에 기록

## PowerShell 실행 예시

아래 경로 placeholder를 승인된 실제 경로로 바꾸되 secret이나 원 계좌번호는 명령행에
넣지 않는다.

```powershell
Set-Location "<저장소 루트 절대경로>"
$python = (Resolve-Path ".\venv_64\Scripts\python.exe").Path
$db = "<checkpointed schema-62 운영 DB 절대경로>"
$migrationApplyReport = "<승인된 schema-61-to-62 migration apply raw.json 절대경로>"
$approvedMigrationApplySha256 = "<승인 기록에 고정된 lowercase 64-hex SHA-256>"
$runtimeLogs = @(
  "<승인된 Core runtime log 절대경로>",
  "<승인된 추가 runtime log 절대경로>"
)
$outDir = "reports\fast_track\fast_0_strict_requalification"

$actualMigrationApplySha256 = (Get-FileHash -LiteralPath $migrationApplyReport -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualMigrationApplySha256 -cne $approvedMigrationApplySha256) {
  throw "승인된 migration apply report SHA-256과 현재 파일이 다릅니다."
}

$checkArgs = @(
  "-B",
  "-m", "tools.ops_fast0_strict_requalification",
  "--db", $db,
  "--migration-apply-report", $migrationApplyReport,
  "--migration-apply-report-sha256", $approvedMigrationApplySha256,
  "--out-dir", $outDir
)
foreach ($runtimeLog in $runtimeLogs) {
  $checkArgs += @("--runtime-log", $runtimeLog)
}

& $python @checkArgs
$exitCode = $LASTEXITCODE
Write-Host "FAST-0R5 exit code: $exitCode"
```

`--runtime-log`는 필요한 파일 수만큼 반복한다. glob이나 디렉터리 전체를 넘기지 말고
승인된 파일을 명시한다. migration apply report와 그 report가 참조하는 preflight report는
과거 migration 증거 체인을 검증하기 위한 read-only 입력이다.

현재 단계의 예상 process exit code는 **`2`**다. offline evidence가 `PASS`하더라도 별도
최종 OBSERVE smoke가 이 checker 범위에서 실행되지 않고 runtime log coverage도 항상
`BOUNDED_HISTORICAL_FILES`/non-authoritative이므로 FAST-0은 `BLOCKED`로 남는다. exit `2`를
DB write나 기준 완화로 없애지 않는다.

다음 정책 값은 fixed contract다. CLI에 노출된 인자라도 기본값과 다르면 fail-closed로
거부된다. 단, `--min-free-bytes`는 1 GiB 이상으로 더 엄격하게만 설정할 수 있다.

| 항목 | fixed 값 | 의미 |
|---|---:|---|
| pipeline max age | 60초 | 현재 source drift qualification 기준 |
| incremental retry | 3회 | `attempts>=3`이면 blocker |
| incremental backlog | warn 100 / fail 1000 | ordinary queued row 자체는 warning |
| incremental stale | warn 30초 / fail 300초 | fail 구간은 blocker |
| projection stale PROCESSING | 120초 | PROCESSING 1건도 quiescent blocker이며 stale이면 blocker 추가 |
| projection pending backlog | warn 1000 / fail 10000 | PENDING 자체는 warning |
| projection recent pending | 300초 window에서 100 초과 fail | 현재 backlog readiness |
| condition pending | blocking 100 초과 / recent 10 초과 fail | condition-event 제한 |
| DB volume 최소 여유 공간 | 1 GiB | 이보다 작게 완화 금지; output directory는 별도 수동 확인 |

## Strict evidence 계약

### DB identity와 파일 불변

- 코드 target schema와 DB `app_metadata.schema_version`이 정확히 `62`
- `app_metadata.app_name=suseok-trader-v2`, 두 metadata key가 각각 정확히 1행
- main DB는 regular file이며 unsafe hardlink alias가 없음
- 하나의 pinned read handle/file identity가 snapshot 전체에 유지되고 DB pathname identity가
  바뀌지 않음. raw file ID는 report에 기록하지 않음
- 실행 전·후 `-wal`, `-shm`, `-journal` 부재
- `mode=ro&immutable=1`, `PRAGMA query_only=ON`
- `PRAGMA quick_check(1)` 결과가 정확히 `ok`
- 동일한 read transaction 안에서 aggregate evidence 수집
- main DB size, mtime, SHA-256이 실행 전·후 동일
- schema-62 critical table/index/trigger의 token-aware SQL fingerprint, owner와 exact persistent
  trigger set 검증
- migration apply report의 승인된 file SHA-256, exact apply function, `61→62`, 세 quick check,
  backup, lease zero-map, 18개 target probe, preservation flag 검증
- 참조 preflight의 `exact-61-to-62-v2` contract와 report-chain SHA-256 검증
- 현재 DB main size/SHA-256/mtime가 migration apply 직후 fingerprint와 정확히 일치

logical preservation은 migration preflight/apply의 preservation chain과 현재 물리 파일 hash
연속성을 검증한다. 현재 모든 table row를 다시 digest하는 독립적인 full row-loss scan으로
해석하지 않는다.

### Aggregate-only 안전 증거

보고서에는 고정된 field 이름과 count/boolean/digest만 기록한다. 다음 함수의 raw 응답을
그대로 복사하지 않는다. 이 응답에는 ID, detail, error message 또는 sample이 포함될 수
있다.

- projection outbox latest error/list
- projection watermark row/recent error list
- order-plan duplicate ID sample
- runtime lock owner/detail list
- Gateway command type 전체 key 목록

필수 aggregate 계약은 다음과 같다.

- projection outbox `ERROR=0`, `DEAD_LETTER=0`, unknown status `0`
- projection watermark의 unresolved count는
  `projection_event_results.status='ERROR'` aggregate로 별도 계산하며 `0`
- `projection_watermarks.last_error_*` non-null 자체를 unresolved로 세지 않음
- order-plan partial UNIQUE index의 이름, 대상 table/column, unique/partial flag와
  `WHERE order_plan_id IS NOT NULL` predicate가 모두 정확히 일치
- order-plan duplicate, column/evidence mismatch, missing backfill, invalid evidence count `0`
- `gateway_commands`, `gateway_command_dedupe_keys`,
  `gateway_order_broker_boundaries` 전체 history에서 normalized `modify_order` count 각각 `0`
- strict offline runtime lock count `0`
- runtime fencing table 존재, non-positive sequence와 lock/fence mismatch count `0`
- `EVALUATION_RUN_FENCE_LOST`, `OWNER_ALIVE_AFTER_TTL`은 runtime log별 raw line 대신
  고정된 marker count만 기록
- lifecycle active/manual/effective blocker `0`; historical canonical count는 삭제하거나
  현재 blocker로 재해석하지 않음
- broker boundary, pipeline, dead-letter 등 다른 FAST-0 gate도 canonical과 effective
  count를 섞지 않고 기존 계약대로 별도 기록
- projection latest reconcile이 존재하고 `PASS`, invalid effective-skip event type `0`

projection `PENDING`은 그 자체로 `ERROR`가 아니다. backlog/stale-processing 계약으로
별도 판정하며 오류 count에 합치지 않는다. malformed metadata/status/lineage 또는 aggregate
보존식 불일치는 evidence failure다. runtime fencing sequence row는 lock release 후에도
보존되므로 fence table 전체 row count가 `0`일 필요는 없다. historical log marker가 `0`인
것은 완전한 runtime 구간 또는 현재 owner liveness 증거가 아니다.

## 결과 해석과 exit code

| evidence_status | fast0_status | process exit | 의미 |
|---|---|---:|---|
| `PASS` | `PASS` | `0` | 외부 최종 adjudication까지 포함한 개념상 상태. 현재 checker 단독으로는 산출 불가 |
| `PASS` | `BLOCKED` | `2` | 증거 수집은 정상이나 blocker 또는 최종 OBSERVE smoke가 남음 |
| `FAIL` | `BLOCKED` | `2` | DB identity, quick check, sidecar, fingerprint, 입력 report/log 또는 evidence 계약 실패 |

예외, malformed input 또는 runtime log 누락도 fail-closed exit `2`다. 정상 실행 보고서에서는
evidence failure와 qualification blocker를 구분하지만, 입력 검증 중 예외가 나면 민감정보를
숨긴 `error_type`만 console에 출력하고 timestamp 산출물 자체가 생성되지 않을 수 있다.

## Evidence 산출물

```text
reports/fast_track/fast_0_strict_requalification/<timestamp>/
  summary.md
  raw.json
  commands.txt
```

- `raw.json`은 aggregate-only이며 원 DB 행이나 원 로그 줄을 포함하지 않는다.
- `commands.txt`는 redacted 재현 명령만 포함한다.
- 입력 DB와 runtime log의 경로 원문은 기록하지 않는다. DB/log의 size·mtime·SHA-256 및
  migration/preflight report SHA-256 중심으로 기록한다.
- Issue/PR에는 `summary.md`의 비민감 verdict와 count만 옮긴다.

## 2026-07-15 운영 실행 결과

- evidence: `reports/fast_track/fast_0_strict_requalification/20260715T211029.344659Z/`
- R5 evidence status: **`PASS`**
- R5 FAST-0 status / process exit: **`BLOCKED` / `2`**
- DB: app/schema identity `suseok-trader-v2/62`, `quick_check(1)=ok`, schema manifest
  `50/50`, pipeline lineage schema PASS
- main DB: `20,465,156,096` bytes,
  SHA-256 `2395197917ab01d940d1005e06177df628afc750440ab83df09e7dd2d74fd249`,
  실행 전·후 size/mtime/SHA-256 동일, sidecar 없음
- DB identity pin: `WINDOWS_READ_HANDLE_DENY_WRITE_DELETE`, snapshot 전체 유지/path stable PASS
- writer probe: 실행 전·후 각각 PASS. 전체 실행 구간 writer 부재 증거는 아님
- migration baseline: 승인 apply SHA, producer, preflight chain, preservation contract 모두 PASS
- order-plan: exact partial UNIQUE contract PASS, intent `7`, duplicate/mismatch/missing
  backfill/invalid evidence 모두 `0`
- broker boundary: raw/effective `UNCONFIRMED=3/0`; raw 보존 계약 때문에 semantic blocker
- incremental: raw/effective dead-letter `38/38`; queue/retry-exhausted/stale-fail `0/0/0`
- pipeline: canonical/qualification `FAIL/BLOCKED`; historical closed `149`, missing-candidate
  manual review `9`, stale-other-date manual review `0`, active-current `1`, disposition pending `158`
- lifecycle: canonical/qualification/strict `BLOCKED/PASS/PASS`; historical audit `611`,
  active/manual/effective blocker 모두 `0`
- projection: readiness `WARN`, latest reconcile `PASS`, pending `717`, processing/error/
  dead-letter/event-result-error `0/0/0/0`, invalid effective skip `0`; APPLIED-without-success
  `65,159`는 현재 정책상 diagnostic warning
- runtime: lock `0`, fence row `1`, fencing contract PASS; DB와 6개 bounded log의
  `EVALUATION_RUN_FENCE_LOST`/`OWNER_ALIVE_AFTER_TTL` count 모두 `0`
- `modify_order`: command/dedupe-key/broker-boundary 세 표면 모두 `0`
- 최종 OBSERVE smoke: 범위 밖이라 미실행

남은 qualification blocker는 정확히 다음 6개다.

1. `BROKER_BOUNDARY_SEMANTIC_BLOCKER`
2. `BROKER_RAW_UNCONFIRMED_PRESENT`
3. `INCREMENTAL_EFFECTIVE_DEAD_LETTER_PRESENT`
4. `PIPELINE_QUALIFICATION_NOT_PASS`
5. `RUNTIME_LOG_COVERAGE_NOT_AUTHORITATIVE`
6. `FINAL_OBSERVE_SMOKE_NOT_RUN`
