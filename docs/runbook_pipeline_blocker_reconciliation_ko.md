# FAST-0R7 pipeline blocker reconciliation strict read-only runbook

## 1. 목적

FAST-0R7은 승인된 FAST-0R6 blocker-resolution plan과 동일한 schema-62 운영 DB snapshot을
strict read-only로 다시 읽어, pipeline disposition 예상 대상의 현재 blocker를 source,
downstream과 outcome으로 분리하는 evidence 단계다.

R7은 다음 질문에 aggregate evidence로 답한다.

- 과거 active event와 현재 latest active source가 구분되는가?
- candidate의 `active_source_count`와 latest projection이 일치하는가?
- active downstream, unsafe command, broker-boundary unknown/unconfirmed 또는 repair 대상이 있는가?
- 현재 DB preview가 이미 eligible한가, 외부 authoritative evidence가 필요한가, 또는 계약 검토가
  먼저 필요한가?

R7은 기존 eligibility 또는 effective disposition을 바꾸지 않는다. Apply, repair, targeted rebuild를
승인하거나 실행하지 않으며, 운영 DB를 변경하거나 Core/Gateway/worker를 시작하지 않는다.
`evidence_status=PASS`, `reconciliation_status=COMPLETE` 또는 exit code 0은 운영 write 승인이 아니다.

## 2. 입력과 범위

### 2.1 입력 계약

실제 명령은 다음 세 입력을 고정한다.

- checkpoint가 끝난 schema-62 운영 DB 절대경로
- 승인된 R6 blocker-resolution plan `raw.json`
- 위 R6 report를 승인 기록과 독립적으로 고정한 SHA-256

R6 report는 `fast0-blocker-resolution-plan.v1`, evidence `PASS`, plan `COMPLETE`, read-only,
database-write false, identifier-recording false와 pinned DB fingerprint를 만족해야 한다. R5 report와
그 SHA-256은 R6 plan이 이미 결속한 source chain으로 검증한다. R7 CLI가 R5 report를 별도 입력으로
다시 받지는 않는다.

2026-07-16 strict read-only 실행과 독립 hash 검증을 완료한 승인 evidence는 다음과 같다.

- `reports/fast_track/fast_0_pipeline_blocker_reconciliation/20260716T011001.053891Z/raw.json`
- SHA-256: `469e827cd586cc3550913afbdc3db5307c48188b71707cc58896fe68a6498849`
- `evidence_status=PASS`, `reconciliation_status=COMPLETE`
- `execution_readiness=PREPARATION_REQUIRED`, `apply_authorized=false`

다른 timestamp 또는 hash를 이 baseline으로 자동 대체하지 않는다.

### 2.2 전체 inventory와 target 범위

R7은 R6와 같은 full pipeline inventory를 page 끝까지 다시 읽고 시작·종료 count/digest를 비교한다.
첫 page나 최대 500행 결과만으로 전체를 판정하지 않는다.

상세 reconciliation 대상은 R6에서 다음 expected action을 가진 subject다.

- `DISPOSE_EXPIRED_PLAN_READY`
- `DISPOSE_ORPHAN_PIPELINE_OBSERVATION`

R6에서 관찰된 expired-plan 11과 orphan 9는 입력 기준선이며 R7 결과를 정하는 hard-coded 상수가
아니다. 최종 R7은 동일한 11/9 action count와 full/collected 159, inventory 시작·종료 digest
`2faf282ae4c113bd5fc0c232cc6da7d01008dfb9ec248247d94db244d186ccbf`를 확인했다. 현재 DB의
action count, full count 또는 inventory digest가 이 승인 baseline과 다르면 새 baseline으로 자동
수용하지 않고 fail closed한다.

Legacy non-ready 138과 active-current 1은 full inventory baseline에는 포함되지만 위 두 action의
reconciliation target이 아니다. R7은 legacy eligibility를 만들거나 active-current rebuild preview를
수행하지 않는다. Legacy authoritative evidence resolver와 active-current preview는 계속 별도
Phase D/E 범위로 남는다.

### 2.3 수집 항목

각 target은 현재 DB에서 한 번 더 disposition preview하고 다음 safe aggregate를 분류한다.

- pipeline fingerprint와 subject version
- source/downstream/boundary CAS hash
- candidate 존재 여부와 active-source count
- historical source event의 active audit count
- latest source projection의 active count
- candidate/latest projection 일치 여부
- active downstream, unsafe/malformed/unbound command·artifact count
- effective broker-boundary unknown/unconfirmed count
- preview eligibility, safety contract와 reason code

Report는 target 원문을 기록하지 않고 action/source/downstream/outcome count와 target-set digest만
공개한다. 외부 process와 broker의 현재 상태는 이 도구가 검증하지 않으며, DB snapshot 밖의
manual evidence를 자동 생성하거나 추론하지 않는다.

## 3. strict read-only 안전 계약

R7은 다음 순서를 지킨다.

1. R6 report를 stable-read하고 전달받은 SHA-256과 exact match시킨다.
2. DB main 외 WAL, SHM과 rollback journal이 없음을 확인한다.
3. 실행 전 writer/open-handle probe를 통과한다.
4. Windows pinned read handle로 DB identity를 고정한다.
5. URI `mode=ro`, immutable, `PRAGMA query_only=ON`으로 하나의 deferred snapshot을 연다.
6. `quick_check(1)=ok`, 애플리케이션/DB identity, schema 62와 runtime lock 0을 확인한다.
7. full pipeline inventory와 target preview를 같은 snapshot에서 읽는다.
8. transaction을 rollback하고 connection을 닫는다.
9. sidecar 부재, path identity, DB fingerprint와 writer probe를 다시 확인한다.

Report의 `whole_window_writer_absence_proven`은 identity pin 방식과 전후 probe가 모두 계약을
만족할 때만 true다. Report 파일 생성은 output directory에 대한 파일시스템 write이며 운영 DB
write가 아니다. Report 생성 실패를 이유로 DB 작업을 재시도하지 않는다.

최소 safety flag는 다음과 같다.

- `read_only=true`
- `observe_only=true`
- `database_write_performed=false`
- `eligibility_changed=false`
- `apply_authorized=false`
- `identifiers_recorded=false`
- `raw_rows_recorded=false`
- `raw_payloads_recorded=false`
- Core/Gateway/worker/order/broker operation invoked: 모두 false
- `live_sim_allowed=false`, `live_real_allowed=false`

`side_effect_scope=THIS_TOOL_EXECUTION_ONLY`다. 외부 process 또는 broker 상태까지 검증했다고 확대
해석하지 않는다.

## 4. 실행 명령

아래는 재검증용 redacted 명령 형식이다. 운영 DB 절대경로는 승인된 실행 환경에서만 주입하고 공개
문서에 기록하지 않는다. R6 입력 report와 SHA-256은 위 승인값에 고정한다.

```powershell
$python = "<승인된 Python 실행 파일>"
$db = "<checkpointed schema-62 운영 DB 절대경로>"
$r6Report = "reports\fast_track\fast_0_blocker_resolution_plan\20260715T232358.741099Z\raw.json"
$r6ReportSha256 = "a51303b49062c0e7e3ab28cdfc2d4c414844384900a010df47c8023dfa75996e"
$outDir = "reports\fast_track\fast_0_pipeline_blocker_reconciliation"

& $python -B -m tools.ops_pipeline_blocker_reconciliation `
  --db $db `
  --blocker-plan-report $r6Report `
  --blocker-plan-report-sha256 $r6ReportSha256 `
  --out-dir $outDir
```

명령은 저장소 루트에서 실행한다. 도구 기본 output root는
`reports/pipeline_blocker_reconciliation`이지만 승인 실행은 위 FAST-0 evidence root를 사용했다.
`commands.txt`에는 DB·Python의 원 경로를 넣지 않는 redacted placeholder만 기록한다.

### 4.1 2026-07-16 승인 결과

- action: expired-plan 11, orphan 9
- preview eligible: 전체 9, expired-plan 0, orphan 9
- source: `CANDIDATE_ABSENT=9`, `CURRENT_ACTIVE=10`, `PROJECTION_INCONSISTENT=1`, 나머지 0
- downstream: `SAFE_DB_PROVEN=17`, `MANUAL_BROKER_EVIDENCE_REQUIRED=1`,
  `UNSAFE_COMMAND_RECONCILIATION_REQUIRED=2`, 나머지 0
- outcome: `ACTIVE_SOURCE_BLOCKED=10`, `MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED=9`,
  `REPAIR_REQUIRED=1`, 나머지 0
- target-set SHA-256: `de9b9d54793b0d21ce13c079588cd9b5baf52a6230f6ec054ebfc3ea77d99fbf`
- invalid item 0, `quick_check(1)=ok`, runtime lock 0, schema invalid object 0
- 전후 DB fingerprint 동일, whole-window writer absence proven, raw ID/path/payload 기록 0

따라서 R7 evidence는 완전하지만 처리 준비는 끝나지 않았다. Expired-plan 11건은 현재 자동 apply할
수 없고, orphan 9건도 DB preview eligibility만으로 authoritative evidence를 대신할 수 없다.

## 5. 분류 계약

### 5.1 Source state

| 상태 | 판정 의미 |
|---|---|
| `CANDIDATE_ABSENT` | candidate row와 active source가 없어 orphan manual evidence 경로가 필요함 |
| `CURRENT_ACTIVE` | latest active source 또는 candidate active-source count가 0보다 큼 |
| `HISTORICAL_EVENT_ONLY` | latest/candidate active는 0이지만 append-only historical event에 active 이력이 남음 |
| `NO_ACTIVE_SOURCE` | historical/latest/candidate active count가 모두 0이고 projection이 일치함 |
| `ORPHAN_ACTIVE_SOURCE_PRESENT` | candidate는 없지만 historical 또는 latest active source가 남아 있음 |
| `PROJECTION_INCONSISTENT` | latest active count와 candidate active-source count가 불일치함 |
| `UNKNOWN` | 필수 count/boolean 계약이 없거나 malformed라 안전하게 판정할 수 없음 |

Historical active event는 감사 이력이고 latest projection은 현재 상태다. 둘을 하나의 `active_count`로
합쳐 현재 source라고 오인하지 않는다. 다만 historical active event가 남은 expired-plan 대상은
기존 eligibility 계약상 자동 eligible이 되지 않으며 별도 계약 검토 대상으로 분류한다.

Preview source의 exact public key는 `historical_event_active_count`, `latest_active_count`,
`candidate_active_source_count`, `source_projection_consistent`, `source_state_classification`이다.
기존 `active_count`는 `historical_event_active_count`의 deprecated audit alias로만 유지한다. 하위
`source_state_classification`의 `SOURCE_PROJECTION_INCONSISTENT`, `LATEST_ACTIVE_SOURCE_PRESENT`,
`HISTORICAL_EVENT_ACTIVE_ONLY`, `NO_ACTIVE_SOURCE_PRESENT`를 R7 aggregate source state와 혼동하지
않고 결정적으로 매핑한다. `source_projection_consistent`는 candidate가 없고 latest active도 0이면
비교 비대상인 `null`, 비교 가능한 불일치면 false, 그 밖의 일치면 true다. 정상 orphan의 `null`을
`UNKNOWN`으로 오인하지 않으며, candidate가 없는데 historical/latest active가 남으면
`ORPHAN_ACTIVE_SOURCE_PRESENT`로 fail closed한다.

### 5.2 Downstream state

| 상태 | 판정 의미 |
|---|---|
| `REPAIR_REQUIRED` | malformed/unbound command 또는 artifact가 있어 코드·데이터 repair 설계가 필요함 |
| `ACTIVE_DOWNSTREAM_BLOCKED` | active intent/order/기타 downstream 또는 unbound active artifact가 남음 |
| `MANUAL_BROKER_EVIDENCE_REQUIRED` | effective broker-boundary가 unknown 또는 unresolved임 |
| `UNSAFE_COMMAND_RECONCILIATION_REQUIRED` | unsafe gateway command가 남아 strict reconcile이 필요함 |
| `SAFE_DB_PROVEN` | 위 downstream blocker를 현재 DB snapshot에서 찾지 못함 |
| `UNKNOWN` | 필수 downstream count가 누락되었거나 음수/bool 등 nonnegative integer 계약을 위반함 |

필수 count가 malformed면 우선 `UNKNOWN`으로 fail closed하고 outcome은 `INVALID_EVIDENCE`가 된다.
정상 count의 분류 우선순위는 repair, active downstream, broker evidence, unsafe command, safe DB
순서다. 하나의 subject가 여러 raw reason을 가질 수 있어도 downstream state는 이 우선순위에 따라
하나만 가진다.

### 5.3 Reconciliation outcome

| Outcome | 의미와 허용되는 해석 |
|---|---|
| `INVALID_EVIDENCE` | preview safety, source 또는 downstream 계약이 불완전함; evidence 실패 대상 |
| `REPAIR_REQUIRED` | projection 불일치나 malformed/unbound downstream repair가 먼저 필요함 |
| `ACTIVE_SOURCE_BLOCKED` | current active source 또는 orphan active source가 남아 disposition 불가 |
| `ACTIVE_DOWNSTREAM_BLOCKED` | active downstream이 남아 disposition 불가 |
| `MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED` | orphan 또는 boundary/unsafe command에 외부 authoritative evidence가 필요함 |
| `DB_PREVIEW_ELIGIBLE` | 현재 DB preview가 이미 eligible함; 별도 apply 승인은 여전히 필요함 |
| `DB_RECONCILIATION_CANDIDATE` | historical-event-only라 eligibility 계약 검토가 필요함; eligible을 뜻하지 않음 |
| `CONTRACT_BLOCKED` | 위 경로로 안전하게 승격할 근거가 없으므로 계약상 차단됨 |

Outcome은 eligibility를 override하지 않는다. 특히 `DB_RECONCILIATION_CANDIDATE`는 새로운 eligible
bucket이 아니고, `DB_PREVIEW_ELIGIBLE`도 apply 권한이 아니다.

Preview 내부 계약도 별도로 보존한다. `eligible`, `status`, `reason_codes`는 서로 일치해야 하고
`expected_action`은 요청 action과 같아야 한다. Eligible expired-plan은 candidate가 존재하는
`NO_ACTIVE_SOURCE`, eligible orphan은 `CANDIDATE_ABSENT` 조합만 허용한다. 이 action/source 결속이
깨지면 evidence를 invalid로 처리한다.

### 5.4 Count, digest와 alias

Report는 다음을 aggregate로 보존한다.

- full/collected/page count와 inventory 시작·종료 digest
- expected action count
- source/downstream/outcome count
- preview eligible 전체 및 action별 count, manual-evidence required, repair-required count
- blocker reason count
- 전체 target, expired-plan target, orphan target SHA-256

Internal 정렬에는 원 candidate ID를 사용하되 공개 mapping은 기록하지 않는다. Alias 형식은 전체
target `C{ordinal:02d}`, expired-plan `E{ordinal:02d}`, orphan `O{ordinal:02d}`이며 alias와 원
식별자의 대응표는 report에 남기지 않는다.

Reason count는 중첩될 수 있으므로 합계를 distinct subject count로 사용하지 않는다. Primary
source/downstream/outcome count는 각 계약의 conservation을 만족해야 하고, reason은 원인 감사용으로
별도 보존한다.
Action별 preview-eligible 합계는 전체 preview-eligible count와 같아야 하며, expired-plan eligible
count는 `DB_PREVIEW_ELIGIBLE` outcome count와 같아야 한다.

## 6. 판정과 exit 의미

R7은 evidence validity와 운영 readiness를 분리한다.

- evidence failure가 없으면 `evidence_status=PASS`, `reconciliation_status=COMPLETE`다.
- evidence failure가 있으면 `FAIL/BLOCKED`다.
- 정상 evidence에도 후속 증거·repair·계약 검토·별도 apply 승인이 남으므로
  `execution_readiness=PREPARATION_REQUIRED`다.
- CLI는 evidence `PASS`일 때 exit code 0, 아니면 2를 반환한다.

Preparation requirement는 실제 outcome에 따라 manual authoritative evidence, data repair design,
active source/downstream reconcile, eligibility contract review와 separate apply approval을 구분한다.
Legacy authoritative evidence resolver requirement는 R7에서 계속 남는다.

Exit code 0은 report가 계약에 맞게 생성됐다는 뜻일 뿐이다. Apply/run을 시작하거나 FAST-0을
`PASS`로 바꾸는 근거가 아니다.

## 7. 산출물과 개인정보 계약

실행은 timestamp directory에 다음 파일을 만든다.

- `raw.json`: aggregate-only machine-readable evidence
- `summary.md`: evidence/reconciliation/readiness와 핵심 outcome 요약
- `commands.txt`: 실제 값이 제거된 재현 명령과 apply 비승인 문구

공개 산출물에는 count, boolean, enum, SHA-256, target-set digest와 alias 형식만 허용한다.
다음 값은 금지한다.

- 원 candidate, source, plan, order, command, boundary 또는 disposition ID
- alias와 원 식별자의 대응표
- 원 계좌번호, token, password, credential 또는 operator secret
- DB/report/log/evidence의 원본 절대경로
- raw row, payload, error/detail, evidence 본문 또는 sample

Source R6 report의 원문과 경로도 embed하지 않고 승인 SHA-256과 safe aggregate만 기록한다. Report
생성 직전에 key와 value 양쪽을 redaction하고 aggregate-only 검사를 통과해야 한다.

## 8. 전면 금지 사항

- `--apply`, `--run`, retry, drain, reset, revoke 또는 bulk disposition 기능 추가·사용
- 운영 DB INSERT/UPDATE/DELETE, DDL, migration, initialization 또는 일반 RW connection
- Core/Gateway/worker/scheduler/API/broker process 시작
- `LIVE_SIM`/`LIVE_REAL` 활성화, 주문·정정·취소 또는 command 생성
- 기본 `.env` 또는 임의 `TRADING_ENV_FILE`을 통한 실행 환경 우회
- count/status/CAS를 수동 변경해 eligibility를 만드는 행위
- historical active event를 current active projection으로 합산하거나 그 반대로 무시
- external evidence 없이 unknown을 false/no-drift/terminal로 추론
- legacy 138 또는 active-current 1을 R7 target으로 확대
- 500행 제한 결과를 full inventory 증거로 사용
- R6 target count를 hard-code하거나 snapshot drift를 새 baseline으로 자동 수용
- blocker reason count를 합산해 distinct 대상 수로 보고
- report에 원 식별자, 원 경로 또는 민감정보 기록

## 9. 즉시 중단 조건

다음 중 하나라도 발생하면 evidence를 `PASS`로 기록하지 않고 apply/run 단계로 넘어가지 않는다.

- R6 report stable-read 또는 SHA-256 불일치
- R6 contract/evidence/plan/read-only/privacy 계약 불일치
- report가 참조하는 DB main fingerprint와 현재 DB 불일치
- WAL/SHM/journal 출현, writer/open handle 또는 runtime lock 존재
- `quick_check(1)` 실패, schema/app identity 또는 identity pin 실패
- 실행 전·후 DB fingerprint 변화
- full/collected/action count 또는 inventory digest가 R6 baseline과 불일치
- page contract, target preview safety 또는 CAS 불일치
- invalid item, source `UNKNOWN` 또는 `INVALID_EVIDENCE` outcome 발생
- `eligibility_changed`, DB write 또는 tool action flag가 안전값과 다름
- report write, aggregate-only 검사 또는 privacy redaction 실패

실패 후 새 baseline이나 새 report hash를 자동 승인하지 않는다. 같은 승인 입력과 DB identity를
strict read-only로 reconcile하고, 코드·데이터 결함이면 별도 repair PR/승인 뒤 다시 실행한다.

## 10. 완료 기준

### 10.1 구현 완료

- `tools.ops_pipeline_blocker_reconciliation` parser와 이 runbook의 option이 일치한다.
- R6 report hash/contract, DB fingerprint, schema 62와 inventory/action baseline을 fail closed로 검증한다.
- immutable/query-only 단일 snapshot, pinned identity, sidecar/writer guard와 full pagination을 테스트한다.
- historical event, latest projection와 candidate active count를 분리하고 projection 불일치를 차단한다.
- source/downstream/outcome의 모든 enum과 우선순위를 회귀 테스트한다.
- preview eligibility가 바뀌지 않고 apply가 항상 비승인임을 테스트한다.
- raw ID/path/payload/credential이 모든 report surface에서 제거되는 privacy 테스트가 있다.
- 운영 DB, Core/Gateway와 LIVE 설정 없이 fixture DB와 synthetic R6 report로 검증 가능하다.

### 10.2 실제 evidence 완료

구현 완료와 실제 운영 evidence 완료는 분리한다. 실제 R7 evidence는 별도 승인된 strict read-only
실행 후 다음을 모두 만족해야 한다.

- 승인된 R6 SHA-256, DB main fingerprint, schema와 inventory/action baseline exact match
- `quick_check(1)=ok`, sidecar/writer/lock 0, 전후 DB fingerprint 동일
- source/downstream/outcome count와 target digest가 완전하고 invalid item 0
- `evidence_status=PASS`, `reconciliation_status=COMPLETE`
- `execution_readiness=PREPARATION_REQUIRED`
- `eligibility_changed=false`, `apply_authorized=false`, `database_write_performed=false`
- Core/Gateway/worker/order/broker operation invoked 0
- aggregate-only privacy/redaction 검증 PASS
- 생성된 R7 report 경로와 SHA-256을 독립 검증하고 이 문서에 고정

위 조건을 만족해도 R7은 blocker를 해소하지 않는다. Manual evidence, repair, eligibility contract,
preview/apply, legacy resolver, active-current rebuild와 최종 FAST-0 requalification은 각각 별도
승인과 새 evidence가 필요하다.
