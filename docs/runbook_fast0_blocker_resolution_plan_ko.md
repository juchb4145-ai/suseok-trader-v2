# FAST-0 blocker 해소 캠페인 runbook

## 1. 목적과 범위

이 문서는 FAST-0 재검증을 막고 있는 incremental dead-letter와 pipeline blocker를 안전하게 해소하기 위한 **계획·승인 runbook**이다. 최신 strict read-only 증거를 기준으로 작업 순서, 승인 경계, 중단 조건을 고정한다.

이 문서 자체는 다음 행위를 승인하지 않는다.

- 운영 DB write 또는 migration
- resolution apply, targeted rebuild run, retry 또는 drain
- Core, Gateway, worker, scheduler의 시작
- `LIVE_SIM` 또는 `LIVE_REAL` 활성화
- 주문·정정·취소 또는 broker command 생성

아래에는 계획 생성 명령만 제공한다. 실제 apply/run 명령은 별도 캠페인 manifest와 명시적 승인이 준비된 운영 단계에서만 작성하고 실행해야 한다.

## 2. 고정된 authoritative evidence

### 2.1 blocker-resolution plan

- 증거 디렉터리: `reports/fast_track/fast_0_blocker_resolution_plan/20260715T232358.741099Z/`
- `raw.json` SHA-256: `a51303b49062c0e7e3ab28cdfc2d4c414844384900a010df47c8023dfa75996e`
- `evidence_status`: `PASS`
- `plan_status`: `COMPLETE`
- `execution_readiness`: `PREPARATION_REQUIRED`

`PASS`와 `COMPLETE`는 계획 증거가 일관되게 생성됐다는 뜻일 뿐이다. 운영 write가 준비됐거나 승인됐다는 뜻이 아니다. 계획 CLI가 exit code 0을 반환하더라도 동일하다.

### 2.2 입력 FAST-0 strict requalification evidence

- 증거 파일: `reports/fast_track/fast_0_strict_requalification/20260715T211029.344659Z/raw.json`
- 승인된 SHA-256: `246eb4dcc3b87e1d31a7e4e29c80ee2113cf525ef3c2b236087e54c7f9c76902`
- 계약 버전: v1
- evidence 판정: `PASS`
- FAST-0 판정: `BLOCKED`

계획을 다시 생성할 때는 위 입력 파일과 SHA-256을 함께 고정한다. 입력 파일의 내용 또는 해시가 달라졌다면 기존 계획을 재사용하지 않는다.

### 2.3 운영 DB read-only 기준선

최신 evidence에서 확인된 기준선은 다음과 같다.

- 애플리케이션/DB schema: `suseok-trader-v2/62`
- 연결 계약: `mode=ro`, immutable, `query_only`, 단일 deferred snapshot
- `quick_check`: `ok`
- schema manifest: 50/50 ready
- runtime execution lock: 0건
- 점검 전·후 DB sidecar: 없음
- 점검 전·후 writer probe: `PASS`
- 전체 점검 구간 writer 부재 증명: true
- DB identity pin: `PASS` — Windows write/delete deny 적용
- main DB 크기: 20,465,156,096 bytes
- main DB SHA-256: `2395197917ab01d940d1005e06177df628afc750440ab83df09e7dd2d74fd249`
- 점검 전·후 크기와 해시: 동일
- 이 plan 도구가 Core/Gateway/worker 시작 또는 주문/broker operation을 호출하지 않음
- 외부 process/broker 상태 검증: 미수행; Phase A와 후속 운영 evidence에서 별도 검증 필요

이 기준선 중 하나라도 달라지면 최신 계획은 현재 운영 DB를 대표하지 못한다.

### 2.4 FAST-0R7 pipeline blocker reconciliation

R7은 R5 evidence를 결속한 승인 R6 report와 동일한 DB identity를 strict read-only로 대조해
expired-plan/orphan blocker의 현재 source/downstream/outcome을 재현하는 evidence 단계다. R7은
기존 preview/legacy eligibility 또는 effective 상태를 바꾸지 않고 apply를 승인하지 않는다.
승인 evidence는
`reports/fast_track/fast_0_pipeline_blocker_reconciliation/20260716T011001.053891Z/raw.json`,
SHA-256은 `469e827cd586cc3550913afbdc3db5307c48188b71707cc58896fe68a6498849`다.
결과는 `PASS/COMPLETE/PREPARATION_REQUIRED`, target 20, invalid 0이며 DB write와 apply 승인은 0이다.
구현 및 실행 계약은
`docs/runbook_pipeline_blocker_reconciliation_ko.md`를 따른다.

## 3. blocker inventory

### 3.1 Incremental dead-letter

| 항목 | 수치/상태 |
|---|---:|
| raw/effective | 38/38 |
| historical pending disposition | 38 |
| target/eligible/blocked/invalid | 38/38/0/0 |
| inventory completeness | complete |
| 실행 계약 | `ONE_ROW_PREVIEW_APPLY_VERIFY` |
| 공개 alias | `U01`~`U38` |
| 정렬 기준 | 내부 dead-letter 식별자 오름차순 |
| campaign manifest SHA-256 | `845b5b9bf82f1a2cebdda9ddf262627f3eda1e91fa53d1b027983284be43f31d` |
| inventory manifest SHA-256 | `f5043991b6d8a36b154c28140d42ffcdf382e3ec5d4c7985133d8d22c78c4878` |
| apply 승인 | false |

내부 식별자와 alias의 대응표는 제한된 로컬 캠페인 manifest에만 둔다. 이 문서나 PR, Issue, 일반 evidence 요약에는 기록하지 않는다.

### 3.2 Pipeline

| 항목 | 수치/상태 |
|---|---:|
| full/collected inventory | 159/159 |
| inventory consistency | consistent |
| reported/collected inventory digest | `2faf282ae4c113bd5fc0c232cc6da7d01008dfb9ec248247d94db244d186ccbf` / same |
| canonical/qualification | `FAIL` / `BLOCKED` |
| historical/missing/active/stale | 149/9/1/0 |
| disposition required | 158 |
| preview eligible/blocked | 9/149 |
| legacy targets/현재 eligible | 138/0 |
| manual evidence required | 9 |
| reconcile required | 11 |
| active-current targeted rebuild target | 1 |
| current-source drift | unknown 159, drift 0, no-drift 0 |
| apply 승인 | false |

추가로 고정할 digest는 다음과 같다.

- manual-evidence campaign SHA-256: `dac9be8867b72637bd0e35bb006802b149ed26ce8a0c37facc6d0d55856d4d3d`
- legacy target SHA-256: `ae0340405c617cea9f72e1b86ec88068fd9b86c8ecb93f316cf4e9b5a842480f`
- legacy eligible-empty SHA-256: `6080852d4852461c4051cb7e6e9bbff2c1ee2993d2931f63e673fcca3d544dfb`
- active-current target SHA-256: `07ceefcf721b9f1d6bd71913fba456f8ed70adb6ca41fb16a913fd3a58b2a77c`

preview-blocked reason은 다음과 같다.

- `PIPELINE_EXPECTED_ACTION_UNAVAILABLE`: 138
- `CANDIDATE_ACTIVE_SOURCE_COUNT_PRESENT`: 11
- `HISTORICAL_ACTIVE_SOURCE_PRESENT`: 11
- `UNSAFE_GATEWAY_COMMAND_PRESENT`: 2
- `BROKER_BOUNDARY_STATUS_UNKNOWN`: 1

reason 수치는 서로 겹칠 수 있으므로 합산해서 별도 대상 수로 해석하지 않는다. 현재 자동 historical disposition 대상은 0건이다. preview eligible 9건만을 전체 pipeline write 가능 대상으로 확대 해석해서도 안 된다.

현재 qualification blocker는 다음 네 범주다.

- `PIPELINE_MANUAL_REVIEW_PRESENT`
- `PIPELINE_ACTIVE_CURRENT_NON_PASS`
- `PIPELINE_CURRENT_SOURCE_DRIFT_UNKNOWN`
- `PIPELINE_DISPOSITION_PENDING`

## 4. 계획 재생성 명령

다음 명령은 저장소 루트에서 실행한다. 운영 DB는 checkpoint가 끝난 schema 62 파일을 절대경로로 지정한다. 명령은 DB를 strict read-only로 읽고 evidence 디렉터리만 생성한다.

```powershell
$python = (Resolve-Path ".\venv_64\Scripts\python.exe").Path
$db = "<checkpointed schema-62 운영 DB 절대경로>"
$fast0Report = "reports\fast_track\fast_0_strict_requalification\20260715T211029.344659Z\raw.json"
$approvedFast0ReportSha256 = "246eb4dcc3b87e1d31a7e4e29c80ee2113cf525ef3c2b236087e54c7f9c76902"
$outDir = "reports\fast_track\fast_0_blocker_resolution_plan"

& $python -B -m tools.ops_fast0_blocker_resolution_plan `
  --db $db `
  --fast0-report $fast0Report `
  --fast0-report-sha256 $approvedFast0ReportSha256 `
  --out-dir $outDir
$exitCode = $LASTEXITCODE
```

생성 후에는 새 `raw.json`의 SHA-256, 입력 FAST-0 report SHA-256, DB fingerprint, inventory count와
manifest digest를 독립적으로 대조한다. `generated_at`이 포함되므로 재실행 report SHA-256 자체는
달라질 수 있다. 입력 report SHA, DB fingerprint, inventory/collected digest, count와 campaign
manifest가 모두 승인 기준과 일치하는지를 판단 기준으로 사용한다.

exit code 0만으로 다음 phase를 시작하지 않는다. `execution_readiness=PREPARATION_REQUIRED`이면 준비 요건이 남아 있으므로 apply/run은 계속 금지된다.

## 5. 단계별 실행 계획

단계는 A부터 F까지 순서대로 진행한다. 각 단계의 완료 evidence와 승인 없이는 다음 단계로 자동 진행하지 않는다.

### Phase A — pre-apply 안전성 및 byte-identical backup

목표는 최초 운영 write 전에 DB identity와 복구 지점을 고정하는 것이다.

필수 조건:

1. 모든 DB writer와 관련 프로세스가 중지돼 있음을 전 구간에 걸쳐 증명한다.
2. main DB의 WAL/SHM/rollback-journal sidecar가 없음을 시작 직전 다시 확인한다.
3. runtime execution lock이 0건인지 strict read-only로 확인한다.
4. DB identity, 애플리케이션 식별자, schema 62, 크기와 SHA-256을 승인 기준선에 pin한다.
5. 충분한 디스크 여유를 확인한 뒤 byte-identical backup을 만든다.
6. 원본과 backup의 byte hash가 같고 backup `quick_check`가 `ok`인지 확인한다.
7. 해당 phase에서 사용할 campaign/inventory manifest SHA-256을 승인 기록에 고정한다.

Phase A의 backup은 파일시스템 write이지만 운영 DB write는 아니다. backup 생성이나 검증이 불완전하면 Phase B 이후로 진행하지 않는다.

### Phase B — incremental dead-letter 38건 append-only disposition

대상은 `U01`부터 `U38`까지 38건이며 campaign manifest SHA-256은 `845b5b9bf82f1a2cebdda9ddf262627f3eda1e91fa53d1b027983284be43f31d`로 고정한다.

각 alias에 대해 반드시 다음 원자적 절차를 한 건씩 수행한다.

1. 최신 상태를 strict read-only로 다시 확인한다.
2. 한 건 preview를 만들고 expected action, 대상 fingerprint, CAS 조건과 manifest digest를 검증한다.
3. 그 한 건에 대한 별도 승인 범위 안에서만 append-only resolution을 적용한다.
4. 같은 요청을 strict read-only로 검증해 raw row 불변, effective 상태 해소, 다른 row와 주문 상태 불변을 확인한다.
5. evidence가 완전할 때만 다음 alias로 이동한다.

원본 dead-letter row를 update/delete하지 않는다. bulk apply, retry sweep, dead-letter reset으로 대체하지 않는다. commit 결과가 불명확하면 새 request 또는 새 resolution을 만들지 말고 같은 요청을 먼저 reconcile한다.

완료 기준:

- raw count 38 유지
- effective count 0
- alias별 preview/apply/verify evidence 완결
- 주문, command, broker-boundary와 다른 DB 영역의 비의도 delta 0

### Phase C — pipeline manual evidence 9건과 blocker 11건 reconcile

Phase B의 append-only disposition은 DB main fingerprint를 바꾼다. 따라서 R6/R7 base에 직접 결속된
R8 preflight를 Phase B 뒤에 그대로 재사용할 수 없다. Phase B 완료 report chain과 최종 post-state
fingerprint를 R8에 전이 결속하는 R9 handoff 계약이 먼저 준비돼야 하며, 그 전에는 단계 순서를 임의로
바꾸거나 stale R8 report로 신규 pipeline disposition을 적용하지 않는다.

#### C1. manual evidence 9건

현재 preview eligible인 9건은 authoritative manual evidence가 필요한 대상이다.

1. 각 대상의 외부 authoritative evidence, provenance, terminal 상태와 현재 source 부재를 수집한다.
2. 증거를 대상 fingerprint 및 manual-evidence campaign SHA-256 `dac9be8867b72637bd0e35bb006802b149ed26ce8a0c37facc6d0d55856d4d3d`에 결속한다.
3. R8 strict read-only preflight로 승인 R6/R7 report SHA, 접근 제한 private manifest/evidence file SHA,
   실제 broker artifact bytes의 SHA/size, final trade-date coverage, hashed account scope와 current
   six-part CAS를 exact match한다.
4. R8 report가 `PASS/COMPLETE/PREPARATION_REQUIRED`, `apply_authorized=false`인지 확인하고
   `evidence_preview_sha256`과 report SHA-256을 별도 승인 기록에 고정한다. 기존 orphan ledger의
   legacy/invalid/noncanonical row 및 invalid chain count도 모두 0이어야 한다.
5. byte-identical backup과 backup `quick_check`가 준비되고 별도 apply 승인이 있을 때만 한
   subject씩 append-only disposition 후 strict verify한다.

9건이 preview eligible이라는 사실만으로 apply가 승인되지는 않는다.
Generic non-empty 파일, `{ "redacted": true }`, self-asserted authoritative claim 또는 DB row 부재만으로
manual evidence를 대신할 수 없다. Private manifest/evidence 원문과 alias↔원 ID mapping은 git, PR,
Issue comment와 aggregate report 밖의 접근 제한 저장소에 둔다. 구현·preflight·향후 apply 계약은
`docs/runbook_pipeline_orphan_evidence_preflight_ko.md`를 따른다.

#### C2. reconcile required 11건

11건에는 active-source blocker가 있으며, 그중 일부에는 unsafe gateway command 2건과 broker-boundary unknown 1건이 겹칠 수 있다.

다음 항목을 strict read-only로 대상별 reconcile한다.

- active source가 실제로 존재하는지와 authoritative source가 무엇인지
- unsafe gateway command가 실제 미종결 command인지
- broker-boundary 상태가 확인 가능한 terminal 상태인지
- source lineage, plan 상태, current-source drift와 expected action

active/unsafe/unknown 상태가 실제로 남아 있으면 중단한다. DB의 count나 상태를 수동 변경해 eligibility를 만들지 않는다. 데이터 불일치 또는 코드 계약 결함으로 판명되면 별도 repair 설계, 코드 검토, 승인 및 새 strict plan이 필요하다. fresh plan에서 eligible로 판정되기 전에는 이 11건에 대한 apply 승인이 성립하지 않는다.

R7 strict read-only reconciliation은 이 분기의 현재 blocker를 aggregate evidence로 분류할 뿐이다.
R7 `PASS` 또는 분류 완료만으로 11건의 eligibility/apply 상태가 달라지지 않는다.

### Phase D — pipeline legacy 138건 eligibility contract와 resolver

legacy target은 138건이지만 현재 eligible은 0건이다. 이 수치는 운영 write 대상을 뜻하지 않는다.

현재 각 legacy 대상은 다음 두 증명이 모두 부족하다.

- `ACTIVE_SOURCE_NOT_PROVEN_ZERO`
- `CURRENT_SOURCE_DRIFT_UNKNOWN`

Phase D는 운영 DB write 단계가 아니라 코드·계약 단계다.

1. pre-schema lineage를 해석하는 명시적 eligibility contract를 정의한다.
2. authoritative evidence resolver를 구현한다.
3. terminal closed, active-source-zero, current-source no-drift를 개별 대상에 대해 증명한다.
4. exact CAS와 provenance를 evidence에 고정한다.
5. 단위·통합·회귀 테스트와 CI를 통과시키고 리뷰 후 main에 병합한다.
6. 병합된 코드로 strict read-only blocker plan을 다시 생성한다.

새 plan에서 eligible로 판정된 exact 대상만 향후 별도 캠페인의 후보가 된다. legacy 138건을 일괄 disposition하거나 unknown drift를 no-drift로 간주하지 않는다.

### Phase E — active-current 1건 targeted rebuild preview

대상 digest는 `07ceefcf721b9f1d6bd71913fba456f8ed70adb6ca41fb16a913fd3a58b2a77c`이다.

1. 기본 `.env`를 사용하지 않고 별도 감사된 OBSERVE 환경을 준비한다.
2. DB writer, 주문 전송, market-scan command 생성이 불가능한 설정을 증명한다.
3. targeted rebuild preview만 수행해 exact target, expected delta, CAS, source drift와 output SHA-256을 고정한다.
4. preview가 `ELIGIBLE`이 아니면 중단한다.
5. 실제 run은 preview와 분리된 별도 승인에서 exact preview SHA-256에만 결속한다.

일반 evaluation cycle을 대신 실행하거나 preview SHA가 바뀐 run을 승인 범위에 포함하지 않는다. 안전 smoke가 필요한 경우 `ThemeRefreshQueueMarketScanCommands`는 false로 강제하며, 최종 OBSERVE smoke 역시 별도 운영 승인으로 다룬다.

### Phase F — full FAST-0 strict requalification

Phase A~E의 승인된 작업과 검증이 모두 끝난 뒤 전체 R5 evidence를 다시 생성한다.

최소 기대 조건:

- incremental raw/effective: 38/0
- pipeline qualification: `PASS`
- DB identity와 schema가 승인 범위와 일치
- quick check, 전체 row/invariant, index/contract, runtime fencing, lineage, broker-boundary 검증 통과
- 승인되지 않은 주문, command, broker side effect 0

Phase F는 실제 현재 상태를 판정하는 단계이지 FAST-0 `PASS`를 약속하는 단계가 아니다. broker-boundary의 raw/semantic blocker, 비권위 runtime coverage 또는 최종 OBSERVE smoke가 남아 있으면 FAST-0은 계속 `BLOCKED`일 수 있다. 특히 broker-boundary `UNCONFIRMED > 0`은 도구가 WARN/exit 0이어도 FAST-0에서는 `BLOCKED`다.

## 6. 승인 경계

| 경계 | 필요한 별도 승인 | 승인으로 허용되지 않는 것 |
|---|---|---|
| 계획 생성 | strict read-only DB 점검과 evidence 파일 생성 | 운영 DB write |
| Phase A | identity pin, 디스크·writer 확인, byte-identical backup | disposition apply 또는 rebuild run |
| Phase B | 고정 manifest에 결속된 incremental 한 건 apply | bulk apply, 다른 alias 자동 진행 |
| Phase C1 preflight | 승인 R6/R7/private manifest/evidence/current CAS의 strict read-only 결속 | 운영 DB write 또는 evidence가 없는 대상의 권위 판정 |
| Phase C1 apply | 승인 evidence/preflight SHA와 한 subject append-only apply | 9건 일괄 처리, C2 대상 처리 |
| Phase C2 | 우선 read-only reconcile; fresh eligible plan 후 새 승인 | blocker가 남은 11건의 apply |
| Phase D | 코드 변경, 테스트, CI, 리뷰와 main 병합 | 운영 DB write 또는 legacy 일괄 처리 |
| Phase E preview | 감사된 OBSERVE 환경에서 exact preview | 실제 targeted rebuild run |
| Phase E run | 별도 승인된 exact preview SHA-256 한 건 | generic cycle 또는 다른 SHA 실행 |
| Phase F | full strict requalification evidence 생성 | LIVE 모드 활성화 또는 최종 smoke 자동 실행 |
| 최종 OBSERVE smoke | 별도 운영 승인과 감사된 env | `LIVE_SIM`/`LIVE_REAL`, 주문 전송 |

어떤 phase의 성공도 다음 phase 승인을 자동으로 부여하지 않는다. 승인 대상, DB fingerprint, manifest/report SHA-256, 예상 delta와 중단 조건이 모두 일치해야 한다.

## 7. 전면 금지 사항

- `LIVE_SIM` 또는 `LIVE_REAL` 활성화
- Core, Gateway, worker, scheduler 또는 일반 evaluation cycle 시작
- 승인된 exact 한 건 범위를 벗어난 DB write, schema migration, DB 초기화 또는 일반 RW 연결
- incremental retry sweep, retry-exhausted sweep, dead-letter reset
- 원본 dead-letter/pipeline row의 update/delete 또는 수동 status/count 조작
- projection backlog drain 또는 watermark/outbox를 임의로 전진
- 주문 전송·정정·취소, gateway command 생성 또는 broker API 호출
- preview/CAS/manifest/backup/strict verify 생략
- 오래된 preview SHA-256이나 이전 DB fingerprint 재사용
- 여러 phase 동시 실행 또는 성공 후 자동 연속 실행
- 기본 `.env`에 의존한 운영 실행
- pipeline 500-row 제한 도구의 결과를 전체 inventory 증거로 간주
- 로그 WARN이나 exit code 0을 FAST-0 통과로 간주

## 8. 즉시 중단 조건

다음 중 하나라도 발생하면 현재 작업을 중단하고 운영 DB를 더 변경하지 않는다.

- FAST-0 report, blocker plan, campaign 또는 inventory manifest SHA-256 불일치
- DB identity, 애플리케이션 ID, schema, 크기 또는 main DB SHA-256 drift
- WAL/SHM sidecar 출현, writer probe 실패 또는 runtime lock 비영(非零)
- backup byte hash 불일치, `quick_check` 실패 또는 디스크 여유 부족
- target count, eligibility, inventory 또는 expected action 변화
- preview blocked/unsupported, CAS 불일치 또는 provenance 부족
- C2 대상에 active source, unsafe command 또는 broker-boundary unknown이 실제로 남음
- Phase D contract/resolver 미병합 또는 테스트·CI 실패
- Phase E preview가 `ELIGIBLE`이 아니거나 승인된 preview SHA-256 변화
- 주문·정정·취소, 신규 command, broker-boundary 또는 order-state 비의도 delta
- 승인되지 않은 write, schema 변화 또는 evidence/report 생성 실패
- commit 결과 불명확, post-check 실패 또는 evidence 파일 기록 실패
- 토큰, 계좌번호, 원 식별자, raw payload 등 민감정보 노출

commit 결과가 불명확할 때는 동일 대상을 새 request ID로 재시도하지 않는다. 같은 요청의 DB 상태와 evidence를 strict read-only로 reconcile한 뒤 별도 판단한다.

## 9. 개인정보·비밀정보·evidence 규칙

- 문서, 일반 로그, PR, Issue, review comment와 `commands.txt`에는 count, boolean, digest와 공개 alias만 기록한다.
- incremental 대상은 `U01`~`U38` alias만 사용한다.
- alias와 원 식별자의 대응은 접근 제한된 로컬 campaign manifest에만 보관한다.
- token, password, 원 계좌번호, 원 주문/command/dead-letter 식별자, raw payload, error detail 또는 sample row를 기록하지 않는다.
- 운영 DB와 로그의 원본 절대경로를 공개 산출물에 넣지 않고 placeholder를 사용한다.
- evidence 본문을 Issue나 PR에 복사하지 않는다. 검증된 상대경로와 SHA-256만 참조한다.
- blocker reason count가 중첩될 수 있음을 유지하고 개인 대상을 역추론할 수 있게 재조합하지 않는다.
- raw evidence를 외부로 전달해야 한다면 먼저 별도의 redaction 검토와 승인을 거친다.

## 10. 캠페인 완료 조건

캠페인은 다음 조건이 모두 충족될 때만 완료로 기록한다.

1. 각 phase에 별도 승인 기록과 완전한 before/preview/apply-or-run/after evidence가 있다.
2. Phase B의 raw/effective가 38/0이며 원본 row와 비대상 상태가 불변이다.
3. Phase C의 manual/reconcile 대상이 fresh strict plan에서 terminal하게 설명된다.
4. Phase D의 contract/resolver가 테스트·CI·리뷰를 거쳐 main에 병합되고 새 계획이 생성됐다.
5. Phase E의 exact target 결과가 승인된 preview SHA-256 및 예상 delta와 일치한다.
6. Phase F full requalification이 최신 운영 DB fingerprint에 대해 실행됐다.
7. 남은 `BLOCKED` 또는 WARN 항목을 숨기지 않고 후속 승인 경계로 명시했다.
8. DB, 주문, command, broker side effect와 privacy audit가 모두 통과했다.

이 조건을 만족하지 못하면 캠페인은 `PREPARATION_REQUIRED`, `BLOCKED` 또는 해당 실패 상태로 남긴다. exit code나 일부 phase 성공만으로 완료 처리하지 않는다.
