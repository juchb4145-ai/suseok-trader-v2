# FAST-0R8 pipeline orphan evidence strict read-only preflight runbook

## 1. 목적과 승인 경계

FAST-0R8은 pipeline orphan disposition 후보를 실제로 처리하는 단계가 아니다. 승인된 R6/R7
증거, 접근 제한 private target manifest, 한 건의 authoritative evidence와 현재 운영 DB의 exact
preview를 strict read-only로 결속하는 단계다.

이 runbook과 R8 도구는 다음 행위를 승인하거나 수행하지 않는다.

- 운영 DB write, migration, initialization 또는 일반 RW connection
- pipeline disposition apply, revoke, repair 또는 targeted rebuild
- Core, Gateway, worker, scheduler 또는 broker process 시작
- `LIVE_SIM`/`LIVE_REAL` 활성화
- 주문·정정·취소, gateway command 또는 broker API 호출
- evidence가 없는 target에 대한 권위 판정 생성

R8 report의 `PASS/COMPLETE`는 입력과 현재 preview의 결속이 완전하다는 뜻이다.
`apply_authorized=false`는 항상 유지되며 실제 append-only apply에는 별도 운영 write 승인이 필요하다.

## 2. R8이 필요한 이유

기존 pipeline resolver의 공통 evidence 경로는 non-empty 파일의 SHA-256과 크기만 저장했다.
그러면 orphan action에서도 `{"redacted":true}` 같은 임의 파일이 evidence gate를 통과할 수 있었다.
R8은 orphan action에 한해 다음 fail-closed 계약을 추가한다.

1. evidence JSON의 exact schema와 중복 key 부재를 확인한다.
2. 승인된 evidence file SHA-256, R6 target-set SHA-256과 alias를 exact match한다.
3. trade date, private candidate ID, classification, action과 six-part CAS를 manifest 및 현재 preview에
   exact match한다.
4. authoritative terminal-orphan 판정과 candidate/source/order-or-broker 부재 claim을 strict boolean으로
   검증한다.
5. allowlisted broker provenance source와 실제 artifact bytes의 hash/size를 대조하고, 대상 거래일의
   final-complete coverage, hashed account scope, UTC 관찰·검토 시각과 reviewer를 검증한다.
6. raw evidence 대신 safe binding과 `evidence_preview_sha256`만 report와 ledger에 보존한다.
7. CLI append, service direct call, 저장된 ledger projection 세 경계에서 같은 binding을 재검증한다.
8. 기존 orphan ledger 전체를 aggregate-only로 감사해 legacy generic, duplicate/noncanonical JSON,
   invalid row/chain이 0건임을 요구한다.

기존 `DISPOSE_EXPIRED_PLAN_READY`, `DISPOSE_STALE_OTHER_DATE`, `REVOKE` evidence 계약은 이 변경으로
확대하거나 승인하지 않는다.

## 3. 승인 입력

현재 운영 기준선은 다음 aggregate 증거에 고정돼 있다.

- R6 blocker-resolution plan
  - contract: `fast0-blocker-resolution-plan.v1`
  - raw SHA-256: `a51303b49062c0e7e3ab28cdfc2d4c414844384900a010df47c8023dfa75996e`
  - manual-evidence target count: 9
  - private target-set SHA-256:
    `dac9be8867b72637bd0e35bb006802b149ed26ce8a0c37facc6d0d55856d4d3d`
- R7 pipeline blocker reconciliation
  - contract: `fast0-pipeline-blocker-reconciliation.v1`
  - raw SHA-256: `469e827cd586cc3550913afbdc3db5307c48188b71707cc58896fe68a6498849`
  - orphan action/preview-eligible/manual-evidence-required: 9/9/9
  - outcome: `MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED=9`

위 count와 digest는 코드에 hard-code하지 않는다. R8은 승인된 R6 report에서 값을 읽고 R7 report,
private manifest 및 현재 DB와 재검증한다. R6/R7 report SHA, DB main fingerprint 또는 target count가
달라지면 기존 승인을 재사용하지 않는다.

Private manifest, evidence 원문과 alias↔원 ID 매핑은 git, PR, Issue comment, 일반 report 디렉터리에
넣지 않는다. 접근 제한 저장소에서 별도 file SHA-256 승인을 받아야 한다. 현재 repository에는 실제
운영 authoritative evidence를 만들거나 대신하는 fixture가 없다.

## 4. Private target manifest 계약

Private manifest root는 다음 exact 형태다.

```json
{
  "contract": "fast0-private-target-set.v1",
  "items": [
    {
      "trade_date": "YYYY-MM-DD",
      "candidate_instance_id": "<private exact id>",
      "classification": "MISSING_CANDIDATE_MANUAL_REVIEW",
      "action": "DISPOSE_ORPHAN_PIPELINE_OBSERVATION",
      "pipeline_fingerprint": "<lowercase sha256>",
      "subject_version": "<lowercase sha256>",
      "source_fingerprint": "<lowercase sha256>",
      "candidate_fingerprint": "<lowercase sha256>",
      "downstream_fingerprint": "<lowercase sha256>",
      "boundary_fingerprint": "<lowercase sha256>",
      "alias": "M001"
    }
  ]
}
```

Items는 private candidate ID 오름차순이며 alias는 `M001`부터 연속돼야 한다. 중복 alias/ID,
추가·누락 key, 비정규 SHA-256, 순서 또는 count 불일치는 모두 거부한다. Canonical payload SHA-256은
R6의 `manual_evidence_campaign_sha256`과 같아야 한다. Manifest 원 파일 bytes의 SHA-256도 별도
승인 입력으로 요구하므로 의미 digest가 같아도 다른 파일을 자동 수용하지 않는다.

## 5. 한 건의 authoritative evidence 계약

Alias별 evidence는 다음 exact JSON contract를 사용한다.

```json
{
  "contract": "fast0-pipeline-orphan-manual-evidence.v1",
  "target_set_sha256": "<approved R6 target-set sha256>",
  "alias": "M001",
  "target": {
    "trade_date": "YYYY-MM-DD",
    "candidate_instance_id": "<private exact id>",
    "classification": "MISSING_CANDIDATE_MANUAL_REVIEW",
    "action": "DISPOSE_ORPHAN_PIPELINE_OBSERVATION",
    "pipeline_fingerprint": "<lowercase sha256>",
    "subject_version": "<lowercase sha256>",
    "source_fingerprint": "<lowercase sha256>",
    "candidate_fingerprint": "<lowercase sha256>",
    "downstream_fingerprint": "<lowercase sha256>",
    "boundary_fingerprint": "<lowercase sha256>"
  },
  "determination": {
    "status": "AUTHORITATIVE",
    "terminal_orphan_confirmed": true,
    "candidate_present": false,
    "current_source_present": false,
    "order_or_broker_activity_present": false
  },
  "provenance": {
    "source_type": "BROKER_ORDER_HISTORY_EXPORT",
    "source_ref": "<safe opaque reference>",
    "artifact_sha256": "<lowercase sha256>",
    "artifact_size": 1,
    "coverage_trade_date": "YYYY-MM-DD",
    "coverage_status": "FINAL_COMPLETE",
    "account_scope_sha256": "<lowercase sha256>",
    "observed_at": "YYYY-MM-DDTHH:MM:SSZ",
    "reviewed_at": "YYYY-MM-DDTHH:MM:SSZ",
    "reviewer_id": "<safe opaque reviewer>"
  }
}
```

허용 `source_type`은 다음과 같다.

- `BROKER_ACCOUNT_ACTIVITY_EXPORT`
- `BROKER_EXECUTION_HISTORY_EXPORT`
- `BROKER_ORDER_HISTORY_CAPTURE`
- `BROKER_ORDER_HISTORY_EXPORT`

Source type 문자열 자체가 권위를 만들지는 않는다. 실제 원 artifact, 승인 file SHA-256, reviewer와
provenance가 함께 있어야 한다. `artifact_sha256`과 `artifact_size`는 CLI가 실제 artifact bytes를
stable-read해 계산한 값과 같아야 한다. `coverage_trade_date`는 target trade date와 같고
`coverage_status`는 `FINAL_COMPLETE`여야 한다. `account_scope_sha256`에는 계좌 원문이 아니라 승인된
계좌 범위의 SHA-256만 넣는다. `source_ref`와 `reviewer_id`에는 token, password, API key 또는 원
계좌번호를 넣지 않는다. `reviewed_at`은 `observed_at`보다 빠를 수 없고 두 값 모두 UTC `Z` 형식이어야
하며, 관찰은 대상 거래일이 한국 시간으로 완전히 끝난 뒤여야 하고 preflight 미래 시각일 수 없다.
JSON duplicate key, `NaN`, 추가 key, bool-as-int와 self-asserted/부분 claim은 거부한다.
운영 DB audit나 내부 dual-control 기록만으로 broker activity 부재를 대신 입증할 수 없으므로
allowlist에 포함하지 않는다. Candidate/current-source 부재는 별도의 current DB preview가 증명한다.

## 6. Strict read-only preflight

다음 명령은 실제 값을 제한된 운영 세션에서 채우되 apply option을 포함하지 않는다.

```powershell
& $python -B -m tools.ops_pipeline_orphan_evidence_preflight `
  --db <checkpointed-schema-62-db> `
  --blocker-plan-report <approved-r6-raw.json> `
  --blocker-plan-report-sha256 <approved-r6-sha256> `
  --reconciliation-report <approved-r7-raw.json> `
  --reconciliation-report-sha256 <approved-r7-sha256> `
  --private-target-manifest <restricted-private-manifest.json> `
  --private-target-manifest-sha256 <approved-private-file-sha256> `
  --evidence-file <restricted-one-alias-evidence.json> `
  --evidence-file-sha256 <approved-evidence-file-sha256> `
  --authoritative-artifact <restricted-broker-artifact> `
  --authoritative-artifact-sha256 <approved-artifact-sha256> `
  --account-scope-sha256 <approved-account-scope-sha256> `
  --alias <Mnnn> `
  --predecessor-report <approved-r9-handoff-or-previous-orphan-apply.json> `
  --predecessor-report-sha256 <approved-predecessor-report-sha256> `
  --out-dir <aggregate-evidence-dir>
```

도구는 다음 순서를 지킨다.

1. R6/R7 report를 stable-read하고 저장 verdict를 독립 재계산한 뒤 exact
   SHA/contract/count/privacy/read-only 상태와 `R6 ≤ R7 ≤ R8` 생성 시각을 검증한다.
2. private manifest file SHA, semantic target-set SHA, count, alias 순서와 선택 target을 검증한다.
3. evidence와 실제 broker artifact를 symlink/hardlink가 아닌 단일 regular file로 stable-read하고
   evidence claim을 artifact bytes, coverage 및 별도 승인 입력인 account-scope digest와 exact match한다.
4. M001은 `fast0-incremental-dead-letter-campaign-handoff.v1`, M002 이후는 직전
   `fast0-pipeline-disposition-apply.v1` report를 stable-read하고 exact SHA, alias 순서, campaign
   chain과 predecessor post-DB main fingerprint를 검증한다.
5. DB main identity를 Windows deny-write/delete handle에 pin하고 sidecar/writer 부재를 확인한다.
6. immutable `mode=ro`, `query_only`, 단일 deferred snapshot에서 `quick_check(1)`, app/schema identity,
   schema manifest와 runtime lock 0을 확인한다.
7. 기존 orphan ledger 전체의 legacy generic/invalid JSON/noncanonical JSON/invalid row/chain count가
   모두 0인지 aggregate-only로 감사한다.
8. private manifest prefix와 저장 ledger를 M001부터 재계산해 predecessor chain과 완료 건수가 exact
   match하는지 확인하고, 선택 alias 한 건의 current orphan preview가 `ELIGIBLE`,
   `CANDIDATE_ABSENT`, `SAFE_DB_PROVEN`,
   reason 0이고 manifest six-part CAS와 exact match하는지 확인한다.
9. DB, predecessor report, manifest, evidence와 broker artifact를 다시 확인해 전체 구간
   content/identity가 변하지 않았음을
   증명한다. 동일 bytes로 파일을 교체해도 identity drift로 거부한다.
10. 원 ID/path/provenance 본문을 제거한 aggregate report만 쓴다.

정상 결과는 `evidence_status=PASS`, `preflight_status=COMPLETE`,
`execution_readiness=PREPARATION_REQUIRED`다. 별도 apply 승인, byte-identical backup과 backup
`quick_check`, 한 alias apply 후 strict verify가 preparation requirement로 남는다.

## 7. 산출물과 privacy

Timestamp directory에는 다음만 생성한다.

- `raw.json`: alias, count, enum, boolean과 SHA-256만 포함한 machine-readable report
- `summary.md`: preflight 판정과 safe binding digest 요약
- `commands.txt`: 실제 값이 제거된 preflight template와 apply 비승인 문구

다음 값은 어느 산출물에도 기록하지 않는다.

- 원 candidate/source/order/command/boundary ID 또는 alias mapping
- evidence 원문, source reference, reviewer, raw row 또는 payload
- DB, report, private manifest, evidence의 원 절대경로
- token, credential, 원 계좌번호 또는 개인정보

Safe binding은 원 ID 대신 target/determination/provenance digest, allowlisted source type,
target-set SHA, alias, file SHA/size와 `evidence_preview_sha256`만 저장한다.
실제 ledger의 `evidence_json`은 이 safe binding을 `fast0-pipeline-orphan-apply-binding.v2`로
감싸고 승인 R8 preflight, private manifest, R6/R7 source report, 승인 DB main, broker scope와
artifact/target-set SHA 및 `approval_binding_sha256`을 함께 영속화한다. 원 report나 private file의
경로·본문은 저장하지 않는다.

## 8. 향후 append-only apply 계약

실제 apply는 이 R8 구현이나 preflight 성공만으로 승인되지 않는다. 별도 운영 write 승인에서는 기존
exact request/CAS/OBSERVE safety 인자 외에 orphan 전용으로 다음 값을 모두 요구한다.

```text
--expected-evidence-sha256 <approved evidence file sha256>
--expected-evidence-preview-sha256 <approved R8 evidence_preview_sha256>
--evidence-target-set-sha256 <approved R6 target-set sha256>
--evidence-alias <Mnnn>
--orphan-preflight-report <approved R8 raw.json>
--expected-orphan-preflight-report-sha256 <approved R8 report sha256>
--private-target-manifest <restricted-private-manifest.json>
--expected-private-target-manifest-sha256 <approved manifest file sha256>
--authoritative-artifact <restricted-broker-artifact>
--expected-authoritative-artifact-sha256 <approved artifact sha256>
```

Resolver는 RW connection을 열기 전에 승인 R8 report SHA와 그 안의 R6/R7 report SHA, private
manifest, evidence와 실제 artifact를 전이 결속한다. `BEGIN EXCLUSIVE` 안에서는 private file identity,
current preview와 six-part CAS를 다시 검증한다. Writer lock 직후 rollback journal 또는 committed
WAL frame이 있으면 중단하고, checkpoint가 끝난 경우에는 main fingerprint drift로 차단한다. 신규
request는 현재 DB main fingerprint가 승인 R8 preflight fingerprint와 exact match할 때만 한 행을
append한다. Service의 신규 orphan append는 이 실제 file/DB 검증 경계에서 발급된 sealed in-process
approval context도 요구하며, plain SHA만 합성한 직접 호출은 거부한다. 이 context는 내부 호출 실수를
막는 process-local capability이지 외부 인증이나 암호학적 운영 승인을 대신하지 않는다. 저장된
effective projection도 같은 durable safe binding을 검증한다. Generic/legacy orphan evidence는
유효하지 않다. 동일 request ID 재시도는 동일 evidence SHA/binding과 current CAS가 모두 같을 때만
idempotent일 수 있다.

Apply를 승인할 때도 한 alias씩 `preview → apply → strict verify`를 완료한 뒤 다음 alias로 이동한다.
Bulk apply나 M001~M009 자동 연속 실행은 허용하지 않는다.
일반 resolver의 orphan preview 산출물도 candidate/subject 원문 대신 SHA-256과 고정 aggregate field만
기록한다. 다만 이 generic preview는 alias, 승인 evidence와 broker artifact를 결속하지 않으므로 R8
strict preflight를 대신하지 않으며 단독으로 apply 승인을 만들 수 없다.

R10부터 R8 preflight는 R6/R7의 stale base fingerprint를 current DB 기준으로 요구하지 않는다. 대신
M001은 R9 완료 handoff의 final DB main과 exact match해야 하고, M002~M009는 직전 orphan apply report의
checkpoint 완료 post-DB main과 exact match해야 한다. Durable evidence binding은 root R9 handoff,
predecessor kind/report/chain과 predecessor DB main을 포함하는 v2 계약이다. Apply report는 commit 뒤 WAL
checkpoint를 끝낸 main SHA/size와 campaign chain을 기록한다. `OUTCOME_UNKNOWN`, warning, checkpoint,
postcheck 또는 evidence write 실패 report는 다음 alias의 predecessor가 될 수 없다.

## 9. 즉시 중단 및 금지 조건

다음 중 하나라도 발생하면 기존 preflight/evidence SHA를 재사용하지 않는다.

- R6/R7/private manifest/evidence file SHA 불일치
- private target-set digest, count, alias 순서 또는 selected target 불일치
- evidence schema/claim/provenance/UTC/safe label 불일치
- DB identity/schema/main fingerprint drift 또는 `quick_check` 실패
- 기존 orphan ledger에 legacy generic, invalid/duplicate/noncanonical JSON 또는 invalid chain 존재
- RW open 전 WAL/SHM/journal, writer/open handle 또는 runtime lock 존재
- M001 R9 handoff 또는 M002 이후 직전 apply report의 SHA/alias/chain/post-main 불일치
- private manifest prefix와 durable v2 ledger campaign chain 또는 완료 건수 불일치
- writer lock 뒤 rollback journal, non-empty WAL 또는 불일치 sidecar 상태 존재
- current preview blocked, reason 비영, six-part CAS 또는 classification/action 불일치
- source가 `CANDIDATE_ABSENT`가 아니거나 downstream이 `SAFE_DB_PROVEN`이 아님
- 점검 전·후 DB/private file 변화
- report에 원 식별자, 절대경로, evidence 본문 또는 민감정보 포함
- `apply_authorized`, write/tool action 또는 LIVE flag가 안전값과 다름

Evidence가 없거나 불완전한 alias를 건너뛰어 disposition하지 않는다. DB count/status를 수동 변경해
eligibility를 만들지 않고, 외부 broker 상태를 DB 부재만으로 추론하지 않는다.

## 10. 완료 기준

### 구현 완료

- orphan exact-schema validator, private manifest validator와 safe binding validator가 있다.
- CLI/service/stored projection 세 경계에서 generic orphan evidence를 fail closed로 거부한다.
- strict read-only preflight가 R6/R7/private manifest/evidence/current DB를 exact 결속한다.
- 실제 broker artifact/coverage/account-scope digest를 evidence에 결속하고 broker 외 source를 거부한다.
- M001은 R9 handoff, M002 이후는 직전 apply report를 predecessor로 강제하고 DB main/chain을 전이
  결속한다.
- apply commit 뒤 WAL checkpoint와 post-main SHA를 기록하며 실패 report는 다음 alias를 승인하지 않는다.
- 승인 R8 report 없이 apply할 수 없고 신규 apply는 committed WAL frame이 없으며 report의 DB main
  fingerprint와 current DB가 같다.
- malformed JSON, duplicate key, target/CAS/claim/provenance/SHA drift, same-byte file replacement와
  TOCTOU 회귀 테스트가 있다.
- non-orphan disposition 경로의 기존 회귀 테스트가 통과한다.

### 실제 운영 evidence 완료

구현 병합과 실제 evidence 준비는 분리한다. 현재 실제 운영 authoritative evidence 또는 private
manifest를 repository에서 생성하지 않았으므로 R8 운영 preflight는 아직 완료된 것으로 간주하지
않는다. 향후 한 alias에 대해 실제 승인 파일을 준비한 뒤 별도 strict read-only 실행으로
`PASS/COMPLETE/PREPARATION_REQUIRED` report와 SHA-256을 고정해야 한다.
