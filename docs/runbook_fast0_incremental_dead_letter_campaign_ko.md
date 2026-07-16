# FAST-0R9 incremental dead-letter 38건 캠페인 runbook

## 1. 목적과 승인 경계

이 문서는 FAST-0R6에서 고정한 incremental dead-letter 38건을 `U01`부터 `U38`까지 한 건씩 append-only disposition하는 계약을 설명한다. 구현과 테스트만으로 운영 적용이 승인되지는 않는다.

- 원본 `incremental_evaluation_dead_letters` 행은 update/delete하지 않는다.
- 허용 write는 승인된 alias 한 건의 `incremental_evaluation_dead_letter_dispositions` insert뿐이다.
- retry sweep, reset, revoke, batch apply, Core/worker 시작, evaluation 실행, 주문·broker 호출은 금지한다.
- 각 alias는 별도 strict read-only preflight와 별도 운영 승인을 받아야 한다.
- 운영 DB write가 승인되지 않은 세션에서는 이 문서의 apply 명령을 실행하지 않는다.

고정 캠페인:

- contract: `fast0-private-target-set.v1`
- alias: `U01`~`U38`
- ordering: `dead_letter_id ASC`
- target count: `38`
- semantic target-set SHA-256: `845b5b9bf82f1a2cebdda9ddf262627f3eda1e91fa53d1b027983284be43f31d`
- action: `DISPOSE_OBSOLETE_CLOSED_CANDIDATE`
- source bucket: `HISTORICAL_PENDING_DISPOSITION`

## 2. 비공개 파일과 공개 evidence

Private manifest에는 다음 exact 필드만 둔다.

```text
contract
items[].alias
items[].action
items[].dead_letter_id
items[].dead_letter_fingerprint
items[].candidate_version
items[].bucket
```

Private manifest, alias↔원 ID 매핑, 운영자 ID, evidence ref와 evidence 원문은 git, PR, Issue, `reports/`, `commands.txt`에 기록하지 않는다. 공개 preflight/apply/handoff report에는 alias, count, boolean, SHA-256과 DB fingerprint만 기록한다. request/disposition ID는 SHA-256으로만 노출한다.

## 3. Phase A 준비물

최초 write 전에 다음을 별도 승인 기록에 고정한다.

1. R6 blocker-plan `raw.json` bytes와 SHA-256
2. private manifest bytes와 SHA-256 및 semantic target-set SHA-256
3. R6 base DB와 byte-identical인 backup 파일 및 SHA-256
4. backup `PRAGMA quick_check(1) = ok`, app name과 schema 62
5. 운영 DB writer 부재, runtime lock 0, WAL/SHM/journal 부재
6. 디스크 여유와 복구 책임자

`--base-backup`은 매 alias preflight에서 같은 R6 base 복구 지점을 검증한다. backup을 새로 만들거나 교체하면 기존 승인 SHA를 재사용하지 않는다.

## 4. alias preflight

U01은 predecessor report가 없다.

```powershell
& $python -B -m tools.ops_incremental_dead_letter_campaign_preflight `
  --db $db `
  --blocker-plan-report $r6Report `
  --blocker-plan-report-sha256 $r6ReportSha `
  --private-target-manifest $privateManifest `
  --private-target-manifest-sha256 $privateManifestSha `
  --base-backup $baseBackup `
  --base-backup-sha256 $baseBackupSha `
  --alias U01 `
  --out-dir $preflightOut
```

U02~U38은 바로 전 alias의 성공 apply report만 predecessor로 받는다.

```powershell
& $python -B -m tools.ops_incremental_dead_letter_campaign_preflight `
  --db $db `
  --blocker-plan-report $r6Report `
  --blocker-plan-report-sha256 $r6ReportSha `
  --private-target-manifest $privateManifest `
  --private-target-manifest-sha256 $privateManifestSha `
  --base-backup $baseBackup `
  --base-backup-sha256 $baseBackupSha `
  --alias $alias `
  --predecessor-apply-report $previousApplyReport `
  --predecessor-apply-report-sha256 $previousApplyReportSha `
  --out-dir $preflightOut
```

승인 가능한 preflight는 다음을 모두 만족해야 한다.

- `evidence_status=PASS`
- `plan_status=COMPLETE`
- `execution_readiness=PREPARATION_REQUIRED`
- `apply_authorized=false`
- 현재 alias만 `eligible=true`
- completed prefix와 predecessor chain exact match
- raw count 38 유지
- disposition row는 완료 prefix에만 정확히 한 건씩 존재
- foreign/duplicate/invalid/revoke/reset row 0
- protected database digest가 직전 승인 상태와 일치
- quick check `ok`, schema 62, runtime lock 0, sidecar/writer 없음

`PASS`는 apply 승인이 아니다. preflight `raw.json` SHA와 exact alias에 대한 별도 승인이 필요하다.

## 5. 단건 apply와 동일 request 재조정

별도 OBSERVE env 파일을 사용하고 `TRADING_ENV_FILE`로 명시한다. 기본 `.env`는 사용하지 않는다. 모든 command producer와 incremental worker를 false로, kill switch를 true로 고정하고 `THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false`를 명시한다.

```powershell
$env:TRADING_ENV_FILE = $safeObserveEnv

& $python -B -m tools.apply_incremental_dead_letter_campaign `
  --db $db `
  --blocker-plan-report $r6Report `
  --blocker-plan-report-sha256 $r6ReportSha `
  --campaign-preflight-report $preflightReport `
  --campaign-preflight-report-sha256 $preflightReportSha `
  --private-target-manifest $privateManifest `
  --private-target-manifest-sha256 $privateManifestSha `
  --alias $alias `
  --request-id $approvedRequestId `
  --operator-id $operatorId `
  --evidence-ref $privateEvidenceRef `
  --evidence-file $privateEvidenceFile `
  --evidence-file-sha256 $privateEvidenceSha `
  --acknowledge APPLY-EXACT-FAST0R9-SINGLE-ALIAS `
  --out-dir $applyOut
```

도구는 RW open 전에 승인 파일을 stable-read하고, `BEGIN EXCLUSIVE` 뒤 동일 입력과 DB fingerprint를 다시 검증한다. transaction 안에서는 disposition table 한 행 insert만 허용하며 raw inventory와 모든 비-disposition table digest가 불변인지 확인한다. commit 뒤 WAL checkpoint, strict read-only post-verify와 DB fingerprint를 수행한다.

commit 결과가 불명확하거나 report write가 실패하면 새 request ID를 만들지 않는다. 같은 alias와 같은 request ID로만 재실행한다. exact durable row가 확인된 경우에만 `REPLAY_VERIFIED`가 가능하다. `OUTCOME_UNKNOWN`, `COMMITTED_POSTCHECK_FAILED`, `COMMITTED_EVIDENCE_WRITE_FAILED`에서는 다음 alias로 진행하지 않는다.

## 6. 완료 handoff

U38이 `VERIFIED` 또는 exact `REPLAY_VERIFIED`로 끝난 뒤, U01~U38 apply report 38개를 순서대로 제공한다.

```powershell
& $python -B -m tools.ops_incremental_dead_letter_campaign_handoff `
  --db $db `
  --blocker-plan-report $r6Report `
  --blocker-plan-report-sha256 $r6ReportSha `
  --private-target-manifest $privateManifest `
  --private-target-manifest-sha256 $privateManifestSha `
  --apply-report $u01Report `
  --apply-report-sha256 $u01ReportSha `
  # ... U02~U37도 동일 순서로 반복 ...
  --apply-report $u38Report `
  --apply-report-sha256 $u38ReportSha `
  --out-dir $handoffOut
```

완료 report `fast0-incremental-dead-letter-campaign-handoff.v1`은 다음을 증명한다.

- U01~U38 누락·중복·재정렬 없음
- 각 predecessor report/chain과 DB pre→post transition exact match
- raw 38 유지
- effective 0, pending 0, disposed 38
- disposition ledger 38, invalid/foreign/unexpected action 0
- protected database digest 불변
- final DB main fingerprint가 U38 report와 일치
- strict read-only snapshot, quick check `ok`, runtime lock 0, sidecar/writer 없음

handoff 자체도 pipeline disposition apply를 승인하지 않는다. R9 write 이후 R6/R7 base에 직접 결속된 기존 R8 preflight는 stale이므로 재사용 금지다. R8 consumer가 이 handoff와 이후 orphan predecessor chain을 검증하는 코드가 main에 병합되기 전에는 Phase C1 apply로 넘어가지 않는다.

## 7. 즉시 중단 조건

- plan/manifest/preflight/predecessor/evidence bytes 또는 SHA 불일치
- alias 누락, 건너뛰기, 역순, 중복 또는 이미 완료된 alias에 새 request 사용
- DB main size/SHA/identity drift
- WAL/SHM/journal 존재 또는 commit 후 checkpoint 실패
- writer probe 실패, runtime lock 비영, schema/quick-check 실패
- target fingerprint/candidate version/CAS drift
- foreign/duplicate/invalid/revoke/reset disposition 발견
- raw inventory 또는 protected database digest 변화
- 주문·command·broker 관련 delta 또는 write-authorizer 위반
- commit outcome unknown, postcheck/report write 실패

어느 경우에도 다음 alias를 자동 진행하지 않는다.
