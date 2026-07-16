# FAST-0R11 pipeline legacy evidence resolver runbook

## 목적

R11은 pre-schema-59 pipeline row를 이유 없이 정상으로 낮추지 않고, 각 legacy subject의 현재
상태와 승인된 schema-59 migration provenance를 함께 검증하는 strict read-only 계약이다. 이
단계는 운영 DB를 변경하지 않고 disposition 또는 rebuild를 승인하지 않는다.

기존 callback hook은 `pre_schema59=true`와 pipeline/subject CAS만 확인했다. 따라서
`ACTIVE_SOURCE_NOT_PROVEN_ZERO`와 `CURRENT_SOURCE_DRIFT_UNKNOWN`을 권위 있게 해소할 수 없었다.
R11은 다음 조건이 모두 참일 때만 두 blocker를 evidence로 대체한다.

- canonical raw 결과는 계속 FAIL이며 원 row를 수정하거나 숨기지 않는다.
- subject는 현재 `HISTORICAL_CLOSED`이고 `closed_at`이 존재한다.
- 최신 pipeline stage가 승인된 schema-59 cutover 시각보다 늦지 않고 lineage 필드가 비어 있다.
- append-only source event의 최신 상태와 `candidate_sources_latest` projection이 정확히 일치한다.
- 두 source view에서 현재 active source가 0이다. stale `candidates.active_source_count`만으로는
  판정하지 않는다.
- pipeline fingerprint, subject version, source/closure/stage fingerprint와 cutover provenance가
  private manifest의 exact CAS와 일치한다.

하나라도 불일치하거나 target이 누락·추가되면 resolver 전체가 `BLOCKED`이며 부분 승격하지 않는다.

## Private manifest

manifest contract는 `fast0-pipeline-legacy-evidence-manifest.v1`, item contract는
`pipeline-legacy-evidence-item.v1`이다. manifest에는 원 candidate ID가 포함되므로 git, PR,
Issue comment, 공개 report 또는 `commands.txt`에 저장하지 않는다. 접근 제한된 로컬 저장소에 두고
파일 SHA-256만 별도로 승인한다.

최상위 필드는 다음 exact set이다.

```text
contract
trade_date
target_set_sha256
schema59_cutover
items
```

`schema59_cutover`는 `completed_at`, `migration_report_sha256`,
`source_database_sha256`, `migrated_database_sha256`을 포함한다. self-asserted timestamp만으로는
충분하지 않으며 실제 승인된 migration evidence의 digest를 사용한다.

각 item은 candidate ID 오름차순으로 `L001`부터 연속되어야 하며 다음 exact CAS를 포함한다.

```text
alias
candidate_instance_id
pipeline_fingerprint
subject_version
source_fingerprint
closure_fingerprint
pipeline_stage_fingerprint
provenance_sha256
```

manifest 생성은 권위 증거 수집·검토 과정이며 이 runbook의 일반 명령이 자동 생성하지 않는다.
DB 상태만 보고 schema-59 provenance를 꾸며내거나 임의 cutover 시각을 사용하지 않는다.

## Strict read-only plan 재생성

Core/Gateway와 모든 writer가 중지되고 DB sidecar가 없는 상태에서 실행한다. 기본 `.env`를 읽거나
프로세스를 시작하지 않는다.

```powershell
python -B -m tools.ops_fast0_blocker_resolution_plan `
  --db $db `
  --fast0-report $fast0Report `
  --fast0-report-sha256 $fast0ReportSha256 `
  --legacy-evidence-manifest $privateManifest `
  --legacy-evidence-manifest-sha256 $privateManifestSha256 `
  --out-dir $evidenceDir
```

두 manifest 인자는 함께 제공하거나 둘 다 생략해야 한다. 도구는 immutable/query-only snapshot,
mandatory quick check, pinned DB identity, writer probe와 전후 파일 fingerprint를 유지한다. 공개
`raw.json`에는 manifest 내용, 경로와 원 식별자를 넣지 않고 contract/file SHA-256과 aggregate
count만 기록한다.

## 판정

Phase D가 `COMPLETE`이려면 `legacy_evidence_target_count`와
`legacy_evidence_eligible_count`가 같고 `legacy_evidence_contract_blocked_count=0`이어야 한다.
resolver가 인정한 legacy subject는 canonical audit에서 제거되지 않으며 qualification view에서만
`legacy_warn_candidate=true`가 된다.

다음은 즉시 중단 조건이다.

- manifest file SHA, target-set SHA, alias sequence 또는 target count 불일치
- schema-59 migration provenance 부재 또는 cutover 이후 stage 발견
- source event/latest projection 불일치 또는 active source 존재
- terminal closure, pipeline/subject/source/closure/stage CAS 불일치
- manifest의 실행 중 변경, DB sidecar/handle/runtime lock 출현 또는 DB fingerprint 변화
- raw ID, 계좌번호, token, payload 또는 실제 운영 경로의 공개 evidence 유출

R11 `COMPLETE`는 운영 write 승인이 아니다. legacy row update/delete, 일괄 disposition, Core 시작,
targeted rebuild run, LIVE 모드와 주문 동작은 계속 금지된다.
