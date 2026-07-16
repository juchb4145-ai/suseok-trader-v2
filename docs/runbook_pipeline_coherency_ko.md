# Pipeline Source Lineage / Coherency Runbook

## 범위

schema 59는 Candidate -> Strategy -> Risk -> EntryTiming -> OrderPlan 관측 경로에
동일 source snapshot을 확인할 수 있는 lineage를 저장한다. 이 기능은 OBSERVE 평가와
read-only 진단만 확장하며 주문, 정정, 취소 또는 Gateway command를 생성하지 않는다.

표준 필드는 다음과 같다.

- `source_run_id`
- `source_watermark`, `source_watermark_hash`
- `source_event_id`, `source_observed_at`
- `trade_date`, `data_age_sec`, `generated_by`
- EntryTiming/OrderPlan의 `strategy_observation_id`, `risk_observation_id`
- OrderPlan의 `entry_timing_evaluation_id`

기존 row는 삭제하거나 추정 backfill하지 않는다. migration 이전 row의 nullable lineage는
`*_LINEAGE_MISSING`으로 표시하며 새 `PLAN_READY` 생성과 LIVE_SIM eligibility에서 fail-closed한다.

## 생성 규칙

Strategy는 candidate state/context, KRX latest tick과 1m/3m/5m bar,
common market-context reference, current theme/condition reference를 canonical JSON
watermark로 만들고 SHA-256 hash를 저장한다.
Risk는 자신이 참조하는 Strategy observation의 lineage를 상속한다.

EntryTiming은 다음 조건을 모두 만족해야 `PLAN_READY`를 허용한다.

1. Strategy/Risk latest row가 모두 존재한다.
2. Risk의 `strategy_observation_id`가 현재 Strategy latest ID와 같다.
3. 두 row의 `source_run_id`, `source_watermark_hash`, `trade_date`가 같다.
4. source age가 `ENTRY_TIMING_STALE_MAX_SECONDS` 이내다.
5. observe-cycle이 expected source run을 제공한 경우 그 run ID와도 같다.
6. EntryTiming 직전 current candidate source watermark가 Strategy watermark와 같다.

불일치하면 evaluation을 `DATA_WAIT`로 기록하고
`PIPELINE_COHERENCY_GUARD_BLOCKED`를 남긴다. near-miss draft가 남더라도 수량은 0이고
`PLAN_READY`가 아니므로 LIVE_SIM selection 대상이 아니다.

LIVE_SIM order-plan eligibility는 persisted plan을 현재 Strategy/Risk/EntryTiming latest와
다시 비교한다. 직접 FK, source run 또는 watermark가 없거나 달라지면
`ORDER_PLAN_LINEAGE_INVALID`로 차단한다.

## 상태 기준

`GET /api/operator/pipeline-coherency/status`는 candidate별 stage metadata와 집계를 반환한다.
기본 범위는 candidates와 네 stage에 존재하는 가장 최신 `trade_date`이며, 과거 날짜는
query parameter로 명시해 별도로 조회한다.

- `PASS`: 존재하는 5단계 row의 run/watermark/FK/trade date가 일치하고 source가 fresh다.
- `WARN`: pipeline row가 없거나 downstream EntryTiming/OrderPlan이 아직 없거나, non-ready
  관측 source가 stale하다.
- `FAIL`: FK/run/watermark/trade-date mismatch, lineage 누락 또는 stale `PLAN_READY`가 있다.

주요 집계:

- `candidate_count`, `coherent_count`, `warning_count`
- `mismatch_count`, `missing_lineage_count`, `stale_count`
- candidate별 `stages.strategy/risk/entry_timing/order_plan`

각 stage metadata에는 `source_run_id`, parsed `source_watermark`, `trade_date`,
`data_age_sec`, `generated_by`가 같은 형식으로 노출된다.

## API / Dashboard

- `GET /api/operator/pipeline-coherency/status?trade_date=YYYY-MM-DD&limit=100`
- Dashboard fast section: `pipeline_coherency`
- Dashboard `pipeline_summary.coherency`
- Operator aggregate status: `pipeline_coherency`

조회 API는 DB를 수정하지 않는다.

## OBSERVE-safe 점검

Core는 다음 process-local 조건으로 실행한다.

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false
```

점검 명령:

```powershell
python -m tools.ops_pipeline_coherency_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

보고서는 `reports/pipeline_coherency/<UTC>/raw.json`과 `summary.md`에 생성된다.
Core mode, LIVE_SIM/LIVE_REAL 허용 여부, operator/Dashboard 일치, command 및
order-command delta 0을 함께 검사한다.

## SQL 점검

```sql
SELECT candidate_instance_id,
       strategy_observation_id,
       source_run_id,
       source_watermark_hash,
       source_observed_at,
       data_age_sec,
       generated_by
FROM strategy_observations_latest
ORDER BY evaluated_at DESC;

SELECT r.candidate_instance_id,
       s.strategy_observation_id AS strategy_latest_id,
       r.strategy_observation_id AS risk_strategy_id,
       s.source_run_id AS strategy_run_id,
       r.source_run_id AS risk_run_id,
       s.source_watermark_hash AS strategy_watermark,
       r.source_watermark_hash AS risk_watermark
FROM strategy_observations_latest AS s
JOIN risk_observations_latest AS r
  ON r.candidate_instance_id = s.candidate_instance_id
WHERE r.strategy_observation_id <> s.strategy_observation_id
   OR r.source_run_id <> s.source_run_id
   OR r.source_watermark_hash <> s.source_watermark_hash;

SELECT order_plan_id,
       status,
       entry_timing_evaluation_id,
       strategy_observation_id,
       risk_observation_id,
       source_run_id,
       source_watermark_hash,
       source_observed_at,
       generated_by
FROM order_plan_drafts_latest
ORDER BY created_at DESC;
```

## 장애 대응 / Rollback

1. `FAIL` candidate의 stage IDs, run ID, watermark, source event와 timestamp를 보존한다.
2. 해당 candidate의 새 EntryTiming/LIVE_SIM 진입을 계속 차단하고 일반 운영 cycle/batch를 실행하지 않는다.
3. Core/Gateway가 정지되고 SQLite sidecar가 없는 상태에서 아래 offline strict RCA로 분류한다.
4. `ACTIVE_CURRENT`만 `Current-active targeted rebuild`의 preview와 별도 승인된 exact-CAS runner 경로로 복구한다.
5. `HISTORICAL_CLOSED`는 지원 action과 preview eligibility가 모두 확인된 건만 별도 승인된
   disposition 경로로 보낸다. `MISSING_CANDIDATE_MANUAL_REVIEW`와
   `STALE_OTHER_DATE_MANUAL_REVIEW`는 수동 증거를 먼저 확정하고, 그 증거와 exact CAS가 맞는
   건에만 각각의 append-only disposition을 허용한다.
6. runner 결과가 `COMPLETED`인지 확인한 뒤 pipeline coherency와 command/order-command delta가 모두 0인지 다시 확인한다.

일반 Strategy/Risk evaluator, `tools.evaluate_entry_timing`, 평상시 운영 cycle/batch를 직접 실행해서는
안 된다. 이 경로들은 기본 설정에 따라 order-plan draft를 쓸 수 있고, 이 runbook의 audited
environment, preview SHA, runtime fencing, no-plan-write guard를 우회한다. lineage 컬럼을 수동
덮어쓰거나 legacy row를 임의 backfill하지 않는다. 긴급 rollback은 EntryTiming/order-plan loop를
중지하고 기존 row와 진단 evidence를 보존하는 방식으로 한다. LIVE_REAL 또는 주문 gate를 완화해
mismatch를 우회하지 않는다.

## FAST-0R3 RCA view

Canonical `GET /api/operator/pipeline-coherency/status`의 의미와 FAIL 수는 변경하지 않는다.
별도 read-only endpoint가 동일한 DB read snapshot에서 전체 inventory를 500행씩 순회하고,
full count와 시작/종료 ordered digest 및 중복 여부를 검증한다.

```text
GET /api/operator/pipeline-coherency/rca/status
    ?trade_date=YYYY-MM-DD
    &candidate_instance_id=<optional-exact-id>
    &limit=100
    &offset=0
```

RCA 분류는 다음 네 값만 사용한다.

- `HISTORICAL_CLOSED`
- `MISSING_CANDIDATE_MANUAL_REVIEW`
- `STALE_OTHER_DATE_MANUAL_REVIEW`
- `ACTIVE_CURRENT`

RCA의 raw audit count와 disposition 적용 뒤의 effective blocker count를 구분한다.

- raw audit: `manual_review_count`, `current_source_drift_count`, `current_source_drift_unknown_count`
- promotion blocker: `manual_review_pending_count`, `current_source_drift_pending_count`,
  `current_source_drift_unknown_pending_count`, `disposition_pending_count`

disposition은 raw inventory를 삭제하거나 재분류하지 않는다. 따라서 승인된 disposition으로
qualification이 PASS여도 raw count는 0보다 클 수 있다. 승격 판정에는 `*_pending_count`를
사용하고, 원인 감사에는 raw count를 보존한다. 현재 active/current이거나 만료되지 않은 plan
blocker는 disposition으로 숨기지 않는다.

각 item은 `pipeline_fingerprint`와 `subject_version`을 제공한다. callback resolver에는 DB
connection이 아니라 detached deep copy만 전달하며 `READY`, `EFFECTIVE`, `effective=true`,
64자 lowercase CAS hash가 모두 일치할 때만 disposition을 effective로 인정한다. callback
오류, write 시도, inventory drift, duplicate, page/full-count 불일치는 모두 BLOCKED다.

Legacy non-ready row라는 사실만으로 WARN으로 낮추지 않는다. pre-schema59임을 입증하는
권위 있는 외부 evidence resolver가 `READY`, `AUTHORITATIVE`, `pre_schema59=true`와 exact CAS를
제공한 경우에만 별도 qualification view의 `legacy_warn_candidate=true`가 될 수 있다.
Canonical 결과는 이 경우에도 그대로 보존한다.

위 GET API와 아래 preview/list API는 업무 의미상 read-only이지만 실행 중인 Core와 일반
애플리케이션 DB 연결을 사용한다. 그 연결은 SQLite WAL/SHM을 만들거나 관찰할 수 있으므로
offline strict immutable 증거를 대신하지 않는다. 최초 offline 증거를 채집할 때는 API를 먼저
호출하지 말고 Core/Gateway 정지와 `-wal`, `-shm`, `-journal` 부재를 먼저 확인한다.

Core를 시작하지 않는 전체 strict read-only evidence 명령은 다음과 같다.

```powershell
python -B -m tools.ops_pipeline_coherency_rca `
  --db $db `
  --trade-date $tradeDate `
  --out-dir reports/pipeline_coherency_rca
```

도구는 WAL/SHM/rollback-journal의 존재 자체를 거부하고 필수 `quick_check(1)`,
immutable/query-only connection과 외부 read transaction을 사용한다. 모든 page의 full
count/digest가 같아야 하며 DB main/WAL/SHM/journal 지문을 전후 비교한다. Qualification이
PASS가 아니면 evidence 생성에는 성공해도
exit code는 2다.

### 2026-07-16 FAST-0 blocker plan 결과

`disposition_pending_count=158`은 apply 가능한 건수가 아니다. 이는 active-current를 제외한
canonical non-PASS 중 effective disposition이 없는 수일 뿐이다. 승인 판단에는 expected action,
preview eligibility와 reason bucket을 별도로 사용한다.

Strict read-only plan evidence
`reports/fast_track/fast_0_blocker_resolution_plan/20260715T232358.741099Z/raw.json`
(SHA-256 `a51303b49062c0e7e3ab28cdfc2d4c414844384900a010df47c8023dfa75996e`)
기준 분기는 다음과 같다.

| 분기 | 수 | 현재 처리 |
|---|---:|---|
| historical closed | 149 | expired-plan 11과 legacy non-ready 138로 분리 |
| missing-candidate manual review | 9 | 수동 부재 증거 후 orphan disposition preview 적격 |
| active-current | 1 | disposition 금지, audited targeted rebuild preview만 허용 |
| expired `PLAN_READY` expected action | 11 | preview 적격 0; 11건 모두 active-source blocker, 그중 unsafe gateway command 2·broker boundary unknown 1 중첩 |
| legacy non-ready | 138 | 기존 legacy contract 적격 0; active-source zero 미입증과 current-source drift unknown 각각 138 |

따라서 pipeline disposition campaign을 `158건`으로 만들지 않는다. 현재 즉시 preview 적격인 것은
수동 증거가 선행돼야 하는 orphan 9건뿐이다. expired-plan 11건은 source/command/boundary를 먼저
권위 있게 조정하고 새 strict snapshot을 만들어야 한다. legacy 138건은 현 hook에 manifest를
연결하는 것만으로는 부족하며, active-source와 current-source drift를 권위 있게 입증하는 계약과
resolver 구현을 먼저 검토해야 한다. canonical `FAIL=159` raw audit은 어느 경로에서도 삭제하거나
낮추지 않는다.

## Pipeline disposition ledger

Schema 62는 `pipeline_coherency_dispositions`를 빈 append-only ledger로 추가한다. 기존 pipeline,
candidate, order, intent, command row를 migration에서 바꾸지 않는다. 허용 action은 다음뿐이다.

- `DISPOSE_EXPIRED_PLAN_READY`
- `DISPOSE_ORPHAN_PIPELINE_OBSERVATION`
- `DISPOSE_STALE_OTHER_DATE`
- `REVOKE`

각 row는 request idempotency, subject sequence, supersedes chain, pipeline/subject/source/candidate/
downstream/boundary CAS hash, evidence hash와 고정 safety field를 저장한다. UPDATE/DELETE trigger는
항상 abort한다. 늦은 intent/order/command/broker evidence 또는 source/candidate 변화가 나타나면
기존 raw ledger row는 남지만 effective CAS는 즉시 무효가 된다.

Read-only API만 제공하며 write POST endpoint는 제공하지 않는다.

```text
GET /api/operator/pipeline-coherency/disposition-preview?trade_date=YYYY-MM-DD&candidate_instance_id=<URL-encoded-exact-id>&action=<exact-action>
GET /api/operator/pipeline-coherency/dispositions?subject_key=...&limit=100
```

CLI 기본 동작은 exact subject 한 건의 strict read-only preview다.

```powershell
python -B -m tools.resolve_pipeline_coherency `
  --db $db `
  --trade-date $tradeDate `
  --candidate-instance-id $candidateId `
  --action $action
```

Apply는 code merge와 schema 62 migration 후 별도로 승인된 운영 단계에서만 수행한다. `--apply`,
exact six CAS hashes, action별 evidence 계약, safe label, request id와
`--acknowledge APPLY_APPEND_ONLY_PIPELINE_DISPOSITION`을 모두 요구한다. repository 기본 `.env`는
금지하며 별도 `TRADING_ENV_FILE`에서 OBSERVE, 양쪽 live false, kill switch true, incremental
worker와 모든 command producer false, `THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false`, 정확한 DB
path를 검증한다. apply transaction의 SQLite authorizer는 disposition table INSERT 한 건 외의
write/schema 변경을 거부하고 도구가 정의한 core order artifact 집합(`order_plan_drafts`,
`order_plan_drafts_latest`, entry-timing, dry-run/live-sim intent·order·execution, gateway command,
broker-boundary 및 resolution)의 digest가 전후 같지 않으면 rollback한다. 그 밖의 DB write는
`BEGIN EXCLUSIVE`와 global SQLite authorizer로 차단한다. evidence 본문은 embed하지 않으며 label
입력에는 token/계좌번호를 넣지 않는다.
CLI는 알려진 token·credential·계좌번호 값 패턴을 입력 단계에서 거부하고, raw.json과 summary를
key 및 값 패턴 양쪽으로 다시 redaction한다. commands에는 실제 인자를 기록하지 않는다.

`DISPOSE_ORPHAN_PIPELINE_OBSERVATION`은 generic non-empty evidence를 허용하지 않는다. R8
`fast0-pipeline-orphan-manual-evidence.v1` exact JSON, 승인 evidence file SHA-256, 실제 broker
artifact와 그 SHA-256, R6 private target-set SHA-256, private manifest file SHA-256, `Mnnn` alias,
승인 R8 preflight report/SHA와 `evidence_preview_sha256`을 추가로 요구한다.
Evidence의 target은 trade date/private candidate ID/classification/action/six-part CAS와 exact match해야
하며 authoritative terminal-orphan, candidate/source/order-or-broker 부재 claim과 allowlisted
broker provenance, 실제 artifact SHA/size, final trade-date coverage와 hashed account scope를 검증한다.
CLI는 RW open 전과 `BEGIN EXCLUSIVE` 안에서 private file identity, current CAS와 신규 request의
preflight DB main fingerprint를 재검증하고, service direct call과 저장된 effective projection도 safe
binding을 검증한다. 기존 orphan ledger의 legacy generic/invalid/noncanonical row 또는 invalid chain이
하나라도 있으면 preflight는 차단된다. 따라서 `{"redacted":true}`나 legacy generic orphan evidence는
유효하지 않다. Strict read-only 준비 절차는
`docs/runbook_pipeline_orphan_evidence_preflight_ko.md`를 따른다.

정상 exit 0은 preview의 `ELIGIBLE`, apply의 `APPLIED` 또는 `IDEMPOTENT`뿐이다.
`*_RECONCILED_WITH_WARNING`, `OUTCOME_UNKNOWN`, `COMMITTED_POSTCHECK_FAILED`,
`COMMITTED_EVIDENCE_WRITE_FAILED` 등은 exit 2이며 운영자 조정 대상이다. committed 또는
outcome-unknown 결과에 새 request ID를 만들어 자동 재시도하지 않는다. exact request ID와
six-part CAS, evidence SHA, durable disposition row를 subject list API 또는 strict read-only SQL로
조정한다. durable row와 현재 idempotency가 모두 일치한다고 증명된 경우에만 동일 request ID와
동일 인자로 idempotent 경로를 다시 확인한다. late drift나 durable 결과 부재가 있으면 중단한다.
evidence write 실패에서는 report file이 생성되지 않았을 수 있으므로 빈 `report_paths`도 그대로
보존한다.

## Current-active targeted rebuild

Disposition 대상이 아니라 genuinely current-active인 candidate만 1~5건씩 재평가할 수 있다.
기본 CLI 동작은 read-only preview이고, preview 자체도 repository 기본 `.env`가 아닌 별도 audited
`TRADING_ENV_FILE`을 요구한다.

```powershell
$env:TRADING_ENV_FILE = $auditedObserveEnv
python -B -m tools.run_targeted_pipeline_rebuild `
  --db $db `
  --trade-date $tradeDate `
  --candidate-instance-id $candidateId
```

Preview는 candidate state/current trade date, active source 실제 수와 candidate count, 전체 active
source fingerprint, current context/freshness, source lineage, audited settings hash와 order artifact
count/high-water/schema guard를 검증하고 `preview_sha256`을 반환한다. batch 6, duplicate,
closed/stale/non-current state,
source drift, producer enabled, engine non-observe, plan-write enabled은 실행 전에 거부한다.

실행은 별도 운영 승인을 받은 경우에만 다음 추가 인자를 사용한다.

```text
--run
--expected-preview-sha256 <exact preview SHA-256>
--acknowledge RUN_TARGETED_PIPELINE_REBUILD_WITHOUT_ORDER_INTENT
```

Runner는 하나의 `evaluation_pipeline` lease/fencing token, 하나의 source run과 하나의 outer
transaction에서 source와 candidate를 다시 검증한 뒤 strategy→risk→entry timing→runtime fence만
저장한다. 최대 wall time은 lease TTL보다 짧고 각 stage에서 fence를 갱신한다. 허용 write table은
Strategy/Risk/EntryTiming과 runtime fence뿐이며 EntryTiming은 evaluation row만 저장하고 order-plan
draft는 저장하지 않는다.
main DB 외 write와 transaction/PRAGMA/DDL 제어는 authorizer가 차단한다. target/non-target pipeline
row의 exact delta·canonical row, send/cancel/modify count, order artifact count/high-water/schema guard,
fence/source/env가 commit 전에 일치하지 않으면 모든 stage write를 rollback한다. `COMPLETED`만
exit 0이다. data commit 뒤 lock cleanup만 실패한 `COMMITTED_CLEANUP_FAILED`와 commit 이후의
`COMMITTED_POSTCHECK_FAILED`, `COMMITTED_REPORT_FAILED`, `COMMITTED_EVIDENCE_WRITE_FAILED`는
write가 durable한 committed 상태이며 모두 exit 2다. commit 호출이 예외를 반환하면 동일
connection의 상태만으로 미적용을 판정하지 않는다. 별도의 `mode=ro`·`query_only` connection에서
canonical target rows, exact delta, source watermark, order artifact와 fencing token을 모두 재확인한
경우에만 `COMMITTED_TRANSACTION_SIGNAL_FAILED`(`committed=true`)로 판정하며, 독립 확인이
불완전하면 `OUTCOME_UNKNOWN`으로 차단한다. `OUTCOME_UNKNOWN`은 적용/미적용 어느 쪽도 가정하지
않는다.

이 상태들에는 자동 재실행을 하지 않는다. exact candidate ID, preview SHA, source run ID, owner ID와
fencing token을 보존하고 strict read-only로 canonical stage row와 live lock을 조정한다. durable
stage write가 없고 live lock도 없다는 별도 재승인 증거가 있을 때만 새 preview/run을 허용한다.
committed가 확인되면 해당 write를 이미 적용된 것으로 취급한다. evidence write 실패에서는 파일
evidence가 남았다고 가정하지 않는다. 현재 운영 plan의 `ACTIVE_CURRENT=1`은 disposition 대상이
아니며, audited OBSERVE environment에서 targeted rebuild preview가 적격일 때만 별도 승인된 exact
preview SHA run으로 진행한다. preview가 차단되면 강제 실행하지 않는다.
