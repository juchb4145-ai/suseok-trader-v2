# Incremental Evaluation Queue 운영 런북

## 목적과 안전 경계

Incremental Evaluation Queue는 가격 또는 candidate source 변화 뒤 Candidate → Strategy →
Risk 관측만 갱신한다. 이 경로 자체는 order intent나 broker command를 만들지 않지만, 갱신된
관측은 나중에 다른 producer가 소비할 수 있다.

- LIVE_SIM/LIVE_REAL을 허용하지 않는다.
- 기존 evaluation runtime lock과 fencing token을 사용한다.
- retry-exhausted 또는 dead-letter audit row를 삭제하지 않는다.
- historical dead letter를 active queue로 reset하지 않는다.
- disposition apply/revoke와 future recovery는 Core POST API가 아니라 전용 offline CLI에서만
  수행한다.

## Raw와 effective 상태

`GET /api/operator/incremental-evaluation/status`는 raw audit와 append-only disposition을 함께
노출한다.

| 필드 | 의미 |
|---|---|
| `dead_letter_count`, `raw_dead_letter_count` | 보존된 raw `DEAD_LETTER` 수 |
| `effective_dead_letter_count` | FAST-0에서 아직 해소되지 않은 effective 수 |
| `active_unresolved_dead_letter_count` | 현재 active candidate의 unresolved 수 |
| `historical_pending_disposition_count` | terminal evidence는 있으나 disposition 전인 historical 수 |
| `historical_disposed_dead_letter_count` | 검증된 append-only disposition이 유효한 historical 수 |
| `manual_review_dead_letter_count` | 자동 분류할 수 없어 운영자 판단이 필요한 수 |
| `invalid_disposition_count` | chain, fingerprint 또는 disposition 계약이 유효하지 않은 수 |
| `raw_status` | raw audit 기준 상태 |
| `effective_status` | effective 운영 상태 |
| `fast_0_status` | `CLEAR` 또는 `BLOCKED` |

Raw count는 disposition 뒤에도 감소하지 않는다. FAST-0 effective clear의 정상 예시는
`raw_dead_letter_count=38`, `historical_disposed_dead_letter_count=38`,
`effective_dead_letter_count=0`, `fast_0_status=CLEAR`다.

다음 중 하나라도 있으면 effective 상태는 fail closed한다.

- active unresolved
- historical pending disposition
- manual review
- invalid disposition
- retry-exhausted active queue
- disposition schema 또는 chain 판정 불가

## Dead-letter 이동

정상 worker failure는 attempt를 증가시킨다. 다음 attempt가 retry limit에 도달하면 같은
transaction에서 dead-letter insert와 active queue delete를 수행한다. legacy retry-exhausted
active row는 guarded sweep으로 보존될 수 있다.

새 event는 이전 failure evidence를 지우거나 guarded recovery를 우회하지 않는다. 해당
candidate에 유효한 unresolved disposition 상태가 있으면 `BLOCKED_DEAD_LETTER`로 중단한다.
검증된 `HISTORICAL_DISPOSED` 또는 recovery 완료 뒤 남은 raw audit 자체는 차단 조건이 아니다.

## Read-only API

```text
GET /api/operator/incremental-evaluation/status
GET /api/operator/incremental-evaluation/dead-letters?limit=100
GET /api/operator/incremental-evaluation/dead-letters/effective?bucket=HISTORICAL_PENDING_DISPOSITION&limit=100
GET /api/operator/incremental-evaluation/dead-letters/disposition-preview?dead_letter_id=...
```

Effective list의 `bucket`은 다음 값만 허용한다.

- `ACTIVE_UNRESOLVED`
- `HISTORICAL_PENDING_DISPOSITION`
- `HISTORICAL_DISPOSED`
- `MANUAL_REVIEW`
- `RECOVERY_PENDING`
- `RECOVERY_VERIFIED`
- `INVALID_DISPOSITION`

성공한 preview는 `eligible`, `reason_codes`, `dead_letter_fingerprint`, `candidate_version`, raw
dead-letter, candidate, active queue, effective disposition snapshot을 반환한다. Not-found,
eligibility 또는 fingerprint/version CAS 충돌은 reason detail을 포함한 HTTP 409로 fail
closed한다. 또한 다음 safety field가 모두 고정돼야 한다.

```text
read_only=true
observe_only=true
no_order_side_effects=true
auto_run_evaluation=false
live_sim_allowed=false
live_real_allowed=false
```

Disposition write POST API는 제공하지 않는다. 기존 endpoint는 의도적으로 비활성화된다.

```text
POST /api/operator/incremental-evaluation/dead-letters/reset
HTTP 409 UNGUARDED_RESET_DISABLED
```

기존 sweep endpoint는 legacy queue row를 dead-letter로 옮기는 기능일 뿐 historical disposition
또는 recovery가 아니다. FAST-0R2 운영 단계에서 호출하지 않는다.

## OBSERVE-safe 점검

기본 ops check는 GET만 호출하고 완전 read-only다. 과거의
`--sweep-retry-exhausted`, `--reset-dead-letter-id` mutation flag는 제거됐다.

```powershell
python -B -m tools.ops_incremental_evaluation_queue_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

기본 계약은 raw dead-letter가 있으면 canonical FAIL을 유지한다. Append-only disposition을
FAST-0 qualification에 반영하려면 명시적으로 effective clear를 요구한다.

```powershell
python -B -m tools.ops_incremental_evaluation_queue_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --require-effective-clear
```

이 모드는 raw row를 숨기지 않는다. Effective 필드 누락, raw/effective count 불일치,
`fast_0_status!=CLEAR`, pending/manual/invalid count, 점검 중 queue/dead-letter 변화는 모두
fail closed한다. 점검 전후 command 및 order-command delta도 0이어야 한다.

Core가 실행 중이지 않다면 이 API 점검을 위해 운영 DB에 연결해 Core를 새로 시작하지 않는다.
Core startup은 schema 초기화나 일반 RW 연결을 수행할 수 있으므로 최초 운영 DB 점검은 별도
strict read-only 절차를 따른다.

## Offline disposition preview

전용 CLI의 기본 동작은 strict read-only preview다.

```powershell
python -B -m tools.resolve_incremental_evaluation_dead_letter `
  --db $db `
  --dead-letter-id $deadLetterId
```

Preview에서 다음을 확인한다.

1. raw dead-letter fingerprint와 candidate version
2. candidate가 `CLOSED`이고 terminal timestamp가 존재함
3. 동일 candidate active queue가 없음
4. bucket이 `HISTORICAL_PENDING_DISPOSITION`임
5. disposition chain이 유효함
6. 모든 safety field가 안전값임

## Disposition apply/revoke

Apply/revoke는 코드 merge 및 DB migration과 분리된 승인 운영 단계다. 구체 옵션은
`python -B -m tools.resolve_incremental_evaluation_dead_letter --help`를 정본으로 사용한다.

필수 조건:

1. 별도 audited `TRADING_ENV_FILE`을 사용하고 repository 기본 `.env`를 상속하지 않는다.
2. OBSERVE profile/mode, LIVE_SIM/LIVE_REAL false, worker 및 모든 command producer off를
   검증한다.
3. `THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false`를 safe env에 명시한다.
4. DB 경로가 safe env와 일치하고 evaluation lease가 비어 있어야 한다.
5. preview의 expected dead-letter fingerprint와 candidate version을 apply 직전에 재확인한다.
6. 비식별 evidence reference, SHA-256, operator identity, reason과 명시적 acknowledgement를
   제공한다.
7. 한 row씩 preview → apply → strict read-only preview 순서로 확인한다.
8. DB main 파일은 hard-link alias가 아닌 단일 link여야 하며 non-empty rollback journal이
   없어야 한다. Strict preview는 non-empty WAL도 거부한다.

Apply는 `incremental_evaluation_dead_letters`를 UPDATE/DELETE하지 않는다.
`incremental_evaluation_dead_letter_dispositions`에 append하며 revoke도 새 event로 기록한다.

## Future active recovery

Historical disposition과 future active recovery를 혼합하지 않는다. 현재 FAST-0의 CLOSED legacy
38건은 recovery/reset 대상이 아니다.

향후 실제 active defect만 다음 조건에서 recovery 후보가 될 수 있다.

- root cause fix 및 evidence 검증
- candidate active/current, source link와 freshness 검증
- active queue 부재
- worker/LIVE/모든 command producer off
- explicit safe env와 DB 경로 일치
- free lease/fence와 fingerprint/version CAS
- canary 1건을 먼저 reset하고 자동 evaluation은 실행하지 않음
- canary 검증 뒤에만 최대 5건 batch 허용
- canary 재평가가 다시 retry-exhausted 되면 기존 raw row를 수정하지 않고 새 raw
  dead-letter generation을 append하며, 해당 canary의 `VERIFY_CANARY`는 거부
- disposition, queue, runtime lock/fence write는 승인된 대상·요청·건수·fencing token과
  정확히 일치해야 하며 apply 뒤 전체 row-set 불변 검증을 통과

Batch 6 이상, canary 미검증, stale fingerprint/version, CLOSED candidate, source lineage 누락,
worker active, 설정 판정 불가는 모두 apply 전에 거부한다.

## SQL 점검

```sql
SELECT COUNT(*) AS raw_dead_letter_count
FROM incremental_evaluation_dead_letters
WHERE status = 'DEAD_LETTER';

SELECT action, COUNT(*) AS count
FROM incremental_evaluation_dead_letter_dispositions
GROUP BY action
ORDER BY action;

SELECT candidate_instance_id, code, attempts, last_error, dead_lettered_at
FROM incremental_evaluation_dead_letters
WHERE status = 'DEAD_LETTER'
ORDER BY dead_lettered_at, dead_letter_id;
```

Schema 점검에는 raw/ledger UPDATE·DELETE 방지 trigger와 다음 index를 포함한다.

- `trg_incremental_evaluation_dead_letters_no_update`
- `trg_incremental_evaluation_dead_letters_no_delete`
- `trg_incremental_dead_letter_dispositions_no_update`
- `trg_incremental_dead_letter_dispositions_no_delete`
- `idx_incremental_evaluation_dead_letter_candidate_time`
- `idx_incremental_dead_letter_disposition_effective`
- `idx_incremental_dead_letter_disposition_session`

## Rollback

이상 시 worker와 모든 producer를 계속 off로 유지하고 raw row와 disposition ledger를 보존한다.
Raw dead-letter를 직접 DELETE/UPDATE하거나 attempts를 수동 감소시키지 않는다. 잘못된
disposition은 revoke event로만 종결하며, 원인 규명 전 reset/retry를 수행하지 않는다.
