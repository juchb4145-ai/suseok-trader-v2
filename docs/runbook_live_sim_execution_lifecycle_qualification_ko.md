# FAST-0 LIVE_SIM Execution Lifecycle Qualification Runbook

## 목적

이 절차는 `live_sim_errors`와 `live_sim_lifecycle_events`의 동일한 과거 감사 흔적을
두 번 더하거나, 과거 오류를 현재 주문 lifecycle 장애로 오인하지 않기 위한 strict
read-only 점검이다. raw 행을 수정·삭제·재분류하지 않고 논리 subject 단위로 다음을
구분한다.

- `ACTIVE_LIFECYCLE_BLOCKER`: 현재 주문·체결·포지션 lifecycle이 닫히지 않은 blocker
- `HISTORICAL_RUNTIME_STATUS_AUDIT`: 이미 닫힌 runtime status의 과거 감사 흔적
- `MANUAL_REVIEW_BLOCKER`: malformed, 식별 불가, 두 surface 불일치 등 자동 판정 금지 대상

latest reconcile snapshot의 실제 mismatch/blocking state와 malformed reconcile evidence도
effective blocker에 포함한다. 반대로 clean latest snapshot에 연결된 과거 mismatch event는
historical reconcile evidence로만 집계한다.

canonical 오류 수와 qualification은 서로 다른 값이다. canonical 오류가 남아 있어도
검증된 historical audit만 존재하고 active/manual blocker가 0이면 qualification은
`PASS`일 수 있다. canonical 행이나 오류 상태를 낮추거나 지우는 의미가 아니다.

## 안전 계약

- 운영 DB를 UPDATE/DELETE하거나 disposition을 추가하지 않는다.
- Core, Gateway, Kiwoom을 이 도구가 시작하지 않는다.
- SQLite source는 writer가 없고 `-wal`, `-shm`, `-journal`이 모두 없는 checkpointed
  상태여야 한다. writer가 살아 있는 상태에서 sidecar를 삭제하거나 이동하지 않는다.
- DB는 `mode=ro&immutable=1`, `PRAGMA query_only=ON`으로 연다.
- `PRAGMA quick_check(1)`은 필수이며 생략 옵션이 없다.
- `app_name=suseok-trader-v2`, schema `62`가 정확히 일치해야 한다.
- 점검 전후 main DB 전체 SHA-256과 파일 지문이 같아야 한다.
- 모든 page를 수집하고 start/scan/end inventory digest가 전 page에서 같아야 한다.
- 보고서에는 raw DB 행, raw payload, error message, 계좌 식별자, token을 기록하지 않는다.

## Operator API

```text
GET /api/operator/live-sim/execution-lifecycle/status
GET /api/operator/live-sim/execution-lifecycle/status?limit=500&offset=0
GET /api/operator/live-sim/execution-lifecycle/status?code=005930&limit=100&offset=0
```

응답은 global canonical/qualification 요약, classification count, full pagination과
안전한 logical-subject summary만 제공한다. `code` filter는 진단용 페이지 범위만 좁힌다.
global qualification, global count, inventory digest를 축소하지 않는다. 이 operator evidence
endpoint는 local token이 필요하며 token 값은 응답이나 report에 기록하지 않는다.

## Strict offline 실행

프로세스와 sidecar 부재를 별도로 확인한 뒤 다음을 실행한다.

```powershell
$python = ".\.venv\Scripts\python.exe"
$db = "<승인된 checkpointed schema-62 DB 절대경로>"

& $python -B -m tools.ops_live_sim_execution_lifecycle_check `
  --db $db `
  --out-dir "reports\live_sim_execution_lifecycle"
```

특정 종목 page도 함께 보고 싶을 때만 `--code 005930`을 사용한다. 이 filter가 global
blocker를 숨기지는 않는다.

## PASS 기준

- DB identity/schema/quick check/strict read-only connection 계약 PASS
- 전후 파일 지문 동일
- full count와 전 page 수집 count 동일, logical subject 중복 0
- 모든 page의 inventory count/digest/qualification이 동일
- `qualification_status=PASS`
- lifecycle active/manual, reconcile active/manual, 전체 effective blocker가 각각 0
- `raw_rows_recorded=false`

하나라도 어긋나면 verdict는 `BLOCKED`, process exit code는 `2`다. historical audit count는
보존되어 보고되며 그 자체만으로 effective blocker가 되지 않는다.

## Market-open RCA 연동

`tools.ops_market_open_rca`는 새 qualification endpoint를 필수 입력으로 사용한다.
endpoint 누락, HTTP 실패, 필수 count/flag/digest 누락, PASS와 effective blocker count의
모순은 모두 `LiveSim=BLOCK`이다. 기존 `/api/live-sim/errors`의 최근 raw 행 존재 여부로
qualification을 대신하지 않는다.

## 금지 사항

- raw lifecycle/error 행 UPDATE/DELETE
- classifier 결과를 근거로 canonical 오류 상태 변경
- `--code` 결과를 global clear 증거로 사용
- quick check 또는 전체 pagination 생략
- raw JSON에 원 payload, error message, 계좌번호, token 복사
- 이 절차 중 Core/Gateway 시작 또는 주문 command 생성
