# FAST-2A Point-in-Time Alpha Replay

FAST-2A는 운영 DB를 변경하지 않고 수집된 Gateway event를 새 격리 DB로 복제해,
각 event가 실제로 관측된 시각까지만 보이는 결정적 replay를 만든다. 이 단계에는 주문,
GatewayCommand 생성, DRY_RUN/LIVE_SIM write, 브로커 호출이 없다.

## 모드와 시간 계약

- `STRUCTURAL_REPLAY`: 입력 순서·격리·side-effect 부재를 검증한다.
- `ALPHA_REPLAY`: structural 검증에 더해 virtual clock으로 freshness와 session을 판정한다.

Virtual clock은 `source_received_at` 순서로만 전진한다. event timestamp가 그 event의
관측 시각보다 미래이거나, 현재 clock보다 미래인 상태를 읽으려 하면
`POINT_IN_TIME_VIOLATION`으로 실패한다. tick/index/theme/candidate freshness,
Strategy/Risk age, EntryTiming/OrderPlan expiration, cooldown, entry window, min/max hold,
EOD 판단 API는 시스템 현재 시각이 아니라 이 clock만 사용한다.

동일한 source record hash, event-order hash, time-policy config hash, commit SHA에서는
경로와 실행 시각에 관계없이 같은 `result_sha256`이 나와야 한다.

## 입력 coverage와 Alpha 판정

report는 다음 입력을 각각 `PRESENT` 또는 `MISSING`으로 기록한다.

- price tick, condition event, candidate quote refresh TR
- market symbols, market-index realtime/TR bootstrap
- market scan TR
- 당시 theme/membership lineage
- replay에 결속한 config lineage

market scan의 모든 응답에 `pagination_complete=true`와 비어 있지 않은
`page_lineage`가 없으면 `FIRST_PAGE_ONLY`다. 이 경우 구조적 replay가 안전하게
끝나도 `ALPHA_QUALIFIED=false`이며, 입력 누락도 같은 방식으로 fail-closed 처리한다.

## 실행

Core와 Gateway를 먼저 종료하고 source DB가 더 이상 변하지 않는 상태에서 실행한다.

```powershell
$python = ".\.venv\Scripts\python.exe"
& $python -B -m tools.ops_alpha_replay `
  --source-db "storage\observe\2026-07-20\market-open-observe.sqlite3" `
  --trade-date "2026-07-20" `
  --mode ALPHA_REPLAY
```

도구는 다음을 수행한다.

1. source main+WAL을 파일 읽기만으로 격리 snapshot에 복사하고 main/WAL/SHM
   fingerprint 전후 불변을 확인한다. SQLite는 원본을 직접 열지 않는다.
2. snapshot을 strict read-only로 export한다.
3. 새 격리 DB만 만들고 projection replay SQLite authorizer를 설치한다.
4. accepted market event를 authoritative row order로 import한다.
5. virtual clock replay와 coverage를 계산하고 `reports/alpha_replay/`에
   `raw.json`, `summary.md`를 남긴다.

snapshot에 WAL이 없으면 SQLite `immutable=1`을 사용해 read-only 열기 자체가 WAL/SHM을
만들지 않게 한다. WAL이 있으면 committed WAL 내용까지 읽기 위해 `mode=ro`를 사용하며,
이때 생성·변경될 수 있는 SHM은 격리 snapshot 쪽에만 존재한다. 원본 fingerprint가
바뀌거나 point-in-time violation 또는 side effect가 있으면 exit code 2다.
