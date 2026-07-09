# Dashboard Snapshot Fast Path Runbook

## 목적

PR-10 장중 검증에서 `condition_event`/outbox/reconcile core 조건은 PASS했지만 `/api/dashboard/snapshot` full build가 HTTP timeout으로 ops script verdict를 FAIL로 만들 수 있었다. PR-10.5는 dashboard 운영성 패치이며 trading/projection/order behavior를 바꾸지 않는다.

## Endpoint

장중 검증 스크립트는 아래 형태의 targeted fast snapshot을 사용한다.

```text
GET /api/dashboard/snapshot?fast=true&sections=gateway,market_data,projection_outbox,pipeline_summary,errors&detail=summary&limit=20&timeout_budget_ms=5000
```

응답에는 다음 메타데이터가 포함된다.

- `fast_path`
- `requested_sections`
- `included_sections`
- `skipped_sections`
- `section_latency_ms`
- `total_latency_ms`
- `timeout_budget_ms`
- `warnings`

## Full Snapshot과 Fast Snapshot

- full snapshot: 기존 `/api/dashboard/snapshot?detail=summary` 경로이며 dashboard 화면 호환을 유지한다.
- fast snapshot: `sections` 또는 `fast=true` 요청에서만 사용된다. 요청된 read-only 섹션만 계산하며 AI cards, no-buy sentinel rebuild, theme leadership rebuild, live_sim 상세 리스트 같은 heavy builder를 기본 실행하지 않는다.
- cache: fast cache는 `db_path/detail/limit/sections/fast` 기준으로 분리된다. cache lock은 hit check와 write에만 사용하고 snapshot build 전체를 막지 않는다.

## Timeout 판정

- core routing/outbox/reconcile 실패: ops verdict `FAIL`
- dashboard fast snapshot timeout: ops verdict `WARN`, `block_next_pr=true`
- timeout budget으로 optional section skip: ops verdict `WARN`, `block_next_pr=true`
- core PASS + dashboard fast snapshot PASS + latency 정상: ops verdict `PASS`

## PR-11 진입 전 확인 조건

1. PR-10 ops script가 targeted fast snapshot으로 2~5초 수준에서 응답한다.
2. `projection_outbox`에 `ERROR`/`DEAD_LETTER`가 없다.
3. latest reconcile이 `PASS` 또는 운영상 허용된 `WARN`이다.
4. `condition_event_effective_skip_count=0`을 유지한다.
5. dashboard timeout이 core 검증 실패와 분리되어 보고된다.

## 안전 원칙

- 이 runbook은 dashboard 조회 성능 개선만 다룬다.
- `LIVE_REAL`, 주문 정책, safety gate, 매수 gate, market_data cutover 정책은 변경하지 않는다.
- Dashboard에는 주문/AI 실행 버튼을 추가하지 않는다.
