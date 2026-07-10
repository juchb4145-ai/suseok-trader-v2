# Market Index TR Bootstrap Runbook

## 범위

이 경로는 Kiwoom OpenAPI+의 generic `request_tr` command를 사용해 KOSPI/KOSDAQ
업종 현재가를 bootstrap하고, parent `tr_response`를 market-index projection과 outbox에
직접 연결한다. 주문, 정정, 취소, SOR command는 생성하지 않는다.

OpenAPI+ 공식 개발가이드는 TR별 입력/출력 항목을 KOA Studio에서 확인하도록 안내한다.
2026-07-10 KOA StudioSA 2.34와 격리 OBSERVE Core에서 `OPT20001` KOSPI/KOSDAQ
실제 callback을 확인했으므로 parser status 기본값은 `VERIFIED`다. bootstrap enable 기본값과
realtime cutover gate는 별개이며 계속 disabled/fail-closed다.

## 기본 안전 설정

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false

KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED=false
MARKET_INDEX_TR_BOOTSTRAP_TR_CODE=OPT20001
MARKET_INDEX_TR_BOOTSTRAP_SCREEN_NO=5701
MARKET_INDEX_TR_BOOTSTRAP_PARSER_STATUS=VERIFIED
MARKET_INDEX_TR_BOOTSTRAP_MARKET_TYPES=KOSPI=0,KOSDAQ=1
MARKET_INDEX_TR_BOOTSTRAP_INDUSTRY_CODES=KOSPI=001,KOSDAQ=101
```

기본값은 disabled다. enabled여도 `run-once`의 기본은 plan-only이므로 Gateway command를
저장하지 않는다. `queue_commands=true`를 명시한 경우에만 KOSPI/KOSDAQ `request_tr`
2건을 저장한다. Core가 OBSERVE가 아니거나 LIVE 허용 flag가 하나라도 true면 queue를
`BLOCKED_SAFETY`로 거부한다.

## Source lineage v1

각 command는 다음 계약을 사용한다.

- source/projection source: `KIWOOM_TR_BOOTSTRAP_MARKET_INDEX`
- request ID: `market_index_tr_bootstrap:<INDEX_CODE>:<trade_date>:<minute_bucket>`
- request name: `market_index_tr_bootstrap_<index_code>`
- `tr_bootstrap_contract_version=v1`
- metadata의 `index_code`, `market_type`, `industry_code`, `parser_status`
- `row_mode=single`, `output_record_name=업종현재가`
- `observe_only=true`, `no_order_side_effects=true`

같은 trade date/1분 bucket/index의 idempotency key는 동일하므로 즉시 재실행은
`DUPLICATE`로 흡수된다. 다음 분의 명시적 재시도만 새 command가 된다.

Kiwoom 동기/비동기 TR handler는 command metadata를 `BrokerTrResponse.metadata`에 그대로
보존한다. Core는 request ID의 index와 metadata index가 같고, TR code, source, contract,
response success, 단일 row가 모두 맞는 경우에만 projection한다.

Gateway command는 KOA 단일 output block의 `현재가`, `전일대비`, `등락률`만 요청한다.
같은 TR의 `업종현재가_시간별` 반복 block은 읽지 않는다. parser는 저장/replay 호환을 위해
다음 alias를 허용한다.

- 현재가: `현재가`, `업종현재가`, `price`, `current_price`
- 전일대비: `전일대비`, `change_value`, `change`
- 등락률: `등락률`, `등락율`, `change_rate`, `change_rate_pct`
- 시각: `체결시간`, `trade_time`, `time`; 없으면 response timestamp

필드명이나 단위가 KOA Studio와 다르면 parser status를 올리지 말고
`MARKET_INDEX_TR_BOOTSTRAP_ROW_PARSE_FAILED` evidence를 보존한 뒤 mapping을 수정한다.

## Projection / Outbox

인식된 parent `tr_response`는 기존 market-data job 외에 `market_index`와
`market_regime` sibling outbox를 생성한다. `market_index_tick_samples.event_id`는 parent
event ID를 사용하고 metadata에 parent event, request ID/name, TR code, parser status와
raw field names를 보존한다.

Gateway inline 경로에서는 bootstrap event의 market-index effective skip을 허용하지 않고
`TR_BOOTSTRAP_INLINE_ONLY`를 남긴다. worker apply가 명시적으로 enabled된 경우에는 같은
parent event를 다시 파싱해 idempotent sample/latest/bar projection을 수행할 수 있다.

reconcile은 realtime `market_index_tick`과 v1 bootstrap `tr_response`를 함께 조회한다.

- valid v1 + usable sample: `TR_BOOTSTRAP` source로 집계
- source/contract/index/TR mismatch: `MARKET_INDEX_TR_BOOTSTRAP_LINEAGE_INVALID`
- parser unverified: projection 정합성과 별도 INFO, append-only readiness false
- sample/outbox/error gap: 기존 market-index FAIL 기준 적용

## API / Dashboard

```text
GET  /api/operator/market-index/tr-bootstrap/status
POST /api/operator/market-index/tr-bootstrap/run-once
POST /api/operator/market-index/tr-bootstrap/run-once?queue_commands=true
```

POST는 local token을 요구한다. queue 실행 전에는 Gateway 로그인/heartbeat, command backlog,
당일 중복 run 여부를 확인한다. 응답 command는 `request_tr`만 허용하며 `send_order` 또는
`cancel_order`가 있으면 즉시 FAIL 처리한다.

- Operator aggregate: `market_index_tr_bootstrap`
- Dashboard full/fast: `market_index_tr_bootstrap`
- Dashboard `market_indexes.tr_bootstrap`
- 명시적으로 함께 요청한 `pipeline_summary.market_index_tr_bootstrap`

## Ops 점검

plan-only와 read-only API matrix:

```powershell
python -m tools.ops_market_index_tr_bootstrap_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

bootstrap event가 이미 있는 fixture/운영 점검:

```powershell
python -m tools.ops_market_index_tr_bootstrap_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --expect-events
```

보고서는 `reports/market_index_tr_bootstrap/<UTC>/raw.json`과 `summary.md`에 생성된다.
Core mode, adapter/source contract, event/sample gap, Dashboard 일치와 점검 전후
command/order-command delta 0을 확인한다.

## 2026-07-10 비장중 실제 기능 evidence

- 격리 Core `OBSERVE`, LIVE_SIM/LIVE_REAL false, Kiwoom 모의투자 로그인 성공
- KOSPI `시장구분=0/업종코드=001`: `7475.94`, `+184.03`, `+2.52%`
- KOSDAQ `시장구분=1/업종코드=101`: `837.43`, `+43.43`, `+5.47%`
- 두 callback 간격 약 80초, 각 response row 1건, raw field 3개 일치
- market-index worker `APPLIED=2`, reconcile `PASS`, parser verified/data usable `2/2`
- projection error, outbox ERROR/DEAD_LETTER, candidate ingest, order command 모두 0
- `quick_check=ok`, 운영 DB와 `.env` 변경 없음
- Reports: `reports/market_index_tr_bootstrap_offhours/20260710T143434Z/bootstrap_summary.md`,
  `reports/market_index_tr_bootstrap_offhours/20260710T143434Z/projection_summary.md`

이 evidence는 TR transport/parser 기능 PASS다. realtime source가 아니므로 KRX 장중 freshness,
regime continuity, effective cutover 또는 연속 10거래일 readiness anchor로 계산하지 않는다.

## PASS / WARN / FAIL

- `PASS`: adapter IMPLEMENTED, parser VERIFIED, event/sample gap 0, API/Dashboard 일치,
  command/order-command delta 0.
- `WARN`: 안전 기본값 disabled, event/sample 없음 또는 realtime 운영 gate 대기.
- `FAIL`: OBSERVE 이탈, adapter/source contract 누락, lineage mismatch, sample gap,
  plan-only command 생성 또는 주문 command 변화.

## NXT 제한

KOSPI/KOSDAQ 지수는 KRX 전용 evidence다. NXT stock tick, NXT quote TR, SOR/통합시세를
market-index bootstrap 성공 근거로 사용하지 않는다. NXT는 generic TR transport,
ordering 또는 venue normalization 회귀에는 활용할 수 있지만 KRX index parser,
freshness, regime 또는 cutover gate를 대체하지 않는다.

## Rollback

1. `KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED=false`로 재기동한다.
2. pending bootstrap `request_tr`는 주문 command와 분리해 만료/상태를 확인한다.
3. event/sample/outbox/error evidence를 삭제하지 않는다.
4. realtime market-index 경로와 bootstrap source를 섞어 backfill하지 않는다.
5. KOA mapping 수정 후 격리 fixture, worker, reconcile, ops 순서로 다시 검증한다.
