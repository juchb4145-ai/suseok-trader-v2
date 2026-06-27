# AI RCA Workflows

## 요약

PR AI-3는 운영자가 “왜 오늘 주문이 없었는가”, “왜 이 Candidate가 막혔는가”를 읽기 쉽게 보는 deterministic RCA report workflow를 추가한다. RCA report는 operator review artifact다. Strategy input, Risk input, OMS input, OrderIntent, GatewayCommand, buy/sell signal, live-mode control이 아니다.

## Workflow 종류

| Workflow | 질문 |
| --- | --- |
| `NO_TRADE_RCA` | 특정 trade date에 왜 주문/체결 흐름이 없었는가 |
| `CANDIDATE_BLOCK_RCA` | candidate가 왜 blocked, stale, closed, data-waiting, not progressing 상태였는가 |

`run_ai=false`가 기본이다. deterministic report는 OpenAI API key 없이 동작한다.

## Deterministic Report vs AI Insight

| 구분 | 의미 |
| --- | --- |
| deterministic report | local SQLite projection과 context packet으로 항상 생성되는 report |
| AI insight | `run_ai=true`를 명시했을 때만 optional로 연결되는 structured output |

AI가 disabled, unavailable, invalid, policy rejected여도 deterministic report는 저장된다. 이때 `ai_request_id`는 연결될 수 있지만 `ai_insight_id`는 valid output에만 기록된다.

## No-trade RCA

`build_no_trade_rca_report(connection, trade_date, run_ai=false)`:

1. `NO_TRADE_RCA` context packet build/persist
2. safety posture, gateway transport, market data, theme, candidate funnel, strategy, risk, projection/evaluation error, AI status section 생성
3. `NO_ORDER_PATH_BY_DESIGN` warning 포함
4. candidate/strategy/risk reason code summary 생성
5. root cause category 분류
6. `run_ai=true`일 때만 AI-2 runner optional 호출
7. report와 section 저장

## Candidate Block RCA

`build_candidate_block_rca_report(connection, candidate_instance_id, run_ai=false)`:

1. `CANDIDATE_BLOCK_RCA` context packet build/persist
2. candidate detail, latest context, sources, transitions, market readiness, theme context, latest strategy/risk observation, related errors 조회
3. candidate state/source/market/theme/strategy/risk/error/safety section 생성
4. `DATA_WAIT`, `STALE`, `CLOSED`, strategy non-match, risk block, market/theme gap, projection/evaluation error 분류
5. `run_ai=true`일 때만 valid AI insight 연결
6. report와 section 저장

## Root-cause Category

- `OBSERVE_ONLY_PIPELINE`
- `NO_ORDER_PATH_BY_DESIGN`
- `DATA_QUALITY`
- `THEME_CONTEXT`
- `CANDIDATE_CONTEXT`
- `STRATEGY_CONTEXT`
- `RISK_CONTEXT`
- `GATEWAY_TRANSPORT`
- `MARKET_DATA_PROJECTION`
- `THEME_PROJECTION`
- `CANDIDATE_PROJECTION`
- `STRATEGY_EVALUATION`
- `RISK_EVALUATION`
- `AI_EXECUTION`
- `UNKNOWN`

## Storage

- `ai_rca_reports`
- `ai_rca_sections`
- `ai_rca_report_links`
- `ai_rca_report_errors`

Report는 `observe_only=false` 또는 `no_trading_side_effects=false`를 거부한다.

## API

- `GET /api/ai-sidecar/rca/status`
- `POST /api/ai-sidecar/rca/no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidate/{candidate_instance_id}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidates/batch`
- `GET /api/ai-sidecar/rca/reports`
- `GET /api/ai-sidecar/rca/reports/{report_id}`
- `GET /api/ai-sidecar/rca/errors`

POST endpoint는 report creation endpoint다. trading endpoint가 아니다.

## CLI

```powershell
python tools/build_no_trade_rca.py --trade-date 2026-06-27
python tools/build_candidate_block_rca.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/build_candidate_block_rca_batch.py --trade-date 2026-06-27 --limit 20
```

`--run-ai`는 명시적 manual AI run이 필요할 때만 붙인다.

## Dashboard Integration

Dashboard는 latest RCA reports, RCA errors, AI Explanation Cards를 read-only로 표시한다. Dashboard에는 RCA 생성 버튼, AI 실행 버튼, RCA POST 호출이 없다.

## 운영자 체크포인트

- RCA report는 원인 분석 문서다.
- RCA 결과로 주문하지 않는다.
- `run_ai=false`가 기본이다.
- AI 실패가 있어도 deterministic report가 남아야 한다.
