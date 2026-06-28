# AI Candidate Scorer Advisory

## 범위

PR-6 AI Candidate Scorer는 PR-2~PR-5 rule-based 흐름 위에 붙는 advisory-only 계층이다.

입력은 `OrderPlanDraft`, ThemeLeadership, EntryTiming, Strategy, Risk, LIVE_SIM eligibility/evidence, 최근 LIVE_SIM/DRY_RUN 성과 요약이다. 출력은 후보별 score, confidence, analysis, flags, avoid reason, clamped risk/reward suggestion, summary다.

AI는 주문을 만들지 않는다. AI score/confidence는 주문 승인, 리스크 승인, 포지션 sizing, 수익 확률이 아니다. 좋은 후보가 없으면 `selected=[]`가 정상이다.

## 절대 금지

- `OrderIntent`, `GatewayCommand`, `LiveSimIntent`, `LiveSimOrderRecord` 생성
- `send_order`, `cancel_order`, `modify_order` 호출
- kill switch, 계좌, 한도, 수량, Strategy/Risk threshold 변경
- `order_plan_drafts_latest.status` 변경 또는 rejected plan 승격
- AI score만으로 `PLAN_READY` 또는 주문 가능 상태 생성
- AI score가 낮다는 이유로 기존 rule-based pipeline hard block
- AI 실패를 LIVE_SIM/DRY_RUN pipeline 장애로 전파
- scheduler, daemon, while-loop 추가
- dashboard buy/sell/cancel 버튼 추가
- AI raw text를 schema 없이 저장하거나 `eval`/`exec` 실행

## 설정

기본값은 모두 safe/off다.

```env
AI_CANDIDATE_SCORER_ENABLED=false
AI_CANDIDATE_SCORER_PROVIDER=mock
AI_CANDIDATE_SCORER_MODEL=
AI_CANDIDATE_SCORER_TIMEOUT_SECONDS=10
AI_CANDIDATE_SCORER_MAX_CANDIDATES=10
AI_CANDIDATE_SCORER_MIN_SCORE=70
AI_CANDIDATE_SCORER_MIN_CONFIDENCE=60
AI_CANDIDATE_SCORER_STORE_RAW_RESPONSE=false
AI_CANDIDATE_SCORER_REQUIRE_STRICT_JSON=true
AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS=false
AI_CANDIDATE_SCORER_FAIL_OPEN=true
AI_CANDIDATE_SCORER_ATTACH_TO_ORDER_PLAN=false
AI_CANDIDATE_SCORER_ATTACH_TO_LIVE_SIM_RUN=false
AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS=12000
AI_CANDIDATE_SCORER_REDACT_ACCOUNT_ID=true
AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MIN=1.0
AI_CANDIDATE_RISK_REWARD_STOP_LOSS_MAX=4.0
AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MIN=2.0
AI_CANDIDATE_RISK_REWARD_TAKE_PROFIT_MAX=8.0
AI_CANDIDATE_RISK_REWARD_TRAILING_MIN=1.0
AI_CANDIDATE_RISK_REWARD_TRAILING_MAX=4.0
AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MIN_SEC=300
AI_CANDIDATE_RISK_REWARD_MAX_HOLD_MAX_SEC=7200
```

`provider=mock`은 외부 네트워크를 쓰지 않는 deterministic local scorer다. 외부 LLM provider는 interface skeleton만 있으며 기본 테스트/운영에서는 호출되지 않는다.

## Context Builder

후보는 `order_plan_drafts_latest`에서 `PLAN_READY`를 우선하고, `WAIT_RETRY`, `DATA_WAIT` near-miss를 일부 포함한다. 기본 최대 후보 수는 10개다.

후보 context에는 다음이 포함된다.

- trade_date, candidate_instance_id, order_plan_id, code, name
- theme_id, theme_name, theme_state, theme_rank, stock_role, priority_score
- setup_type, entry_timing_state, price_location_state
- current_price, limit_price, change_rate_pct, turnover_krw, execution_strength
- momentum_1m/3m/5m, vwap, pullback_from_high_pct, spread_ticks
- strategy_status, strategy_score, strategy_confidence
- risk_status, risk_reason_codes
- live_sim_eligibility_status, live_sim_rejection_reason_codes
- order_plan_status, order_plan_reason_codes
- stale, vi_active, upper_limit_near
- recent_trade_performance, lifecycle_warnings

계좌 식별자와 raw Gateway payload는 prompt/context에서 제거한다. prompt는 `AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS`로 제한한다.

## Prompt Builder

prompt는 system instruction, forbidden action list, market/theme/risk/performance summary, candidate JSON, required schema로 구성한다.

필수 안전 문구:

- 너는 주문을 만들 수 없다.
- 너는 buy/sell/cancel/modify를 지시할 수 없다.
- 너는 후보 평가와 risk/reward 조언만 한다.
- 좋은 후보가 없으면 selected를 빈 배열로 둔다.
- safety gate, 계좌 한도, kill switch, LIVE_SIM 여부는 코드가 판단한다.
- JSON schema만 출력한다.

## JSON Schema와 검증

필수 root field:

- `selected`
- `analysis`
- `score`
- `confidence`
- `risk_reward`
- `candidate_flags`
- `avoid`
- `summary`
- `no_trade_reason`

검증 규칙:

- root unknown field는 invalid schema 처리한다.
- `selected`는 후보 목록에 있는 code만 허용한다.
- unknown code는 score/analysis/risk_reward/flags/avoid에서 제거하고 warning으로 남긴다.
- score/confidence는 0~100 범위를 벗어나면 invalid schema다.
- JSON 외 텍스트는 JSON object 추출을 시도하고, 실패하면 invalid schema다.
- `BUY_NOW`, `SEND_ORDER`, `CANCEL_ORDER`, `MODIFY_ORDER`, `OrderIntent`, `GatewayCommand`, `KILL_SWITCH`, 계좌/수량 변경성 토큰은 invalid schema다.
- summary/analysis/avoid/flag는 길이 제한을 적용한다.

schema 실패는 `ai_candidate_scoring_runs.status=INVALID_SCHEMA`와 `ai_advisory_errors`로 저장되며 trading pipeline을 중단하지 않는다.

## Risk/Reward Clamp

AI risk/reward는 자동 적용값이 아니라 clamped suggestion이다.

- stop_loss_pct: 1.0~4.0
- take_profit_pct: 2.0~8.0
- trailing_stop_pct: 1.0~4.0
- max_hold_sec: 300~7200

범위를 벗어나면 clamp 후 `clamped=true`, `clamp_reason_codes`를 저장한다.

## 저장 테이블

- `ai_candidate_scoring_runs`
- `ai_candidate_scores`
- `ai_risk_reward_suggestions`
- `ai_advisory_errors`

raw response 저장은 기본 false다. 저장하더라도 민감 정보를 제거한다. advisory 저장은 주문 관련 테이블을 변경하지 않는다.

## API

Read-only:

- `GET /api/ai-advisory/status`
- `GET /api/ai-advisory/candidate-scores/latest`
- `GET /api/ai-advisory/runs`
- `GET /api/ai-advisory/runs/{run_id}`
- `GET /api/ai-advisory/errors`

Local-token protected:

- `POST /api/ai-advisory/score-candidates?trade_date=YYYY-MM-DD&limit=10`

POST는 scoring run만 생성한다. 모든 응답은 `advisory_only=true`, `no_order_side_effects=true`를 포함한다.

## CLI

```powershell
python -m tools.score_ai_candidates --dry-run
python -m tools.score_ai_candidates --trade-date 2026-06-27 --limit 10
python -m tools.inspect_ai_candidate_scores --latest
python -m tools.inspect_ai_candidate_scores --run-id AI-RUN-...
```

`--dry-run`은 DB 저장 없이 context/prompt/validation preview를 출력한다. scoring disabled면 `DISABLED`를 명확히 출력한다.

## Dashboard

Dashboard에는 read-only 요약만 추가한다.

- AI scoring enabled/disabled
- latest run status
- selected count
- top AI scored candidates
- score/confidence
- AI summary
- invalid schema/error count
- advisory-only/no-side-effect badge

AI 추천 매수/매도/취소 버튼과 설정 변경 UI는 없다.

## Rollback

```powershell
$env:AI_CANDIDATE_SCORER_ENABLED = "false"
$env:AI_CANDIDATE_SCORER_PROVIDER = "mock"
$env:AI_CANDIDATE_SCORER_ATTACH_TO_ORDER_PLAN = "false"
$env:AI_CANDIDATE_SCORER_ATTACH_TO_LIVE_SIM_RUN = "false"
```

Rollback은 AI advisory 생성을 멈추는 것이며 기존 rule-based OrderPlan/LIVE_SIM pipeline은 계속 독립적으로 동작한다.

## 다음 PR 검토 항목

- No-Buy Sentinel에 AI 관망 reason을 시스템 무매수 reason과 분리해 표시
- 운영 대시보드에서 AI advisory drill-down 강화
- AI risk_reward를 Exit policy에 선택 적용할지 별도 safety review
- 외부 LLM adapter를 붙일 경우 민감 정보 redact와 timeout/error fail-open 재검증
