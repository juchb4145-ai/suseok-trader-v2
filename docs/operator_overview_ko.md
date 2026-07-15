# Operator Overview KO

## 요약

이 문서는 운영자가 `suseok-trader-v2` 전체 구조를 한 번에 이해하기 위한 입구다. 시스템은 Gateway에서 들어온 관찰 event를 단계별 projection으로 바꾸고, Dashboard와 review artifact로 보여준다. 기본은 `OBSERVE`이며 실계좌 주문 시스템이 아니다.

## 전체 흐름

```text
Gateway
  -> Event Store
  -> Market Data
  -> Theme
  -> Candidate
  -> Strategy
  -> Risk
  -> Dashboard
  -> AI Candidate Scorer Advisory
  -> AI Sidecar
  -> DRY_RUN OMS
  -> DRY_RUN Exit
  -> LIVE_SIM
```

## 단계별 운영자 관점

| 단계 | 하는 일 | 하지 않는 일 | 운영자가 봐야 할 것 |
| --- | --- | --- | --- |
| Gateway | broker-neutral event를 Core에 전달하고 command를 polling | 전략/리스크 판단, 임의 주문 생성 | `/api/gateway/status`, recent events |
| Event Store | raw/canonical event 저장 | 주문 판단 | duplicate/conflict/unknown event |
| Market Data | tick/latest/bar/VWAP/readiness projection | OMS/주문 생성 | `/api/market-data/status`, readiness |
| Theme | membership과 snapshot 집계 | 투자 추천 | theme state, quality, coverage |
| Candidate | 후보 관찰 에피소드 lifecycle 관리 | 매수 후보 확정 | candidate state, source, context |
| Strategy | setup observation 저장 | entry plan, position size | `MATCHED_OBSERVATION` 여부와 reason |
| Risk | risk observation 저장 | 주문 승인하지 않음 | `OBSERVE_PASS`, caution/block reason |
| Dashboard | pipeline read-only 표시 | 실행 버튼 제공 | safety banner, funnel, errors |
| AI Candidate Scorer | 상위 후보 score/confidence/risk_reward suggestion 저장, 선택적 external LLM advisory | 주문 생성, plan 승격, risk block 생성 | `/api/ai-advisory/status`, latest scores/errors |
| AI Sidecar | context/insight/RCA/review 보조 | 자동 판단 입력 | failed request, validated insight |
| DRY_RUN OMS | 내부 모의 회계 | broker 주문 | dry-run status, paper position |
| DRY_RUN Exit | simulated close accounting | 실제 매도 주문 | exit signal/evaluation |
| LIVE_SIM | 키움 모의투자 전용 safety-gated path, PR-5 execution/cancel/exit/reconcile | 실계좌 주문, modify, 신규 SELL/short, scheduler 반복 | `/api/live-sim/status`, `/api/operator/live-sim/execution-lifecycle/status`, positions, reconcile |

## 주요 상태값

| 상태값 | 운영자 해석 |
| --- | --- |
| `DATA_WAIT` | 데이터가 부족해 다음 관측이 어렵다 |
| `CONTEXT_READY` | Strategy 평가 입력이 준비되었다 |
| `MATCHED_OBSERVATION` | setup classifier 관측 조건이 맞았다 |
| `OBSERVE_PASS` | risk observation에서 block/caution이 관측되지 않았다 |
| `OBSERVE_CAUTION` | 주의할 risk note가 있다 |
| `OBSERVE_BLOCK` | block reason이 관측되었다 |
| `AI_OUTPUT_INVALID` | AI 출력이 schema를 통과하지 못했다 |
| `INVALID_SCHEMA` | AI Candidate Scorer 출력이 strict schema를 통과하지 못했다 |
| `POLICY_REJECTED` | AI output에서 금지된 action shape가 감지되었다 |

`MATCHED_OBSERVATION`은 매수 신호가 아니다. `OBSERVE_PASS`는 주문 승인이 아니다.

## 흔한 문제와 확인 순서

| 증상 | 확인 순서 |
| --- | --- |
| Dashboard가 비어 있음 | Core 실행 -> Gateway status -> Market Data status |
| tick은 있는데 Theme 없음 | membership import -> snapshot rebuild -> projection errors |
| Candidate가 `DATA_WAIT` | latest tick -> 1m bar -> VWAP/readiness -> source context |
| Strategy가 없음 | Candidate state가 `CONTEXT_READY`인지 확인 |
| Risk가 없음 | latest Strategy observation 존재 여부 확인 |
| DRY_RUN intent가 안 생김 | DRY_RUN flags -> safety gate -> eligibility checks |
| LIVE_SIM order가 안 생김 | kill switch -> account mode -> Gateway heartbeat -> rejections |
| LIVE_SIM position/exit가 이상함 | execution-lifecycle qualification -> `/api/live-sim/positions` -> reconcile latest |
| 미체결이 남아 있음 | cancel flags -> broker order no -> `/api/live-sim/cancel-intents` |
| AI Candidate Score가 없음 | `AI_CANDIDATE_SCORER_ENABLED` -> `/api/ai-advisory/status` -> latest errors |
| External LLM이 호출되지 않음 | `AI_CANDIDATE_SCORER_PROVIDER` -> `AI_EXTERNAL_LLM_ENABLED` -> `AI_EXTERNAL_LLM_ALLOW_NETWORK` -> API/CLI `allow_external` |
| AI card가 실패 | API key, model config, timeout, schema/policy rejection 확인 |

## 운영자가 절대 혼동하면 안 되는 것

- Candidate는 매수 후보가 아니라 후보 관찰 에피소드다.
- `CONTEXT_READY`는 매수 준비가 아니다.
- `MATCHED_OBSERVATION`은 매수 신호가 아니다.
- `OBSERVE_PASS`는 주문 승인이 아니다.
- DRY_RUN은 내부 모의 회계이며 브로커 주문이 아니다.
- LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아니다.
- LIVE_SIM cancel은 미체결 BUY 취소 전용이다.
- LIVE_SIM SELL은 open position close-only exit 전용이다.
- LIVE_SIM scheduler는 아직 없고 run_once API/CLI만 있다.
- AI Candidate Scorer score/confidence는 주문 승인, 리스크 승인, 수익 확률이 아니다.
- AI Candidate Scorer risk_reward는 clamp된 제안이며 자동 적용값이 아니다.
- External LLM adapter는 advisory-only이며 기본값에서는 호출되지 않는다.
- External LLM timeout/schema/provider error는 trading pipeline 장애가 아니다.
- AI/RCA/Codex output은 자동 주문 입력이 아니다.
- Dashboard는 읽기 전용이며 실행 버튼이 없다.
- LIVE_REAL은 현재 구현되어 있지 않다.

## 운영자 체크포인트

- 문제를 볼 때 항상 왼쪽 단계에서 오른쪽 단계로 확인한다.
- 가장 먼저 safety banner와 `/api/status`를 본다.
- execution이 필요해 보이는 순간에도 해당 기능이 read-only/review-only인지 먼저 확인한다.
- LIVE_SIM과 LIVE_REAL을 분리해 말하고 기록한다.
