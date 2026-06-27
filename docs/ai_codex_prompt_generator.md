# AI Codex Prompt Generator

## 요약

PR AI-5는 사람이 복사할 수 있는 Codex prompt draft를 생성한다. draft는 RCA report, candidate context, no-trade context, ops incident, AI context packet, AI insight, manual safety-review note에서 만들 수 있다. Generator는 Codex를 호출하지 않고, branch/commit/push/PR을 만들지 않고, repository file을 수정하지 않는다.

## Deterministic vs AI-assisted Draft

| mode | 설명 |
| --- | --- |
| deterministic | 기본값. OpenAI API key 없이 동작 |
| AI-assisted | `--run-ai` 또는 `run_ai=true`가 명시된 경우에만 PR AI-2 `CODEX_PROMPT_DRAFT` task 사용 |

AI가 disabled, unavailable, invalid, policy rejected이면 deterministic draft가 그대로 저장된다.

## Source Types

- `RCA_REPORT`
- `CANDIDATE`
- `NO_TRADE`
- `OPS_INCIDENT`
- `AI_CONTEXT_PACKET`
- `AI_INSIGHT`
- `MANUAL_NOTE`

## Target Area Inference

Generator는 root-cause category와 reason code를 다음 target area로 매핑한다.

- `GATEWAY_TRANSPORT`
- `MARKET_DATA`
- `THEME_SERVICE`
- `CANDIDATE_FSM`
- `STRATEGY_ENGINE`
- `RISK_GATE`
- `DASHBOARD`
- `AI_SIDECAR`
- `RCA_WORKFLOW`
- `TESTING`
- `DOCS`
- `SAFETY_REVIEW`
- `UNKNOWN`

## Prompt Format

`prompt_text`는 한국어 section heading을 사용한다.

1. 역할
2. 대상 저장소
3. 현재 상태
4. 문제/관찰 요약
5. 관련 evidence
6. 작업 목표
7. 구현 범위
8. 명시적 금지
9. 테스트 요구사항
10. 완료 기준
11. 작업 후 보고 형식

Prompt draft에는 다음 safety text가 포함되어야 한다.

- 자동 주문/매수/매도 기능을 추가하지 말 것
- `OrderIntent`, `GatewayCommand`, `send_order`, `cancel_order`, `modify_order`를 만들지 말 것
- Strategy/Risk/OMS 자동 판단으로 AI/RCA output을 사용하지 말 것
- GitHub branch/commit/push/PR을 자동 생성하지 말 것
- draft generator는 파일을 수정하지 않는다

## Safety Sanitizer

Sanitizer는 forbidden action name을 명시적 부정 또는 review-only context에서만 허용한다. 다음 instruction은 reject한다.

- order execution 지시
- live-flag 변경 지시
- Codex execution 지시
- GitHub branch/commit/push/PR 생성 지시
- automatic apply behavior

Stored draft는 다음 flag를 유지한다.

- `observe_only=true`
- `human_review_required=true`
- `auto_apply_allowed=false`
- `github_write_allowed=false`
- `codex_execution_allowed=false`
- `no_trading_side_effects=true`

## Storage

- `ai_codex_prompt_drafts`
- `ai_codex_prompt_sections`
- `ai_codex_prompt_links`
- `ai_codex_prompt_errors`

## API

- `GET /api/ai-sidecar/codex-prompts/status`
- `POST /api/ai-sidecar/codex-prompts/from-rca/{report_id}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-candidate/{candidate_instance_id}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-ops-incident`
- `POST /api/ai-sidecar/codex-prompts/safety-review`
- `GET /api/ai-sidecar/codex-prompts`
- `GET /api/ai-sidecar/codex-prompts/{draft_id}`
- `GET /api/ai-sidecar/codex-prompts/{draft_id}/text`
- `GET /api/ai-sidecar/codex-prompts/errors`

POST endpoint는 draft artifact 생성용이다. Codex execution API나 GitHub write API가 아니다.

## CLI

```powershell
python tools/build_codex_prompt_from_rca.py --report-id ai_rca_report_x
python tools/build_codex_prompt_from_candidate.py --candidate-instance-id candidate-1
python tools/build_codex_prompt_from_no_trade.py --trade-date 2026-06-27
python tools/build_codex_safety_review_prompt.py
python tools/inspect_codex_prompt.py --draft-id ai_codex_prompt_x --text
```

CLI tool은 branch, commit, push, PR, code patch를 만들지 않는다.

## Dashboard Display

Dashboard는 prompt draft count, latest draft, latest errors, safety flags를 read-only로 표시한다. clipboard copy button은 허용된다. Codex 실행, GitHub write, branch, commit, push, PR, automatic apply, order control은 없다.

## PR10 Safety Review Prompt

`tools/build_codex_safety_review_prompt.py`는 PR10 OMS + DRY_RUN safety-review prompt를 만든다. 이 draft는 사람이 검토하는 문서이며 DRY_RUN intent, order, simulated fill, Candidate/Strategy/Risk mutation을 만들지 않는다.

## 운영자 체크포인트

- Codex prompt draft는 사람이 복사하는 초안이다.
- 자동 branch/commit/push/PR 생성이 아니다.
- prompt draft는 주문/전략 자동 입력이 아니다.
- draft text가 존재해도 OMS signal로 사용하지 않는다.
