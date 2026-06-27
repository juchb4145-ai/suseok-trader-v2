# AI Codex Prompt Generator

## Purpose

PR AI-5 adds a read-only generator for human-copyable Codex prompt drafts. Drafts can be built
from RCA reports, candidate context, no-trade context, ops incident context, AI context packets,
AI insights, or manual safety-review notes.

The generator does not call Codex, does not create branches, commits, pushes, or PRs, and does not
modify repository files. A draft is text for a human operator to review and copy.

## Deterministic vs AI-assisted Draft

Deterministic mode is the default and works without an OpenAI API key:

```powershell
python tools/build_codex_prompt_from_no_trade.py --trade-date 2026-06-27
```

AI-assisted mode is optional. It runs only when `--run-ai` or `run_ai=true` is explicitly passed.
It uses the PR AI-2 `CODEX_PROMPT_DRAFT` structured-output task. If AI is disabled, unavailable,
invalid, or policy rejected, the deterministic draft remains saved and linked to the failure
status.

## Source Types

- `RCA_REPORT`
- `CANDIDATE`
- `NO_TRADE`
- `OPS_INCIDENT`
- `AI_CONTEXT_PACKET`
- `AI_INSIGHT`
- `MANUAL_NOTE`

## Target Area Inference

The generator maps RCA root-cause categories and reason codes to areas such as
`GATEWAY_TRANSPORT`, `MARKET_DATA`, `THEME_SERVICE`, `CANDIDATE_FSM`, `STRATEGY_ENGINE`,
`RISK_GATE`, `DASHBOARD`, `AI_SIDECAR`, `RCA_WORKFLOW`, `TESTING`, `DOCS`, and `SAFETY_REVIEW`.
Unknown evidence falls back to `UNKNOWN`.

## Prompt Format

Every `prompt_text` uses Korean section headings:

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

Required safety text includes:

- 자동 주문/매수/매도 기능을 추가하지 말 것
- OrderIntent, GatewayCommand, send_order/cancel_order/modify_order를 만들지 말 것
- Strategy/Risk/OMS 자동 판단으로 AI/RCA output을 사용하지 말 것
- GitHub branch/commit/push/PR을 자동 생성하지 말 것
- 파일 수정은 Codex가 사용자의 명시적 작업 범위 안에서만 수행하며, 이 draft generator는 파일을 수정하지 않는다
- 테스트와 문서 업데이트를 포함할 것

## Safety Sanitizer

The sanitizer allows forbidden action names only in explicit negation or review-only contexts. It
rejects prompt lines that instruct order execution, live-flag changes, Codex execution, GitHub
branch/commit/push/PR creation, or automatic apply behavior.

Stored drafts must keep:

- `observe_only=true`
- `human_review_required=true`
- `auto_apply_allowed=false`
- `github_write_allowed=false`
- `codex_execution_allowed=false`
- `no_trading_side_effects=true`

## Storage Tables

PR AI-5 adds additive SQLite tables:

- `ai_codex_prompt_drafts`
- `ai_codex_prompt_sections`
- `ai_codex_prompt_links`
- `ai_codex_prompt_errors`

Draft links connect related RCA reports, context packets, AI requests, AI insights, and related
entities when present.

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

POST endpoints create draft artifacts only. They are not Codex execution APIs and are not GitHub
write APIs.

## CLI

```powershell
python tools/build_codex_prompt_from_rca.py --report-id ai_rca_report_x
python tools/build_codex_prompt_from_candidate.py --candidate-instance-id candidate-1
python tools/build_codex_prompt_from_no_trade.py --trade-date 2026-06-27
python tools/build_codex_safety_review_prompt.py
python tools/inspect_codex_prompt.py --draft-id ai_codex_prompt_x --text
```

CLI tools do not create branches, commits, pushes, PRs, or code patches.

## Dashboard Display / Copy-only Policy

Dashboard snapshot includes Codex prompt draft counts, latest drafts, latest errors, and safety
flags. AI explanation cards include `CODEX_PROMPT_DRAFT` cards with read-only text areas.

The browser clipboard copy button is allowed. The Dashboard JavaScript does not POST to Codex
prompt endpoints and does not expose Codex execution, GitHub write, branch, commit, push, PR,
automatic apply, or order controls.

## PR10 Safety Review Prompt

`tools/build_codex_safety_review_prompt.py` creates a PR10 OMS + DRY_RUN safety-review prompt. It
asks Codex to confirm that Sidecar/RCA/Codex prompt artifacts remain review-only before any
OrderIntent/OMS path is introduced.

After PR10, Codex prompt artifacts remain review-only. The PR10 safety gate can require that a
safety-review draft exists, but it does not use draft text as an OMS signal. Codex prompt drafts do
not create DRY_RUN intents, do not convert orders, do not simulate fills, and do not mutate
Candidate, Strategy, or Risk rows.

## Forbidden Scope

PR AI-5 does not implement OMS, OrderIntent, EntryPlan, PositionSizing,
send_order/cancel_order/modify_order, `POST /api/orders/enqueue`, GatewayCommand creation,
OpenAI tools/function calling, web search, code interpreter, MCP tool servers, order tools,
background workers, AI/Codex-prompt-driven candidate/strategy/risk mutation, live flag mutation,
or automated trading decisions.
