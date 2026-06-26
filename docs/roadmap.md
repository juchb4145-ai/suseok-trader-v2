# Roadmap

## Done: PR 0. Bootstrap

- Create the project layout.
- Add Python project configuration, lint/test settings, and environment examples.
- Add FastAPI Core API with `GET /health` and `GET /api/status`.
- Add settings defaults with `OBSERVE` mode and disabled live flags.
- Add SQLite initialization with WAL, `busy_timeout`, `synchronous=NORMAL`, and `app_metadata`.
- Document architecture boundaries, event contract direction, and excluded legacy scope.

## Done: PR 1. Broker-neutral Contract

- Add typed contract models for `GatewayEvent`, `GatewayCommand`, `BrokerPriceTick`,
  `BrokerOrderRequest`, and `BrokerExecutionEvent`.
- Add serialization tests and idempotency field validation.
- Keep contracts free of Kiwoom/PyQt runtime imports.

## Done: PR 2A. Roadmap Reset + AI Sidecar Read-Only Contract

- Reset the delivery roadmap around broker-neutral Core plus later Gateway transport.
- Add the read-only AI Sidecar architecture and safety policy.
- Add AI Sidecar task, schema, and policy contracts without OpenAI API calls.
- Add additive SQLite tables for future prompt, request, insight, and evaluation records.
- Expose read-only AI Sidecar status/task/insight list APIs only.

## Done: PR 2B. Event Store + Gateway Transport Surface

- Add the Core event store and transport-facing Gateway API surface.
- Store inbound Gateway events and command state in SQLite.
- Add command idempotency and duplicate suppression tests.
- Keep the surface broker-neutral and isolated from Kiwoom/PyQt imports.

## Done: PR 3. Mock Gateway + Gateway Adapter Skeleton

- Add a separate 32-bit Gateway package skeleton.
- Keep Kiwoom OpenAPI+ and PyQt dependencies isolated from Core requirements.
- Implement heartbeat and mock event posting before any broker action.

## Next: PR 4. Market Data Service

- Normalize price ticks, condition events, and TR-derived market snapshots.
- Provide read-only market data access to strategy and dashboard surfaces.
- Preserve broker-neutral market contracts.

## PR 5. Theme Membership + Theme Snapshot

- Track theme membership and daily theme snapshots.
- Connect condition and market data observations to theme-level context.
- Keep theme data as observation input only.

## PR 6. Candidate FSM

- Add candidate lifecycle states and deterministic transitions.
- Store candidate state changes through the event store.
- Keep all candidate handling observe-only.

## PR 7. Strategy Engine observe-only

- Add deterministic strategy evaluation in observe-only mode.
- Emit explanations and candidate state updates without order intent creation.
- Keep strategy thresholds controlled by configuration and code review only.

## PR 8. Risk Gate observe-only

- Add deterministic risk evaluation in observe-only mode.
- Record blocked reasons and risk observations.
- Do not enable order routing.

## PR 9. Dashboard V1

- Add read-only operator dashboard views for status, events, candidates, themes, and risk notes.
- Include AI Sidecar insight display areas without triggering AI execution.
- Keep the default operating posture observable and non-trading.

## PR AI-1. LLM Context Builder

- Build bounded, redacted, read-only context packets from Event Store and domain state.
- Enforce max context size and order-context restrictions.
- Do not call OpenAI APIs.

## PR AI-2. OpenAI Client + Structured Outputs

- Add the OpenAI client behind explicit AI Sidecar enablement.
- Use structured outputs and schema validation.
- Store invalid outputs as failures only; never feed outputs into trading automation.

## PR AI-3. No-trade RCA / Candidate Block RCA

- Add Sidecar tasks for no-trade and candidate block explanations.
- Surface results in reports and operator review.
- Keep results out of Strategy, Risk, and OMS automatic decisions.

## PR AI-4. Dashboard AI Explanation Cards

- Display validated AI insights in Dashboard cards.
- Include invalid/error/timeout states.
- Avoid any execution controls in AI cards.

## PR AI-5. Codex Prompt Generator

- Generate human-copyable Codex prompt drafts from validated context.
- Do not add automatic code changes, branch creation, commits, pushes, or PR creation.
- Keep prompts reviewable by the operator.

## PR 10. OMS + DRY_RUN

- Add order management contracts and DRY_RUN-only flow.
- Require deterministic Strategy and Risk decisions before any order intent can be created.
- Keep live flags disabled by default.

## PR 11. Exit Engine

- Add deterministic exit evaluation and DRY_RUN exit order handling.
- Record exit rationale and risk checks.
- Keep broker execution paths gated.

## PR 12. LIVE_SIM Enablement

- Enable LIVE_SIM only behind explicit configuration, risk gates, and acceptance tests.
- Keep LIVE_REAL disabled.
- Verify Gateway isolation and operator observability.

## PR AI-6. LIVE_SIM Review Sidecar

- Add read-only Sidecar review reports for LIVE_SIM sessions.
- Summarize behavior, incidents, no-trade causes, and trade reviews.
- Preserve the rule that Sidecar output never drives automated trading decisions.
