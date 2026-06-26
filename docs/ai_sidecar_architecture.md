# AI Sidecar Architecture

## Purpose

The ChatGPT/OpenAI Sidecar is a read-only analysis assistant for `suseok-trader-v2`.
It helps the operator understand market context, candidate blocks, no-trade sessions,
trade reviews, operational incidents, and Codex prompt drafts.

The Sidecar is not a trading engine. It does not create `OrderIntent`, does not create
`GatewayCommand`, and does not call or connect to `send_order`, `cancel_order`, or
`modify_order`.

## Boundaries

Core owns configuration, API routing, storage initialization, and system status.
Strategy owns deterministic candidate evaluation. Risk owns deterministic risk checks.
OMS owns order lifecycle behavior in later PRs. Gateway owns broker transport isolation.

The Sidecar sits outside those decision paths. It may read bounded, sanitized context
from the Event Store in later PRs, then produce validated explanatory output for human
review surfaces. Strategy, Risk, and OMS must not use Sidecar output as automatic
decision input.

## Read-only Principles

- Sidecar output is for Dashboard, Report, Operator Review, and Codex Prompt Draft
  surfaces only.
- Sidecar output cannot change strategy thresholds, risk limits, trading mode, live
  flags, or position sizes.
- Sidecar output cannot enqueue, submit, cancel, or modify orders.
- Every Sidecar output must pass schema validation before it can be stored as an
  insight.
- Validation failures must not produce normal insights. They may be rejected or
  recorded only with an invalid-output status such as `AI_OUTPUT_INVALID`.
- The default Sidecar posture is disabled.

## Event Store Analysis Shape

Future Sidecar PRs will build context from Event Store records instead of reaching into
live execution components. The intended flow is:

1. Deterministic services write market, candidate, risk, OMS, Gateway, and ops events.
2. A context builder creates a bounded read-only packet for one allowed Sidecar task.
3. The Sidecar validates the requested task and output schema.
4. Valid insights are stored for read-only display.
5. Invalid, timed out, or errored runs are recorded as failures without insight storage.

This PR creates only the domain contracts, storage tables, docs, and read-only list/status
API surface. It does not implement context building or OpenAI calls.

## Session Usage

Pre-market use cases include daily market briefs, theme summaries, and operator review
notes. Intraday use is disabled by default and must remain read-only even when explicitly
allowed later. Post-market use cases include no-trade RCA, candidate block RCA, trade
review, operations incident summaries, and Codex prompt drafts.

## Output Surfaces

Dashboard cards may display validated Sidecar insights and error states. Reports may
include summaries and suggested checks. Operator Review may use insights as human-readable
context. Codex Prompt Draft output may provide text that a human copies into Codex, but
it must not perform automatic code changes, branch creation, commits, pushes, or PR
creation.

## OpenAI Client Timing

OpenAI API integration is intentionally out of scope for PR 2A. A later PR may add an
OpenAI client with structured outputs, explicit enablement, timeout handling, and schema
validation. The system must continue to boot, test, and serve status without an API key.
