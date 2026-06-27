# AI Sidecar Safety Policy

## Default Posture

The AI Sidecar is disabled by default. The Core API must operate normally without an
OpenAI API key, SDK initialization, or model call. PR AI-1 adds read-only context preview
endpoints, but still has no model execution path.

## Forbidden Actions

The Sidecar must not:

- Create `OrderIntent`.
- Create `GatewayCommand`.
- Call or connect to `send_order`, `cancel_order`, or `modify_order`.
- Change trading mode.
- Change live flags.
- Change strategy thresholds.
- Change risk limits.
- Change position sizes.
- Enqueue orders or create tool-like trading commands.
- Feed Sidecar output into Strategy, Risk, or OMS automatic decisions.

These restrictions apply in `OBSERVE`, `LIVE_SIM`, and `LIVE_REAL`. Even if later PRs
enable Sidecar execution or `LIVE_SIM`, Sidecar output remains read-only.

## Allowed Actions

The Sidecar may produce validated read-only output for:

- Pre-market and post-market summaries.
- Theme briefs.
- Candidate block explanations.
- No-trade RCA.
- Trade review.
- Operations incident summaries.
- Codex prompt drafts for human copy/paste.

Allowed output is limited to Dashboard, Report, Operator Review, and Codex Prompt Draft
surfaces.

PR AI-1 additionally allows bounded, redacted context packet previews for the same task list.
Those packets are input material for future human-reviewed model execution, not trading signals
and not strategy/risk/OMS automatic input.

## Schema Validation

Every Sidecar output must match the schema for its task type. Output schemas must not
include executable fields such as `order_intent`, `gateway_command`, `send_order`,
`cancel_order`, or `modify_order`.

If validation fails, the system must not store a normal insight. It may reject the output
or record the request with a failure status such as `AI_OUTPUT_INVALID`.

The policy distinguishes natural-language safety discussion from executable action
fields. For example, a summary may mention that `send_order` is forbidden, but a tool-like
field such as `tool`, `function_name`, `command`, `action`, or `order_intent` must not
contain or represent a trading action.

## Timeout And Error Handling

Future model calls must use bounded request timeouts and must degrade to a non-trading
failure state. Timeouts, unavailable API keys, model errors, schema errors, and policy
errors must not block the Core API from serving health/status endpoints and must not
create trading side effects.

## API Key Handling

An OpenAI API key is optional until a later explicit integration PR. Missing keys must
leave `openai_client_available=false` and must not fail app startup or tests.
