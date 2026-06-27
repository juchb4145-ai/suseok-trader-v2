# OMS DRY_RUN

PR10 adds an internal Order Management Dry Run layer. It is a simulation ledger, not a broker
order path.

## Purpose

- Record a manually requested internal `DryRunIntent` when Strategy, Risk, Candidate, Market Data,
  duplicate, limit, configuration, and safety checks all pass.
- Convert an intent to an internal `DryRunOrder` only by explicit API or CLI call.
- Simulate fills and paper positions only when `DRY_RUN_SIMULATED_FILL_ENABLED=true`.
- From PR11, reduce or close existing paper positions only through DRY_RUN Exit Engine simulated
  exit fills.
- Keep Strategy, Risk, Candidate, Gateway, AI Sidecar, RCA, and Codex prompt artifacts isolated
  from automated trading side effects.

## DRY_RUN-only Principles

- `DRY_RUN_OMS_ENABLED=false` by default.
- `DRY_RUN_INTENT_CREATION_ENABLED=false` by default.
- `DRY_RUN_SIMULATED_FILL_ENABLED=false` by default.
- `DRY_RUN_ORDER_ROUTING_ENABLED` and `DRY_RUN_GATEWAY_COMMAND_ENABLED` must remain false.
- `DRY_RUN_ALLOW_SHORT` must remain false.
- DRY_RUN records carry `dry_run_only=true`, `live_order_allowed=false`,
  `gateway_command_allowed=false`, and `broker_order_sent=false`.
- PR10 does not implement LIVE_SIM, LIVE_REAL, broker order submission, broker cancellation, broker
  modification, account balance, holdings, or broker execution mutation.

## Safety Gate

`check_pr10_safety_gate(connection, settings)` blocks intent creation unless:

- live flags and live trading mode are disabled;
- DRY_RUN routing and gateway command switches are disabled;
- AI Sidecar tools and order tools are disabled;
- Dashboard order controls are unavailable;
- PR AI-5 safety-review draft exists, or the test-only bypass setting is enabled.

Failures are recorded in `dry_run_intent_rejections` when intent creation is attempted.

## Eligibility Rule

An internal intent can be created only when all required checks pass:

- latest Strategy observation exists and is `MATCHED_OBSERVATION`;
- latest Risk observation exists and is `OBSERVE_PASS`;
- Candidate state is `CONTEXT_READY`;
- latest market tick exists and is not stale;
- no active DRY_RUN position for the same code;
- no recent active DRY_RUN intent or order for the same code;
- daily intent and active position limits are below configured caps;
- DRY_RUN OMS and intent creation are explicitly enabled;
- safety gate passes.

Passing eligibility still does not mean live readiness. It creates only an internal simulation
record.

## Lifecycle

1. `evaluate_dry_run_eligibility` stores a `dry_run_eligibility_checks` row.
2. `create_dry_run_intent` stores a `dry_run_intents` row with status `CREATED`, or a rejection row.
3. `convert_intent_to_dry_run_order` stores a `dry_run_orders` row and converts the intent status.
4. `simulate_fill_dry_run_order` stores `dry_run_executions`, updates the order to
   `SIMULATED_FILLED`, and opens or updates a `dry_run_positions` paper position.
5. `update_dry_run_positions_mark_to_market` updates paper PnL from latest ticks.
6. `dry_run_ledger` records `INTENT_CREATED`, `ORDER_CREATED`, `SIMULATED_FILL`,
   `POSITION_OPENED` or `POSITION_UPDATED`, and optional `MARK_TO_MARKET` events.

## PR11 Exit Engine Link

PR11 adds `ExitEngineDryRun` for existing `dry_run_positions`. It evaluates deterministic exit
conditions and can create internal `DryRunExitIntent`, `DryRunExitOrder`, and
`DryRunExitExecution` rows only through explicit local-token API or CLI calls. The exit path is
close-only and sell-side in name, but it remains a DRY_RUN simulation:

- no broker sell request is built;
- no Gateway command is created;
- no LIVE_SIM or LIVE_REAL route is enabled;
- no actual account, balance, or holding lookup is performed;
- Strategy, Risk, Candidate, AI Sidecar, RCA, and Codex prompt rows are not mutated or consumed as
  automatic order input.

When `simulate_fill_dry_run_exit_order` runs with `DRY_RUN_EXIT_SIMULATED_FILL_ENABLED=true`, it
calculates realized PnL as `(fill_price - avg_price) * quantity - commission - tax`, inserts a
`dry_run_exit_executions` row, marks the exit order `SIMULATED_FILLED`, and reduces the paper
position quantity. If remaining quantity reaches zero, the position becomes `CLOSED` and
`closed_at` is set. Ledger events include `EXIT_EVALUATION`, `EXIT_INTENT_CREATED`,
`EXIT_ORDER_CREATED`, `SIMULATED_EXIT_FILL`, and `POSITION_CLOSED` or `POSITION_REDUCED`.

## DB Tables

- `dry_run_intents`
- `dry_run_orders`
- `dry_run_executions`
- `dry_run_positions`
- `dry_run_ledger`
- `dry_run_eligibility_checks`
- `dry_run_intent_rejections`
- `dry_run_runs`
- `dry_run_errors`

## API

Read-only:

- `GET /api/dry-run/status`
- `GET /api/dry-run/eligibility`
- `GET /api/dry-run/intents`
- `GET /api/dry-run/intents/{dry_run_intent_id}`
- `GET /api/dry-run/orders`
- `GET /api/dry-run/orders/{dry_run_order_id}`
- `GET /api/dry-run/executions`
- `GET /api/dry-run/positions`
- `GET /api/dry-run/ledger`
- `GET /api/dry-run/errors`

Manual token-protected simulation endpoints:

- `POST /api/dry-run/evaluate`
- `POST /api/dry-run/intents/from-candidate/{candidate_instance_id}`
- `POST /api/dry-run/orders/from-intent/{dry_run_intent_id}`
- `POST /api/dry-run/orders/{dry_run_order_id}/simulate-fill`
- `POST /api/dry-run/positions/mark-to-market`

There is still no `POST /api/orders/enqueue`.

## CLI

```powershell
python tools/evaluate_dry_run_eligibility.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_intent.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_order.py --dry-run-intent-id dry_run_intent_x
python tools/simulate_dry_run_fill.py --dry-run-order-id dry_run_order_x
python tools/mark_to_market_dry_run_positions.py
```

The CLI tools never call a broker and never create Gateway command rows.

## Dashboard Policy

Dashboard snapshot includes a `dry_run` section with status, counts, recent intents, recent orders,
positions, and warnings. The browser UI displays those rows read-only. It does not contain intent,
order, fill, mark-to-market, buy, sell, cancel, or modify buttons and does not POST to dry-run
endpoints.

## Forbidden Scope

PR10 does not add Kiwoom OpenAPI+ order code, PyQt5/QAxWidget imports, 32-bit ActiveX order code,
LIVE_SIM, LIVE_REAL, broker order submission, broker cancellation, broker modification, account
balance, holdings, broker-driven position mutation, AI-output-driven order creation,
RCA-driven order creation, Codex-prompt-driven order creation, public order enqueue APIs, or
background automatic order workers.

## PR11 And PR12

PR11 is now the deterministic DRY_RUN-only Exit Engine described in
`docs/exit_engine_dry_run.md`. PR12 LIVE_SIM is now a separate safety-gated simulation-account-only
path described in `docs/live_sim_enablement.md`.

DRY_RUN evidence can be required before LIVE_SIM intent creation, but DRY_RUN never automatically
creates or promotes a LIVE_SIM intent/order. DRY_RUN still does not create `GatewayCommand` rows and
does not transmit broker orders.
