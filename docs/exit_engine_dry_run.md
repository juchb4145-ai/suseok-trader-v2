# DRY_RUN Exit Engine

PR11 adds deterministic exit evaluation and simulated close accounting for existing
`dry_run_positions`. It is not a broker sell path.

## Purpose

- Evaluate active DRY_RUN positions with deterministic exit rules.
- Persist `DryRunExitEvaluation` and `DryRunExitSignal` rows for operator review.
- Allow internal `DryRunExitIntent`, `DryRunExitOrder`, and `DryRunExitExecution` creation only by
  explicit local-token API or CLI calls.
- Reduce or close DRY_RUN positions through simulated exit fills only.
- Keep PR12 LIVE_SIM as a separate future project.

## DRY_RUN-only Principles

- `DRY_RUN_EXIT_ENGINE_ENABLED=false` by default.
- `DRY_RUN_EXIT_INTENT_CREATION_ENABLED=false` by default.
- `DRY_RUN_EXIT_ORDER_CREATION_ENABLED=false` by default.
- `DRY_RUN_EXIT_SIMULATED_FILL_ENABLED=false` by default.
- `DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=false` and
  `DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=false` are enforced by config validation.
- `DRY_RUN_EXIT_ALLOW_SHORT=false` and `DRY_RUN_EXIT_ALLOW_SELL_CLOSE_ONLY=true` are enforced.
- Exit records carry `dry_run_only=true`, `close_only=true`, `live_order_allowed=false`,
  `gateway_command_allowed=false`, and `broker_order_sent=false`.

## Exit Rules

- `STOP_LOSS`: latest price <= average price * `(1 - stop_loss_pct)`.
- `TAKE_PROFIT`: latest price >= average price * `(1 + take_profit_pct)`.
- `TRAILING_STOP`: latest price has drawn down from the position high watermark by at least
  `trailing_stop_pct`.
- `MAX_HOLD`: position age is at least `max_hold_sec`.
- `DATA_STALE_EXIT_CAUTION`: latest tick age exceeds `stale_tick_sec`.
- `THEME_WEAKENING`: related latest theme is `DATA_WAIT`, `WATCH`, `FADING`, `ROTATED_OUT`, or its
  fresh coverage/rising ratio weakens below configured thresholds.
- `RISK_DETERIORATION`: latest risk observation for code/trade date is `OBSERVE_BLOCK` or
  `OBSERVE_CAUTION`.
- `STRATEGY_INVALIDATED`: latest strategy observation for code/trade date is `DATA_WAIT`,
  `NO_SETUP`, `STALE_CONTEXT`, or `INVALID_CONTEXT`.
- `MANUAL_REVIEW`: placeholder reason for operator review; it does not auto-fill.

`STOP_LOSS`, `TAKE_PROFIT`, `TRAILING_STOP`, and `MAX_HOLD` are simulated exit signals only. They do
not approve or send live sell orders.

## Lifecycle

1. `evaluate_dry_run_exit_for_position` reads one active DRY_RUN position, latest tick, optional
   position metrics, latest theme/strategy/risk context, and stores one evaluation plus signal rows.
2. `evaluate_all_dry_run_exits` evaluates active positions and records `dry_run_exit_runs` plus
   per-position errors.
3. `create_dry_run_exit_intent` requires engine and intent flags, PR11 safety gate, an active
   closeable position, and an `EXIT_SIGNAL_OBSERVED` evaluation. It creates an internal intent only.
4. `convert_exit_intent_to_dry_run_order` requires order creation enablement and creates an
   internal exit order only.
5. `simulate_fill_dry_run_exit_order` requires simulated fill enablement, uses the latest tick as
   fill price, inserts a simulated execution, marks the order filled, and reduces or closes the
   paper position.

There is no automatic background exit worker.

## Safety Gate

The PR11 exit safety gate extends the PR10 safety gate and confirms:

- trading mode is `OBSERVE`;
- live flags are disabled;
- exit order routing is disabled;
- exit gateway command creation is disabled;
- sell close-only is true;
- short selling is disabled;
- broker order sent stays false;
- AI Sidecar/RCA/Codex outputs are not used as automatic exit inputs.

Failures block manual intent/order/fill creation and are recorded in `dry_run_exit_errors` or as a
rejected exit intent.

## DB Tables

- `dry_run_exit_evaluations`
- `dry_run_exit_signals`
- `dry_run_exit_intents`
- `dry_run_exit_orders`
- `dry_run_exit_executions`
- `dry_run_exit_runs`
- `dry_run_exit_errors`
- `dry_run_position_metrics`

The existing `dry_run_ledger` receives `EXIT_EVALUATION`, `EXIT_INTENT_CREATED`,
`EXIT_ORDER_CREATED`, `SIMULATED_EXIT_FILL`, `POSITION_CLOSED`, and `POSITION_REDUCED` events.

## API

Read-only:

- `GET /api/dry-run/exits/status`
- `GET /api/dry-run/exits/evaluations`
- `GET /api/dry-run/exits/evaluations/{exit_evaluation_id}`
- `GET /api/dry-run/exits/signals`
- `GET /api/dry-run/exits/intents`
- `GET /api/dry-run/exits/intents/{dry_run_exit_intent_id}`
- `GET /api/dry-run/exits/orders`
- `GET /api/dry-run/exits/orders/{dry_run_exit_order_id}`
- `GET /api/dry-run/exits/executions`
- `GET /api/dry-run/exits/runs`
- `GET /api/dry-run/exits/errors`

Manual local-token protected simulation endpoints:

- `POST /api/dry-run/exits/evaluate`
- `POST /api/dry-run/exits/intents/from-position/{dry_run_position_id}`
- `POST /api/dry-run/exits/orders/from-intent/{dry_run_exit_intent_id}`
- `POST /api/dry-run/exits/orders/{dry_run_exit_order_id}/simulate-fill`

Every POST remains simulation-only and returns safe flags.

## CLI

```powershell
python tools/evaluate_dry_run_exits.py --dry-run-position-id dry_run_position_x
python tools/evaluate_dry_run_exits.py --trade-date 2026-06-27 --limit 50
python tools/create_dry_run_exit_intent.py --dry-run-position-id dry_run_position_x
python tools/create_dry_run_exit_order.py --dry-run-exit-intent-id dry_run_exit_intent_x
python tools/simulate_dry_run_exit_fill.py --dry-run-exit-order-id dry_run_exit_order_x
python tools/inspect_dry_run_exit.py --exit-evaluation-id dry_run_exit_eval_x
```

The CLI never sends broker orders and never creates Gateway commands.

## Dashboard Policy

Dashboard V1 shows a read-only DRY_RUN Exit panel under the DRY_RUN section. It displays status
counts, recent evaluations, and recent signals. It has no sell, close, fill, cancel, modify, or
exit execution buttons and does not call DRY_RUN Exit POST endpoints.

## PR12 Boundary

PR11 is a prerequisite for simulated exit accounting only. PR12 LIVE_SIM adds a separate
simulation-account-only safety gate and command queue path for small buy-side acceptance tests.
PR12 does not convert DRY_RUN exits to broker exits by default. LIVE_SIM sell and exit-sell remain
disabled unless a later safety review explicitly enables them. LIVE_REAL remains disabled.

## Forbidden Scope

- Kiwoom OpenAPI+ actual order code
- PyQt5/QAxWidget imports
- 32-bit ActiveX order code
- `GatewayCommand` creation or transmission
- `BrokerOrderRequest` transmission
- DRY_RUN `send_order`, `cancel_order`, or `modify_order`
- `POST /api/orders/enqueue`
- LIVE_REAL implementation
- automatic DRY_RUN-to-LIVE_SIM exit promotion
- real account, balance, holding, or broker execution mutation
- AI/RCA/Codex-output-driven exit/order intent creation
- Dashboard exit action buttons
- background automatic exit workers
