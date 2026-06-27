# LIVE_SIM Enablement

PR12 enables a narrow Kiwoom mock-trading path. It is simulation-account-only, disabled by default,
manual only, and separated from DRY_RUN and LIVE_REAL.

## Boundary

- LIVE_SIM may enqueue a broker command only through the Gateway command queue.
- LIVE_REAL remains disabled and out of scope.
- DRY_RUN evidence can be a prerequisite, but DRY_RUN never auto-creates LIVE_SIM intents.
- AI Sidecar, RCA reports, and Codex prompt drafts are review-only and never order inputs.
- Dashboard is read-only and has no buy, sell, cancel, modify, reconcile, or queue buttons.

## Safety Gate

`check_live_sim_safety_gate(connection, settings)` blocks unless all critical checks pass:

- `TRADING_MODE=LIVE_SIM` or explicit LIVE_SIM enablement
- `TRADING_ALLOW_LIVE_SIM=true`
- `TRADING_ALLOW_LIVE_REAL=false`
- `LIVE_SIM_ENABLED=true`
- `LIVE_SIM_ORDER_ROUTING_ENABLED=true`
- `LIVE_SIM_GATEWAY_COMMAND_ENABLED=true`
- `LIVE_SIM_KILL_SWITCH=false`
- fresh Gateway heartbeat
- Gateway orderable status
- simulation-like configured and reported account/server/broker mode
- configured simulation account ID
- positive notional and daily limits
- daily order count remaining
- Gateway queue healthy
- AI tools and AI order tools disabled

Any failure creates no intent and no Gateway command. Intent creation failures are recorded in
`live_sim_rejections`.

## Eligibility

`evaluate_live_sim_eligibility` additionally requires:

- Candidate is `CONTEXT_READY`
- latest Strategy is `MATCHED_OBSERVATION`
- latest Risk is `OBSERVE_PASS`
- latest tick exists and is fresh
- DRY_RUN evidence exists when `LIVE_SIM_REQUIRE_DRY_RUN_EVIDENCE=true`
- no duplicate active LIVE_SIM intent/order for the code
- per-order notional, daily count, daily notional, active order, and active position limits pass
- buy is allowed, market order is disabled by default, sell/exit-sell are disabled by default

## Command Flow

Manual flow:

1. Evaluate eligibility with API or CLI.
2. Create a `LiveSimIntent`.
3. Queue a `LiveSimOrderRecord` from that intent.
4. Gateway polls the command.
5. Mock Gateway emits `command_started` and `command_ack`.
6. Optional mock execution event records a `live_sim_executions` row.
7. Reconcile stores a local-only `live_sim_reconcile_snapshots` row.

The queued command is `send_order` and must include:

- `source=live_sim`
- `mode=LIVE_SIM` and `live_mode=LIVE_SIM`
- command-level and payload `idempotency_key`
- simulation-like `account_mode`, `broker_env`, and `server_mode`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- `metadata.live_sim_intent_id`
- BUY side only in PR12

`cancel_order` and `modify_order` remain disabled.

## DB Tables

- `live_sim_intents`
- `live_sim_orders`
- `live_sim_executions`
- `live_sim_rejections`
- `live_sim_runs`
- `live_sim_reconcile_snapshots`
- `live_sim_errors`

## API

Read-only:

- `GET /api/live-sim/status`
- `GET /api/live-sim/intents`
- `GET /api/live-sim/intents/{live_sim_intent_id}`
- `GET /api/live-sim/orders`
- `GET /api/live-sim/orders/{live_sim_order_id}`
- `GET /api/live-sim/executions`
- `GET /api/live-sim/rejections`
- `GET /api/live-sim/reconcile`
- `GET /api/live-sim/errors`

Manual local-token protected:

- `POST /api/live-sim/evaluate`
- `POST /api/live-sim/intents/from-candidate/{candidate_instance_id}`
- `POST /api/live-sim/orders/from-intent/{live_sim_intent_id}`
- `POST /api/live-sim/reconcile`

Every POST response includes `live_sim_only=true`, `live_real_allowed=false`,
`broker_order_path=LIVE_SIM_ONLY`, and `real_order_allowed=false`.

## CLI

```powershell
python tools/evaluate_live_sim_eligibility.py --candidate-instance-id CANDIDATE_ID
python tools/create_live_sim_intent.py --candidate-instance-id CANDIDATE_ID
python tools/queue_live_sim_order.py --live-sim-intent-id live_sim_intent_x
python tools/reconcile_live_sim.py
```

CLI tools never bypass the safety gate and never create LIVE_REAL orders.

## Dashboard

Dashboard snapshot includes `live_sim` with status, safety gate, counts, recent intents, orders,
executions, rejections, and reconcile snapshots. The browser UI renders these read-only and performs
no LIVE_SIM POST calls.

## Mock Gateway Acceptance

The mock Gateway accepts `send_order` only when the LIVE_SIM metadata is present and simulation-like.
It emits `command_started` and `command_ack` with a mock broker order number. Tests may request an
optional execution event through mock metadata. `apps/kiwoom_gateway.py` remains a placeholder; Core
does not import PyQt5/QAxWidget or call Kiwoom order APIs.

## Reconcile Policy

PR12 reconcile is local-only. It compares local open LIVE_SIM orders with Gateway command state and
stores `LOCAL_ONLY` or `RECONCILE_MISMATCH`. Broker account snapshots are marked unavailable until a
separate simulation-account reconcile command is reviewed.

## Rollback

1. Set `LIVE_SIM_KILL_SWITCH=true`.
2. Set `LIVE_SIM_ORDER_ROUTING_ENABLED=false`.
3. Set `LIVE_SIM_GATEWAY_COMMAND_ENABLED=false`.
4. Set `LIVE_SIM_ENABLED=false` and `TRADING_ALLOW_LIVE_SIM=false`.
5. Confirm `/api/live-sim/status` safety gate is blocked.
6. Inspect `live_sim_orders`, `gateway_commands`, and `live_sim_reconcile_snapshots`.

## Forbidden Scope

- LIVE_REAL implementation
- real account mode or real server mode
- real Kiwoom/PyQt order calls in Core
- generic `/api/orders/enqueue`
- cancel/modify endpoints
- background automatic LIVE_SIM workers
- Dashboard order execution controls
- AI/RCA/Codex-output-driven order creation
- idempotency-free order commands
- safety gate bypass
