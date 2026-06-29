# Market Open Runtime RCA

- trade_date: `2026-06-29`
- overall_status: `BLOCK`
- generated_at: `2026-06-29T09:58:43.334658+00:00`
- core_url: `http://127.0.0.1:8000`
- mode: read-only, queue_commands=false, LIVE_REAL disallowed

| Stage | Status | Reason codes | Summary |
| --- | --- | --- | --- |
| Core | PASS | - | Core health and /api/status are ok. |
| Gateway | BLOCK | COMM_CONNECT_NO_RETURN, ON_EVENT_CONNECT_TIMEOUT, KIWOOM_LOGIN_DIALOG_OR_VERSION_SUSPECTED, ACTIVE_X_LOGIN_BLOCKED, CONNECT_STATE_FALLBACK_SKIPPED, CORE_STATUS_ERROR | Kiwoom CommConnect/OnEventConnect did not complete; realtime registration was not attempted. |
| MarketData | BLOCK | TICK_STALE, NO_CONDITION_HIT | All latest ticks are stale by threshold 10s. |
| Theme | PASS | - | theme_snapshots=100, LEADING=0, SPREADING=0 |
| Candidate | WARN | CANDIDATE_EMPTY | No active candidates exist. |
| Strategy | WARN | STRATEGY_EMPTY | Strategy ran but is empty. |
| Risk | WARN | RISK_EMPTY | Risk ran but is empty. |
| EntryTiming | WARN | ENTRY_TIMING_NO_INPUT | EntryTiming has not evaluated inputs; PLAN_READY=0, WAIT_RETRY=0, DATA_WAIT=0, NO_PLAN=0 |
| LiveSim | WARN | LIVE_SIM_DISABLED_EXPECTED, LIVE_SIM_KILL_SWITCH_ON_EXPECTED | LIVE_SIM operator status has blocking reasons for order modes. |
| OrderSafety | PASS | ORDER_COMMAND_ZERO_EXPECTED | No order-like GatewayCommand exists in command status. |

## Error Details

| Stage | event_id | code | error | payload_excerpt |
| --- | --- | --- | --- | --- |
| MarketData | evt_5663fa947c254be189c8992738239852 | 005930 | INVALID_PRICE_TICK:PRICE_MISSING | {"best_ask": 327500, "best_bid": 326500, "change_rate": 0.0, "code": "005930", "day_high": 1, "day_low": 1, "execution_strength": 0.0, "metadata": {"open_price": 0, "raw_fids_pr... |
| MarketData | evt_f6e9f2cf11e84e66aa42f6f072f37c12 | 005930 | INVALID_PRICE_TICK:PRICE_MISSING | {"best_ask": 327000, "best_bid": 326500, "change_rate": 0.0, "code": "005930", "day_high": 1, "day_low": 1, "execution_strength": 0.0, "metadata": {"open_price": 0, "raw_fids_pr... |
| MarketData | evt_c32e777ada8947d19780ae225d24b94a | 005930 | INVALID_PRICE_TICK:PRICE_MISSING | {"best_ask": 326500, "best_bid": 326000, "change_rate": 0.0, "code": "005930", "day_high": 1, "day_low": 1, "execution_strength": 0.0, "metadata": {"open_price": 0, "raw_fids_pr... |
| MarketData | evt_dfce8667da98454db3b8b048174fc156 | 005930 | INVALID_PRICE_TICK:PRICE_MISSING | {"best_ask": 327000, "best_bid": 326500, "change_rate": 0.0, "code": "005930", "day_high": 1, "day_low": 1, "execution_strength": 0.0, "metadata": {"open_price": 0, "raw_fids_pr... |
| MarketData | evt_640b6be2e0974c5b98b6e52bad47325d | 005930 | INVALID_PRICE_TICK:PRICE_MISSING | {"best_ask": 327000, "best_bid": 326000, "change_rate": 0.0, "code": "005930", "day_high": 1, "day_low": 1, "execution_strength": 0.0, "metadata": {"open_price": 0, "raw_fids_pr... |

## Operator Notes

- `PASS`는 해당 관찰 단계가 읽히거나 기대된 안전 차단 상태라는 뜻입니다.
- `WARN`은 다음 단계가 비어 있거나 아직 run_once/rebuild가 필요할 수 있다는 뜻입니다.
- `BLOCK`은 인증, Core/Gateway, projection error처럼 먼저 해소해야 하는 지점입니다.
- `LIVE_SIM_DISABLED_EXPECTED`, `LIVE_SIM_KILL_SWITCH_ON_EXPECTED`, `ORDER_COMMAND_ZERO_EXPECTED`는 observe-only 운영에서 정상 안전 상태입니다.
