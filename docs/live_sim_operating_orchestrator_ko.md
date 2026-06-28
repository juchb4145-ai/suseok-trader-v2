# LIVE_SIM Operating Orchestrator

이 문서는 장중 운영자가 LIVE_SIM run_once cycle을 안전한 순서로 한 번 실행하기 위한 절차를 정리한다. 이 기능은 scheduler, daemon, LIVE_REAL, 매수 조건 완화, AI 자동 주문 승격을 구현하지 않는다.

## OperatingMode

| mode | 의미 | command 정책 |
| --- | --- | --- |
| `OBSERVE_CYCLE` | 테마/후보/EntryTiming/AI/No-Buy 관찰 | command 생성 금지 |
| `PILOT_BUY_ONLY` | 신규 BUY pipeline 파일럿 | BUY만 가능, cancel/exit command 금지 |
| `PILOT_FULL_LIFECYCLE` | BUY + 미체결 cancel + close-only exit | LIVE_SIM 전용 command 가능 |
| `PROTECT_ONLY` | 장애/리컨실/포지션 보호 | 신규 BUY 금지, cancel/exit만 가능 |

기본 mode는 `OBSERVE_CYCLE`이며, CLI/API 모두 `queue_commands=false`가 기본값이다.

## Run Once 순서

1. preflight
2. reconcile run_once
3. cancel_unfilled evaluate/run_once
4. exit evaluate/run_once
5. ThemeLeadership inspect/rebuild
6. EntryTiming evaluate
7. AI candidate scoring, optional
8. LIVE_SIM pilot BUY run_once
9. No-Buy Sentinel rebuild
10. operator summary 생성 및 `live_sim_operating_runs` 저장

preflight `BLOCK`이면 command는 생성되지 않는다. `WARN`은 summary에 표시되며, `LIVE_SIM_OPERATING_REQUIRE_PREFLIGHT_PASS_FOR_QUEUE` 정책에 따라 queue가 제한될 수 있다.

## 장전 절차

```powershell
python -m tools.import_naver_themes --dry-run
python -m tools.import_naver_themes
python -m tools.inspect_theme_leadership
python -m tools.run_live_sim_operating_cycle_once --mode OBSERVE_CYCLE
```

## 장중 관찰

```powershell
python -m tools.run_live_sim_operating_cycle_once --mode OBSERVE_CYCLE
```

## 장중 매수 파일럿

```powershell
python -m tools.run_live_sim_operating_cycle_once --mode PILOT_BUY_ONLY --queue-commands
```

BUY command는 mode, preflight, safety gate, LIVE_SIM 설정, command budget이 모두 허용할 때만 queue된다.

## 장중 전체 Lifecycle

```powershell
python -m tools.run_live_sim_operating_cycle_once --mode PILOT_FULL_LIFECYCLE --queue-commands
```

신규 BUY보다 reconcile/cancel/exit 점검이 먼저 실행된다. exit는 close-only이며 short는 금지된다.

## 장애/보호 모드

```powershell
python -m tools.run_live_sim_operating_cycle_once --mode PROTECT_ONLY --queue-commands
```

`PROTECT_ONLY`는 신규 BUY를 금지하고 cancel/exit/reconcile만 허용한다.

## 실행 이력 조회

```powershell
python -m tools.inspect_live_sim_operating_runs --latest
python -m tools.inspect_live_sim_operating_runs --limit 20
```

## API

- `GET /api/live-sim/operator/runs`
- `GET /api/live-sim/operator/runs/latest`
- `GET /api/live-sim/operator/preflight`
- `GET /api/live-sim/operator/status`
- `POST /api/live-sim/operator/run-once?mode=OBSERVE_CYCLE&queue_commands=false`

POST는 local token으로 보호된다. 모든 응답은 `live_sim_only=true`, `live_real_allowed=false`를 포함한다.

## Command Budget

- `LIVE_SIM_OPERATING_MAX_BUY_COMMANDS_PER_CYCLE=1`
- `LIVE_SIM_OPERATING_MAX_CANCEL_COMMANDS_PER_CYCLE=3`
- `LIVE_SIM_OPERATING_MAX_EXIT_COMMANDS_PER_CYCLE=3`

모든 command count는 run result와 operator summary에 남는다.

## Rollback

```text
LIVE_SIM_KILL_SWITCH=true
LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=false
LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=false
LIVE_SIM_CANCEL_ENABLED=false
LIVE_SIM_EXIT_ENGINE_ENABLED=false
AI_CANDIDATE_SCORER_ENABLED=false
```

Dashboard에는 실행 버튼, buy/sell/cancel 버튼, settings toggle, AI execute 버튼이 없다.
