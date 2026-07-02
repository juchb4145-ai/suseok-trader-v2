# Theme Refresh Cycle 운영 메모

PR-3 범위의 `services/runtime/theme_refresh_cycle.py`는 장중 경량 갱신 루프다.

실행 순서는 다음과 같다.

1. market scan request 계획 또는 큐잉
2. `calculate_all_theme_snapshots`
3. `rebuild_theme_leadership`
4. leadership watchset 기반 realtime subscription 재배치

전략, 리스크, 진입 평가는 포함하지 않는다. 이 루프는 기존 `EVALUATION_PIPELINE_LOCK`을 재사용한다. 이유는 전체 observe cycle과 같은 theme projection 및 gateway command queue를 갱신하므로 동시에 돌 때 최신 projection과 subscription 계획이 엇갈릴 수 있기 때문이다.

수동 API:

```text
POST /api/themes/refresh-cycle/run-once
```

토큰이 설정된 환경에서는 기존 local token 인증이 필요하다. 결과 payload에는 `order_command_delta`가 포함되며 정상 경로에서는 모든 값이 0이어야 한다.
