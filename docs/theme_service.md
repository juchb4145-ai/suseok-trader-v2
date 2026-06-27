# Theme Service

## 요약

Theme Service는 theme membership과 theme snapshot을 관리하는 read-only 관찰 계층이다. 운영자가 모든 테마를 손으로 묶어 운영하는 구조가 아니다. 수동 파일은 bootstrap/testing only이며, production membership은 나중에 legacy export, 외부 reference importer, condition-derived source 등으로 확장될 수 있다.

## 하는 일 / 하지 않는 일

| 구분 | 내용 |
| --- | --- |
| 하는 일 | theme membership 저장과 조회 |
| 하는 일 | Market Data projection을 theme level snapshot으로 집계 |
| 하는 일 | theme state, quality, leader/co-leader/follower 관찰 context 제공 |
| 하지 않는 일 | 투자 추천, Strategy 판단, Risk 판단 |
| 하지 않는 일 | `OrderIntent`, `GatewayCommand`, broker order 생성 |

## Membership Schema

`themes`:

- `theme_id`
- `theme_name`
- `source_type`
- `source_name`
- `active`
- `metadata_json`
- `created_at`
- `updated_at`

`theme_members`:

- `theme_id`
- `code`
- `name`
- `source_type`
- `source_name`
- `active`
- `weight`
- `metadata_json`
- `created_at`
- `updated_at`

`source_type`:

- `MANUAL`
- `IMPORTED`
- `NAVER_REFERENCE`
- `CONDITION_DERIVED`
- `MOCK`

## Import Payload

```json
{
  "source_type": "MANUAL",
  "source_name": "local_seed",
  "themes": [
    {
      "theme_id": "semiconductor",
      "theme_name": "반도체",
      "members": [
        {"code": "005930", "name": "삼성전자"},
        {"code": "000660", "name": "SK하이닉스"}
      ]
    }
  ]
}
```

동작:

- stock code는 broker contract validator로 정규화된다.
- duplicate import는 idempotent upsert다.
- `replace=false`는 기존 member를 유지한다.
- `replace=true`는 같은 theme/source scope의 member를 inactive 처리한 뒤 upsert한다.
- import batch는 `theme_import_batches`에 기록된다.

## Snapshot Table

| Table | 의미 |
| --- | --- |
| `theme_snapshots` | theme-level summary |
| `theme_snapshot_members` | snapshot별 member observation |
| `theme_latest_snapshots` | theme별 latest snapshot pointer |
| `theme_projection_errors` | member/theme projection error |

Snapshot은 파생 projection이다. Event Store와 Market Data Service가 source of truth다.

## Snapshot 계산

읽는 데이터:

- `market_ticks_latest`
- 60/180/300 second `market_minute_bars`
- `get_market_data_readiness()`
- `market_condition_latest`

Theme summary:

- active/observed/fresh member count
- fresh coverage ratio
- rising count와 rising ratio
- average/max change rate
- total cumulative trade value
- 1/3/5 minute trade-value delta
- leading code/name
- co-leader/follower list
- state, quality, reason code

## Member Role

| Role | 의미 |
| --- | --- |
| `LEADER_CANDIDATE` | fresh observed member 중 score 상위 |
| `CO_LEADER_CANDIDATE` | leader score 대비 일정 비율 이상 |
| `FOLLOWER_CANDIDATE` | fresh rising member |
| `LAGGARD` | fresh observed but non-rising |
| `STALE` | stale/degraded observed member |
| `UNKNOWN` | missing member |

`LEADER_CANDIDATE`는 매수 신호가 아니다. Candidate FSM으로 넘길 수 있는 관찰 source일 뿐이다.

## ThemeState

| State | 운영자 해석 |
| --- | --- |
| `DATA_WAIT` | membership 또는 fresh data 부족 |
| `WATCH` | 관찰은 있으나 확산/주도 조건은 약함 |
| `SPREADING` | theme 내 상승 확산 관찰 |
| `LEADING` | leader와 rising ratio, trade value 조건 관찰 |
| `FADING` | future hysteresis/time-based rule 예약 |
| `ROTATED_OUT` | future rotation rule 예약 |

`SPREADING`, `LEADING`은 theme 관찰 상태다. 주문 상태가 아니다.

## API

- `GET /api/themes/status`
- `GET /api/themes?active_only=true`
- `GET /api/themes/{theme_id}?include_members=true`
- `GET /api/themes/{theme_id}/members`
- `GET /api/themes/by-code/{code}`
- `GET /api/themes/snapshots/latest?limit=100&state=LEADING`
- `GET /api/themes/{theme_id}/snapshot/latest?include_members=true`
- `GET /api/themes/{theme_id}/snapshots?limit=100`
- `GET /api/themes/projection-errors?limit=100`
- `POST /api/themes/import?replace=false`
- `POST /api/themes/snapshots/rebuild?theme_id=semiconductor`

POST endpoint는 import/rebuild 용도이며 주문 endpoint가 아니다.

## Rebuild

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_theme_snapshots --theme-id semiconductor
```

HTTP rebuild:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/themes/snapshots/rebuild?theme_id=semiconductor" `
  -Method Post `
  -Headers @{"X-Core-Token" = $env:TRADING_CORE_TOKEN}
```

## Candidate FSM 연결

Candidate FSM은 `theme_latest_snapshots`와 `theme_snapshot_members`를 source context로 읽는다. 기본적으로 `LEADING`, `SPREADING` theme state와 `LEADER_CANDIDATE`, `CO_LEADER_CANDIDATE`, `FOLLOWER_CANDIDATE` role이 candidate source가 될 수 있다.

이것은 후보 관찰 에피소드의 source attribution이다. `CONTEXT_READY`도 매수 준비가 아니라 Strategy 평가 입력 준비다.

## 운영자 체크포인트

- sample theme file은 bootstrap/testing only다.
- `LEADING` theme를 투자 추천이나 주문 지시로 해석하지 않는다.
- Theme snapshot이 비어 있으면 membership import와 market data freshness를 확인한다.
- Theme Service는 `send_order`, `cancel_order`, `modify_order`를 만들지 않는다.
