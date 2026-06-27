# Market Data Service

## 요약

Market Data Service는 accepted Gateway event에서 tick/latest/bar/VWAP/readiness를 만드는 read-only projection이다. Event Store가 source of truth이고, market data table은 언제든 rebuild 가능한 파생 상태다. 이 기능은 실계좌 주문을 의미하지 않는다.

## 하는 일 / 하지 않는 일

| 구분 | 내용 |
| --- | --- |
| 하는 일 | `price_tick`, `condition_event`, `tr_response`를 정규화해 SQLite projection으로 저장 |
| 하는 일 | latest tick, tick sample, 1/3/5 minute bar, VWAP, readiness, condition latest 제공 |
| 하지 않는 일 | Strategy 판단, Risk 판단, OMS 생성, `GatewayCommand` 생성 |
| 하지 않는 일 | `send_order`, `cancel_order`, `modify_order` 호출 |

## Projection Table

| Table | 의미 |
| --- | --- |
| `market_ticks_latest` | code별 최신 normalized tick |
| `market_tick_samples` | event별 append-only tick sample |
| `market_minute_bars` | `(code, interval_sec, bucket_start)` 기준 bar |
| `market_condition_signals` | condition ENTER/EXIT append-only signal |
| `market_condition_latest` | `(condition_id, code)`별 최신 condition state |
| `market_tr_snapshots` | TR response row snapshot |
| `market_projection_errors` | projection 실패 기록 |

## Price Tick 처리

`process_gateway_event()`는 `price_tick` payload를 먼저 `BrokerPriceTick.from_dict()`로 검증한다.

저장/계산 내용:

- `market_ticks_latest` upsert
- `market_tick_samples` insert
- 이전 latest tick 대비 `volume_delta`, `trade_value_delta` 계산
- cumulative 값 reset처럼 보이는 negative delta는 0으로 clamp
- `MARKET_DATA_BAR_INTERVALS_SEC` 기준 bar 업데이트
- tick shape에 따라 `FRESH`, `DEGRADED`, `INVALID` quality 저장

## Minute Bar와 VWAP

bar bucket은 tick trade time 기준이다.

| 값 | 의미 |
| --- | --- |
| `open` | bucket 첫 가격 |
| `high` | bucket 최고가 |
| `low` | bucket 최저가 |
| `close` | bucket 마지막 가격 |
| `volume_delta` | bucket 내 volume delta 합 |
| `trade_value_delta` | bucket 내 trade value delta 합 |
| `tick_count` | projected tick 수 |
| `vwap` | 가능하면 cumulative trade value / cumulative volume |

VWAP이 없다는 것은 가격 위치 판단의 입력이 부족하다는 뜻이다. 주문 금지나 주문 승인 상태가 아니다.

## Readiness

`get_market_data_readiness()`는 운영자가 “데이터가 충분한가”를 보는 값이다.

| 상태 | 의미 |
| --- | --- |
| `MISSING` | latest tick 없음 |
| `FRESH` | tick age가 `MARKET_DATA_TICK_STALE_SEC` 이내 |
| `STALE` | stale threshold 초과, degraded threshold 이내 |
| `DEGRADED` | degraded threshold 초과 |
| `INVALID` | tick shape가 유효하지 않음 |

또한 1m/3m/5m bar 존재 여부, VWAP 가능 여부, `BAR_MISSING`, `BAR_MISSING_60`, `VWAP_MISSING` 같은 reason code를 함께 준다.

데이터 부족은 중요하다. Candidate, Strategy, Risk가 모두 이 projection을 읽기 때문에 tick이나 bar가 부족하면 다음 단계가 `DATA_WAIT`로 남을 수 있다.

## API

- `GET /api/market-data/status`
- `GET /api/market-data/ticks/latest`
- `GET /api/market-data/ticks/{code}`
- `GET /api/market-data/bars/{code}`
- `GET /api/market-data/readiness/{code}`
- `GET /api/market-data/conditions/recent`
- `GET /api/market-data/tr-snapshots/recent`
- `GET /api/market-data/projection-errors`

Market Data Service에는 POST, PUT, DELETE, order, strategy, risk, OMS endpoint가 없다.

## Rebuild

비파괴 replay:

```powershell
python -m tools.rebuild_market_data_projection
```

projection table을 clear하고 replay:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

## Downstream 연결

| 다음 단계 | 읽는 데이터 |
| --- | --- |
| Theme Service | latest tick, minute bar, VWAP, readiness, condition latest |
| Candidate FSM | condition signal, readiness, latest tick, bar/VWAP readiness |
| Strategy Engine | candidate context와 market projection |
| Risk Gate | market freshness, liquidity, spread, VWAP extension |
| Dashboard | status, counts, errors, latest values |

## 운영자 체크포인트

- Dashboard가 비어 있으면 먼저 `/api/market-data/status`를 본다.
- code별 `readiness`에서 `MISSING`, `STALE`, `DEGRADED`, `INVALID` 이유를 확인한다.
- bar/VWAP 부족은 다음 단계의 `DATA_WAIT` 원인이 될 수 있다.
- Market Data Service는 주문 판단을 하지 않는다.
