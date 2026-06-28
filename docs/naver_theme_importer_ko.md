# Naver Theme Importer

## 요약

Naver Theme Importer는 네이버 금융 테마 페이지를 theme universe와 membership reference source로만 사용한다.

흐름:

```text
Naver theme list/detail fetch
  -> theme/member evidence normalize
  -> stable naver theme_id canonicalize
  -> Theme Service payload 생성
  -> themes/theme_members upsert
  -> import batch/error audit 기록
  -> ThemeLeadershipService active membership 입력
```

네이버 등락률, 순위, 주도주 표시는 metadata로만 저장한다. 장중 주도 테마 판단, 대장주 판단, 후보 판단은 계속 Kiwoom realtime tick, MarketData projection, ThemeLeadershipService가 담당한다.

## Source Scope

- `source_type=NAVER_REFERENCE`
- `source_name=naver_theme`
- 기본 `replace=false`
- `replace=true`여도 `NAVER_REFERENCE/naver_theme` scope에서만 inactive 처리한다.
- 기존 `MANUAL`, `IMPORTED` membership은 비활성화하지 않는다.

`themes.theme_name`은 기존 스키마에서 unique다. 같은 theme_name이 다른 theme_id로 이미 있으면 importer는 해당 theme을 skip하고 `theme_import_errors`에 기록해 기존 source를 덮어쓰지 않는다.

## Fetch / Parser

리스트 parser:

- `https://finance.naver.com/sise/theme.naver`의 `table.type_1.theme`를 우선 찾는다.
- 각 행의 `/sise/sise_group_detail.naver?type=theme&no=...` 링크에서 `source_theme_id`와 theme name을 읽는다.
- 전일대비 등락률 text와 리스트 순위는 theme metadata에만 저장한다.

상세 parser:

- 각 theme 상세 URL의 `table.type_5`를 우선 찾는다.
- `/item/main.naver?code=...` 링크에서 구성 종목 code/name을 읽는다.
- 편입 사유 layer의 `p.info_txt`는 evidence text로 저장한다.
- price, 등락률, 거래량 같은 네이버 화면 시장 데이터는 매수 판단에 쓰지 않는다.

Fetcher는 user-agent, timeout, retry, request sleep을 사용한다. 네트워크 실패나 parser 실패는 `theme_import_errors`에 남기며, fetch 결과가 비어 있으면 기본적으로 safe abort한다. 이 경우 기존 `theme_members`를 삭제하거나 비활성화하지 않는다.

## 설정

기본값:

```env
NAVER_THEME_IMPORT_ENABLED=false
NAVER_THEME_IMPORT_BASE_URL=https://finance.naver.com/sise/theme.naver
NAVER_THEME_IMPORT_TIMEOUT_SECONDS=10
NAVER_THEME_IMPORT_MAX_THEMES=50
NAVER_THEME_IMPORT_REQUEST_SLEEP_SECONDS=0.3
NAVER_THEME_IMPORT_REPLACE=false
NAVER_THEME_IMPORT_MIN_MEMBER_COUNT=2
NAVER_THEME_IMPORT_ABORT_ON_EMPTY=true
```

`NAVER_THEME_IMPORT_ENABLED=false`가 기본이다. 자동 스케줄러가 붙더라도 운영자가 명시적으로 켠 경우에만 사용해야 한다. CLI를 직접 실행하는 것은 명시적 운영 작업으로 본다.

## CLI

Dry-run:

```powershell
python -m tools.import_naver_themes --dry-run
```

상위 20개 theme만 fetch:

```powershell
python -m tools.import_naver_themes --limit-themes 20
```

같은 네이버 source scope에서 replace:

```powershell
python -m tools.import_naver_themes --replace
```

결과 JSON 저장:

```powershell
python -m tools.import_naver_themes --dry-run --output data/themes/naver_themes_latest.json
```

Dry-run은 DB를 수정하지 않고 fetched theme/member count, duplicate count, parser error count, sample themes, sample members를 출력한다.

## 저장 정책

저장 payload는 기존 `import_theme_memberships()` 형식을 사용한다.

- theme id는 `naver_theme_{source_theme_id}` 형식으로 안정화한다.
- 종목 코드는 6자리 국내 코드로 validate한다.
- duplicate theme/member는 normalize 단계에서 제거한다.
- member metadata에는 `confidence`, `freshness.fetched_at`, `source_url`, `reason_text`, evidence를 저장한다.
- 네이버 `change_rate_text`, list rank, member rank는 metadata이며 ThemeLeadership score에 직접 사용하지 않는다.

## ThemeLeadership 연결

ThemeLeadershipService는 기존처럼 active `theme_members`와 MarketData projection을 읽는다. 네이버 importer가 저장한 membership도 active이면 universe에 포함된다.

중요 경계:

- 네이버 theme hit는 buy decision이 아니다.
- 네이버 등락률/순위는 `LEADING`, `READY`, `PLAN_READY`를 만들지 않는다.
- condition boost와 마찬가지로 네이버 source도 source attribution과 membership confidence일 뿐이다.
- PR-4 LIVE_SIM 주문 파이프라인과 직접 연결하지 않는다.

## 운영 전 명령 예시

PR-4 전에 운영자가 확인할 최소 절차:

```powershell
python -m tools.import_naver_themes --dry-run --limit-themes 20
python -m tools.import_naver_themes --limit-themes 20
python -m tools.inspect_theme_leadership
python -m tools.evaluate_entry_timing
python -m tools.run_live_sim_pilot_once
python -m tools.run_live_sim_pilot_once --queue-commands
```

멤버십 교체가 의도된 날에만:

```powershell
python -m tools.import_naver_themes --limit-themes 20 --replace
```

## 실패 정책

- 리스트 fetch 실패: import를 abort하고 기존 membership 유지.
- 상세 fetch 일부 실패: 성공한 theme/member만 후보 payload로 만들고 실패는 audit 기록.
- member 0개 또는 최소 member 수 미만 theme: 해당 theme skip.
- 전체 payload empty: 기본 safe abort.
- parser failure: `theme_import_errors` 기록.
- 주문 관련 테이블, `GatewayCommand`, `send_order` 경로는 변경하지 않는다.

## PR-4 경계

PR-4 LIVE_SIM run_once는 네이버 fetch를 자동 실행하지 않는다. Naver importer는 장전 또는 운영자 명시 CLI 작업이며, importer 결과는 theme membership reference일 뿐이다.

`PLAN_READY`가 있더라도 LIVE_SIM OrderPlan safety gate를 통과하지 않으면 `LiveSimIntent`나 `GatewayCommand(send_order)`가 생성되지 않는다. 네이버 등락률/순위, theme hit, condition hit만으로 주문을 만들지 않는다.
