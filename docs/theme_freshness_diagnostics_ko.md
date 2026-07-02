# 테마 신선도와 정합성 가드 운영 메모

PR-4 범위는 대시보드 신선도와 정합성 진단을 추가한다.

- `THEME_SNAPSHOT_STALE_SEC` 기본값은 300초다.
- Dashboard snapshot의 테마 row에는 `age_sec`, `stale`, `stale_sec`가 포함된다.
- `web/static/dashboard.js`는 stale 테마에 `STALE` 배지를 표시한다.
- market index tick metadata의 `parser_status`가 `VERIFIED`가 아니면 dashboard payload에 `unverified=true`가 전달된다.
- KOSPI 값이 1000~6000 범위를 벗어나거나 `abs(change_rate) > 15`면 `market_index_projection_errors.reason_code=INDEX_IMPLAUSIBLE`로 기록된다.

NAVER_REFERENCE 정합성 진단은 read-only 함수와 API로 제공된다.

```text
GET /api/themes/diagnostics/naver-overlap
```

이 진단은 시스템의 LEADING 테마와 NAVER_REFERENCE 멤버십의 겹침률을 기록 및 반환하지만 주문, 후보, 구독 상태를 변경하지 않는다.
