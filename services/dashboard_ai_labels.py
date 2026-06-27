from __future__ import annotations

from typing import Any

AI_STATUS_LABELS = {
    "CREATED": "생성됨",
    "COMPLETED": "완료",
    "PARTIAL": "부분 완료",
    "FAILED": "실패",
    "AI_DISABLED": "AI 비활성",
    "AI_UNAVAILABLE": "AI 사용 불가",
    "CLIENT_UNAVAILABLE": "클라이언트 사용 불가",
    "AI_OUTPUT_INVALID": "AI 출력 형식 오류",
    "POLICY_REJECTED": "안전 정책 차단",
    "TIMEOUT": "시간 초과",
    "MODEL_ERROR": "모델 오류",
    "API_KEY_MISSING": "API key 없음",
    "CONTEXT_ERROR": "컨텍스트 오류",
}

AI_SEVERITY_LABELS = {
    "INFO": "정보",
    "LOW": "낮음",
    "MEDIUM": "중간",
    "HIGH": "높음",
    "CRITICAL": "치명",
}

RCA_CATEGORY_LABELS = {
    "OBSERVE_ONLY_PIPELINE": "관찰 전용 파이프라인",
    "NO_ORDER_PATH_BY_DESIGN": "주문 경로 없음",
    "DATA_QUALITY": "데이터 품질",
    "THEME_CONTEXT": "테마 컨텍스트",
    "CANDIDATE_CONTEXT": "후보 컨텍스트",
    "STRATEGY_CONTEXT": "전략 관측",
    "RISK_CONTEXT": "리스크 관측",
    "GATEWAY_TRANSPORT": "게이트웨이 전송",
    "MARKET_DATA_PROJECTION": "시세 데이터 투영",
    "THEME_PROJECTION": "테마 투영",
    "CANDIDATE_PROJECTION": "후보 투영",
    "STRATEGY_EVALUATION": "전략 평가",
    "RISK_EVALUATION": "리스크 평가",
    "AI_EXECUTION": "AI 실행 기록",
    "MARKET_DATA": "시세 데이터",
    "THEME_SERVICE": "테마 서비스",
    "CANDIDATE_FSM": "후보 FSM",
    "STRATEGY_ENGINE": "전략 엔진",
    "RISK_GATE": "리스크 게이트",
    "DASHBOARD": "대시보드",
    "AI_SIDECAR": "AI 사이드카",
    "RCA_WORKFLOW": "RCA 워크플로",
    "TESTING": "테스트",
    "DOCS": "문서",
    "SAFETY_REVIEW": "안전 검토",
    "UNKNOWN": "미분류",
}

CARD_TYPE_LABELS = {
    "NO_TRADE_RCA": "매수/주문 없음 RCA",
    "CANDIDATE_BLOCK_RCA": "후보 차단 RCA",
    "AI_INSIGHT": "AI 인사이트",
    "CODEX_PROMPT_DRAFT": "Codex 프롬프트 초안",
    "AI_REQUEST_FAILURE": "AI 요청 오류",
    "AI_CONTEXT_WARNING": "AI 컨텍스트 경고",
}

READONLY_OPERATOR_ACTION_LABELS = {
    "WATCH_ONLY": "관찰만",
    "REVIEW_ONLY": "검토만",
    "CHECK_DATA": "데이터 점검",
    "CHECK_PIPELINE": "파이프라인 점검",
    "CHECK_POLICY": "정책 점검",
    "NO_ACTION": "조치 없음",
    "OBSERVE_ONLY": "관찰만",
    "관찰": "관찰만",
    "검토": "검토만",
    "점검": "점검",
}


def map_ai_status_label(status: Any) -> str:
    normalized = _normalize(status)
    return AI_STATUS_LABELS.get(normalized, normalized or "-")


def map_ai_severity_label(severity: Any) -> str:
    normalized = _normalize(severity)
    return AI_SEVERITY_LABELS.get(normalized, normalized or "-")


def map_rca_category_label(category: Any) -> str:
    normalized = _normalize(category)
    return RCA_CATEGORY_LABELS.get(normalized, normalized or "-")


def map_readonly_operator_action(operator_action: Any) -> str:
    normalized = _normalize(operator_action)
    return READONLY_OPERATOR_ACTION_LABELS.get(normalized, normalized or "-")


def map_card_type_label(card_type: Any) -> str:
    normalized = _normalize(card_type)
    return CARD_TYPE_LABELS.get(normalized, normalized or "-")


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()
