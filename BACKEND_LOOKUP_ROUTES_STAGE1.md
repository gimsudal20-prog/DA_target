# 조회 라우트 분리 1단계

## 목적
`app.py`에 직접 붙어 있던 단순 조회 라우트 일부를 Blueprint 기반 라우트 파일로 분리합니다.
기존 URL, 요청 payload, 응답 형태는 유지합니다.

## 분리된 라우트
- `POST /get_campaigns`
- `POST /get_adgroups`
- `POST /get_biz_channels`

## 추가 파일
- `routes/__init__.py`
- `routes/lookup_routes.py`

## 유지한 부분
- 기존 프론트 호출 URL 유지
- 기존 응답 형태 유지
- 기존 `LookupService` 사용 유지
- 기존 캐시/force 옵션 유지
- 기존 광고그룹 media enrich 기본값 유지

## 다음 후보
- `get_keywords`, `get_ads`, `get_ad_extensions`를 detail lookup 라우트로 분리
- 계정 단위 조회/엑셀 라우트는 export/service 정리 후 분리
