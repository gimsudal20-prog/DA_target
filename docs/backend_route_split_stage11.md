# Backend Route Split Stage 11

## 목적

`app.py`에 남아 있던 조회 계열 레거시 핸들러 본문을 제거하고, 라우트 분리 구조가 다시 모놀리식으로 되돌아가지 않도록 정적 점검을 강화했다.

## 반영 내용

- `app.py`에서 이미 blueprint/service로 이동된 미사용 조회 핸들러 제거
  - `get_keywords`
  - `get_ads`
  - `get_ad_extensions`
  - `get_restricted_keywords`
  - `query_account_ads`
  - `query_account_extensions`
  - `query_account_keywords`
  - `export_account_keywords_excel`
  - `export_account_ads_excel`
  - `export_account_extensions_excel`
- 기존 URL은 그대로 유지
- 기존 응답 형태는 각 route/service 파일 기준으로 유지
- 등록/변경/복사/삭제 기능 함수는 blueprint wrapper에서 계속 참조하므로 제거하지 않음
- `scripts/route_split_audit.py` 강화
  - 분리 대상 URL이 `app.py`에 `@app.route`로 재등록됐는지 확인
  - 분리 대상 핸들러 함수명이 `app.py`에 다시 생겼는지 확인
  - 각 분리 URL이 기대한 route 파일에 정확히 1회만 존재하는지 확인
  - route 파일별 담당 URL 맵 출력

## 검증 명령

```bash
python -S -m py_compile app.py scripts/route_split_audit.py
python -S scripts/route_split_audit.py
```

현재 환경에서는 일반 `python` 실행 시 site 로딩이 지연될 수 있어, 정적 검증에는 `python -S`를 사용했다.
