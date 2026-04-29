# 백엔드 조회/작업 라우트 분리 7~10단계 적용 메모

## 7단계: 라우트 분리 안정화

- `scripts/route_split_audit.py`를 추가했습니다.
- 실행 명령:
  - `python scripts/route_split_audit.py`
- 확인 내용:
  - 2~6단계 분리 대상 URL이 각 `routes/*_routes.py`에 존재하는지 확인
  - 분리된 URL이 다시 `app.py`의 `@app.route`로 등록되지 않았는지 확인
  - 같은 URL이 여러 라우트 파일에 중복 등록되지 않았는지 확인

## 8단계: 프론트 조회 성능 보강

- `templates/index.html`에 조회 전용 API 캐시/중복 요청 공유 로직을 추가했습니다.
- 적용 대상:
  - 선택 광고그룹 상세 조회: 키워드/소재/확장소재/제외키워드
  - 계정 단위 조회: 키워드/소재/확장소재
- 같은 payload로 짧은 시간 안에 재조회할 경우 브라우저가 동일 응답을 재사용합니다.
- 같은 조회가 이미 진행 중이면 새 요청을 추가로 날리지 않고 기존 요청 결과를 공유합니다.
- 광고그룹을 빠르게 바꿀 때 이전 조회 결과가 뒤늦게 도착해 화면을 덮어쓰지 않도록 `detailRequestSeq` stale guard를 추가했습니다.

## 9단계: 계정 단위 조회/엑셀 최적화

- `services/account_lookup_service.py`에 짧은 TTL의 서버 측 조회 캐시를 추가했습니다.
- 계정 단위 조회 후 바로 엑셀 다운로드를 실행할 때 같은 데이터를 다시 수집하지 않고 재사용합니다.
- 프론트에서 변경/등록/삭제성 작업이 성공하면 `_cache_bust` 값이 올라가서 이후 계정 조회는 새 캐시 키를 사용합니다.

## 10단계: 공통 응답 헬퍼 정리

- `services/api_response.py`를 추가했습니다.
- 상세 조회/계정 조회 서비스의 오류 응답 생성부를 공통 헬퍼로 정리했습니다.
- 기존 프론트 호환을 위해 성공 응답 형태는 기존 배열/객체 구조를 유지했습니다.

## 검증

- `python -S -m py_compile app.py routes/*.py services/*.py utils/*.py scripts/route_split_audit.py`
- `python -S scripts/route_split_audit.py`
- `node --check`로 `templates/index.html` 내부 JavaScript 문법 확인
