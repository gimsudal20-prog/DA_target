# Uibowl reference component patch (2026-06-28)

사용자가 첨부한 uibowl 파트너 화면 이미지의 시각 요소만 반영했다.
전체 사이트 구조를 복제하지 않고, 현재 검색광고 툴의 기능 구조는 유지한 상태에서 아래 컴포넌트 톤을 재현했다.

## 반영 범위

- 흰 배경 중심의 SaaS 대시보드 톤
- 얇은 회색 border 카드
- 큰 숫자 중심 KPI 카드
- 텍스트 탭 + 하단 라인형 active 표시
- 성과 조회 결과 상단에 `성과 패턴 요약` 블록 추가
- `주목받은 성과 TOP 5` / `관심이 낮은 패턴 TOP 5` 가로 막대 랭킹 카드 추가
- 보고서, 쇼핑검색어, 연령/시간/지역 데이터 카드의 여백과 border 톤 정리

## 적용 파일

- `templates/index.html`
  - CSS: `Uibowl reference component layer 2026-06-28`
  - JS: `Uibowl reference summary renderer 2026-06-28`

## 로직 변경 여부

- API 호출, 데이터 조회, 등록/변경/복사/삭제 로직은 변경하지 않았다.
- 성과 조회 결과 렌더링 이후 요약 UI를 삽입하는 방식으로만 추가했다.
- 요약 UI는 기존 `/get_performance_stats` 응답의 `summary`, `rows`, `metrics`를 사용한다.
