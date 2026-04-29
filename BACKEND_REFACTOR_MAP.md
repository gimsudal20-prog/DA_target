# Backend Refactor Map — Stage 0

> 기준 파일: `app.py`  
> 목적: 기능 삭제 없이, 다음 백엔드 분리 작업의 기준점을 만든다.  
> 이번 단계에서는 런타임 코드를 수정하지 않고, 현재 라우트/기능/중복 영역만 분류한다.

## 1. 현재 상태 요약

| 항목 | 확인 결과 |
|---|---:|
| `app.py` 전체 라인 수 | 10,612 lines |
| `def` 함수 수 | 337개 |
| `@app.route` 데코레이터 수 | 75개 |
| 주요 문제 | 조회/등록/변경/복사/삭제/엑셀/로그 로직이 단일 파일에 혼재 |

현재 구조는 기능 자체는 많고 강력하지만, 같은 성격의 기능이 여러 위치에 나뉘어 있어 유지보수 리스크가 크다. 특히 입찰가/시간대/상태/타겟/소재 엑셀/키워드 조회 계열은 서로 관련된 헬퍼를 공유하면서도 라우트와 서비스 코드가 한 파일에 섞여 있다.

## 2. 라우트 카테고리 요약

| 카테고리 | 라우트 수 | 예시 |
|---|---:|---|
| System | 5 | `/`, `/health`, `/sample_headers` |
| Lookup | 20 | `/get_campaigns`, `/get_keywords`, `/query_account_ads`, `/search_powerlink_keywords` |
| Export | 5 | `/export_account_ads_excel`, `/export_powerlink_keywords_excel` |
| Logs | 3 | `/get_action_logs`, `/export_action_logs_excel`, `/clear_action_logs` |
| Create | 12 | `/create_campaign`, `/create_keywords_simple`, `/bulk_upload_text_ads` |
| Copy | 4 | `/copy_campaigns`, `/copy_adgroups_to_target`, `/copy_entities_to_adgroups` |
| Update | 23 | `/update_budget`, `/update_schedule`, `/update_keyword_bids`, `/apply_target_settings_bulk` |
| Delete | 3 | `/bulk_delete_by_parent`, `/bulk_delete`, `/delete_selected` |

전체 라우트 목록은 `docs/backend_route_inventory.csv`에 별도 정리했다.

## 3. 현재 퍼널 기준 백엔드 그룹핑

프론트 메뉴가 `조회 / 등록 / 변경 / 복사 / 삭제`로 정리되었으므로, 백엔드도 같은 목적 기준으로 나누는 것이 가장 안전하다.

### 3-1. 조회

주요 라우트:

- `/get_campaigns`
- `/get_adgroups`
- `/get_biz_channels`
- `/get_keywords`
- `/get_ads`
- `/get_ad_extensions`
- `/get_restricted_keywords`
- `/query_account_ads`
- `/query_account_extensions`
- `/query_account_keywords`
- `/find_powerlink_duplicate_keywords`
- `/find_account_powerlink_duplicate_keywords`
- `/get_powerlink_keyword_stats`
- `/search_powerlink_keywords`
- `/preview_keyword_bids_by_search`
- `/preview_keyword_bid_weights_by_search`
- `/preview_keyword_avg_position_by_search` 계열 alias

관련 헬퍼 범위:

- 캠페인/그룹/키워드/소재 fetch 계열
- 파워링크 키워드 검색/중복검색 계열
- 상세 조회용 소재/확장소재/제외키워드 수집 계열

추천 분리 위치:

```text
routes/lookup_routes.py
services/lookup_service.py
services/keyword_search_service.py
```

### 3-2. 등록

주요 라우트:

- `/create_campaign`
- `/create_adgroup_simple`
- `/create_keywords_simple`
- `/create_text_ad_simple`
- `/create_ad_advanced`
- `/create_extension_simple`
- `/create_extension_raw`
- `/create_shopping_ad_simple`
- `/create_restricted_keywords_simple`
- `/bulk_upload_text_ads`
- `/bulk_upload_headlines`
- `/bulk_register`

관련 헬퍼 범위:

- 캠페인/그룹/키워드/소재/확장소재 payload normalize
- 엑셀/붙여넣기 테이블 파싱
- TEXT_45/SHOPPING_PRODUCT_AD/확장소재 생성 fallback

추천 분리 위치:

```text
routes/create_routes.py
services/create_service.py
services/ad_create_service.py
services/extension_create_service.py
```

### 3-3. 변경

주요 라우트:

- `/update_media`
- `/update_adgroup_options` 및 `/update_adgroup_search_options` alias
- `/update_powerlink_device_bid_weights`
- `/apply_target_settings_bulk`
- `/update_budget`
- `/update_schedule`
- `/update_schedule_campaign_bulk`
- `/update_non_search_keyword_exclusion`
- `/update_keyword_bids_by_search`
- `/update_keyword_bid_weights_by_search`
- `/update_keyword_bids`
- `/update_bid_mode_by_scope`
- `/adjust_keyword_bids_by_threshold`
- `/update_keyword_avg_position_by_search` 계열 alias
- `/update_keyword_bids_avg_position`
- `/set_searched_powerlink_keyword_state`
- `/set_campaign_state`

관련 헬퍼 범위:

- 입찰가 직접 변경
- 검색어 기준 입찰가 변경
- PC/MO 입찰가중치 변경
- 목표 평균순위 기반 입찰가 산정
- 시간대/요일 스케줄 변경
- 예산 변경
- 매체/타겟/검색옵션 변경
- ON/OFF 상태 변경

추천 분리 위치:

```text
routes/update_routes.py
services/bid_service.py
services/schedule_service.py
services/target_service.py
services/state_budget_service.py
```

### 3-4. 복사

주요 라우트:

- `/copy_campaigns`
- `/copy_adgroups_to_target`
- `/copy_entities_to_adgroups`
- `/rename_adgroups_bulk`

관련 헬퍼 범위:

- 이름 충돌/복사본 suffix 처리
- 캠페인/그룹 복사
- 키워드/소재/확장소재/제외키워드 복사
- 타겟/시간대/매체/검색옵션 복사
- 복사 후 검증/재시도

추천 분리 위치:

```text
routes/copy_routes.py
services/copy_service.py
services/copy_validation_service.py
```

### 3-5. 삭제

주요 라우트:

- `/bulk_delete_by_parent`
- `/bulk_delete`
- `/delete_selected`

관련 헬퍼 범위:

- 캠페인/그룹/키워드/소재/확장소재/제외키워드 삭제
- 부모 기준 하위 항목 수집
- 확장소재 타입별 삭제 필터

추천 분리 위치:

```text
routes/delete_routes.py
services/delete_service.py
```

### 3-6. 엑셀 내보내기

주요 라우트:

- `/export_account_keywords_excel`
- `/export_account_ads_excel`
- `/export_account_extensions_excel`
- `/export_powerlink_duplicate_keywords_excel`
- `/export_powerlink_keywords_excel`
- `/export_action_logs_excel`

관련 헬퍼 범위:

- 한글 컬럼명 변환
- 소재/확장소재/캠페인/그룹 유형 라벨링
- openpyxl workbook 생성
- 필터/헤더/컬럼 너비/상태값 변환

추천 분리 위치:

```text
routes/export_routes.py
services/export_service.py
utils/excel.py
utils/labels.py
```

## 4. 중복/혼재 리스크 체크

### 4-1. 라벨/상태값 변환이 여러 곳에 흩어져 있음

현재 확인된 라벨 함수:

- `_campaign_label`
- `_adgroup_label`
- `_ad_label`
- `_extension_label`
- `_label_negative_type`
- `_normalize_campaign_tp`
- `_normalize_adgroup_tp`
- `_normalize_ad_type`
- `_normalize_extension_type`
- `_bulk_extension_type_label`
- `_target_type_label`

다음 단계에서는 이 계열을 먼저 `utils/labels.py`로 빼는 것이 안전하다. 기능 영향이 작고, 엑셀/조회/UI 응답의 표기 통일 효과가 크다.

### 4-2. API 요청 공통부는 가장 먼저 분리 후보

현재 핵심 공통 함수:

- `_sig`
- `_open_headers`
- `_do_req`
- `_make_fake_response`
- `_extract_response_message`

추천 위치:

```text
services/naver_client.py
```

단, 이 함수는 전체 기능이 의존하므로 바로 분리하기보다 라벨/엑셀 분리 이후 진행하는 것이 안전하다.

### 4-3. 입찰 관련 로직이 가장 복잡함

관련 범위:

- 검색어 기준 입찰가 변경
- 검색어 기준 입찰가중치 변경
- 직접 입찰가 변경
- 목표 평균순위 입찰가 산정
- 조건부 입찰가 조정
- 그룹/개별 입찰 설정 전환

이 영역은 사용 빈도가 높고 오류 위험도 높으므로 바로 분리하지 말고, 라우트별 테스트 케이스를 먼저 확보한 뒤 `services/bid_service.py`로 이동해야 한다.

### 4-4. 시간대/스케줄 로직은 독립 분리 가능성이 높음

관련 함수:

- `_normalize_schedule_days`
- `_normalize_schedule_hours`
- `_merge_schedule_hour_ranges`
- `_build_schedule_codes`
- `_schedule_code_to_slots`
- `_schedule_map_to_slot_map`
- `_collapse_schedule_slot_map`
- `_normalize_schedule_blocks`
- `_build_schedule_weighted_codes`
- `_extract_schedule_weight_map`
- `_apply_schedule_action`
- `_put_schedule_weight_map`

이 영역은 비교적 독립적이라 `services/schedule_service.py`로 분리하기 좋다.

### 4-5. 복사 로직은 나중에 분리해야 함

복사 기능은 캠페인/그룹/키워드/소재/확장소재/제외키워드/타겟/시간대/매체까지 연결되어 있다. 초기에 분리하면 깨질 위험이 크다.

따라서 복사는 마지막 단계에 분리하고, 그 전에 조회/등록/변경 서비스가 안정적으로 나뉘어 있어야 한다.

## 5. 추천 분리 순서

### Stage 1 — 안전한 공통 유틸 분리

목표: 기능 영향이 적은 라벨/엑셀 유틸부터 분리한다.

```text
utils/labels.py
utils/excel.py
```

대상:

- 캠페인/그룹/소재/확장소재 유형 한글화
- 상태값 한글화
- openpyxl 스타일/워크북 생성 보조 함수

### Stage 2 — Export 라우트 분리

목표: 엑셀 내보내기 기능을 독립시킨다.

```text
routes/export_routes.py
services/export_service.py
```

이유:

- 조회/변경/등록보다 부작용이 적음
- 최근 소재 엑셀 한글화 이슈와 직접 연결됨
- 향후 보고용 엑셀 품질 개선이 쉬워짐

### Stage 3 — Lookup 라우트 분리

목표: 조회/검색/미리보기 계열을 분리한다.

```text
routes/lookup_routes.py
services/lookup_service.py
services/keyword_search_service.py
```

이유:

- 최근 렉 개선, 10개 미리보기, 검색 기능과 연결됨
- 화면 속도 개선과 가장 연관이 큼

### Stage 4 — Create 라우트 분리

목표: 캠페인/그룹/키워드/소재/확장소재 등록 로직을 분리한다.

```text
routes/create_routes.py
services/create_service.py
```

### Stage 5 — Update 라우트 분리

목표: 입찰/예산/스케줄/타겟/상태 변경을 분리한다.

```text
routes/update_routes.py
services/bid_service.py
services/schedule_service.py
services/target_service.py
services/state_budget_service.py
```

### Stage 6 — Copy/Delete 라우트 분리

목표: 가장 복잡하고 위험한 복사/삭제 계열을 마지막에 분리한다.

```text
routes/copy_routes.py
routes/delete_routes.py
services/copy_service.py
services/delete_service.py
```

## 6. 다음 패치 제안

다음 패치는 **Stage 1: 라벨/엑셀 유틸 분리**가 가장 안전하다.

권장 작업:

```text
1. utils/labels.py 생성
2. 캠페인/그룹/소재/확장소재/상태값 한글화 함수 이동
3. app.py에는 import만 연결
4. 기존 함수명은 compatibility alias로 유지
5. 소재/확장소재 엑셀 생성 샘플 검증
```

주의:

- `_do_req` 같은 API 공통 함수는 아직 건드리지 않는다.
- 입찰가/스케줄/복사 로직은 아직 이동하지 않는다.
- 프론트 JS는 건드리지 않는다.

## 7. 이번 Stage 0 패치 산출물

```text
BACKEND_REFACTOR_MAP.md
/docs/backend_route_inventory.csv
/docs/backend_route_summary.json
```

이번 패치는 실행 코드 변경이 아니라, 다음 리팩토링의 기준점 생성이다. 따라서 앱 동작에는 영향이 없어야 한다.
